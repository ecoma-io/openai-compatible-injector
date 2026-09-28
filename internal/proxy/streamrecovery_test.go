package proxy

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"openai-compatible-injector/internal/credential"
	"openai-compatible-injector/internal/transport"
)

// The post-commitment recovery loop's tests. Every one of them drives the
// real handler through the real walk: the point of the suite is that the
// continuation is a SECOND orchestrator layered on the committed answer, so
// it has to be observed where it runs — through the client's single response
// recorder, with the upstream's dials counted.
//
// The invariant they collectively pin, in the order it matters:
//   - off by default, an unconfigured deployment's traffic is byte-identical;
//   - a stream that carried a terminal marker is never continued;
//   - a stream this proxy cannot continue fails CLOSED, never guessing;
//   - a hop is one exchange out of the request's own envelope, and its
//     bounds are the ones the request froze;
//   - the client keeps ONE connection and ONE terminal marker throughout.

// dialFunc answers one upstream dial.
type dialFunc func(req *http.Request) (*http.Response, error)

// scriptedDoer answers the n-th dial with the n-th scripted response and
// records the body of every request it was handed, so a test can assert both
// how many times a provider was asked and what the second ask carried. An
// unscripted dial is an error rather than a default answer: a hop the test did
// not expect must not look like a passing run.
type scriptedDoer struct {
	mu     sync.Mutex
	bodies []string
	script []dialFunc
}

func (d *scriptedDoer) Do(req *http.Request) (*http.Response, error) {
	body := ""
	if req.Body != nil {
		b, err := io.ReadAll(req.Body)
		if err != nil {
			return nil, err
		}
		body = string(b)
	}
	d.mu.Lock()
	d.bodies = append(d.bodies, body)
	n := len(d.bodies)
	d.mu.Unlock()
	if n > len(d.script) {
		return nil, errors.New("unscripted dial")
	}
	return d.script[n-1](req)
}

func (d *scriptedDoer) dials() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.bodies)
}

func (d *scriptedDoer) body(i int) string {
	d.mu.Lock()
	defer d.mu.Unlock()
	if i >= len(d.bodies) {
		return ""
	}
	return d.bodies[i]
}

// sseChat renders one chat content delta as a complete SSE event.
func sseChat(content string) string {
	return `data: {"model":"up-a","choices":[{"delta":{"content":"` + content + `"}}]}` + "\n\n"
}

// sseResponses renders one Responses output-text delta, carrying the full
// identity (`item_id`/`output_index`/`content_index`) a real upstream sends:
// those three members are what the accumulator proves the prefix's provenance
// from, so a fixture without them is not a stream this feature will continue.
func sseResponses(delta string) string {
	return sseResponsesIdent("response.output_text.delta", "m1", 0, 0, delta)
}

// sseResponsesIdent renders one Responses text-bearing delta with an explicit
// identity, so a test can make two deltas disagree about which output they
// belong to.
func sseResponsesIdent(typ, itemID string, outputIndex, contentIndex int, delta string) string {
	return `event: ` + typ + "\n" +
		`data: {"type":"` + typ + `","item_id":"` + itemID + `","output_index":` + itoa(outputIndex) +
		`,"content_index":` + itoa(contentIndex) + `,"delta":"` + delta + `"}` + "\n\n"
}

// sseResponsesUnknown renders an event whose type this build does not know.
func sseResponsesUnknown(typ string) string {
	return `event: ` + typ + "\n" + `data: {"type":"` + typ + `"}` + "\n\n"
}

func itoa(n int) string { return strconv.Itoa(n) }

// sseStream answers with a body that ends at EOF — an upstream that closed the
// connection. With no terminal marker in it, the stream was CUT, not finished.
func sseStream(body string) dialFunc {
	return func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
			Body:       io.NopCloser(strings.NewReader(body)),
		}, nil
	}
}

// sseCut answers with a body that yields its bytes and then fails — a reset
// connection rather than a clean close. Both shapes are a cut stream; they
// differ only in whether CopySSE reports a read error.
func sseCut(body string) dialFunc {
	return func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
			Body:       io.NopCloser(&errBody{data: []byte(body)}),
		}, nil
	}
}

// recoveryBlock renders a global recovery block enabling stream recovery.
func recoveryBlock(t *testing.T, stream string) string {
	t.Helper()
	return "recovery:\n  stream:\n" + stream
}

const chatRequest = `{"model":"chain-model","stream":true,"messages":[{"role":"user","content":"hi"}]}`

// recoveryHandler wires a chain store with the given recovery block to a
// scripted direct upstream (candidate pa) and a scripted proxied one
// (candidate pb), returning the handler, the logs, and both doers. Every test
// below asserts on the proxied doer's dial count: a continuation never moves
// the request to another candidate, so pb must stay at zero.
func recoveryHandler(t *testing.T, block string) (http.Handler, *logBuffer, *scriptedDoer, *scriptedDoer) {
	t.Helper()
	store := newChainStore(t, block)
	pa := &scriptedDoer{}
	pb := &scriptedDoer{}
	logBuf, log := captureLog(zerolog.DebugLevel)
	return NewHandler(store, kindResolver{direct: pa, proxied: pb}, nil, nil, nil, log), logBuf, pa, pb
}

// TestStreamRecoveryOffByDefault pins the compatibility hinge at the unit
// level: with no `recovery.stream` block, a stream cut mid-generation ends
// exactly as it always has — one dial, one `stream_completed`, and no
// recovery machinery anywhere in the response or the logs.
func TestStreamRecoveryOffByDefault(t *testing.T) {
	h, logBuf, pa, pb := recoveryHandler(t, "")
	pa.script = []dialFunc{sseCut(sseChat("Hello"))}

	rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions", chatRequest, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want the committed 200", rec.Code)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "Hello") {
		t.Fatalf("relayed events wrong: %s", body)
	}
	if pa.dials() != 1 || pb.dials() != 0 {
		t.Fatalf("dials = %d/%d, want the primary once and the fallback never", pa.dials(), pb.dials())
	}
	for _, slug := range []string{"stream_recovery_started", "stream_recovery_failed", "stream_recovery_succeeded", "stream_recovery_exhausted"} {
		if ev := logBuf.events(t, slug); len(ev) != 0 {
			t.Errorf("%s fired with stream recovery unconfigured: %v", slug, ev)
		}
	}
	// The outcome is the walk's — the cut is a stream_truncated either way,
	// and the recovery fields report zero hops.
	trunc := logBuf.events(t, "stream_truncated")
	if len(trunc) != 1 || trunc[0]["phase"] != "upstream_read" {
		t.Fatalf("stream_truncated = %v, want one upstream_read truncation", trunc)
	}
	if trunc[0]["stream_recoveries"] != float64(0) {
		t.Errorf("stream_recoveries = %v, want 0", trunc[0]["stream_recoveries"])
	}
	if _, ok := trunc[0]["recovery_reason"]; ok {
		t.Errorf("recovery_reason set without a recovery attempt: %v", trunc[0])
	}
}

// pausedReader yields its first chunk, holds the read for a pause, and then
// yields the rest — a slow but perfectly healthy upstream, which is the shape
// a bound must never mistake for a stalled one.
type pausedReader struct {
	first  string
	rest   string
	pause  time.Duration
	closed chan struct{}
	i      int
}

// Close is what makes this reader a faithful stand-in for a response body: a
// body that ignored Close would let the very bug this test exists to catch
// pass it, because closing the body is the only way a watchdog can unblock a
// read.
func (r *pausedReader) Close() error {
	select {
	case <-r.closed:
	default:
		close(r.closed)
	}
	return nil
}

func (r *pausedReader) Read(p []byte) (int, error) {
	switch r.i {
	case 0:
		r.i++
		return copy(p, r.first), nil
	case 1:
		r.i++
		select {
		case <-time.After(r.pause):
			return copy(p, r.rest), nil
		case <-r.closed:
			return 0, errors.New("http: read on closed response body")
		}
	}
	return 0, io.EOF
}

// TestStreamRecoveryWindowIsNotArmedWhenDisabled is the regression this
// hardening pass introduced and then fixed, pinned in the quiet direction.
//
// The window's watchdog closes the upstream body at `max-elapsed`. The
// RESOLVED policy carries a nonzero `max-elapsed` even when the block is
// disabled — it is the default that makes a layer's `enabled: true` meaningful
// — so arming it unconditionally would cut every healthy stream short on
// deployments that never asked for recovery at all. Here the block is present
// and disabled with a deliberately tiny window, and the upstream pauses
// three times longer than it: the stream must still finish, because a
// disabled policy has no window.
func TestStreamRecoveryWindowIsNotArmedWhenDisabled(t *testing.T) {
	h, logBuf, pa, _ := recoveryHandler(t, recoveryBlock(t, "    enabled: false\n    max-elapsed: 150ms\n"))
	upstream := &pausedReader{
		first:  sseChat("Hello"),
		rest:   sseChat(", world") + "data: [DONE]\n\n",
		pause:  450 * time.Millisecond,
		closed: make(chan struct{}),
	}
	pa.script = []dialFunc{func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
			Body:       upstream,
		}, nil
	}}

	rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions", chatRequest, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want the committed 200", rec.Code)
	}
	body := rec.Body.String()
	for _, want := range []string{"Hello", ", world", "[DONE]"} {
		if !strings.Contains(body, want) {
			t.Fatalf("relayed stream is missing %q: %s", want, body)
		}
	}
	if pa.dials() != 1 {
		t.Errorf("dials = %d, want the primary once and no continuation", pa.dials())
	}
	for _, slug := range []string{"stream_recovery_started", "stream_recovery_failed", "stream_recovery_succeeded", "stream_recovery_exhausted"} {
		if ev := logBuf.events(t, slug); len(ev) != 0 {
			t.Errorf("%s fired with stream recovery disabled: %v", slug, ev)
		}
	}
	completed := logBuf.events(t, "stream_completed")
	if len(completed) != 1 {
		t.Fatalf("stream_completed = %v, want one — a paused stream is not a stalled one", completed)
	}
	if got := completed[0]["stream_recoveries"]; got != float64(0) {
		t.Errorf("stream_recoveries = %v, want 0", got)
	}
	if trunc := logBuf.events(t, "stream_truncated"); len(trunc) != 0 {
		t.Errorf("a healthy paused stream was reported truncated: %v", trunc)
	}
}

// TestStreamRecoveryContinuesAChatStream is the feature's whole point: the
// upstream cuts mid-answer, the proxy re-asks the SAME candidate with the
// committed text as an assistant turn, and the client — who never saw a
// second header block — receives one stream ending in one terminal marker.
func TestStreamRecoveryContinuesAChatStream(t *testing.T) {
	h, logBuf, pa, pb := recoveryHandler(t, recoveryBlock(t, "    enabled: true\n"))
	pa.script = []dialFunc{
		sseCut(sseChat("Hello")),
		sseStream(sseChat(", world") + "data: [DONE]\n\n"),
	}

	rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions", chatRequest, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want the committed 200", rec.Code)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "Hello") || !strings.Contains(body, ", world") {
		t.Fatalf("the continuation's events never reached the client: %s", body)
	}
	if n := strings.Count(body, "data: [DONE]"); n != 1 {
		t.Fatalf("terminal markers = %d, want exactly one: %s", n, body)
	}
	if n := strings.Count(body, "Hello"); n != 1 {
		t.Fatalf("the committed text was relayed %d times, want once: %s", n, body)
	}
	// One candidate, twice: the same provider re-asked, never the fallback.
	if pa.dials() != 2 || pb.dials() != 0 {
		t.Fatalf("dials = %d/%d, want the primary twice and the fallback never", pa.dials(), pb.dials())
	}

	// The hop is the original request plus the answer so far, re-injected and
	// re-aliased exactly like any other attempt on this candidate.
	hop := pa.body(1)
	for _, must := range []string{`"role":"assistant"`, `"content":"Hello"`, `"role":"user"`, `"content":"hi"`, "CHAIN-PROMPT-MARKER", `"model":"up-a"`} {
		if !strings.Contains(hop, must) {
			t.Errorf("the continuation body is missing %s: %s", must, hop)
		}
	}
	if strings.Contains(hop, "up-b") {
		t.Errorf("the continuation body carries the wrong candidate's alias: %s", hop)
	}

	started := logBuf.events(t, "stream_recovery_started")
	if len(started) != 1 {
		t.Fatalf("stream_recovery_started = %v, want one", started)
	}
	if started[0]["recovery_index"] != float64(1) || started[0]["partial_bytes"] != float64(5) {
		t.Errorf("started fields = %v, want index 1 and 5 partial bytes", started[0])
	}
	if started[0]["provider"] != "pa" {
		t.Errorf("started provider = %v, want pa", started[0]["provider"])
	}
	if started[0]["upstream"] != "https://a.example/v1" && started[0]["upstream"] != "https://a.example" {
		t.Errorf("started upstream = %v, want pa's origin only", started[0]["upstream"])
	}
	succ := logBuf.events(t, "stream_recovery_succeeded")
	if len(succ) != 1 {
		t.Fatalf("stream_recovery_succeeded = %v, want one", succ)
	}
	if succ[0]["recovered_events"] != float64(2) || succ[0]["upstream_exchanges"] != float64(2) {
		t.Errorf("succeeded fields = %v, want two recovered events over two exchanges", succ[0])
	}
	if ev := logBuf.events(t, "stream_recovery_failed"); len(ev) != 0 {
		t.Errorf("stream_recovery_failed fired on a successful recovery: %v", ev)
	}
	if ev := logBuf.events(t, "stream_recovery_exhausted"); len(ev) != 0 {
		t.Errorf("stream_recovery_exhausted fired on a successful recovery: %v", ev)
	}
	// No truncation record: the stream reached its terminal marker.
	trunc := logBuf.events(t, "stream_truncated")
	if len(trunc) != 0 {
		t.Errorf("stream_truncated fired for a completed stream: %v", trunc)
	}
	done := logBuf.events(t, "request_completed")
	if len(done) != 1 || done[0]["outcome"] != "completed" {
		t.Fatalf("request_completed = %v, want one completed request", done)
	}
	// Both axes count the hop: two provider attempts, two real exchanges.
	if done[0]["provider_attempts"] != float64(2) || done[0]["upstream_exchanges"] != float64(2) {
		t.Errorf("completion counters = %v/%v, want 2/2", done[0]["provider_attempts"], done[0]["upstream_exchanges"])
	}
}

// TestStreamRecoveryContinuesAResponsesStream is the same guarantee on the
// other surface: the continuation items are appended after the client's own
// input, and the hop's own `response.completed` terminates the client's one
// stream.
//
// The hop carries its OWN item id, which is what a real upstream emits for a
// new response. Reusing the committed reply's id on both dials is the fixture
// accident that hid the identity being scoped to the logical stream, so this
// test asserts the realistic shape: the hop's identity differs and the prefix
// still joins.
func TestStreamRecoveryContinuesAResponsesStream(t *testing.T) {
	h, logBuf, pa, pb := recoveryHandler(t, recoveryBlock(t, "    enabled: true\n"))
	pa.script = []dialFunc{
		sseCut(sseResponses("Once")),
		sseStream(sseResponsesIdent("response.output_text.delta", "m2", 0, 0, " upon a time") +
			"event: response.completed\ndata: {}\n\n"),
	}

	rec := doRequest(t, h, http.MethodPost, "/v1/responses",
		`{"model":"chain-model","stream":true,"input":"hi"}`, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want the committed 200", rec.Code)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "Once") || !strings.Contains(body, " upon a time") {
		t.Fatalf("the continuation's events never reached the client: %s", body)
	}
	if n := strings.Count(body, "event: response.completed"); n != 1 {
		t.Fatalf("terminal markers = %d, want exactly one: %s", n, body)
	}
	if pa.dials() != 2 || pb.dials() != 0 {
		t.Fatalf("dials = %d/%d, want the primary twice and the fallback never", pa.dials(), pb.dials())
	}
	hop := pa.body(1)
	for _, must := range []string{`"role":"assistant"`, `"output_text"`, "Once", "Continue it from the exact point", "hi"} {
		if !strings.Contains(hop, must) {
			t.Errorf("the continuation body is missing %s: %s", must, hop)
		}
	}
	if len(logBuf.events(t, "stream_recovery_succeeded")) != 1 {
		t.Fatalf("the Responses hop did not report success: %v", logBuf.events(t, "stream_recovery_succeeded"))
	}
}

// TestStreamRecoveryTakesItsSecondResponsesHop is the defect itself, end to
// end: a Responses stream cut twice. The identity the accumulator proves each
// delta against is a property of ONE upstream response, so hop 2's own
// item_id must not be read as a second output of hop 1's response. Holding it
// against the earlier response refuses the hop as multiple_outputs once the
// verdict is read — which, on a hop that was itself cut, is before the third
// dial is ever made.
//
// The verdict is what this test keys on, deliberately: a hop that reaches its
// own terminal marker breaks the loop on the marker, before the accumulator
// is consulted, so a single-hop test would pass with the identity still
// scoped to the logical stream.
func TestStreamRecoveryTakesItsSecondResponsesHop(t *testing.T) {
	h, logBuf, pa, pb := recoveryHandler(t, recoveryBlock(t, "    enabled: true\n    max-recoveries: 2\n"))
	pa.script = []dialFunc{
		sseCut(sseResponses("Once")),
		sseCut(sseResponsesIdent("response.output_text.delta", "m2", 0, 0, " upon a")),
		sseStream(sseResponsesIdent("response.output_text.delta", "m3", 0, 0, " time") +
			"event: response.completed\ndata: {}\n\n"),
	}

	rec := doRequest(t, h, http.MethodPost, "/v1/responses",
		`{"model":"chain-model","stream":true,"input":"hi"}`, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want the committed 200", rec.Code)
	}
	body := rec.Body.String()
	for _, want := range []string{"Once", " upon a", " time"} {
		if !strings.Contains(body, want) {
			t.Fatalf("the client never saw %q: %s", want, body)
		}
	}
	if n := strings.Count(body, "event: response.completed"); n != 1 {
		t.Fatalf("terminal markers = %d, want exactly one: %s", n, body)
	}
	if pa.dials() != 3 || pb.dials() != 0 {
		t.Fatalf("dials = %d/%d, want the committed candidate re-asked twice", pa.dials(), pb.dials())
	}
	if ev := logBuf.events(t, "stream_recovery_exhausted"); len(ev) != 0 {
		t.Fatalf("a recovered stream reported exhaustion: %v", ev)
	}
	if len(logBuf.events(t, "stream_recovery_succeeded")) != 1 {
		t.Fatalf("the second hop did not report success")
	}
	if len(logBuf.events(t, "stream_recovery_started")) != 2 {
		t.Fatalf("stream_recovery_started = %v, want both hops", logBuf.events(t, "stream_recovery_started"))
	}
}

// TestStreamRecoveryComparesADoneWithItsOwnHop is the boundary's second
// facet, and it needs a cut after the .done on purpose. A hop's
// output_text.done states the hop's OWN text, so it is compared with the
// hop's own deltas; a hop that emits nothing but a .done has a non-empty
// CROSS-HOP prefix, which is exactly the case a comparison against the whole
// prefix gets wrong — it refuses the hop for being correct. The cut is what
// forces the verdict to be read: the .done also latches the accumulator's
// logical terminal, so with the boundary fixed the loop stops as
// logical_terminal rather than as an unsafe stream.
func TestStreamRecoveryComparesADoneWithItsOwnHop(t *testing.T) {
	h, logBuf, pa, _ := recoveryHandler(t, recoveryBlock(t, "    enabled: true\n    max-recoveries: 2\n"))
	pa.script = []dialFunc{
		sseCut(sseResponses("Once")),
		sseCut(
			"event: response.output_text.done\n" +
				`data: {"type":"response.output_text.done","item_id":"m2","output_index":0,"content_index":0,"text":" upon a time"}` + "\n\n"),
	}

	rec := doRequest(t, h, http.MethodPost, "/v1/responses",
		`{"model":"chain-model","stream":true,"input":"hi"}`, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want the committed 200", rec.Code)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "Once") || !strings.Contains(body, " upon a time") {
		t.Fatalf("the hop's own done never reached the client: %s", body)
	}
	exh := logBuf.events(t, "stream_recovery_exhausted")
	if len(exh) != 1 {
		t.Fatalf("stream_recovery_exhausted = %v, want the one bound", exh)
	}
	if exh[0]["reason"] != recoveryLogicalTerminal {
		t.Fatalf("reason = %v, want %q — the hop's own done was refused instead of ending the answer",
			exh[0]["reason"], recoveryLogicalTerminal)
	}
	if _, ok := exh[0]["unsafe_reason"]; ok {
		t.Fatalf("a correct hop was reported as unsafe content: %v", exh[0])
	}
}

// TestStreamRecoverySkipsATerminatedStream pins the terminal-marker gate: the
// client already received its terminal event, so the answer is FINISHED and a
// hop would append to it. Whatever happens to the connection afterwards, the
// stream is never continued.
func TestStreamRecoverySkipsATerminatedStream(t *testing.T) {
	h, logBuf, pa, _ := recoveryHandler(t, recoveryBlock(t, "    enabled: true\n"))
	pa.script = []dialFunc{sseCut(sseChat("Hello") + "data: [DONE]\n\n")}

	rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions", chatRequest, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want the committed 200", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "data: [DONE]") {
		t.Fatalf("the terminal marker never reached the client: %s", rec.Body.String())
	}
	if pa.dials() != 1 {
		t.Fatalf("dials = %d, want the terminal stream never re-asked", pa.dials())
	}
	if ev := logBuf.events(t, "stream_recovery_started"); len(ev) != 0 {
		t.Errorf("a terminated stream was continued: %v", ev)
	}
	if ev := logBuf.events(t, "stream_recovery_exhausted"); len(ev) != 0 {
		t.Errorf("a terminated stream reported exhaustion: %v", ev)
	}
}

// TestStreamRecoveryRefusesToolCalls is the fail-closed half for the hard
// case the feature was told to disable rather than attempt: a stream that
// carries a tool call cannot be continued without risking a duplicated call
// or a hallucinated result, so the proxy stops — and says so.
func TestStreamRecoveryRefusesToolCalls(t *testing.T) {
	h, logBuf, pa, _ := recoveryHandler(t, recoveryBlock(t, "    enabled: true\n"))
	pa.script = []dialFunc{sseCut(sseChat("Let me check") +
		`data: {"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call_1"}]}}]}` + "\n\n")}

	rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions", chatRequest, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want the committed 200", rec.Code)
	}
	if n := strings.Count(rec.Body.String(), "tool_calls"); n != 1 {
		t.Fatalf("the tool call was not relayed exactly once: %s", rec.Body.String())
	}
	if pa.dials() != 1 {
		t.Fatalf("dials = %d, want no hop on an unsafe stream", pa.dials())
	}
	if ev := logBuf.events(t, "stream_recovery_started"); len(ev) != 0 {
		t.Errorf("an unsafe stream was dialed anyway: %v", ev)
	}
	exh := logBuf.events(t, "stream_recovery_exhausted")
	if len(exh) != 1 {
		t.Fatalf("stream_recovery_exhausted = %v, want one", exh)
	}
	if exh[0]["reason"] != "unsafe_content" || exh[0]["unsafe_reason"] != "tool_calls" {
		t.Errorf("exhaustion reason = %v/%v, want unsafe_content/tool_calls", exh[0]["reason"], exh[0]["unsafe_reason"])
	}
	trunc := logBuf.events(t, "stream_truncated")
	if len(trunc) != 1 || trunc[0]["recovery_reason"] != "unsafe_content" {
		t.Fatalf("stream_truncated = %v, want the unsafe-content reason", trunc)
	}
}

// TestStreamRecoveryRefusesAStreamWithNoText: a stream that produced no
// assistant text at all has nothing to continue FROM, so a hop would re-ask
// the original question — a blind retry wearing a continuation's shape.
func TestStreamRecoveryRefusesAStreamWithNoText(t *testing.T) {
	h, logBuf, pa, _ := recoveryHandler(t, recoveryBlock(t, "    enabled: true\n"))
	pa.script = []dialFunc{sseCut(`data: {"choices":[{"delta":{"role":"assistant"}}]}` + "\n\n")}

	doRequest(t, h, http.MethodPost, "/v1/chat/completions", chatRequest, nil)
	if pa.dials() != 1 {
		t.Fatalf("dials = %d, want no hop without a prefix", pa.dials())
	}
	exh := logBuf.events(t, "stream_recovery_exhausted")
	if len(exh) != 1 || exh[0]["unsafe_reason"] != "no_prefix" {
		t.Fatalf("stream_recovery_exhausted = %v, want one no_prefix refusal", exh)
	}
}

// TestStreamRecoveryRefusesAnOversizePrefix: max-partial-bytes bounds the
// answer this proxy will hold in memory to build a continuation from. Past
// it, the stream is refused — the bound is a refusal, never a silent
// truncation of the prefix, which would hand the model a hole to write around.
func TestStreamRecoveryRefusesAnOversizePrefix(t *testing.T) {
	h, logBuf, pa, _ := recoveryHandler(t,
		recoveryBlock(t, "    enabled: true\n    max-partial-bytes: 1024\n"))
	pa.script = []dialFunc{sseCut(sseChat(strings.Repeat("x", 2048)))}

	doRequest(t, h, http.MethodPost, "/v1/chat/completions", chatRequest, nil)
	if pa.dials() != 1 {
		t.Fatalf("dials = %d, want no hop past the prefix bound", pa.dials())
	}
	exh := logBuf.events(t, "stream_recovery_exhausted")
	if len(exh) != 1 || exh[0]["unsafe_reason"] != "oversize" {
		t.Fatalf("stream_recovery_exhausted = %v, want one oversize refusal", exh)
	}
}

// TestStreamRecoveryStopsAtItsWindow: max-elapsed closes the recovery window,
// so a stream whose generation ran long is left truncated rather than chased.
//
// The clock is what makes the window observable without a sleep, and it is
// deliberately two-phased: it stands still until the upstream is dialed — so
// the walk's own budgets and envelopes see an ordinary request — and then
// advances a minute per read, so the window is already closed by the time the
// relay reports what happened.
func TestStreamRecoveryStopsAtItsWindow(t *testing.T) {
	origNow := retryNow
	t.Cleanup(func() { retryNow = origNow })
	base := time.Now()
	var dialed atomic.Bool
	var reads atomic.Int64
	retryNow = func() time.Time {
		if !dialed.Load() {
			return base
		}
		return base.Add(time.Duration(reads.Add(1)) * time.Minute)
	}

	h, logBuf, pa, _ := recoveryHandler(t,
		recoveryBlock(t, "    enabled: true\n    max-elapsed: 5s\n"))
	pa.script = []dialFunc{func(req *http.Request) (*http.Response, error) {
		dialed.Store(true)
		return sseCut(sseChat("Hello"))(req)
	}}

	doRequest(t, h, http.MethodPost, "/v1/chat/completions", chatRequest, nil)
	if pa.dials() != 1 {
		t.Fatalf("dials = %d, want no hop past the recovery window", pa.dials())
	}
	exh := logBuf.events(t, "stream_recovery_exhausted")
	if len(exh) != 1 || exh[0]["reason"] != "max_elapsed" {
		t.Fatalf("stream_recovery_exhausted = %v, want one max_elapsed refusal", exh)
	}
}

// TestStreamRecoveryRefusesATruncatedLine: the last line of a cut stream can
// be torn mid-JSON. It reached the client, but it is not a prefix this proxy
// can read, so it is refused rather than guessed at.
func TestStreamRecoveryRefusesATruncatedLine(t *testing.T) {
	h, logBuf, pa, _ := recoveryHandler(t, recoveryBlock(t, "    enabled: true\n"))
	pa.script = []dialFunc{sseStream(`data: {"choices":[{"delta":{"content":"Hi","fi`)}

	doRequest(t, h, http.MethodPost, "/v1/chat/completions", chatRequest, nil)
	if pa.dials() != 1 {
		t.Fatalf("dials = %d, want no hop on an unreadable prefix", pa.dials())
	}
	exh := logBuf.events(t, "stream_recovery_exhausted")
	if len(exh) != 1 || exh[0]["unsafe_reason"] != "not_object" {
		t.Fatalf("stream_recovery_exhausted = %v, want one not_object refusal", exh)
	}
}

// TestStreamRecoveryHopFailureTruncatesCleanly: the hop answered with a
// status, not a stream. The client's headers are long since written, so the
// status is evidence and nothing more — no error body is spliced into an SSE
// stream, no second header block is written, and the stream ends truncated
// exactly as it would have without the feature.
func TestStreamRecoveryHopFailureTruncatesCleanly(t *testing.T) {
	h, logBuf, pa, _ := recoveryHandler(t, recoveryBlock(t, "    enabled: true\n"))
	pa.script = []dialFunc{
		sseStream(sseChat("Hello")),
		func(*http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusInternalServerError,
				Header:     http.Header{"Content-Type": []string{"application/json"}},
				Body:       io.NopCloser(strings.NewReader(`{"error":{"message":"boom"}}`)),
			}, nil
		},
	}

	rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions", chatRequest, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want the already-committed 200", rec.Code)
	}
	body := rec.Body.String()
	if strings.Contains(body, "boom") || strings.Contains(body, "upstream provider returned") {
		t.Fatalf("the hop's error body reached the client's stream: %s", body)
	}
	if pa.dials() != 2 {
		t.Fatalf("dials = %d, want exactly one hop", pa.dials())
	}
	failed := logBuf.events(t, "stream_recovery_failed")
	if len(failed) != 1 {
		t.Fatalf("stream_recovery_failed = %v, want one", failed)
	}
	if failed[0]["phase"] != "upstream_status" || failed[0]["upstream_status"] != float64(500) {
		t.Errorf("failure fields = %v, want phase upstream_status and status 500", failed[0])
	}
	trunc := logBuf.events(t, "stream_truncated")
	if len(trunc) != 1 {
		t.Fatalf("stream_truncated = %v, want one", trunc)
	}
	// The relay itself did not fail — the continuation did. That is its own
	// phase, and the error field is absent rather than invented.
	if trunc[0]["phase"] != "recovery" {
		t.Errorf("truncation phase = %v, want recovery", trunc[0]["phase"])
	}
	if _, hasErr := trunc[0]["error"]; hasErr {
		t.Errorf("a forced truncation carries an invented error field: %v", trunc[0])
	}
	if trunc[0]["stream_recoveries"] != float64(1) {
		t.Errorf("stream_recoveries = %v, want 1", trunc[0]["stream_recoveries"])
	}
}

// TestStreamRecoveryStopsAtASpentBudget: a continuation is real outbound
// traffic, so it pays out of the request's own exchange envelope. When the
// walk has already spent it, the hop is refused before any dial.
func TestStreamRecoveryStopsAtASpentBudget(t *testing.T) {
	h, logBuf, pa, _ := recoveryHandler(t, "recovery:\n"+
		"  retries:\n    max-retries: 0\n"+
		"  budget:\n    request:\n      max-exchanges: 1\n    candidate:\n      max-exchanges: 1\n"+
		"  stream:\n    enabled: true\n")
	pa.script = []dialFunc{sseStream(sseChat("Hello"))}

	doRequest(t, h, http.MethodPost, "/v1/chat/completions", chatRequest, nil)
	if pa.dials() != 1 {
		t.Fatalf("dials = %d, want the hop refused before the wire", pa.dials())
	}
	exh := logBuf.events(t, "stream_recovery_exhausted")
	if len(exh) != 1 || exh[0]["reason"] != "budget_spent" {
		t.Fatalf("stream_recovery_exhausted = %v, want one budget_spent refusal", exh)
	}
	trunc := logBuf.events(t, "stream_truncated")
	if len(trunc) != 1 || trunc[0]["recovery_reason"] != "budget_spent" {
		t.Fatalf("stream_truncated = %v, want the budget reason", trunc)
	}
	// Zero hops, and the record says so: the refusal is distinguishable from
	// an attempted continuation that failed.
	if trunc[0]["stream_recoveries"] != float64(0) {
		t.Errorf("stream_recoveries = %v, want 0 for a refused hop", trunc[0]["stream_recoveries"])
	}
}

// zeroDialRefuser is a transport.Doer that is ALSO a transport.Executor and
// answers its refuseOn-th Execute with the pool's ZERO-DIAL BUDGET REFUSAL.
//
// That shape is not invented here: internal/transport/pool.go returns exactly
// it — a nil response, a nil error, BudgetExhausted set — when the request's
// exchange envelope will not fund an attempt's first dial. It is the one
// Execute outcome that carries NOTHING to read: no answer and no error. A
// continuation hop that reached for a status would dereference the response
// that was never produced, so the hop has to name a phase for it instead.
//
// Refusing on the n-th call is what keeps the test honest: call 1 is the
// WALK's own attempt and must behave normally, so the refusal below lands on
// the hop and nowhere else.
type zeroDialRefuser struct {
	refuseOn int
	inner    *scriptedDoer
	mu       sync.Mutex
	calls    int
}

func (d *zeroDialRefuser) Do(req *http.Request) (*http.Response, error) {
	return d.inner.Do(req)
}

func (d *zeroDialRefuser) Execute(ar *transport.AttemptRequest) (*http.Response, transport.AttemptInfo, error) {
	d.mu.Lock()
	d.calls++
	n := d.calls
	d.mu.Unlock()
	if n == d.refuseOn {
		return nil, transport.AttemptInfo{BudgetExhausted: true}, nil
	}
	req, err := http.NewRequest(ar.Method, ar.URL.String(), bytes.NewReader(ar.Body))
	if err != nil {
		return nil, transport.AttemptInfo{}, err
	}
	req.Header = ar.Header.Clone()
	resp, err := d.inner.Do(req)
	return resp, transport.AttemptInfo{Attempts: 1}, err
}

// TestStreamRecoveryPooledBudgetRefusalIsAPhase pins the hop's reading of the
// one Execute outcome that is neither an answer nor an error. The refusal is
// reported as the budget phase — nothing was dialed and no endpoint is at
// fault — the client's stream truncates cleanly as it would have with the
// feature off, and the loop never reads a status off a response that does not
// exist.
func TestStreamRecoveryPooledBudgetRefusalIsAPhase(t *testing.T) {
	store := newChainStore(t, recoveryBlock(t, "    enabled: true\n"))
	inner := &scriptedDoer{script: []dialFunc{sseCut(sseChat("Hello"))}}
	// The walk's attempt is Execute call 1; the hop's is call 2.
	pooled := &zeroDialRefuser{refuseOn: 2, inner: inner}
	logBuf, log := captureLog(zerolog.DebugLevel)

	h := NewHandler(store, kindResolver{direct: pooled, proxied: inner}, nil, nil, nil, log)

	rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions", chatRequest, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want the already-committed 200", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "Hello") {
		t.Fatalf("relayed events wrong: %s", rec.Body.String())
	}
	if inner.dials() != 1 {
		t.Fatalf("upstream dials = %d, want the walk's one and no hop", inner.dials())
	}
	failed := logBuf.events(t, "stream_recovery_failed")
	if len(failed) != 1 {
		t.Fatalf("stream_recovery_failed = %v, want one refusal", failed)
	}
	if failed[0]["phase"] != "budget" {
		t.Errorf("phase = %v, want budget for a refusal that reached no wire", failed[0]["phase"])
	}
	if failed[0]["upstream_status"] != nil {
		t.Errorf("upstream_status = %v, want none for a hop that never dialed", failed[0]["upstream_status"])
	}
	if _, hasErr := failed[0]["error"]; hasErr {
		t.Errorf("a refusal this proxy made carries an invented error field: %v", failed[0])
	}
	if ev := logBuf.events(t, "stream_recovery_succeeded"); len(ev) != 0 {
		t.Errorf("stream_recovery_succeeded fired for a refused hop: %v", ev)
	}
	// The stream was already cut before the hop, so the truncation's phase and
	// error belong to that relay — the hop's refusal is on its own event
	// above. What the truncation must NOT carry is a recovery_reason: that
	// field names a bound the loop stopped at, and a refused hop is a failure,
	// not a bound.
	trunc := logBuf.events(t, "stream_truncated")
	if len(trunc) != 1 {
		t.Fatalf("stream_truncated = %v, want one clean truncation", trunc)
	}
	if trunc[0]["stream_recoveries"] != float64(1) {
		t.Errorf("stream_recoveries = %v, want the refused hop counted", trunc[0]["stream_recoveries"])
	}
	if _, has := trunc[0]["recovery_reason"]; has {
		t.Errorf("a refused hop reported a recovery_reason: %v", trunc[0])
	}
	if ev := logBuf.events(t, "stream_completed"); len(ev) != 0 {
		t.Errorf("stream_completed fired for an unterminated stream: %v", ev)
	}
}

// TestStreamRecoveryStopsAtItsReach: the reach is a bound on hops, not a
// target — a stream that keeps dying gets exactly `max-recoveries` asks and
// then stops, whatever the upstream does next.
func TestStreamRecoveryStopsAtItsReach(t *testing.T) {
	h, logBuf, pa, _ := recoveryHandler(t,
		recoveryBlock(t, "    enabled: true\n    max-recoveries: 2\n"))
	pa.script = []dialFunc{
		sseStream(sseChat("one")),
		sseStream(sseChat("two")),
		sseStream(sseChat("three")),
	}

	rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions", chatRequest, nil)
	body := rec.Body.String()
	if !strings.Contains(body, "one") || !strings.Contains(body, "two") || !strings.Contains(body, "three") {
		t.Fatalf("the recovered hops never reached the client: %s", body)
	}
	// One walk attempt plus exactly max-recoveries hops: the third script
	// entry is never asked for, which is what makes the reach a bound.
	if pa.dials() != 3 {
		t.Fatalf("dials = %d, want the walk attempt plus two hops", pa.dials())
	}
	if n := len(logBuf.events(t, "stream_recovery_started")); n != 2 {
		t.Fatalf("stream_recovery_started = %d, want one per hop", n)
	}
	exh := logBuf.events(t, "stream_recovery_exhausted")
	if len(exh) != 1 || exh[0]["reason"] != "max_recoveries" || exh[0]["recoveries"] != float64(2) {
		t.Fatalf("stream_recovery_exhausted = %v, want two recoveries and the reach reason", exh)
	}
}

// TestStreamRecoveryStopsWhenTheClientLeaves: a canceled client ends the
// recovery effort before the next dial — no upstream exchange is spent on a
// stream nobody is reading, and the outcome names the disconnect rather than
// a provider fault.
func TestStreamRecoveryStopsWhenTheClientLeaves(t *testing.T) {
	store := newChainStore(t, recoveryBlock(t, "    enabled: true\n"))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pa := &scriptedDoer{script: []dialFunc{func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
			Body:       &selfCancelingBody{data: []byte(sseChat("Hello")), cancel: cancel},
		}, nil
	}}}
	logBuf, log := captureLog(zerolog.DebugLevel)
	h := NewHandler(store, kindResolver{direct: pa, proxied: &scriptedDoer{}}, nil, nil, nil, log)

	doRequestWithContext(t, h, ctx, http.MethodPost, "/v1/chat/completions", chatRequest, nil)
	if pa.dials() != 1 {
		t.Fatalf("dials = %d, want no hop after the client left", pa.dials())
	}
	if ev := logBuf.events(t, "stream_recovery_started"); len(ev) != 0 {
		t.Errorf("a hop was started for a departed client: %v", ev)
	}
	done := logBuf.events(t, "request_completed")
	if len(done) != 1 || done[0]["outcome"] != "client_disconnected" {
		t.Fatalf("request_completed = %v, want client_disconnected", done)
	}
}

// TestStreamRecoveryStopsWhenTheClientLeavesMidHop: the client can walk away
// while a hop is in flight, and the loop must notice before it spends another
// exchange on a stream nobody is reading.
//
// The cancel fires from inside the hop's own dial, so the window is exact
// rather than timed — and the hop answers with a clean EOF, which is what
// isolates the caller gate: every other gate would have let a second hop
// through. The outcome then names the disconnect rather than blaming the
// upstream for a truncation this proxy chose.
func TestStreamRecoveryStopsWhenTheClientLeavesMidHop(t *testing.T) {
	store := newChainStore(t, recoveryBlock(t, "    enabled: true\n    max-recoveries: 2\n"))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pa := &scriptedDoer{script: []dialFunc{
		sseStream(sseChat("Hello")),
		func(req *http.Request) (*http.Response, error) {
			cancel() // the client leaves with the hop in flight
			return sseStream(sseChat(", world"))(req)
		},
		sseStream(sseChat("never asked")),
	}}
	logBuf, log := captureLog(zerolog.DebugLevel)
	h := NewHandler(store, kindResolver{direct: pa, proxied: &scriptedDoer{}}, nil, nil, nil, log)

	doRequestWithContext(t, h, ctx, http.MethodPost, "/v1/chat/completions", chatRequest, nil)
	if pa.dials() != 2 {
		t.Fatalf("dials = %d, want the walk attempt and the one hop that was in flight", pa.dials())
	}
	if n := len(logBuf.events(t, "stream_recovery_started")); n != 1 {
		t.Errorf("stream_recovery_started = %d, want 1: the next hop was refused", n)
	}
	// No bound was reached, so there is nothing to report as exhausted.
	if ev := logBuf.events(t, "stream_recovery_exhausted"); len(ev) != 0 {
		t.Errorf("stream_recovery_exhausted fired for a departed client: %v", ev)
	}
	done := logBuf.events(t, "request_completed")
	if len(done) != 1 || done[0]["outcome"] != "client_disconnected" {
		t.Fatalf("request_completed = %v, want client_disconnected", done)
	}
}

// selfCancelingBody yields its bytes once, then cancels the request context
// and reports it — the deterministic shape of a client that walks away while
// the upstream is mid-generation. A timer would be a race; this is not.
type selfCancelingBody struct {
	data   []byte
	cancel context.CancelFunc
	used   bool
}

func (b *selfCancelingBody) Read(p []byte) (int, error) {
	if !b.used {
		b.used = true
		return copy(p, b.data), nil
	}
	b.cancel()
	return 0, context.Canceled
}

func (b *selfCancelingBody) Close() error { return nil }

// parkedBody is an upstream that sends its bytes and then HOLDS THE
// CONNECTION OPEN: the next read parks until something closes the body.
//
// It is the shape `max-elapsed` exists for. A peer that streams a partial
// event and then goes quiet leaves the relay parked inside Body.Read, where
// no check the loop makes afterwards can reach it — the recovery window's
// watchdog closing the body is the only lever that returns the read. Before
// this body existed the suite had nothing that could block a read at all, so
// the window was only ever observed as a clock reading, never as a bound.
//
// ctx, when non-nil, models what net/http does to a REAL response body when
// the client goes away: the parked read is aborted with the context's error.
// A nil ctx is an upstream whose socket nobody else will close.
type parkedBody struct {
	data []byte
	ctx  context.Context
	// closed is closed by Close. The watchdog calls it.
	closed chan struct{}
	once   sync.Once
	used   bool
	// released is set by the parked read itself, so a test can prove the read
	// RETURNED rather than merely that a timer fired somewhere near it: a
	// relay goroutine left parked on an upstream that never speaks again is
	// exactly the leak this bound must not create.
	released atomic.Bool
}

func newParkedBody(ctx context.Context, data string) *parkedBody {
	return &parkedBody{data: []byte(data), ctx: ctx, closed: make(chan struct{})}
}

func (b *parkedBody) Read(p []byte) (int, error) {
	if !b.used {
		b.used = true
		return copy(p, b.data), nil
	}
	var clientGone <-chan struct{}
	if b.ctx != nil {
		clientGone = b.ctx.Done()
	}
	select {
	case <-b.closed:
		b.released.Store(true)
		return 0, errors.New("http: read on closed response body")
	case <-clientGone:
		b.released.Store(true)
		return 0, b.ctx.Err()
	case <-time.After(parkedSafety):
		// NOT a behaviour of the service: a regression that leaves the read
		// parked forever would otherwise hang the suite until the package
		// timeout. The test fails on its assertions instead.
		b.released.Store(true)
		return 0, errors.New("parked read was never released")
	}
}

func (b *parkedBody) Close() error {
	b.once.Do(func() { close(b.closed) })
	return nil
}

// parkedSafety is the test-only escape hatch above. It must be far longer
// than any window a test arms, and a test that reaches it fails.
const parkedSafety = 20 * time.Second

// sseParked answers with a body that streams `body` and then holds the
// connection open, optionally aborting on the client's context.
func sseParked(ctx context.Context, body string) (*parkedBody, dialFunc) {
	b := newParkedBody(ctx, body)
	return b, func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
			Body:       b,
		}, nil
	}
}

// TestStreamRecoveryMaxElapsedCutsABlockedRead is the hard-bound half of
// `max-elapsed`, and the case that used to be a hole: an upstream that sends
// a partial event and then holds the TCP connection open.
//
// The sequence the test drives is the one the bound has to cover — the partial
// event reaches the client, the relay parks in the next read, the window
// expires, the watchdog closes the upstream body, the read returns, and the
// loop stops. Without the watchdog the relay never returns at all: there is no
// read deadline anywhere else on this path, and the check that reads
// `max-elapsed` sits after the very call that is blocked.
func TestStreamRecoveryMaxElapsedCutsABlockedRead(t *testing.T) {
	h, logBuf, pa, _ := recoveryHandler(t,
		recoveryBlock(t, "    enabled: true\n    max-elapsed: 150ms\n"))
	body, dial := sseParked(context.Background(), sseChat("Hello"))
	pa.script = []dialFunc{dial}

	rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions", chatRequest, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want the already-committed 200", rec.Code)
	}
	// The partial event was relayed before the read parked: the bound cuts the
	// stream where it stopped, it does not withhold what already happened.
	if !strings.Contains(rec.Body.String(), "Hello") {
		t.Fatalf("the committed partial event never reached the client: %s", rec.Body.String())
	}
	if !body.released.Load() {
		t.Fatal("the relay's read never returned: the window closed no upstream body")
	}
	if pa.dials() != 1 {
		t.Fatalf("dials = %d, want no hop past the window", pa.dials())
	}
	if ev := logBuf.events(t, "stream_recovery_started"); len(ev) != 0 {
		t.Errorf("a hop was dialed past the window: %v", ev)
	}
	exh := logBuf.events(t, "stream_recovery_exhausted")
	if len(exh) != 1 || exh[0]["reason"] != "max_elapsed" {
		t.Fatalf("stream_recovery_exhausted = %v, want one max_elapsed refusal", exh)
	}
	trunc := logBuf.events(t, "stream_truncated")
	if len(trunc) != 1 {
		t.Fatalf("stream_truncated = %v, want one", trunc)
	}
	if trunc[0]["recovery_reason"] != "max_elapsed" {
		t.Errorf("truncation reason = %v, want max_elapsed", trunc[0]["recovery_reason"])
	}
	// The error the relay saw is THIS proxy's closed body. Left in place it
	// would be reported as an upstream read failure, which is the one reading
	// an operator must not get from their own configured bound.
	if _, hasErr := trunc[0]["error"]; hasErr {
		t.Errorf("the bound reported an upstream error it invented: %v", trunc[0])
	}
	if trunc[0]["phase"] != "recovery" {
		t.Errorf("truncation phase = %v, want recovery", trunc[0]["phase"])
	}
	done := logBuf.events(t, "request_completed")
	if len(done) != 1 || done[0]["outcome"] != "stream_truncated" {
		t.Fatalf("request_completed = %v, want stream_truncated", done)
	}
	// max_elapsed must stay tellable apart from the three causes it is
	// adjacent to. None of them can produce this pair.
	if done[0]["outcome"] == "client_disconnected" {
		t.Errorf("the operator's own bound was reported as a client disconnect: %v", done[0])
	}
}

// TestStreamRecoveryWindowCoversEveryHop: one window since the commit, not a
// fresh one per hop.
//
// The first relay ends cleanly but only after most of the window has already
// run, so the hop starts with almost nothing left. The SAME instant the relay
// used must be the one that closes the hop's parked read.
//
// Measuring the WHOLE request is the only assertion that tells one shared
// window from N per-hop ones: with a fresh window per hop the total would be
// the committed relay's pause PLUS a full window for the hop, so the request
// would run for roughly twice the configured bound. A test that only asserted
// "the hop was cut as max_elapsed" cannot tell the two apart — a per-hop
// window cuts it too, just later — and that is exactly how a per-hop budget
// passes a test named for a shared one.
func TestStreamRecoveryWindowCoversEveryHop(t *testing.T) {
	const window = 300 * time.Millisecond
	// The committed relay spends ~200ms of the 300ms window before its stream
	// ends cleanly, leaving the hop ~100ms of the SAME window.
	const firstPause = 200 * time.Millisecond
	h, logBuf, pa, _ := recoveryHandler(t,
		recoveryBlock(t, "    enabled: true\n    max-elapsed: 300ms\n    max-recoveries: 2\n"))
	body, dial := sseParked(context.Background(), sseChat(", world"))
	pa.script = []dialFunc{
		func(*http.Request) (*http.Response, error) {
			// A slow but perfectly healthy committed stream: it yields text,
			// pauses, then ends at EOF. The pause is real time the window
			// must account for.
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
				Body: io.NopCloser(&pausedReader{
					first:  sseChat("Hello"),
					pause:  firstPause,
					closed: make(chan struct{}),
				}),
			}, nil
		},
		dial,
	}

	start := time.Now()
	rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions", chatRequest, nil)
	elapsed := time.Since(start)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want the already-committed 200", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "Hello") {
		t.Fatalf("the first hop's text never reached the client: %s", rec.Body.String())
	}
	if !body.released.Load() {
		t.Fatal("the hop's parked read never returned: the window did not cover the hop")
	}
	if pa.dials() != 2 {
		t.Fatalf("dials = %d, want the walk attempt and the one hop", pa.dials())
	}
	exh := logBuf.events(t, "stream_recovery_exhausted")
	if len(exh) != 1 || exh[0]["reason"] != "max_elapsed" {
		t.Fatalf("stream_recovery_exhausted = %v, want one max_elapsed refusal", exh)
	}
	// The hop DID happen — it was under the bound when it started. What the
	// bound cut is the read, and the count says so.
	if exh[0]["recoveries"] != float64(1) {
		t.Errorf("recoveries = %v, want the hop that was cut counted", exh[0]["recoveries"])
	}
	// THE ASSERTION THAT SEPARATES ONE WINDOW FROM N. Generous headroom on
	// both sides, because CI scheduling is not a clock this test can trust to
	// the millisecond: the bound must be beaten by a comfortable margin, and a
	// per-hop window (pause + a fresh full window ≈ 2×) must miss it by one.
	if elapsed > window+150*time.Millisecond {
		t.Errorf("request took %v against a %v window: a fresh window per hop would let the "+
			"hop outlive the instant the committed relay used", elapsed.Round(time.Millisecond), window)
	}
}

// TestStreamRecoveryClientCancelBeatsTheWindow: the client's context is a
// higher hard-stop than any recovery policy, and it stays distinguishable
// from the window even when both are live. Here the window is wide open
// (10s) and the client leaves while the read is parked: the read returns the
// context's error, the caller gate refuses every further hop, and the record
// names the disconnect rather than the bound.
func TestStreamRecoveryClientCancelBeatsTheWindow(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	h, logBuf, pa, _ := recoveryHandler(t,
		recoveryBlock(t, "    enabled: true\n    max-elapsed: 10s\n"))
	body, dial := sseParked(ctx, sseChat("Hello"))
	pa.script = []dialFunc{dial}

	// The client leaves 40ms in, while the relay is parked.
	go func() {
		time.Sleep(40 * time.Millisecond)
		cancel()
	}()

	rec := doRequestWithContext(t, h, ctx, http.MethodPost, "/v1/chat/completions", chatRequest, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want the already-committed 200", rec.Code)
	}
	if !body.released.Load() {
		t.Fatal("the parked read never returned after the client left")
	}
	if pa.dials() != 1 {
		t.Fatalf("dials = %d, want zero recovery requests after the client left", pa.dials())
	}
	for _, slug := range []string{"stream_recovery_started", "stream_recovery_failed", "stream_recovery_succeeded", "stream_recovery_exhausted"} {
		if ev := logBuf.events(t, slug); len(ev) != 0 {
			t.Errorf("%s fired for a departed client: %v", slug, ev)
		}
	}
	done := logBuf.events(t, "request_completed")
	if len(done) != 1 || done[0]["outcome"] != "client_disconnected" {
		t.Fatalf("request_completed = %v, want client_disconnected", done)
	}
	trunc := logBuf.events(t, "stream_truncated")
	if len(trunc) != 1 {
		t.Fatalf("stream_truncated = %v, want one", trunc)
	}
	if _, has := trunc[0]["recovery_reason"]; has {
		t.Errorf("a departed client reported a recovery bound: %v", trunc[0])
	}
}

// TestStreamRecoveryWindowOutranksASimultaneousDisconnect pins the gate order
// when the window and the reader both go away: the bound the operator
// configured is the answer. The construction is exact rather than timed — the
// client is already gone when the watchdog closes the body — so what it
// proves is the ordering, not a race that happened to land one way.
func TestStreamRecoveryWindowOutranksASimultaneousDisconnect(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	h, logBuf, pa, _ := recoveryHandler(t,
		recoveryBlock(t, "    enabled: true\n    max-elapsed: 120ms\n"))
	// The body ignores the client's context and cancels it instead: the relay
	// stays parked on an upstream that will not speak again, and by the time
	// the window fires the caller is already gone.
	body := newParkedBody(context.Background(), sseChat("Hello"))
	canceled := make(chan struct{})
	pa.script = []dialFunc{func(*http.Request) (*http.Response, error) {
		go func() {
			time.Sleep(20 * time.Millisecond)
			cancel()
			close(canceled)
		}()
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
			Body:       body,
		}, nil
	}}

	doRequestWithContext(t, h, ctx, http.MethodPost, "/v1/chat/completions", chatRequest, nil)
	<-canceled
	if !body.released.Load() {
		t.Fatal("the parked read never returned")
	}
	if pa.dials() != 1 {
		t.Fatalf("dials = %d, want zero recovery requests", pa.dials())
	}
	exh := logBuf.events(t, "stream_recovery_exhausted")
	if len(exh) != 1 || exh[0]["reason"] != "max_elapsed" {
		t.Fatalf("stream_recovery_exhausted = %v, want the window's own reason", exh)
	}
	trunc := logBuf.events(t, "stream_truncated")
	if len(trunc) != 1 || trunc[0]["recovery_reason"] != "max_elapsed" {
		t.Fatalf("stream_truncated = %v, want max_elapsed rather than a disconnect", trunc)
	}
}

// failAfterWriter is a client connection that breaks after n successful
// writes. httptest's recorder never fails, so without this the recovery
// loop's client-write path could not be exercised at the handler at all.
type failAfterWriter struct {
	*httptest.ResponseRecorder
	mu    sync.Mutex
	after int
	n     int
}

func (w *failAfterWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	w.n++
	over := w.n > w.after
	w.mu.Unlock()
	if over {
		return 0, errors.New("client connection broke")
	}
	return w.ResponseRecorder.Write(p)
}

func (w *failAfterWriter) Flush() { w.ResponseRecorder.Flush() }

// TestStreamRecoveryStopsOnAClientWriteFailure: the client's socket breaks
// while a hop is streaming. There is nobody left to continue for, so the loop
// makes no further request — the same judgement the caller gate makes, taken
// from the failure that actually happened rather than from the context.
func TestStreamRecoveryStopsOnAClientWriteFailure(t *testing.T) {
	store := newChainStore(t, recoveryBlock(t, "    enabled: true\n    max-recoveries: 2\n"))
	pa := &scriptedDoer{script: []dialFunc{
		sseStream(sseChat("Hello")),
		sseStream(sseChat("there")),
		sseStream(sseChat("never asked")),
	}}
	logBuf, log := captureLog(zerolog.DebugLevel)
	h := NewHandler(store, kindResolver{direct: pa, proxied: &scriptedDoer{}}, nil, nil, nil, log)

	// The walk relays one event (two writes). The hop's data line is write 3;
	// write 4 — the hop's event boundary — is where the client is gone.
	w := &failAfterWriter{ResponseRecorder: httptest.NewRecorder(), after: 3}
	h.ServeHTTP(w, buildRequest(t, http.MethodPost, "/v1/chat/completions", chatRequest, nil))

	if pa.dials() != 2 {
		t.Fatalf("dials = %d, want no continuation after the client write failed", pa.dials())
	}
	failed := logBuf.events(t, "stream_recovery_failed")
	if len(failed) != 1 || failed[0]["phase"] != "client_write" {
		t.Fatalf("stream_recovery_failed = %v, want one client_write phase", failed)
	}
	done := logBuf.events(t, "request_completed")
	if len(done) != 1 || done[0]["outcome"] != "client_disconnected" {
		t.Fatalf("request_completed = %v, want client_disconnected", done)
	}
}

// TestStreamRecoveryFinishReasonIsNotUnsafeContent is Task 2's contract at the
// handler: a stream whose upstream declared the answer FINISHED — a non-null
// Chat `finish_reason` — is never continued, and the record says the
// generation ENDED rather than that this proxy refused to read it.
//
// The distinction is the whole point. "The upstream finished without sending
// the marker" and "the proxy found something it must not continue" call for
// completely different operator responses, and reporting the first as
// `unsafe_content` sends them looking for a parsing problem that does not
// exist.
func TestStreamRecoveryFinishReasonIsNotUnsafeContent(t *testing.T) {
	for _, finish := range []string{"stop", "length", "content_filter"} {
		t.Run(finish, func(t *testing.T) {
			h, logBuf, pa, _ := recoveryHandler(t, recoveryBlock(t, "    enabled: true\n"))
			pa.script = []dialFunc{sseCut(sseChat("Hello") +
				`data: {"choices":[{"delta":{},"finish_reason":"` + finish + `"}]}` + "\n\n")}

			rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions", chatRequest, nil)
			if rec.Code != http.StatusOK {
				t.Fatalf("status = %d, want the committed 200", rec.Code)
			}
			if pa.dials() != 1 {
				t.Fatalf("dials = %d, want zero recovery dials after a declared finish", pa.dials())
			}
			if ev := logBuf.events(t, "stream_recovery_started"); len(ev) != 0 {
				t.Errorf("a finished stream was continued: %v", ev)
			}
			exh := logBuf.events(t, "stream_recovery_exhausted")
			if len(exh) != 1 {
				t.Fatalf("stream_recovery_exhausted = %v, want one", exh)
			}
			if exh[0]["reason"] != "logical_terminal" {
				t.Errorf("reason = %v, want logical_terminal", exh[0]["reason"])
			}
			if _, has := exh[0]["unsafe_reason"]; has {
				t.Errorf("a declared finish was reported as unsafe content: %v", exh[0])
			}
			trunc := logBuf.events(t, "stream_truncated")
			if len(trunc) != 1 || trunc[0]["recovery_reason"] != "logical_terminal" {
				t.Fatalf("stream_truncated = %v, want the logical_terminal reason", trunc)
			}
			// NO SYNTHESIZED MARKER. The client gets exactly the bytes the
			// upstream sent: a stream that never said [DONE] still never says it.
			if strings.Contains(rec.Body.String(), "[DONE]") {
				t.Errorf("the proxy invented a terminal marker: %s", rec.Body.String())
			}
			done := logBuf.events(t, "request_completed")
			if len(done) != 1 || done[0]["outcome"] != "stream_truncated" {
				t.Fatalf("request_completed = %v, want stream_truncated", done)
			}
		})
	}
}

// TestStreamRecoveryFinishReasonThenMarkerIsTheClientTerminal: the upstream
// declared the finish AND sent the marker. The client keys on the marker, so
// this stream is finished on its own terms — `stream_completed`, no refusal,
// no hop. The two facts are tracked separately and neither imitates the other.
func TestStreamRecoveryFinishReasonThenMarkerIsTheClientTerminal(t *testing.T) {
	h, logBuf, pa, _ := recoveryHandler(t, recoveryBlock(t, "    enabled: true\n"))
	pa.script = []dialFunc{sseStream(sseChat("Hello") +
		`data: {"choices":[{"delta":{},"finish_reason":"stop"}]}` + "\n\n" +
		"data: [DONE]\n\n")}

	rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions", chatRequest, nil)
	if n := strings.Count(rec.Body.String(), "data: [DONE]"); n != 1 {
		t.Fatalf("terminal markers = %d, want exactly the upstream's one", n)
	}
	if pa.dials() != 1 {
		t.Fatalf("dials = %d, want zero recovery dials", pa.dials())
	}
	if ev := logBuf.events(t, "stream_recovery_exhausted"); len(ev) != 0 {
		t.Errorf("a marked stream reported a recovery refusal: %v", ev)
	}
	if ev := logBuf.events(t, "stream_completed"); len(ev) != 1 {
		t.Errorf("stream_completed = %v, want one", ev)
	}
	done := logBuf.events(t, "request_completed")
	if len(done) != 1 || done[0]["outcome"] != "completed" {
		t.Fatalf("request_completed = %v, want completed", done)
	}
}

// TestStreamRecoveryFinishReasonWithADelayedEOF: the finish is read in one
// chunk and the EOF arrives in a later one — a provider that declares the
// finish and then takes its time closing. The latch is what makes the answer
// the same either way: nothing after a declared finish can un-finish it.
func TestStreamRecoveryFinishReasonWithADelayedEOF(t *testing.T) {
	h, logBuf, pa, _ := recoveryHandler(t, recoveryBlock(t, "    enabled: true\n"))
	pa.script = []dialFunc{func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
			Body: io.NopCloser(&stagedReader{chunks: []string{
				sseChat("Hello"),
				`data: {"choices":[{"delta":{},"finish_reason":"stop"}]}` + "\n\n",
			}}),
		}, nil
	}}

	doRequest(t, h, http.MethodPost, "/v1/chat/completions", chatRequest, nil)
	if pa.dials() != 1 {
		t.Fatalf("dials = %d, want zero recovery dials", pa.dials())
	}
	exh := logBuf.events(t, "stream_recovery_exhausted")
	if len(exh) != 1 || exh[0]["reason"] != "logical_terminal" {
		t.Fatalf("stream_recovery_exhausted = %v, want one logical_terminal refusal", exh)
	}
}

// stagedReader hands each chunk back from its own Read, so a test can control
// where the reads fall rather than pretending every read returns everything.
type stagedReader struct {
	chunks []string
	i      int
}

func (r *stagedReader) Read(p []byte) (int, error) {
	if r.i >= len(r.chunks) {
		return 0, io.EOF
	}
	n := copy(p, r.chunks[r.i])
	r.i++
	return n, nil
}

// TestStreamRecoveryRefusesResponsesMultiOutput is Task 3 at the handler: the
// MVP continues plain text from exactly one message output's exactly one
// content stream, and a Responses stream that does not match that topology
// fails closed rather than concatenating two answers into one assistant turn.
func TestStreamRecoveryRefusesResponsesMultiOutput(t *testing.T) {
	cases := []struct {
		name     string
		payloads string
		want     string
	}{
		{
			name:     "identity changes mid-stream",
			payloads: sseResponses("Once") + sseResponsesIdent("response.output_text.delta", "m2", 1, 0, " upon a time"),
			want:     "multiple_outputs",
		},
		{
			name:     "the content index changes",
			payloads: sseResponses("Once") + sseResponsesIdent("response.output_text.delta", "m1", 0, 1, " upon a time"),
			want:     "multiple_outputs",
		},
		{
			name:     "a second message item is announced",
			payloads: sseResponses("Once") + `event: response.output_item.added` + "\n" + `data: {"type":"response.output_item.added","output_index":1,"item":{"type":"message","id":"m2"}}` + "\n\n",
			want:     "multiple_outputs",
		},
		{
			name:     "an event this build does not know",
			payloads: sseResponses("Once") + sseResponsesUnknown("response.some_future_event"),
			want:     "unknown_shape",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h, logBuf, pa, _ := recoveryHandler(t, recoveryBlock(t, "    enabled: true\n"))
			pa.script = []dialFunc{sseCut(tc.payloads)}

			doRequest(t, h, http.MethodPost, "/v1/responses", `{"model":"chain-model","stream":true,"input":"hi"}`, nil)
			if pa.dials() != 1 {
				t.Fatalf("dials = %d, want no hop on a multi-output stream", pa.dials())
			}
			exh := logBuf.events(t, "stream_recovery_exhausted")
			if len(exh) != 1 {
				t.Fatalf("stream_recovery_exhausted = %v, want one", exh)
			}
			if exh[0]["reason"] != "unsafe_content" || exh[0]["unsafe_reason"] != tc.want {
				t.Fatalf("refusal = %v/%v, want unsafe_content/%v", exh[0]["reason"], exh[0]["unsafe_reason"], tc.want)
			}
		})
	}
}

// TestStreamRecoveryHopMatrix pins the two semantics Task 5 is about, across
// the whole space of first-hop outcomes:
//
//   - `max-recoveries` counts continuation REQUESTS INITIATED, never
//     continuations that succeeded. A hop that was dialed and then failed has
//     spent its slot even though it recovered nothing, and a stream that keeps
//     dying can never make more requests than the bound says.
//   - A hop that failed to produce a continuable stream ENDS the effort. That
//     is a deliberate policy and not a bound: the bound says how many requests
//     the stream MAY make, and nothing obliges it to make them all. A refused
//     dial or an error answer says something another immediate ask would not
//     change, and the client is holding an open, silent stream the whole time.
//     What DOES continue is the one failure that is evidence of progress — a
//     hop that streamed and truncated again — and only while the bounds allow.
func TestStreamRecoveryHopMatrix(t *testing.T) {
	okHop := sseStream(sseChat(", world") + "data: [DONE]\n\n")
	cutHop := sseStream(sseChat(", world"))
	nonSSE := func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body:       io.NopCloser(strings.NewReader(`{"choices":[]}`)),
		}, nil
	}
	status500 := func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusInternalServerError,
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body:       io.NopCloser(strings.NewReader(`{"error":{"message":"boom"}}`)),
		}, nil
	}
	dialFail := func(*http.Request) (*http.Response, error) {
		return nil, &net.OpError{Op: "dial", Err: errors.New("connection refused")}
	}

	cases := []struct {
		name string
		// reach is max-recoveries; hops[0] is the first continuation's answer
		// and hops[1] the second's.
		reach int
		hops  []dialFunc
		// wantDials counts the walk attempt plus every hop actually dialed.
		wantDials int
		// wantStarted counts hops INITIATED — the slot claims.
		wantStarted int
		// wantFailed counts `stream_recovery_failed` events: a hop that
		// truncated and was retried produced one, and so did the one that
		// ended the effort.
		wantFailed   int
		wantSucceed  int
		wantPhase    string
		wantExhaust  string
		wantRecovery float64
	}{
		// `max-recoveries: 0` is not a row here because the config layer
		// already refuses it: `enabled: true` with a zero reach states an
		// intent it could never act on (TestStreamValidateRejectsOutOfRange).
		// One is therefore the smallest reach a live policy can have, and it
		// is the row below.
		{
			name: "one hop succeeds", reach: 1, hops: []dialFunc{okHop},
			wantDials: 2, wantStarted: 1, wantSucceed: 1, wantRecovery: 1,
		},
		{
			name: "first hop refused at the dial", reach: 2, hops: []dialFunc{dialFail, okHop},
			wantDials: 2, wantStarted: 1, wantFailed: 1, wantPhase: "dial", wantRecovery: 1,
		},
		{
			name: "first hop answers a status", reach: 2, hops: []dialFunc{status500, okHop},
			wantDials: 2, wantStarted: 1, wantFailed: 1, wantPhase: "upstream_status", wantRecovery: 1,
		},
		{
			name: "first hop answers a non-stream 2xx", reach: 2, hops: []dialFunc{nonSSE, okHop},
			wantDials: 2, wantStarted: 1, wantFailed: 1, wantPhase: "upstream_status", wantRecovery: 1,
		},
		{
			name: "first hop truncates and the reach is one", reach: 1, hops: []dialFunc{cutHop},
			wantDials: 2, wantStarted: 1, wantFailed: 1, wantPhase: "upstream_read",
			wantExhaust: "max_recoveries", wantRecovery: 1,
		},
		{
			name: "two truncating hops spend the reach", reach: 2, hops: []dialFunc{cutHop, cutHop},
			wantDials: 3, wantStarted: 2, wantFailed: 2, wantPhase: "upstream_read",
			wantExhaust: "max_recoveries", wantRecovery: 2,
		},
		{
			name: "the second hop recovers", reach: 2, hops: []dialFunc{cutHop, okHop},
			wantDials: 3, wantStarted: 2, wantFailed: 1, wantSucceed: 1, wantPhase: "upstream_read", wantRecovery: 2,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h, logBuf, pa, _ := recoveryHandler(t, recoveryBlock(t,
				"    enabled: true\n    max-recoveries: "+strconv.Itoa(tc.reach)+"\n"))
			script := append([]dialFunc{sseStream(sseChat("Hello"))}, tc.hops...)
			pa.script = script

			doRequest(t, h, http.MethodPost, "/v1/chat/completions", chatRequest, nil)
			if pa.dials() != tc.wantDials {
				t.Fatalf("dials = %d, want %d", pa.dials(), tc.wantDials)
			}
			if n := len(logBuf.events(t, "stream_recovery_started")); n != tc.wantStarted {
				t.Errorf("stream_recovery_started = %d, want %d", n, tc.wantStarted)
			}
			if n := len(logBuf.events(t, "stream_recovery_succeeded")); n != tc.wantSucceed {
				t.Errorf("stream_recovery_succeeded = %d, want %d", n, tc.wantSucceed)
			}
			failed := logBuf.events(t, "stream_recovery_failed")
			if len(failed) != tc.wantFailed {
				t.Fatalf("stream_recovery_failed = %v, want %d", failed, tc.wantFailed)
			}
			if tc.wantPhase != "" && failed[len(failed)-1]["phase"] != tc.wantPhase {
				t.Errorf("last failure = %v, want phase %q", failed[len(failed)-1], tc.wantPhase)
			}
			exh := logBuf.events(t, "stream_recovery_exhausted")
			if tc.wantExhaust == "" {
				if len(exh) != 0 {
					t.Errorf("stream_recovery_exhausted = %v, want none", exh)
				}
			} else if len(exh) != 1 || exh[0]["reason"] != tc.wantExhaust {
				t.Errorf("stream_recovery_exhausted = %v, want reason %q", exh, tc.wantExhaust)
			}
			trunc := logBuf.events(t, "stream_truncated")
			if tc.wantSucceed == 1 {
				if len(trunc) != 0 {
					t.Errorf("stream_truncated = %v, want none after a successful recovery", trunc)
				}
				return
			}
			if len(trunc) != 1 {
				t.Fatalf("stream_truncated = %v, want one", trunc)
			}
			// The slot count is the REQUEST count, and it is reported whether
			// or not the hop that spent it produced anything.
			if trunc[0]["stream_recoveries"] != tc.wantRecovery {
				t.Errorf("stream_recoveries = %v, want %v", trunc[0]["stream_recoveries"], tc.wantRecovery)
			}
		})
	}
}

// TestStreamRecoveryHopFailureCarriesNoBoundReason: a hop that failed is a
// failure, not a bound the loop stopped at, so `recovery_reason` — the field
// that names which bound stopped the effort — stays absent. The hop's own
// `stream_recovery_failed` event is where the failure is reported, and the
// two must not be read as the same thing.
func TestStreamRecoveryHopFailureCarriesNoBoundReason(t *testing.T) {
	h, logBuf, pa, _ := recoveryHandler(t,
		recoveryBlock(t, "    enabled: true\n    max-recoveries: 2\n"))
	pa.script = []dialFunc{
		sseStream(sseChat("Hello")),
		func(*http.Request) (*http.Response, error) {
			return nil, errors.New("connection reset by peer")
		},
	}

	doRequest(t, h, http.MethodPost, "/v1/chat/completions", chatRequest, nil)
	if pa.dials() != 2 {
		t.Fatalf("dials = %d, want the walk attempt and the failed hop", pa.dials())
	}
	if ev := logBuf.events(t, "stream_recovery_exhausted"); len(ev) != 0 {
		t.Errorf("a failed hop was reported as a bound: %v", ev)
	}
	trunc := logBuf.events(t, "stream_truncated")
	if len(trunc) != 1 {
		t.Fatalf("stream_truncated = %v, want one", trunc)
	}
	if _, has := trunc[0]["recovery_reason"]; has {
		t.Errorf("a failed hop reported a recovery_reason: %v", trunc[0])
	}
	if trunc[0]["stream_recoveries"] != float64(1) {
		t.Errorf("stream_recoveries = %v, want the failed hop counted as a request", trunc[0]["stream_recoveries"])
	}
}

// TestStreamRecoveryWindowOwnsAHopCutByIt is the misattribution regression.
// A continuation hop that the window's own watchdog cuts produces a read
// error — this proxy's closed body — and the naive classification calls that
// `upstream_read`, then reports the SAME cause a second time as
// `stream_recovery_exhausted reason=max_elapsed` when the loop re-checks its
// gates. Two records, one cause, and the one an operator reads first blames a
// peer that behaved perfectly.
//
// The invariant: exactly one owner. A read that the watchdog ended is owned by
// `max_elapsed` and nothing else, and the bound is reported once.
func TestStreamRecoveryWindowOwnsAHopCutByIt(t *testing.T) {
	h, logBuf, pa, _ := recoveryHandler(t,
		recoveryBlock(t, "    enabled: true\n    max-elapsed: 250ms\n    max-recoveries: 2\n"))
	// The committed stream relays one event and ends cleanly under the bound;
	// the HOP parks, so the window that fires is cutting a hop's read, not the
	// first relay's.
	body, dial := sseParked(context.Background(), sseChat(", world"))
	pa.script = []dialFunc{sseStream(sseChat("Hello")), dial}

	rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions", chatRequest, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want the already-committed 200", rec.Code)
	}
	if !body.released.Load() {
		t.Fatal("the hop's parked read never returned: the window did not cover the hop")
	}
	if pa.dials() != 2 {
		t.Fatalf("dials = %d, want the walk attempt and the one hop", pa.dials())
	}

	// The hop DID happen and DID fail — it is a real request that produced no
	// terminal marker, so it must still be recorded as a hop failure.
	failed := logBuf.events(t, "stream_recovery_failed")
	if len(failed) != 1 {
		t.Fatalf("stream_recovery_failed = %v, want one for the cut hop", failed)
	}
	// ... and its owner is the bound, NOT the peer.
	if failed[0]["phase"] != "max_elapsed" {
		t.Errorf("hop failure phase = %v, want max_elapsed (a read the watchdog ended is this proxy's, not the upstream's)", failed[0]["phase"])
	}
	// The bound is reported exactly once, by the loop, and not duplicated by a
	// second event claiming to be an upstream fault.
	exh := logBuf.events(t, "stream_recovery_exhausted")
	if len(exh) != 1 || exh[0]["reason"] != "max_elapsed" {
		t.Fatalf("stream_recovery_exhausted = %v, want exactly one max_elapsed", exh)
	}
	// The cut is not a success, however its partial bytes landed.
	if ev := logBuf.events(t, "stream_recovery_succeeded"); len(ev) != 0 {
		t.Errorf("a hop the window cut was reported as a success: %v", ev)
	}
	// The truncation carries the bound, and no invented upstream error: the
	// read failed because this proxy closed the body.
	trunc := logBuf.events(t, "stream_truncated")
	if len(trunc) != 1 {
		t.Fatalf("stream_truncated = %v, want one", trunc)
	}
	if trunc[0]["recovery_reason"] != "max_elapsed" {
		t.Errorf("truncation reason = %v, want max_elapsed", trunc[0]["recovery_reason"])
	}
	if _, hasErr := trunc[0]["error"]; hasErr {
		t.Errorf("the bound reported an upstream error it invented: %v", trunc[0])
	}
	if trunc[0]["stream_recoveries"] != float64(1) {
		t.Errorf("stream_recoveries = %v, want the cut hop counted as a request", trunc[0]["stream_recoveries"])
	}
}

// partialLineBody delivers a PARTIAL `data:` line — no newline — and then
// parks. It exists because the usual parked-body fixture ends its event with
// a blank line, which leaves the relay parked on a ReadSlice for the NEXT
// line; closing a body at that boundary makes `bufio` return io.EOF, which
// CopySSE maps to a nil error. A relay parked mid-line does not get that
// luck: the close surfaces as a real read error, so this is the only shape
// that can tell a bound-owned truncation from an upstream-owned one.
type partialLineBody struct {
	data     []byte
	closed   chan struct{}
	once     sync.Once
	released atomic.Bool
}

func (b *partialLineBody) Read(p []byte) (int, error) {
	if b.data != nil {
		d := b.data
		b.data = nil
		return copy(p, d), nil
	}
	select {
	case <-b.closed:
		b.released.Store(true)
		return 0, errors.New("http: read on closed response body")
	case <-time.After(parkedSafety):
		b.released.Store(true)
		return 0, errors.New("parked read was never released")
	}
}

func (b *partialLineBody) Close() error {
	b.once.Do(func() { close(b.closed) })
	return nil
}

// TestStreamRecoveryWindowOwnsACommittedRelayCutIt is the committed path's
// half of the same ownership invariant the hop path pins.
//
// A watchdog that closes the body mid-line produces a genuine read error —
// this proxy's own close, not the peer's. The record must still say the
// bound: `recovery_reason=max_elapsed`, `phase=recovery`, and NO `error`
// field, because there was no upstream failure to report and inventing one
// would send an operator after a peer that behaved perfectly. It is also
// exactly the case the line-boundary fixtures cannot reach, which is why the
// transport-level misattribution it rules out is otherwise untested.
func TestStreamRecoveryWindowOwnsACommittedRelayCutIt(t *testing.T) {
	h, logBuf, pa, _ := recoveryHandler(t,
		recoveryBlock(t, "    enabled: true\n    max-elapsed: 150ms\n    max-recoveries: 2\n"))
	body := &partialLineBody{
		data:   []byte(`data: {"choices":[{"delta":{"content":"hel`),
		closed: make(chan struct{}),
	}
	pa.script = []dialFunc{func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
			Body:       body,
		}, nil
	}}

	rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions", chatRequest, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want the already-committed 200", rec.Code)
	}
	if !body.released.Load() {
		t.Fatal("the relay's read never returned: the window closed no upstream body")
	}
	if pa.dials() != 1 {
		t.Fatalf("dials = %d, want no hop past the window", pa.dials())
	}
	// The bound is the story, and it is told once.
	exh := logBuf.events(t, "stream_recovery_exhausted")
	if len(exh) != 1 || exh[0]["reason"] != "max_elapsed" {
		t.Fatalf("stream_recovery_exhausted = %v, want exactly one max_elapsed", exh)
	}
	trunc := logBuf.events(t, "stream_truncated")
	if len(trunc) != 1 {
		t.Fatalf("stream_truncated = %v, want one", trunc)
	}
	if trunc[0]["recovery_reason"] != "max_elapsed" {
		t.Errorf("truncation reason = %v, want max_elapsed", trunc[0]["recovery_reason"])
	}
	if trunc[0]["phase"] != "recovery" {
		t.Errorf("truncation phase = %v, want recovery (the bound, not a read fault)", trunc[0]["phase"])
	}
	// This proxy's own closed body must never be relayed as a peer failure.
	if got, has := trunc[0]["error"]; has {
		t.Errorf("the bound reported an upstream error it invented: %v", got)
	}
	if ev := logBuf.events(t, "stream_recovery_failed"); len(ev) != 0 {
		t.Errorf("a bound-owned cut was reported as a hop failure: %v", ev)
	}
}

// TestStreamRecoveryHop429MarksTheCredential closes the gap that made the
// credential seam dead on the continuation path: every other recovery test
// wires `NewHandler(..., nil, ...)`, so `hop.pool` is always nil and a
// continuation has never once been observed against a real rotation pool.
//
// The defect it pins: a rate limit is an ACCOUNT fact, so the account a hop
// went out with must leave rotation however the recovery ends. `MarkRateLimited`
// used to be called on the walk's answer path only, which made the mark
// walk-only — a provider answering the COMMITTING stream with 429 (rather
// than cutting it) left its key in rotation, and the next request acquired the
// same key in cursor order. The continuation would then be the thing that
// silently defeats the rotation the pool exists to provide.
func TestStreamRecoveryHop429MarksTheCredential(t *testing.T) {
	store := newCredChainRecoveryStore(t)
	creds := credential.NewRegistry()
	pool := credPool(t, store, creds)

	pa := &scriptedDoer{}
	logBuf, log := captureLog(zerolog.DebugLevel)
	h := NewHandler(store, kindResolver{direct: pa, proxied: pa}, creds, nil, nil, log)

	// The committed stream is cut, so a hop is made — and the hop is answered
	// with 429, the shape that only reaches the status branch on a hop.
	pa.script = []dialFunc{
		sseCut(sseChat("Hello")),
		func(*http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusTooManyRequests,
				Header:     http.Header{"Content-Type": []string{"application/json"}},
				Body:       io.NopCloser(&errBody{data: []byte(`{"error":"rate limited"}`)}),
			}, nil
		},
	}
	doRequest(t, h, http.MethodPost, "/v1/chat/completions", chatRequest, nil)
	if pa.dials() != 2 {
		t.Fatalf("dials = %d, want the walk attempt and the hop", pa.dials())
	}

	// The hop went out under a key. That key must now be cooling, so the NEXT
	// request cannot acquire it.
	var used string
	for _, ev := range logBuf.events(t, "stream_recovery_failed") {
		if ev["phase"] == "upstream_status" {
			used, _ = ev["upstream_credential_id"].(string)
		}
	}
	if used == "" {
		t.Fatal("the 429 hop named no credential id; cannot tell which key to check")
	}
	// The handler marks against the REAL clock (no frozen test clock is wired
	// here), so the check reads the real one: asking at the frozen instant
	// would predate the deadline and report the key as ready.
	after := time.Now()
	if until := pool.CoolingUntil(after, used); until.IsZero() {
		t.Errorf("credential %q was NOT marked rate-limited by the hop's 429", used)
	}
	// And the state is real: an Acquire that PREFERS the key the hop used must
	// skip it. The pool still has a healthy key, so this is a rotation, not a
	// refusal — what matters is that the rate-limited one is not handed out.
	next, ok := pool.Acquire(after, used)
	if !ok {
		t.Fatalf("no credential was acquirable at all: %q", used)
	}
	if next.ID == used {
		t.Errorf("credential %q was still acquirable after a 429", used)
	}
}

// TestStreamRecoveryACRFramedTerminalMarkerIsTheTerminal pins the failure
// mode a lone CR produced: the SSE grammar admits CR, LF and CRLF, but only
// two of the three were recognized. A provider whose final line was
// `data: [DONE]\r` had its terminal marker read as payload — so the relay
// reported the stream TRUNCATED, and stream recovery dialed a continuation
// for an answer the model had already finished.
//
// That is a duplicate upstream request against a provider that behaved
// correctly, on a stream whose contract says a terminal marker ends it.
func TestStreamRecoveryACRFramedTerminalMarkerIsTheTerminal(t *testing.T) {
	h, logBuf, pa, _ := recoveryHandler(t,
		recoveryBlock(t, "    enabled: true\n    max-recoveries: 2\n"))
	// LF throughout, and the terminal marker framed with a lone CR — the
	// dialect the relay must not mistake for a missing marker.
	pa.script = []dialFunc{sseStream(sseChat("Hello") + "data: [DONE]\r\r")}

	rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions", chatRequest, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want the committed 200", rec.Code)
	}
	if pa.dials() != 1 {
		t.Fatalf("dials = %d, want exactly 1: a CR-framed terminal marker must not be continued", pa.dials())
	}
	for _, slug := range []string{"stream_recovery_started", "stream_recovery_failed", "stream_recovery_succeeded", "stream_recovery_exhausted"} {
		if ev := logBuf.events(t, slug); len(ev) != 0 {
			t.Errorf("%s fired for a stream that reached its terminal marker: %v", slug, ev)
		}
	}
	trunc := logBuf.events(t, "stream_truncated")
	if len(trunc) != 0 {
		t.Errorf("a terminated stream was reported truncated: %v", trunc)
	}
	// stream_completed is a DEBUG event with no outcome field; the request
	// outcome is on request_completed. Existence + the truncation check above
	// are the contract.
	done := logBuf.events(t, "stream_completed")
	if len(done) != 1 {
		t.Errorf("stream_completed = %v, want one", done)
	}
	rc := logBuf.events(t, "request_completed")
	if len(rc) != 1 || rc[0]["outcome"] != "completed" {
		t.Errorf("request_completed = %v, want one completed", rc)
	}
}
