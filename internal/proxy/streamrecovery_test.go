package proxy

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"

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

// sseResponses renders one Responses output-text delta.
func sseResponses(delta string) string {
	return `event: response.output_text.delta` + "\n" +
		`data: {"type":"response.output_text.delta","delta":"` + delta + `"}` + "\n\n"
}

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
func TestStreamRecoveryContinuesAResponsesStream(t *testing.T) {
	h, logBuf, pa, pb := recoveryHandler(t, recoveryBlock(t, "    enabled: true\n"))
	pa.script = []dialFunc{
		sseCut(sseResponses("Once")),
		sseStream(sseResponses(" upon a time") + "event: response.completed\ndata: {}\n\n"),
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
