package e2e_test

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"
)

// commonBody is a chat request against the reload scenarios' model name
// ("common"), carrying the same unknown fields as chatBody.
const commonBody = `{"model":"common","messages":[{"role":"user","content":"hello"}],"temperature":0.5,"extra":"x"}`

// ssePayload is the subset of an SSE chat data payload we assert on.
type ssePayload struct {
	Model   string `json:"model"`
	Choices []struct {
		Delta struct {
			Content string `json:"content"`
		} `json:"delta"`
	} `json:"choices"`
}

// assertChatSSE checks that an SSE event's data line carries the rewritten
// public model and the expected delta content.
func assertChatSSE(t *testing.T, lines []string, wantModel, wantContent string) {
	t.Helper()
	found := false
	for _, ln := range lines {
		if !strings.HasPrefix(ln, "data:") {
			continue
		}
		payload := sseDataContent(t, ln)
		var p ssePayload
		if err := json.Unmarshal([]byte(payload), &p); err != nil {
			t.Fatalf("SSE data payload %q: %v", payload, err)
		}
		if p.Model != wantModel {
			t.Fatalf("SSE model %q, want %q (line %q)", p.Model, wantModel, ln)
		}
		if len(p.Choices) == 0 || p.Choices[0].Delta.Content != wantContent {
			t.Fatalf("SSE content %+v, want %q (line %q)", p.Choices, wantContent, ln)
		}
		found = true
	}
	if !found {
		t.Fatalf("no data line in event %q", lines)
	}
}

// chatStreamRequest is a chat completion request with stream: true.
func chatStreamRequest(model string) string {
	return fmt.Sprintf(`{"model":%q,"messages":[{"role":"user","content":"hello"}],"stream":true}`, model)
}

// Scenario 14: stream=true. Upstream emits 3 data chunks (model in each) plus
// [DONE], gating chunks 2-3 behind a channel. The client must read chunk 1
// while the upstream is still blocked (incremental delivery), and all model
// fields must be rewritten to the public name.
func TestChatStreamIncremental(t *testing.T) {
	up := newFakeUpstream(t)
	gate := make(chan struct{})
	up.setHandler(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		fl := w.(http.Flusher)
		chunks := []string{"chunk1", "chunk2", "chunk3"}
		for i, c := range chunks {
			_, _ = fmt.Fprintf(w, "data: {\"id\":\"s1\",\"model\":\"upstream-chat\",\"choices\":[{\"index\":%d,\"delta\":{\"content\":%q},\"finish_reason\":null}]}\n\n", i, c)
			fl.Flush()
			if i == 0 {
				<-gate // hold chunks 2-3 until the client has read chunk 1
			}
		}
		_, _ = io.WriteString(w, "data: [DONE]\n\n")
		fl.Flush()
	})
	p := startSubprocess(t, startOpts{
		yaml: runtimeYAML(chatPublic, up.url()+"/v1", chatUpstream, ""),
	})

	resp := openJSON(t, p.addr, "/v1/chat/completions", chatStreamRequest(chatPublic),
		map[string]string{"Accept": "text/event-stream"})
	defer func() { _ = resp.Body.Close() }()
	br := bufio.NewReader(resp.Body)

	// Chunk 1 must arrive while the upstream is still blocked on gate (we
	// only close gate after reading it), proving incremental delivery.
	lines, eof := nextSSEEvent(t, br, 3*time.Second)

	if eof {
		t.Fatal("stream ended before chunk1 arrived")
	}

	assertChatSSE(t, lines, chatPublic, "chunk1")

	close(gate)
	for _, want := range []string{"chunk2", "chunk3"} {
		lines, eof = nextSSEEvent(t, br, 3*time.Second)
		if eof {
			t.Fatalf("stream ended before %s arrived", want)
		}
		assertChatSSE(t, lines, chatPublic, want)
	}
	lines, eof = nextSSEEvent(t, br, 3*time.Second)
	if eof || !strings.Contains(strings.Join(lines, "\n"), "[DONE]") {
		t.Fatalf("missing [DONE] terminator (eof=%v lines=%q)", eof, lines)
	}
}

// Scenario 15: stream=true but the upstream ignores it and returns a single
// JSON body: the client receives one JSON response with the model rewritten
// and no "data:" framing.
func TestChatStreamIgnoredByUpstream(t *testing.T) {
	up := newFakeUpstream(t)
	up.setHandler(jsonChatHandler(chatUpstream))
	p := startSubprocess(t, startOpts{
		yaml: runtimeYAML(chatPublic, up.url()+"/v1", chatUpstream, ""),
	})

	status, _, body := postJSON(t, p.addr, "/v1/chat/completions", chatStreamRequest(chatPublic),
		map[string]string{"Accept": "text/event-stream"})
	if status != http.StatusOK {
		t.Fatalf("status %d, want 200", status)
	}
	if strings.HasPrefix(string(body), "data:") {
		t.Fatalf("expected a plain JSON body, got SSE framing: %q", body)
	}
	m := decodeMap(t, body)
	if m["model"] != chatPublic {
		t.Fatalf("single JSON body model %v, want rewritten to %s", m["model"], chatPublic)
	}
}

// Scenario 16: a stream data line without a model field is forwarded
// byte-identically (whitespace preserved); [DONE] passes through.
func TestChatStreamNoModelByteIdentical(t *testing.T) {
	const sentLine = "data:   {\"id\":\"c1\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hi\"},\"finish_reason\":null}]}"
	up := newFakeUpstream(t)
	up.setHandler(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		fl := w.(http.Flusher)
		_, _ = fmt.Fprintf(w, "%s\n\n", sentLine)
		fl.Flush()
		_, _ = io.WriteString(w, "data: [DONE]\n\n")
		fl.Flush()
	})
	p := startSubprocess(t, startOpts{
		yaml: runtimeYAML(chatPublic, up.url()+"/v1", chatUpstream, ""),
	})

	resp := openJSON(t, p.addr, "/v1/chat/completions", chatStreamRequest(chatPublic),
		map[string]string{"Accept": "text/event-stream"})
	defer func() { _ = resp.Body.Close() }()
	br := bufio.NewReader(resp.Body)

	lines, eof := nextSSEEvent(t, br, 3*time.Second)
	if eof {
		t.Fatal("stream ended before data line")
	}
	if len(lines) != 1 || lines[0] != sentLine {
		t.Fatalf("no-model line not forwarded byte-identically:\n got %q\nwant %q", lines, sentLine)
	}
	lines, eof = nextSSEEvent(t, br, 3*time.Second)
	if eof || !strings.Contains(strings.Join(lines, "\n"), "[DONE]") {
		t.Fatalf("missing [DONE] (eof=%v lines=%q)", eof, lines)
	}
}

// Scenario 17: hot reload. Rewriting the config file A -> B routes new
// requests to B within the poll window; at most the single in-flight request
// straddling the swap may still hit A.
func TestHotReloadSwitchesUpstream(t *testing.T) {
	upA := newFakeUpstream(t)
	upA.setHandler(jsonChatHandler("upA"))
	upB := newFakeUpstream(t)
	upB.setHandler(jsonChatHandler("upB"))
	yamlA := runtimeYAML("common", upA.url()+"/v1", "upA", "")
	yamlB := runtimeYAML("common", upB.url()+"/v1", "upB", "")
	p := startSubprocess(t, startOpts{yaml: yamlA})

	// Warm request: must go to A.
	status, _, _ := postJSON(t, p.addr, "/v1/chat/completions", commonBody, nil)
	if status != http.StatusOK {
		t.Fatalf("warm request status %d, want 200", status)
	}
	if before := upA.count(); before != 1 {
		t.Fatalf("warm request did not reach A (count %d)", before)
	}

	rewriteConfig(t, p.cfgPath, yamlB)
	if err := waitForUpstream(t, p, upB, 10*time.Second); err != nil {
		t.Fatal(err)
	}

	// A request made after the swap must hit B, and A must not see any more
	// requests beyond its pre-swap count plus at most one straddler.
	if _, _, _, err := postJSONRaw(p.addr, "/v1/chat/completions", commonBody, nil); err != nil {
		t.Fatalf("post-swap request failed: %v", err)
	}
	if afterA := upA.count(); afterA > 2 {
		t.Fatalf("requests hit A after swap (before=1 after=%d), want at most the one straddler", afterA)
	}
}

// Scenario 18: an invalid reload keeps the last-known-good snapshot: after the
// config file is rewritten to something invalid, requests are still served by
// the old routing and the poller logs the keep message.
func TestInvalidReloadKeepsLastKnownGood(t *testing.T) {
	upA := newFakeUpstream(t)
	upA.setHandler(jsonChatHandler("upA"))
	yamlA := runtimeYAML("common", upA.url()+"/v1", "upA", "")
	p := startSubprocess(t, startOpts{yaml: yamlA})

	status, _, _ := postJSON(t, p.addr, "/v1/chat/completions", commonBody, nil)
	if status != http.StatusOK {
		t.Fatalf("warm request status %d, want 200", status)
	}
	before := upA.count()

	// Plane-violating top-level key: deterministically rejected by LoadRuntime.
	rewriteConfig(t, p.cfgPath, "listen: :9999\n"+yamlA)
	time.Sleep(300 * time.Millisecond) // >= 2 poll cycles at 50ms

	status, _, body := postJSON(t, p.addr, "/v1/chat/completions", commonBody, nil)
	if status != http.StatusOK {
		t.Fatalf("request after invalid reload status %d, want 200 (body %s)", status, body)
	}
	if after := upA.count(); after <= before {
		t.Fatalf("invalid reload lost routing: A count before=%d after=%d", before, after)
	}
}

// Scenario 19: a request started before a reload is bound to the old
// snapshot. Upstream A blocks on a channel; reload to B happens mid-request;
// when A releases, the response reflects A (its marker id, model rewritten to
// the public name) and B never saw the request.
func TestInFlightRequestUsesOldSnapshot(t *testing.T) {
	upA := newFakeUpstream(t)
	upB := newFakeUpstream(t)
	received := make(chan struct{}, 1)
	release := make(chan struct{})
	var once sync.Once
	upA.setHandler(func(w http.ResponseWriter, r *http.Request) {
		once.Do(func() { received <- struct{}{} })
		<-release
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, `{"id":"A-MARKER","model":"upA","choices":[]}`)
	})
	upB.setHandler(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, `{"id":"B-MARKER","model":"upB","choices":[]}`)
	})
	yamlA := runtimeYAML("common", upA.url()+"/v1", "upA", "P1")
	yamlB := runtimeYAML("common", upB.url()+"/v1", "upB", "P2")
	p := startSubprocess(t, startOpts{yaml: yamlA})

	resCh := make(chan result, 1)
	go func() {
		status, _, body, err := postJSONRaw(p.addr, "/v1/chat/completions", commonBody, nil)
		resCh <- result{status: status, body: body, err: err}
	}()
	<-received // A is holding the request

	rewriteConfig(t, p.cfgPath, yamlB)
	time.Sleep(600 * time.Millisecond) // several poll cycles: reload lands at B
	if n := upB.count(); n != 0 {
		t.Fatalf("B saw %d requests before release, want 0 (request predates reload)", n)
	}

	close(release)
	select {
	case res := <-resCh:
		if res.err != nil {
			t.Fatalf("in-flight request failed: %v", res.err)
		}
		if res.status != http.StatusOK {
			t.Fatalf("in-flight request status %d, want 200", res.status)
		}
		m := decodeMap(t, res.body)
		if m["id"] != "A-MARKER" {
			t.Fatalf("response id %v, want A-MARKER (old snapshot)", m["id"])
		}
		if m["model"] != "common" {
			t.Fatalf("response model %v, want rewritten to public name %q", m["model"], "common")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("in-flight request never completed after release")
	}
	if n := upB.count(); n != 0 {
		t.Fatalf("B saw %d requests after completion, want 0", n)
	}
}

// Scenario 20: an active stream survives a reload. Chunk 1 (model A) is read;
// the config reloads to B mid-stream; the remaining chunks still carry A's
// public model name and B never sees the request.
func TestActiveStreamSurvivesReload(t *testing.T) {
	upA := newFakeUpstream(t)
	upB := newFakeUpstream(t)
	chunk1Sent := make(chan struct{})
	gate := make(chan struct{})
	var once sync.Once
	upA.setHandler(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		fl := w.(http.Flusher)
		_, _ = fmt.Fprintf(w, "data: {\"id\":\"s1\",\"model\":\"upA\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"one\"},\"finish_reason\":null}]}\n\n")
		fl.Flush()
		once.Do(func() { close(chunk1Sent) })
		<-gate
		_, _ = fmt.Fprintf(w, "data: {\"id\":\"s1\",\"model\":\"upA\",\"choices\":[{\"index\":1,\"delta\":{\"content\":\"two\"},\"finish_reason\":null}]}\n\n")
		fl.Flush()
		_, _ = io.WriteString(w, "data: [DONE]\n\n")
		fl.Flush()
	})
	upB.setHandler(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, `{"id":"b","model":"upB","choices":[]}`)
	})
	yamlA := runtimeYAML("common", upA.url()+"/v1", "upA", "")
	yamlB := runtimeYAML("common", upB.url()+"/v1", "upB", "")
	p := startSubprocess(t, startOpts{yaml: yamlA})

	resp := openJSON(t, p.addr, "/v1/chat/completions", chatStreamRequest("common"),
		map[string]string{"Accept": "text/event-stream"})
	defer func() { _ = resp.Body.Close() }()
	br := bufio.NewReader(resp.Body)

	<-chunk1Sent // upstream has flushed chunk 1
	lines, eof := nextSSEEvent(t, br, 3*time.Second)
	if eof {
		t.Fatal("stream ended before chunk1")
	}
	assertChatSSE(t, lines, "common", "one")

	// Reload mid-stream; the stream must stay bound to A's snapshot.
	rewriteConfig(t, p.cfgPath, yamlB)
	time.Sleep(600 * time.Millisecond) // several poll cycles: reload lands at B
	if n := upB.count(); n != 0 {
		t.Fatalf("B saw %d requests, want 0 (stream bound to pre-reload snapshot)", n)
	}

	close(gate)
	lines, eof = nextSSEEvent(t, br, 3*time.Second)
	if eof {
		t.Fatal("stream ended before chunk2")
	}
	assertChatSSE(t, lines, "common", "two") // still A's public model
	lines, eof = nextSSEEvent(t, br, 3*time.Second)
	if eof || !strings.Contains(strings.Join(lines, "\n"), "[DONE]") {
		t.Fatalf("missing [DONE] (eof=%v lines=%q)", eof, lines)
	}
	if n := upB.count(); n != 0 {
		t.Fatalf("B saw %d requests, want 0", n)
	}
}

// Scenario 23: two reloads with distinct content route to the configured
// upstream each time — A, then B, then A again with a new prompt. (Internal
// bookkeeping like generation numbers is not asserted from logs: observable
// routing is the behavior under test.)
func TestReloadFollowsChangedContent(t *testing.T) {
	upA := newFakeUpstream(t)
	upA.setHandler(jsonChatHandler("upA"))
	upB := newFakeUpstream(t)
	upB.setHandler(jsonChatHandler("upB"))
	cfgA := runtimeYAML("common", upA.url()+"/v1", "upA", "P1")
	cfgB := runtimeYAML("common", upB.url()+"/v1", "upB", "P2")
	cfgC := runtimeYAML("common", upA.url()+"/v1", "upA", "P3")
	p := startSubprocess(t, startOpts{yaml: cfgA})

	// Reload 1 -> B: routing must follow the new content.
	rewriteConfig(t, p.cfgPath, cfgB)
	if err := waitForUpstream(t, p, upB, 10*time.Second); err != nil {
		t.Fatal(err)
	}
	// Reload 2 -> A (new prompt): routing follows again.
	rewriteConfig(t, p.cfgPath, cfgC)
	if err := waitForUpstream(t, p, upA, 10*time.Second); err != nil {
		t.Fatal(err)
	}
}

// Scenario 26: a malformed SSE data line (unparseable JSON that nevertheless
// contains "model") is forwarded verbatim — never rewritten, never dropped.
func TestMalformedSSEForwardedVerbatum(t *testing.T) {
	const sentLine = `data: {"id":"x","model":broken-broken,"choices":[]}`
	up := newFakeUpstream(t)
	up.setHandler(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		fl := w.(http.Flusher)
		_, _ = fmt.Fprintf(w, "%s\n\n", sentLine)
		fl.Flush()
		_, _ = io.WriteString(w, "data: [DONE]\n\n")
		fl.Flush()
	})
	p := startSubprocess(t, startOpts{
		yaml: runtimeYAML(chatPublic, up.url()+"/v1", chatUpstream, ""),
	})

	resp := openJSON(t, p.addr, "/v1/chat/completions", chatStreamRequest(chatPublic),
		map[string]string{"Accept": "text/event-stream"})
	defer func() { _ = resp.Body.Close() }()
	br := bufio.NewReader(resp.Body)

	lines, eof := nextSSEEvent(t, br, 3*time.Second)
	if eof {
		t.Fatal("stream ended before malformed line")
	}
	if len(lines) != 1 || lines[0] != sentLine {
		t.Fatalf("malformed line not forwarded verbatim:\n got %q\nwant %q", lines, sentLine)
	}
	lines, eof = nextSSEEvent(t, br, 3*time.Second)
	if eof || !strings.Contains(strings.Join(lines, "\n"), "[DONE]") {
		t.Fatalf("missing [DONE] (eof=%v lines=%q)", eof, lines)
	}
}

// Scenario 28: responses stream with event:+data: envelopes carrying
// response.model; no [DONE]. Every envelope model is rewritten to the public
// name and the stream completes when the upstream closes.
func TestResponsesStreamEnvelopeRewrite(t *testing.T) {
	up := newFakeUpstream(t)
	events := []string{
		"event: response.created\ndata: {\"type\":\"response.created\",\"response\":{\"id\":\"r1\",\"model\":\"upstream-resp\"}}",
		"event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"hi\"}",
		"event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"r1\",\"model\":\"upstream-resp\"}}",
	}
	up.setHandler(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		fl := w.(http.Flusher)
		for _, e := range events {
			_, _ = fmt.Fprintf(w, "%s\n\n", e)
			fl.Flush()
		}
		// Handler returns: upstream closes the stream, no [DONE].
	})
	p := startSubprocess(t, startOpts{
		yaml: runtimeYAML(respPublic, up.url()+"/v1", respUpstream, ""),
	})

	resp := openJSON(t, p.addr, "/v1/responses",
		`{"model":"resp-public","input":"hello","stream":true}`,
		map[string]string{"Accept": "text/event-stream"})
	defer func() { _ = resp.Body.Close() }()
	br := bufio.NewReader(resp.Body)

	type envelope struct {
		Type     string `json:"type"`
		Response *struct {
			Model string `json:"model"`
		} `json:"response"`
	}
	rewritten := 0
	sawDelta := false
	for {
		lines, eof := nextSSEEvent(t, br, 3*time.Second)
		for _, ln := range lines {
			if !strings.HasPrefix(ln, "data:") {
				continue
			}
			var ev envelope
			if err := json.Unmarshal([]byte(sseDataContent(t, ln)), &ev); err != nil {
				t.Fatalf("unmarshal responses envelope %q: %v", ln, err)
			}
			if ev.Response != nil {
				if ev.Response.Model != respPublic {
					t.Fatalf("envelope %q model %q, want rewritten to %s", ev.Type, ev.Response.Model, respPublic)
				}
				rewritten++
			}
			if ev.Type == "response.output_text.delta" {
				sawDelta = true
			}
		}
		if eof {
			break // upstream closed the stream
		}
	}
	if rewritten < 2 {
		t.Fatalf("expected >=2 rewritten envelopes, got %d", rewritten)
	}
	if !sawDelta {
		t.Fatal("never saw the delta event")
	}
}

// TestPromptHotSwapsOnWire pins the reload half of the injection contract on
// the wire: scenario 2 proves a configured prompt reaches the upstream at
// boot, but nothing proved a CHANGED prompt takes over — the load-bearing
// direction is a prompt that stops being the old one after a reload.
func TestPromptHotSwapsOnWire(t *testing.T) {
	up := newFakeUpstream(t)
	up.setHandler(jsonChatHandler(chatUpstream))
	yamlP1 := runtimeYAML(chatPublic, up.url()+"/v1", chatUpstream, "P1")
	yamlP2 := runtimeYAML(chatPublic, up.url()+"/v1", chatUpstream, "P2")
	p := startSubprocess(t, startOpts{yaml: yamlP1})

	if status, _, _ := postJSON(t, p.addr, "/v1/chat/completions", chatBody, nil); status != http.StatusOK {
		t.Fatalf("first request status %d, want 200", status)
	}
	first := decodeMap(t, up.requests()[0].Body)
	msgs := first["messages"].([]any)
	sys := msgs[0].(map[string]any)
	if sys["content"] != "P1" {
		t.Fatalf("boot prompt = %v, want P1", sys["content"])
	}

	rewriteConfig(t, p.cfgPath, yamlP2)
	// Await the reload ack: it is the one reload event emitted AT the live
	// level, visible even at the harness default level (error), where the
	// INFO config_reloaded line never shows.
	waitForEventCount(t, p, "log_level_applied", 1)

	if status, _, _ := postJSON(t, p.addr, "/v1/chat/completions", chatBody, nil); status != http.StatusOK {
		t.Fatalf("second request status %d, want 200", status)
	}
	reqs := up.requests()
	if len(reqs) != 2 {
		t.Fatalf("upstream got %d requests, want 2", len(reqs))
	}
	second := decodeMap(t, reqs[1].Body)
	msgs = second["messages"].([]any)
	sys = msgs[0].(map[string]any)
	if sys["content"] != "P2" {
		t.Fatalf("post-reload prompt = %v, want P2 (reload did not take over on the wire)", sys["content"])
	}
}

// TestActiveStreamPromptBoundToOldSnapshot extends scenario 20 with prompt
// binding: a stream in flight against a prompt-carrying snapshot survives a
// reload that changes BOTH the prompt and the upstream. The request the old
// upstream is still holding was transformed with the OLD prompt; the new
// upstream is never contacted; the remaining chunks still rewrite to the old
// public name.
func TestActiveStreamPromptBoundToOldSnapshot(t *testing.T) {
	upA := newFakeUpstream(t)
	upB := newFakeUpstream(t)
	chunk1Sent := make(chan struct{})
	gate := make(chan struct{})
	// A failed assertion must not leave the upstream handler blocked on the
	// gate forever: httptest.Server.Close waits for it, and with it the
	// whole test binary.
	t.Cleanup(func() {
		select {
		case <-gate:
		default:
			close(gate)
		}
	})
	upA.setHandler(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		fl := w.(http.Flusher)
		_, _ = fmt.Fprintf(w, "data: {\"id\":\"s1\",\"model\":\"upA\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"one\"},\"finish_reason\":null}]}\n\n")
		fl.Flush()
		close(chunk1Sent)
		<-gate
		_, _ = fmt.Fprintf(w, "data: {\"id\":\"s1\",\"model\":\"upA\",\"choices\":[{\"index\":1,\"delta\":{\"content\":\"two\"},\"finish_reason\":null}]}\n\n")
		fl.Flush()
		_, _ = io.WriteString(w, "data: [DONE]\n\n")
		fl.Flush()
	})
	upB.setHandler(jsonChatHandler("upB"))
	yamlA := runtimeYAML("common", upA.url()+"/v1", "upA", "P1")
	yamlB := runtimeYAML("common", upB.url()+"/v1", "upB", "P2")
	p := startSubprocess(t, startOpts{yaml: yamlA})

	resp := openJSON(t, p.addr, "/v1/chat/completions", chatStreamRequest("common"),
		map[string]string{"Accept": "text/event-stream"})
	defer func() { _ = resp.Body.Close() }()
	br := bufio.NewReader(resp.Body)

	<-chunk1Sent
	lines, eof := nextSSEEvent(t, br, 3*time.Second)
	if eof {
		t.Fatal("stream ended before chunk1")
	}
	assertChatSSE(t, lines, "common", "one")

	rewriteConfig(t, p.cfgPath, yamlB)
	waitForEventCount(t, p, "log_level_applied", 1) // ack visible at any level

	// The forwarded request was bound to the old snapshot: its body carries
	// P1 and only P1 (the body was recorded when A received the request).
	if n := upA.count(); n != 1 {
		t.Fatalf("A saw %d requests, want 1", n)
	}
	forwarded := string(upA.requests()[0].Body)
	if !strings.Contains(forwarded, "P1") || strings.Contains(forwarded, "P2") {
		t.Fatalf("forwarded body not bound to the old prompt: %s", forwarded)
	}
	if n := upB.count(); n != 0 {
		t.Fatalf("B saw %d requests mid-stream, want 0", n)
	}

	close(gate)
	lines, eof = nextSSEEvent(t, br, 3*time.Second)
	if eof {
		t.Fatal("stream ended before chunk2")
	}
	assertChatSSE(t, lines, "common", "two")
	lines, eof = nextSSEEvent(t, br, 3*time.Second)
	if eof || !strings.Contains(strings.Join(lines, "\n"), "[DONE]") {
		t.Fatalf("missing [DONE] (eof=%v lines=%q)", eof, lines)
	}
	if n := upB.count(); n != 0 {
		t.Fatalf("B saw %d requests after completion, want 0", n)
	}
}
