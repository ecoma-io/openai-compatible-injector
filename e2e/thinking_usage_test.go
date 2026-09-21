package e2e_test

import (
	"bufio"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

// thinkingYAML renders a model entry with a thinking-usage block. mode ""
// omits the block entirely. The block's members carry two leading spaces:
// runtimeYAML indents every extra field by the entry's four, landing nested
// members at six.
func thinkingYAML(publicName, endpoint, upstreamModel, mode, lo, hi string) string {
	var fields []string
	if mode != "" {
		fields = append(fields, "thinking-usage:", "  mode: "+mode)
		if lo != "" {
			fields = append(fields, "  min-ratio: "+lo)
		}
		if hi != "" {
			fields = append(fields, "  max-ratio: "+hi)
		}
	}
	return runtimeYAML(publicName, endpoint, upstreamModel, "", fields...)
}

// reasoningOf digs the synthesized reasoning count out of a chat usage
// object: usage.completion_tokens_details.reasoning_tokens.
func chatReasoning(t *testing.T, body []byte) float64 {
	t.Helper()
	var parsed struct {
		Usage struct {
			CompletionTokensDetails struct {
				ReasoningTokens float64 `json:"reasoning_tokens"`
			} `json:"completion_tokens_details"`
		} `json:"usage"`
	}
	if err := json.Unmarshal(body, &parsed); err != nil {
		t.Fatalf("unmarshal %s: %v", body, err)
	}
	return parsed.Usage.CompletionTokensDetails.ReasoningTokens
}

// responsesReasoning digs the synthesized count out of a Responses payload's
// usage object (top level or inside response), whichever carries it.
func responsesReasoning(t *testing.T, body []byte) (float64, bool) {
	t.Helper()
	var parsed struct {
		Usage struct {
			OutputTokensDetails struct {
				ReasoningTokens float64 `json:"reasoning_tokens"`
			} `json:"output_tokens_details"`
		} `json:"usage"`
		Response struct {
			Usage *struct {
				OutputTokensDetails struct {
					ReasoningTokens float64 `json:"reasoning_tokens"`
				} `json:"output_tokens_details"`
			} `json:"usage"`
		} `json:"response"`
	}
	if err := json.Unmarshal(body, &parsed); err != nil {
		t.Fatalf("unmarshal %s: %v", body, err)
	}
	if parsed.Response.Usage != nil {
		return parsed.Response.Usage.OutputTokensDetails.ReasoningTokens, true
	}
	return parsed.Usage.OutputTokensDetails.ReasoningTokens, true
}

// Scenario T1: mode always, buffered chat. The client-facing usage object
// gains reasoning_tokens = floor(0.75 × completion_tokens); the model is
// rewritten as ever; nothing else in the body changes.
func TestThinkingUsageBufferedChat(t *testing.T) {
	up := newFakeUpstream(t)
	up.setHandler(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"id":"c1","model":"upstream-chat","choices":[{"message":{"role":"assistant","content":"hi"}}],"usage":{"prompt_tokens":10,"completion_tokens":100,"total_tokens":110}}`)
	})
	p := startSubprocess(t, startOpts{
		yaml: thinkingYAML(chatPublic, up.url()+"/v1", chatUpstream, "always", "", ""),
	})

	status, _, body := postJSON(t, p.addr, "/v1/chat/completions",
		`{"model":"chat-public","messages":[]}`, nil)
	if status != http.StatusOK {
		t.Fatalf("status %d, want 200", status)
	}
	m := decodeMap(t, body)
	if m["model"] != chatPublic {
		t.Fatalf("model %v, want %s", m["model"], chatPublic)
	}
	if got := chatReasoning(t, body); got != 75 {
		t.Fatalf("reasoning_tokens %v, want 75 (floor(0.75 × 100))", got)
	}
}

// Scenario T2: mode always, buffered Responses envelope. The usage object
// inside response.output — the response.completed shape — is enriched with
// the responses member names.
func TestThinkingUsageBufferedResponses(t *testing.T) {
	up := newFakeUpstream(t)
	up.setHandler(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"type":"response.completed","response":{"id":"r1","usage":{"input_tokens":10,"output_tokens":100,"total_tokens":110}}}`)
	})
	p := startSubprocess(t, startOpts{
		yaml: thinkingYAML(chatPublic, up.url()+"/v1", chatUpstream, "always", "", ""),
	})

	status, _, body := postJSON(t, p.addr, "/v1/responses",
		`{"model":"chat-public","input":"hello"}`, nil)
	if status != http.StatusOK {
		t.Fatalf("status %d, want 200", status)
	}
	got, ok := responsesReasoning(t, body)
	if !ok || got != 75 {
		t.Fatalf("response.usage reasoning_tokens %v (ok=%v), want 75", got, ok)
	}
}

// Scenario T3: mode always, streamed chat. The usage-bearing final chunk is
// enriched; the model-only chunk is rewritten as ever; [DONE] untouched.
func TestThinkingUsageStreamChat(t *testing.T) {
	up := newFakeUpstream(t)
	up.setHandler(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fl := w.(http.Flusher)
		_, _ = io.WriteString(w, "data: {\"model\":\"upstream-chat\",\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n")
		_, _ = io.WriteString(w, "data: {\"model\":\"upstream-chat\",\"choices\":[],\"usage\":{\"completion_tokens\":100}}\n\n")
		_, _ = io.WriteString(w, "data: [DONE]\n\n")
		fl.Flush()
	})
	p := startSubprocess(t, startOpts{
		yaml: thinkingYAML(chatPublic, up.url()+"/v1", chatUpstream, "always", "", ""),
	})

	resp := openJSON(t, p.addr, "/v1/chat/completions",
		`{"model":"chat-public","messages":[],"stream":true}`, nil)
	defer func() { _ = resp.Body.Close() }()
	br := bufio.NewReader(resp.Body)

	lines, eof := nextSSEEvent(t, br, 3*time.Second)
	if eof || len(lines) == 0 {
		t.Fatalf("first chunk missing (eof=%v lines=%q)", eof, lines)
	}
	assertChatSSE(t, lines, chatPublic, "hi")

	lines, eof = nextSSEEvent(t, br, 3*time.Second)
	if eof || len(lines) != 1 {
		t.Fatalf("usage chunk missing (eof=%v lines=%q)", eof, lines)
	}
	if got := chatReasoning(t, []byte(sseDataContent(t, lines[0]))); got != 75 {
		t.Fatalf("streamed reasoning_tokens %v, want 75", got)
	}

	lines, eof = nextSSEEvent(t, br, 3*time.Second)
	if eof || !strings.Contains(strings.Join(lines, "\n"), "[DONE]") {
		t.Fatalf("missing [DONE] (eof=%v lines=%q)", eof, lines)
	}
}

// Scenario T4: mode always, streamed Responses. A response.completed event
// whose data payload carries usage only inside the response object — no
// top-level model — still gets the synthesis (the widened SSE gate), and the
// event framing survives.
func TestThinkingUsageStreamResponses(t *testing.T) {
	up := newFakeUpstream(t)
	up.setHandler(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fl := w.(http.Flusher)
		_, _ = io.WriteString(w, "event: response.completed\n")
		_, _ = io.WriteString(w, "data: {\"type\":\"response.completed\",\"response\":{\"usage\":{\"output_tokens\":100}}}\n\n")
		fl.Flush()
	})
	p := startSubprocess(t, startOpts{
		yaml: thinkingYAML(chatPublic, up.url()+"/v1", chatUpstream, "always", "", ""),
	})

	resp := openJSON(t, p.addr, "/v1/responses",
		`{"model":"chat-public","input":"hello","stream":true}`, nil)
	defer func() { _ = resp.Body.Close() }()
	br := bufio.NewReader(resp.Body)

	lines, eof := nextSSEEvent(t, br, 3*time.Second)
	if eof || len(lines) != 2 || !strings.HasPrefix(lines[0], "event:") {
		t.Fatalf("response.completed event missing (eof=%v lines=%q)", eof, lines)
	}
	got, ok := responsesReasoning(t, []byte(sseDataContent(t, lines[1])))
	if !ok || got != 75 {
		t.Fatalf("streamed responses reasoning_tokens %v (ok=%v), want 75", got, ok)
	}
}

// Scenario T5: the default. No thinking-usage block: both response paths
// relay byte-identically — the quiet direction a regression would silently
// break.
func TestThinkingUsageDefaultOffByteIdentity(t *testing.T) {
	const bufferedBody = `{"id":"c1","model":"upstream-chat","choices":[{"message":{"role":"assistant","content":"hi"}}],"usage":{"prompt_tokens":10,"completion_tokens":100,"total_tokens":110}}`
	const streamBody = "data: {\"id\":\"c1\",\"model\":\"upstream-chat\",\"choices\":[{\"delta\":{\"content\":\"hi\"}}],\"usage\":{\"completion_tokens\":100}}\n\n" +
		"data: [DONE]\n\n"

	t.Run("buffered", func(t *testing.T) {
		up := newFakeUpstream(t)
		up.setHandler(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, bufferedBody)
		})
		p := startSubprocess(t, startOpts{
			yaml: runtimeYAML(chatPublic, up.url()+"/v1", chatUpstream, ""),
		})
		status, _, body := postJSON(t, p.addr, "/v1/chat/completions",
			`{"model":"chat-public","messages":[]}`, nil)
		if status != http.StatusOK {
			t.Fatalf("status %d, want 200", status)
		}
		// Model rewrite still applies; usage untouched.
		want := strings.Replace(bufferedBody, `"upstream-chat"`, `"chat-public"`, 1)
		if string(body) != want {
			t.Fatalf("default-off buffered body:\n got %s\nwant %s", body, want)
		}
	})

	t.Run("streamed", func(t *testing.T) {
		up := newFakeUpstream(t)
		up.setHandler(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/event-stream")
			fl := w.(http.Flusher)
			_, _ = io.WriteString(w, streamBody)
			fl.Flush()
		})
		p := startSubprocess(t, startOpts{
			yaml: runtimeYAML(chatPublic, up.url()+"/v1", chatUpstream, ""),
		})
		resp := openJSON(t, p.addr, "/v1/chat/completions",
			`{"model":"chat-public","messages":[],"stream":true}`, nil)
		defer func() { _ = resp.Body.Close() }()
		got, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatalf("read stream: %v", err)
		}
		want := strings.Replace(streamBody, `"upstream-chat"`, `"chat-public"`, 1)
		if string(got) != want {
			t.Fatalf("default-off stream body:\n got %q\nwant %q", got, want)
		}
	})
}

// Scenario T6: auto mode follows the request's thinking signal end to end —
// and a mid-stream share stays per-request: the same stream reports one
// consistent number.
func TestThinkingUsageAutoIntent(t *testing.T) {
	up := newFakeUpstream(t)
	up.setHandler(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"model":"upstream-chat","usage":{"completion_tokens":100}}`)
	})
	p := startSubprocess(t, startOpts{
		yaml: thinkingYAML(chatPublic, up.url()+"/v1", chatUpstream, "auto", "", ""),
	})

	tests := []struct {
		name string
		body string
		want float64
	}{
		{"reasoning_effort signals thinking", `{"model":"chat-public","messages":[],"reasoning_effort":"high"}`, 75},
		{"no signal means no synthesis", `{"model":"chat-public","messages":[]}`, 0},
		{"effort none means no synthesis", `{"model":"chat-public","messages":[],"reasoning_effort":"none"}`, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			status, _, body := postJSON(t, p.addr, "/v1/chat/completions", tt.body, nil)
			if status != http.StatusOK {
				t.Fatalf("status %d, want 200", status)
			}
			if got := chatReasoning(t, body); got != tt.want {
				t.Fatalf("reasoning_tokens %v, want %v", got, tt.want)
			}
		})
	}
}

// Scenario T7: ranged ratios draw a share per request. A fixed-completion
// upstream called repeatedly under min-ratio 0.25 / max-ratio 0.5 must
// report values only inside [25, 50] — and different requests eventually
// report different values (the draw actually varies).
func TestThinkingUsageRangedSharePerRequest(t *testing.T) {
	up := newFakeUpstream(t)
	up.setHandler(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"model":"upstream-chat","usage":{"completion_tokens":100}}`)
	})
	p := startSubprocess(t, startOpts{
		yaml: thinkingYAML(chatPublic, up.url()+"/v1", chatUpstream, "always", "0.25", "0.5"),
	})

	seen := map[float64]bool{}
	for i := 0; i < 40; i++ {
		status, _, body := postJSON(t, p.addr, "/v1/chat/completions",
			`{"model":"chat-public","messages":[]}`, nil)
		if status != http.StatusOK {
			t.Fatalf("request %d: status %d", i, status)
		}
		got := chatReasoning(t, body)
		if got < 25 || got > 50 {
			t.Fatalf("request %d: reasoning_tokens %v outside [25,50]", i, got)
		}
		seen[got] = true
	}
	if len(seen) < 2 {
		t.Fatalf("40 requests reported a single share %v — the per-request draw never varied", seen)
	}
}

// Scenario T8: one stream, many usage objects — every usage object in the
// request reports the same share (drawn once, before any upstream I/O), and
// the upstream-reported reasoning always wins.
func TestThinkingUsageStreamConsistentShare(t *testing.T) {
	up := newFakeUpstream(t)
	up.setHandler(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fl := w.(http.Flusher)
		_, _ = io.WriteString(w, "data: {\"usage\":{\"completion_tokens\":100}}\n\n")
		_, _ = io.WriteString(w, "data: {\"usage\":{\"completion_tokens\":40}}\n\n")
		_, _ = io.WriteString(w, "data: {\"usage\":{\"completion_tokens\":100,\"reasoning_tokens\":40}}\n\n")
		_, _ = io.WriteString(w, "data: [DONE]\n\n")
		fl.Flush()
	})
	p := startSubprocess(t, startOpts{
		yaml: thinkingYAML(chatPublic, up.url()+"/v1", chatUpstream, "always", "0.75", "0.75"),
	})

	resp := openJSON(t, p.addr, "/v1/chat/completions",
		`{"model":"chat-public","messages":[],"stream":true}`, nil)
	defer func() { _ = resp.Body.Close() }()
	br := bufio.NewReader(resp.Body)

	wantShares := []float64{75, 30} // the third chunk reports its own number
	for i, want := range wantShares {
		lines, eof := nextSSEEvent(t, br, 3*time.Second)
		if eof || len(lines) != 1 {
			t.Fatalf("usage chunk %d missing (eof=%v lines=%q)", i, eof, lines)
		}
		if got := chatReasoning(t, []byte(sseDataContent(t, lines[0]))); got != want {
			t.Fatalf("usage chunk %d: reasoning_tokens %v, want %v", i, got, want)
		}
	}
	// The third chunk reports reasoning_tokens directly (Claude-style): the
	// upstream's own 40 wins — untouched, and no synthesized details member
	// appears beside it.
	lines, eof := nextSSEEvent(t, br, 3*time.Second)
	if eof || len(lines) != 1 {
		t.Fatalf("third usage chunk missing (eof=%v lines=%q)", eof, lines)
	}
	raw := decodeMap(t, []byte(sseDataContent(t, lines[0])))
	usage, _ := raw["usage"].(map[string]any)
	if usage == nil {
		t.Fatalf("third chunk has no usage object: %v", raw)
	}
	if got, ok := usage["reasoning_tokens"].(float64); !ok || got != 40 {
		t.Fatalf("third chunk direct reasoning_tokens = %v (ok=%v), want 40 untouched", usage["reasoning_tokens"], ok)
	}
	if _, has := usage["completion_tokens_details"]; has {
		t.Fatalf("third chunk gained a synthesized details member beside the upstream's own number: %v", usage)
	}
	lines, eof = nextSSEEvent(t, br, 3*time.Second)
	if eof || !strings.Contains(strings.Join(lines, "\n"), "[DONE]") {
		t.Fatalf("missing [DONE] (eof=%v lines=%q)", eof, lines)
	}
}

// Scenario T9: hot reload mid-stream. A stream opened under the old config
// keeps the old synthesis to its last chunk (one snapshot per request); a
// request after the reload follows the new config.
func TestThinkingUsageReloadBindsStreamToOldSnapshot(t *testing.T) {
	up := newFakeUpstream(t)
	gate := make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-gate:
		default:
			close(gate)
		}
	})
	up.setHandler(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fl := w.(http.Flusher)
		_, _ = io.WriteString(w, "data: {\"usage\":{\"completion_tokens\":100}}\n\n")
		fl.Flush()
		<-gate // hold the second usage chunk across the reload
		_, _ = io.WriteString(w, "data: {\"usage\":{\"completion_tokens\":100}}\n\n")
		_, _ = io.WriteString(w, "data: [DONE]\n\n")
		fl.Flush()
	})
	cfgBody := func(mode string) string {
		return thinkingYAML(chatPublic, up.url()+"/v1", chatUpstream, mode, "", "")
	}
	p := startSubprocess(t, startOpts{yaml: cfgBody("always")})

	resp := openJSON(t, p.addr, "/v1/chat/completions",
		`{"model":"chat-public","messages":[],"stream":true}`, nil)
	defer func() { _ = resp.Body.Close() }()
	br := bufio.NewReader(resp.Body)

	// First chunk under the old config: fixed 0.75 default share.
	lines, eof := nextSSEEvent(t, br, 3*time.Second)
	if eof || len(lines) != 1 {
		t.Fatalf("first usage chunk missing (eof=%v lines=%q)", eof, lines)
	}
	if got := chatReasoning(t, []byte(sseDataContent(t, lines[0]))); got != 75 {
		t.Fatalf("first chunk reasoning_tokens %v, want 75 (old snapshot)", got)
	}

	// Swap the running config to mode off while the stream is open, and give
	// the 50ms poller ample time to publish the new snapshot.
	rewriteConfig(t, p.cfgPath, withLoggingLevel(cfgBody("off"), "error"))
	time.Sleep(500 * time.Millisecond)

	close(gate)
	// The in-flight stream stays bound to the OLD snapshot: second chunk
	// still synthesized at 75.
	lines, eof = nextSSEEvent(t, br, 3*time.Second)
	if eof || len(lines) != 1 {
		t.Fatalf("second usage chunk missing (eof=%v lines=%q)", eof, lines)
	}
	if got := chatReasoning(t, []byte(sseDataContent(t, lines[0]))); got != 75 {
		t.Fatalf("second chunk reasoning_tokens %v, want 75 (in-flight request keeps its snapshot)", got)
	}
	lines, eof = nextSSEEvent(t, br, 3*time.Second)
	if eof || !strings.Contains(strings.Join(lines, "\n"), "[DONE]") {
		t.Fatalf("missing [DONE] (eof=%v lines=%q)", eof, lines)
	}

	// A fresh stream after the reload follows the NEW config: no synthesis.
	// The reload has long since landed (the 500ms sleep above spans ten poll
	// windows), so the first chunk must already report nothing.
	resp2 := openJSON(t, p.addr, "/v1/chat/completions",
		`{"model":"chat-public","messages":[],"stream":true}`, nil)
	defer func() { _ = resp2.Body.Close() }()
	br2 := bufio.NewReader(resp2.Body)
	lines, eof = nextSSEEvent(t, br2, 3*time.Second)
	if eof || len(lines) != 1 {
		t.Fatalf("post-reload first chunk missing (eof=%v lines=%q)", eof, lines)
	}
	if got := chatReasoning(t, []byte(sseDataContent(t, lines[0]))); got != 0 {
		t.Fatalf("post-reload reasoning_tokens %v, want 0 (new snapshot, feature off)", got)
	}
}
