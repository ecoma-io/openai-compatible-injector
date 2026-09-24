package proxy

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/rs/zerolog"

	"openai-compatible-injector/internal/config"
	"openai-compatible-injector/internal/inject"
)

// thinkingStore builds a store whose test-model carries a thinking-usage
// block. Empty mode builds the default store (no block at all).
func thinkingStore(t *testing.T, endpoint, mode, lo, hi string) *config.Store {
	t.Helper()
	block := ""
	if mode != "" {
		block = fmt.Sprintf("    thinking-usage:\n      mode: %s\n", mode)
		if lo != "" {
			block += fmt.Sprintf("      min-ratio: %s\n", lo)
		}
		if hi != "" {
			block += fmt.Sprintf("      max-ratio: %s\n", hi)
		}
	}
	yaml := fmt.Sprintf("api-key: %s\nmodels:\n  test-model:\n    endpoint: %s\n    upstream-model: upstream-name\n%s", testAPIKey, endpoint, block)
	snap, err := config.LoadRuntime([]byte(yaml))
	if err != nil {
		t.Fatalf("LoadRuntime: %v", err)
	}
	return config.NewStore(snap)
}

// withDraw swaps the plan resolver's share draw for the test and restores it
// after, so cases can pin the draw-once contract and the exact share.
func withDraw(t *testing.T, f func() float64) {
	t.Helper()
	old := thinkingDraw
	thinkingDraw = f
	t.Cleanup(func() { thinkingDraw = old })
}

// dataPayloads extracts every data-line payload from an SSE body.
func dataPayloads(t *testing.T, body string) []string {
	t.Helper()
	var out []string
	for _, ln := range strings.Split(body, "\n") {
		if rest, ok := strings.CutPrefix(ln, "data: "); ok {
			out = append(out, rest)
		}
	}
	return out
}

func TestThinkingUsageBufferedChat(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"model":"upstream-name","choices":[{"index":0,"message":{"role":"assistant","content":"hi"}}],"usage":{"prompt_tokens":10,"completion_tokens":100,"total_tokens":110}}`)
	}))
	defer upstream.Close()

	h := newTestHandler(t, thinkingStore(t, upstream.URL+"/v1", "always", "", ""))
	rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions", `{"model":"test-model"}`, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body %s)", rec.Code, rec.Body.String())
	}
	// Both response transforms apply in one pass: the model rewrite and the
	// synthesized reasoning share, everything else byte-preserved.
	want := `{"model":"test-model","choices":[{"index":0,"message":{"role":"assistant","content":"hi"}}],"usage":{"completion_tokens_details":{"reasoning_tokens":75},"prompt_tokens":10,"completion_tokens":100,"total_tokens":110}}`
	if got := rec.Body.String(); got != want {
		t.Fatalf("body:\n got %s\nwant %s", got, want)
	}
}

func TestThinkingUsageBufferedResponses(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"type":"response.completed","response":{"id":"resp_1","usage":{"input_tokens":10,"output_tokens":100,"output_tokens_details":{"reasoning_tokens":0}}}}`)
	}))
	defer upstream.Close()

	h := newTestHandler(t, thinkingStore(t, upstream.URL+"/v1", "always", "", ""))
	rec := doRequest(t, h, http.MethodPost, "/v1/responses", `{"model":"test-model"}`, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body %s)", rec.Code, rec.Body.String())
	}
	want := `{"type":"response.completed","response":{"id":"resp_1","usage":{"input_tokens":10,"output_tokens":100,"output_tokens_details":{"reasoning_tokens":75}}}}`
	if got := rec.Body.String(); got != want {
		t.Fatalf("body:\n got %s\nwant %s", got, want)
	}
}

func TestThinkingUsageStreamChat(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: {\"model\":\"upstream-name\",\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n")
		_, _ = io.WriteString(w, "data: {\"model\":\"upstream-name\",\"choices\":[],\"usage\":{\"completion_tokens\":100}}\n\n")
		_, _ = io.WriteString(w, "data: [DONE]\n\n")
	}))
	defer upstream.Close()

	h := newTestHandler(t, thinkingStore(t, upstream.URL+"/v1", "always", "", ""))
	rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions", `{"model":"test-model","stream":true}`, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	payloads := dataPayloads(t, rec.Body.String())
	if len(payloads) != 3 {
		t.Fatalf("data lines = %v, want 3", payloads)
	}
	// A mid-stream chunk without usage: only the model rewrite touches it.
	if got, want := payloads[0], `{"model":"test-model","choices":[{"delta":{"content":"hi"}}]}`; got != want {
		t.Fatalf("first chunk:\n got %s\nwant %s", got, want)
	}
	// The final usage chunk gains the synthesized reasoning share.
	if got, want := payloads[1], `{"model":"test-model","choices":[],"usage":{"completion_tokens_details":{"reasoning_tokens":75},"completion_tokens":100}}`; got != want {
		t.Fatalf("usage chunk:\n got %s\nwant %s", got, want)
	}
	if payloads[2] != "[DONE]" {
		t.Fatalf("terminator = %q, want [DONE] untouched", payloads[2])
	}
}

func TestThinkingUsageStreamResponses(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		// A usage-only payload with no top-level model: only the widened
		// usage gate lets the rewriter see it.
		_, _ = io.WriteString(w, "event: response.completed\n")
		_, _ = io.WriteString(w, "data: {\"type\":\"response.completed\",\"response\":{\"usage\":{\"output_tokens\":100}}}\n\n")
		_, _ = io.WriteString(w, "event: done\n")
		_, _ = io.WriteString(w, "data: [DONE]\n\n")
	}))
	defer upstream.Close()

	h := newTestHandler(t, thinkingStore(t, upstream.URL+"/v1", "always", "", ""))
	rec := doRequest(t, h, http.MethodPost, "/v1/responses", `{"model":"test-model","stream":true}`, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	// The event line and terminator pass through; the usage-only data line
	// is enriched despite carrying no model key.
	want := "event: response.completed\n" +
		"data: {\"type\":\"response.completed\",\"response\":{\"usage\":{\"output_tokens_details\":{\"reasoning_tokens\":75},\"output_tokens\":100}}}\n\n" +
		"event: done\ndata: [DONE]\n\n"
	if got := rec.Body.String(); got != want {
		t.Fatalf("stream body:\n got %s\nwant %s", got, want)
	}
}

// TestThinkingUsageDrawOnce pins the plan's draw-once contract end to end: a
// stream whose events carry SEVERAL usage objects draws the share exactly
// once, and every usage object in the request reports the same one.
func TestThinkingUsageDrawOnce(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: {\"usage\":{\"completion_tokens\":100}}\n\n")
		_, _ = io.WriteString(w, "data: {\"usage\":{\"completion_tokens\":40}}\n\n")
	}))
	defer upstream.Close()

	draws := 0
	withDraw(t, func() float64 { draws++; return 0 }) // share pinned at Lo = 0.5
	h := newTestHandler(t, thinkingStore(t, upstream.URL+"/v1", "always", "0.5", "1.0"))
	rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions", `{"model":"test-model","stream":true}`, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	payloads := dataPayloads(t, rec.Body.String())
	if len(payloads) != 2 {
		t.Fatalf("data lines = %v, want 2", payloads)
	}
	if draws != 1 {
		t.Fatalf("share drawn %d times, want exactly 1 per request", draws)
	}
	if got, want := payloads[0], `{"usage":{"completion_tokens_details":{"reasoning_tokens":50},"completion_tokens":100}}`; got != want {
		t.Fatalf("first usage chunk:\n got %s\nwant %s", got, want)
	}
	if got, want := payloads[1], `{"usage":{"completion_tokens_details":{"reasoning_tokens":20},"completion_tokens":40}}`; got != want {
		t.Fatalf("second usage chunk:\n got %s\nwant %s", got, want)
	}
}

// TestThinkingUsageDefaultOffByteIdentity pins the default at the handler
// level: a model entry without a thinking-usage block relays both response
// paths byte for byte — the quiet direction, which a regression would
// silently break.
func TestThinkingUsageDefaultOffByteIdentity(t *testing.T) {
	bufferedBody := `{"id":"x","choices":[{"message":{"role":"assistant","content":"hi"}}],"usage":{"prompt_tokens":10,"completion_tokens":100,"total_tokens":110}}`
	streamBody := "data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n" +
		"data: {\"usage\":{\"completion_tokens\":100}}\n\n" +
		"data: [DONE]\n\n"

	t.Run("streamed", func(t *testing.T) {
		upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = io.WriteString(w, streamBody)
		}))
		defer upstream.Close()
		h := newTestHandler(t, newTestStore(t, upstream.URL+"/v1"))
		rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions", `{"model":"test-model","stream":true}`, nil)
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200", rec.Code)
		}
		if got := rec.Body.String(); got != streamBody {
			t.Fatalf("default-off stream must be byte-identical:\n got %q\nwant %q", got, streamBody)
		}
	})

	t.Run("buffered", func(t *testing.T) {
		upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, bufferedBody)
		}))
		defer upstream.Close()
		h := newTestHandler(t, newTestStore(t, upstream.URL+"/v1"))
		rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions", `{"model":"test-model"}`, nil)
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200", rec.Code)
		}
		if got := rec.Body.String(); got != bufferedBody {
			t.Fatalf("default-off buffered body must be byte-identical:\n got %q\nwant %q", got, bufferedBody)
		}
	})
}

// TestThinkingUsageAutoIntent pins the auto mode's gate through the full
// handler: the request's own thinking signal decides.
func TestThinkingUsageAutoIntent(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"model":"upstream-name","usage":{"completion_tokens":100}}`)
	}))
	defer upstream.Close()

	tests := []struct {
		name string
		body string
		want string
	}{
		{"reasoning_effort signals intent", `{"model":"test-model","reasoning_effort":"high"}`, `{"model":"test-model","usage":{"completion_tokens_details":{"reasoning_tokens":75},"completion_tokens":100}}`},
		{"no signal no synthesis", `{"model":"test-model"}`, `{"model":"test-model","usage":{"completion_tokens":100}}`},
		{"explicit none no synthesis", `{"model":"test-model","reasoning_effort":"none"}`, `{"model":"test-model","usage":{"completion_tokens":100}}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newTestHandler(t, thinkingStore(t, upstream.URL+"/v1", "auto", "", ""))
			rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions", tt.body, nil)
			if rec.Code != http.StatusOK {
				t.Fatalf("status = %d, want 200 (body %s)", rec.Code, rec.Body.String())
			}
			if got := rec.Body.String(); got != tt.want {
				t.Fatalf("body:\n got %s\nwant %s", got, tt.want)
			}
		})
	}
}

// TestThinkingUsageStreamBufferedParity pins the composed-rewriter parity: a
// stream chunk and a buffered body carrying the same JSON rewrite to the
// same bytes, because one closure serves both paths.
func TestThinkingUsageStreamBufferedParity(t *testing.T) {
	payload := `{"model":"upstream-name","usage":{"completion_tokens":100}}`
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if strings.Contains(string(body), `"stream":true`) {
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = io.WriteString(w, "data: "+payload+"\n\n")
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, payload)
	}))
	defer upstream.Close()
	h := newTestHandler(t, thinkingStore(t, upstream.URL+"/v1", "always", "", ""))

	buffered := doRequest(t, h, http.MethodPost, "/v1/chat/completions", `{"model":"test-model"}`, nil)
	if buffered.Code != http.StatusOK {
		t.Fatalf("buffered status = %d", buffered.Code)
	}
	streamed := doRequest(t, h, http.MethodPost, "/v1/chat/completions", `{"model":"test-model","stream":true}`, nil)
	if streamed.Code != http.StatusOK {
		t.Fatalf("streamed status = %d", streamed.Code)
	}
	payloads := dataPayloads(t, streamed.Body.String())
	if len(payloads) != 1 {
		t.Fatalf("stream data lines = %v, want 1", payloads)
	}
	if payloads[0] != buffered.Body.String() {
		t.Fatalf("stream and buffered rewrites disagree:\n stream   %s\nbuffered %s", payloads[0], buffered.Body.String())
	}
}

// TestSSEUsageGate pins the widened data-line gate on rewriteSSELine itself:
// a usage-only payload reaches the rewriter, and under an inactive plan the
// composed rewriter's no-op keeps the line byte-identical — the same
// backing slice, so the pointer shortcut fires.
func TestSSEUsageGate(t *testing.T) {
	active := inject.ThinkingPlan{Active: true, Share: 0.75}
	inactive := inject.ThinkingPlan{}
	compose := func(plan inject.ThinkingPlan) func([]byte) []byte {
		return func(p []byte) []byte {
			out := inject.RewriteChatModel(p, "test-model")
			if plan.Active {
				out = inject.SynthesizeChatThinkingUsage(out, plan)
			}
			return out
		}
	}

	line := []byte("data: {\"usage\":{\"completion_tokens\":100}}\n")

	out := rewriteSSELine(line, compose(active), nil)
	if got, want := string(out), "data: {\"usage\":{\"completion_tokens_details\":{\"reasoning_tokens\":75},\"completion_tokens\":100}}\n"; got != want {
		t.Fatalf("active gate:\n got %s\nwant %s", got, want)
	}

	out = rewriteSSELine(line, compose(inactive), nil)
	if string(out) != string(line) {
		t.Fatalf("inactive gate must keep the line byte-identical:\n got %s\nwant %s", out, line)
	}
	if &out[0] != &line[0] {
		t.Fatalf("inactive gate must return the same backing slice")
	}
}

// TestThinkingUsageResolvedDebugEvent pins the feature's DEBUG lifecycle
// event: present with mode/intent/active when the model configures the
// feature, absent on the default store — and never carrying a share, a
// ratio, or any payload byte.
func TestThinkingUsageResolvedDebugEvent(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"model":"upstream-name","usage":{"completion_tokens":100}}`)
	}))
	defer upstream.Close()

	t.Run("configured model logs the event", func(t *testing.T) {
		buf, log := captureLog(zerolog.DebugLevel)
		h := NewHandler(thinkingStore(t, upstream.URL+"/v1", "auto", "", ""), directResolver(), nil, nil, log)
		rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions", `{"model":"test-model","reasoning_effort":"high"}`, nil)
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d", rec.Code)
		}
		events := buf.events(t, "thinking_usage_resolved")
		if len(events) != 1 {
			t.Fatalf("thinking_usage_resolved events = %d, want 1", len(events))
		}
		ev := events[0]
		if ev["mode"] != "auto" || ev["intent"] != true || ev["active"] != true {
			t.Fatalf("event fields = %v, want mode=auto intent=true active=true", ev)
		}
		for _, forbidden := range []string{"share", "ratio", "body", "payload"} {
			if _, ok := ev[forbidden]; ok {
				t.Fatalf("event carries field %q — metadata only", forbidden)
			}
		}
	})

	t.Run("default model logs nothing", func(t *testing.T) {
		buf, log := captureLog(zerolog.DebugLevel)
		h := NewHandler(newTestStore(t, upstream.URL+"/v1"), directResolver(), nil, nil, log)
		rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions", `{"model":"test-model"}`, nil)
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d", rec.Code)
		}
		if events := buf.events(t, "thinking_usage_resolved"); len(events) != 0 {
			t.Fatalf("thinking_usage_resolved events = %d, want 0 on the default store", len(events))
		}
	})
}
