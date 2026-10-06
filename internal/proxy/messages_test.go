package proxy

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/rs/zerolog"
)

// The Messages route's route-level contract. These tests run through
// NewHandler exactly like every other surface's, because the route's whole
// claim is that it is ordinary serve traffic under a different dialect: the
// same auth gate, the same 405-before-401 ordering, the same snapshot
// binding, the same request id. What differs is what goes upstream (Chat
// Completions) and what comes back (Anthropic), and those are the two
// directions pinned here.

// messagesRequest exercises every member the translation reads: a string
// system prompt, plain content, a tool with input_schema, stop_sequences,
// metadata, and a thinking block that must NOT reach the upstream. No
// "stream": the buffered path is the subject of the end-to-end case below.
const messagesRequest = `{"model":"test-model","max_tokens":512,"temperature":0.2,"system":"You are terse.","messages":[{"role":"user","content":"hi"},{"role":"assistant","content":"earlier"}],"tools":[{"name":"get_weather","description":"look up weather","cache_control":{"type":"ephemeral"},"input_schema":{"type":"object","properties":{"city":{"type":"string"}}}}],"stop_sequences":["END"],"metadata":{"user_id":"u-1"},"thinking":{"type":"enabled","budget_tokens":1024}}`

// chatAnswer is the upstream's Chat Completions reply to messagesRequest.
const chatAnswer = `{"id":"chatcmpl-abc","object":"chat.completion","model":"upstream-name","choices":[{"index":0,"message":{"role":"assistant","content":"Hello there"},"finish_reason":"stop"}],"usage":{"prompt_tokens":3,"completion_tokens":4,"total_tokens":7}}`

// upstreamHit is a mutex-guarded record of the ONE upstream exchange these
// tests care about: the handler goroutine writes it, the test reads it after
// the response returns, and -race runs in CI.
type upstreamHit struct {
	mu     sync.Mutex
	hits   int
	path   string
	header http.Header
	body   string
}

func (u *upstreamHit) record(r *http.Request, body string) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.hits++
	u.path = r.URL.Path
	u.header = r.Header.Clone()
	u.body = body
}

func (u *upstreamHit) count() int {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.hits
}

func (u *upstreamHit) snapshot() (string, http.Header, string) {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.path, u.header, u.body
}

// TestMessagesMethodNotAllowedBeforeAuth pins the ordering the whole gate
// rests on: a wrong method on /v1/messages is answered in the Messages
// dialect before any credential is looked at. The dialect is resolved before
// the method check for exactly this reason — a client told something is a
// client that quotes a ticket, and it must be told in its own shape with an
// id it can quote.
func TestMessagesMethodNotAllowedBeforeAuth(t *testing.T) {
	var up upstreamHit
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		up.record(r, string(b))
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, chatAnswer)
	}))
	defer srv.Close()

	h := newTestHandler(t, newTestStore(t, srv.URL+"/v1"))

	// No credential at all: Authorization is present but empty, so the
	// default bearer buildRequest would otherwise attach never lands.
	rec := doRequest(t, h, http.MethodGet, "/v1/messages", "", map[string]string{"Authorization": ""})
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("GET status = %d, want 405", rec.Code)
	}
	if got := rec.Header().Get(requestIDHeader); !isRequestID(got) {
		t.Errorf("%s = %q on the 405, want 16 hex chars", requestIDHeader, got)
	}
	if body := rec.Body.String(); body != anthropicDialect.badMethod {
		t.Errorf("405 body:\n got %s\nwant %s", body, anthropicDialect.badMethod)
	}
	if up.count() != 0 {
		t.Errorf("upstream hits = %d on a 405, want 0", up.count())
	}

	// The same path with the right method falls through to the auth gate —
	// that is what 405-before-401 names, and it is worth asserting from the
	// other side so a reorder cannot pass by answering both in dialect.
	rec = doRequest(t, h, http.MethodPost, "/v1/messages", messagesRequest, map[string]string{"Authorization": ""})
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("POST status = %d, want 401", rec.Code)
	}
	if body := rec.Body.String(); body != anthropicDialect.authMissing {
		t.Errorf("401 body:\n got %s\nwant %s", body, anthropicDialect.authMissing)
	}
	if up.count() != 0 {
		t.Errorf("upstream hits = %d on a 401, want 0", up.count())
	}
}

// TestMessagesAuthGateExactBodies pins the auth gate as Claude Code sees it:
// x-api-key alone is a full credential, Bearer wins when both appear (the
// precedence e2e/wire_resource_test.go pins from the other side), and every
// failure lands in the Anthropic envelope with byte-exact bodies while the
// upstream hit count stays frozen.
func TestMessagesAuthGateExactBodies(t *testing.T) {
	var up upstreamHit
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		up.record(r, string(b))
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, chatAnswer)
	}))
	defer srv.Close()

	h := newTestHandler(t, newTestStore(t, srv.URL+"/v1"))

	cases := []struct {
		name    string
		headers map[string]string
		want    int
		body    string
	}{
		{"x-api-key only", map[string]string{"Authorization": "", "X-Api-Key": testAPIKey}, http.StatusOK, ""},
		{"bearer wins over a wrong x-api-key", map[string]string{"Authorization": "Bearer " + testAPIKey, "X-Api-Key": "wrong-key"}, http.StatusOK, ""},
		{"wrong x-api-key", map[string]string{"Authorization": "", "X-Api-Key": "wrong-key"}, http.StatusUnauthorized, anthropicDialect.authInvalid},
		{"neither header", map[string]string{"Authorization": "", "X-Api-Key": ""}, http.StatusUnauthorized, anthropicDialect.authMissing},
		{"non-bearer scheme", map[string]string{"Authorization": "Basic abc", "X-Api-Key": ""}, http.StatusUnauthorized, anthropicDialect.authMissing},
		{"bearer with no token", map[string]string{"Authorization": "Bearer", "X-Api-Key": ""}, http.StatusUnauthorized, anthropicDialect.authMissing},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			before := up.count()
			rec := doRequest(t, h, http.MethodPost, "/v1/messages", messagesRequest, tc.headers)
			if rec.Code != tc.want {
				t.Fatalf("status = %d, want %d (body %q)", rec.Code, tc.want, rec.Body.String())
			}
			if tc.want == http.StatusOK {
				if after := up.count(); after == before {
					t.Errorf("upstream hits = %d, want a relayed exchange", after)
				}
				return
			}
			if got := rec.Body.String(); got != tc.body {
				t.Errorf("401 body:\n got %s\nwant %s", got, tc.body)
			}
			if after := up.count(); after != before {
				t.Errorf("upstream hits moved %d -> %d on an unauthorized request", before, after)
			}
		})
	}

	// The missing-key envelope must name BOTH ways in, or a Claude Code
	// install configured with only ANTHROPIC_API_KEY is told nothing useful.
	if !strings.Contains(anthropicDialect.authMissing, "Authorization") ||
		!strings.Contains(anthropicDialect.authMissing, "x-api-key") {
		t.Fatalf("authMissing does not name both credential headers: %s", anthropicDialect.authMissing)
	}
}

// TestMessagesStripsContextMarker pins [1m] handling and, just as
// importantly, the quiet direction beside it: only the Messages surface
// strips. This machine's Claude Code has CLAUDE_CODE_DISABLE_1M_CONTEXT set
// and strips client-side, so the wire never carried the marker and the
// behavior cannot be proven through a client — it is proven here.
func TestMessagesStripsContextMarker(t *testing.T) {
	var up upstreamHit
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		up.record(r, string(b))
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, chatAnswer)
	}))
	defer srv.Close()

	h := newTestHandler(t, newTestStore(t, srv.URL+"/v1"))

	t.Run("configured model resolves with the marker", func(t *testing.T) {
		body := `{"model":"test-model[1m]","messages":[{"role":"user","content":"hi"}]}`
		rec := doRequest(t, h, http.MethodPost, "/v1/messages", body, nil)
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200 (body %q)", rec.Code, rec.Body.String())
		}
		_, _, got := up.snapshot()
		var doc map[string]json.RawMessage
		if err := json.Unmarshal([]byte(got), &doc); err != nil {
			t.Fatalf("upstream body is not JSON: %v (%s)", err, got)
		}
		if string(doc["model"]) != `"upstream-name"` {
			t.Errorf("upstream model = %s, want the configured alias (the marker must not reach the lookup)", doc["model"])
		}
		if strings.Contains(got, "[1m]") {
			t.Errorf("upstream body carries the context marker: %s", got)
		}
	})

	t.Run("unknown model 404s under its stripped name", func(t *testing.T) {
		before := up.count()
		body := `{"model":"ghost-model[1m]","messages":[{"role":"user","content":"hi"}]}`
		rec := doRequest(t, h, http.MethodPost, "/v1/messages", body, nil)
		if rec.Code != http.StatusNotFound {
			t.Fatalf("status = %d, want 404 (body %q)", rec.Code, rec.Body.String())
		}
		want := `{"type":"error","error":{"type":"not_found_error","message":"The model 'ghost-model' does not exist or you do not have access to it."}}`
		if got := rec.Body.String(); got != want {
			t.Errorf("404 body:\n got %s\nwant %s", got, want)
		}
		if after := up.count(); after != before {
			t.Errorf("upstream hits moved %d -> %d on an unknown model", before, after)
		}
	})

	t.Run("chat route does not strip", func(t *testing.T) {
		before := up.count()
		body := `{"model":"ghost-model[1m]","messages":[]}`
		rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions", body, nil)
		if rec.Code != http.StatusNotFound {
			t.Fatalf("status = %d, want 404 (body %q)", rec.Code, rec.Body.String())
		}
		want := `{"error":{"message":"The model 'ghost-model[1m]' does not exist or you do not have access to it.","type":"invalid_request_error","param":null,"code":"model_not_found"}}`
		if got := rec.Body.String(); got != want {
			t.Errorf("404 body:\n got %s\nwant %s", got, want)
		}
		if after := up.count(); after != before {
			t.Errorf("upstream hits moved %d -> %d on an unknown model", before, after)
		}
	})
}

// TestMessagesBufferedTranslate is the end-to-end direction check: an
// Anthropic request in, a Chat Completions exchange upstream, an Anthropic
// answer out. Every assertion is a property the translation could quietly
// break — the injection prompt losing its place, input_schema leaking as
// parameters or vice versa, a client-only member reaching a strict upstream,
// or a credential header riding along because the route reused the wrong
// forward list.
func TestMessagesBufferedTranslate(t *testing.T) {
	var up upstreamHit
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		up.record(r, string(b))
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, chatAnswer)
	}))
	defer srv.Close()

	const marker = "INJECTION-MARKER must be messages[0]"
	h := newTestHandler(t, promptStore(t, srv.URL+"/v1", marker, testAPIKey))

	rec := doRequest(t, h, http.MethodPost, "/v1/messages", messagesRequest, map[string]string{
		"Authorization":     "",
		"X-Api-Key":         testAPIKey,
		"Anthropic-Version": "2023-06-01",
		"Anthropic-Beta":    "prompt-caching-2024-07-31",
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body %q)", rec.Code, rec.Body.String())
	}

	// --- upstream side ---
	path, header, sent := up.snapshot()
	if path != "/v1/chat/completions" {
		t.Errorf("upstream path = %q, want /v1/chat/completions", path)
	}
	for _, name := range []string{"Authorization", "X-Api-Key", "Anthropic-Version", "Anthropic-Beta"} {
		if v := header.Get(name); v != "" {
			t.Errorf("upstream %s = %q, want absent (client credentials and dialect markers are consumed, never forwarded)", name, v)
		}
	}
	upID := header.Get(requestIDHeader)
	if !isRequestID(upID) {
		t.Errorf("upstream %s = %q, want 16 hex chars", requestIDHeader, upID)
	}
	if got := rec.Header().Get(requestIDHeader); got != upID {
		t.Errorf("response %s = %q, upstream %q — one id across every surface", requestIDHeader, got, upID)
	}

	var doc map[string]any
	if err := json.Unmarshal([]byte(sent), &doc); err != nil {
		t.Fatalf("upstream body is not JSON: %v (%s)", err, sent)
	}
	if doc["model"] != "upstream-name" {
		t.Errorf("upstream model = %v, want upstream-name", doc["model"])
	}
	if strings.Contains(sent, "input_schema") {
		t.Errorf("upstream body carries input_schema: %s", sent)
	}
	if strings.Contains(sent, "cache_control") || strings.Contains(sent, "thinking") {
		t.Errorf("upstream body carries a client-only member: %s", sent)
	}
	if strings.Contains(sent, "stream_options") {
		t.Errorf("non-streaming request must not gain stream_options: %s", sent)
	}
	if doc["stop"] != nil {
		if stop, ok := doc["stop"].([]any); !ok || len(stop) != 1 || stop[0] != "END" {
			t.Errorf("stop = %v, want [END] (stop_sequences)", doc["stop"])
		}
	}
	if doc["user"] != "u-1" {
		t.Errorf("user = %v, want u-1 (metadata.user_id)", doc["user"])
	}
	if doc["max_tokens"] != float64(512) {
		t.Errorf("max_tokens = %v, want 512", doc["max_tokens"])
	}
	if doc["temperature"] != 0.2 {
		t.Errorf("temperature = %v, want 0.2", doc["temperature"])
	}
	tools, _ := doc["tools"].([]any)
	if len(tools) != 1 {
		t.Fatalf("tools = %v, want exactly one", doc["tools"])
	}
	tool, _ := tools[0].(map[string]any)
	fn, _ := tool["function"].(map[string]any)
	if fn["name"] != "get_weather" {
		t.Errorf("function name = %v, want get_weather", fn["name"])
	}
	if _, ok := fn["parameters"].(map[string]any); !ok {
		t.Errorf("parameters = %v, want the input_schema object under OpenAI's own key", fn["parameters"])
	}

	msgs, _ := doc["messages"].([]any)
	if len(msgs) != 4 {
		t.Fatalf("messages = %v, want 4 (injection, system, user, assistant)", doc["messages"])
	}
	assertMessage := func(i int, role, content string) {
		t.Helper()
		m, _ := msgs[i].(map[string]any)
		if m["role"] != role {
			t.Errorf("messages[%d].role = %v, want %s", i, m["role"], role)
		}
		if m["content"] != content {
			t.Errorf("messages[%d].content = %v, want %s", i, m["content"], content)
		}
	}
	assertMessage(0, "system", marker)
	assertMessage(1, "system", "You are terse.")
	assertMessage(2, "user", "hi")
	assertMessage(3, "assistant", "earlier")

	// --- client side ---
	var ans map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &ans); err != nil {
		t.Fatalf("client body is not JSON: %v (%s)", err, rec.Body.String())
	}
	if ans["type"] != "message" || ans["role"] != "assistant" {
		t.Errorf("envelope = type %v role %v, want message/assistant", ans["type"], ans["role"])
	}
	if ans["model"] != "test-model" {
		t.Errorf("model = %v, want the PUBLIC name the client asked for", ans["model"])
	}
	if ans["id"] != "chatcmpl-abc" {
		t.Errorf("id = %v, want the upstream's id passed through opaquely", ans["id"])
	}
	if ans["stop_reason"] != "end_turn" {
		t.Errorf("stop_reason = %v, want end_turn", ans["stop_reason"])
	}
	content, _ := ans["content"].([]any)
	if len(content) != 1 {
		t.Fatalf("content = %v, want one text block", ans["content"])
	}
	blk, _ := content[0].(map[string]any)
	if blk["type"] != "text" || blk["text"] != "Hello there" {
		t.Errorf("content[0] = %v, want the text block carrying the answer", blk)
	}
	usage, _ := ans["usage"].(map[string]any)
	if usage["input_tokens"] != float64(3) || usage["output_tokens"] != float64(4) {
		t.Errorf("usage = %v, want input_tokens 3 / output_tokens 4", ans["usage"])
	}
	if strings.Contains(rec.Body.String(), `"finish_reason"`) ||
		strings.Contains(rec.Body.String(), `"prompt_tokens"`) {
		t.Errorf("client body carries Chat vocabulary: %s", rec.Body.String())
	}
}

// TestMessagesTransformErrorAndMetering pins the two bookkeeping surfaces
// that are easy to leave behind a route addition: a local translation
// failure must answer in the Messages dialect with the existing
// transform_error outcome (and no metered row — it never reached a
// provider), and a successful exchange must meter under api "messages"
// while reading the chat-shaped usage object the upstream actually sent.
func TestMessagesTransformErrorAndMetering(t *testing.T) {
	t.Run("transform failure answers in the Messages dialect", func(t *testing.T) {
		var up upstreamHit
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			b, _ := io.ReadAll(r.Body)
			up.record(r, string(b))
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, chatAnswer)
		}))
		defer srv.Close()

		buf, log := captureLog(zerolog.InfoLevel)
		meter := &recordingMeter{}
		h := NewHandler(newTestStore(t, srv.URL+"/v1"), directResolver(), nil, nil, meter, log)

		// Valid JSON, routable model, and a "messages" member this
		// translation refuses — the body that fails this candidate's
		// transform fails every candidate's, so the answer is a local 400
		// and never a fallback.
		body := `{"model":"test-model","messages":"not-an-array"}`
		rec := doRequest(t, h, http.MethodPost, "/v1/messages", body, nil)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("status = %d, want 400 (body %q)", rec.Code, rec.Body.String())
		}
		if got := rec.Body.String(); got != anthropicDialect.invalidReq {
			t.Errorf("400 body:\n got %s\nwant %s", got, anthropicDialect.invalidReq)
		}
		if !isRequestID(rec.Header().Get(requestIDHeader)) {
			t.Errorf("%s = %q on the 400, want 16 hex chars", requestIDHeader, rec.Header().Get(requestIDHeader))
		}
		if up.count() != 0 {
			t.Errorf("upstream hits = %d on a local transform failure, want 0", up.count())
		}
		if evs := meter.recorded(); len(evs) != 0 {
			t.Errorf("metered %d events on a request that never reached a provider, want 0", len(evs))
		}

		evs := buf.events(t, "request_completed")
		if len(evs) != 1 {
			t.Fatalf("request_completed logged %d times, want exactly 1: %s", len(evs), buf.String())
		}
		ev := evs[0]
		if ev["api"] != "messages" {
			t.Errorf("api = %v, want messages", ev["api"])
		}
		if ev["outcome"] != "transform_error" {
			t.Errorf("outcome = %v, want transform_error", ev["outcome"])
		}
		if ev["status"].(float64) != 400 {
			t.Errorf("status = %v, want 400", ev["status"])
		}
		for _, secret := range []string{"not-an-array", "SECRET"} {
			if strings.Contains(buf.String(), secret) {
				t.Errorf("log output contains %q — payload leak", secret)
			}
		}
	})

	t.Run("metering reads chat-shaped usage under api messages", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, chatAnswer)
		}))
		defer srv.Close()

		meter := &recordingMeter{}
		h := usageHandler(t, newTestStore(t, srv.URL+"/v1"), meter)

		rec := doRequest(t, h, http.MethodPost, "/v1/messages", messagesRequest, nil)
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200 (body %q)", rec.Code, rec.Body.String())
		}
		ev := meter.single(t)
		if ev.API != "messages" {
			t.Errorf("API = %q, want messages", ev.API)
		}
		if ev.Stream || ev.HTTPStatus != 200 || ev.Outcome != "completed" {
			t.Errorf("event facts = %+v", ev)
		}
		if ev.PublicModel != "test-model" || ev.UpstreamModel != "upstream-name" {
			t.Errorf("model facts = %q/%q, want test-model/upstream-name", ev.PublicModel, ev.UpstreamModel)
		}
		if ev.PromptTokens == nil || *ev.PromptTokens != 3 ||
			ev.CompletionTokens == nil || *ev.CompletionTokens != 4 ||
			ev.TotalTokens == nil || *ev.TotalTokens != 7 {
			t.Errorf("metered tokens = %v/%v/%v, want 3/4/7 (the chat-shaped object the upstream sent)",
				ev.PromptTokens, ev.CompletionTokens, ev.TotalTokens)
		}
		// The row is Anthropic on the wire; the meter must still describe
		// the upstream exchange it actually observed.
		if strings.Contains(rec.Body.String(), `"prompt_tokens"`) {
			t.Errorf("client body carries Chat usage vocabulary: %s", rec.Body.String())
		}
	})
}
