// Anthropic Messages surface, black-box: the third model-serving route
// through the built binary. Everything here asserts only what a real
// Anthropic client can observe — the request the upstream received, the
// answer the client received, and the headers each side saw — because the
// route's whole claim is that it is ordinary serve traffic under a
// different dialect.
//
// internal/proxy/messages_test.go pins the same contract against the
// handler directly. What only this level can prove is that the BINARY
// wires the route at all, that the dialect survives the real HTTP stack,
// and that nothing leaks past the forward allow-list on the wire.
package e2e_test

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

const (
	// messagesAnswer is the upstream Chat reply every buffered scenario
	// relays. It carries real usage counts because the Anthropic envelope
	// must report input_tokens/output_tokens, and a zero would satisfy a
	// shape check while being a lie about the exchange.
	messagesAnswer = `{"id":"chatcmpl-e2e","object":"chat.completion","model":"upstream-chat","choices":[{"index":0,"message":{"role":"assistant","content":"Hello there"},"finish_reason":"stop"}],"usage":{"prompt_tokens":3,"completion_tokens":4,"total_tokens":7}}`

	// messagesBody exercises every member the translation reads, plus two
	// that must NOT survive it: `thinking` (client-only) and the tool's
	// `cache_control` (a beta marker riding on the schema).
	messagesBody = `{"model":"chat-public","max_tokens":512,"temperature":0.2,"system":"You are terse.","messages":[{"role":"user","content":"hi"},{"role":"assistant","content":"earlier"}],"tools":[{"name":"get_weather","description":"look up weather","cache_control":{"type":"ephemeral"},"input_schema":{"type":"object","properties":{"city":{"type":"string"}}}}],"stop_sequences":["END"],"metadata":{"user_id":"u-1"},"thinking":{"type":"enabled","budget_tokens":1024}}`
)

// Byte-exact Anthropic envelopes (mirrors internal/proxy/envelope_dialect.go).
// They are literals rather than built values because the dialect's contract
// IS the bytes: a Messages client parses these, and a shape that merely
// decodes is not the shape it was promised.
const (
	anthropicAuthMissing = `{"type":"error","error":{"type":"authentication_error","message":"you must provide an API key in the Authorization header (Bearer <key>) or the x-api-key header"}}`
	anthropicAuthInvalid = `{"type":"error","error":{"type":"authentication_error","message":"invalid API key"}}`
	anthropicBadMethod   = `{"type":"error","error":{"type":"invalid_request_error","message":"method not allowed"}}`
	anthropicInvalidReq  = `{"type":"error","error":{"type":"invalid_request_error","message":"invalid JSON in request body"}}`
)

// upstreamSaysStream reports whether the request the fake upstream JUST
// recorded declared stream:true. fakeUpstream reads and drains r.Body
// before it invokes the handler, so a handler that re-reads the body sees
// nothing — the recorded copy is the only one left, and it is already this
// request's, because the record is appended before the handler runs.
//
// Failures are reported with Errorf, never Fatalf: this runs on the
// upstream server's goroutine, where Goexit would kill a goroutine that has
// no test to end.
func upstreamSaysStream(t *testing.T, up *fakeUpstream) bool {
	req, ok := up.last()
	if !ok {
		t.Errorf("upstream recorded no request")
		return false
	}
	var doc map[string]any
	if err := json.Unmarshal(req.Body, &doc); err != nil {
		t.Errorf("upstream body is not JSON: %v (%s)", err, req.Body)
		return false
	}
	stream, _ := doc["stream"].(bool)
	return stream
}

// messagesUpstream answers every request with a fixed 200 Chat completion
// carrying the given upstream model name. The request itself is already
// recorded by fakeUpstream, so the handler only has to answer.
func messagesUpstream(upstreamModel string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = fmt.Fprintf(w,
			`{"id":"chatcmpl-e2e","object":"chat.completion","model":%q,"choices":[{"index":0,"message":{"role":"assistant","content":"Hello there"},"finish_reason":"stop"}],"usage":{"prompt_tokens":3,"completion_tokens":4,"total_tokens":7}}`,
			upstreamModel)
	}
}

// postMessages issues a POST carrying exactly the headers the caller names —
// no default bearer, no extras. The suite's postJSON auto-presents
// `Authorization: Bearer <e2eAPIKey>` whenever the key is absent from its
// map, which makes an "x-api-key and nothing else" case inexpressible, and
// substituting an empty Authorization would test a present-but-empty header
// rather than the absent one a real Claude Code install sends.
func postMessages(t *testing.T, addr, path, body string, hdr map[string]string) (int, http.Header, []byte) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, "http://"+addr+path, strings.NewReader(body))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST %s: %v", path, err)
	}
	defer func() { _ = resp.Body.Close() }()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read response body: %v", err)
	}
	return resp.StatusCode, resp.Header.Clone(), b
}

// messagesRequest is postMessages for the canonical buffered body with no
// credentials at all — the case the default bearer would otherwise mask.
func messagesRequest(t *testing.T, addr, body string, hdr map[string]string) (int, http.Header, []byte) {
	t.Helper()
	return postMessages(t, addr, "/v1/messages", body, hdr)
}

// TestMessagesBufferedRoundTrip is the direction check through the BUILT
// BINARY: an Anthropic request in, a Chat Completions exchange upstream, an
// Anthropic answer out. Every assertion is a property the translation could
// quietly break — the injection prompt losing first place, `parameters`
// arriving as `input_schema`, a client-only member reaching a strict
// upstream, or a credential header riding along because the route reused
// the wrong forward list.
func TestMessagesBufferedRoundTrip(t *testing.T) {
	up := newFakeUpstream(t)
	up.setHandler(messagesUpstream(chatUpstream))
	const marker = "INJECTION-MARKER must be messages[0]"
	p := startSubprocess(t, startOpts{
		yaml:     runtimeYAML(chatPublic, up.url()+"/v1", chatUpstream, marker),
		logLevel: "error",
	})

	status, hdr, body := messagesRequest(t, p.addr, messagesBody, map[string]string{
		"X-Api-Key":         e2eAPIKey, // the credential this surface is built for
		"Anthropic-Version": "2023-06-01",
		"Anthropic-Beta":    "prompt-caching-2024-07-31",
		"X-App":             "cli",
	})
	if status != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body %s)", status, body)
	}

	// --- upstream side ---
	req, ok := up.last()
	if !ok {
		t.Fatal("upstream recorded no request")
	}
	if req.Path != "/v1/chat/completions" {
		t.Errorf("upstream path = %q, want /v1/chat/completions", req.Path)
	}
	// Client credentials and dialect markers are consumed by the proxy. The
	// allow-list carries only Content-Type/Accept/OpenAI-Beta, so none of
	// these may appear — a leak here is a credential disclosure, not a
	// cosmetic diff.
	for _, name := range []string{
		"Authorization", "X-Api-Key", "Anthropic-Version", "Anthropic-Beta", "X-App",
	} {
		if v := req.Headers.Get(name); v != "" {
			t.Errorf("upstream %s = %q, want absent (consumed, never forwarded)", name, v)
		}
	}
	upID := req.Headers.Get("X-Request-Id")
	if !e2eRequestID.MatchString(upID) {
		t.Errorf("upstream X-Request-Id = %q, want 16 hex chars", upID)
	}
	if got := hdr.Get("X-Request-Id"); got != upID {
		t.Errorf("response X-Request-Id = %q, upstream %q — one id across every surface", got, upID)
	}

	var doc map[string]any
	if err := json.Unmarshal(req.Body, &doc); err != nil {
		t.Fatalf("upstream body is not JSON: %v (%s)", err, req.Body)
	}
	if doc["model"] != chatUpstream {
		t.Errorf("upstream model = %v, want %s (the configured alias)", doc["model"], chatUpstream)
	}
	for _, banned := range []string{"input_schema", "cache_control", "thinking", "stop_sequences", "stream_options"} {
		if strings.Contains(string(req.Body), banned) {
			t.Errorf("upstream body carries %s: %s", banned, req.Body)
		}
	}
	if doc["max_tokens"] != float64(512) || doc["temperature"] != 0.2 {
		t.Errorf("scalar knobs = %v/%v, want 512/0.2", doc["max_tokens"], doc["temperature"])
	}
	if doc["user"] != "u-1" {
		t.Errorf("user = %v, want u-1 (metadata.user_id)", doc["user"])
	}
	stop, _ := doc["stop"].([]any)
	if len(stop) != 1 || stop[0] != "END" {
		t.Errorf("stop = %v, want [END] (stop_sequences)", doc["stop"])
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
	assertChatMessage := func(i int, role, content string) {
		t.Helper()
		m, _ := msgs[i].(map[string]any)
		if m["role"] != role || m["content"] != content {
			t.Errorf("messages[%d] = role %v content %v, want %s %q", i, m["role"], m["content"], role, content)
		}
	}
	assertChatMessage(0, "system", marker)
	assertChatMessage(1, "system", "You are terse.")
	assertChatMessage(2, "user", "hi")
	assertChatMessage(3, "assistant", "earlier")

	// --- client side ---
	var ans map[string]any
	if err := json.Unmarshal(body, &ans); err != nil {
		t.Fatalf("client body is not JSON: %v (%s)", err, body)
	}
	if ans["type"] != "message" || ans["role"] != "assistant" {
		t.Errorf("envelope = type %v role %v, want message/assistant", ans["type"], ans["role"])
	}
	if ans["model"] != chatPublic {
		t.Errorf("model = %v, want the PUBLIC name the client asked for", ans["model"])
	}
	if ans["id"] != "chatcmpl-e2e" {
		t.Errorf("id = %v, want the upstream id passed through opaquely", ans["id"])
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
	if strings.Contains(string(body), "finish_reason") || strings.Contains(string(body), "prompt_tokens") {
		t.Errorf("client body carries Chat vocabulary: %s", body)
	}
}

// TestMessagesXAPIKeyAuth pins the credential this surface exists for: a
// lone x-api-key is a full credential, the failure modes land in byte-exact
// Anthropic envelopes that name both ways in, and none of it ever reaches
// an upstream.
func TestMessagesXAPIKeyAuth(t *testing.T) {
	up := newFakeUpstream(t)
	up.setHandler(messagesUpstream(chatUpstream))
	p := startSubprocess(t, startOpts{
		yaml:     runtimeYAML(chatPublic, up.url()+"/v1", chatUpstream, ""),
		logLevel: "error",
	})

	t.Run("x-api-key alone authenticates", func(t *testing.T) {
		status, _, body := messagesRequest(t, p.addr, messagesBody,
			map[string]string{"X-Api-Key": e2eAPIKey})
		if status != http.StatusOK {
			t.Fatalf("x-api-key only: status = %d, want 200 (body %s)", status, body)
		}
		req, ok := up.last()
		if !ok {
			t.Fatal("upstream recorded no request")
		}
		if got := req.Headers.Get("X-Api-Key"); got != "" {
			t.Fatalf("upstream X-Api-Key = %q — the client credential was forwarded", got)
		}
	})

	for _, tc := range []struct {
		name string
		hdr  map[string]string
		want string
	}{
		{"wrong x-api-key", map[string]string{"X-Api-Key": "not-the-configured-key"}, anthropicAuthInvalid},
		{"neither header", map[string]string{}, anthropicAuthMissing},
		{"non-bearer scheme", map[string]string{"Authorization": "Basic abc"}, anthropicAuthMissing},
	} {
		t.Run(tc.name, func(t *testing.T) {
			before := up.count()
			status, _, body := messagesRequest(t, p.addr, messagesBody, tc.hdr)
			if status != http.StatusUnauthorized {
				t.Fatalf("status = %d, want 401 (body %s)", status, body)
			}
			if string(body) != tc.want {
				t.Fatalf("401 body:\n got %s\nwant %s", body, tc.want)
			}
			if after := up.count(); after != before {
				t.Errorf("upstream hits moved %d -> %d on an unauthorized request", before, after)
			}
		})
	}

	// The missing-key envelope must name BOTH ways in, or a Claude Code
	// install configured with only ANTHROPIC_API_KEY is told nothing useful.
	if !strings.Contains(anthropicAuthMissing, "Authorization") ||
		!strings.Contains(anthropicAuthMissing, "x-api-key") {
		t.Fatalf("authMissing does not name both credential headers: %s", anthropicAuthMissing)
	}
}

// TestMessagesMethodNotAllowedBeforeAuth pins the ordering through the real
// binary: a wrong method on /v1/messages is answered in the Messages
// dialect before any credential is looked at, with a request id the client
// can quote in a ticket.
func TestMessagesMethodNotAllowedBeforeAuth(t *testing.T) {
	up := newFakeUpstream(t)
	up.setHandler(messagesUpstream(chatUpstream))
	p := startSubprocess(t, startOpts{
		yaml:     runtimeYAML(chatPublic, up.url()+"/v1", chatUpstream, ""),
		logLevel: "error",
	})

	// A bare GET: no Authorization, no X-Api-Key — the case a default-bearer
	// helper would silently fill in.
	resp, err := http.Get("http://" + p.addr + "/v1/messages")
	if err != nil {
		t.Fatalf("GET /v1/messages: %v", err)
	}
	body := readBody(t, resp)
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("GET /v1/messages status = %d, want 405 (body %s)", resp.StatusCode, body)
	}
	if string(body) != anthropicBadMethod {
		t.Fatalf("405 body:\n got %s\nwant %s", body, anthropicBadMethod)
	}
	if id := resp.Header.Get("X-Request-Id"); !e2eRequestID.MatchString(id) {
		t.Errorf("X-Request-Id on the 405 = %q, want 16 hex chars", id)
	}
	if n := up.count(); n != 0 {
		t.Fatalf("upstream hits = %d on a 405, want 0", n)
	}

	// The same path with the right method falls through to the auth gate —
	// that is what 405-before-401 names, and it is worth asserting from the
	// other side so a reorder cannot pass by answering both in dialect.
	status, _, body := messagesRequest(t, p.addr, messagesBody, nil)
	if status != http.StatusUnauthorized {
		t.Fatalf("POST without credentials status = %d, want 401", status)
	}
	if string(body) != anthropicAuthMissing {
		t.Fatalf("401 body:\n got %s\nwant %s", body, anthropicAuthMissing)
	}
	if n := up.count(); n != 0 {
		t.Fatalf("upstream hits = %d on a 401, want 0", n)
	}
}

// TestMessagesUnknownModel404ByteExact pins the 404 every Anthropic client
// parses: the envelope is the Anthropic one, the message interpolates the
// requested model byte-exact with no HTML escaping, and the request never
// reaches a provider.
func TestMessagesUnknownModel404ByteExact(t *testing.T) {
	up := newFakeUpstream(t)
	up.setHandler(messagesUpstream(chatUpstream))
	p := startSubprocess(t, startOpts{
		yaml:     runtimeYAML(chatPublic, up.url()+"/v1", chatUpstream, ""),
		logLevel: "error",
	})

	// A name carrying the two characters a JSON encoder would escape: the
	// body must quote it verbatim, because it is what the client typed.
	const ghost = `<script>&ghost`
	status, _, body := messagesRequest(t, p.addr,
		`{"model":"`+ghost+`","messages":[{"role":"user","content":"hi"}]}`,
		map[string]string{"X-Api-Key": e2eAPIKey})
	if status != http.StatusNotFound {
		t.Fatalf("status = %d, want 404 (body %s)", status, body)
	}
	want := `{"type":"error","error":{"type":"not_found_error","message":"The model '` + ghost + `' does not exist or you do not have access to it."}}`
	if string(body) != want {
		t.Fatalf("404 body:\n got %s\nwant %s", body, want)
	}
	if strings.Contains(string(body), `\u003c`) || strings.Contains(string(body), `\u0026`) {
		t.Fatalf("the model name was HTML-escaped: %s", body)
	}
	if n := up.count(); n != 0 {
		t.Fatalf("upstream hits = %d on an unknown model, want 0", n)
	}
}

// TestMessagesStripsContextMarker pins [1m] handling against the BUILT
// BINARY. This machine's Claude Code has CLAUDE_CODE_DISABLE_1M_CONTEXT set
// and strips client-side, so the wire never carries the marker and no real
// client can prove this — it is proven here, in the direction that matters:
// the marker never reaches the model lookup, and the 404 quotes the
// stripped name. The quiet half sits beside it: the chat route does NOT
// strip, because an operator who literally configured a model named
// "…[1m]" must still resolve it.
func TestMessagesStripsContextMarker(t *testing.T) {
	up := newFakeUpstream(t)
	up.setHandler(messagesUpstream(chatUpstream))
	p := startSubprocess(t, startOpts{
		yaml:     runtimeYAML(chatPublic, up.url()+"/v1", chatUpstream, ""),
		logLevel: "error",
	})

	t.Run("configured model resolves with the marker", func(t *testing.T) {
		status, _, body := messagesRequest(t, p.addr,
			`{"model":"chat-public[1m]","messages":[{"role":"user","content":"hi"}]}`,
			map[string]string{"X-Api-Key": e2eAPIKey})
		if status != http.StatusOK {
			t.Fatalf("status = %d, want 200 (body %s)", status, body)
		}
		req, ok := up.last()
		if !ok {
			t.Fatal("upstream recorded no request")
		}
		var doc map[string]any
		if err := json.Unmarshal(req.Body, &doc); err != nil {
			t.Fatalf("upstream body is not JSON: %v (%s)", err, req.Body)
		}
		if doc["model"] != chatUpstream {
			t.Errorf("upstream model = %v, want the configured alias", doc["model"])
		}
		if strings.Contains(string(req.Body), "[1m]") {
			t.Errorf("the context marker reached the upstream: %s", req.Body)
		}
	})

	t.Run("unknown model 404s under its stripped name", func(t *testing.T) {
		before := up.count()
		status, _, body := messagesRequest(t, p.addr,
			`{"model":"ghost-model[1m]","messages":[{"role":"user","content":"hi"}]}`,
			map[string]string{"X-Api-Key": e2eAPIKey})
		if status != http.StatusNotFound {
			t.Fatalf("status = %d, want 404 (body %s)", status, body)
		}
		want := `{"type":"error","error":{"type":"not_found_error","message":"The model 'ghost-model' does not exist or you do not have access to it."}}`
		if string(body) != want {
			t.Fatalf("404 body:\n got %s\nwant %s", body, want)
		}
		if after := up.count(); after != before {
			t.Errorf("upstream hits moved %d -> %d on an unknown model", before, after)
		}
	})

	t.Run("chat route does not strip", func(t *testing.T) {
		before := up.count()
		status, _, body := postJSON(t, p.addr, "/v1/chat/completions",
			`{"model":"ghost-model[1m]","messages":[{"role":"user","content":"hi"}]}`, nil)
		if status != http.StatusNotFound {
			t.Fatalf("status = %d, want 404 (body %s)", status, body)
		}
		want := `{"error":{"message":"The model 'ghost-model[1m]' does not exist or you do not have access to it.","type":"invalid_request_error","param":null,"code":"model_not_found"}}`
		if string(body) != want {
			t.Fatalf("404 body:\n got %s\nwant %s", body, want)
		}
		if after := up.count(); after != before {
			t.Errorf("upstream hits moved %d -> %d on an unknown model", before, after)
		}
	})
}

// messagesChatStream is a complete, healthy upstream Chat SSE stream: a role
// chunk, one text delta, the finish chunk carrying usage, and [DONE]. It is
// the input every translated-stream scenario starts from.
const messagesChatStream = "data: {\"id\":\"c1\",\"model\":\"upstream-chat\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\"}}]}\n\n" +
	"data: {\"id\":\"c1\",\"model\":\"upstream-chat\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"PONG\"}}]}\n\n" +
	"data: {\"id\":\"c1\",\"model\":\"upstream-chat\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":3,\"completion_tokens\":4,\"total_tokens\":7}}\n\n" +
	"data: [DONE]\n\n"

// chatStreamUpstream serves a fixed Chat SSE stream for any request.
func chatStreamUpstream(body string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		fl := w.(http.Flusher)
		_, _ = io.WriteString(w, body)
		fl.Flush()
	}
}

// readAllLines drains a streamed response into its raw lines, in order. A
// keep-alive comment is its own line, so a rule about what may follow a
// particular line is expressible directly rather than through event framing
// that would have to reassemble it.
func readAllLines(t *testing.T, resp *http.Response) []string {
	t.Helper()
	s := newSSEStream(t, resp)
	defer s.close()
	var lines []string
	for {
		line, err := s.line(30 * time.Second)
		if line != "" {
			lines = append(lines, strings.TrimRight(line, "\r\n"))
		}
		if err != nil {
			if err == io.EOF {
				return lines
			}
			t.Fatalf("read stream: %v", err)
		}
	}
}

// TestMessagesStreamTranslate is the headline streaming direction check: an
// upstream Chat stream in, the full ordered Anthropic event list out, and
// not one byte of the upstream's own dialect. The usage placement is
// asserted too — real counts belong on message_delta, because a client that
// reads only message_start would otherwise be told the answer was free.
func TestMessagesStreamTranslate(t *testing.T) {
	up := newFakeUpstream(t)
	up.setHandler(chatStreamUpstream(messagesChatStream))
	p := startSubprocess(t, startOpts{
		yaml:     runtimeYAML(chatPublic, up.url()+"/v1", chatUpstream, ""),
		logLevel: "error",
	})

	resp := openJSON(t, p.addr, "/v1/messages",
		`{"model":"chat-public","max_tokens":64,"messages":[{"role":"user","content":"ping"}],"stream":true}`,
		map[string]string{"X-Api-Key": e2eAPIKey, "Accept": "text/event-stream"})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/event-stream") {
		t.Fatalf("Content-Type = %q, want text/event-stream", ct)
	}
	lines := readAllLines(t, resp)

	wantEvents := []string{
		"message_start",
		"content_block_start",
		"content_block_delta",
		"content_block_stop",
		"message_delta",
		"message_stop",
	}
	var gotEvents, payloads []string
	for _, ln := range lines {
		if name, ok := strings.CutPrefix(ln, "event: "); ok {
			gotEvents = append(gotEvents, name)
		}
		if payload, ok := strings.CutPrefix(ln, "data: "); ok {
			payloads = append(payloads, payload)
		}
	}
	if strings.Join(gotEvents, ",") != strings.Join(wantEvents, ",") {
		t.Fatalf("events = %v\nwant  = %v\nbody:\n%s", gotEvents, wantEvents, strings.Join(lines, "\n"))
	}
	joined := strings.Join(lines, "\n")
	if strings.Contains(joined, "[DONE]") {
		t.Fatalf("the upstream's [DONE] reached the client:\n%s", joined)
	}
	if strings.Contains(joined, "upstream-chat") {
		t.Fatalf("the upstream model name reached the client:\n%s", joined)
	}
	if !strings.Contains(joined, `"model":"`+chatPublic+`"`) {
		t.Fatalf("message_start did not carry the public model:\n%s", joined)
	}
	if !strings.Contains(joined, `"text":"PONG"`) {
		t.Fatalf("the answer text did not ride a text_delta:\n%s", joined)
	}

	// The counts live on message_delta, not message_start.
	var startDoc, deltaDoc map[string]any
	if err := json.Unmarshal([]byte(payloads[0]), &startDoc); err != nil {
		t.Fatalf("message_start payload is not JSON: %v (%s)", err, payloads[0])
	}
	msg, _ := startDoc["message"].(map[string]any)
	if start, _ := msg["usage"].(map[string]any); start["input_tokens"] != float64(0) {
		t.Errorf("message_start usage = %v, want zeros (the counts are not known yet)", msg["usage"])
	}
	if err := json.Unmarshal([]byte(payloads[len(payloads)-2]), &deltaDoc); err != nil {
		t.Fatalf("message_delta payload is not JSON: %v (%s)", err, payloads[len(payloads)-2])
	}
	deltaUsage, _ := deltaDoc["usage"].(map[string]any)
	if deltaUsage["input_tokens"] != float64(3) || deltaUsage["output_tokens"] != float64(4) {
		t.Errorf("message_delta usage = %v, want input_tokens 3 / output_tokens 4", deltaDoc["usage"])
	}
	delta, _ := deltaDoc["delta"].(map[string]any)
	if delta["stop_reason"] != "end_turn" {
		t.Errorf("stop_reason = %v, want end_turn", delta["stop_reason"])
	}
}

// TestMessagesStreamNoPingAfterStop drives the keep-alive heartbeat against
// the translated stream through the real binary. The upstream goes silent
// TWICE: before the terminal, where pings are correct and serve as the
// control, and after it, where every ping would be a violation — the
// heartbeat's `finished` latch has to close on the `event: message_stop`
// line itself, not on the connection closing.
func TestMessagesStreamNoPingAfterStop(t *testing.T) {
	// 1s is the configuration floor for sse-keep-alive.interval; the silence
	// windows straddle two of them so the heartbeat cannot miss them on phase.
	const interval = time.Second
	const silence = interval*2 + 200*time.Millisecond

	up := newFakeUpstream(t)
	up.setHandler(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		fl := w.(http.Flusher)
		_, _ = io.WriteString(w,
			"data: {\"id\":\"c1\",\"model\":\"upstream-chat\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hi\"}}]}\n\n")
		fl.Flush()
		time.Sleep(silence) // pings are correct here
		_, _ = io.WriteString(w,
			"data: {\"id\":\"c1\",\"model\":\"upstream-chat\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":1,\"completion_tokens\":1,\"total_tokens\":2}}\n\n")
		fl.Flush()
		_, _ = io.WriteString(w, "data: [DONE]\n\n")
		fl.Flush()
		time.Sleep(silence) // the half that matters: still open, already terminated
	})
	p := startSubprocess(t, startOpts{
		yaml: runtimeYAML(chatPublic, up.url()+"/v1", chatUpstream, "") +
			fmt.Sprintf("sse-keep-alive:\n  interval: %s\n", interval),
	})

	resp := openJSON(t, p.addr, "/v1/messages",
		`{"model":"chat-public","max_tokens":64,"messages":[{"role":"user","content":"ping"}],"stream":true}`,
		map[string]string{"X-Api-Key": e2eAPIKey, "Accept": "text/event-stream"})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	lines := readAllLines(t, resp)

	stop := -1
	for i, ln := range lines {
		if ln == "event: message_stop" {
			stop = i
			break
		}
	}
	if stop < 0 {
		t.Fatalf("no event: message_stop in the stream:\n%s", strings.Join(lines, "\n"))
	}
	before, after := 0, 0
	for i, ln := range lines {
		if ln != ": ping" {
			continue
		}
		if i < stop {
			before++
		} else {
			after++
		}
	}
	if before < 1 {
		t.Fatalf("the pre-terminal silence produced %d pings, want >= 1 — without the control "+
			"a heartbeat that never fires at all would pass the assertion below:\n%s",
			before, strings.Join(lines, "\n"))
	}
	if after != 0 {
		t.Fatalf("%d keep-alive pings followed event: message_stop (line %d of %d):\n%s",
			after, stop, len(lines), strings.Join(lines, "\n"))
	}
}

// TestMessagesToolRoundTrip covers the 27-tool contract Claude Code lives
// on, in both directions: a streamed upstream tool call becomes
// content_block_start/input_json_delta/content_block_stop with the id
// passed through opaquely, and a follow-up request carrying the tool_result
// reaches the upstream as a role:"tool" message with the SAME id — the
// pairing an OpenAI upstream validates.
func TestMessagesToolRoundTrip(t *testing.T) {
	const toolStream = "data: {\"id\":\"c1\",\"model\":\"upstream-chat\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"tool_calls\":[{\"index\":0,\"id\":\"call_abc\",\"type\":\"function\",\"function\":{\"name\":\"get_weather\",\"arguments\":\"\"}}]}}]}\n\n" +
		"data: {\"id\":\"c1\",\"model\":\"upstream-chat\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"function\":{\"arguments\":\"{\\\"city\\\":\\\"Oslo\\\"}\"}}]}}]}\n\n" +
		"data: {\"id\":\"c1\",\"model\":\"upstream-chat\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"tool_calls\"}],\"usage\":{\"prompt_tokens\":5,\"completion_tokens\":7,\"total_tokens\":12}}\n\n" +
		"data: [DONE]\n\n"

	up := newFakeUpstream(t)
	up.setHandler(func(w http.ResponseWriter, r *http.Request) {
		if upstreamSaysStream(t, up) {
			chatStreamUpstream(toolStream)(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, messagesAnswer)
	})
	p := startSubprocess(t, startOpts{
		yaml:     runtimeYAML(chatPublic, up.url()+"/v1", chatUpstream, ""),
		logLevel: "error",
	})

	// --- direction 1: upstream tool call, Anthropic events out ---
	resp := openJSON(t, p.addr, "/v1/messages",
		`{"model":"chat-public","max_tokens":256,"messages":[{"role":"user","content":"weather?"}],"stream":true,`+
			`"tools":[{"name":"get_weather","description":"look up weather","input_schema":{"type":"object","properties":{"city":{"type":"string"}}}}]}`,
		map[string]string{"X-Api-Key": e2eAPIKey, "Accept": "text/event-stream"})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("stream status = %d, want 200", resp.StatusCode)
	}
	joined := strings.Join(readAllLines(t, resp), "\n")
	for _, want := range []string{
		`"type":"tool_use","id":"call_abc","name":"get_weather","input":{}`,
		`"partial_json":"{\"city\":\"Oslo\"}"`,
		`"stop_reason":"tool_use"`,
		`event: message_stop`,
	} {
		if !strings.Contains(joined, want) {
			t.Fatalf("streamed tool call missing %s:\n%s", want, joined)
		}
	}

	// --- direction 2: the tool_result rides back with the same id ---
	second := `{"model":"chat-public","max_tokens":256,"messages":[` +
		`{"role":"user","content":"weather?"},` +
		`{"role":"assistant","content":[{"type":"tool_use","id":"call_abc","name":"get_weather","input":{"city":"Oslo"}}]},` +
		`{"role":"user","content":[{"type":"tool_result","tool_use_id":"call_abc","content":"sunny"}]}]}`
	status, _, body := postJSON(t, p.addr, "/v1/messages", second,
		map[string]string{"X-Api-Key": e2eAPIKey})
	if status != http.StatusOK {
		t.Fatalf("follow-up status = %d, want 200 (body %s)", status, body)
	}

	req, ok := up.last()
	if !ok {
		t.Fatal("upstream recorded no request")
	}
	var doc map[string]any
	if err := json.Unmarshal(req.Body, &doc); err != nil {
		t.Fatalf("upstream body is not JSON: %v (%s)", err, req.Body)
	}
	msgs, _ := doc["messages"].([]any)
	if len(msgs) != 3 {
		t.Fatalf("upstream messages = %v, want 3 (user, assistant tool_calls, tool)", doc["messages"])
	}
	assistant, _ := msgs[1].(map[string]any)
	calls, _ := assistant["tool_calls"].([]any)
	if len(calls) != 1 {
		t.Fatalf("assistant tool_calls = %v, want exactly one", assistant["tool_calls"])
	}
	call, _ := calls[0].(map[string]any)
	if call["id"] != "call_abc" {
		t.Errorf("tool_call id = %v, want the id the upstream itself issued", call["id"])
	}
	tool, _ := msgs[2].(map[string]any)
	if tool["role"] != "tool" {
		t.Errorf("messages[2].role = %v, want tool", tool["role"])
	}
	if tool["tool_call_id"] != "call_abc" {
		t.Errorf("tool_call_id = %v, want call_abc (the pairing an upstream validates)", tool["tool_call_id"])
	}
	if tool["content"] != "sunny" {
		t.Errorf("tool content = %v, want sunny", tool["content"])
	}
}

// TestMessagesStreamOptionInjected pins the one member the translation ADDS
// rather than maps: a streamed Messages request carries
// stream_options.include_usage so the upstream reports the counts this
// proxy otherwise has no way to show, and a buffered one does not — an
// extra member on a non-streaming request would be noise a strict upstream
// could reject. Neither affects the meter, which reads the upstream's own
// bytes before any rewrite.
func TestMessagesStreamOptionInjected(t *testing.T) {
	up := newFakeUpstream(t)
	up.setHandler(func(w http.ResponseWriter, r *http.Request) {
		if upstreamSaysStream(t, up) {
			chatStreamUpstream(messagesChatStream)(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, messagesAnswer)
	})
	p := startSubprocess(t, startOpts{
		yaml:     runtimeYAML(chatPublic, up.url()+"/v1", chatUpstream, ""),
		logLevel: "error",
	})

	resp := openJSON(t, p.addr, "/v1/messages",
		`{"model":"chat-public","max_tokens":64,"messages":[{"role":"user","content":"ping"}],"stream":true}`,
		map[string]string{"X-Api-Key": e2eAPIKey, "Accept": "text/event-stream"})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("stream status = %d, want 200", resp.StatusCode)
	}
	if lines := readAllLines(t, resp); len(lines) == 0 {
		t.Fatal("streamed answer was empty")
	}

	status, _, body := postJSON(t, p.addr, "/v1/messages",
		`{"model":"chat-public","max_tokens":64,"messages":[{"role":"user","content":"ping"}]}`,
		map[string]string{"X-Api-Key": e2eAPIKey})
	if status != http.StatusOK {
		t.Fatalf("buffered status = %d, want 200 (body %s)", status, body)
	}

	reqs := up.requests()
	if len(reqs) != 2 {
		t.Fatalf("upstream hits = %d, want 2", len(reqs))
	}
	assertStreamOptions := func(r recordedRequest, want bool) {
		t.Helper()
		var doc map[string]any
		if err := json.Unmarshal(r.Body, &doc); err != nil {
			t.Fatalf("upstream body is not JSON: %v (%s)", err, r.Body)
		}
		so, present := doc["stream_options"]
		if present != want {
			t.Fatalf("stream_options present = %v, want %v (body %s)", present, want, r.Body)
		}
		if !want {
			return
		}
		opts, _ := so.(map[string]any)
		if opts["include_usage"] != true {
			t.Fatalf("stream_options = %v, want include_usage true", so)
		}
	}
	assertStreamOptions(reqs[0], true)
	assertStreamOptions(reqs[1], false)

	var buffered map[string]any
	if err := json.Unmarshal(reqs[1].Body, &buffered); err != nil {
		t.Fatalf("buffered upstream body is not JSON: %v (%s)", err, reqs[1].Body)
	}
	if stream, _ := buffered["stream"].(bool); stream {
		t.Fatalf("the buffered request declared stream:true upstream: %s", reqs[1].Body)
	}
}

// TestMessagesUpstreamErrorDialect: a received upstream status is relayed
// with its status and re-rendered in the Anthropic envelope, never with the
// provider's own words. A Messages client dispatches on `error.type`, so
// the wrong dialect here is a mis-routed client rather than an ugly one.
func TestMessagesUpstreamErrorDialect(t *testing.T) {
	up := newFakeUpstream(t)
	up.setHandler(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Retry-After", "1")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = io.WriteString(w, `{"error":{"message":"provider says rate limited","type":"rate_limit_error"}}`)
	})
	p := startSubprocess(t, startOpts{
		yaml:     runtimeYAML(chatPublic, up.url()+"/v1", chatUpstream, ""),
		logLevel: "error",
	})

	status, hdr, body := postJSON(t, p.addr, "/v1/messages", messagesBody,
		map[string]string{"X-Api-Key": e2eAPIKey})
	if status != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429 (the provider's status survives normalization)", status)
	}
	want := `{"type":"error","error":{"type":"rate_limit_error","message":"upstream provider returned HTTP 429"}}`
	if string(body) != want {
		t.Fatalf("429 body:\n got %s\nwant %s", body, want)
	}
	if strings.Contains(string(body), "provider says rate limited") {
		t.Fatalf("provider body bytes relayed: %s", body)
	}
	if ra := hdr.Get("Retry-After"); ra != "1" {
		t.Fatalf("Retry-After = %q, want 1 relayed through the allow-list", ra)
	}
	if ct := hdr.Get("Content-Type"); ct != "application/json" {
		t.Fatalf("Content-Type = %q, want application/json", ct)
	}
	if id := hdr.Get("X-Request-Id"); !e2eRequestID.MatchString(id) {
		t.Errorf("X-Request-Id = %q, want 16 hex chars", id)
	}
	// The default policy retries once before giving up, so the upstream saw
	// the exchange more than once — but the CLIENT still received exactly
	// one answer, and it is the provider's own 429 rather than a collapsed
	// 502.
	if n := up.count(); n < 2 {
		t.Fatalf("upstream hits = %d, want the default single retry to re-ask", n)
	}
}

// TestMessagesTransformErrorIsLocal pins the refusal that never falls back:
// a body this translation cannot read fails every candidate equally, so the
// answer is a local 400 in the Messages dialect, the upstream is never
// touched, and the reason the client is told is a static literal rather
// than anything derived from its own bytes.
func TestMessagesTransformErrorIsLocal(t *testing.T) {
	up := newFakeUpstream(t)
	up.setHandler(messagesUpstream(chatUpstream))
	p := startSubprocess(t, startOpts{
		yaml:     runtimeYAML(chatPublic, up.url()+"/v1", chatUpstream, ""),
		logLevel: "error",
	})

	// Valid JSON, routable model, and a "messages" member this translation
	// refuses — the shape Anthropic never sends but a client can.
	status, _, body := messagesRequest(t, p.addr,
		`{"model":"chat-public","messages":"not-an-array"}`,
		map[string]string{"X-Api-Key": e2eAPIKey})
	if status != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 (body %s)", status, body)
	}
	if string(body) != anthropicInvalidReq {
		t.Fatalf("400 body:\n got %s\nwant %s", body, anthropicInvalidReq)
	}
	if n := up.count(); n != 0 {
		t.Fatalf("upstream hits = %d on a local transform failure, want 0", n)
	}
}
