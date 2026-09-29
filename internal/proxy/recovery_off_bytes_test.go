package proxy

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"openai-compatible-injector/internal/config"
)

// The compatibility hinge is exact: with `recovery.stream` absent — or present
// with `enabled: false` — the relay must be byte-identical to an unconfigured
// deployment. No accumulator, no hop, no watchdog, no extra heartbeat, no
// response-byte change, no extra log event.
//
// TestStreamRecoveryOffByDefault (absent, chat SSE cut) and
// TestStreamRecoveryWindowIsNotArmedWhenDisabled (disabled with a 150ms
// `max-elapsed` and an upstream that pauses 450ms — proof the disabled path
// arms no timer, since a healthy stream that outlives the configured window is
// relayed in full) already cover the behavioural half. This file pins the
// BYTE-LEVEL half for all four surface/transport combinations, and compares
// the absent and disabled configurations against each other.
//
// The fixtures are built so a byte-equality assertion is meaningful rather
// than trivially true: unusual key order and spacing everywhere, a unicode
// escape the rewrite must not decode, a `usage` object, a `model` value the
// rewrite MUST touch — and a decoy string value that quotes the upstream id,
// so a naive global replacement or a re-serialization of the document shows up
// as a diff.

// offFeatureStore builds a store with the given recovery block ("" = none),
// the keep-alive heartbeat OFF (so "no extra bytes" is deterministic rather
// than a race against a 15s ticker), and two models over one endpoint.
func offFeatureStore(t *testing.T, block string) *config.Store {
	t.Helper()
	yaml := "api-key: " + testAPIKey + "\n" + block + `sse-keep-alive:
  enabled: false
models:
  test-model:
    endpoint: https://up.example/v1
    upstream-model: up-chat
    injection-prompt: ""
  resp-model:
    endpoint: https://up.example/v1
    upstream-model: up-resp
    injection-prompt: ""
`
	snap, err := config.LoadRuntime([]byte(yaml))
	if err != nil {
		t.Fatalf("LoadRuntime: %v", err)
	}
	return config.NewStore(snap)
}

// offFeatureHandler wires an off-feature store to a scripted upstream and a
// captured log.
func offFeatureHandler(t *testing.T, block string, doer *scriptedDoer) (http.Handler, *logBuffer) {
	t.Helper()
	logBuf, log := captureLog(zerolog.DebugLevel)
	return NewHandler(offFeatureStore(t, block), kindResolver{direct: doer, proxied: doer}, nil, nil, nil, log), logBuf
}

// The four fixtures and their exact expected relays. `want` is the fixture with
// EXACTLY one span rewritten — the top-level `model` string value (plus, for
// the Responses envelope events, the `model` inside the top-level `response`
// object). The decoy `up-*` occurrences elsewhere are expected to survive
// byte-for-byte.

const (
	chatBufferedFixture = `{ "id":"chatcmpl-1" , "model" : "up-chat" , "note" : "up-chat is the upstream id" , "usage" : {"prompt_tokens":7,"completion_tokens":3,"total_tokens":10} , "choices" : [ {"index":0,"message":{"role":"assistant","content":"héllo é"}} ] , "created" : 1700000000 }`
	chatBufferedWant    = `{ "id":"chatcmpl-1" , "model" : "test-model" , "note" : "up-chat is the upstream id" , "usage" : {"prompt_tokens":7,"completion_tokens":3,"total_tokens":10} , "choices" : [ {"index":0,"message":{"role":"assistant","content":"héllo é"}} ] , "created" : 1700000000 }`

	chatSSEFixture = "data: {\"model\":\"up-chat\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"h\\u00e9llo\"}}]}\n\n" +
		"data: {\"model\":\"up-chat\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\" world\"}}]}\n\n" +
		"data: {\"model\":\"up-chat\",\"note\":\"up-chat is the upstream id\",\"choices\":[],\"usage\":{\"prompt_tokens\":7,\"completion_tokens\":3,\"total_tokens\":10}}\n\n" +
		"data: [DONE]\n\n"
	chatSSEWant = "data: {\"model\":\"test-model\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"h\\u00e9llo\"}}]}\n\n" +
		"data: {\"model\":\"test-model\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\" world\"}}]}\n\n" +
		"data: {\"model\":\"test-model\",\"note\":\"up-chat is the upstream id\",\"choices\":[],\"usage\":{\"prompt_tokens\":7,\"completion_tokens\":3,\"total_tokens\":10}}\n\n" +
		"data: [DONE]\n\n"

	responsesBufferedFixture = `{ "id":"resp_1" , "object":"response" , "model" : "up-resp" , "note" : "up-resp is the upstream id" , "output" : [ {"type":"message","content":[{"type":"output_text","text":"hé"}]} ] , "usage" : {"input_tokens":5,"output_tokens":2,"total_tokens":7} , "status" : "completed" }`
	responsesBufferedWant    = `{ "id":"resp_1" , "object":"response" , "model" : "resp-model" , "note" : "up-resp is the upstream id" , "output" : [ {"type":"message","content":[{"type":"output_text","text":"hé"}]} ] , "usage" : {"input_tokens":5,"output_tokens":2,"total_tokens":7} , "status" : "completed" }`

	responsesSSEFixture = "event: response.created\n" +
		"data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_1\",\"model\":\"up-resp\"}}\n\n" +
		"event: response.output_text.delta\n" +
		"data: {\"type\":\"response.output_text.delta\",\"item_id\":\"m1\",\"output_index\":0,\"content_index\":0,\"delta\":\"h\\u00e9\"}\n\n" +
		"event: response.completed\n" +
		"data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_1\",\"model\":\"up-resp\",\"note\":\"up-resp is the upstream id\",\"usage\":{\"input_tokens\":5,\"output_tokens\":2,\"total_tokens\":7}}}\n\n"
	responsesSSEWant = "event: response.created\n" +
		"data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_1\",\"model\":\"resp-model\"}}\n\n" +
		"event: response.output_text.delta\n" +
		"data: {\"type\":\"response.output_text.delta\",\"item_id\":\"m1\",\"output_index\":0,\"content_index\":0,\"delta\":\"h\\u00e9\"}\n\n" +
		"event: response.completed\n" +
		"data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_1\",\"model\":\"resp-model\",\"note\":\"up-resp is the upstream id\",\"usage\":{\"input_tokens\":5,\"output_tokens\":2,\"total_tokens\":7}}}\n\n"
)

// jsonUpstream answers with one canned JSON body — the buffered relay shape.
func jsonUpstream(body string) dialFunc {
	return func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body:       io.NopCloser(strings.NewReader(body)),
		}, nil
	}
}

// sseUpstream answers with one canned SSE body — the streamed relay shape.
func sseUpstream(body string) dialFunc {
	return func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
			Body:       io.NopCloser(strings.NewReader(body)),
		}, nil
	}
}

// assertNoRecoveryEvents pins the quiet direction of the feature-off hinge at
// the log level: no recovery machinery may announce itself.
func assertNoRecoveryEvents(t *testing.T, logBuf *logBuffer) {
	t.Helper()
	for _, slug := range []string{
		"stream_recovery_started", "stream_recovery_failed",
		"stream_recovery_succeeded", "stream_recovery_exhausted",
	} {
		if ev := logBuf.events(t, slug); len(ev) != 0 {
			t.Errorf("%s fired with stream recovery off: %v", slug, ev)
		}
	}
}

// TestRecoveryStreamOffRelaysBytesUnchanged is the byte-level compatibility
// hinge for all four combinations. It asserts three things per combination:
// the relayed bytes are exactly the fixture with only the model renamed; the
// `enabled: false` configuration produces the SAME bytes as the absent one;
// and no recovery event fires under either.
func TestRecoveryStreamOffRelaysBytesUnchanged(t *testing.T) {
	cases := []struct {
		name    string
		path    string
		request string
		answer  dialFunc
		want    string
		stream  bool
	}{
		{
			name:    "chat buffered",
			path:    "/v1/chat/completions",
			request: `{"model":"test-model","messages":[{"role":"user","content":"hi"}]}`,
			answer:  jsonUpstream(chatBufferedFixture),
			want:    chatBufferedWant,
		},
		{
			name:    "chat sse",
			path:    "/v1/chat/completions",
			request: `{"model":"test-model","stream":true,"messages":[{"role":"user","content":"hi"}]}`,
			answer:  sseUpstream(chatSSEFixture),
			want:    chatSSEWant,
			stream:  true,
		},
		{
			name:    "responses buffered",
			path:    "/v1/responses",
			request: `{"model":"resp-model","input":"hi"}`,
			answer:  jsonUpstream(responsesBufferedFixture),
			want:    responsesBufferedWant,
		},
		{
			name:    "responses sse",
			path:    "/v1/responses",
			request: `{"model":"resp-model","stream":true,"input":"hi"}`,
			answer:  sseUpstream(responsesSSEFixture),
			want:    responsesSSEWant,
			stream:  true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var outs []string
			for _, block := range []string{"", recoveryBlock(t, "    enabled: false\n")} {
				doer := &scriptedDoer{script: []dialFunc{tc.answer}}
				h, logBuf := offFeatureHandler(t, block, doer)

				rec := doRequest(t, h, http.MethodPost, tc.path, tc.request, nil)
				if rec.Code != http.StatusOK {
					t.Fatalf("status = %d, want 200 (body %q)", rec.Code, rec.Body.String())
				}
				got := rec.Body.String()
				if got != tc.want {
					t.Fatalf("relayed bytes differ (block %q):\n got  %q\n want %q", block, got, tc.want)
				}
				// No extra bytes anywhere: the fixture is not a prefix or
				// suffix of the answer, and nothing (a synthesized marker, a
				// keep-alive ping, a trailing newline) was appended.
				if strings.Contains(got, ": ping") {
					t.Fatalf("a keep-alive ping appeared with the heartbeat unconfigured: %q", got)
				}
				if len(got) != len(tc.want) {
					t.Fatalf("byte count = %d, want %d", len(got), len(tc.want))
				}
				if doer.dials() != 1 {
					t.Fatalf("dials = %d, want exactly one upstream call", doer.dials())
				}
				assertNoRecoveryEvents(t, logBuf)
				if tc.stream {
					// The completion line reports zero recoveries under both
					// configurations — the loop never ran.
					evs := logBuf.events(t, "stream_completed")
					if len(evs) != 1 {
						t.Fatalf("stream_completed = %v, want one", evs)
					}
					if got := evs[0]["stream_recoveries"]; got != float64(0) {
						t.Errorf("stream_recoveries = %v, want 0", got)
					}
					if _, ok := evs[0]["recovery_reason"]; ok {
						t.Errorf("recovery_reason set with the feature off: %v", evs[0])
					}
				}
				outs = append(outs, got)
			}
			if outs[0] != outs[1] {
				t.Fatalf("the disabled configuration changed the bytes:\n absent  %q\n disabled %q", outs[0], outs[1])
			}
		})
	}
}

// TestRecoveryStreamOffRelaysACutStreamUnchanged extends the cut-stream
// coverage of TestStreamRecoveryOffByDefault to the `enabled: false`
// configuration and to byte equality: a generation cut mid-flight is relayed
// exactly as it arrived, reported truncated with no `recovery_reason`, and
// nothing is appended to the client's stream — not even the terminal marker
// the client would have wanted. The timer question is covered by
// TestStreamRecoveryWindowIsNotArmedWhenDisabled, which is the cheap
// construction: a 150ms configured window and an upstream that pauses 450ms
// cannot both hold unless no watchdog was armed.
func TestRecoveryStreamOffRelaysACutStreamUnchanged(t *testing.T) {
	cut := "data: {\"model\":\"up-chat\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"Hello\"}}]}\n\n"
	want := "data: {\"model\":\"test-model\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"Hello\"}}]}\n\n"

	for _, block := range []string{"", recoveryBlock(t, "    enabled: false\n    max-elapsed: 30s\n")} {
		doer := &scriptedDoer{script: []dialFunc{sseCut(cut)}}
		h, logBuf := offFeatureHandler(t, block, doer)

		rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions",
			`{"model":"test-model","stream":true,"messages":[{"role":"user","content":"hi"}]}`, nil)
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want the committed 200", rec.Code)
		}
		if got := rec.Body.String(); got != want {
			t.Fatalf("relayed bytes differ (block %q):\n got  %q\n want %q", block, got, want)
		}
		if doer.dials() != 1 {
			t.Fatalf("dials = %d, want the primary once and no continuation", doer.dials())
		}
		assertNoRecoveryEvents(t, logBuf)

		trunc := logBuf.events(t, "stream_truncated")
		if len(trunc) != 1 || trunc[0]["phase"] != "upstream_read" {
			t.Fatalf("stream_truncated = %v, want one upstream_read truncation", trunc)
		}
		if _, ok := trunc[0]["recovery_reason"]; ok {
			t.Errorf("recovery_reason set with the feature off: %v", trunc[0])
		}
	}
}

// TestRecoveryStreamDisabledConfigIsWellFormed is a guard on the fixtures
// above: the `enabled: false` block must actually resolve as a disabled stream
// policy (not be silently dropped or rejected), so the "same bytes" assertion
// compares two live configurations rather than one configuration with itself.
func TestRecoveryStreamDisabledConfigIsWellFormed(t *testing.T) {
	store := offFeatureStore(t, recoveryBlock(t, "    enabled: false\n    max-elapsed: 30s\n"))
	snap := store.Load()
	if snap == nil {
		t.Fatal("store returned no snapshot")
	}
	model, ok := snap.Model("test-model")
	if !ok {
		t.Fatal("test-model missing from the snapshot")
	}
	if len(model.Chain) != 1 {
		t.Fatalf("chain = %d candidates, want 1", len(model.Chain))
	}
	sp := model.Chain[0].Recovery.Stream
	if sp.Enabled {
		t.Fatal("enabled: false resolved to an enabled stream policy")
	}
	if sp.MaxElapsed != 30*time.Second {
		t.Fatalf("max-elapsed = %v, want the configured 30s carried on the disabled policy", sp.MaxElapsed)
	}
}

// TestRecoveryStreamOffLeavesTheDefaultHeartbeatSilent pins the "no extra
// heartbeat" half with the keep-alive at its DEFAULT — enabled, 15s — rather
// than explicitly disabled: a relay that finishes in milliseconds must put no
// ping on the wire, and the completion line must report zero pings. A
// regression that armed the heartbeat early, or that made recovery start its
// own ticker, would show up here rather than in a 15-second test.
func TestRecoveryStreamOffLeavesTheDefaultHeartbeatSilent(t *testing.T) {
	snap, err := config.LoadRuntime([]byte("api-key: " + testAPIKey + "\n" +
		recoveryBlock(t, "    enabled: false\n") + `models:
  test-model:
    endpoint: https://up.example/v1
    upstream-model: up-chat
    injection-prompt: ""
`))
	if err != nil {
		t.Fatalf("LoadRuntime: %v", err)
	}
	doer := &scriptedDoer{script: []dialFunc{sseUpstream(chatSSEFixture)}}
	logBuf, log := captureLog(zerolog.DebugLevel)
	h := NewHandler(config.NewStore(snap), kindResolver{direct: doer, proxied: doer}, nil, nil, nil, log)

	rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions",
		`{"model":"test-model","stream":true,"messages":[{"role":"user","content":"hi"}]}`, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if got := rec.Body.String(); got != chatSSEWant {
		t.Fatalf("relayed bytes differ:\n got  %q\n want %q", got, chatSSEWant)
	}
	if strings.Contains(rec.Body.String(), ": ping") {
		t.Fatalf("a keep-alive ping appeared on a stream that finished immediately: %q", rec.Body.String())
	}
	evs := logBuf.events(t, "stream_completed")
	if len(evs) != 1 {
		t.Fatalf("stream_completed = %v, want one", evs)
	}
	if got := evs[0]["keep_alive_pings"]; got != float64(0) {
		t.Errorf("keep_alive_pings = %v, want 0", got)
	}
	assertNoRecoveryEvents(t, logBuf)
}

// TestRecoveryStreamOffKeepsTheBufferedJSONShape pins the other quiet
// direction of "no extra bytes": a buffered answer is relayed with no
// structural change at all — the document still parses, with the same keys in
// the same order and the same values. A re-serialization would pass a naive
// substring check but fail this.
func TestRecoveryStreamOffKeepsTheBufferedJSONShape(t *testing.T) {
	doer := &scriptedDoer{script: []dialFunc{jsonUpstream(chatBufferedFixture)}}
	h, logBuf := offFeatureHandler(t, recoveryBlock(t, "    enabled: false\n"), doer)

	rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions",
		`{"model":"test-model","messages":[{"role":"user","content":"hi"}]}`, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if !json.Valid(rec.Body.Bytes()) {
		t.Fatalf("relayed body is not valid JSON: %q", rec.Body.String())
	}
	// The key ORDER is the fixture's, not a map's — Go's encoder sorts keys,
	// so a re-serialization would move `choices` before `id`.
	if got := rec.Body.String(); !strings.HasPrefix(got, `{ "id":"chatcmpl-1" , "model" : "test-model" , "note"`) {
		t.Fatalf("key order or spacing changed: %q", got)
	}
	assertNoRecoveryEvents(t, logBuf)
}
