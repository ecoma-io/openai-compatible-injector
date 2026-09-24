package proxy

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"openai-compatible-injector/internal/config"
	"openai-compatible-injector/internal/inject"
)

// stripStore builds a store whose chat-model strips the given fields from
// every response. innerYAML is the literal 4-space-indented strip-fields
// block placed under the model ("" = no block at all, the default-off
// case).
func stripStore(t *testing.T, endpoint, innerYAML string) *config.Store {
	t.Helper()
	yaml := "api-key: " + testAPIKey + `
models:
  strip-model:
    endpoint: ` + endpoint + `
    upstream-model: upstream-name
` + innerYAML
	snap, err := config.LoadRuntime([]byte(yaml))
	if err != nil {
		t.Fatalf("LoadRuntime: %v", err)
	}
	return config.NewStore(snap)
}

// chainStripStore builds a two-candidate chain whose providers each carry
// their own strip list (the model states none, so each hop strips under its
// own provider's list). pa strips provider only, pb strips service_tier
// only.
func chainStripStore(t *testing.T, endpointA, endpointB string) *config.Store {
	t.Helper()
	yaml := "api-key: " + testAPIKey + `
providers:
  pa:
    base-url: ` + endpointA + `
    strip-fields:
      - provider
  pb:
    base-url: ` + endpointB + `
    strip-fields:
      - service_tier
models:
  chain-model:
    providers:
      - provider: pa
        upstream-model: up-a
      - provider: pb
        upstream-model: up-b
`
	snap, err := config.LoadRuntime([]byte(yaml))
	if err != nil {
		t.Fatalf("LoadRuntime: %v", err)
	}
	return config.NewStore(snap)
}

// kiloBefore/kiloAfter are the upstream's and the relayed buffered chat
// shapes. The junk fields differ so a strip that forgot one shows up.
const (
	kiloBefore = `{"id":"chatcmpl-9","model":"upstream-name","provider":"kilo","service_tier":"std","choices":[],"usage":{"prompt_tokens":5}}`
	kiloAfter  = `{"id":"chatcmpl-9","model":"strip-model","choices":[],"usage":{"prompt_tokens":5}}`
)

// sseAwareUpstream answers buffered requests as JSON and stream requests as
// SSE, sniffing the request body for the stream flag — the live shape of a
// real upstream, and the only way one test exercises both relay forms
// through the same endpoint.
func sseAwareUpstream(t *testing.T, payload string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("flush") != "" {
			_ = r.Body.Close()
		}
		body, _ := io.ReadAll(r.Body)
		if strings.Contains(string(body), `"stream":true`) {
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = io.WriteString(w, "data: "+payload+"\n\n")
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, payload)
	}))
}

func TestStripBufferedChat(t *testing.T) {
	up := sseAwareUpstream(t, kiloBefore)
	defer up.Close()
	h := newTestHandler(t, stripStore(t, up.URL, "    strip-fields:\n      - provider\n      - service_tier\n"))
	rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions", `{"model":"strip-model"}`, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body %s)", rec.Code, rec.Body.String())
	}
	if got := rec.Body.String(); got != kiloAfter {
		t.Fatalf("stripped buffered chat body:\n got %s\nwant %s", got, kiloAfter)
	}
}

func TestStripBufferedResponses(t *testing.T) {
	before := `{"type":"response.completed","response":{"id":"r","model":"upstream-name","service_tier":"std","provider":"kilo"},"sequence_number":1}`
	up := sseAwareUpstream(t, before)
	defer up.Close()
	h := newTestHandler(t, stripStore(t, up.URL, "    strip-fields:\n      - provider\n      - service_tier\n"))
	rec := doRequest(t, h, http.MethodPost, "/v1/responses", `{"model":"strip-model"}`, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body %s)", rec.Code, rec.Body.String())
	}
	want := `{"type":"response.completed","response":{"id":"r","model":"strip-model"},"sequence_number":1}`
	if got := rec.Body.String(); got != want {
		t.Fatalf("stripped buffered responses body:\n got %s\nwant %s", got, want)
	}
}

// TestStripStreamChatStripOnlyChunk pins the widened SSE gate end to end: a
// streamed data line that carries ONLY a to-be-excised key — no "model", no
// "usage" — must still reach the strip rewriter and come out stripped.
func TestStripStreamChatStripOnlyChunk(t *testing.T) {
	payload := `{"service_tier":"std","choices":[{"index":0,"delta":{"content":"hi"}}]}`
	up := sseAwareUpstream(t, payload)
	defer up.Close()
	h := newTestHandler(t, stripStore(t, up.URL, "    strip-fields:\n      - service_tier\n"))
	rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions", `{"model":"strip-model","stream":true}`, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body %s)", rec.Code, rec.Body.String())
	}
	payloads := dataPayloads(t, rec.Body.String())
	if len(payloads) != 1 {
		t.Fatalf("data lines = %v, want 1", payloads)
	}
	if got, want := payloads[0], `{"choices":[{"index":0,"delta":{"content":"hi"}}]}`; got != want {
		t.Fatalf("strip-only streamed chunk:\n got %s\nwant %s", got, want)
	}
}

// TestStripStreamParity pins stream/buffered parity under the composed
// rewriter: one closure serves both paths, so a chunk and a body carrying
// the same JSON strip to the same bytes.
func TestStripStreamParity(t *testing.T) {
	payload := `{"model":"upstream-name","provider":"kilo","service_tier":"std","choices":[]}`
	up := sseAwareUpstream(t, payload)
	defer up.Close()
	h := newTestHandler(t, stripStore(t, up.URL, "    strip-fields:\n      - provider\n      - service_tier\n"))

	buffered := doRequest(t, h, http.MethodPost, "/v1/chat/completions", `{"model":"strip-model"}`, nil)
	if buffered.Code != http.StatusOK {
		t.Fatalf("buffered status = %d", buffered.Code)
	}
	streamed := doRequest(t, h, http.MethodPost, "/v1/chat/completions", `{"model":"strip-model","stream":true}`, nil)
	if streamed.Code != http.StatusOK {
		t.Fatalf("streamed status = %d", streamed.Code)
	}
	payloads := dataPayloads(t, streamed.Body.String())
	if len(payloads) != 1 {
		t.Fatalf("stream data lines = %v, want 1", payloads)
	}
	if payloads[0] != buffered.Body.String() {
		t.Fatalf("stream and buffered strips disagree:\n stream   %s\nbuffered %s", payloads[0], buffered.Body.String())
	}
}

// TestStripDefaultOff pins the byte-identity contract: absent strip config
// mutates nothing on either path.
func TestStripDefaultOff(t *testing.T) {
	payload := `{"id":"c","model":"upstream-name","provider":"kilo","service_tier":"std","choices":[]}`
	up := sseAwareUpstream(t, payload)
	defer up.Close()
	h := newTestHandler(t, stripStore(t, up.URL, ""))

	for _, body := range []string{`{"model":"strip-model"}`, `{"model":"strip-model","stream":true}`} {
		rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions", body, nil)
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d (body %s)", rec.Code, rec.Body.String())
		}
		if got := rec.Body.String(); !strings.Contains(got, `"provider":"kilo"`) {
			t.Fatalf("absent strip config removed a field:\n got %s", got)
		}
	}
}

// TestStripModelOverridesProvider pins the precedence: a model-level strip
// list replaces the provider's, whatever the provider says.
func TestStripModelOverridesProvider(t *testing.T) {
	up := sseAwareUpstream(t, kiloBefore)
	defer up.Close()
	// The provider strips service_tier; the model strips provider. The model
	// list wins — the response loses provider, keeps service_tier.
	yaml := "api-key: " + testAPIKey + `
providers:
  kilo:
    base-url: ` + up.URL + `
    transport: t1
    strip-fields:
      - service_tier
transports:
  t1:
    type: direct
models:
  override-model:
    endpoint: ` + up.URL + `
    upstream-model: upstream-name
    strip-fields:
      - provider
`
	snap, err := config.LoadRuntime([]byte(yaml))
	if err != nil {
		t.Fatalf("LoadRuntime: %v", err)
	}
	h := newTestHandler(t, config.NewStore(snap))
	rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions", `{"model":"override-model"}`, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d (body %s)", rec.Code, rec.Body.String())
	}
	want := `{"id":"chatcmpl-9","model":"override-model","service_tier":"std","choices":[],"usage":{"prompt_tokens":5}}`
	if got := rec.Body.String(); got != want {
		t.Fatalf("model override:\n got %s\nwant %s", got, want)
	}
}

// TestStripMeterStaysPreStrip pins the metering seam: the usage event
// records the upstream's own tokens even though the on-wire response no
// longer carries its provider field.
func TestStripMeterStaysPreStrip(t *testing.T) {
	before := `{"model":"upstream-name","provider":"kilo","choices":[],"usage":{"prompt_tokens":10,"completion_tokens":20,"total_tokens":30}}`
	up := sseAwareUpstream(t, before)
	defer up.Close()

	meter := &recordingMeter{}
	h := usageHandler(t, stripStore(t, up.URL, "    strip-fields:\n      - provider\n"), meter)
	rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions", `{"model":"strip-model"}`, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d (body %s)", rec.Code, rec.Body.String())
	}
	if got := rec.Body.String(); strings.Contains(got, `"provider"`) {
		t.Fatalf("stripped body still carries the field: %s", got)
	}
	ev := meter.single(t)
	if ev.PromptTokens == nil || *ev.PromptTokens != 10 ||
		ev.CompletionTokens == nil || *ev.CompletionTokens != 20 ||
		ev.TotalTokens == nil || *ev.TotalTokens != 30 {
		t.Fatalf("metered tokens = %v/%v/%v, want 10/20/30 (strip must never be metered)", ev.PromptTokens, ev.CompletionTokens, ev.TotalTokens)
	}
}

// TestStripChainedCandidatesDifferentStrips pins per-candidate provider
// lists: the model states no model-level list, so the relayed candidate
// answers under its own provider's list. pa strips provider; pb strips
// service_tier — the chain's first answer strips pa's key and leaves pb's.
func TestStripChainedCandidatesDifferentStrips(t *testing.T) {
	pa := &fakeUpstream{status: http.StatusOK, body: `{"id":"a","model":"up-a","provider":"pa","service_tier":"x","choices":[]}`}
	pb := &fakeUpstream{status: http.StatusOK, body: `{"id":"b","model":"up-b","provider":"pb","service_tier":"y","choices":[]}`}
	store := chainStripStore(t, "https://a.example/v1", "https://b.example/v1")
	h := NewHandler(store, kindResolver{direct: pa, proxied: pb}, nil, nil, quietLogger())

	rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions", `{"model":"chain-model"}`, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d (body %s)", rec.Code, rec.Body.String())
	}
	got := rec.Body.String()
	if strings.Contains(got, `"provider"`) {
		t.Fatalf("pa's provider field survived: %s", got)
	}
	if !strings.Contains(got, `"service_tier":"x"`) {
		t.Fatalf("pa's service_tier should survive (not in pa's list): %s", got)
	}
	if !strings.Contains(got, `"model":"chain-model"`) {
		t.Fatalf("model not rewritten to public name: %s", got)
	}
}

// TestStripSSEGateUnit pins the widened gate on rewriteSSELine itself: a
// payload that mentions a strip key but neither "model" nor "usage" reaches
// the rewriter, and a nil/no-match key list keeps the line byte-identical.
func TestStripSSEGateUnit(t *testing.T) {
	stripOnly := func(p []byte) []byte { return inject.StripChatFields(p, [][]string{{"service_tier"}}) }

	t.Run("strip key admits the line", func(t *testing.T) {
		line := []byte("data: {\"service_tier\":\"std\",\"choices\":[]}\n")
		out := rewriteSSELine(line, stripOnly, [][]byte{[]byte(`"service_tier"`)})
		if got, want := string(out), "data: {\"choices\":[]}\n"; got != want {
			t.Fatalf("strip-key gate:\n got %s\nwant %s", got, want)
		}
	})

	t.Run("nil patterns keep the line byte-identical", func(t *testing.T) {
		line := []byte("data: {\"service_tier\":\"std\"}\n")
		out := rewriteSSELine(line, stripOnly, nil)
		if string(out) != string(line) {
			t.Fatalf("nil gate must keep the line byte-identical:\n got %s\nwant %s", out, line)
		}
		if &out[0] != &line[0] {
			t.Fatalf("nil gate must return the same backing slice")
		}
	})

	t.Run("no-match patterns keep the line byte-identical", func(t *testing.T) {
		line := []byte("data: {\"service_tier\":\"std\"}\n")
		out := rewriteSSELine(line, stripOnly, [][]byte{[]byte(`"other"`)})
		if string(out) != string(line) {
			t.Fatalf("no-match gate must keep the line byte-identical:\n got %s\nwant %s", out, line)
		}
	})
}
