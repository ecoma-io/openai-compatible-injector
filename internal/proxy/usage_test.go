package proxy

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/rs/zerolog"

	"openai-compatible-injector/internal/auth"
	"openai-compatible-injector/internal/config"
	"openai-compatible-injector/internal/inject"
	"openai-compatible-injector/internal/transport"
	"openai-compatible-injector/internal/usage"
)

// recordingMeter captures every event the handler records.
type recordingMeter struct {
	mu     sync.Mutex
	events []usage.Event
}

func (m *recordingMeter) Record(ev usage.Event) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.events = append(m.events, ev)
}

func (m *recordingMeter) recorded() []usage.Event {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]usage.Event(nil), m.events...)
}

func (m *recordingMeter) single(t *testing.T) usage.Event {
	t.Helper()
	evs := m.recorded()
	if len(evs) != 1 {
		t.Fatalf("recorded %d events, want exactly 1", len(evs))
	}
	return evs[0]
}

func quietLogger() zerolog.Logger { return zerolog.Nop() }

func usageHandler(t *testing.T, store *config.Store, meter *recordingMeter) http.Handler {
	t.Helper()
	return NewHandler(store, directResolver(), nil, meter, quietLogger())
}

// The buffered chat path meters the upstream's own usage — even when the
// thinking-usage synthesizer is enriching what the client sees.

func TestUsageChatBufferedUpstreamTokensOnly(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"1","model":"upstream-name","usage":{"prompt_tokens":10,"completion_tokens":20,"total_tokens":30}}`))
	}))
	defer up.Close()

	meter := &recordingMeter{}
	// mode always + a pinned 0.5 share: the client's completion tokens get
	// a reasoning_tokens splice the upstream never reported.
	h := usageHandler(t, thinkingStore(t, up.URL+"/v1", "always", "0.5", "0.5"), meter)
	withDraw(t, func() float64 { return 0.5 })

	body := `{"model":"test-model","messages":[{"role":"user","content":"hi"}]}`
	rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions", body, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	// The synthesis actually fired for the client...
	if !strings.Contains(rec.Body.String(), `"reasoning_tokens":10`) {
		t.Fatalf("client body lacks the synthesized reasoning_tokens: %s", rec.Body.String())
	}
	// ...but the meter saw only the upstream's own numbers.
	ev := meter.single(t)
	if ev.PromptTokens == nil || *ev.PromptTokens != 10 ||
		ev.CompletionTokens == nil || *ev.CompletionTokens != 20 ||
		ev.TotalTokens == nil || *ev.TotalTokens != 30 {
		t.Fatalf("metered tokens = %v/%v/%v, want 10/20/30 (synthesis must never be metered)", ev.PromptTokens, ev.CompletionTokens, ev.TotalTokens)
	}
	if ev.API != "chat" || ev.Stream || ev.HTTPStatus != 200 || ev.Outcome != "completed" ||
		ev.PublicModel != "test-model" || ev.UpstreamModel != "upstream-name" {
		t.Fatalf("event facts = %+v", ev)
	}
	if ev.EventID == "" || ev.RequestID == "" || ev.OccurredAt.IsZero() {
		t.Fatalf("event identity = %+v", ev)
	}
	if ev.PartnerID != "" || ev.KeyID != "" {
		t.Fatalf("static mode carries no identity: %+v", ev)
	}
}

// Streaming: cumulative usage chunks converge on the final object — one
// event, last wins, never the sum.

func TestUsageChatStreamingLastWins(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: {\"model\":\"upstream-name\",\"choices\":[{\"delta\":{\"content\":\"a\"}}],\"usage\":{\"prompt_tokens\":10,\"completion_tokens\":20,\"total_tokens\":30}}\n\n" +
			"data: {\"model\":\"upstream-name\",\"choices\":[{\"delta\":{\"content\":\"b\"}}],\"usage\":{\"prompt_tokens\":15,\"completion_tokens\":25,\"total_tokens\":40}}\n\n" +
			"data: [DONE]\n\n"))
	}))
	defer up.Close()

	meter := &recordingMeter{}
	h := usageHandler(t, newTestStore(t, up.URL+"/v1"), meter)

	body := `{"model":"test-model","stream":true,"messages":[{"role":"user","content":"hi"}]}`
	rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions", body, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	ev := meter.single(t)
	if !ev.Stream {
		t.Fatal("event lost the stream flag")
	}
	if ev.PromptTokens == nil || *ev.PromptTokens != 15 ||
		ev.CompletionTokens == nil || *ev.CompletionTokens != 25 ||
		ev.TotalTokens == nil || *ev.TotalTokens != 40 {
		t.Fatalf("metered tokens = %v/%v/%v, want the final object 15/25/40, never the sum", ev.PromptTokens, ev.CompletionTokens, ev.TotalTokens)
	}
}

// The Responses surface meters its own usage shape, out of the streaming
// envelope.

func TestUsageResponsesStreamingEnvelope(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("event: response.created\n" +
			"data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_1\"}}\n\n" +
			"event: response.completed\n" +
			"data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_1\",\"usage\":{\"input_tokens\":7,\"output_tokens\":8,\"total_tokens\":15}}}\n\n"))
	}))
	defer up.Close()

	meter := &recordingMeter{}
	h := usageHandler(t, newTestStore(t, up.URL+"/v1"), meter)

	body := `{"model":"test-model","stream":true,"input":"hi"}`
	rec := doRequest(t, h, http.MethodPost, "/v1/responses", body, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	ev := meter.single(t)
	if ev.API != "responses" {
		t.Fatalf("api = %q, want responses", ev.API)
	}
	if ev.PromptTokens == nil || *ev.PromptTokens != 7 ||
		ev.CompletionTokens == nil || *ev.CompletionTokens != 8 ||
		ev.TotalTokens == nil || *ev.TotalTokens != 15 {
		t.Fatalf("metered tokens = %v/%v/%v, want 7/8/15", ev.PromptTokens, ev.CompletionTokens, ev.TotalTokens)
	}
}

// Absent usage stays NULL — never a fabricated zero.

func TestUsageAbsentStaysNull(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"1","model":"upstream-name","choices":[{"message":{"role":"assistant","content":"hello"}}]}`))
	}))
	defer up.Close()

	meter := &recordingMeter{}
	h := usageHandler(t, newTestStore(t, up.URL+"/v1"), meter)
	body := `{"model":"test-model","messages":[{"role":"user","content":"hi"}]}`
	if rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions", body, nil); rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	ev := meter.single(t)
	if ev.PromptTokens != nil || ev.CompletionTokens != nil || ev.TotalTokens != nil {
		t.Fatalf("absent usage metered as %v/%v/%v, want nils", ev.PromptTokens, ev.CompletionTokens, ev.TotalTokens)
	}
}

// Upstream HTTP errors are metered as facts: the relayed status, the
// normalized outcome, and no tokens.

func TestUsageUpstreamHTTPErrorMetered(t *testing.T) {
	stubRetryTiming(t)
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`{"error":{"message":"slow down"}}`))
	}))
	defer up.Close()

	meter := &recordingMeter{}
	h := usageHandler(t, newTestStore(t, up.URL+"/v1"), meter)
	body := `{"model":"test-model","messages":[{"role":"user","content":"hi"}]}`
	rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions", body, nil)
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429", rec.Code)
	}
	ev := meter.single(t)
	if ev.HTTPStatus != 429 || ev.Outcome != "upstream_http_error" {
		t.Fatalf("status/outcome = %d/%q, want 429/upstream_http_error", ev.HTTPStatus, ev.Outcome)
	}
	if ev.PromptTokens != nil || ev.CompletionTokens != nil || ev.TotalTokens != nil {
		t.Fatal("an upstream error answer carried tokens")
	}
	// An inline-endpoint model labels its provider by the endpoint origin.
	if ev.Provider != up.URL {
		t.Fatalf("provider label = %q, want endpoint origin %q", ev.Provider, up.URL)
	}
}

// The fallback walk meters ONE event for the whole chain: the answering
// candidate's identity and the walk's counters — provider attempts and
// egress dials summed across candidates, with the final candidate's egress
// mode.

func TestUsageFallbackChainFacts(t *testing.T) {
	store := newChainStore(t, "")
	pa := &fakeUpstream{err: dialError("a.example")}
	pb := &fakeUpstream{status: http.StatusOK, body: `{"model":"up-b","usage":{"prompt_tokens":3,"completion_tokens":4,"total_tokens":7}}`}
	meter := &recordingMeter{}
	h := NewHandler(store, kindResolver{direct: pa, proxied: pb}, nil, meter, quietLogger())

	if rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions", chainChatBody, nil); rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	ev := meter.single(t)
	if ev.Provider != "pb" || ev.UpstreamModel != "up-b" {
		t.Fatalf("provider/upstream = %q/%q, want the answering candidate pb/up-b", ev.Provider, ev.UpstreamModel)
	}
	if ev.ProviderAttempts != 2 {
		t.Fatalf("provider_attempts = %d, want 2 (both candidates walked)", ev.ProviderAttempts)
	}
	// One single-endpoint dial per candidate, summed across the walk; kind
	// is the final candidate's mode.
	if ev.EgressAttempts != 2 || ev.EgressKind != "direct" {
		t.Fatalf("egress = %d/%q, want 2 dials with the final candidate's direct kind", ev.EgressAttempts, ev.EgressKind)
	}
	if ev.PromptTokens == nil || *ev.PromptTokens != 3 ||
		ev.CompletionTokens == nil || *ev.CompletionTokens != 4 ||
		ev.TotalTokens == nil || *ev.TotalTokens != 7 {
		t.Fatalf("metered tokens = %v/%v/%v, want only the answerer's 3/4/7", ev.PromptTokens, ev.CompletionTokens, ev.TotalTokens)
	}
	if ev.Outcome != "completed" || ev.HTTPStatus != http.StatusOK {
		t.Fatalf("outcome/status = %q/%d", ev.Outcome, ev.HTTPStatus)
	}
}

// Retries meter the same one event with the exchange counters: each
// same-candidate retry is a provider attempt and one egress dial, so the
// walk's counters sum them, while the answering candidate's identity and
// usage are the only facts on the event.

func TestUsageRetryChainFacts(t *testing.T) {
	stubRetryTiming(t)
	store := newChainStore(t, "")
	pa := newScript(scriptStep{status: http.StatusTooManyRequests, body: `{"error":{"message":"rate limited"}}`})
	pb := newScript(scriptStep{status: http.StatusOK, body: `{"model":"up-b","usage":{"prompt_tokens":3,"completion_tokens":4,"total_tokens":7}}`})
	meter := &recordingMeter{}
	h := NewHandler(store, kindResolver{direct: pa, proxied: pb}, nil, meter, quietLogger())

	if rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions", chainChatBody, nil); rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	ev := meter.single(t)
	if ev.Provider != "pb" || ev.UpstreamModel != "up-b" {
		t.Fatalf("provider/upstream = %q/%q, want the answering candidate pb/up-b", ev.Provider, ev.UpstreamModel)
	}
	// pa's initial attempt plus its one default retry, then pb: three
	// exchanges, three dials.
	if ev.ProviderAttempts != 3 || ev.EgressAttempts != 3 {
		t.Fatalf("attempts = %d/%d, want 3 exchanges / 3 dials", ev.ProviderAttempts, ev.EgressAttempts)
	}
	if ev.PromptTokens == nil || *ev.PromptTokens != 3 ||
		ev.CompletionTokens == nil || *ev.CompletionTokens != 4 ||
		ev.TotalTokens == nil || *ev.TotalTokens != 7 {
		t.Fatalf("metered tokens = %v/%v/%v, want only the answerer's 3/4/7", ev.PromptTokens, ev.CompletionTokens, ev.TotalTokens)
	}
	if ev.Outcome != "completed" || ev.HTTPStatus != http.StatusOK {
		t.Fatalf("outcome/status = %q/%d", ev.Outcome, ev.HTTPStatus)
	}
}

// statusExecutor is a pool-seam stand-in that answers every exchange with
// one fixed HTTP status — the answer-shaped result a retained-answer test
// needs from the Execute branch. Do panics: a pool candidate must never
// fall back to it.
//
// It claims the request's exchange envelope the way a real pool does — one
// unit per dial, immediately before the dial — so the handler's exchange
// accounting sees the traffic this stand-in actually stands for.
type statusExecutor struct {
	status   int
	info     transport.AttemptInfo
	doCalled bool
}

func (e *statusExecutor) Execute(ar *transport.AttemptRequest) (*http.Response, transport.AttemptInfo, error) {
	if ar.Budget != nil && !ar.Budget.ConsumeExchange() {
		return nil, transport.AttemptInfo{BudgetExhausted: true}, errors.New("exchange budget exhausted")
	}
	return &http.Response{
		StatusCode: e.status,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader(`{"error":{"message":"rate limited"}}`)),
	}, e.info, nil
}

func (e *statusExecutor) Do(*http.Request) (*http.Response, error) {
	e.doCalled = true
	panic("handler called Do on an Executor-capable doer")
}

// A retained answer's usage record names the RELAYED candidate's egress, not
// the last failed one's: pa answers 429 through a POOL (Executor seam), its
// budget runs out, pb then fails to dial at all — the adopted answer came
// from the pool, so EgressKind is the pool member's kind, never pb's direct.
func TestUsageRetainedAnswerNamesOwnEgress(t *testing.T) {
	stubRetryTiming(t)
	snap, err := config.LoadRuntime([]byte("api-key: " + testAPIKey + `
transports:
  t1:
    type: direct
  t2:
    type: proxy
    proxy: http://127.0.0.1:9090
  pool-egress:
    type: pool
    members: [t1]
providers:
  pa:
    base-url: https://a.example/v1
    transport: pool-egress
  pb:
    base-url: https://b.example/v1
    transport: t2
models:
  chain-model:
    providers:
      - provider: pa
        upstream-model: up-a
      - provider: pb
        upstream-model: up-b
`))
	if err != nil {
		t.Fatalf("LoadRuntime: %v", err)
	}
	store := config.NewStore(snap)

	px := &statusExecutor{
		status: http.StatusTooManyRequests,
		info:   transport.AttemptInfo{Attempts: 1, Kind: "proxy", Target: "http://127.0.0.1:9090"},
	}
	pb := &fakeUpstream{err: dialError("b.example")}
	meter := &recordingMeter{}
	h := NewHandler(store, &kindDoerResolver{pool: px, direct: pb}, nil, meter, quietLogger())

	rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions", chainChatBody, nil)
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want the retained 429", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), `"code":"upstream_http_429"`) {
		t.Fatalf("body = %s, want the canonical envelope", rec.Body.String())
	}
	if px.doCalled {
		t.Fatal("handler fell back to Do on the pool candidate")
	}

	ev := meter.single(t)
	if ev.Provider != "pa" || ev.UpstreamModel != "up-a" {
		t.Fatalf("provider/upstream = %q/%q, want the answering candidate pa/up-a", ev.Provider, ev.UpstreamModel)
	}
	// pa's initial attempt plus its one default retry, then pb's failed dial:
	// three exchanges, three dials.
	if ev.ProviderAttempts != 3 || ev.EgressAttempts != 3 {
		t.Fatalf("attempts = %d/%d, want 3 exchanges / 3 dials", ev.ProviderAttempts, ev.EgressAttempts)
	}
	if ev.EgressKind != "proxy" {
		t.Fatalf("egress kind = %q, want the retained candidate's pool member kind %q", ev.EgressKind, "proxy")
	}
	if ev.Outcome != "upstream_http_error" || ev.HTTPStatus != http.StatusTooManyRequests {
		t.Fatalf("outcome/status = %q/%d, want upstream_http_error/429", ev.Outcome, ev.HTTPStatus)
	}
}

// Exhaustion meters the LAST attempted candidate with the full attempt
// counters and no tokens: no answer arrived, so there is no usage — and
// still exactly one event, never one per failed attempt.

func TestUsageExhaustionFacts(t *testing.T) {
	store := newChainStore(t, "")
	pa := &fakeUpstream{err: dialError("a.example")}
	pb := &fakeUpstream{err: dialError("b.example")}
	meter := &recordingMeter{}
	h := NewHandler(store, kindResolver{direct: pa, proxied: pb}, nil, meter, quietLogger())

	if rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions", chainChatBody, nil); rec.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502", rec.Code)
	}
	ev := meter.single(t)
	if ev.Provider != "pb" || ev.UpstreamModel != "up-b" || ev.ProviderAttempts != 2 || ev.EgressAttempts != 2 {
		t.Fatalf("exhaustion facts = provider %q/%q attempts %d/%d, want pb/up-b 2/2",
			ev.Provider, ev.UpstreamModel, ev.ProviderAttempts, ev.EgressAttempts)
	}
	if ev.PromptTokens != nil || ev.CompletionTokens != nil || ev.TotalTokens != nil {
		t.Fatal("an exhausted walk carried usage tokens")
	}
	if ev.Outcome != "upstream_unreachable" || ev.HTTPStatus != http.StatusBadGateway {
		t.Fatalf("outcome/status = %q/%d", ev.Outcome, ev.HTTPStatus)
	}
}

// Local rejections before model resolution are not metered at all.

func TestUsagePreResolutionRejectionsNotMetered(t *testing.T) {
	meter := &recordingMeter{}
	h := usageHandler(t, newTestStore(t, "http://127.0.0.1:1/v1"), meter)

	cases := []struct {
		name   string
		path   string
		body   string
		status int
	}{
		{"invalid json", "/v1/chat/completions", `{not json`, 400},
		{"missing model", "/v1/chat/completions", `{"messages":[]}`, 400},
		{"unknown model", "/v1/chat/completions", `{"model":"nope","messages":[]}`, 404},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := doRequest(t, h, http.MethodPost, tc.path, tc.body, nil)
			if rec.Code != tc.status {
				t.Fatalf("status = %d, want %d", rec.Code, tc.status)
			}
		})
	}
	if evs := meter.recorded(); len(evs) != 0 {
		t.Fatalf("pre-resolution rejections metered: %+v", evs)
	}
}

// Unauthenticated requests are not metered: no identity, no event.

func TestUsageUnauthorizedNotMetered(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		t.Error("unauthenticated request reached the upstream")
	}))
	defer up.Close()

	meter := &recordingMeter{}
	h := usageHandler(t, newTestStore(t, up.URL+"/v1"), meter)
	body := `{"model":"test-model","messages":[]}`
	rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions", body, map[string]string{"Authorization": ""})
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", rec.Code)
	}
	if evs := meter.recorded(); len(evs) != 0 {
		t.Fatalf("unauthenticated request metered: %+v", evs)
	}
}

// A transform failure is purely local. It resolves a public model, but never
// builds an outbound request or reaches a provider, so it must not become a
// factual provider-usage event.
func TestUsageTransformFailureNotMetered(t *testing.T) {
	meter := &recordingMeter{}
	h := &injectorHandler{
		store: newTestStore(t, "http://127.0.0.1:1/v1"),
		doers: directResolver(),
		auth:  auth.StaticProvider{},
		meter: meter,
		log:   quietLogger(),
	}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"test-model","messages":[]}`))
	req = req.WithContext(context.Background())
	req.Header.Set("Authorization", "Bearer "+testAPIKey)
	h.serve(rec, req, "chat",
		func([]byte, config.Model) ([]byte, error) { return nil, errors.New("test transform failure") },
		inject.RewriteChatModel, inject.SynthesizeChatThinkingUsage, "/chat/completions")
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
	if evs := meter.recorded(); len(evs) != 0 {
		t.Fatalf("local transform failure metered: %+v", evs)
	}
}

// Partner identity binds to the event from the auth-time principal.

func TestUsageCarriesPartnerIdentity(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"1","model":"upstream-name","usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}`))
	}))
	defer up.Close()

	provider := &stubAuthProvider{principal: auth.Principal{PartnerID: "acme", KeyID: "pak_123"}, reason: auth.ReasonOK}
	meter := &recordingMeter{}
	h := NewHandler(newTestStore(t, up.URL+"/v1"), directResolver(), provider, meter, quietLogger())

	body := `{"model":"test-model","messages":[{"role":"user","content":"hi"}]}`
	if rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions", body, nil); rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	ev := meter.single(t)
	if ev.PartnerID != "acme" || ev.KeyID != "pak_123" {
		t.Fatalf("identity = %q/%q, want acme/pak_123", ev.PartnerID, ev.KeyID)
	}
}

// A reload mid-flight cannot re-attach the event to the new generation:
// the request is bound to the snapshot it loaded at entry.

func TestUsageReloadCannotRebindGeneration(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	store := newTestStore(t, "http://127.0.0.1:1/v1")
	resolver := &blockingResolver{entered: entered, release: release}

	meter := &recordingMeter{}
	h := NewHandler(store, resolver, nil, meter, quietLogger())

	body := `{"model":"test-model","messages":[{"role":"user","content":"hi"}]}`
	done := make(chan int, 1)
	go func() {
		rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions", body, nil)
		done <- rec.Code
	}()

	<-entered
	// The reload: publish a fresh snapshot mid-flight. The active
	// generation advances; the in-flight request must not follow it.
	next, err := config.LoadRuntime([]byte(fmt.Sprintf("api-key: %s\nmodels:\n  test-model:\n    endpoint: http://127.0.0.1:2/v1\n    upstream-model: other-upstream\n", testAPIKey)))
	if err != nil {
		t.Fatalf("LoadRuntime: %v", err)
	}
	if err := store.Publish(next); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	close(release)

	if code := <-done; code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502", code)
	}
	ev := meter.single(t)
	if ev.ConfigGeneration != 0 {
		t.Fatalf("event generation = %d, want 0 (the entry snapshot, never the mid-flight reload)", ev.ConfigGeneration)
	}
	if ev.Outcome != "upstream_unreachable" || ev.HTTPStatus != http.StatusBadGateway {
		t.Fatalf("outcome/status = %q/%d", ev.Outcome, ev.HTTPStatus)
	}
}

// blockingResolver holds every outbound hop until the test releases it,
// letting the reload land strictly mid-request.
type blockingResolver struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (b *blockingResolver) Doer(transport.Config) transport.Doer { return &blockingUsageDoer{b: b} }

type blockingUsageDoer struct{ b *blockingResolver }

func (d *blockingUsageDoer) Do(*http.Request) (*http.Response, error) {
	d.b.once.Do(func() { close(d.b.entered) })
	<-d.b.release
	return nil, errors.New("dial tcp: connection refused")
}

// Metering off: the nil pipeline records nothing, and the response is
// exactly what it would be without the seam.

func TestUsageNilMeterRecordsNothing(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"1","model":"upstream-name","usage":{"prompt_tokens":1,"completion_tokens":2,"total_tokens":3}}`))
	}))
	defer up.Close()

	h := NewHandler(newTestStore(t, up.URL+"/v1"), directResolver(), nil, nil, quietLogger())
	body := `{"model":"test-model","messages":[{"role":"user","content":"hi"}]}`
	rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions", body, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), `"total_tokens":3`) {
		t.Fatalf("usage off changed the response body: %s", rec.Body.String())
	}
}
