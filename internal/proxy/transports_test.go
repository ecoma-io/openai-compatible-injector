package proxy

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"testing"

	"github.com/rs/zerolog"

	"openai-compatible-injector/internal/config"
	"openai-compatible-injector/internal/transport"
)

// recordingResolver pins the handler↔transport seam: it records every
// transport Config the handler asks for and answers each with a stub Doer
// that records the outbound request it executed. A handler rewired to one
// global client (the coupling this PR removes) leaves both empty.
type recordingResolver struct {
	mu   sync.Mutex
	seen []transport.Config
	doer *stubDoer
}

func (r *recordingResolver) Doer(c transport.Config) transport.Doer {
	r.mu.Lock()
	r.seen = append(r.seen, c)
	r.mu.Unlock()
	return r.doer
}

func (r *recordingResolver) configs() []transport.Config {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]transport.Config(nil), r.seen...)
}

// stubDoer answers every request from a canned table and records the URLs.
type stubDoer struct {
	mu   sync.Mutex
	urls []string
	code int
	body string
}

func (s *stubDoer) Do(req *http.Request) (*http.Response, error) {
	s.mu.Lock()
	s.urls = append(s.urls, req.URL.String())
	code, body := s.code, s.body
	s.mu.Unlock()
	return &http.Response{
		StatusCode: code,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader(body)),
		Request:    req,
	}, nil
}

func (s *stubDoer) calls() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.urls...)
}

// newTransportStore builds a store with one legacy inline model and one
// provider+proxy model, so the two forms sit side by side.
func newTransportStore(t *testing.T, proxyURL, providerBase string) *config.Store {
	t.Helper()
	yaml := fmt.Sprintf(`
api-key: %s
transports:
  egress:
    type: proxy
    proxy: %s
providers:
  opencode:
    base-url: %s
    transport: egress
models:
  direct-model:
    endpoint: https://direct.example/v1
    upstream-model: dm
  proxied-model:
    provider: opencode
    upstream-model: pm
`, testAPIKey, proxyURL, providerBase)
	snap, err := config.LoadRuntime([]byte(yaml))
	if err != nil {
		t.Fatalf("LoadRuntime: %v", err)
	}
	return config.NewStore(snap)
}

// TestHandlerExecutesOnTheModelsResolvedTransport is the seam pin: for a
// provider model the handler resolves that provider's transport config and
// executes the upstream call on the returned Doer, hitting the provider's
// base URL; for a legacy model the resolved config is the zero (direct)
// value. The quiet direction is a handler silently back on one global
// client, where proxied egress becomes direct egress.
func TestHandlerExecutesOnTheModelsResolvedTransport(t *testing.T) {
	proxyURL, _ := url.Parse("http://127.0.0.1:8080")
	store := newTransportStore(t, "http://127.0.0.1:8080", "https://api.opencode.example/v1")
	doer := &stubDoer{code: http.StatusOK, body: `{"id":"x","choices":[]}`}
	res := &recordingResolver{doer: doer}
	h := NewHandler(store, res, nil, nil, zerolog.New(zerolog.TestWriter{T: t}).Level(zerolog.Disabled))

	rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions",
		`{"model":"proxied-model","messages":[{"role":"user","content":"hi"}]}`, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body %s", rec.Code, rec.Body.String())
	}

	cfgs := res.configs()
	if len(cfgs) != 1 {
		t.Fatalf("resolver asked for %d transport configs, want 1 — handler bypassed the seam?", len(cfgs))
	}
	if cfgs[0].Kind != transport.Proxy || cfgs[0].ProxyURL.String() != proxyURL.String() {
		t.Errorf("resolved config = %+v, want the provider's proxy", cfgs[0])
	}
	calls := doer.calls()
	if len(calls) != 1 || calls[0] != "https://api.opencode.example/v1/chat/completions" {
		t.Errorf("outbound calls = %v", calls)
	}

	rec = doRequest(t, h, http.MethodPost, "/v1/chat/completions",
		`{"model":"direct-model","messages":[{"role":"user","content":"hi"}]}`, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("legacy model status = %d", rec.Code)
	}
	cfgs = res.configs()
	if len(cfgs) != 2 {
		t.Fatalf("resolver asked for %d configs after both requests, want 2", len(cfgs))
	}
	if cfgs[1] != (transport.Config{}) {
		t.Errorf("legacy model resolved %+v, want the zero (direct) config", cfgs[1])
	}
	if calls := doer.calls(); len(calls) != 2 || calls[1] != "https://direct.example/v1/chat/completions" {
		t.Errorf("outbound calls = %v", calls)
	}
}
