package proxy

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/rs/zerolog"

	"openai-compatible-injector/internal/auth"
	"openai-compatible-injector/internal/config"
	"openai-compatible-injector/internal/inject"
	"openai-compatible-injector/internal/transport"
)

// blockingDoer blocks every request until released, signalling entry —
// the reload test's in-flight window.
type blockingDoer struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (b *blockingDoer) Do(*http.Request) (*http.Response, error) {
	b.once.Do(func() { close(b.entered) })
	<-b.release
	return nil, errors.New("dial tcp: connection refused")
}

// chainStore builds a store whose chain-model walks two provider
// candidates — pa (direct transport) then pb (proxy transport) — with the
// given provider-fallback block ("" = defaults). The injection prompt is a
// swept marker.
func newChainStore(t *testing.T, fallbackBlock string) *config.Store {
	t.Helper()
	snap, err := config.LoadRuntime([]byte("api-key: " + testAPIKey + "\n" + fallbackBlock + `
transports:
  t1:
    type: direct
  t2:
    type: proxy
    proxy: http://127.0.0.1:9090
providers:
  pa:
    base-url: https://a.example/v1
    transport: t1
  pb:
    base-url: https://b.example/v1
    transport: t2
models:
  chain-model:
    injection-prompt: "CHAIN-PROMPT-MARKER"
    providers:
      - provider: pa
        upstream-model: up-a
      - provider: pb
        upstream-model: up-b
`))
	if err != nil {
		t.Fatalf("LoadRuntime: %v", err)
	}
	return config.NewStore(snap)
}

// kindResolver answers direct-kind configs with one Doer and proxy-kind
// configs with another — the chain's two candidates in one store.
type kindResolver struct {
	direct  transport.Doer
	proxied transport.Doer
}

func (r kindResolver) Doer(c transport.Config) transport.Doer {
	if c.Kind == transport.Proxy {
		return r.proxied
	}
	return r.direct
}

// fakeUpstream is a per-candidate Doer stand-in: it records every request
// URL and body it is handed and answers with a canned status/body or a
// canned error. The canned body is re-wrapped per call — the handler
// consumes each response body.
type fakeUpstream struct {
	mu     sync.Mutex
	urls   []string
	bodies []string
	status int
	body   string
	ct     string
	err    error
}

func (f *fakeUpstream) Do(req *http.Request) (*http.Response, error) {
	b, _ := io.ReadAll(req.Body)
	f.mu.Lock()
	f.urls = append(f.urls, req.URL.String())
	f.bodies = append(f.bodies, string(b))
	st, body, ct, err := f.status, f.body, f.ct, f.err
	f.mu.Unlock()
	if err != nil {
		return nil, err
	}
	if ct == "" {
		ct = "application/json"
	}
	return &http.Response{
		StatusCode: st,
		Header:     http.Header{"Content-Type": []string{ct}},
		Body:       io.NopCloser(strings.NewReader(body)),
		Request:    req,
	}, nil
}

func (f *fakeUpstream) calls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.urls)
}

func (f *fakeUpstream) lastBody() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.bodies) == 0 {
		return ""
	}
	return f.bodies[len(f.bodies)-1]
}

// dialError builds the *url.Error shape a real dial failure produces —
// with fake secret material in the query string so the no-leak assertion
// has something to catch: a log line quoting the raw error would embed it.
func dialError(host string) error {
	return &url.Error{
		Op:  "Post",
		URL: "https://" + host + "/v1/chat/completions?trace=sup3r-s3cret-m4terial",
		Err: errors.New("dial tcp: connection refused"),
	}
}

const chainChatBody = `{"model":"chain-model","messages":[{"role":"user","content":"hi"}]}`

// TestProviderChainFirstCandidateSucceeds pins the quiet direction: a
// healthy primary means one provider attempt, and the fallback candidate
// is never dialed.
func TestProviderChainFirstCandidateSucceeds(t *testing.T) {
	store := newChainStore(t, "")
	pa := &fakeUpstream{status: http.StatusOK, body: `{"model":"up-a","choices":[]}`}
	pb := &fakeUpstream{status: http.StatusOK, body: `{"model":"up-b","choices":[]}`}
	logBuf, log := captureLog(zerolog.InfoLevel)
	h := NewHandler(store, kindResolver{direct: pa, proxied: pb}, nil, nil, log)

	rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions", chainChatBody, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"model":"chain-model"`) {
		t.Errorf("response model not rewritten to the public name: %s", rec.Body.String())
	}
	if pb.calls() != 0 {
		t.Errorf("fallback candidate dialed on a healthy primary: %d calls", pb.calls())
	}
	done := logBuf.events(t, "request_completed")
	if len(done) != 1 {
		t.Fatalf("request_completed events = %d, want 1", len(done))
	}
	if done[0]["provider_attempts"] != float64(1) || done[0]["final_provider"] != "pa" {
		t.Errorf("provider fields = %v %v, want 1/pa", done[0]["provider_attempts"], done[0]["final_provider"])
	}
	if _, exhausted := done[0]["provider_exhausted"]; exhausted {
		t.Errorf("provider_exhausted set on a successful walk")
	}
}

// TestProviderChainFallsBackOnTransportFailure pins the fallback trigger:
// a transport-level failure on the primary moves to the next candidate,
// which replays the same client body under ITS upstream identity. The
// failed attempt is one sanitized WARN; the client sees only the winner's
// answer; the raw error text (here carrying a fake credential in its
// query string) reaches no log line.
func TestProviderChainFallsBackOnTransportFailure(t *testing.T) {
	store := newChainStore(t, "")
	pa := &fakeUpstream{err: dialError("a.example")}
	pb := &fakeUpstream{status: http.StatusOK, body: `{"model":"up-b","choices":[]}`}
	logBuf, log := captureLog(zerolog.InfoLevel)
	h := NewHandler(store, kindResolver{direct: pa, proxied: pb}, nil, nil, log)

	rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions", chainChatBody, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"model":"chain-model"`) {
		t.Errorf("response model not rewritten to the public name: %s", rec.Body.String())
	}
	// Replay safety: each candidate received a FRESH request transformed
	// from the same immutable client body — its own upstream model, the
	// same injected prompt.
	if !strings.Contains(pa.lastBody(), `"model":"up-a"`) || !strings.Contains(pa.lastBody(), "CHAIN-PROMPT-MARKER") {
		t.Errorf("primary got the wrong transformed body: %s", pa.lastBody())
	}
	if !strings.Contains(pb.lastBody(), `"model":"up-b"`) || !strings.Contains(pb.lastBody(), "CHAIN-PROMPT-MARKER") {
		t.Errorf("fallback got the wrong transformed body: %s", pb.lastBody())
	}

	failed := logBuf.events(t, "provider_attempt_failed")
	if len(failed) != 1 {
		t.Fatalf("provider_attempt_failed events = %d, want 1", len(failed))
	}
	if failed[0]["provider"] != "pa" || failed[0]["provider_attempt"] != float64(1) {
		t.Errorf("provider_attempt_failed fields = %v %v, want pa/1", failed[0]["provider"], failed[0]["provider_attempt"])
	}
	if failed[0]["error_class"] != "connection" {
		t.Errorf("error_class = %v, want connection", failed[0]["error_class"])
	}
	if failed[0]["error_cause"] != "dial" {
		t.Errorf("error_cause = %v, want dial", failed[0]["error_cause"])
	}
	if failed[0]["egress_attempt"] != float64(1) {
		t.Errorf("egress_attempt = %v, want 1 (the direct dial)", failed[0]["egress_attempt"])
	}
	// The uniform per-dial evidence the single-endpoint path now emits: a
	// direct failure carries the same egress_attempt_failed record a pool
	// member's failure gets.
	eg := logBuf.events(t, "egress_attempt_failed")
	if len(eg) != 1 {
		t.Fatalf("egress_attempt_failed events = %d, want 1", len(eg))
	}
	if eg[0]["egress_kind"] != "direct" || eg[0]["egress_target"] != "direct" || eg[0]["egress_attempt"] != float64(1) {
		t.Errorf("direct egress evidence = %v/%v/%v, want direct/direct/1",
			eg[0]["egress_kind"], eg[0]["egress_target"], eg[0]["egress_attempt"])
	}
	done := logBuf.events(t, "request_completed")
	if len(done) != 1 {
		t.Fatalf("request_completed events = %d, want 1", len(done))
	}
	if done[0]["provider_attempts"] != float64(2) || done[0]["final_provider"] != "pb" {
		t.Errorf("provider fields = %v %v, want 2/pb", done[0]["provider_attempts"], done[0]["final_provider"])
	}
	if _, exhausted := done[0]["provider_exhausted"]; exhausted {
		t.Errorf("provider_exhausted set on a successful fallback")
	}
	if b := logBuf.String(); strings.Contains(b, "sup3r-s3cret-m4terial") {
		t.Errorf("raw error text (with fake credential) reached the logs")
	}
}

// TestProviderChainHTTPStatusIsTerminal pins the boundary the whole design
// rests on: an HTTP status — 429 included — is an ANSWER. No fallback, no
// second provider; the status is preserved and the body normalized.
func TestProviderChainHTTPStatusIsTerminal(t *testing.T) {
	for _, status := range []int{http.StatusTooManyRequests, http.StatusInternalServerError} {
		store := newChainStore(t, "")
		pa := &fakeUpstream{status: status, body: `{"error":{"message":"provider says no"}}`}
		pb := &fakeUpstream{status: http.StatusOK, body: `{"model":"up-b","choices":[]}`}
		logBuf, log := captureLog(zerolog.InfoLevel)
		h := NewHandler(store, kindResolver{direct: pa, proxied: pb}, nil, nil, log)

		rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions", chainChatBody, nil)
		if rec.Code != status {
			t.Errorf("status %d: client status = %d, want %d", status, rec.Code, status)
		}
		want := fmt.Sprintf(`"code":"upstream_http_%d"`, status)
		if !strings.Contains(rec.Body.String(), want) {
			t.Errorf("status %d: body %s, want canonical envelope with %s", status, rec.Body.String(), want)
		}
		if strings.Contains(rec.Body.String(), "provider says no") {
			t.Errorf("status %d: provider error body relayed raw", status)
		}
		if pb.calls() != 0 {
			t.Errorf("status %d: fallback dialed after an HTTP answer: %d calls", status, pb.calls())
		}
		done := logBuf.events(t, "request_completed")
		if len(done) != 1 || done[0]["provider_attempts"] != float64(1) || done[0]["final_provider"] != "pa" {
			t.Errorf("status %d: provider fields = %v, want one attempt on pa", status, done)
		}
		if n := len(logBuf.events(t, "provider_attempt_failed")); n != 0 {
			t.Errorf("status %d: %d provider_attempt_failed events, want 0 (statuses are answers)", status, n)
		}
	}
}

// TestProviderChainExhaustion pins the walk's dead end: every budgeted
// candidate fails at the transport level, the client gets the canonical
// 502, and the observability trail says so — two WARNs, one ERROR, and
// provider_exhausted on the completion event.
func TestProviderChainExhaustion(t *testing.T) {
	store := newChainStore(t, "")
	pa := &fakeUpstream{err: dialError("a.example")}
	pb := &fakeUpstream{err: dialError("b.example")}
	logBuf, log := captureLog(zerolog.InfoLevel)
	h := NewHandler(store, kindResolver{direct: pa, proxied: pb}, nil, nil, log)

	rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions", chainChatBody, nil)
	if rec.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502", rec.Code)
	}
	if rec.Body.String() != envelopeUpUnreach {
		t.Errorf("body = %s, want the canonical unreachable envelope", rec.Body.String())
	}
	if pa.calls() != 1 || pb.calls() != 1 {
		t.Errorf("calls = %d/%d, want exactly one attempt per candidate", pa.calls(), pb.calls())
	}
	failed := logBuf.events(t, "provider_attempt_failed")
	if len(failed) != 2 {
		t.Fatalf("provider_attempt_failed events = %d, want 2", len(failed))
	}
	if failed[0]["provider"] != "pa" || failed[1]["provider"] != "pb" {
		t.Errorf("failure order = %v %v, want pa then pb", failed[0]["provider"], failed[1]["provider"])
	}
	if evs := logBuf.events(t, "upstream_request_failed"); len(evs) != 1 || evs[0]["provider_exhausted"] != true {
		t.Errorf("upstream_request_failed = %v, want one event with provider_exhausted", evs)
	}
	done := logBuf.events(t, "request_completed")
	if len(done) != 1 || done[0]["provider_attempts"] != float64(2) ||
		done[0]["final_provider"] != "pb" || done[0]["provider_exhausted"] != true {
		t.Errorf("request_completed provider fields = %v, want 2/pb/exhausted", done)
	}
}

// TestProviderChainFallbackDisabled pins the policy off-switch: with
// enabled: false the walk never leaves the primary, even with a healthy
// candidate waiting.
func TestProviderChainFallbackDisabled(t *testing.T) {
	store := newChainStore(t, "provider-fallback:\n  enabled: false\n")
	pa := &fakeUpstream{err: dialError("a.example")}
	pb := &fakeUpstream{status: http.StatusOK, body: `{"model":"up-b","choices":[]}`}
	logBuf, log := captureLog(zerolog.InfoLevel)
	h := NewHandler(store, kindResolver{direct: pa, proxied: pb}, nil, nil, log)

	rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions", chainChatBody, nil)
	if rec.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502", rec.Code)
	}
	if pb.calls() != 0 {
		t.Errorf("fallback candidate dialed with the feature disabled: %d calls", pb.calls())
	}
	done := logBuf.events(t, "request_completed")
	if len(done) != 1 || done[0]["provider_attempts"] != float64(1) || done[0]["final_provider"] != "pa" {
		t.Errorf("provider fields = %v, want one attempt on pa", done)
	}
}

// TestProviderChainTransformErrorNeverFallsBack pins the local-validation
// boundary: a body that fails the transform is a client error, not a
// provider failure — no candidate is dialed, and the 400 comes back
// immediately. The real Chat/Responses transforms fail only on
// structurally impossible bodies (they pass non-array messages through
// untouched), so the seam is driven directly with a failing transform —
// the same injection point the handler owns.
func TestProviderChainTransformErrorNeverFallsBack(t *testing.T) {
	store := newChainStore(t, "")
	pa := &fakeUpstream{status: http.StatusOK, body: `{"model":"up-a","choices":[]}`}
	pb := &fakeUpstream{status: http.StatusOK, body: `{"model":"up-b","choices":[]}`}
	logBuf, log := captureLog(zerolog.InfoLevel)
	h := &injectorHandler{store: store, doers: kindResolver{direct: pa, proxied: pb}, auth: auth.StaticProvider{}, log: log}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(chainChatBody))
	req.Header.Set("Authorization", "Bearer "+testAPIKey)
	h.serve(rec, req, "chat",
		func([]byte, config.Model) ([]byte, error) { return nil, errors.New("boom") },
		inject.RewriteChatModel, inject.SynthesizeChatThinkingUsage, "/chat/completions")

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
	if rec.Body.String() != envelopeInvalidReq {
		t.Errorf("body = %s, want the canonical invalid-request envelope", rec.Body.String())
	}
	if pa.calls() != 0 || pb.calls() != 0 {
		t.Errorf("upstream dialed on a local validation error: %d/%d calls", pa.calls(), pb.calls())
	}
	done := logBuf.events(t, "request_completed")
	if len(done) != 1 || done[0]["outcome"] != "transform_error" {
		t.Errorf("outcome = %v, want transform_error", done)
	}
	if n := len(logBuf.events(t, "provider_attempt_failed")); n != 0 {
		t.Errorf("%d provider_attempt_failed events, want 0 (local errors never fall back)", n)
	}
}

// TestProviderChainCancellationAbortsWalk pins the cancellation boundary:
// when the client goes away mid-walk there is no fallback — nobody is left
// to answer — and the outcome is the disconnect, not a 502. The request
// context arrives genuinely canceled (doDisconnectedRequest): under the
// ownership rule the context, not the error shape, decides what is the
// caller's failure.
func TestProviderChainCancellationAbortsWalk(t *testing.T) {
	store := newChainStore(t, "")
	pa := &fakeUpstream{err: context.Canceled}
	pb := &fakeUpstream{status: http.StatusOK, body: `{"model":"up-b","choices":[]}`}
	logBuf, log := captureLog(zerolog.InfoLevel)
	h := NewHandler(store, kindResolver{direct: pa, proxied: pb}, nil, nil, log)

	rec := doDisconnectedRequest(t, h, http.MethodPost, "/v1/chat/completions", chainChatBody, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want the bare recorder default (nothing written)", rec.Code)
	}
	if rec.Body.Len() != 0 {
		t.Errorf("body = %q, want nothing written for a disconnected client", rec.Body.String())
	}
	if pb.calls() != 0 {
		t.Errorf("fallback dialed after client cancellation: %d calls", pb.calls())
	}
	done := logBuf.events(t, "request_completed")
	if len(done) != 1 || done[0]["outcome"] != "client_disconnected" {
		t.Fatalf("outcome = %v, want client_disconnected", done)
	}
	if done[0]["provider_attempts"] != float64(1) || done[0]["final_provider"] != "pa" {
		t.Errorf("provider fields = %v %v, want one attempt on pa", done[0]["provider_attempts"], done[0]["final_provider"])
	}
	if _, exhausted := done[0]["provider_exhausted"]; exhausted {
		t.Errorf("provider_exhausted set on a client cancellation")
	}
}

// TestProviderChainStreamingCommitment pins the streaming boundary: a 200
// SSE answer from the primary is THE response — the stream is relayed, and
// the fallback candidate is never dialed. Commitment needs no extra
// machinery: the whole walk happens before the first response byte.
func TestProviderChainStreamingCommitment(t *testing.T) {
	store := newChainStore(t, "")
	pa := &fakeUpstream{
		status: http.StatusOK,
		ct:     "text/event-stream",
		body:   "data: {\"model\":\"up-a\",\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\ndata: [DONE]\n\n",
	}
	pb := &fakeUpstream{status: http.StatusOK, body: `{"model":"up-b","choices":[]}`}
	logBuf, log := captureLog(zerolog.InfoLevel)
	h := NewHandler(store, kindResolver{direct: pa, proxied: pb}, nil, nil, log)

	rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions",
		`{"model":"chain-model","stream":true,"messages":[{"role":"user","content":"hi"}]}`, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "text/event-stream" {
		t.Errorf("Content-Type = %q, want text/event-stream", ct)
	}
	if !strings.Contains(rec.Body.String(), `"model":"chain-model"`) || !strings.Contains(rec.Body.String(), "data: [DONE]") {
		t.Errorf("stream relayed wrong: %s", rec.Body.String())
	}
	if pb.calls() != 0 {
		t.Errorf("fallback dialed after a committed stream: %d calls", pb.calls())
	}
	done := logBuf.events(t, "request_completed")
	if len(done) != 1 || done[0]["provider_attempts"] != float64(1) {
		t.Errorf("provider fields = %v, want one attempt", done)
	}
}

// keyResolver answers each transport config by its content key — the
// reload test's way to give two snapshots' different transports distinct
// doers.
type keyResolver struct {
	m        map[string]transport.Doer
	fallback transport.Doer
}

func (r keyResolver) Doer(c transport.Config) transport.Doer {
	if d, ok := r.m[c.Key()]; ok {
		return d
	}
	return r.fallback
}

// TestProviderChainBindsToItsSnapshot pins the reload invariant for the
// walk: a request binds its snapshot once, so a reload that lands while
// the request sits inside its primary's upstream call cannot reshape the
// chain under it. The new snapshot's chain has a different fallback
// candidate on a different transport; the in-flight request must still
// walk the OLD chain and report the OLD generation.
func TestProviderChainBindsToItsSnapshot(t *testing.T) {
	build := func(pbProxy string) (*config.Store, *config.Snapshot) {
		snap, err := config.LoadRuntime([]byte("api-key: " + testAPIKey + "\ntransports:\n" +
			"  t1:\n    type: direct\n" +
			"  t2:\n    type: proxy\n    proxy: " + pbProxy + "\n" +
			"providers:\n" +
			"  pa:\n    base-url: https://a.example/v1\n    transport: t1\n" +
			"  pb:\n    base-url: https://pb.example/v1\n    transport: t2\n" +
			"models:\n  chain-model:\n    providers:\n" +
			"      - provider: pa\n        upstream-model: up-a\n" +
			"      - provider: pb\n        upstream-model: up-b\n"))
		if err != nil {
			t.Fatalf("LoadRuntime: %v", err)
		}
		return config.NewStore(snap), snap
	}
	store, snap1 := build("http://127.0.0.1:9090")
	_, snap2 := build("http://127.0.0.1:9091")

	entered := make(chan struct{})
	pa := &blockingDoer{entered: entered, release: make(chan struct{})}
	pbOld := &fakeUpstream{status: http.StatusOK, body: `{"model":"up-b","choices":[]}`}
	pbNew := &fakeUpstream{status: http.StatusOK, body: `{"model":"up-b","choices":[]}`}
	res := keyResolver{m: map[string]transport.Doer{}}
	for _, tc := range snap1.Transports() {
		if tc.Kind == transport.Proxy {
			res.m[tc.Key()] = pbOld
		} else {
			res.m[tc.Key()] = pa
		}
	}
	for _, tc := range snap2.Transports() {
		if tc.Kind == transport.Proxy {
			res.m[tc.Key()] = pbNew
		}
	}
	logBuf, log := captureLog(zerolog.InfoLevel)
	h := NewHandler(store, res, nil, nil, log)

	type result struct {
		code int
		body string
	}
	done := make(chan result, 1)
	go func() {
		rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions", chainChatBody, nil)
		done <- result{rec.Code, rec.Body.String()}
	}()
	<-entered
	if err := store.Publish(snap2); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	close(pa.release)

	got := <-done
	if got.code != http.StatusOK {
		t.Fatalf("status = %d, body %s", got.code, got.body)
	}
	if pbOld.calls() != 1 {
		t.Errorf("old chain's fallback calls = %d, want 1 (the walk stayed on its snapshot)", pbOld.calls())
	}
	if pbNew.calls() != 0 {
		t.Errorf("new snapshot's fallback dialed mid-flight: %d calls", pbNew.calls())
	}
	evs := logBuf.events(t, "request_completed")
	if len(evs) != 1 || evs[0]["config_generation"] != float64(0) {
		t.Errorf("request_completed = %v, want config_generation 0 (the bound snapshot)", evs)
	}
}

// TestProviderChainBoundedByProviderAndEgressBudget is the multiplication
// proof with the real stack: a two-candidate chain, each candidate on a
// two-member SOCKS pool whose fallback budget is 2. The worst case dials
// exactly provider_budget × egress_budget = 2 × 2 = 4 endpoints — every
// dial lands on a counting listener, so the total is pinned, not assumed.
// Anything above 4 (unbounded provider fanout, egress retry past the pool
// budget) fails this test.
func TestProviderChainBoundedByProviderAndEgressBudget(t *testing.T) {
	listener := func() (string, *int32, func()) {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("listen: %v", err)
		}
		var conns int32
		done := make(chan struct{})
		go func() {
			defer close(done)
			for {
				c, err := l.Accept()
				if err != nil {
					return
				}
				atomic.AddInt32(&conns, 1)
				_ = c.Close()
			}
		}()
		return "socks5h://127.0.0.1:" + fmt.Sprint(l.Addr().(*net.TCPAddr).Port), &conns, func() {
			_ = l.Close()
			<-done
		}
	}

	type counter struct {
		addr  string
		conns *int32
		close func()
	}
	var counters [4]counter
	for i := range counters {
		addr, n, closer := listener()
		counters[i] = counter{addr: addr, conns: n, close: closer}
		defer counters[i].close()
	}

	poolYAML := func(name, a, b string) string {
		return "  " + name + `:
    type: pool
    members:
      - transport: ` + name + `-a
      - transport: ` + name + `-b
    fallback:
      max-attempts: 2
  ` + name + `-a:
    type: proxy
    proxy: ` + a + `
  ` + name + `-b:
    type: proxy
    proxy: ` + b + `
`
	}
	cfg := "api-key: " + testAPIKey + "\nprovider-fallback:\n  max-attempts: 2\ntransports:\n" +
		poolYAML("pool-a", counters[0].addr, counters[1].addr) +
		poolYAML("pool-b", counters[2].addr, counters[3].addr) + `
providers:
  pa:
    base-url: http://dead-a.example/v1
    transport: pool-a
  pb:
    base-url: http://dead-b.example/v1
    transport: pool-b
models:
  chain-model:
    providers:
      - provider: pa
        upstream-model: up-a
      - provider: pb
        upstream-model: up-b
`
	snap, err := config.LoadRuntime([]byte(cfg))
	if err != nil {
		t.Fatalf("LoadRuntime: %v", err)
	}
	store := config.NewStore(snap)
	reg := transport.NewRegistry()
	reg.Retain(snap.Transports())
	logBuf, log := captureLog(zerolog.InfoLevel)
	h := NewHandler(store, reg, nil, nil, log)

	rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions", chainChatBody, nil)
	if rec.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, body %s, want 502", rec.Code, rec.Body.String())
	}
	if rec.Body.String() != envelopeUpUnreach {
		t.Errorf("body = %s, want the canonical unreachable envelope", rec.Body.String())
	}

	var total int32
	for i, c := range counters {
		got := atomic.LoadInt32(c.conns)
		if got != 1 {
			t.Errorf("listener %d conns = %d, want exactly 1 (one egress attempt)", i, got)
		}
		total += got
	}
	if total != 4 {
		t.Errorf("total dials = %d, want exactly provider_budget × egress_budget = 4", total)
	}

	egress := logBuf.events(t, "egress_attempt_failed")
	if len(egress) != 4 {
		t.Fatalf("egress_attempt_failed events = %d, want 4 (2 per provider attempt)", len(egress))
	}
	// The nested attempt identity, real stack edition: dial order pins the
	// (provider_attempt, egress_attempt) pairs — (1,1),(1,2),(2,1),(2,2) —
	// and attempt rides along as the egress_attempt alias.
	wantPairs := [][2]int{{1, 1}, {1, 2}, {2, 1}, {2, 2}}
	perProvider := map[string]int{}
	for i, ev := range egress {
		perProvider[ev["provider"].(string)]++
		if ev["error_class"] != "proxy_connect" {
			t.Errorf("egress error_class = %v, want proxy_connect", ev["error_class"])
		}
		if ev["provider_attempt"] != float64(wantPairs[i][0]) || ev["egress_attempt"] != float64(wantPairs[i][1]) {
			t.Errorf("event %d attempt indexes = %v/%v, want %d/%d",
				i, ev["provider_attempt"], ev["egress_attempt"], wantPairs[i][0], wantPairs[i][1])
		}
		if ev["attempt"] != ev["egress_attempt"] {
			t.Errorf("event %d attempt alias = %v, want %v (the egress_attempt alias)", i, ev["attempt"], ev["egress_attempt"])
		}
	}
	if perProvider["pa"] != 2 || perProvider["pb"] != 2 {
		t.Errorf("egress failures per provider = %v, want 2 each", perProvider)
	}
	attemptFailed := logBuf.events(t, "provider_attempt_failed")
	if len(attemptFailed) != 2 {
		t.Fatalf("provider_attempt_failed events = %d, want 2", len(attemptFailed))
	}
	for i, provider := range []string{"pa", "pb"} {
		ev := attemptFailed[i]
		if ev["provider"] != provider || ev["provider_attempt"] != float64(i+1) {
			t.Errorf("provider_attempt_failed %d = %v/%v, want %s/%d", i, ev["provider"], ev["provider_attempt"], provider, i+1)
		}
		if ev["egress_attempt"] != float64(2) {
			t.Errorf("provider_attempt_failed %d egress_attempt = %v, want 2", i, ev["egress_attempt"])
		}
		if ev["error_class"] != "proxy_connect" || ev["error_cause"] != "proxy_connect" {
			t.Errorf("provider_attempt_failed %d class/cause = %v/%v, want proxy_connect/proxy_connect",
				i, ev["error_class"], ev["error_cause"])
		}
	}
	exhausted := findLogEvent(t, logBuf.String(), "upstream_request_failed")
	if exhausted["error_class"] != "provider_exhausted" || exhausted["error_cause"] != "proxy_connect" {
		t.Errorf("exhaustion class/cause = %v/%v, want provider_exhausted/proxy_connect",
			exhausted["error_class"], exhausted["error_cause"])
	}
	if exhausted["provider_attempt"] != float64(2) || exhausted["egress_attempt"] != float64(2) {
		t.Errorf("exhaustion attempt indexes = %v/%v, want 2/2", exhausted["provider_attempt"], exhausted["egress_attempt"])
	}
	done := logBuf.events(t, "request_completed")
	if len(done) != 1 || done[0]["provider_attempts"] != float64(2) ||
		done[0]["provider_exhausted"] != true || done[0]["egress_attempts"] != float64(2) {
		t.Errorf("request_completed = %v, want provider 2/exhausted with last pool report", done)
	}
}

// timeoutError mimics the net.Error timeout shape a provider-local dial or
// header timeout produces — endpoint-owned and fallback-eligible under a
// live caller context.
type timeoutError struct{}

func (timeoutError) Error() string   { return "dial tcp: provider-local timeout" }
func (timeoutError) Timeout() bool   { return true }
func (timeoutError) Temporary() bool { return false }

// TestProviderChainCallerDeadlinePreventsCandidateB pins the deadline half
// of the ownership rule at the walk level: a caller whose deadline has
// already fired is done waiting — the primary's failure is the caller's
// event, the fallback candidate is never dialed, and the answer is the
// disconnect (no envelope, no 502). A caller deadline must never read as a
// provider-local timeout, which would burn the fallback budget for a client
// that is gone.
func TestProviderChainCallerDeadlinePreventsCandidateB(t *testing.T) {
	store := newChainStore(t, "")
	pa := &fakeUpstream{err: dialError("a.example")}
	pb := &fakeUpstream{status: http.StatusOK, body: `{"model":"up-b","choices":[]}`}
	logBuf, log := captureLog(zerolog.InfoLevel)
	h := NewHandler(store, kindResolver{direct: pa, proxied: pb}, nil, nil, log)

	rec := doDeadlineRequest(t, h, http.MethodPost, "/v1/chat/completions", chainChatBody, nil)
	if rec.Body.Len() != 0 {
		t.Errorf("body = %q, want nothing written for a done caller", rec.Body.String())
	}
	if pb.calls() != 0 {
		t.Errorf("fallback dialed after the caller's deadline: %d calls", pb.calls())
	}
	failed := logBuf.events(t, "upstream_request_failed")
	if len(failed) != 1 {
		t.Fatalf("upstream_request_failed events = %d, want 1", len(failed))
	}
	if failed[0]["error_class"] != "canceled" {
		t.Errorf("error_class = %v, want canceled", failed[0]["error_class"])
	}
	if failed[0]["error_cause"] != "caller_deadline_exceeded" {
		t.Errorf("error_cause = %v, want caller_deadline_exceeded", failed[0]["error_cause"])
	}
	done := logBuf.events(t, "request_completed")
	if len(done) != 1 || done[0]["outcome"] != "client_disconnected" {
		t.Fatalf("outcome = %v, want client_disconnected", done)
	}
	if done[0]["provider_attempts"] != float64(1) {
		t.Errorf("provider_attempts = %v, want 1 (no second candidate)", done[0]["provider_attempts"])
	}
	if _, exhausted := done[0]["provider_exhausted"]; exhausted {
		t.Errorf("provider_exhausted set on a caller deadline")
	}
}

// TestProviderChainProviderLocalTimeoutStillFallsBack pins the complement:
// a timeout-shaped failure under a LIVE caller context is the endpoint's —
// canonical class timeout, bounded cause network_timeout — and keeps its
// bounded fallback eligibility, so the walk reaches candidate B and answers.
// Both pieces of per-dial evidence fire: the direct attempt's uniform
// egress_attempt_failed record and the candidate's provider_attempt_failed.
func TestProviderChainProviderLocalTimeoutStillFallsBack(t *testing.T) {
	store := newChainStore(t, "")
	pa := &fakeUpstream{err: timeoutError{}}
	pb := &fakeUpstream{status: http.StatusOK, body: `{"model":"up-b","choices":[]}`}
	logBuf, log := captureLog(zerolog.InfoLevel)
	h := NewHandler(store, kindResolver{direct: pa, proxied: pb}, nil, nil, log)

	rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions", chainChatBody, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"model":"chain-model"`) {
		t.Errorf("response not the fallback candidate's answer: %s", rec.Body.String())
	}
	eg := logBuf.events(t, "egress_attempt_failed")
	if len(eg) != 1 {
		t.Fatalf("egress_attempt_failed events = %d, want 1 (the direct dial)", len(eg))
	}
	if eg[0]["egress_kind"] != "direct" || eg[0]["egress_target"] != "direct" || eg[0]["egress_attempt"] != float64(1) {
		t.Errorf("direct egress evidence = %v/%v/%v, want direct/direct/1",
			eg[0]["egress_kind"], eg[0]["egress_target"], eg[0]["egress_attempt"])
	}
	if eg[0]["error_class"] != "timeout" || eg[0]["error_cause"] != "network_timeout" {
		t.Errorf("direct egress class/cause = %v/%v, want timeout/network_timeout",
			eg[0]["error_class"], eg[0]["error_cause"])
	}
	failed := logBuf.events(t, "provider_attempt_failed")
	if len(failed) != 1 {
		t.Fatalf("provider_attempt_failed events = %d, want 1", len(failed))
	}
	if failed[0]["provider"] != "pa" || failed[0]["provider_attempt"] != float64(1) || failed[0]["egress_attempt"] != float64(1) {
		t.Errorf("provider_attempt_failed fields = %v/%v/%v, want pa/1/1",
			failed[0]["provider"], failed[0]["provider_attempt"], failed[0]["egress_attempt"])
	}
	if failed[0]["error_class"] != "timeout" || failed[0]["error_cause"] != "network_timeout" {
		t.Errorf("provider_attempt_failed class/cause = %v/%v, want timeout/network_timeout",
			failed[0]["error_class"], failed[0]["error_cause"])
	}
	done := logBuf.events(t, "request_completed")
	if len(done) != 1 || done[0]["provider_attempts"] != float64(2) || done[0]["final_provider"] != "pb" {
		t.Errorf("provider fields = %v, want 2/pb", done)
	}
}
