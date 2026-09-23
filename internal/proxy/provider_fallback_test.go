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
	"time"

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

// newChainStoreRetries is newChainStore with a retries block under the
// model ("" = no block: the built-in defaults apply). It is how retry
// tests size the budgets their assertions count against.
func newChainStoreRetries(t *testing.T, fallbackBlock, retriesBlock string) *config.Store {
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
` + retriesBlock + `
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

// scriptStep is one scripted answer: status/body/content-type, optional
// extra headers (Retry-After), or a transport-level error. The zero
// content type means application/json, the zero status 200.
type scriptStep struct {
	status int
	body   string
	ct     string
	header http.Header
	err    error
}

// scriptUpstream answers from a fixed script — step 0 to the first call,
// step 1 to the second, and the LAST step repeats once the script runs
// out — recording every request's URL and body like fakeUpstream. The
// sequence is what the retry matrix needs: the same candidate answering
// differently per attempt.
type scriptUpstream struct {
	mu     sync.Mutex
	steps  []scriptStep
	urls   []string
	bodies []string
}

func newScript(steps ...scriptStep) *scriptUpstream { return &scriptUpstream{steps: steps} }

func (s *scriptUpstream) Do(req *http.Request) (*http.Response, error) {
	b, _ := io.ReadAll(req.Body)
	s.mu.Lock()
	i := len(s.urls)
	s.urls = append(s.urls, req.URL.String())
	s.bodies = append(s.bodies, string(b))
	step := s.steps[len(s.steps)-1]
	if i < len(s.steps) {
		step = s.steps[i]
	}
	s.mu.Unlock()
	if step.err != nil {
		return nil, step.err
	}
	h := step.header.Clone()
	if h == nil {
		h = http.Header{}
	}
	ct := step.ct
	if ct == "" {
		ct = "application/json"
	}
	h.Set("Content-Type", ct)
	return &http.Response{
		StatusCode: step.status,
		Header:     h,
		Body:       io.NopCloser(strings.NewReader(step.body)),
		Request:    req,
	}, nil
}

func (s *scriptUpstream) calls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.urls)
}

func (s *scriptUpstream) allBodies() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.bodies...)
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

// TestProviderChainStatusMatrix walks the closed status matrix at the
// handler level, with no retries block declared (the defaults apply):
// terminal statuses answer once and relay as a single-provider deployment
// would; fallback-only statuses move straight to the next candidate — no
// same-candidate retry; retryable statuses spend the default one retry on
// the candidate first, then fall back. Every received error answers one
// evidence event, its disposition naming what the walk did about it.
func TestProviderChainStatusMatrix(t *testing.T) {
	okB := `{"model":"up-b","choices":[]}`
	for _, tc := range []struct {
		name       string
		status     int
		wantStatus int
		wantPa     int
		wantPb     int
		wantDisp   string // the LAST pa evidence event's disposition
	}{
		// Terminal — including the unlisted 418: answered once, relayed.
		{"terminal 400", http.StatusBadRequest, http.StatusBadRequest, 1, 0, "terminal"},
		{"terminal 418", http.StatusTeapot, http.StatusTeapot, 1, 0, "terminal"},
		{"terminal 451", http.StatusUnavailableForLegalReasons, http.StatusUnavailableForLegalReasons, 1, 0, "terminal"},
		{"terminal 501", http.StatusNotImplemented, http.StatusNotImplemented, 1, 0, "terminal"},
		{"terminal 505", http.StatusHTTPVersionNotSupported, http.StatusHTTPVersionNotSupported, 1, 0, "terminal"},
		// Fallback-only: one exchange on pa, straight to pb.
		{"fallback 401", http.StatusUnauthorized, http.StatusOK, 1, 1, "fallback"},
		{"fallback 403", http.StatusForbidden, http.StatusOK, 1, 1, "fallback"},
		{"fallback 404", http.StatusNotFound, http.StatusOK, 1, 1, "fallback"},
		{"fallback 405", http.StatusMethodNotAllowed, http.StatusOK, 1, 1, "fallback"},
		{"fallback 409", http.StatusConflict, http.StatusOK, 1, 1, "fallback"},
		{"fallback 422", http.StatusUnprocessableEntity, http.StatusOK, 1, 1, "fallback"},
		// Retry-then-fallback: the default budget retries pa once, then pb.
		{"retry 408", http.StatusRequestTimeout, http.StatusOK, 2, 1, "fallback"},
		{"retry 425", http.StatusTooEarly, http.StatusOK, 2, 1, "fallback"},
		{"retry 429", http.StatusTooManyRequests, http.StatusOK, 2, 1, "fallback"},
		{"retry 500", http.StatusInternalServerError, http.StatusOK, 2, 1, "fallback"},
		{"retry 503", http.StatusServiceUnavailable, http.StatusOK, 2, 1, "fallback"},
		{"retry unknown 529", 529, http.StatusOK, 2, 1, "fallback"},
		{"retry edge 599", 599, http.StatusOK, 2, 1, "fallback"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stubRetryTiming(t)
			store := newChainStore(t, "")
			pa := newScript(scriptStep{status: tc.status, body: `{"error":{"message":"provider says no"}}`})
			pb := newScript(scriptStep{status: http.StatusOK, body: okB})
			logBuf, log := captureLog(zerolog.InfoLevel)
			h := NewHandler(store, kindResolver{direct: pa, proxied: pb}, nil, nil, log)

			rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions", chainChatBody, nil)
			if rec.Code != tc.wantStatus {
				t.Fatalf("client status = %d, want %d (body %s)", rec.Code, tc.wantStatus, rec.Body.String())
			}
			if pa.calls() != tc.wantPa || pb.calls() != tc.wantPb {
				t.Fatalf("exchanges pa/pb = %d/%d, want %d/%d", pa.calls(), pb.calls(), tc.wantPa, tc.wantPb)
			}
			done := logBuf.events(t, "request_completed")
			// Terminal rows finalize on the answering pa; every row that
			// moves ends on pb.
			wantFinal, wantCand := "pb", float64(2)
			if tc.status == tc.wantStatus {
				wantFinal, wantCand = "pa", float64(1)
			}
			if len(done) != 1 || done[0]["provider_attempts"] != float64(tc.wantPa+tc.wantPb) ||
				done[0]["retries_total"] != float64(tc.wantPa-1) || done[0]["final_provider"] != wantFinal ||
				done[0]["final_candidate"] != wantCand {
				t.Fatalf("request_completed = %v, want %d attempts, %d retries, final %v/%v", done, tc.wantPa+tc.wantPb, tc.wantPa-1, wantFinal, wantCand)
			}
			// One evidence event per received error, the last one carrying
			// the disposition the walk acted on.
			evs := logBuf.events(t, "upstream_http_error")
			if len(evs) != tc.wantPa {
				t.Fatalf("upstream_http_error events = %d, want %d", len(evs), tc.wantPa)
			}
			if evs[len(evs)-1]["disposition"] != tc.wantDisp || evs[len(evs)-1]["upstream_status"] != float64(tc.status) {
				t.Errorf("last evidence disposition/status = %v/%v, want %v/%d",
					evs[len(evs)-1]["disposition"], evs[len(evs)-1]["upstream_status"], tc.wantDisp, tc.status)
			}
			if len(evs) == 2 && evs[0]["disposition"] != "retry" {
				t.Errorf("first evidence disposition = %v, want retry", evs[0]["disposition"])
			}
			if n := len(logBuf.events(t, "provider_attempt_failed")); n != 0 {
				t.Errorf("%d provider_attempt_failed events, want 0 (statuses are answers)", n)
			}
			if tc.status == tc.wantStatus {
				// The terminal direction: canonical envelope, status exact,
				// provider bytes relayed nowhere.
				want := fmt.Sprintf(`"code":"upstream_http_%d"`, tc.status)
				if !strings.Contains(rec.Body.String(), want) {
					t.Errorf("body %s, want canonical envelope with %s", rec.Body.String(), want)
				}
				if strings.Contains(rec.Body.String(), "provider says no") {
					t.Errorf("provider error body relayed raw")
				}
			}
		})
	}
}

// TestProviderChainRetryBudgetExact pins the budget semantics: max-retries
// counts the retries AFTER a candidate's initial attempt — a policy of N
// gives every candidate exactly N+1 provider-level attempts before the walk
// moves on. provider_attempts is the compatibility alias of
// candidate_attempts; upstream_exchanges is the separately named count of
// real egress dials, and retries_total remains the compatibility alias of
// retry_attempts.
func TestProviderChainRetryBudgetExact(t *testing.T) {
	for _, tc := range []struct {
		name         string
		retries      string
		wantPa       int
		wantAttempts float64
		wantRetries  float64
	}{
		{"zero retries", "    retries:\n      max-retries: 0\n", 1, 2, 0},
		{"default one retry", "", 2, 3, 1},
		{"two retries", "    retries:\n      max-retries: 2\n", 3, 4, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stubRetryTiming(t)
			store := newChainStoreRetries(t, "", tc.retries)
			pa := newScript(scriptStep{status: http.StatusTooManyRequests, body: `{"error":{"message":"rate limited"}}`})
			pb := newScript(scriptStep{status: http.StatusOK, body: `{"model":"up-b","choices":[]}`})
			logBuf, log := captureLog(zerolog.InfoLevel)
			h := NewHandler(store, kindResolver{direct: pa, proxied: pb}, nil, nil, log)

			rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions", chainChatBody, nil)
			if rec.Code != http.StatusOK {
				t.Fatalf("status = %d, body %s", rec.Code, rec.Body.String())
			}
			if pa.calls() != tc.wantPa || pb.calls() != 1 {
				t.Fatalf("exchanges pa/pb = %d/%d, want %d/1", pa.calls(), pb.calls(), tc.wantPa)
			}
			done := logBuf.events(t, "request_completed")
			if len(done) != 1 || done[0]["provider_attempts"] != tc.wantAttempts ||
				done[0]["retries_total"] != tc.wantRetries || done[0]["final_provider"] != "pb" ||
				done[0]["final_candidate"] != float64(2) {
				t.Fatalf("request_completed = %v, want %v attempts, %v retries, final pb/2", done, tc.wantAttempts, tc.wantRetries)
			}
		})
	}
}

// TestProviderChainRetryBudgetIsPerCandidate pins the non-shared budget: a
// candidate's retry allowance is its own, never a pool the chain draws down.
// Both candidates answer a retryable status, so the walk performs the full
// allowance against EACH of them — (max-retries+1) exchanges per candidate,
// and the client still gets the last received answer.
func TestProviderChainRetryBudgetIsPerCandidate(t *testing.T) {
	stubRetryTiming(t)
	store := newChainStore(t, "")
	pa := newScript(scriptStep{status: http.StatusTooManyRequests, body: `{"error":{"message":"rate limited"}}`})
	pb := newScript(scriptStep{status: http.StatusServiceUnavailable, body: `{"error":{"message":"overloaded"}}`})
	logBuf, log := captureLog(zerolog.InfoLevel)
	h := NewHandler(store, kindResolver{direct: pa, proxied: pb}, nil, nil, log)

	rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions", chainChatBody, nil)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want the last received answer 503 (body %s)", rec.Code, rec.Body.String())
	}
	if pa.calls() != 2 || pb.calls() != 2 {
		t.Fatalf("exchanges pa/pb = %d/%d, want 2/2 — the budget is per candidate", pa.calls(), pb.calls())
	}
	done := logBuf.events(t, "request_completed")
	if len(done) != 1 || done[0]["provider_attempts"] != float64(4) ||
		done[0]["retries_total"] != float64(2) || done[0]["final_provider"] != "pb" ||
		done[0]["final_candidate"] != float64(2) {
		t.Fatalf("request_completed = %v, want 4 attempts / 2 retries / final pb", done)
	}
}

// TestProviderChainRetainedAnswerOverTransportFailure pins the final-error
// rule: the last received HTTP answer is the client's answer even when a
// LATER candidate then fails at the transport level. A provider answered,
// so there is no unreachable 502 to synthesize and provider_exhausted
// stays false — that vocabulary names walks that received no answer at
// all. The answering candidate becomes the final provider.
func TestProviderChainRetainedAnswerOverTransportFailure(t *testing.T) {
	stubRetryTiming(t)
	store := newChainStore(t, "")
	pa := newScript(scriptStep{status: http.StatusTooManyRequests, body: `{"error":{"message":"rate limited"}}`})
	pb := newScript(scriptStep{err: dialError("b.example")})
	logBuf, log := captureLog(zerolog.InfoLevel)
	h := NewHandler(store, kindResolver{direct: pa, proxied: pb}, nil, nil, log)

	rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions", chainChatBody, nil)
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want the retained 429 (body %s)", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"code":"upstream_http_429"`) {
		t.Errorf("body %s, want the canonical 429 envelope", rec.Body.String())
	}
	if strings.Contains(rec.Body.String(), "rate limited") {
		t.Errorf("provider body relayed raw: %s", rec.Body.String())
	}
	done := logBuf.events(t, "request_completed")
	if len(done) != 1 || done[0]["provider_attempts"] != float64(3) || done[0]["retries_total"] != float64(1) ||
		done[0]["final_provider"] != "pa" || done[0]["final_candidate"] != float64(1) {
		t.Fatalf("request_completed = %v, want 3 exchanges, 1 retry, final pa/1", done)
	}
	if _, exhausted := done[0]["provider_exhausted"]; exhausted {
		t.Errorf("provider_exhausted set although a provider answered")
	}
	evs := logBuf.events(t, "upstream_http_error")
	// The second 429's disposition is fallback: the walk moved on to pb —
	// which then failed before answering, and only then did the retained
	// 429 become the client's answer.
	if len(evs) != 2 || evs[0]["disposition"] != "retry" || evs[1]["disposition"] != "fallback" {
		t.Errorf("evidence events = %v, want two with dispositions retry then fallback", evs)
	}
	if n := len(logBuf.events(t, "provider_attempt_failed")); n != 1 {
		t.Errorf("provider_attempt_failed events = %d, want 1 (pb's dial failure)", n)
	}
}

// TestProviderChainRetryAfterFloorAndCap pins the directive's bounds at
// the handler level: Retry-After sizes a same-candidate retry's wait as a
// floor over the backoff, and backoff.max caps it — a hostile
// Retry-After: 3600 cannot stretch the sleep past the configured ceiling.
func TestProviderChainRetryAfterFloorAndCap(t *testing.T) {
	for _, tc := range []struct {
		name      string
		ra        string
		wantDelay time.Duration
	}{
		{"seconds floor over backoff", "1", time.Second},
		{"hours capped at backoff max", "3600", 2 * time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stubRetryTiming(t)
			var waited time.Duration
			retryWait = func(_ context.Context, d time.Duration) bool { waited = d; return true }
			store := newChainStoreRetries(t, "", "    retries:\n      backoff:\n        initial: 100ms\n        max: 2s\n        jitter: 0\n")
			pa := newScript(
				scriptStep{status: http.StatusTooManyRequests, body: `{}`, header: http.Header{"Retry-After": []string{tc.ra}}},
				scriptStep{status: http.StatusOK, body: `{"model":"up-a","choices":[]}`},
			)
			pb := newScript(scriptStep{status: http.StatusOK, body: `{"model":"up-b","choices":[]}`})
			logBuf, log := captureLog(zerolog.InfoLevel)
			h := NewHandler(store, kindResolver{direct: pa, proxied: pb}, nil, nil, log)

			rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions", chainChatBody, nil)
			if rec.Code != http.StatusOK {
				t.Fatalf("status = %d, body %s", rec.Code, rec.Body.String())
			}
			if waited != tc.wantDelay {
				t.Errorf("retry wait = %v, want %v", waited, tc.wantDelay)
			}
			if pa.calls() != 2 || pb.calls() != 0 {
				t.Errorf("exchanges pa/pb = %d/%d, want 2/0 (recovered on the same candidate)", pa.calls(), pb.calls())
			}
			if n := len(logBuf.events(t, "provider_attempt_failed")); n != 0 {
				t.Errorf("%d provider_attempt_failed events, want 0 (the recovery was status-driven)", n)
			}
		})
	}
}

// TestProviderChainCallerGoneDuringRetryWait pins the wait boundary: when
// the caller's context ends during a same-candidate retry sleep, the
// request ends as a disconnect — no second attempt, no fallback, no
// envelope, only the completion record.
func TestProviderChainCallerGoneDuringRetryWait(t *testing.T) {
	stubRetryTiming(t)
	retryWait = func(context.Context, time.Duration) bool { return false }
	store := newChainStore(t, "")
	pa := newScript(scriptStep{status: http.StatusTooManyRequests, body: `{}`})
	pb := newScript(scriptStep{status: http.StatusOK, body: `{"model":"up-b","choices":[]}`})
	logBuf, log := captureLog(zerolog.InfoLevel)
	h := NewHandler(store, kindResolver{direct: pa, proxied: pb}, nil, nil, log)

	rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions", chainChatBody, nil)
	if rec.Body.Len() != 0 {
		t.Errorf("body = %q, want nothing written for a gone caller", rec.Body.String())
	}
	if pa.calls() != 1 || pb.calls() != 0 {
		t.Errorf("exchanges pa/pb = %d/%d, want 1/0 (the walk died in the wait)", pa.calls(), pb.calls())
	}
	done := logBuf.events(t, "request_completed")
	if len(done) != 1 || done[0]["outcome"] != "client_disconnected" || done[0]["provider_attempts"] != float64(1) {
		t.Fatalf("request_completed = %v, want disconnected after one attempt", done)
	}
}

// TestProviderChainMalformed200RetriesThenFallsBack pins the
// pre-commitment direction of the retryable set: an unparseable 200 body
// is an attempt failure, not a commitment — the candidate is retried,
// then the walk falls back, and the client sees the healthy candidate's
// rewritten answer. Each discarded answer fires one
// upstream_invalid_response WARN with its disposition.
func TestProviderChainMalformed200RetriesThenFallsBack(t *testing.T) {
	stubRetryTiming(t)
	store := newChainStore(t, "")
	pa := newScript(scriptStep{status: http.StatusOK, body: `not json at all`})
	pb := newScript(scriptStep{status: http.StatusOK, body: `{"model":"up-b","choices":[]}`})
	logBuf, log := captureLog(zerolog.InfoLevel)
	h := NewHandler(store, kindResolver{direct: pa, proxied: pb}, nil, nil, log)

	rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions", chainChatBody, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"model":"chain-model"`) {
		t.Errorf("response not rewritten to the public name: %s", rec.Body.String())
	}
	if pa.calls() != 2 || pb.calls() != 1 {
		t.Fatalf("exchanges pa/pb = %d/%d, want 2/1", pa.calls(), pb.calls())
	}
	evs := logBuf.events(t, "upstream_invalid_response")
	if len(evs) != 2 || evs[0]["disposition"] != "retry" || evs[1]["disposition"] != "fallback" {
		t.Fatalf("evidence events = %v, want two with dispositions retry then fallback", evs)
	}
	if evs[1]["reason"] != "upstream_invalid_response" {
		t.Errorf("reason = %v, want the typed token", evs[1]["reason"])
	}
}

// TestProviderChainMalformed200Exhausted pins the exhaustion direction of
// the same shape: with fallback disabled the spent budget finalizes the
// walk on the unusable answer — the canonical 502, never half a provider
// body, with the dedicated outcome.
func TestProviderChainMalformed200Exhausted(t *testing.T) {
	stubRetryTiming(t)
	store := newChainStore(t, "provider-fallback:\n  enabled: false\n")
	pa := newScript(scriptStep{status: http.StatusOK, body: `not json at all`})
	logBuf, log := captureLog(zerolog.InfoLevel)
	h := NewHandler(store, kindResolver{direct: pa, proxied: nil}, nil, nil, log)

	rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions", chainChatBody, nil)
	if rec.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502", rec.Code)
	}
	if rec.Body.String() != envelopeUpInvalid {
		t.Errorf("body = %s, want the canonical invalid-upstream envelope", rec.Body.String())
	}
	if pa.calls() != 2 {
		t.Errorf("pa calls = %d, want 2 (initial + the default one retry)", pa.calls())
	}
	done := logBuf.events(t, "request_completed")
	if len(done) != 1 || done[0]["outcome"] != "upstream_invalid_response" {
		t.Fatalf("request_completed = %v, want outcome upstream_invalid_response", done)
	}
}

// TestProviderChainRetryReplaysIdenticalBody pins transform isolation:
// every retry attempt transforms the SAME immutable client body again —
// no accumulation across attempts, each request carrying the candidate's
// own upstream model and the same injected prompt.
func TestProviderChainRetryReplaysIdenticalBody(t *testing.T) {
	stubRetryTiming(t)
	store := newChainStoreRetries(t, "", "    retries:\n      max-retries: 2\n")
	pa := newScript(
		scriptStep{status: http.StatusServiceUnavailable, body: `{}`},
		scriptStep{status: http.StatusServiceUnavailable, body: `{}`},
		scriptStep{status: http.StatusOK, body: `{"model":"up-a","choices":[]}`},
	)
	pb := newScript(scriptStep{status: http.StatusOK, body: `{"model":"up-b","choices":[]}`})
	logBuf, log := captureLog(zerolog.InfoLevel)
	h := NewHandler(store, kindResolver{direct: pa, proxied: pb}, nil, nil, log)

	rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions", chainChatBody, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body %s", rec.Code, rec.Body.String())
	}
	if pa.calls() != 3 || pb.calls() != 0 {
		t.Fatalf("exchanges pa/pb = %d/%d, want 3/0 (recovered on the third attempt)", pa.calls(), pb.calls())
	}
	bodies := pa.allBodies()
	for i, b := range bodies {
		if b != bodies[0] {
			t.Errorf("attempt %d body differs from attempt 1: %s vs %s", i+1, b, bodies[0])
		}
		if !strings.Contains(b, `"model":"up-a"`) || !strings.Contains(b, "CHAIN-PROMPT-MARKER") {
			t.Errorf("attempt %d lost the candidate transform: %s", i+1, b)
		}
	}
	done := logBuf.events(t, "request_completed")
	if len(done) != 1 || done[0]["provider_attempts"] != float64(3) || done[0]["retries_total"] != float64(2) {
		t.Errorf("request_completed = %v, want 3 attempts, 2 retries", done)
	}
}

// TestProviderChainReloadMidRetryKeepsSnapshotPolicy pins snapshot
// binding: a reload that lands while a request sleeps between retries
// cannot change the policy that request walks under — its budget was
// decided against its own snapshot, and its completion record still names
// that snapshot's generation. The next request walks under the new one.
func TestProviderChainReloadMidRetryKeepsSnapshotPolicy(t *testing.T) {
	store := newChainStoreRetries(t, "", "    retries:\n      max-retries: 2\n      backoff:\n        initial: 5ms\n        max: 10ms\n        jitter: 0\n")
	pa := newScript(scriptStep{status: http.StatusServiceUnavailable, body: `{}`})
	pb := newScript(scriptStep{status: http.StatusOK, body: `{"model":"up-b","choices":[]}`})
	logBuf, log := captureLog(zerolog.InfoLevel)
	h := NewHandler(store, kindResolver{direct: pa, proxied: pb}, nil, nil, log)

	// The FIRST retry sleep holds until the reload has landed, then lets
	// the request continue under the OLD policy (two retries). Later waits
	// of the same request pass straight through — the hold is armed once.
	inWait := make(chan struct{})
	release := make(chan struct{})
	armed := atomic.Bool{}
	armed.Store(true)
	origWait := retryWait
	retryWait = func(ctx context.Context, d time.Duration) bool {
		if !origWait(ctx, d) {
			return false
		}
		if !armed.CompareAndSwap(true, false) {
			return true
		}
		select {
		case inWait <- struct{}{}:
		case <-ctx.Done():
			return false
		}
		<-release
		return true
	}
	defer func() { retryWait = origWait }()

	done := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(chainChatBody))
		req.Header.Set("Authorization", "Bearer "+testAPIKey)
		h.ServeHTTP(rec, req)
		done <- rec
	}()
	<-inWait
	// Reload: the same chain, the retry budget cut to zero.
	next, err := config.LoadRuntime([]byte("api-key: " + testAPIKey + "\n" + `
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
    retries:
      max-retries: 0
    providers:
      - provider: pa
        upstream-model: up-a
      - provider: pb
        upstream-model: up-b
`))
	if err != nil {
		t.Fatalf("reload LoadRuntime: %v", err)
	}
	if err := store.Publish(next); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	close(release)
	rec := <-done
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body %s", rec.Code, rec.Body.String())
	}
	// The OLD policy walked: three pa exchanges (initial + two retries)
	// then pb — the zero-retry policy would have moved on after one.
	if pa.calls() != 3 || pb.calls() != 1 {
		t.Fatalf("exchanges pa/pb = %d/%d, want 3/1 (the old policy, not the reloaded one)", pa.calls(), pb.calls())
	}
	completed := logBuf.events(t, "request_completed")
	if len(completed) != 1 || completed[0]["config_generation"] != float64(0) || completed[0]["provider_attempts"] != float64(4) {
		t.Fatalf("request_completed = %v, want generation 0 with 4 attempts", completed)
	}

	// The next request is bound to the reloaded snapshot and walks the
	// new budget: one pa exchange, straight to pb.
	pa2 := newScript(scriptStep{status: http.StatusServiceUnavailable, body: `{}`})
	pb2 := newScript(scriptStep{status: http.StatusOK, body: `{"model":"up-b","choices":[]}`})
	h2 := NewHandler(store, kindResolver{direct: pa2, proxied: pb2}, nil, nil, log)
	rec2 := doRequest(t, h2, http.MethodPost, "/v1/chat/completions", chainChatBody, nil)
	if rec2.Code != http.StatusOK {
		t.Fatalf("second request status = %d", rec2.Code)
	}
	if pa2.calls() != 1 || pb2.calls() != 1 {
		t.Errorf("second request exchanges pa/pb = %d/%d, want 1/1 (the new policy)", pa2.calls(), pb2.calls())
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
	stubRetryTiming(t)
	waits := 0
	retryWait = func(_ context.Context, _ time.Duration) bool { waits++; return true }
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
	if waits != 0 {
		t.Errorf("retry waits after client cancellation = %d, want 0", waits)
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

// TestProviderChainStreamDeathAfterCommitment pins the streaming half of
// the no-retry-after-commitment rule: a 200 SSE answer commits the walk at
// its HEADERS — the body is not read in-walk — so an upstream that dies
// before (or between) events truncates a committed stream. No same-
// candidate retry, no candidate switch, no error envelope: the client's
// response was already the 200 stream.
func TestProviderChainStreamDeathAfterCommitment(t *testing.T) {
	stubRetryTiming(t)
	store := newChainStore(t, "")
	// errBody (logging_test.go) yields the one event, then errors — an
	// upstream that dies mid-stream after its headers committed the walk.
	pa := doerFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
			Body:       io.NopCloser(&errBody{data: []byte("data: {\"model\":\"up-a\",\"choices\":[{\"delta\":{\"content\":\"partial\"}}]}\n\n")}),
		}, nil
	})
	pb := &fakeUpstream{status: http.StatusOK, body: `{"model":"up-b","choices":[]}`}
	logBuf, log := captureLog(zerolog.InfoLevel)
	h := NewHandler(store, kindResolver{direct: pa, proxied: pb}, nil, nil, log)

	rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions",
		`{"model":"chain-model","stream":true,"messages":[{"role":"user","content":"hi"}]}`, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want the committed 200", rec.Code)
	}
	body := rec.Body.String()
	if !strings.Contains(body, `"model":"chain-model"`) {
		t.Errorf("relayed events wrong: %s", body)
	}
	if strings.Contains(body, "data: [DONE]") {
		t.Errorf("truncated stream somehow terminated cleanly: %s", body)
	}
	if pb.calls() != 0 {
		t.Errorf("fallback dialed after commitment: %d calls", pb.calls())
	}
	done := logBuf.events(t, "request_completed")
	if len(done) != 1 || done[0]["stream"] != true || done[0]["provider_attempts"] != float64(1) {
		t.Errorf("request_completed = %v, want one streamed attempt", done)
	}
	trunc := logBuf.events(t, "stream_truncated")
	if len(trunc) != 1 || trunc[0]["phase"] != "upstream_read" {
		t.Errorf("stream_truncated = %v, want one upstream_read truncation", trunc)
	}
}

// doerFunc adapts a function to the Doer seam.
type doerFunc func(*http.Request) (*http.Response, error)

func (f doerFunc) Do(req *http.Request) (*http.Response, error) { return f(req) }

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
	stubRetryTiming(t)
	waits := 0
	retryWait = func(_ context.Context, _ time.Duration) bool { waits++; return true }
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
	if waits != 0 {
		t.Errorf("retry waits after caller deadline = %d, want 0", waits)
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

// TestProviderWalkEngineRecoveryActions pins the engine-owned status walk at
// the handler boundary: a retryable answer waits once and re-asks the SAME
// candidate; a fallback-only answer moves immediately; and terminal rows do
// neither. It also checks that the evidence carries the matrix identity and
// effective policy data that explain the action without exposing the policy.
func TestProviderWalkEngineRecoveryActions(t *testing.T) {
	t.Run("retry waits and reasks the same candidate", func(t *testing.T) {
		stubRetryTiming(t)
		var waits []time.Duration
		retryWait = func(_ context.Context, d time.Duration) bool {
			waits = append(waits, d)
			return true
		}
		store := newChainStoreRetries(t, "", "    retries:\n      backoff:\n        initial: 125ms\n        max: 125ms\n        jitter: 0\n")
		pa := newScript(
			scriptStep{status: http.StatusTooManyRequests, body: `{}`},
			scriptStep{status: http.StatusOK, body: `{"model":"up-a","choices":[]}`},
		)
		pb := newScript(scriptStep{status: http.StatusOK, body: `{"model":"up-b","choices":[]}`})
		logs, log := captureLog(zerolog.InfoLevel)
		rec := doRequest(t, NewHandler(store, kindResolver{direct: pa, proxied: pb}, nil, nil, log), http.MethodPost, "/v1/chat/completions", chainChatBody, nil)
		if rec.Code != http.StatusOK || pa.calls() != 2 || pb.calls() != 0 {
			t.Fatalf("status/calls = %d/%d/%d, want 200/2/0", rec.Code, pa.calls(), pb.calls())
		}
		if len(waits) != 1 || waits[0] != 125*time.Millisecond {
			t.Fatalf("waits = %v, want one 125ms wait", waits)
		}
		evs := logs.events(t, "upstream_http_error")
		if len(evs) != 1 || evs[0]["disposition"] != "retry" || evs[0]["policy_rule_id"] != "http-429" || evs[0]["reason"] != "http_429" {
			t.Errorf("429 evidence = %v, want retry/status-429/http_429", evs)
		}
		// 31, not 15: the field is named for the REQUEST envelope, and the
		// default request envelope is 32 while the candidate's is 16. A field
		// reporting the tighter of the two would answer with the candidate's
		// remainder under a request's name, hiding three quarters of the
		// headroom the request was granted.
		if evs[0]["policy_hash"] == "" || evs[0]["policy_generation"] != float64(0) || evs[0]["upstream_exchange"] != float64(1) || evs[0]["request_exchange_budget_remaining"] != float64(31) {
			t.Errorf("retry observability = %v", evs[0])
		}
		done := logs.events(t, "request_completed")
		if len(done) != 1 || done[0]["candidates_entered"] != float64(1) || done[0]["candidate_attempts"] != float64(2) || done[0]["retry_attempts"] != float64(1) || done[0]["upstream_exchanges"] != float64(2) {
			t.Errorf("completion counters = %v", done)
		}
	})

	t.Run("fallback-only answer never waits", func(t *testing.T) {
		stubRetryTiming(t)
		waits := 0
		retryWait = func(_ context.Context, _ time.Duration) bool { waits++; return true }
		store := newChainStore(t, "")
		pa := newScript(scriptStep{status: http.StatusUnauthorized, body: `{}`})
		pb := newScript(scriptStep{status: http.StatusOK, body: `{"model":"up-b","choices":[]}`})
		logs, log := captureLog(zerolog.InfoLevel)
		rec := doRequest(t, NewHandler(store, kindResolver{direct: pa, proxied: pb}, nil, nil, log), http.MethodPost, "/v1/chat/completions", chainChatBody, nil)
		if rec.Code != http.StatusOK || pa.calls() != 1 || pb.calls() != 1 || waits != 0 {
			t.Fatalf("status/calls/waits = %d/%d/%d/%d, want 200/1/1/0", rec.Code, pa.calls(), pb.calls(), waits)
		}
		evs := logs.events(t, "upstream_http_error")
		if len(evs) != 1 || evs[0]["disposition"] != "fallback" || evs[0]["policy_rule_id"] != "http-401" {
			t.Errorf("401 evidence = %v, want fallback/status-401", evs)
		}
	})

	for _, status := range []int{http.StatusBadRequest, http.StatusNotImplemented, http.StatusHTTPVersionNotSupported} {
		t.Run(fmt.Sprintf("terminal %d never reasks", status), func(t *testing.T) {
			stubRetryTiming(t)
			waits := 0
			retryWait = func(_ context.Context, _ time.Duration) bool { waits++; return true }
			store := newChainStore(t, "")
			pa := newScript(scriptStep{status: status, body: `{}`})
			pb := newScript(scriptStep{status: http.StatusOK, body: `{"model":"up-b","choices":[]}`})
			logs, log := captureLog(zerolog.InfoLevel)
			rec := doRequest(t, NewHandler(store, kindResolver{direct: pa, proxied: pb}, nil, nil, log), http.MethodPost, "/v1/chat/completions", chainChatBody, nil)
			if rec.Code != status || pa.calls() != 1 || pb.calls() != 0 || waits != 0 {
				t.Fatalf("status/calls/waits = %d/%d/%d/%d, want %d/1/0/0", rec.Code, pa.calls(), pb.calls(), waits, status)
			}
			evs := logs.events(t, "upstream_http_error")
			if len(evs) != 1 || evs[0]["disposition"] != "terminal" || evs[0]["policy_rule_id"] != terminalRuleID(status) {
				t.Errorf("terminal %d evidence = %v", status, evs)
			}
		})
	}
}

// TestProviderWalkEntryBudgetStopsBeforeLongerChain proves the engine's
// EnterCandidate gate is the only candidate counter: a chain longer than the
// primary policy's fallback reach cannot execute an extra candidate even
// though the YAML contains one, and the completion count names entries, not
// an accidental count of iterations.
func TestProviderWalkEntryBudgetStopsBeforeLongerChain(t *testing.T) {
	stubRetryTiming(t)
	cfg := "api-key: " + testAPIKey + `
recovery:
  fallback:
    max-candidates: 2
transports:
  direct:
    type: direct
providers:
  pa:
    base-url: https://a.example/v1
    transport: direct
  pb:
    base-url: https://b.example/v1
    transport: direct
  pc:
    base-url: https://c.example/v1
    transport: direct
models:
  chain-model:
    providers:
      - provider: pa
        upstream-model: up-a
      - provider: pb
        upstream-model: up-b
      - provider: pc
        upstream-model: up-c
`
	snap, err := config.LoadRuntime([]byte(cfg))
	if err != nil {
		t.Fatalf("LoadRuntime: %v", err)
	}
	pa := &fakeUpstream{err: dialError("a.example")}
	pb := &fakeUpstream{err: dialError("b.example")}
	pc := &fakeUpstream{status: http.StatusOK, body: `{"model":"up-c","choices":[]}`}
	logs, log := captureLog(zerolog.InfoLevel)
	resolver := fixedDoer{d: doerFunc(func(req *http.Request) (*http.Response, error) {
		switch req.URL.Host {
		case "a.example":
			return pa.Do(req)
		case "b.example":
			return pb.Do(req)
		default:
			return pc.Do(req)
		}
	})}
	rec := doRequest(t, NewHandler(config.NewStore(snap), resolver, nil, nil, log), http.MethodPost, "/v1/chat/completions", chainChatBody, nil)
	if rec.Code != http.StatusBadGateway || pa.calls() != 1 || pb.calls() != 1 || pc.calls() != 0 {
		t.Fatalf("status/calls = %d/%d/%d/%d, want 502/1/1/0", rec.Code, pa.calls(), pb.calls(), pc.calls())
	}
	done := logs.events(t, "request_completed")
	if len(done) != 1 || done[0]["candidates_entered"] != float64(2) || done[0]["candidate_attempts"] != float64(2) {
		t.Errorf("completion = %v, want two entered/two attempts", done)
	}
}

// terminalRuleID states the shipped default matrix's exact rule identity for
// each terminal row the handler-level test walks. The broad 4xx catch-all is
// deliberately one named rule; 501/505 have their own 5xx carve-out rows.
func terminalRuleID(status int) string {
	if status == http.StatusBadRequest {
		return "http-class-4xx"
	}
	return fmt.Sprintf("http-%d", status)
}

// TestProviderWalkOversizedAnswerIsItsOwnProtocolCause pins that the
// oversized_response shorthand is a policy an operator can actually reach.
// An over-cap 200 and an unparseable 200 are both unusable answers, but they
// are different decisions: a provider whose every answer exceeds this proxy's
// buffer will answer the same way on a re-ask, while a malformed one may not.
// The handler must classify them apart, or the narrower rule silently never
// fires while changing the policy hash.
func TestProviderWalkOversizedAnswerIsItsOwnProtocolCause(t *testing.T) {
	stubRetryTiming(t)
	old := maxBufferedResponseBytes
	maxBufferedResponseBytes = 1 << 10 // 1 KiB for the test
	defer func() { maxBufferedResponseBytes = old }()

	cfg := "api-key: " + testAPIKey + `
recovery:
  fallback:
    max-candidates: 2
  matrix:
    protocol:
      oversized_response: fallback
      invalid_response: terminal
transports:
  t-direct:
    type: direct
providers:
  pa:
    base-url: https://a.example/v1
    transport: t-direct
  pb:
    base-url: https://b.example/v1
    transport: t-direct
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
	big := `{"model":"up-a","padding":"` + strings.Repeat("x", 4<<10) + `"}`
	pa := &fakeUpstream{status: http.StatusOK, body: big}
	pb := &fakeUpstream{status: http.StatusOK, body: `{"model":"up-b","choices":[]}`}
	logs, log := captureLog(zerolog.InfoLevel)
	resolver := poolKindResolver{pool: pb, plain: doerFunc(func(req *http.Request) (*http.Response, error) {
		if req.URL.Host == "a.example" {
			return pa.Do(req)
		}
		return pb.Do(req)
	})}
	h := NewHandler(config.NewStore(snap), resolver, nil, nil, log)
	rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions", chainChatBody, nil)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"model":"chain-model"`) {
		t.Fatalf("response = %d %s, want 200 from the fallback candidate", rec.Code, rec.Body.String())
	}
	if pa.calls() != 1 || pb.calls() != 1 {
		t.Errorf("calls = %d/%d, want 1/1 (an oversized answer falls back without a re-ask)", pa.calls(), pb.calls())
	}
	ev := logs.events(t, "upstream_invalid_response")
	if len(ev) != 1 || ev[0]["policy_rule_id"] != "protocol-oversized_response" || ev[0]["disposition"] != "fallback" {
		t.Errorf("oversized evidence = %v, want protocol-oversized_response/fallback", ev)
	}
}

// TestProviderWalkSpentCandidateEnvelopeStillFallsBack is the boundary
// between the two exchange envelopes. Provider A's transport is a pool that
// fans ONE provider attempt out into two real dials; with a two-exchange
// candidate envelope, that single attempt spends the candidate's whole
// envelope in one go — before A's one permitted same-candidate retry could
// use it. A spent CANDIDATE envelope forbids another exchange on A (a retry
// would be an over-budget dial), but it is not a verdict about the walk: the
// request-wide envelope has room, so the policy's fallback still reaches B,
// whose own envelope opens fresh. Treating the candidate ceiling as terminal
// would let a per-candidate number silently override the configured
// fallback — the envelope sizes one candidate's retries, it never pins the
// chain.
func TestProviderWalkSpentCandidateEnvelopeStillFallsBack(t *testing.T) {
	stubRetryTiming(t)
	cfg := "api-key: " + testAPIKey + `
recovery:
  retries:
    max-retries: 1
  budget:
    candidate:
      max-exchanges: 2
transports:
  t-pool:
    type: pool
    members: [m1]
  m1:
    type: direct
  t-direct:
    type: direct
providers:
  pa:
    base-url: https://a.example/v1
    transport: t-pool
  pb:
    base-url: https://b.example/v1
    transport: t-direct
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
	pa := &budgetScriptExecutor{perCall: 2, terminal: errors.New("pool failed")}
	pb := &fakeUpstream{status: http.StatusOK, body: `{"model":"up-b","choices":[]}`}
	logs, log := captureLog(zerolog.InfoLevel)
	// The pool-shaped resolver: A's transport is the pool, B's is plain
	// direct, so the two candidates must not share a Doer.
	resolver := poolKindResolver{pool: pa, plain: pb}
	h := NewHandler(config.NewStore(snap), resolver, nil, nil, log)
	rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions", chainChatBody, nil)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"model":"chain-model"`) {
		t.Fatalf("response = %d %s, want 200 from the fallback candidate", rec.Code, rec.Body.String())
	}
	// A was asked exactly once: the envelope refused the retry the matrix
	// would otherwise have authorized.
	if calls, dials := pa.counts(); calls != 1 || dials != 2 {
		t.Errorf("A calls/dials = %d/%d, want 1/2 (the retry was over budget)", calls, dials)
	}
	if pb.calls() != 1 {
		t.Errorf("B calls = %d, want 1 (the spent candidate envelope must not pin the chain)", pb.calls())
	}
	done := logs.events(t, "request_completed")
	if len(done) != 1 || done[0]["outcome"] != "completed" || done[0]["candidates_entered"] != float64(2) ||
		done[0]["final_provider"] != "pb" {
		t.Errorf("completion = %v, want a completed two-candidate walk ending on pb", done)
	}
	// Both real dials carry their record; the envelope refusal is not one of
	// them, and it is not an endpoint verdict either — egress_exhausted means
	// the pool found no usable member, and A's member was never unusable.
	eg := logs.events(t, "egress_attempt_failed")
	if len(eg) != 2 || eg[0]["egress_target"] != "direct" || eg[1]["upstream_exchange"] != float64(2) {
		t.Errorf("egress evidence = %v, want one record per real dial", eg)
	}
	failed := logs.events(t, "provider_attempt_failed")
	if len(failed) != 1 || failed[0]["provider"] != "pa" || failed[0]["disposition"] != "fallback" {
		t.Errorf("provider_attempt_failed = %v, want one fallback from pa", failed)
	}
}

// poolKindResolver answers pool-shaped configs with the scripted executor
// and everything else with the plain Doer. kindResolver splits on Proxy vs
// not, which cannot separate a pool from a direct transport.
type poolKindResolver struct {
	pool  transport.Doer
	plain transport.Doer
}

func (r poolKindResolver) Doer(c transport.Config) transport.Doer {
	if c.Kind == transport.EgressPool {
		return r.pool
	}
	return r.plain
}

// budgetScriptExecutor stands in for a pool with a scripted number of real
// outbound exchanges. It claims the shared exchange envelope immediately
// before each exchange, just as poolDoer does; the test can therefore prove
// a request-wide ceiling against traffic that fans one provider attempt out
// into several egress dials.
type budgetScriptExecutor struct {
	mu       sync.Mutex
	calls    int
	dials    int
	perCall  int
	status   int
	terminal error
}

func (e *budgetScriptExecutor) Execute(ar *transport.AttemptRequest) (*http.Response, transport.AttemptInfo, error) {
	e.mu.Lock()
	e.calls++
	e.mu.Unlock()
	info := transport.AttemptInfo{Kind: "direct", Target: "direct"}
	for i := 0; i < e.perCall; i++ {
		if ar.Budget != nil && !ar.Budget.ConsumeExchange() {
			info.BudgetExhausted = true
			if info.Attempts == 0 {
				// The same distinction poolDoer draws: an envelope that
				// refused the first dial is the request's own condition, not
				// an endpoint verdict, so no exhaustion sentinel is raised.
				return nil, info, nil
			}
			return nil, info, e.terminal
		}
		e.mu.Lock()
		e.dials++
		e.mu.Unlock()
		info.Attempts++
		// One sanitized record per real dial, as the real pool emits: a
		// scripted executor that dialed and failed must look like one.
		info.Failures = append(info.Failures, transport.AttemptFailure{
			Kind: info.Kind, Target: info.Target, Class: "connection", Cause: "connection_refused",
		})
	}
	if e.status != 0 {
		return &http.Response{
			StatusCode: e.status,
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body:       io.NopCloser(strings.NewReader(`{}`)),
		}, info, nil
	}
	return nil, info, e.terminal
}

func (e *budgetScriptExecutor) counts() (calls, dials int) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.calls, e.dials
}

func (e *budgetScriptExecutor) Do(*http.Request) (*http.Response, error) {
	panic("handler called Do on an Executor-capable budget script")
}

// TestProviderWalkRequestExchangeEnvelopeCountsRealPoolDials proves the
// envelope bounds exchanges rather than provider attempts. A's first and
// retried 429 each consume three pool dials (six); B starts with two dials
// planned, receives the seventh, and its eighth claim is refused. The
// handler stops before B can dial again. A's final received 429 remains the
// client answer: no synthetic 502 outranks a retained HTTP answer.
func TestProviderWalkRequestExchangeEnvelopeCountsRealPoolDials(t *testing.T) {
	stubRetryTiming(t)
	cfg := "api-key: " + testAPIKey + `
recovery:
  budget:
    request:
      max-exchanges: 7
    candidate:
      max-exchanges: 7
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
	pa := &budgetScriptExecutor{perCall: 3, status: http.StatusTooManyRequests, terminal: errors.New("pool failed")}
	pb := &budgetScriptExecutor{perCall: 2, terminal: errors.New("pool failed")}
	logs, log := captureLog(zerolog.InfoLevel)
	h := NewHandler(config.NewStore(snap), kindResolver{direct: pa, proxied: pb}, nil, nil, log)
	rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions", chainChatBody, nil)
	if rec.Code != http.StatusTooManyRequests || !strings.Contains(rec.Body.String(), `"code":"upstream_http_429"`) {
		t.Fatalf("response = %d %s, want retained canonical 429", rec.Code, rec.Body.String())
	}
	if calls, dials := pa.counts(); calls != 2 || dials != 6 {
		t.Errorf("A calls/dials = %d/%d, want 2/6", calls, dials)
	}
	if calls, dials := pb.counts(); calls != 1 || dials != 1 {
		t.Errorf("B calls/dials = %d/%d, want 1/1 (eighth refused)", calls, dials)
	}
	_, ad := pa.counts()
	_, bd := pb.counts()
	if ad+bd != 7 {
		t.Errorf("real exchanges = %d, want request ceiling 7", ad+bd)
	}
	// A retained HTTP answer wins before the no-answer exhaustion branch, so
	// no synthetic upstream_request_failed event is emitted for the refusal.
	if failed := logs.events(t, "upstream_request_failed"); len(failed) != 0 {
		t.Errorf("synthetic exhaustion evidence = %v, want none behind retained 429", failed)
	}
	done := logs.events(t, "request_completed")
	if len(done) != 1 || done[0]["candidates_entered"] != float64(2) || done[0]["candidate_attempts"] != float64(3) || done[0]["retry_attempts"] != float64(1) || done[0]["upstream_exchanges"] != float64(7) {
		t.Errorf("completion = %v, want entries 2 attempts 3 retries 1 exchanges 7", done)
	}
}
