package proxy

import (
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"openai-compatible-injector/internal/config"
	"openai-compatible-injector/internal/credential"
)

// The credential seam: one key per attempt, acquired before the transport
// runs; a 429 marks the key; the recovery engine decides what a walk does
// about a pool with no ready key. These tests pin the seam's contract —
// what reaches the wire, what rotates, what is bounded, and what never
// leaks.
//
// credFrozen matches the instant stubRetryTiming freezes the walk's clock
// to, so pool cooldown deadlines written by the handler and by the tests
// land on the same timeline and every assertion is exact.
var credFrozen = time.Date(2026, time.September, 23, 12, 0, 0, 0, time.UTC)

// credAuthBlock is the auth block the cred chain's pa provider carries:
// two keys under the default round-robin, Authorization/Bearer surface,
// default cooldown policy (2s / 60s) unless the test overrides.
const credAuthBlock = `    auth:
      type: api_key
      header: Authorization
      prefix: "Bearer "
      strategy: round_robin
      keys:
        - id: kilo-1
          value: sk-test-one
        - id: kilo-2
          value: sk-test-two
`

// newCredChainStore is the standard two-candidate chain (pa direct, pb
// proxied) with an auth block on pa only — pb exercises the no-credential
// candidate path in the same walk.
func newCredChainStore(t *testing.T, paAuth string) *config.Store {
	t.Helper()
	snap, err := config.LoadRuntime([]byte("api-key: " + testAPIKey + "\n" + `
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
` + paAuth + `  pb:
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
	return config.NewStore(snap)
}

// credPool resolves pa's pool out of the store's chain, the way the handler
// would, so a test can pre-mark or inspect the exact rotation state a
// request will see.
func credPool(t *testing.T, store *config.Store, creds *credential.Registry) *credential.Pool {
	t.Helper()
	m, ok := store.Load().Model("chain-model")
	if !ok {
		t.Fatal("chain-model not found")
	}
	if m.Chain[0].Cred == nil {
		t.Fatal("pa candidate carries no credential")
	}
	return creds.Pool(m.Chain[0].Cred)
}

// credStep is one scripted answer with an optional mid-read body failure —
// the shape a stalled or dying 429 body needs.
type credStep struct {
	status  int
	body    string
	header  http.Header
	bodyErr error
}

// authScript records the Authorization header of every request it receives
// and answers from a fixed script, last step repeating — the credential
// seam's receipt at the wire.
type authScript struct {
	mu    sync.Mutex
	auth  []string
	steps []credStep
}

func (a *authScript) Do(req *http.Request) (*http.Response, error) {
	a.mu.Lock()
	i := len(a.auth)
	a.auth = append(a.auth, req.Header.Get("Authorization"))
	step := a.steps[len(a.steps)-1]
	if i < len(a.steps) {
		step = a.steps[i]
	}
	a.mu.Unlock()
	if step.bodyErr != nil && step.body == "" {
		// A body that fails on the first read.
		return &http.Response{
			StatusCode: step.status,
			Header:     withCT(step.header),
			Body:       io.NopCloser(errOnce(step.bodyErr)),
			Request:    req,
		}, nil
	}
	return &http.Response{
		StatusCode: step.status,
		Header:     withCT(step.header),
		Body:       io.NopCloser(strings.NewReader(step.body)),
		Request:    req,
	}, nil
}

func (a *authScript) calls() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]string(nil), a.auth...)
}

func withCT(h http.Header) http.Header {
	out := h.Clone()
	if out == nil {
		out = http.Header{}
	}
	if out.Get("Content-Type") == "" {
		out.Set("Content-Type", "application/json")
	}
	return out
}

func errOnce(err error) io.Reader {
	return errReader{err: err}
}

type errReader struct{ err error }

func (r errReader) Read([]byte) (int, error) { return 0, r.err }

// okAnswer is the 200 a successful attempt ends on.
func okAnswer() credStep {
	return credStep{status: http.StatusOK, body: `{"model":"up-a","choices":[]}`}
}

// TestCredentialHeaderInjectedAndClientCannotOverride pins the wire
// surface: the pool's key reaches the upstream under the configured header
// and prefix, and the caller's own client api-key — presented in the same
// header name to THIS proxy — cannot ride along: Authorization is not in
// the forward allow-list, so the only Authorization on the wire is the one
// the pool composed.
func TestCredentialHeaderInjectedAndClientCannotOverride(t *testing.T) {
	stubRetryTiming(t)
	store := newCredChainStore(t, credAuthBlock)
	pa := &authScript{steps: []credStep{okAnswer()}}
	creds := credential.NewRegistry()
	h := NewHandler(store, kindResolver{direct: pa, proxied: pa}, creds, nil, nil, quietLogger())

	rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions", chainChatBody,
		map[string]string{"Authorization": "Bearer " + testAPIKey})
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body %s", rec.Code, rec.Body.String())
	}
	got := pa.calls()
	if len(got) != 1 {
		t.Fatalf("upstream received %d calls, want 1: %v", len(got), got)
	}
	if got[0] != "Bearer sk-test-one" {
		t.Errorf("upstream Authorization = %q, want the pool's first key", got[0])
	}
	if strings.Contains(got[0], testAPIKey) {
		t.Error("the client's api-key reached the upstream alongside the pool key")
	}
}

// TestCredentialStickyAcrossSameCandidateRetry pins the same-candidate
// rule: a 5xx retry is the same logical attempt on the same account — the
// re-ask goes out with the SAME key (never rotated), and the sticky
// hand-back does not move the rotation cursor (the pool's next
// no-preference acquire is still the key after the first).
func TestCredentialStickyAcrossSameCandidateRetry(t *testing.T) {
	stubRetryTiming(t)
	store := newCredChainStore(t, credAuthBlock+"      rate-limit:\n        cooldown: 3s\n")
	pa := &authScript{steps: []credStep{
		{status: http.StatusServiceUnavailable, body: `{}`},
		okAnswer(),
	}}
	creds := credential.NewRegistry()
	pool := credPool(t, store, creds)
	h := NewHandler(store, kindResolver{direct: pa, proxied: pa}, creds, nil, nil, quietLogger())

	rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions", chainChatBody, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body %s", rec.Code, rec.Body.String())
	}
	got := pa.calls()
	if len(got) != 2 || got[0] != "Bearer sk-test-one" || got[1] != "Bearer sk-test-one" {
		t.Fatalf("retry Authorization sequence = %v, want the same key twice", got)
	}
	// The sticky hand-back left the cursor where the first acquire put it.
	if k, ok := pool.Acquire(credFrozen, ""); !ok || k.ID != "kilo-2" {
		t.Errorf("post-retry cursor acquire = %q, %v; want kilo-2 (cursor untouched)", k.ID, ok)
	}
}

// TestCredentialAllCoolingFallsThroughWithoutDialing pins the no-ready-key
// path end to end: the blocked attempts are decided by the engine (one
// credential_unavailable event each), consume NO exchange, keep their real
// attempt numbering, and exhaust the candidate's retry budget into the
// on-exhausted fallback — the walk lands on the uncredentialed next
// candidate and the client gets its 200. No busy loop, no dial to the
// cooling provider, no invented answer.
func TestCredentialAllCoolingFallsThroughWithoutDialing(t *testing.T) {
	stubRetryTiming(t)
	store := newCredChainStore(t, credAuthBlock)
	pa := &authScript{steps: []credStep{okAnswer()}}
	pb := &authScript{steps: []credStep{okAnswer()}}
	creds := credential.NewRegistry()
	pool := credPool(t, store, creds)
	pool.MarkRateLimited(credFrozen, "kilo-1", time.Minute)
	pool.MarkRateLimited(credFrozen, "kilo-2", time.Minute)
	buf, log := captureLog(zerolog.DebugLevel)
	h := NewHandler(store, kindResolver{direct: pa, proxied: pb}, creds, nil, nil, log)

	rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions", chainChatBody, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body %s", rec.Code, rec.Body.String())
	}
	if got := pa.calls(); len(got) != 0 {
		t.Errorf("the cooling provider was dialed %d times, want 0", len(got))
	}
	cooled := buf.events(t, "credential_unavailable")
	if len(cooled) != 2 {
		t.Fatalf("credential_unavailable events = %d, want 2 (one per blocked attempt): %s", len(cooled), buf.String())
	}
	for i, ev := range cooled {
		if ev["error_cause"] != "cooldown" || ev["failure_origin"] != "credential" {
			t.Errorf("event %d carries class %v / origin %v", i, ev["error_cause"], ev["failure_origin"])
		}
		if ev["candidate_index"] != float64(1) {
			t.Errorf("event %d candidate_index = %v, want 1", i, ev["candidate_index"])
		}
		if ev["policy_rule_id"] != "credential-cooldown" {
			t.Errorf("event %d policy_rule_id = %v, want the default row", i, ev["policy_rule_id"])
		}
	}
	// The exhaustion report's counter axes: three logical attempts (two
	// blocked, one answering) but a single real dial — and only the
	// answering attempt emitted a started marker, because a blocked
	// attempt never reaches a transport. The blocked attempts' evidence is
	// the credential_unavailable events above.
	done := buf.events(t, "request_completed")
	if done[0]["provider_attempts"] != float64(3) || done[0]["candidates_entered"] != float64(2) {
		t.Errorf("attempts/candidates = %v/%v, want 3/2", done[0]["provider_attempts"], done[0]["candidates_entered"])
	}
	if done[0]["upstream_exchanges"] != float64(1) {
		t.Errorf("upstream_exchanges = %v, want 1 (blocked attempts consume no exchange)", done[0]["upstream_exchanges"])
	}
	if done[0]["retries_total"] != float64(1) {
		t.Errorf("retries_total = %v, want 1 (the blocked re-ask is a real retry)", done[0]["retries_total"])
	}
	if done[0]["final_provider"] != "pb" {
		t.Errorf("final_provider = %v, want pb", done[0]["final_provider"])
	}
	if _, has := done[0]["upstream_credential_id"]; has {
		t.Error("completion record named a credential the walk never used")
	}
}
