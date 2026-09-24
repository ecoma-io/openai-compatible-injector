package proxy

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"openai-compatible-injector/internal/config"
	"openai-compatible-injector/internal/credential"
	"openai-compatible-injector/internal/transport"
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

// TestCredential429RotatesToNextReadyKey pins the headline behavior:
// a 429 marks the key that went out (for the parsed Retry-After, folded
// under the provider's ceiling), the engine's retry re-acquires, and the
// re-ask lands on the NEXT ready account — not the marked one.
func TestCredential429RotatesToNextReadyKey(t *testing.T) {
	stubRetryTiming(t)
	store := newCredChainStore(t, credAuthBlock)
	pa := &authScript{steps: []credStep{
		{status: http.StatusTooManyRequests, body: `{"error":{"message":"slow down"}}`,
			header: http.Header{"Retry-After": []string{"30"}}},
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
	if len(got) != 2 || got[0] != "Bearer sk-test-one" || got[1] != "Bearer sk-test-two" {
		t.Fatalf("429 rotation sequence = %v, want kilo-1 then kilo-2", got)
	}
	if until := pool.CoolingUntil("kilo-1"); !until.Equal(credFrozen.Add(30 * time.Second)) {
		t.Errorf("kilo-1 cools until %v, want %v (the parsed directive)", until, credFrozen.Add(30*time.Second))
	}
	if until := pool.CoolingUntil("kilo-2"); !until.IsZero() {
		t.Errorf("kilo-2 was marked: %v", until)
	}
}

// TestCredential429DefaultCooldownWithoutDirective: a 429 with no usable
// Retry-After still marks — the account fact stands — for the provider's
// configured default cooldown.
func TestCredential429DefaultCooldownWithoutDirective(t *testing.T) {
	stubRetryTiming(t)
	store := newCredChainStore(t, credAuthBlock)
	pa := &authScript{steps: []credStep{
		{status: http.StatusTooManyRequests, body: `{}`},
		okAnswer(),
	}}
	creds := credential.NewRegistry()
	pool := credPool(t, store, creds)
	h := NewHandler(store, kindResolver{direct: pa, proxied: pa}, creds, nil, nil, quietLogger())

	if rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions", chainChatBody, nil); rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	if until := pool.CoolingUntil("kilo-1"); !until.Equal(credFrozen.Add(2 * time.Second)) {
		t.Errorf("kilo-1 cools until %v, want %v (the default cooldown)", until, credFrozen.Add(2*time.Second))
	}
}

// TestCredential429DirectiveCappedAtMaxCooldown: an upstream cannot pin a
// key out of rotation by shouting a huge Retry-After — the mark folds the
// directive under the provider's max-cooldown.
func TestCredential429DirectiveCappedAtMaxCooldown(t *testing.T) {
	stubRetryTiming(t)
	store := newCredChainStore(t, credAuthBlock+"      rate-limit:\n        cooldown: 3s\n        max-cooldown: 45s\n")
	pa := &authScript{steps: []credStep{
		{status: http.StatusTooManyRequests, body: `{}`,
			header: http.Header{"Retry-After": []string{"3600"}}},
		okAnswer(),
	}}
	creds := credential.NewRegistry()
	pool := credPool(t, store, creds)
	h := NewHandler(store, kindResolver{direct: pa, proxied: pa}, creds, nil, nil, quietLogger())

	if rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions", chainChatBody, nil); rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	if until := pool.CoolingUntil("kilo-1"); !until.Equal(credFrozen.Add(45 * time.Second)) {
		t.Errorf("kilo-1 cools until %v, want %v (capped at max-cooldown)", until, credFrozen.Add(45*time.Second))
	}
}

// TestCredential429MarksWhenBodyCaptureFails: the mark is taken off the raw
// status BEFORE the error body is read, so a 429 whose body stalls or dies
// mid-capture still cools its key — the classification of the observation
// (a protocol failure, not an HTTP 429) never un-says the provider's rate
// limit. The walk still rotates: the re-ask goes to the next ready key.
func TestCredential429MarksWhenBodyCaptureFails(t *testing.T) {
	stubRetryTiming(t)
	store := newCredChainStore(t, credAuthBlock)
	pa := &authScript{steps: []credStep{
		{status: http.StatusTooManyRequests, bodyErr: errors.New("read: connection reset")},
		okAnswer(),
	}}
	creds := credential.NewRegistry()
	pool := credPool(t, store, creds)
	h := NewHandler(store, kindResolver{direct: pa, proxied: pa}, creds, nil, nil, quietLogger())

	if rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions", chainChatBody, nil); rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	if until := pool.CoolingUntil("kilo-1"); until.IsZero() {
		t.Fatal("kilo-1 was not marked despite the capture failure")
	}
	got := pa.calls()
	if len(got) != 2 || got[1] != "Bearer sk-test-two" {
		t.Fatalf("post-capture-failure rotation = %v, want kilo-1 then kilo-2", got)
	}
}

// TestCredential429MarkSurvivesClientCancel: the mark is taken the moment
// the 429's headers arrive, before any decision — so a caller already gone
// (the engine hard-stops every observation of a dead context to
// caller/terminal) still leaves the key cooling. No second attempt is made
// for a caller that is gone, and the 429 that ended the walk is relayed
// verbatim exactly as any terminal answer.
func TestCredential429MarkSurvivesClientCancel(t *testing.T) {
	stubRetryTiming(t)
	store := newCredChainStore(t, credAuthBlock)
	pa := &authScript{steps: []credStep{
		{status: http.StatusTooManyRequests, body: `{"error":{"message":"slow down"}}`},
		okAnswer(),
	}}
	creds := credential.NewRegistry()
	pool := credPool(t, store, creds)
	buf, log := captureLog(zerolog.DebugLevel)
	h := NewHandler(store, kindResolver{direct: pa, proxied: pa}, creds, nil, nil, log)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	doRequestWithContext(t, h, ctx, http.MethodPost, "/v1/chat/completions", chainChatBody, nil)
	if got := pa.calls(); len(got) != 1 {
		t.Fatalf("upstream received %d calls, want 1 (no retry for a gone caller)", len(got))
	}
	if until := pool.CoolingUntil("kilo-1"); until.IsZero() {
		t.Fatal("the 429 mark did not survive the client's departure")
	}
	failed := buf.events(t, "upstream_http_error")
	if len(failed) != 1 || failed[0]["policy_rule_id"] != "caller" {
		t.Fatalf("upstream_http_error = %v, want one decided by the caller rule", failed)
	}
	done := buf.events(t, "request_completed")
	if len(done) != 1 || done[0]["outcome"] != "upstream_http_error" {
		t.Fatalf("completion = %v, want one upstream_http_error", done)
	}
}

// TestCredentialCooldownFold pins the fold from an upstream directive to a
// mark duration: usable directive wins up to the ceiling, the ceiling wins
// above it, the configured default covers every unusable shape.
func TestCredentialCooldownFold(t *testing.T) {
	rl := credential.RateLimit{Cooldown: 2 * time.Second, MaxCooldown: time.Minute}
	for _, tc := range []struct {
		name string
		d    time.Duration
		want time.Duration
	}{
		{"no directive", 0, 2 * time.Second},
		{"hostile negative", -time.Second, 2 * time.Second},
		{"small directive", 5 * time.Second, 5 * time.Second},
		{"exact ceiling", time.Minute, time.Minute},
		{"above ceiling", 90 * time.Minute, time.Minute},
	} {
		if got := credentialCooldown(tc.d, rl); got != tc.want {
			t.Errorf("%s: credentialCooldown = %v, want %v", tc.name, got, tc.want)
		}
	}
}

// TestCredentialValuesNeverLogged sweeps the whole rotation scenario's log
// at debug — the loudest level — for the key VALUES: ids may ride the
// credential fields, values may ride nothing.
func TestCredentialValuesNeverLogged(t *testing.T) {
	stubRetryTiming(t)
	store := newCredChainStore(t, credAuthBlock)
	pa := &authScript{steps: []credStep{
		{status: http.StatusTooManyRequests, body: `{"error":{"message":"slow down"}}`,
			header: http.Header{"Retry-After": []string{"30"}}},
		okAnswer(),
	}}
	creds := credential.NewRegistry()
	buf, log := captureLog(zerolog.DebugLevel)
	h := NewHandler(store, kindResolver{direct: pa, proxied: pa}, creds, nil, nil, log)

	if rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions", chainChatBody, nil); rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	for _, secret := range []string{"sk-test-one", "sk-test-two"} {
		if strings.Contains(buf.String(), secret) {
			t.Errorf("key value %q reached the log: %s", secret, buf.String())
		}
	}
	done := buf.events(t, "request_completed")
	if len(done) != 1 {
		t.Fatalf("request_completed events = %d, want 1", len(done))
	}
	if id, _ := done[0]["upstream_credential_id"].(string); id != "kilo-2" {
		t.Errorf("completion upstream_credential_id = %v, want the answering key kilo-2", done[0]["upstream_credential_id"])
	}
	// The failed attempt's own event names ITS key — never the client's.
	failed := buf.events(t, "upstream_http_error")
	if len(failed) != 1 || failed[0]["upstream_credential_id"] != "kilo-1" {
		t.Fatalf("upstream_http_error = %v, want exactly one naming kilo-1", failed)
	}
}

// newCredChainStoreBlocks is newCredChainStore with a model-level YAML
// fragment (retries blocks and the like) inserted under chain-model.
func newCredChainStoreBlocks(t *testing.T, paAuth, modelExtra string) *config.Store {
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
` + modelExtra + `
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

// newCredPoolStore is the pool-transport store with a two-key auth block on
// its provider — the egress-pool branch of the seam with credentials on.
func newCredPoolStore(t *testing.T) *config.Store {
	t.Helper()
	snap, err := config.LoadRuntime([]byte(`
api-key: ` + testAPIKey + `
transports:
  relay:
    type: proxy
    proxy: http://127.0.0.1:20130
  pool-egress:
    type: pool
    members: [relay]
providers:
  kilo:
    base-url: https://api.kilo.example/v1
    transport: pool-egress
    auth:
      type: api_key
      header: Authorization
      prefix: "Bearer "
      strategy: round_robin
      keys:
        - id: kilo-1
          value: sk-test-one
        - id: kilo-2
          value: sk-test-two
models:
  pool-model:
    provider: kilo
    upstream-model: pm
`))
	if err != nil {
		t.Fatalf("LoadRuntime: %v", err)
	}
	return config.NewStore(snap)
}

// twoMemberExecutor stands for a pool that moves to its second member
// WITHIN one attempt: member one's dial provably fails before the request
// byte (replay-safe), member two answers 200. Each member's receipt of the
// Authorization header is recorded — the credential seam's egress-switch
// invariant lives here.
type twoMemberExecutor struct {
	mu   sync.Mutex
	auth []string
}

func (e *twoMemberExecutor) Execute(ar *transport.AttemptRequest) (*http.Response, transport.AttemptInfo, error) {
	// One Execute is one handler attempt; the pool's member walk lives
	// entirely inside it. Member one's dial provably fails before any
	// request byte (replay-safe), so the pool replays on member two with
	// the SAME request — same bytes, same headers, same key.
	for member := 0; ; member++ {
		if ar.Budget != nil && !ar.Budget.ConsumeExchange() {
			return nil, transport.AttemptInfo{BudgetExhausted: true, Attempts: member}, errors.New("exchange budget exhausted")
		}
		e.mu.Lock()
		e.auth = append(e.auth, ar.Header.Get("Authorization"))
		e.mu.Unlock()
		if member == 0 {
			continue // member one refused; the pool moves on
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body:       io.NopCloser(strings.NewReader(`{"id":"x","choices":[]}`)),
		}, transport.AttemptInfo{Attempts: 2, Kind: "pool", Target: "member-two"}, nil
	}
}

func (e *twoMemberExecutor) receipts() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]string(nil), e.auth...)
}

func (e *twoMemberExecutor) Do(*http.Request) (*http.Response, error) {
	panic("handler called Do on an Executor-capable doer")
}

// TestCredentialEgressSwitchKeepsSameKey pins the axis boundary: an egress
// switch inside ONE attempt moves the PATH, never the ACCOUNT — every
// member the pool dials receives the same acquired key.
func TestCredentialEgressSwitchKeepsSameKey(t *testing.T) {
	stubRetryTiming(t)
	store := newCredPoolStore(t)
	ex := &twoMemberExecutor{}
	creds := credential.NewRegistry()
	h := NewHandler(store, &singleDoerResolver{d: ex}, creds, nil, nil, quietLogger())

	rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions", poolChatBody, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body %s", rec.Code, rec.Body.String())
	}
	got := ex.receipts()
	if len(got) != 2 {
		t.Fatalf("member receipts = %v, want two dials in one attempt", got)
	}
	if got[0] != "Bearer sk-test-one" || got[1] != "Bearer sk-test-one" {
		t.Errorf("egress switch changed the key: %v", got)
	}
}

// TestCredentialRotationAcrossBudget pins rotation under an explicit retry
// budget: with three keys and max-retries 2, two consecutive 429s walk
// kilo-1 → kilo-2 → kilo-3 and the third attempt answers. Each marked key
// cools on its own default; the last key stays clean.
func TestCredentialRotationAcrossBudget(t *testing.T) {
	stubRetryTiming(t)
	threeKeys := `    auth:
      type: api_key
      header: Authorization
      prefix: "Bearer "
      strategy: round_robin
      keys:
        - id: kilo-1
          value: sk-test-one
        - id: kilo-2
          value: sk-test-two
        - id: kilo-3
          value: sk-test-three
`
	store := newCredChainStoreBlocks(t, threeKeys, "    retries:\n      max-retries: 2\n")
	pa := &authScript{steps: []credStep{
		{status: http.StatusTooManyRequests, body: `{}`},
		{status: http.StatusTooManyRequests, body: `{}`},
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
	if len(got) != 3 || got[0] != "Bearer sk-test-one" || got[1] != "Bearer sk-test-two" || got[2] != "Bearer sk-test-three" {
		t.Fatalf("rotation sequence = %v, want kilo-1, kilo-2, kilo-3", got)
	}
	for _, id := range []string{"kilo-1", "kilo-2"} {
		if pool.CoolingUntil(id).IsZero() {
			t.Errorf("%s was not marked by its 429", id)
		}
	}
	if until := pool.CoolingUntil("kilo-3"); !until.IsZero() {
		t.Errorf("kilo-3 was marked: %v", until)
	}
}

// TestCredentialSSECommitEndsRotation pins the commitment boundary on a
// credential-bearing candidate: the SSE headers are the commitment — the
// key they were authenticated with is the request's last credential fact,
// and nothing after them (rotation included) can move the walk.
func TestCredentialSSECommitEndsRotation(t *testing.T) {
	stubRetryTiming(t)
	store := newCredChainStore(t, credAuthBlock)
	pa := &authScript{steps: []credStep{{
		status: http.StatusOK,
		body:   "data: {\"model\":\"up-a\",\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\ndata: [DONE]\n\n",
		header: http.Header{"Content-Type": []string{"text/event-stream"}},
	}}}
	creds := credential.NewRegistry()
	buf, log := captureLog(zerolog.InfoLevel)
	h := NewHandler(store, kindResolver{direct: pa, proxied: pa}, creds, nil, nil, log)

	rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions",
		`{"model":"chain-model","stream":true,"messages":[{"role":"user","content":"hi"}]}`, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "data:") {
		t.Fatalf("answer was not relayed as SSE: %s", rec.Body.String())
	}
	if got := pa.calls(); len(got) != 1 || got[0] != "Bearer sk-test-one" {
		t.Fatalf("SSE receipts = %v, want one authenticated attempt", got)
	}
	done := buf.events(t, "request_completed")
	if len(done) != 1 || done[0]["upstream_credential_id"] != "kilo-1" {
		t.Fatalf("completion = %v, want one naming the committing key", done)
	}
}

// TestCredentialIDDoesNotBleedAcrossCandidates pins the attribution rule
// reviewer C flagged: upstream_credential_id names the credential of the
// ATTEMPT the event describes — a later credential-less candidate neither
// inherits the previous candidate's id on its own events nor on the
// completion line.
func TestCredentialIDDoesNotBleedAcrossCandidates(t *testing.T) {
	stubRetryTiming(t)
	store := newCredChainStore(t, credAuthBlock)
	pa := &authScript{steps: []credStep{
		{status: http.StatusUnauthorized, body: `{}`}, // 401 -> fallback (default matrix)
	}}
	pb := &authScript{steps: []credStep{okAnswer()}}
	creds := credential.NewRegistry()
	buf, log := captureLog(zerolog.InfoLevel)
	h := NewHandler(store, kindResolver{direct: pa, proxied: pb}, creds, nil, nil, log)

	rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions", chainChatBody, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body %s", rec.Code, rec.Body.String())
	}
	// The 401 attempt's own event names pa's key.
	failed := buf.events(t, "upstream_http_error")
	if len(failed) != 1 || failed[0]["upstream_credential_id"] != "kilo-1" {
		t.Fatalf("401 event = %v, want one naming pa's key", failed)
	}
	// The completion describes pb, which never acquired a credential: the
	// id must be absent entirely, not stale from pa.
	done := buf.events(t, "request_completed")
	if len(done) != 1 {
		t.Fatalf("completion events = %d, want 1", len(done))
	}
	if id, present := done[0]["upstream_credential_id"]; present {
		t.Fatalf("completion carries stale upstream_credential_id %v after falling back to a credential-less candidate", id)
	}
}
