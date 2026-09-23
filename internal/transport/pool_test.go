package transport

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

// stubEndpoint is a member client a pool test controls completely: a
// per-call script (the last entry repeats) of either a canned response or a
// canned error, plus the call log the assertions read back.
type stubEndpoint struct {
	mu     sync.Mutex
	calls  int
	sizes  []int
	script []stubResult
}

type stubResult struct {
	err    error
	status int
	body   string
}

func (s *stubEndpoint) Do(req *http.Request) (*http.Response, error) {
	s.mu.Lock()
	i := s.calls
	if i >= len(s.script)-1 {
		i = len(s.script) - 1 // the last script entry repeats
	}
	s.calls++
	s.sizes = append(s.sizes, int(req.ContentLength))
	s.mu.Unlock()
	r := s.script[i]
	if r.err != nil {
		return nil, r.err
	}
	status := r.status
	if status == 0 {
		status = http.StatusOK
	}
	return &http.Response{
		StatusCode: status,
		Header:     http.Header{},
		Body:       io.NopCloser(strings.NewReader(r.body)),
	}, nil
}

func (s *stubEndpoint) hitCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

func (s *stubEndpoint) lastSize() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.sizes) == 0 {
		return -1
	}
	return s.sizes[len(s.sizes)-1]
}

// fakeClock is the deterministic clock the health cooldown tests advance by
// hand.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock() *fakeClock { return &fakeClock{now: time.Unix(0, 0)} }

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// newTestPool assembles a poolDoer over the given clients without a
// registry — the unit surface for the attempt loop. The state's clock is
// returned so cooldown tests can drive it.
func newTestPool(members []Member, s Strategy, f FallbackPolicy, h HealthPolicy, clock func() time.Time, clients ...Doer) (*poolDoer, *poolState) {
	p := NewPool(members, s, f, h)
	if clock == nil {
		clock = time.Now
	}
	threshold := h.FailureThreshold
	if !h.Enabled {
		threshold = 0
	}
	st := &poolState{
		pool:    p,
		key:     "pool " + p.Identity(),
		members: make([]memberState, len(members)),
		clock:   clock,
		cw:      make([]int64, len(members)),
	}
	for i := range members {
		st.members[i] = memberState{
			client: clients[i],
			health: &endpointHealth{threshold: threshold, cooldown: h.Cooldown, clock: clock},
			lim:    &limiter{max: members[i].MaxConcurrency},
		}
	}
	return &poolDoer{pool: p, st: st}, st
}

func poolMembers(cfgs ...Config) []Member {
	ms := make([]Member, len(cfgs))
	for i, c := range cfgs {
		ms[i] = Member{Endpoint: c, Streaming: true, Weight: 1}
	}
	return ms
}

func execReq(streaming bool, body string) *AttemptRequest {
	return execReqCtx(context.Background(), streaming, body)
}

func execReqCtx(ctx context.Context, streaming bool, body string) *AttemptRequest {
	u, err := url.Parse("http://upstream.example/v1/chat/completions")
	if err != nil {
		panic(err)
	}
	return &AttemptRequest{
		Ctx:       ctx,
		Method:    http.MethodPost,
		URL:       u,
		Header:    http.Header{"Content-Type": []string{"application/json"}},
		Body:      []byte(body),
		Streaming: streaming,
	}
}

func okResult(body string) stubResult { return stubResult{status: http.StatusOK, body: body} }

// canceledCtx/deadlineCtx are caller-side contexts for ownership tests: the
// caller is gone (or its deadline fired) before the attempt returned.
func canceledCtx(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}

func deadlineCtx(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithDeadline(context.Background(), time.Unix(0, 1))
	cancel()
	return ctx
}

// ---- scheduling ----

// TestPoolRoundRobinRotatesEligibleMembers pins the default strategy: three
// healthy members receive the first three requests in declaration order,
// one each.
func TestPoolRoundRobinRotatesEligibleMembers(t *testing.T) {
	a, b, c := &stubEndpoint{script: []stubResult{okResult("{}")}}, &stubEndpoint{script: []stubResult{okResult("{}")}}, &stubEndpoint{script: []stubResult{okResult("{}")}}
	pd, _ := newTestPool(poolMembers(Config{}, Config{}, Config{}), RoundRobin,
		FallbackPolicy{Enabled: true, MaxAttempts: 3}, HealthPolicy{Enabled: true, FailureThreshold: 3}, nil, a, b, c)

	for i := 0; i < 3; i++ {
		resp, info, err := pd.Execute(execReq(false, "{}"))
		if err != nil {
			t.Fatalf("request %d: %v", i, err)
		}
		_ = resp.Body.Close()
		if info.Attempts != 1 || info.Exhausted {
			t.Errorf("request %d: attempts=%d exhausted=%v", i, info.Attempts, info.Exhausted)
		}
	}
	if a.hitCount() != 1 || b.hitCount() != 1 || c.hitCount() != 1 {
		t.Errorf("rotation = %d/%d/%d, want 1/1/1", a.hitCount(), b.hitCount(), c.hitCount())
	}
}

// TestPoolWeightedShare pins smooth weighted round-robin: weights 2:1 over
// six requests land 4:2, and the interleaving never repeats a member while
// others are owed a turn.
func TestPoolWeightedShare(t *testing.T) {
	a, b := &stubEndpoint{script: []stubResult{okResult("{}")}}, &stubEndpoint{script: []stubResult{okResult("{}")}}
	members := []Member{
		{Endpoint: Config{}, Streaming: true, Weight: 2},
		{Endpoint: Config{}, Streaming: true, Weight: 1},
	}
	pd, _ := newTestPool(members, WeightedRoundRobin,
		FallbackPolicy{Enabled: true, MaxAttempts: 2}, HealthPolicy{Enabled: false}, nil, a, b)

	var order []int
	for i := 0; i < 6; i++ {
		resp, _, err := pd.Execute(execReq(false, "{}"))
		if err != nil {
			t.Fatalf("request %d: %v", i, err)
		}
		_ = resp.Body.Close()
		// Attribute the hit by whoever was called most recently.
		switch {
		case a.hitCount() > countOf(order, 0):
			order = append(order, 0)
		default:
			order = append(order, 1)
		}
	}
	if a.hitCount() != 4 || b.hitCount() != 2 {
		t.Errorf("weighted share = %d/%d, want 4/2", a.hitCount(), b.hitCount())
	}
	// No three consecutive requests to the same member: the smooth
	// interleave property (w2 member is picked at most twice in a row).
	for i := 2; i < len(order); i++ {
		if order[i] == order[i-1] && order[i-1] == order[i-2] {
			t.Errorf("member %d served 3 consecutive requests: %v", order[i], order)
		}
	}
}

func countOf(order []int, v int) int {
	n := 0
	for _, x := range order {
		if x == v {
			n++
		}
	}
	return n
}

// dialErr builds the shape a real refused/failed CONNECT produces — a
// *net.OpError on the dial op (see sendstate_test.go, which pins the real
// stack's shapes). The pool's send-state gate reads the WIRE OP, never the
// class, so a bare synthetic error is send_unknown by design and can no
// longer stand in for "the connection was never established" in these
// tests.
func dialErr(msg string) error {
	return &net.OpError{Op: "dial", Net: "tcp", Err: errors.New(msg)}
}

// TestPoolWeightNeverSteersFallback pins the weight boundary: weight only
// chooses the FIRST endpoint; once it fails, the fallback walks declaration
// order (the unweighted member is next, not the heavy one).
func TestPoolWeightNeverSteersFallback(t *testing.T) {
	dead := &stubEndpoint{script: []stubResult{{err: dialErr("boom")}}}
	standby := &stubEndpoint{script: []stubResult{okResult(`{"ok":true}`)}}
	members := []Member{
		{Endpoint: Config{}, Streaming: true, Weight: 10},
		{Endpoint: Config{}, Streaming: true, Weight: 1},
	}
	pd, _ := newTestPool(members, WeightedRoundRobin,
		FallbackPolicy{Enabled: true, MaxAttempts: 3}, HealthPolicy{Enabled: false}, nil, dead, standby)

	resp, info, err := pd.Execute(execReq(false, "{}"))
	if err != nil {
		t.Fatalf("fallback failed: %v", err)
	}
	_ = resp.Body.Close()
	if dead.hitCount() != 1 || standby.hitCount() != 1 {
		t.Errorf("dials = %d/%d, want 1/1", dead.hitCount(), standby.hitCount())
	}
	if info.Attempts != 2 {
		t.Errorf("attempts = %d, want 2", info.Attempts)
	}
}

// ---- eligibility ----

// TestPoolBodySizeIsAnEligibilityGate pins the pre-send gate: a 6 MB
// request never touches the 4.5 MB member — it goes straight to the member
// that can carry it. The quiet direction is dialing the small relay first
// and turning its 413 into a retry.
func TestPoolBodySizeIsAnEligibilityGate(t *testing.T) {
	small := &stubEndpoint{script: []stubResult{okResult("{}")}}
	big := &stubEndpoint{script: []stubResult{okResult(`{"via":"big"}`)}}
	members := []Member{
		{Endpoint: Config{}, Streaming: true, Weight: 1, MaxBodyBytes: 4718592},
		{Endpoint: Config{}, Streaming: true, Weight: 1},
	}
	pd, _ := newTestPool(members, RoundRobin,
		FallbackPolicy{Enabled: true, MaxAttempts: 3}, HealthPolicy{Enabled: false}, nil, small, big)

	resp, info, err := pd.Execute(execReq(false, strings.Repeat("x", 6<<20)))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	_ = resp.Body.Close()
	if small.hitCount() != 0 {
		t.Errorf("oversized request dialed the capped member %d times", small.hitCount())
	}
	if big.hitCount() != 1 {
		t.Errorf("uncapped member dials = %d, want 1", big.hitCount())
	}
	if big.lastSize() != 6<<20 {
		t.Errorf("member saw body of %d bytes", big.lastSize())
	}
	if info.Target != "direct" || info.Kind != "direct" || info.Attempts != 1 {
		t.Errorf("info = %+v", info)
	}
}

// TestPoolStreamingGate pins the stream compatibility gate: a stream request
// skips a streaming:false member entirely, while a non-stream request may
// use it.
func TestPoolStreamingGate(t *testing.T) {
	noStream := &stubEndpoint{script: []stubResult{okResult("{}")}}
	any := &stubEndpoint{script: []stubResult{okResult("{}")}}
	members := []Member{
		{Endpoint: Config{}, Streaming: false, Weight: 1},
		{Endpoint: Config{}, Streaming: true, Weight: 1},
	}
	pd, _ := newTestPool(members, RoundRobin,
		FallbackPolicy{Enabled: true, MaxAttempts: 2}, HealthPolicy{Enabled: false}, nil, noStream, any)

	resp, _, err := pd.Execute(execReq(true, "{}"))
	if err != nil {
		t.Fatalf("stream request: %v", err)
	}
	_ = resp.Body.Close()
	if noStream.hitCount() != 0 {
		t.Errorf("stream request dialed the non-streaming member")
	}

	resp, _, err = pd.Execute(execReq(false, "{}"))
	if err != nil {
		t.Fatalf("plain request: %v", err)
	}
	_ = resp.Body.Close()
	if noStream.hitCount() != 1 {
		t.Errorf("plain request skipped the non-streaming member (%d hits)", noStream.hitCount())
	}
}

// TestPoolAllIneligibleIsExhausted pins zero dials: nothing eligible means
// the exhausted sentinel, Attempts 0, and no endpoint error misattributed.
func TestPoolAllIneligibleIsExhausted(t *testing.T) {
	never := &stubEndpoint{script: []stubResult{okResult("{}")}}
	members := []Member{
		{Endpoint: Config{}, Streaming: true, Weight: 1, MaxBodyBytes: 10},
		{Endpoint: Config{}, Streaming: false, Weight: 1},
	}
	pd, _ := newTestPool(members, RoundRobin,
		FallbackPolicy{Enabled: true, MaxAttempts: 3}, HealthPolicy{Enabled: false}, nil, never, never)

	resp, info, err := pd.Execute(execReq(true, strings.Repeat("x", 100)))
	if err == nil || !errors.Is(err, ErrExhausted) {
		t.Fatalf("err = %v, want the exhaustion sentinel", err)
	}
	if resp != nil {
		t.Errorf("exhaustion returned a response")
	}
	if !info.Exhausted || info.Attempts != 0 {
		t.Errorf("info = %+v, want exhausted with 0 attempts", info)
	}
	if never.hitCount() != 0 {
		t.Errorf("ineligible request dialed an endpoint %d times", never.hitCount())
	}
}

// ---- fallback ----

// TestPoolFallsBackOnPreResponseFailure pins the core case: the first
// endpoint's transport failure falls to the next member and the answer
// comes back through it.
func TestPoolFallsBackOnPreResponseFailure(t *testing.T) {
	dead := &stubEndpoint{script: []stubResult{{err: &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}}}}
	live := &stubEndpoint{script: []stubResult{okResult(`{"ok":true}`)}}
	pd, _ := newTestPool(poolMembers(Config{}, Config{}), RoundRobin,
		FallbackPolicy{Enabled: true, MaxAttempts: 3}, HealthPolicy{Enabled: false}, nil, dead, live)

	resp, info, err := pd.Execute(execReq(false, "{}"))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if string(body) != `{"ok":true}` {
		t.Errorf("body = %q", body)
	}
	if info.Attempts != 2 || info.Target != "direct" {
		t.Errorf("info = %+v", info)
	}
	// The failure is a refused dial — positive evidence the connection was
	// never established, which is exactly what licenses the egress fallback.
	// The class bucket ("connection") is NOT what decides it: a reset on an
	// established connection lands in the same bucket and must not replay.
	if len(info.Failures) != 1 || info.Failures[0].SendState != "definitely_not_sent" {
		t.Errorf("failures = %+v, want one definitely_not_sent record", info.Failures)
	}
}

// TestPoolMaxAttemptsCapsDistinctDials pins the bound: three failing
// members with max-attempts 2 produce exactly two dials and the second's
// error.
func TestPoolMaxAttemptsCapsDistinctDials(t *testing.T) {
	e1 := &stubEndpoint{script: []stubResult{{err: dialErr("first down")}}}
	e2 := &stubEndpoint{script: []stubResult{{err: dialErr("second down")}}}
	e3 := &stubEndpoint{script: []stubResult{okResult("{}")}}
	pd, _ := newTestPool(poolMembers(Config{}, Config{}, Config{}), RoundRobin,
		FallbackPolicy{Enabled: true, MaxAttempts: 2}, HealthPolicy{Enabled: false}, nil, e1, e2, e3)

	_, info, err := pd.Execute(execReq(false, "{}"))
	if err == nil || !strings.Contains(err.Error(), "second down") {
		t.Fatalf("err = %v, want the second endpoint's failure", err)
	}
	if info.Attempts != 2 {
		t.Errorf("attempts = %d, want 2", info.Attempts)
	}
	if e3.hitCount() != 0 {
		t.Errorf("max-attempts breached: third endpoint dialed %d times", e3.hitCount())
	}
}

// TestPoolFallbackDisabledDialsOnce pins fallback.enabled=false: one dial,
// the failure surfaces immediately.
func TestPoolFallbackDisabledDialsOnce(t *testing.T) {
	e1 := &stubEndpoint{script: []stubResult{{err: errors.New("down")}}}
	e2 := &stubEndpoint{script: []stubResult{okResult("{}")}}
	pd, _ := newTestPool(poolMembers(Config{}, Config{}), RoundRobin,
		FallbackPolicy{Enabled: false, MaxAttempts: 3}, HealthPolicy{Enabled: false}, nil, e1, e2)

	_, info, err := pd.Execute(execReq(false, "{}"))
	if err == nil {
		t.Fatal("expected the endpoint failure")
	}
	if e2.hitCount() != 0 || info.Attempts != 1 {
		t.Errorf("fallback ran despite disabled policy: attempts=%d e2=%d", info.Attempts, e2.hitCount())
	}
}

// TestPoolHTTPStatusNeverFallsBack is the load-bearing seam inside the
// pool: a 429 (and any 5xx) from the first endpoint is an ANSWER — returned
// as-is, no second dial, no health strike. PR1's normalization contract
// depends on this staying true behind the pool.
func TestPoolHTTPStatusNeverFallsBack(t *testing.T) {
	for _, status := range []int{429, 500, 502, 503} {
		busy := &stubEndpoint{script: []stubResult{{status: status, body: `{"error":"no"}`}}}
		idle := &stubEndpoint{script: []stubResult{okResult("{}")}}
		pd, _ := newTestPool(poolMembers(Config{}, Config{}), RoundRobin,
			FallbackPolicy{Enabled: true, MaxAttempts: 3}, HealthPolicy{Enabled: true, FailureThreshold: 1, Cooldown: 30 * time.Second}, nil, busy, idle)

		resp, info, err := pd.Execute(execReq(false, "{}"))
		if err != nil {
			t.Errorf("status %d: Execute err = %v, want the response", status, err)
			continue
		}
		if resp.StatusCode != status {
			t.Errorf("status %d: caller saw %d", status, resp.StatusCode)
		}
		_ = resp.Body.Close()
		if idle.hitCount() != 0 {
			t.Errorf("status %d: fallback dialed a second endpoint", status)
		}
		if info.Attempts != 1 {
			t.Errorf("status %d: attempts = %d", status, info.Attempts)
		}
	}
}

// TestPoolResponseIsLiveUntilClosed pins both halves of the commitment: the
// body streams lazily after Execute returns (no buffering), and closing it
// releases the member's concurrency permit.
func TestPoolResponseIsLiveUntilClosed(t *testing.T) {
	solo := &stubEndpoint{script: []stubResult{okResult(`{"big":true}`)}}
	members := []Member{{Endpoint: Config{}, Streaming: true, Weight: 1, MaxConcurrency: 1}}
	pd, _ := newTestPool(members, RoundRobin,
		FallbackPolicy{Enabled: true, MaxAttempts: 1}, HealthPolicy{Enabled: false}, nil, solo)

	resp, _, err := pd.Execute(execReq(false, "{}"))
	if err != nil {
		t.Fatalf("first: %v", err)
	}
	// Body not yet consumed: while it stays open, the member is at capacity
	// and a second request must skip it (and find nothing else → exhausted).
	_, info2, err2 := pd.Execute(execReq(false, "{}"))
	if !errors.Is(err2, ErrExhausted) || !info2.Exhausted {
		t.Fatalf("second request while body open: err=%v info=%+v, want exhaustion", err2, info2)
	}
	if solo.hitCount() != 1 {
		t.Errorf("permit leaked: member dialed %d times", solo.hitCount())
	}
	_ = resp.Body.Close()
	resp3, _, err3 := pd.Execute(execReq(false, "{}"))
	if err3 != nil {
		t.Fatalf("after close: %v", err3)
	}
	_ = resp3.Body.Close()
	if solo.hitCount() != 2 {
		t.Errorf("permit not released on body close: dials = %d", solo.hitCount())
	}
}

// ---- exchange budget ----

// stubBudget is the consumer side of the exchange-budget seam: a fixed
// allowance of claims, with the grants counted. The grant count is the number
// of exchanges the pool was willing to start, so a test can compare it with
// the independently instrumented dial count — the two must agree exactly, and
// a claim that does not sit immediately before a dial breaks the agreement.
type stubBudget struct {
	mu        sync.Mutex
	allowance int
	granted   int
}

func (b *stubBudget) ConsumeExchange() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.granted >= b.allowance {
		return false
	}
	b.granted++
	return true
}

func (b *stubBudget) grants() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.granted
}

func withBudget(ar *AttemptRequest, b ExchangeBudget) *AttemptRequest {
	ar.Budget = b
	return ar
}

// TestPoolBudgetStopsTheLoopAfterOneExchange pins the post-dial refusal: a
// one-exchange envelope over three failing members dials exactly once, keeps
// that dial's evidence, and stops with BudgetExhausted — the fallback policy
// still had two members to offer, the envelope had nothing.
func TestPoolBudgetStopsTheLoopAfterOneExchange(t *testing.T) {
	e1 := &stubEndpoint{script: []stubResult{{err: dialErr("first down")}}}
	e2 := &stubEndpoint{script: []stubResult{{err: dialErr("second down")}}}
	e3 := &stubEndpoint{script: []stubResult{{err: dialErr("third down")}}}
	members := []Member{
		{Endpoint: Config{}, Streaming: true, Weight: 1, MaxConcurrency: 1},
		{Endpoint: Config{}, Streaming: true, Weight: 1, MaxConcurrency: 1},
		{Endpoint: Config{}, Streaming: true, Weight: 1, MaxConcurrency: 1},
	}
	pd, _ := newTestPool(members, RoundRobin,
		FallbackPolicy{Enabled: true, MaxAttempts: 3}, HealthPolicy{Enabled: false}, nil, e1, e2, e3)

	budget := &stubBudget{allowance: 1}
	_, info, err := pd.Execute(withBudget(execReq(false, "{}"), budget))
	if err == nil || !strings.Contains(err.Error(), "first down") {
		t.Fatalf("err = %v, want the one dialed endpoint's failure", err)
	}
	if dials := e1.hitCount() + e2.hitCount() + e3.hitCount(); dials != 1 {
		t.Errorf("dialed %d endpoints, want 1", dials)
	}
	if budget.grants() != 1 {
		t.Errorf("budget granted %d exchanges, want 1", budget.grants())
	}
	if info.Attempts != 1 || !info.BudgetExhausted || info.Exhausted {
		t.Errorf("info = %+v, want one attempt, budget-exhausted, not exhausted", info)
	}
	if len(info.Failures) != 1 {
		t.Errorf("failures = %+v, want evidence for the one real dial", info.Failures)
	}
	// The refused fallback never reached the wire, so it holds no permit: a
	// leaked one would shrink that member for every later request.
	if cur := pd.st.members[1].lim.cur; cur != 0 {
		t.Errorf("refused member holds %d permits, want 0", cur)
	}
}

// TestPoolBudgetCapsTheWalkAtTwoExchanges pins the count above one: a
// two-exchange envelope walks exactly two of the three members and closes on
// the second endpoint's error — the envelope ended the walk, not
// max-attempts.
func TestPoolBudgetCapsTheWalkAtTwoExchanges(t *testing.T) {
	e1 := &stubEndpoint{script: []stubResult{{err: dialErr("first down")}}}
	e2 := &stubEndpoint{script: []stubResult{{err: dialErr("second down")}}}
	e3 := &stubEndpoint{script: []stubResult{{err: dialErr("third down")}}}
	pd, _ := newTestPool(poolMembers(Config{}, Config{}, Config{}), RoundRobin,
		FallbackPolicy{Enabled: true, MaxAttempts: 3}, HealthPolicy{Enabled: false}, nil, e1, e2, e3)

	budget := &stubBudget{allowance: 2}
	_, info, err := pd.Execute(withBudget(execReq(false, "{}"), budget))
	if err == nil || !strings.Contains(err.Error(), "second down") {
		t.Fatalf("err = %v, want the second dialed endpoint's failure", err)
	}
	if dials := e1.hitCount() + e2.hitCount() + e3.hitCount(); dials != 2 {
		t.Errorf("dialed %d endpoints, want 2", dials)
	}
	if budget.grants() != 2 {
		t.Errorf("budget granted %d exchanges, want 2", budget.grants())
	}
	if info.Attempts != 2 || !info.BudgetExhausted || info.Exhausted {
		t.Errorf("info = %+v, want two attempts, budget-exhausted, not exhausted", info)
	}
	if len(info.Failures) != 2 {
		t.Errorf("failures = %+v, want one record per real dial", info.Failures)
	}
}

// TestPoolNilBudgetDialsTheWholeChain is the quiet direction of the new
// field: no budget attached means the walk is exactly as wide as the fallback
// policy says, unchanged from before the seam existed.
func TestPoolNilBudgetDialsTheWholeChain(t *testing.T) {
	e1 := &stubEndpoint{script: []stubResult{{err: dialErr("first down")}}}
	e2 := &stubEndpoint{script: []stubResult{{err: dialErr("second down")}}}
	e3 := &stubEndpoint{script: []stubResult{{err: dialErr("third down")}}}
	pd, _ := newTestPool(poolMembers(Config{}, Config{}, Config{}), RoundRobin,
		FallbackPolicy{Enabled: true, MaxAttempts: 3}, HealthPolicy{Enabled: false}, nil, e1, e2, e3)

	_, info, err := pd.Execute(execReq(false, "{}"))
	if err == nil || !strings.Contains(err.Error(), "third down") {
		t.Fatalf("err = %v, want the third dialed endpoint's failure", err)
	}
	if dials := e1.hitCount() + e2.hitCount() + e3.hitCount(); dials != 3 {
		t.Errorf("dialed %d endpoints, want all 3", dials)
	}
	if info.Attempts != 3 || info.BudgetExhausted || info.Exhausted {
		t.Errorf("info = %+v, want three plain attempts", info)
	}
}

// TestPoolBudgetRefusedBeforeAnyDialIsNotEndpointExhaustion pins the
// zero-dial edge: an envelope with no room left refuses the very first dial,
// so the request reports the budget flag WITHOUT the exhaustion sentinel —
// no endpoint was tried and failed, so no member may be blamed — and the
// permit the selection took for that dial comes back, so the member is not
// left saturated behind a dial that never happened. The distinction matters
// upstream: egress exhaustion is an endpoint verdict, a refused exchange is
// the request's own envelope.
func TestPoolBudgetRefusedBeforeAnyDialIsNotEndpointExhaustion(t *testing.T) {
	solo := &stubEndpoint{script: []stubResult{okResult("{}")}}
	members := []Member{{Endpoint: Config{}, Streaming: true, Weight: 1, MaxConcurrency: 1}}
	pd, _ := newTestPool(members, RoundRobin,
		FallbackPolicy{Enabled: true, MaxAttempts: 3}, HealthPolicy{Enabled: false}, nil, solo)

	budget := &stubBudget{allowance: 0}
	resp, info, err := pd.Execute(withBudget(execReq(false, "{}"), budget))
	if err != nil {
		t.Fatalf("err = %v, want no transport error for a refused dial", err)
	}
	if resp != nil {
		t.Errorf("a refused dial returned a response")
	}
	if info.Attempts != 0 || info.Exhausted || !info.BudgetExhausted {
		t.Errorf("info = %+v, want zero attempts, budget-exhausted, not endpoint-exhausted", info)
	}
	if solo.hitCount() != 0 || budget.grants() != 0 {
		t.Errorf("refused dial happened anyway: hits=%d grants=%d", solo.hitCount(), budget.grants())
	}

	// No permit leaked: the same pool still serves the next request.
	resp2, _, err2 := pd.Execute(execReq(false, "{}"))
	if err2 != nil {
		t.Fatalf("after a refused dial: %v", err2)
	}
	_ = resp2.Body.Close()
	if solo.hitCount() != 1 {
		t.Errorf("permit leaked after a refused dial: member dialed %d times", solo.hitCount())
	}

	// No LEASE leaked either. This path returns before any dial, and a lease
	// left standing at the pool state keeps it from ever satisfying the
	// registry's retire condition — the retired generation would linger with
	// its idle connections open. A state that has released everything counts
	// zero here.
	pd.st.mu.Lock()
	leases := pd.st.leases
	pd.st.mu.Unlock()
	if leases != 0 {
		t.Errorf("leases = %d after the requests drained, want 0", leases)
	}
}

// TestPoolUnbuildableRequestCostsNothing pins WHERE the build sits relative to
// the claim. Constructing the attempt's request is not a wire operation, so a
// request this transport cannot build must cost the request nothing at all:
// no exchange claimed for a dial that never happened, no permit held, no
// endpoint blamed or struck, and no attempt counted as one the member
// answered for. The failure leaves as a local pre-dial error.
func TestPoolUnbuildableRequestCostsNothing(t *testing.T) {
	ep := &stubEndpoint{script: []stubResult{okResult("{}")}}
	members := []Member{{Endpoint: Config{}, Streaming: true, Weight: 1}}
	pd, _ := newTestPool(members, RoundRobin,
		FallbackPolicy{Enabled: true, MaxAttempts: 3}, HealthPolicy{Enabled: false}, nil, ep)

	bad := execReq(false, "{}")
	// A space is not a valid HTTP method token: the request cannot be built.
	bad.Method = "BAD METHOD"
	budget := &stubBudget{allowance: 5}
	resp, info, err := pd.Execute(withBudget(bad, budget))
	if err == nil {
		t.Fatal("an unbuildable request reported no error")
	}
	if resp != nil {
		t.Errorf("an unbuildable request returned a response")
	}
	// Typed, so the caller can tell a local construction failure from an
	// endpoint's — and does not name, strike or characterise a member for it.
	var rbe *RequestBuildError
	if !errors.As(err, &rbe) {
		t.Errorf("error = %T (%v), want a *RequestBuildError", err, err)
	}
	// And its text carries none of the cause: NewRequest's error quotes the
	// request URL, whose query string can hold credentials.
	if s := err.Error(); strings.Contains(s, "BAD METHOD") || strings.Contains(s, "://") {
		t.Errorf("build error text = %q, want our own static wording", s)
	}
	if budget.grants() != 0 {
		t.Errorf("exchange grants = %d, want 0: nothing was dialed", budget.grants())
	}
	if ep.hitCount() != 0 {
		t.Errorf("member dialed %d times, want 0", ep.hitCount())
	}
	if info.Attempts != 0 || len(info.Failures) != 0 || info.Exhausted || info.BudgetExhausted {
		t.Errorf("info = %+v, want no attempt and no endpoint evidence", info)
	}

	// Nothing leaked — not the member's permit, not the pool's lease: the
	// same pool still serves the next request.
	resp2, _, err2 := pd.Execute(execReq(false, "{}"))
	if err2 != nil {
		t.Fatalf("after an unbuildable request: %v", err2)
	}
	_ = resp2.Body.Close()
	if ep.hitCount() != 1 {
		t.Errorf("permit leaked after an unbuildable request: member dialed %d times", ep.hitCount())
	}
	pd.st.mu.Lock()
	leases := pd.st.leases
	pd.st.mu.Unlock()
	if leases != 0 {
		t.Errorf("leases = %d after the requests drained, want 0", leases)
	}
}

// TestPoolSkippedMemberConsumesNoBudget pins WHERE the claim sits. The
// saturated member's only permit is held by an earlier request, so the
// fallback step reaching it is skipped — passed over without a dial. A unit
// spent on that skip would leave the envelope empty before the member behind
// it, which is a dial the request should still get: the live member is dialed
// and the flag stays clear.
func TestPoolSkippedMemberConsumesNoBudget(t *testing.T) {
	busy := &stubEndpoint{script: []stubResult{okResult("{}")}}
	dead := &stubEndpoint{script: []stubResult{{err: errors.New("down")}}}
	members := []Member{
		{Endpoint: Config{}, Streaming: true, Weight: 1, MaxConcurrency: 1},
		{Endpoint: Config{}, Streaming: true, Weight: 1},
	}
	pd, _ := newTestPool(members, RoundRobin,
		FallbackPolicy{Enabled: true, MaxAttempts: 3}, HealthPolicy{Enabled: false}, nil, busy, dead)

	// Occupy the capped member with a live body; the request after it must be
	// served by the other member.
	held, _, err := pd.Execute(execReq(false, "{}"))
	if err != nil {
		t.Fatalf("priming request: %v", err)
	}
	defer func() { _ = held.Body.Close() }()

	budget := &stubBudget{allowance: 1}
	_, info, err := pd.Execute(withBudget(execReq(false, "{}"), budget))
	if err == nil || !strings.Contains(err.Error(), "down") {
		t.Fatalf("err = %v, want the live member's failure", err)
	}
	if dead.hitCount() != 1 {
		t.Errorf("live member dials = %d, want 1", dead.hitCount())
	}
	if busy.hitCount() != 1 {
		t.Errorf("saturated member was dialed %d times, want only the priming dial", busy.hitCount())
	}
	if budget.grants() != 1 {
		t.Errorf("budget granted %d exchanges, want 1 (a skip spends nothing)", budget.grants())
	}
	if info.Attempts != 1 || info.BudgetExhausted || info.Exhausted {
		t.Errorf("info = %+v, want one attempt, no exhaustion", info)
	}
}

// ---- cancellation ----

// TestPoolCancellationAbortsAndSparesHealth pins the canceled class: the
// caller's own cancellation returns immediately, no fallback, no health
// strike.
func TestPoolCancellationAbortsAndSparesHealth(t *testing.T) {
	slow := &stubEndpoint{script: []stubResult{{err: context.Canceled}}}
	other := &stubEndpoint{script: []stubResult{okResult("{}")}}
	pd, _ := newTestPool(poolMembers(Config{}, Config{}), RoundRobin,
		FallbackPolicy{Enabled: true, MaxAttempts: 3}, HealthPolicy{Enabled: true, FailureThreshold: 1, Cooldown: 30 * time.Second}, nil, slow, other)

	_, _, err := pd.Execute(execReqCtx(canceledCtx(t), false, "{}"))
	if err == nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if other.hitCount() != 0 {
		t.Errorf("cancellation fell back to a second endpoint")
	}
	// No strike: threshold 1 with a strike would have opened a cooldown.
	if h := pd.st.members[0].health; !h.usable() || h.fails != 0 {
		t.Errorf("cancellation struck health: fails=%d usable=%v", h.fails, h.usable())
	}
	// A wrapped cancellation against a dead context classifies the same way —
	// caller ownership beats the typed wrapper.
	wrapped := &stubEndpoint{script: []stubResult{{err: &ProxyConnectError{msg: "socks5: dial proxy", cause: context.Canceled}}}}
	pd2, _ := newTestPool(poolMembers(Config{}), RoundRobin,
		FallbackPolicy{Enabled: true, MaxAttempts: 2}, HealthPolicy{Enabled: true, FailureThreshold: 1, Cooldown: 30 * time.Second}, nil, wrapped)
	_, _, err = pd2.Execute(execReqCtx(canceledCtx(t), false, "{}"))
	if err == nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("wrapped err = %v, want context.Canceled through the typed wrapper", err)
	}
	if h := pd2.st.members[0].health; !h.usable() || h.fails != 0 {
		t.Errorf("cancellation struck health: fails=%d usable=%v", h.fails, h.usable())
	}
}

// TestPoolCallerDeadlineIsTerminalNoFallback pins the deadline half of
// caller ownership: a caller deadline that fired mid-dial must not read as
// the endpoint's timeout — no second member is dialed, no health strike
// lands, and the context's own error surfaces.
func TestPoolCallerDeadlineIsTerminalNoFallback(t *testing.T) {
	deadline := &stubEndpoint{script: []stubResult{{err: context.DeadlineExceeded}}}
	other := &stubEndpoint{script: []stubResult{okResult("{}")}}
	pd, _ := newTestPool(poolMembers(Config{}, Config{}), RoundRobin,
		FallbackPolicy{Enabled: true, MaxAttempts: 3}, HealthPolicy{Enabled: true, FailureThreshold: 1, Cooldown: 30 * time.Second}, nil, deadline, other)

	_, info, err := pd.Execute(execReqCtx(deadlineCtx(t), false, "{}"))
	if err == nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want context.DeadlineExceeded", err)
	}
	if other.hitCount() != 0 {
		t.Errorf("caller deadline fell back to a second endpoint")
	}
	if h := pd.st.members[0].health; !h.usable() || h.fails != 0 {
		t.Errorf("caller deadline struck health: fails=%d usable=%v", h.fails, h.usable())
	}
	_ = info
}

// TestPoolSendUnknownTimeoutNeverReplays pins the load-bearing boundary: a
// timeout on an established connection may have reached the upstream, so no
// layer below the injector may replay it. The pool dials no second member,
// strikes no health (the endpoint is unproven, not failed), and hands the
// failure up carrying its send state.
func TestPoolSendUnknownTimeoutNeverReplays(t *testing.T) {
	slow := &stubEndpoint{script: []stubResult{{err: &fakeNetError{timeout: true}}}}
	live := &stubEndpoint{script: []stubResult{okResult("{}")}}
	pd, _ := newTestPool(poolMembers(Config{}, Config{}), RoundRobin,
		FallbackPolicy{Enabled: true, MaxAttempts: 3}, HealthPolicy{Enabled: true, FailureThreshold: 3, Cooldown: 30 * time.Second}, nil, slow, live)

	_, info, err := pd.Execute(execReq(false, "{}"))
	if err == nil {
		t.Fatal("expected the timeout failure to surface")
	}
	if slow.hitCount() != 1 || live.hitCount() != 0 {
		t.Errorf("dials = %d/%d, want 1/0: a send-unknown failure must never replay", slow.hitCount(), live.hitCount())
	}
	if info.Attempts != 1 {
		t.Errorf("attempts = %d, want 1", info.Attempts)
	}
	if len(info.Failures) != 1 || info.Failures[0].Class != "timeout" ||
		info.Failures[0].Cause != CauseNetworkTimeout || info.Failures[0].SendState != "send_unknown" {
		t.Errorf("failures = %+v, want one timeout/network_timeout/send_unknown record", info.Failures)
	}
	if h := pd.st.members[0].health; h.fails != 0 {
		t.Errorf("a send-unknown failure struck health: fails=%d, want 0", h.fails)
	}
}

// ---- health ----

// TestPoolHealthCooldownSkipsThenRecovers pins the passive lifecycle with a
// driven clock: threshold-1 failure opens the cooldown, the member is
// skipped without a dial, and after the cooldown it is dialed again.
func TestPoolHealthCooldownSkipsThenRecovers(t *testing.T) {
	clk := newFakeClock()
	flaky := &stubEndpoint{script: []stubResult{{err: dialErr("refused")}, okResult("{}")}}
	backup := &stubEndpoint{script: []stubResult{okResult("{}")}}
	pd, _ := newTestPool(poolMembers(Config{}, Config{}), RoundRobin,
		FallbackPolicy{Enabled: true, MaxAttempts: 3}, HealthPolicy{Enabled: true, FailureThreshold: 1, Cooldown: 30 * time.Second}, clk.Now, flaky, backup)

	// Request 1: flaky fails, backup serves; flaky is now cooling down.
	resp, info, err := pd.Execute(execReq(false, "{}"))
	if err != nil {
		t.Fatalf("first: %v", err)
	}
	_ = resp.Body.Close()
	if info.Attempts != 2 {
		t.Errorf("first attempts = %d, want 2", info.Attempts)
	}

	// Request 2: flaky is skipped — no dial, no attempt consumed, and the
	// request still reaches backup.
	resp, info, err = pd.Execute(execReq(false, "{}"))
	if err != nil {
		t.Fatalf("second: %v", err)
	}
	_ = resp.Body.Close()
	if flaky.hitCount() != 1 {
		t.Errorf("cooling member was dialed again (%d hits)", flaky.hitCount())
	}
	if info.Attempts != 1 {
		t.Errorf("second attempts = %d, want 1 (skip consumes no attempt)", info.Attempts)
	}

	// Request 3, cooldown still open: everything holds.
	if _, _, err := pd.Execute(execReq(false, "{}")); err != nil {
		t.Fatalf("third: %v", err)
	}

	// Cooldown expires. While flaky cooled, its scheduler turn was never
	// consumed (a skipped member is not a passed-over one), so the cursor
	// still points at it: recovery means immediate rescheduling, not one
	// more request through the healthy head first.
	clk.advance(31 * time.Second)
	resp, info, err = pd.Execute(execReq(false, "{}"))
	if err != nil {
		t.Fatalf("after cooldown (flaky's turn): %v", err)
	}
	_ = resp.Body.Close()
	if flaky.hitCount() != 2 {
		t.Errorf("recovered member not re-scheduled first (%d hits)", flaky.hitCount())
	}
	if info.Attempts != 1 {
		t.Errorf("recovered member needed fallback: %+v", info)
	}
}

// TestPoolHealthRecoveryOnAnyResponse pins that 4xx/5xx reset health: an
// answer proves the path delivered.
func TestPoolHealthRecoveryOnAnyResponse(t *testing.T) {
	clk := newFakeClock()
	errThen429 := &stubEndpoint{script: []stubResult{{err: errors.New("refused")}, {status: http.StatusTooManyRequests, body: "{}"}}}
	pd, _ := newTestPool(poolMembers(Config{}), RoundRobin,
		FallbackPolicy{Enabled: true, MaxAttempts: 1}, HealthPolicy{Enabled: true, FailureThreshold: 2, Cooldown: 30 * time.Second}, clk.Now, errThen429)

	if _, _, err := pd.Execute(execReq(false, "{}")); err == nil {
		t.Fatal("first dial should fail")
	}
	clk.advance(1 * time.Second)
	resp, _, err := pd.Execute(execReq(false, "{}"))
	if err != nil {
		t.Fatalf("second: %v", err)
	}
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Errorf("status = %d", resp.StatusCode)
	}
	_ = resp.Body.Close()
	h := pd.st.members[0].health
	if h.fails != 0 || !h.until.IsZero() {
		t.Errorf("429 did not reset health: fails=%d until=%v", h.fails, h.until)
	}
}

// TestPoolHealthDisabledNeverStrikes pins health.enabled=false: failures
// never cool anything down.
func TestPoolHealthDisabledNeverStrikes(t *testing.T) {
	dead := &stubEndpoint{script: []stubResult{{err: errors.New("down")}}}
	pd, _ := newTestPool(poolMembers(Config{}), RoundRobin,
		FallbackPolicy{Enabled: false, MaxAttempts: 1}, HealthPolicy{Enabled: false, FailureThreshold: 1, Cooldown: time.Second}, nil, dead)

	for i := 0; i < 5; i++ {
		if _, _, err := pd.Execute(execReq(false, "{}")); err == nil {
			t.Fatalf("request %d: expected failure", i)
		}
	}
	h := pd.st.members[0].health
	if h.fails != 0 || !h.until.IsZero() {
		t.Errorf("disabled health tracked strikes: fails=%d until=%v", h.fails, h.until)
	}
}

// ---- concurrency cap ----

// TestPoolSaturatedMemberSkippedWithoutStrike pins the limiter gate: a full
// member is skipped (no dial, no attempt, no health strike), and takes
// traffic again once its permit returns.
func TestPoolSaturatedMemberSkippedWithoutStrike(t *testing.T) {
	capped := &stubEndpoint{script: []stubResult{okResult("{}")}}
	overflow := &stubEndpoint{script: []stubResult{okResult("{}")}}
	members := []Member{
		{Endpoint: Config{}, Streaming: true, Weight: 1, MaxConcurrency: 1},
		{Endpoint: Config{}, Streaming: true, Weight: 1},
	}
	pd, _ := newTestPool(members, RoundRobin,
		FallbackPolicy{Enabled: true, MaxAttempts: 3}, HealthPolicy{Enabled: true, FailureThreshold: 1, Cooldown: 30 * time.Second}, nil, capped, overflow)

	// First request occupies the capped member's permit (body held open).
	resp1, _, err := pd.Execute(execReq(false, "{}"))
	if err != nil {
		t.Fatalf("first: %v", err)
	}
	// Second request skips the saturated member for the free one.
	resp2, info, err := pd.Execute(execReq(false, "{}"))
	if err != nil {
		t.Fatalf("second: %v", err)
	}
	_ = resp2.Body.Close()
	if overflow.hitCount() != 1 {
		t.Errorf("saturated member not skipped (overflow dials = %d)", overflow.hitCount())
	}
	if info.Attempts != 1 {
		t.Errorf("skip consumed an attempt: %d", info.Attempts)
	}
	if h := pd.st.members[0].health; h.fails != 0 || !h.until.IsZero() {
		t.Errorf("saturation struck health: fails=%d until=%v", h.fails, h.until)
	}
	_ = resp1.Body.Close()

	// With the permit released, rotation reaches the capped member again.
	resp3, _, err := pd.Execute(execReq(false, "{}"))
	if err != nil {
		t.Fatalf("third: %v", err)
	}
	_ = resp3.Body.Close()
	if capped.hitCount() != 2 {
		t.Errorf("capped member not rescheduled after release (%d hits)", capped.hitCount())
	}
}

// TestPoolSkipThenFailureConsumesOnlyRealDials pins the attempt budget's
// shape: skipped members are free, dialed ones count — a saturated head
// plus a dead tail under max-attempts=1 fails with the dead endpoint's
// error after exactly one dial.
func TestPoolSkipThenFailureConsumesOnlyRealDials(t *testing.T) {
	full := &stubEndpoint{script: []stubResult{okResult("{}")}}
	dead := &stubEndpoint{script: []stubResult{{err: errors.New("dead")}}}
	members := []Member{
		{Endpoint: Config{}, Streaming: true, Weight: 1, MaxConcurrency: 1},
		{Endpoint: Config{}, Streaming: true, Weight: 1},
	}
	pd, _ := newTestPool(members, RoundRobin,
		FallbackPolicy{Enabled: true, MaxAttempts: 1}, HealthPolicy{Enabled: false}, nil, full, dead)

	// Fill the head member's permit and hold it.
	held, _, err := pd.Execute(execReq(false, "{}"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = held.Body.Close() }()

	_, info, err := pd.Execute(execReq(false, "{}"))
	if err == nil || !strings.Contains(err.Error(), "dead") {
		t.Fatalf("err = %v, want the dialed endpoint's failure", err)
	}
	if info.Attempts != 1 {
		t.Errorf("attempts = %d, want 1 (skip is free, dial counts)", info.Attempts)
	}
	if info.Exhausted {
		t.Errorf("a real dial happened; not exhaustion")
	}
}

// ---- info metadata ----

// TestPoolAttemptInfoCarriesEndpointIdentity pins the log-facing metadata:
// kind and scheme+host of the last dialed endpoint, never userinfo.
func TestPoolAttemptInfoCarriesEndpointIdentity(t *testing.T) {
	proxyURL, err := url.Parse("socks5h://user:secret@proxy.example:1080")
	if err != nil {
		t.Fatal(err)
	}
	dead := &stubEndpoint{script: []stubResult{{err: dialErr("boom")}}}
	live := &stubEndpoint{script: []stubResult{okResult("{}")}}
	members := poolMembers(Config{Kind: Proxy, ProxyURL: proxyURL}, Config{})
	pd, _ := newTestPool(members, RoundRobin,
		FallbackPolicy{Enabled: true, MaxAttempts: 3}, HealthPolicy{Enabled: false}, nil, dead, live)

	// Success through the second endpoint: info names the one that answered.
	resp, info, err := pd.Execute(execReq(false, "{}"))
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if info.Kind != "direct" || info.Target != "direct" {
		t.Errorf("info = %+v, want the serving endpoint", info)
	}

	// All fail: info names the last one dialed, sanitized.
	pd2, _ := newTestPool(poolMembers(Config{Kind: Proxy, ProxyURL: proxyURL}), RoundRobin,
		FallbackPolicy{Enabled: true, MaxAttempts: 1}, HealthPolicy{Enabled: false}, nil, dead)
	_, info, err = pd2.Execute(execReq(false, "{}"))
	if err == nil {
		t.Fatal("expected failure")
	}
	if info.Kind != "socks5h" || info.Target != "socks5h://proxy.example:1080" {
		t.Errorf("info = %+v, want proxy scheme and scheme+host", info)
	}
	if strings.Contains(info.Target, "secret") {
		t.Errorf("userinfo leaked into attempt info: %q", info.Target)
	}
}
