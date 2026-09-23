package recovery

import (
	"context"
	"testing"
	"time"
)

// newTestEngineWithClock builds an engine on a caller-supplied clock and a
// jitter draw of zero, so every delay in these tests is the schedule's exact
// value rather than a range. The clock is supplied rather than made here
// when the test needs to build a context whose deadline lives on that same
// clock — a request context and the engine must not be measured by two
// different clocks.
func newTestEngineWithClock(ctx context.Context, pol Policy, clk *testClock) *Engine {
	return NewEngine(ctx, pol,
		WithClock(clk.now),
		WithJitterSource(func() float64 { return 0 }),
	)
}

func newTestEngine(t *testing.T, ctx context.Context, pol Policy) (*Engine, *testClock) {
	t.Helper()
	clk := newTestClock()
	return newTestEngineWithClock(ctx, pol, clk), clk
}

func httpObs(status, attempt int) Observation {
	return Observation{
		Class:            FailureHTTP,
		HTTPStatus:       status,
		StatusClass:      StatusClassOf(status),
		CandidateIndex:   1,
		CandidateAttempt: attempt,
		RetryIndex:       attempt - 1,
	}
}

// TestEngineRetriesThenFallsBackThenStops walks the default policy's whole
// shape: one same-candidate retry, one fallback, then the walk ends.
func TestEngineRetriesThenFallsBackThenStops(t *testing.T) {
	e, clk := newTestEngine(t, context.Background(), Default())
	if !e.EnterCandidate(Default()) {
		t.Fatal("the primary candidate was not enterable")
	}
	clk.advance(time.Millisecond)

	if d := e.Observe(httpObs(429, 1)); d.Action != ActionRetry || d.RuleID != "http-429" || d.Reason != "http_429" {
		t.Fatalf("first 429: %+v", d)
	}
	if d := e.Observe(httpObs(429, 2)); d.Action != ActionFallback {
		t.Fatalf("second 429 should exhaust the retry budget and fall back: %+v", d)
	}
	if !e.EnterCandidate(Default()) {
		t.Fatal("the second candidate was not enterable")
	}
	if d := e.Observe(httpObs(429, 1)); d.Action != ActionRetry {
		t.Fatalf("the second candidate has its own retry budget: %+v", d)
	}
	if d := e.Observe(httpObs(503, 2)); d.Action != ActionTerminal {
		t.Fatalf("the walk should stop once the fallback budget is spent: %+v", d)
	}
	if e.CandidatesEntered() != 2 {
		t.Fatalf("candidates entered = %d, want 2", e.CandidatesEntered())
	}
	if e.EnterCandidate(Default()) {
		t.Fatal("a third candidate was enterable under a two-candidate budget")
	}
}

// TestEngineFallbackBudgetCountsCandidatesEntered pins the semantics: the
// budget is candidates ENTERED, not chain entries listed.
func TestEngineFallbackBudgetCountsCandidatesEntered(t *testing.T) {
	pol := Default()
	pol.Fallback.MaxCandidates = 3
	e, _ := newTestEngine(t, context.Background(), pol)
	for i := 0; i < 3; i++ {
		if !e.EnterCandidate(pol) {
			t.Fatalf("candidate %d was refused inside a three-candidate budget", i+1)
		}
	}
	if e.EnterCandidate(pol) {
		t.Fatal("a fourth candidate was enterable")
	}
	if e.CandidatesEntered() != 3 {
		t.Fatalf("candidates entered = %d", e.CandidatesEntered())
	}
}

// TestEngineFallbackDisabledPinsThePrimary: with the walk off, a retryable
// failure still gets its same-candidate retries, and every other budget
// exhaustion is terminal.
func TestEngineFallbackDisabledPinsThePrimary(t *testing.T) {
	pol, err := Merge(Default(), Partial{Fallback: &FallbackPartial{Enabled: boolp(false)}})
	if err != nil {
		t.Fatalf("Merge: %v", err)
	}
	e, _ := newTestEngine(t, context.Background(), pol)
	if !e.EnterCandidate(pol) {
		t.Fatal("the primary was not enterable")
	}
	if d := e.Observe(httpObs(429, 1)); d.Action != ActionRetry {
		t.Fatalf("same-candidate retries must survive a pinned primary: %+v", d)
	}
	if d := e.Observe(httpObs(429, 2)); d.Action != ActionTerminal {
		t.Fatalf("a pinned primary must not fall back: %+v", d)
	}
	if d := e.Observe(httpObs(401, 1)); d.Action != ActionTerminal {
		t.Fatalf("a fallback-only status must be terminal with no fallback: %+v", d)
	}
}

func TestEngineTerminalStatusEndsTheWalk(t *testing.T) {
	e, _ := newTestEngine(t, context.Background(), Default())
	e.EnterCandidate(Default())
	if d := e.Observe(httpObs(400, 1)); d.Action != ActionTerminal || d.RuleID != "http-class-4xx" || d.Reason != "http_400" {
		t.Fatalf("400: %+v", d)
	}
}

// TestEngineCallerIsAnUnconditionalHardStop is the caller invariant: no
// rule, no budget, and no policy can turn a dead client into another
// upstream exchange.
func TestEngineCallerIsAnUnconditionalHardStop(t *testing.T) {
	cases := []struct {
		name string
		ctx  func() context.Context
		o    Observation
	}{
		{
			"an expired client context",
			func() context.Context {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				return ctx
			},
			httpObs(429, 1), // a status the matrix would retry
		},
		{
			"a caller-class failure under a live context",
			func() context.Context { return context.Background() },
			Observation{Class: FailureCaller, CallerCause: CallerCanceled, CandidateAttempt: 1},
		},
	}
	for _, c := range cases {
		e, _ := newTestEngine(t, c.ctx(), Default())
		e.EnterCandidate(Default())
		d := e.Observe(c.o)
		if d.Action != ActionTerminal || d.RuleID != RuleIDCaller {
			t.Errorf("%s: %+v", c.name, d)
		}
	}
}

// TestEngineCommittedIsAbsolute is the commitment invariant: a response the
// client has already started receiving is final, however retryable the
// failure that follows it.
func TestEngineCommittedIsAbsolute(t *testing.T) {
	e, clk := newTestEngine(t, context.Background(), Default())
	e.EnterCandidate(Default())
	clk.advance(time.Millisecond)
	o := httpObs(503, 1)
	o.Committed = true
	o.Streaming = true
	d := e.Observe(o)
	if d.Action != ActionTerminal || d.RuleID != RuleIDCommitted {
		t.Fatalf("a committed stream that failed got %+v", d)
	}
}

func TestEngineExchangeEnvelopeStopsTheWalk(t *testing.T) {
	pol := Default()
	pol.Budget.Candidate.MaxExchanges = 2
	e, _ := newTestEngine(t, context.Background(), pol)
	e.EnterCandidate(pol)
	if !e.Budget().ConsumeExchange() || !e.Budget().ConsumeExchange() {
		t.Fatal("the candidate's two exchanges were not allowed")
	}
	d := e.Observe(httpObs(429, 1))
	if d.Action != ActionTerminal || d.RuleID != RuleIDBudgetCandidate {
		t.Fatalf("a spent candidate envelope got %+v", d)
	}
}

func TestEngineRequestEnvelopeStopsTheWalk(t *testing.T) {
	pol := Default()
	pol.Budget.Request.MaxExchanges = 4
	pol.Budget.Candidate.MaxExchanges = 4
	// The walk may reach four candidates; the request envelope is what stops
	// it, one exchange short of the fourth.
	pol.Fallback.MaxCandidates = 4
	e, _ := newTestEngine(t, context.Background(), pol)
	for i := 0; i < 4; i++ {
		if !e.EnterCandidate(pol) {
			t.Fatalf("candidate %d was refused", i+1)
		}
		if !e.Budget().ConsumeExchange() {
			t.Fatalf("exchange %d refused", i+1)
		}
		d := e.Observe(httpObs(429, 1))
		if i < 3 && d.Action == ActionTerminal {
			t.Fatalf("the walk stopped early at exchange %d: %+v", i+1, d)
		}
		if i == 3 {
			if d.Action != ActionTerminal || d.RuleID != RuleIDBudgetRequest {
				t.Fatalf("a spent request envelope got %+v", d)
			}
		}
	}
}

// TestEngineRetryDelaySchedule pins the backoff ladder and its ceiling.
func TestEngineRetryDelaySchedule(t *testing.T) {
	pol, err := Merge(Default(), Partial{
		Retry:  &RetryPartial{MaxRetries: intp(5)},
		Budget: &BudgetPartial{Candidate: &EnvelopePartial{MaxExchanges: intp(8)}},
	})
	if err != nil {
		t.Fatalf("Merge: %v", err)
	}
	e, clk := newTestEngine(t, context.Background(), pol)
	e.EnterCandidate(pol)
	want := []time.Duration{
		250 * time.Millisecond,
		500 * time.Millisecond,
		time.Second,
		2 * time.Second, // the ladder reaches the ceiling
		2 * time.Second, // and stays there rather than doubling past it
	}
	for i, w := range want {
		d := e.Observe(httpObs(429, i+1))
		if d.Action != ActionRetry {
			t.Fatalf("attempt %d: %+v", i+1, d)
		}
		if d.Delay != w {
			t.Fatalf("attempt %d delay = %v, want %v", i+1, d.Delay, w)
		}
		clk.advance(d.Delay)
	}
	if d := e.Observe(httpObs(429, len(want)+1)); d.Action != ActionFallback {
		t.Fatalf("a spent retry budget got %+v", d)
	}
}

// TestEngineRetryAfterIsOnlyAFloorAndNeverUnbounded: an upstream asking for
// an hour gets the backoff ceiling, which is the threat model the policy is
// built around.
func TestEngineRetryAfterIsOnlyAFloorAndNeverUnbounded(t *testing.T) {
	cases := []struct {
		name string
		pol  func() Policy
		ra   time.Duration
		want time.Duration
	}{
		{
			"a directive above the backoff ceiling is ceilinged",
			func() Policy { return Default() },
			time.Hour,
			DefaultBackoffMax,
		},
		{
			"a directive below the backoff is ignored as a floor",
			func() Policy { return Default() },
			50 * time.Millisecond,
			DefaultBackoffInitial,
		},
		{
			"a directive between the backoff and its ceiling wins",
			func() Policy { return Default() },
			time.Second,
			time.Second,
		},
		{
			"ignore discards the directive",
			func() Policy {
				p, err := Merge(Default(), Partial{RetryAfter: &RetryAfterPartial{Mode: func() *RetryAfterMode {
					m := RetryAfterIgnore
					return &m
				}()}})
				if err != nil {
					t.Fatalf("Merge: %v", err)
				}
				return p
			},
			time.Hour,
			DefaultBackoffInitial,
		},
		{
			"the retry-after policy's own ceiling binds first",
			func() Policy {
				p, err := Merge(Default(), Partial{RetryAfter: &RetryAfterPartial{MaxDelay: durp(300 * time.Millisecond)}})
				if err != nil {
					t.Fatalf("Merge: %v", err)
				}
				return p
			},
			time.Second,
			300 * time.Millisecond,
		},
	}
	for _, c := range cases {
		pol := c.pol()
		e, _ := newTestEngine(t, context.Background(), pol)
		e.EnterCandidate(pol)
		o := httpObs(429, 1)
		o.RetryAfter = c.ra
		d := e.Observe(o)
		if d.Action != ActionRetry {
			t.Fatalf("%s: %+v", c.name, d)
		}
		if d.Delay != c.want {
			t.Errorf("%s: delay = %v, want %v", c.name, d.Delay, c.want)
		}
	}
}

func TestEngineRetryDelayNeverPassesTheCallerDeadline(t *testing.T) {
	clk := newTestClock()
	// The deadline lives on the same clock the engine reads: a request
	// context and its engine must never be measured by two clocks.
	ctx, cancel := context.WithDeadline(context.Background(), clk.now().Add(100*time.Millisecond))
	defer cancel()
	pol := Default()
	e := newTestEngineWithClock(ctx, pol, clk)
	e.EnterCandidate(pol)
	o := httpObs(429, 1)
	o.RetryAfter = time.Hour
	if d := e.Observe(o); d.Delay > 100*time.Millisecond {
		t.Fatalf("delay %v reaches past the caller's deadline", d.Delay)
	}
	// An expired deadline is the caller's condition, not a retry: it is
	// terminal, decided from the context rather than from the status.
	clk.advance(200 * time.Millisecond)
	if d := e.Observe(httpObs(429, 2)); d.Action != ActionTerminal || d.RuleID != RuleIDCaller {
		t.Fatalf("an expired deadline got %+v", d)
	}
}

func TestEngineRetryDelayNeverPassesTheWindow(t *testing.T) {
	pol, err := Merge(Default(), Partial{
		// The candidate envelope must stay at or above the retry window.
		Retry: &RetryPartial{
			MaxRetries: intp(4),
			MaxElapsed: durp(time.Second),
			Backoff:    &BackoffPartial{Initial: durp(900 * time.Millisecond), Max: durp(900 * time.Millisecond)},
		},
	})
	if err != nil {
		t.Fatalf("Merge: %v", err)
	}
	e, clk := newTestEngine(t, context.Background(), pol)
	e.EnterCandidate(pol)
	clk.advance(400 * time.Millisecond)
	d := e.Observe(httpObs(429, 1))
	if d.Action != ActionRetry {
		t.Fatalf("%+v", d)
	}
	if d.Delay != 600*time.Millisecond {
		t.Fatalf("delay = %v, want the remaining window (600ms)", d.Delay)
	}
}

func TestEngineRetryWindowClosesTheCandidate(t *testing.T) {
	pol, err := Merge(Default(), Partial{Retry: &RetryPartial{MaxElapsed: durp(time.Second)}})
	if err != nil {
		t.Fatalf("Merge: %v", err)
	}
	e, clk := newTestEngine(t, context.Background(), pol)
	e.EnterCandidate(pol)
	clk.advance(time.Second)
	if d := e.Observe(httpObs(429, 1)); d.Action != ActionFallback {
		t.Fatalf("a closed retry window should move on: %+v", d)
	}
}

func TestEngineRetryOnExhaustedTerminal(t *testing.T) {
	pol, err := Merge(Default(), Partial{Retry: &RetryPartial{OnExhausted: action(ActionTerminal)}})
	if err != nil {
		t.Fatalf("Merge: %v", err)
	}
	e, _ := newTestEngine(t, context.Background(), pol)
	e.EnterCandidate(pol)
	if d := e.Observe(httpObs(429, 2)); d.Action != ActionTerminal {
		t.Fatalf("a terminal on-exhausted must not fall back: %+v", d)
	}
}

// TestEngineRetryCountLayering is the layered-budget scenario: global 2,
// provider 5, model 3 — the model wins outright (the layers replace, they do
// not add), so the walk gets three retries, not ten.
func TestEngineRetryCountLayering(t *testing.T) {
	pol, err := Resolve(Default(),
		Layer{Name: "global", Partial: Partial{
			Retry:  &RetryPartial{MaxRetries: intp(2)},
			Budget: &BudgetPartial{Candidate: &EnvelopePartial{MaxExchanges: intp(16)}},
		}},
		Layer{Name: "provider", Partial: Partial{Retry: &RetryPartial{MaxRetries: intp(5)}}},
		Layer{Name: "model", Partial: Partial{Retry: &RetryPartial{MaxRetries: intp(3)}}},
	)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if pol.Retry.MaxRetries != 3 {
		t.Fatalf("effective retries = %d, want 3", pol.Retry.MaxRetries)
	}
	e, _ := newTestEngine(t, context.Background(), pol)
	e.EnterCandidate(pol)
	for attempt := 1; attempt <= 3; attempt++ {
		if d := e.Observe(httpObs(429, attempt)); d.Action != ActionRetry {
			t.Fatalf("attempt %d: %+v", attempt, d)
		}
	}
	if d := e.Observe(httpObs(429, 4)); d.Action != ActionFallback {
		t.Fatalf("attempt 4: %+v", d)
	}
}

// TestEngineMidWalkReloadInvisibility: the engine's policy comes from the
// snapshot the request started under, so resolving a different policy later
// — a reload — cannot reshape a walk already in flight.
func TestEngineMidWalkReloadInvisibility(t *testing.T) {
	old := Default()
	e, _ := newTestEngine(t, context.Background(), old)
	e.EnterCandidate(old)

	// A reload lands: a new policy that would retry a 401 and never fall back.
	reloaded, err := Merge(Default(), Partial{
		Matrix: &MatrixPartial{Rules: []Rule{
			{ID: StatusRuleID(401), Match: Match{Status: 401}, Action: ActionRetry},
		}},
	})
	if err != nil {
		t.Fatalf("Merge: %v", err)
	}
	_ = reloaded

	// The in-flight request still answers under the policy it froze.
	if d := e.Observe(httpObs(401, 1)); d.Action != ActionFallback {
		t.Fatalf("the in-flight walk followed a later policy: %+v", d)
	}
}

// TestEngineIsPureForTheSameInputs is the replayability invariant: identical
// policy, identical observation, identical budget state, identical decision.
func TestEngineIsPureForTheSameInputs(t *testing.T) {
	run := func() Decision {
		e, _ := newTestEngine(t, context.Background(), Default())
		e.EnterCandidate(Default())
		o := httpObs(429, 1)
		o.RetryAfter = 700 * time.Millisecond
		return e.Observe(o)
	}
	first := run()
	for i := 0; i < 10; i++ {
		if d := run(); d != first {
			t.Fatalf("run %d differed: %+v vs %+v", i, d, first)
		}
	}
}
