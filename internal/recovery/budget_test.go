package recovery

import (
	"testing"
	"time"
)

type testClock struct{ t time.Time }

func newTestClock() *testClock {
	return &testClock{t: time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC)}
}

func (c *testClock) now() time.Time          { return c.t }
func (c *testClock) advance(d time.Duration) { c.t = c.t.Add(d) }

// mustConsume asserts that the envelope funds n more exchanges. It exists so
// multi-consume expectations read as one statement rather than an operator
// chain.
func mustConsume(t *testing.T, b *Budget, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		if !b.ConsumeExchange() {
			t.Fatalf("exchange %d of %d was refused with room to spare", i+1, n)
		}
	}
}

func TestBudgetConsumesPerExchange(t *testing.T) {
	clk := newTestClock()
	b := NewBudget(Envelope{MaxExchanges: 3, MaxElapsed: time.Minute}, clk.now)
	b.BeginCandidate(Envelope{MaxExchanges: 3, MaxElapsed: time.Minute})
	for i := 1; i <= 3; i++ {
		if !b.ConsumeExchange() {
			t.Fatalf("exchange %d was refused with room to spare", i)
		}
	}
	if b.ConsumeExchange() {
		t.Fatal("the request envelope allowed an exchange past its ceiling")
	}
	if got := b.RequestExchanges(); got != 3 {
		t.Fatalf("request exchanges = %d, want 3", got)
	}
	if got := b.Remaining(); got != 0 {
		t.Fatalf("remaining = %d, want 0", got)
	}
	if got := b.Exhausted(); got != ExhaustionRequest {
		t.Fatalf("exhaustion = %v, want request", got)
	}
}

// TestBudgetNestsCandidateInsideRequest is the invariant that makes
// amplification impossible to configure by accident: the candidate envelope
// stops spending before the request envelope is anywhere near spent, and the
// request envelope is never refilled by entering a new candidate.
func TestBudgetNestsCandidateInsideRequest(t *testing.T) {
	clk := newTestClock()
	b := NewBudget(Envelope{MaxExchanges: 6, MaxElapsed: time.Minute}, clk.now)
	cand := Envelope{MaxExchanges: 2, MaxElapsed: time.Minute}

	b.BeginCandidate(cand)
	mustConsume(t, b, 2)
	if b.ConsumeExchange() {
		t.Fatal("the candidate envelope allowed a third exchange")
	}
	if got := b.Exhausted(); got != ExhaustionCandidate {
		t.Fatalf("exhaustion = %v, want candidate", got)
	}

	// A new candidate refills ITS own envelope and nothing else.
	b.BeginCandidate(cand)
	if got := b.CandidateExchanges(); got != 0 {
		t.Fatalf("a new candidate did not start fresh: %d", got)
	}
	if got := b.RequestExchanges(); got != 2 {
		t.Fatalf("entering a candidate refilled the request envelope: %d", got)
	}
	mustConsume(t, b, 2)
	if got := b.Exhausted(); got != ExhaustionCandidate {
		t.Fatalf("a spent candidate envelope should report the candidate: %v", got)
	}

	// A third candidate has its own room left, but the request does not.
	b.BeginCandidate(cand)
	mustConsume(t, b, 2)
	if got := b.Exhausted(); got != ExhaustionRequest {
		t.Fatalf("the request ceiling should now bind: %v", got)
	}
	if b.ConsumeExchange() {
		t.Fatal("the request ceiling was crossed")
	}
	if got := b.RequestExchanges(); got != 6 {
		t.Fatalf("request exchanges = %d, want 6", got)
	}
}

func TestBudgetElapsedEnvelopes(t *testing.T) {
	clk := newTestClock()
	b := NewBudget(Envelope{MaxExchanges: 100, MaxElapsed: 30 * time.Second}, clk.now)
	b.BeginCandidate(Envelope{MaxExchanges: 100, MaxElapsed: 10 * time.Second})

	clk.advance(9 * time.Second)
	if !b.ConsumeExchange() {
		t.Fatal("the candidate envelope expired early")
	}
	clk.advance(2 * time.Second)
	if got := b.Exhausted(); got != ExhaustionCandidate {
		t.Fatalf("exhaustion = %v, want candidate", got)
	}
	if b.ConsumeExchange() {
		t.Fatal("an exchange was allowed past the candidate's wall-clock ceiling")
	}

	// The candidate envelope resets with the candidate; the request's clock
	// keeps running from the request's start.
	b.BeginCandidate(Envelope{MaxExchanges: 100, MaxElapsed: 10 * time.Second})
	if got := b.Exhausted(); got != ExhaustionNone {
		t.Fatalf("a fresh candidate did not reopen the gate: %v", got)
	}
	clk.advance(20 * time.Second)
	if got := b.Exhausted(); got != ExhaustionRequest {
		t.Fatalf("exhaustion = %v, want request", got)
	}
}

func TestBudgetElapsedBoundaryIsSpentAtTheCeiling(t *testing.T) {
	clk := newTestClock()
	b := NewBudget(Envelope{MaxExchanges: 10, MaxElapsed: time.Second}, clk.now)
	b.BeginCandidate(Envelope{MaxExchanges: 10, MaxElapsed: time.Second})
	clk.advance(time.Second)
	if got := b.Exhausted(); got != ExhaustionRequest {
		t.Fatalf("exactly at the ceiling the envelope is %v, want spent", got)
	}
	if b.ConsumeExchange() {
		t.Fatal("an exchange was allowed exactly at the ceiling")
	}
}

// TestBudgetIsTheExchangeTruth documents that the budget, consumed where the
// dials happen, is what the evidence counts — not a handler-side tally that
// could drift from it.
func TestBudgetIsTheExchangeTruth(t *testing.T) {
	clk := newTestClock()
	b := NewBudget(Envelope{MaxExchanges: 32, MaxElapsed: time.Minute}, clk.now)
	b.BeginCandidate(Envelope{MaxExchanges: 16, MaxElapsed: time.Minute})
	for i := 0; i < 5; i++ {
		if !b.ConsumeExchange() {
			t.Fatalf("exchange %d refused", i+1)
		}
	}
	if got := b.RequestExchanges(); got != 5 {
		t.Fatalf("request exchanges = %d, want 5", got)
	}
	if got := b.CandidateExchanges(); got != 5 {
		t.Fatalf("candidate exchanges = %d, want 5", got)
	}
	if got := b.Remaining(); got != 11 {
		t.Fatalf("remaining = %d, want the tighter envelope's remainder (11)", got)
	}
}
