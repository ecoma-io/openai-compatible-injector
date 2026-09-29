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

// TestBudgetRequestRemainingIgnoresTheCandidateEnvelope pins the distinction
// the completion record depends on: Remaining answers "can the walk do one
// more of anything", RequestRemaining answers "how much of the request-wide
// ceiling is left". After an exhausted candidate the first is zero and the
// second is not — and a finished walk that reported only the first would tell
// an operator nothing about the headroom their request was granted.
func TestBudgetRequestRemainingIgnoresTheCandidateEnvelope(t *testing.T) {
	clk := newTestClock()
	b := NewBudget(Envelope{MaxExchanges: 32, MaxElapsed: time.Minute}, clk.now)
	b.BeginCandidate(Envelope{MaxExchanges: 2, MaxElapsed: time.Minute})
	mustConsume(t, b, 2)
	if got := b.Remaining(); got != 0 {
		t.Fatalf("remaining = %d, want the spent candidate envelope to bind it", got)
	}
	if got := b.RequestRemaining(); got != 30 {
		t.Fatalf("request remaining = %d, want the request envelope's own remainder (30)", got)
	}
	if got := b.Exhausted(); got != ExhaustionCandidate {
		t.Fatalf("exhaustion = %v, want candidate", got)
	}
	// A fresh candidate restores headroom on the tighter envelope too, which
	// is why the request-wide number is the one worth reporting as history.
	b.BeginCandidate(Envelope{MaxExchanges: 2, MaxElapsed: time.Minute})
	if got := b.RequestRemaining(); got != 30 {
		t.Fatalf("request remaining after a fresh candidate = %d, want 30", got)
	}
	if got := b.Remaining(); got != 2 {
		t.Fatalf("remaining after a fresh candidate = %d, want 2", got)
	}
}

// TestBudgetRequestRemainingFloorsAtZero guards the evidence field against a
// negative reading. A request envelope cannot be overspent through the seam,
// but the accessor must not depend on that to stay a sane number.
func TestBudgetRequestRemainingFloorsAtZero(t *testing.T) {
	clk := newTestClock()
	b := NewBudget(Envelope{MaxExchanges: 1, MaxElapsed: time.Minute}, clk.now)
	b.BeginCandidate(Envelope{MaxExchanges: 4, MaxElapsed: time.Minute})
	mustConsume(t, b, 1)
	// The request envelope is checked first, so the candidate's spare units
	// are unreachable here: the seat is refused, never overspent.
	if b.ConsumeExchange() {
		t.Fatal("a candidate envelope bought an exchange the request envelope refused")
	}
	if got := b.RequestRemaining(); got != 0 {
		t.Fatalf("request remaining = %d, want 0", got)
	}
}

// TestBudgetRemainingElapsedIsTheTighterEnvelope pins the exchange-bound
// seam: what an exchange that starts now is still allowed to take is the
// smaller of the two envelopes' remaining time. The candidate's window is
// the tighter one in the shipped defaults, so the candidate's remaining
// governs until the request's own ceiling is closer.
func TestBudgetRemainingElapsedIsTheTighterEnvelope(t *testing.T) {
	clk := newTestClock()
	b := NewBudget(Envelope{MaxExchanges: 100, MaxElapsed: 30 * time.Second}, clk.now)
	b.BeginCandidate(Envelope{MaxExchanges: 100, MaxElapsed: 10 * time.Second})

	// Nothing has elapsed: the candidate's shorter window governs.
	if got := b.RemainingElapsed(); got != 10*time.Second {
		t.Fatalf("remaining at start = %v, want the candidate's 10s", got)
	}
	clk.advance(6 * time.Second)
	if got := b.RemainingElapsed(); got != 4*time.Second {
		t.Fatalf("remaining at 6s = %v, want the candidate's 4s", got)
	}

	// The request clock outlives the candidate's 10s. A fresh candidate
	// reopens its own 10s window, but the request's is now only 24s away —
	// which is still looser, so the candidate's window governs again.
	b.BeginCandidate(Envelope{MaxExchanges: 100, MaxElapsed: 10 * time.Second})
	if got := b.RemainingElapsed(); got != 10*time.Second {
		t.Fatalf("remaining after fresh candidate = %v, want 10s", got)
	}
	clk.advance(21 * time.Second) // 27s into the request, 21s into the candidate
	// The candidate has overrun, so whatever the request still allows, the
	// exchange must refuse: the seam reports the min of the two envelopes,
	// and a spent candidate means zero usable time even while the request
	// still has 3s.
	if got := b.RemainingElapsed(); got > 0 {
		t.Fatalf("remaining at candidate 21s = %v, want non-positive (candidate spent)", got)
	}
}

// TestBudgetRemainingElapsedReportsASpentWindowAsNonPositive pins the
// fail-fast direction: a remaining elapsed of zero or less must be returned
// as-is, not clamped to a fresh window — clamping would turn a spent
// envelope into one more dial. This is the read that makes a late exchange
// refuse exactly where ConsumeExchange would.
func TestBudgetRemainingElapsedReportsASpentWindowAsNonPositive(t *testing.T) {
	clk := newTestClock()
	b := NewBudget(Envelope{MaxExchanges: 100, MaxElapsed: 10 * time.Second}, clk.now)
	b.BeginCandidate(Envelope{MaxExchanges: 100, MaxElapsed: 10 * time.Second})
	clk.advance(12 * time.Second)
	if got := b.RemainingElapsed(); got > 0 {
		t.Fatalf("remaining after both windows overrun = %v, want non-positive", got)
	}
}
