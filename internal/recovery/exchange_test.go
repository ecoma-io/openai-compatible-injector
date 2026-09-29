package recovery

// Boundary tests for the ATOMIC exchange acquisition — the P2 finding.
//
// The defect these pin is that `ConsumeExchange() bool` and
// `RemainingElapsed() time.Duration` were two observations of one budget, taken
// one after the other. Under a single walk they agreed by luck, because
// nothing moved between them. The day a second goroutine could ask the same
// question — the future concurrent dial path the seam's own comment names — they
// would disagree at exactly the interesting moment: the last unit.
//
// So the acquisition is now one value, granted or refused, and this file holds
// the cases that decide whether that value is honest. Every case is stated as
// what must NOT happen: an exchange claimed that the envelope never funded, a
// refusal that still moved a counter, two callers that both believe they hold
// the final unit, a window handed out from a clock reading the envelope had
// already given up.

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestAcquireRefusesAtExactlyTheElapsedCeiling is the exact-expiry case, and
// the elapsed half is a `>=`: at precisely the ceiling the envelope is spent.
// Half-open vs closed is not a matter of taste here. If it were `<`, a caller
// that sampled the clock at the exact instant of expiry would be handed a
// positive window for an exchange the envelope has already given up, and
// `window > 0` is precisely the condition the transport treats as "a real
// bound, dial within it".
func TestAcquireRefusesAtExactlyTheElapsedCeiling(t *testing.T) {
	clk := newTestClock()
	b := NewBudget(Envelope{MaxExchanges: 10, MaxElapsed: time.Second}, clk.now)
	b.BeginCandidate(Envelope{MaxExchanges: 10, MaxElapsed: 10 * time.Second})

	// One nanosecond before the ceiling: funded, with a window that is exactly
	// that nanosecond. The window is not clamped away from zero, because a
	// caller about to dial needs to see how little room there is.
	clk.advance(time.Second - time.Nanosecond)
	e := b.AcquireExchange()
	if !e.Granted {
		t.Fatal("an exchange one nanosecond inside the window was refused")
	}
	if e.Window != time.Nanosecond {
		t.Errorf("window = %v, want the 1ns that remained", e.Window)
	}

	// At the ceiling: refused, and refused with nothing granted.
	clk.advance(time.Nanosecond)
	if got := b.AcquireExchange(); got.Granted {
		t.Error("the exact expiry instant was funded — the elapsed half is half-open, not closed")
	}
	if got := b.RemainingElapsed(); got > 0 {
		t.Errorf("remaining elapsed at the ceiling = %v, want non-positive", got)
	}
}

// TestAcquireRefusesAfterExpiryAndSpendsNothing covers "acquire after expiry"
// together with the refusal's accounting, because they are one fact: a refusal
// that moved a counter would make `upstream_exchanges` name exchanges that were
// never authorised, and the walk would stop with its own evidence lying.
func TestAcquireRefusesAfterExpiryAndSpendsNothing(t *testing.T) {
	clk := newTestClock()
	b := NewBudget(Envelope{MaxExchanges: 10, MaxElapsed: 10 * time.Second}, clk.now)
	b.BeginCandidate(Envelope{MaxExchanges: 10, MaxElapsed: 10 * time.Second})

	mustGrant(t, b, 2)
	before := b.RequestExchanges()
	clk.advance(30 * time.Second)

	for i := 0; i < 5; i++ {
		if b.AcquireExchange().Granted {
			t.Fatalf("refusal %d was funded after the window elapsed", i)
		}
	}
	if got := b.RequestExchanges(); got != before {
		t.Fatalf("request exchanges = %d after five refusals, want the %d that were authorised", got, before)
	}
	if got := b.CandidateExchanges(); got != before {
		t.Fatalf("candidate exchanges = %d after five refusals, want %d", got, before)
	}
	if got := b.RequestRemaining(); got != 8 {
		t.Errorf("request remaining = %d, want 8: a refusal must leave the headroom report untouched", got)
	}
}

// TestAcquireGrantsTheWindowFromTheSameReadingItRefusesOn is the atomicity
// itself, stated as an invariant over both halves. Under the old two-call seam a
// caller could be granted a claim by one clock reading and a window from a
// later one, so the window could outlive the authority that funded it. The
// property here is that a grant's window is always consistent with the envelope
// state at the instant the grant was made: never more than the tighter
// envelope's ceiling, and never positive once that envelope is spent.
func TestAcquireGrantsTheWindowFromTheSameReadingItRefusesOn(t *testing.T) {
	clk := newTestClock()
	const ceiling = 10 * time.Second
	b := NewBudget(Envelope{MaxExchanges: 100, MaxElapsed: 30 * time.Second}, clk.now)
	b.BeginCandidate(Envelope{MaxExchanges: 100, MaxElapsed: ceiling})

	// Walk the candidate's window in steps, checking at each grant that the
	// window never exceeds what the envelope still allows at that moment. The
	// last grant lands one second inside the ceiling; the step past it is the
	// refusal, checked separately below.
	for i := 0; i < 9; i++ {
		clk.advance(time.Second)
		e := b.AcquireExchange()
		if !e.Granted {
			t.Fatalf("grant %d refused one second before the ceiling", i)
		}
		want := ceiling - time.Duration(i+1)*time.Second
		if e.Window != want {
			t.Fatalf("grant %d window = %v, want %v — the window is not from the same reading that funded the claim", i, e.Window, want)
		}
	}
	// One second more puts the candidate exactly at the ceiling; the grant is
	// refused and it grants no time.
	clk.advance(time.Second)
	got := b.AcquireExchange()
	if got.Granted {
		t.Error("an exchange was funded at the candidate's elapsed ceiling")
	}
	if got.Window > 0 {
		t.Errorf("a refusal carried window %v: a refusal must grant no time at all", got.Window)
	}
}

// steppingClock advances on EVERY read, so a method that consults the clock
// twice sees two different instants. The frozen clock every other test here
// uses cannot express the difference between one reading and two: it moves
// only when a test tells it to, so a two-observation implementation produces
// exactly the same numbers as a one-observation one and passes in spite of the
// defect this method exists to fix.
type steppingClock struct {
	t    time.Time
	step time.Duration
	// reads counts the samples taken, so a test can assert the count and not
	// only the consequence.
	reads int
}

func (c *steppingClock) now() time.Time {
	c.reads++
	c.t = c.t.Add(c.step)
	return c.t
}

// TestAcquireReadsTheClockExactlyOnce is the test that distinguishes the fixed
// shape from the defect it fixes. Every other test here pins a value; this one
// pins the NUMBER of observations, which is the whole content of the fix.
//
// Two observations — a check that asks the clock, then a grant that asks again
// — hand back a window measured from a LATER instant than the one that
// authorised the exchange. The gap is the two-observation implementation's
// window, and the caller is about to dial on it: the claim succeeded against a
// reading the budget had already moved past. Asserting the window VALUE cannot
// catch that, because against a frozen clock the two readings are equal and
// the two implementations are indistinguishable; only the count differs.
func TestAcquireReadsTheClockExactlyOnce(t *testing.T) {
	for _, tc := range []struct {
		name    string
		advance time.Duration
	}{
		// A window that expires DURING the acquisition: the check reads
		// inside it and the grant does not. This is the case the finding is
		// about — the last unit, exactly.
		{name: "granted", advance: 5 * time.Second},
		// The refusal path must be equally single-sampled: two reads there
		// would let a spent envelope report a window that is not yet spent.
		{name: "refused", advance: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clk := &steppingClock{t: time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC), step: tc.advance}
			b := NewBudget(Envelope{MaxExchanges: 2, MaxElapsed: 10 * time.Second}, clk.now)
			b.BeginCandidate(Envelope{MaxExchanges: 2, MaxElapsed: 10 * time.Second})
			if tc.name == "refused" {
				// Spend the envelope by exchange count so the second call is
				// refused on the COUNT, with the elapsed half still open.
				if !b.AcquireExchange().Granted {
					t.Fatal("setup: the first exchange was refused")
				}
			}

			before := clk.reads
			got := b.AcquireExchange()
			if reads := clk.reads - before; reads != 1 {
				t.Errorf("AcquireExchange took %d clock readings, want exactly 1: "+
					"a second reading is a second observation, and a window granted "+
					"from it is not vouched for by the check that funded the claim", reads)
			}
			if !got.Granted {
				return
			}
			// The window must be the one measured AT the reading that funded
			// the claim. Only the stepping case can see the difference: its
			// second reading would report the ceiling fully spent and hand back
			// a 0s window, which the transport reads as "no real bound".
			if tc.advance == 0 {
				return
			}
			if want := 5 * time.Second; got.Window != want {
				t.Errorf("window = %v, want %v — the grant was measured a second time "+
					"after the funding check", got.Window, want)
			}
		})
	}
}

// TestAcquireNestsTheCandidateInsideTheRequest keeps the two envelope
// identities apart at the acquisition, not only at the predicate. The two stop
// different walks, and the walk reacts to them differently: a spent candidate
// may fall back to the next one, a spent request may not. If the acquisition
// could not say which of the two refused, the walk could not honour that
// difference.
func TestAcquireNestsTheCandidateInsideTheRequest(t *testing.T) {
	clk := newTestClock()
	b := NewBudget(Envelope{MaxExchanges: 4, MaxElapsed: time.Minute}, clk.now)

	// Candidate spent first: two of the request's four units, so the request
	// still has headroom and the refusal names the candidate.
	b.BeginCandidate(Envelope{MaxExchanges: 2, MaxElapsed: time.Minute})
	mustGrant(t, b, 2)
	if got := b.Exhausted(); got != ExhaustionCandidate {
		t.Fatalf("exhaustion = %v, want candidate", got)
	}
	if b.AcquireExchange().Granted {
		t.Fatal("a spent candidate envelope funded an exchange")
	}
	if got := b.RequestRemaining(); got != 2 {
		t.Errorf("request remaining = %d, want 2: a candidate refusal must not spend the request's units", got)
	}

	// Entering a fresh candidate reopens only the candidate's half. The two
	// units already spent are still spent, and the walk may still move.
	b.BeginCandidate(Envelope{MaxExchanges: 2, MaxElapsed: time.Minute})
	mustGrant(t, b, 2)
	if got := b.RequestExchanges(); got != 4 {
		t.Fatalf("request exchanges = %d, want 4", got)
	}

	// Now the request itself is spent, and no candidate can reopen it.
	b.BeginCandidate(Envelope{MaxExchanges: 2, MaxElapsed: time.Minute})
	if b.AcquireExchange().Granted {
		t.Fatal("a spent request envelope funded an exchange through a fresh candidate")
	}
	if got := b.Exhausted(); got != ExhaustionRequest {
		t.Errorf("exhaustion = %v, want request: the two identities must stay distinguishable at the acquisition", got)
	}
}

// TestAcquireBothExchangeLimitsBindAtOnce is the "both exchange limits" case:
// each envelope has its own count, and the acquisition must stop at whichever
// runs out first — and the stop it reports must be the one that actually ran
// out, since a wrong one sends the walk down the wrong branch.
func TestAcquireBothExchangeLimitsBindAtOnce(t *testing.T) {
	clk := newTestClock()
	// The request is looser on exchanges and tighter on time; the candidate is
	// the reverse. Each limit is therefore the binding one for a different
	// envelope, and both must be honoured.
	t.Run("candidate count binds", func(t *testing.T) {
		b := NewBudget(Envelope{MaxExchanges: 5, MaxElapsed: time.Hour}, clk.now)
		b.BeginCandidate(Envelope{MaxExchanges: 1, MaxElapsed: time.Hour})
		mustGrant(t, b, 1)
		if b.AcquireExchange().Granted {
			t.Fatal("a spent candidate exchange count funded another")
		}
		if got := b.Exhausted(); got != ExhaustionCandidate {
			t.Errorf("exhaustion = %v, want candidate", got)
		}
	})

	t.Run("request count binds", func(t *testing.T) {
		b := NewBudget(Envelope{MaxExchanges: 2, MaxElapsed: time.Hour}, clk.now)
		b.BeginCandidate(Envelope{MaxExchanges: 9, MaxElapsed: time.Hour})
		mustGrant(t, b, 2)
		if b.AcquireExchange().Granted {
			t.Fatal("a spent request exchange count funded another")
		}
		if got := b.Exhausted(); got != ExhaustionRequest {
			t.Errorf("exhaustion = %v, want request", got)
		}
	})

	t.Run("candidate time binds", func(t *testing.T) {
		b := NewBudget(Envelope{MaxExchanges: 5, MaxElapsed: time.Hour}, clk.now)
		b.BeginCandidate(Envelope{MaxExchanges: 5, MaxElapsed: time.Second})
		clk.advance(time.Second)
		if b.AcquireExchange().Granted {
			t.Fatal("a spent candidate elapsed half funded another")
		}
		if got := b.Exhausted(); got != ExhaustionCandidate {
			t.Errorf("exhaustion = %v, want candidate", got)
		}
	})

	t.Run("request time binds", func(t *testing.T) {
		b := NewBudget(Envelope{MaxExchanges: 5, MaxElapsed: time.Second}, clk.now)
		b.BeginCandidate(Envelope{MaxExchanges: 5, MaxElapsed: time.Hour})
		clk.advance(time.Second)
		if b.AcquireExchange().Granted {
			t.Fatal("a spent request elapsed half funded another")
		}
		if got := b.Exhausted(); got != ExhaustionRequest {
			t.Errorf("exhaustion = %v, want request", got)
		}
	})
}

// TestAcquireGrantsExactlyTheFinalExchange is the one-final-exchange case. A
// budget of one must fund exactly one exchange and refuse every later one —
// both because the walk must not be able to spend what it was not given, and
// because the count in the evidence must equal the number of real outbound
// exchanges for the log to mean anything.
func TestAcquireGrantsExactlyTheFinalExchange(t *testing.T) {
	clk := newTestClock()
	b := NewBudget(Envelope{MaxExchanges: 1, MaxElapsed: time.Hour}, clk.now)
	b.BeginCandidate(Envelope{MaxExchanges: 1, MaxElapsed: time.Hour})

	e := b.AcquireExchange()
	if !e.Granted {
		t.Fatal("the single funded exchange was refused")
	}
	if e.Window <= 0 {
		t.Errorf("the final exchange's window = %v, want a positive one", e.Window)
	}
	if b.AcquireExchange().Granted {
		t.Fatal("a second exchange was funded from a budget of one")
	}
	if got := b.RequestExchanges(); got != 1 {
		t.Fatalf("request exchanges = %d, want exactly the one authorised", got)
	}
	if got := b.CandidateExchanges(); got != 1 {
		t.Fatalf("candidate exchanges = %d, want exactly the one authorised", got)
	}
	if got := b.Remaining(); got != 0 {
		t.Errorf("remaining = %d, want 0", got)
	}
	if got := b.RequestRemaining(); got != 0 {
		t.Errorf("request remaining = %d, want 0", got)
	}
}

// TestAcquireRacesGrantTheLastUnitToExactlyOneCaller is the concurrency case
// the seam exists for. The budget is documented as safe for concurrent use
// because a future concurrent dial path must not be able to double-spend the
// last unit, so this is the case that documents it. Every racer asks for the
// same single remaining unit, and exactly one may be told it is funded.
//
// Run under -race, a counter that moved without the lock, or a grant handed
// out twice, fails here rather than on a loaded production host.
func TestAcquireRacesGrantTheLastUnitToExactlyOneCaller(t *testing.T) {
	const racers = 64
	for attempt := 0; attempt < 50; attempt++ {
		clk := newTestClock()
		b := NewBudget(Envelope{MaxExchanges: 1, MaxElapsed: time.Hour}, clk.now)
		b.BeginCandidate(Envelope{MaxExchanges: 1, MaxElapsed: time.Hour})

		var granted atomic.Int64
		var wg sync.WaitGroup
		start := make(chan struct{})
		for i := 0; i < racers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				if b.AcquireExchange().Granted {
					granted.Add(1)
				}
			}()
		}
		close(start)
		wg.Wait()

		if got := granted.Load(); got != 1 {
			t.Fatalf("attempt %d: %d of %d racers were funded the same last unit, want exactly 1", attempt, got, racers)
		}
		if got := b.RequestExchanges(); got != 1 {
			t.Fatalf("attempt %d: request exchanges = %d, want 1 — the counter must name authorised exchanges only", attempt, got)
		}
		if got := b.CandidateExchanges(); got != 1 {
			t.Fatalf("attempt %d: candidate exchanges = %d, want 1", attempt, got)
		}
	}
}

// TestAcquireRacesTheLastUnitOfALargerEnvelope is the same race with a
// non-trivial amount of room, which is where a check-then-increment would
// plausibly hide. Eight racers contend for the final four units of a twelve-unit
// envelope; exactly four may be funded, and the counters must agree.
func TestAcquireRacesTheLastUnitOfALargerEnvelope(t *testing.T) {
	const racers = 8
	const budgeted = 12
	clk := newTestClock()
	b := NewBudget(Envelope{MaxExchanges: budgeted, MaxElapsed: time.Hour}, clk.now)
	b.BeginCandidate(Envelope{MaxExchanges: budgeted, MaxElapsed: time.Hour})

	var granted atomic.Int64
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < racers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			// Two claims each, so the envelope's tail is contended rather
			// than exhausted by a single fast racer.
			for j := 0; j < 2; j++ {
				if b.AcquireExchange().Granted {
					granted.Add(1)
				}
			}
		}()
	}
	close(start)
	wg.Wait()

	if got := granted.Load(); got != budgeted {
		t.Fatalf("%d grants for a %d-exchange envelope", got, budgeted)
	}
	if got := b.RequestExchanges(); got != budgeted {
		t.Fatalf("request exchanges = %d, want %d", got, budgeted)
	}
	if got := b.CandidateExchanges(); got != budgeted {
		t.Fatalf("candidate exchanges = %d, want %d", got, budgeted)
	}
	if got := b.Remaining(); got != 0 {
		t.Errorf("remaining = %d, want 0", got)
	}
}

// TestAcquireRefusalUnderContentionSpendsNothing is the pairing of the two
// race tests: once the envelope is empty, however many callers keep asking, the
// counters must not drift. A walk that raced the last unit and then asked once
// more must still be able to report the truth.
func TestAcquireRefusalUnderContentionSpendsNothing(t *testing.T) {
	const racers = 32
	clk := newTestClock()
	b := NewBudget(Envelope{MaxExchanges: 3, MaxElapsed: time.Hour}, clk.now)
	b.BeginCandidate(Envelope{MaxExchanges: 3, MaxElapsed: time.Hour})
	mustGrant(t, b, 3)

	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < racers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for j := 0; j < 20; j++ {
				if b.AcquireExchange().Granted {
					t.Error("a spent envelope funded an exchange under contention")
				}
			}
		}()
	}
	close(start)
	wg.Wait()

	if got := b.RequestExchanges(); got != 3 {
		t.Fatalf("request exchanges = %d after 640 refusals, want 3", got)
	}
	if got := b.CandidateExchanges(); got != 3 {
		t.Fatalf("candidate exchanges = %d after 640 refusals, want 3", got)
	}
}

// TestAcquireWindowIsTheTighterEnvelopeTightest pins the window a grant carries
// against both envelopes independently: an exchange is funded by both, so its
// window may not outlive either. Two sub-cases, one per binding envelope, so a
// regression cannot hide by having the other envelope happen to bind.
func TestAcquireWindowIsTheTighterEnvelopeTightest(t *testing.T) {
	t.Run("candidate binds", func(t *testing.T) {
		clk := newTestClock()
		b := NewBudget(Envelope{MaxExchanges: 10, MaxElapsed: time.Hour}, clk.now)
		b.BeginCandidate(Envelope{MaxExchanges: 10, MaxElapsed: 10 * time.Second})
		clk.advance(3 * time.Second)
		if got := b.AcquireExchange().Window; got != 7*time.Second {
			t.Errorf("window = %v, want the candidate's 7s", got)
		}
	})

	t.Run("request binds", func(t *testing.T) {
		clk := newTestClock()
		b := NewBudget(Envelope{MaxExchanges: 10, MaxElapsed: 10 * time.Second}, clk.now)
		b.BeginCandidate(Envelope{MaxExchanges: 10, MaxElapsed: time.Hour})
		clk.advance(4 * time.Second)
		if got := b.AcquireExchange().Window; got != 6*time.Second {
			t.Errorf("window = %v, want the request's 6s", got)
		}
	})
}

// TestAcquireSatisfiesTheTransportSeam is a compile-time statement with teeth:
// the budget is the transport's ExchangeBudget, and the value it returns is the
// transport's own Exchange. If either half of the seam ever decouples, the
// transport's own accounting would be reading a shape its producer does not
// fill — a granted claim with an unreadable window, or worse, one whose window
// is not the one the envelope vouched for.
func TestAcquireSatisfiesTheTransportSeam(t *testing.T) {
	var b any = &Budget{}
	if _, ok := b.(interface {
		AcquireExchange() Exchange
	}); !ok {
		t.Fatal("*Budget does not satisfy the exchange-budget seam")
	}
}
