package recovery

import (
	"sync"
	"time"

	"openai-compatible-injector/internal/transport"
)

// Exhaustion names which envelope stopped the walk, so the evidence can say
// whether the request-wide ceiling or the candidate's own ran out. Both are
// hard stops; the distinction is diagnostic.
type Exhaustion int

const (
	// ExhaustionNone means both envelopes still have room.
	ExhaustionNone Exhaustion = iota
	// ExhaustionRequest means the request-wide envelope is spent.
	ExhaustionRequest
	// ExhaustionCandidate means the current candidate's envelope is spent.
	ExhaustionCandidate
)

// String renders the exhaustion as the config-facing and log-facing token.
func (e Exhaustion) String() string {
	switch e {
	case ExhaustionRequest:
		return "request"
	case ExhaustionCandidate:
		return "candidate"
	default:
		return "none"
	}
}

// Budget is one request's exchange envelope, live. It is shared by the two
// layers that must agree on it:
//
//   - the engine reads it (Exhausted) to stop deciding for a request that
//     has spent everything it may spend;
//   - the transport layer claims from it (AcquireExchange) immediately
//     before an outbound HTTP exchange actually starts.
//
// Claiming at the transport — not at the handler — is what makes the count
// honest. A candidate attempt that fans out into an egress pool's fallback
// is several real exchanges, and each of them claims its own unit; a member
// skipped before dialing (ineligible, unhealthy, saturated) claims none,
// because it never reached the wire. The envelope therefore bounds real
// traffic, not the intent to produce it.
//
// The budget is safe for concurrent use, though today's walk is
// single-goroutine: the transport may run a pool's attempts on the request's
// goroutine today, and a future concurrent dial path must not be able to
// double-spend the last unit.
type Budget struct {
	mu        sync.Mutex
	now       func() time.Time
	start     time.Time
	req       Envelope
	cand      Envelope
	candStart time.Time
	reqUsed   int
	candUsed  int
}

// NewBudget opens a request-wide envelope. The request envelope's clock
// starts now; a candidate's starts when the walk enters it.
func NewBudget(req Envelope, now func() time.Time) *Budget {
	return &Budget{now: now, start: now(), req: req}
}

// BeginCandidate starts a fresh candidate envelope: the candidate's counter
// and clock reset, the request's do not.
func (b *Budget) BeginCandidate(cand Envelope) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.cand = cand
	b.candStart = b.now()
	b.candUsed = 0
}

// Exchange is the outcome of ONE atomic acquisition from a request's exchange
// envelope: a claim that either funded an exchange, or refused to.
//
// The two facts are one value rather than a bool and a separate duration
// because they are decided under one lock, and a caller that has a bool and
// then re-asks for the time would be asking twice about a budget that may move
// between the two questions. Granted and Window are read together, so the
// window belongs to the exchange it was granted for.
//
// The shape is the transport's own (internal/transport.Exchange) and this is
// an ALIAS of it, not a second declaration of it. The consumer declares the
// seam — this package satisfies transport.ExchangeBudget structurally and
// imports nothing from it — and a producer-local struct would satisfy the
// method while returning a value the consumer cannot read, which is not an
// independent seam but a broken one. The dependency therefore runs
// recovery → transport for a type declaration and nothing else: this package
// still performs no I/O, dials nothing, and holds no reference to any
// transport behaviour. internal/credential/guards_test.go parses both packages
// and fails on an unparsable target rather than on mere membership, so the
// boundaries it already watches stay enforceable now that a third edge
// exists.
type Exchange = transport.Exchange

// AcquireExchange claims one unit for an exchange that is about to start AND
// reports the time that exchange is allowed to take. It is the
// transport.ExchangeBudget seam — the transport package declares the
// interface and the shape it returns, this type satisfies both, and the
// packages meet on a type and nothing else.
//
// The claim and the window are decided in ONE critical section, so the
// invariant this method exists to establish holds by construction:
//
//	every increment of RequestExchanges() corresponds to an exchange the
//	caller is authorized to start under the envelope semantics.
//
// Two acquisitions are the two halves of that authority, so they must not be
// two observations. Were the count and the window read separately, a caller
// could be handed "funded" by one reading and a window that a later reading
// no longer vouches for — a claim succeeded, the counter moved, and the
// exchange then ran on a deadline the envelope had already given up. Under a
// concurrent path (two goroutines racing for the last unit) that is not a
// theoretical window: it is the last unit, exactly.
//
// So: a granted claim has already checked BOTH envelopes' exchange counts and
// BOTH elapsed halves against a single reading of the clock, and has already
// incremented both counters, under one lock. A caller that receives
// Granted == true holds every authorisation the envelope could give it, and
// the Window that came with it was valid at the instant the authority was
// granted.
func (b *Budget) AcquireExchange() Exchange {
	b.mu.Lock()
	defer b.mu.Unlock()
	// One reading of the clock serves both halves of the decision: the
	// funding check and the window it grants. Two samples could straddle a
	// boundary and hand back a claim the envelope no longer vouches for.
	now := b.now()
	if b.stateLocked(now) != ExhaustionNone {
		// A refusal spends nothing: the counters are untouched, so
		// RequestExchanges() keeps naming exactly the exchanges that were
		// authorised, and a request that was refused its last unit does not
		// look like one that made it.
		return Exchange{}
	}
	b.reqUsed++
	b.candUsed++
	// The smaller of the two envelopes, not the candidate's alone: an
	// exchange is funded by both, so an exchange outliving either one
	// outlives a bound that was written to hold it. In the shipped defaults the
	// candidate window is the tighter of the two, so this reduces to the
	// candidate's ceiling whenever an operator has not set a request window
	// shorter than the candidate's.
	return Exchange{Granted: true, Window: b.remainingElapsedLocked(now)}
}

// Exhausted reports which envelope, if any, is spent.
func (b *Budget) Exhausted() Exhaustion {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.stateLocked(b.now())
}

// stateLocked evaluates both envelopes against the clock reading the caller
// already took. The elapsed halves are checked at the moment an exchange would
// start, so a candidate that has been retried up to its wall-clock ceiling
// stops even with exchanges to spare.
func (b *Budget) stateLocked(now time.Time) Exhaustion {
	if b.reqUsed >= b.req.MaxExchanges || b.spentLocked(now, b.start, b.req.MaxElapsed) {
		return ExhaustionRequest
	}
	if b.candUsed >= b.cand.MaxExchanges || b.spentLocked(now, b.candStart, b.cand.MaxElapsed) {
		return ExhaustionCandidate
	}
	return ExhaustionNone
}

// spentLocked reports whether an envelope's elapsed half is used up. It is
// a >= comparison: at exactly the ceiling the envelope is spent, which is
// what makes the boundary testable rather than a matter of scheduling luck.
func (b *Budget) spentLocked(now, start time.Time, limit time.Duration) bool {
	return now.Sub(start) >= limit
}

// Remaining is how many further exchanges the tighter of the two envelopes
// still allows — the number the walk reports as its remaining headroom.
// Zero or negative means spent.
func (b *Budget) Remaining() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	n := b.req.MaxExchanges - b.reqUsed
	if c := b.cand.MaxExchanges - b.candUsed; c < n {
		n = c
	}
	if n < 0 {
		return 0
	}
	return n
}

// RequestRemaining is how many further exchanges the REQUEST-wide envelope
// allows, ignoring the candidate's own. It is what the completion record
// reports: after a walk ends, the candidate envelope is usually spent (that
// is often why the walk ended), so the tighter-of-the-two Remaining would
// answer "none left" for every ordinary request and tell an operator nothing
// about the headroom the request itself was given. This one answers the
// question the evidence asks — how much of the request-wide ceiling the
// request did not use.
func (b *Budget) RequestRemaining() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	n := b.req.MaxExchanges - b.reqUsed
	if n < 0 {
		return 0
	}
	return n
}

// RequestExchanges is how many exchanges this request has actually spent.
// It is the ground truth behind the `upstream_exchanges` evidence field:
// counted where the dials happen, so no handler bookkeeping can drift from
// it.
func (b *Budget) RequestExchanges() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.reqUsed
}

// CandidateExchanges is how many exchanges the current candidate has spent.
func (b *Budget) CandidateExchanges() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.candUsed
}

// RemainingElapsed is how much wall-clock time the exchange envelopes still
// allow before an exchange that starts now must have finished: the smaller
// of the two envelopes' remaining elapsed halves, each measured on this
// budget's own clock (the request's from NewBudget, the candidate's from
// BeginCandidate). Both halves are read against ONE reading of the clock, so
// the answer is a single self-consistent instant rather than a difference of
// two.
//
// It is what makes `max-elapsed` a bound on an exchange itself rather than
// only a gap between exchanges. The elapsed halves are otherwise consulted at
// exactly two moments — before a wait is scheduled, and before a dial is
// claimed — and both are before the exchange that then blocks for as long as
// the peer makes it. A provider that completes the handshake and sends no
// status line parks the walk in Do until the client gives up, which no
// operator-set number prevents. A timeout the transport applies to the
// exchange itself is the only place that wait can be cut.
//
// It returns a remaining DURATION measured on this budget's clock, not an
// absolute instant: the transport anchors it on the machine's own wall clock
// with context.WithTimeout. In production the two clocks are the same reading.
// In tests the policy clock is a fake the test advances by hand, and a fake
// clock that has barely ticked must report a full remaining window rather
// than an absolute period anchored to the fake's epoch — otherwise every dial
// under a slow test clock would fire instantly. A spent window (the returned
// duration is zero or negative) is reported as-is: the caller is about to
// spend real outbound traffic, and a spent envelope is the acquire's answer to
// refuse, not this method's. Clamping here would hand the transport a fresh
// window and turn a spent envelope into one more dial.
//
// This method is ADVISORY and reads the budget without claiming anything: no
// caller dials from it, because a dial's authority and its window are decided
// together in AcquireExchange and must not be two observations. What it is for
// is asking a question that is not about to spend — "how much is left?" — which
// is a question the acquisition cannot answer, since a spent envelope's answer
// there is to refuse rather than to report. It differs from AcquireExchange in
// exactly that: it reports a spent window as non-positive rather than refusing,
// and it moves no counter on either answer.
//
// It has no production caller. The walk's waits are sized by the resolved
// policy's own Retry.MaxElapsed (engine.go), not by what is left of the
// envelope, and every dial goes through AcquireExchange. It is retained as the
// package's read-only view of the budget — a question this package is asked
// about its own state, answered from one clock reading — rather than deleted
// with the finding that made it look load-bearing.
func (b *Budget) RemainingElapsed() time.Duration {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.remainingElapsedLocked(b.now())
}

// remainingElapsedLocked is RemainingElapsed against a clock reading the
// caller already took. One sample for both envelopes is the point: reading the
// clock twice and subtracting would let the two halves describe different
// instants, and the smaller of two different instants is neither.
func (b *Budget) remainingElapsedLocked(now time.Time) time.Duration {
	rem := b.req.MaxElapsed - now.Sub(b.start)
	if cand := b.cand.MaxElapsed - now.Sub(b.candStart); cand < rem {
		rem = cand
	}
	return rem
}
