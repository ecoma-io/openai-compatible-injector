package recovery

import (
	"sync"
	"time"
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
//   - the transport layer consumes it (ConsumeExchange) immediately before
//     an outbound HTTP exchange actually starts.
//
// Consuming at the transport — not at the handler — is what makes the count
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

// ConsumeExchange claims one unit for an exchange that is about to start.
// It reports false when either envelope is spent, in which case the caller
// MUST NOT dial: an exhausted envelope stops real traffic, it does not
// merely record that it would have stopped.
//
// It is the transport.ExchangeBudget seam — the transport package declares
// the interface, this type satisfies it, and neither package imports the
// other for it.
func (b *Budget) ConsumeExchange() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.stateLocked() != ExhaustionNone {
		return false
	}
	b.reqUsed++
	b.candUsed++
	return true
}

// Exhausted reports which envelope, if any, is spent.
func (b *Budget) Exhausted() Exhaustion {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.stateLocked()
}

// stateLocked evaluates both envelopes. The elapsed halves are checked at
// the moment an exchange would start, so a candidate that has been retried
// up to its wall-clock ceiling stops even with exchanges to spare.
func (b *Budget) stateLocked() Exhaustion {
	if b.reqUsed >= b.req.MaxExchanges || b.spentLocked(b.start, b.req.MaxElapsed) {
		return ExhaustionRequest
	}
	if b.candUsed >= b.cand.MaxExchanges || b.spentLocked(b.candStart, b.cand.MaxElapsed) {
		return ExhaustionCandidate
	}
	return ExhaustionNone
}

// spentLocked reports whether an envelope's elapsed half is used up. It is
// a >= comparison: at exactly the ceiling the envelope is spent, which is
// what makes the boundary testable rather than a matter of scheduling luck.
func (b *Budget) spentLocked(start time.Time, limit time.Duration) bool {
	return b.now().Sub(start) >= limit
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
