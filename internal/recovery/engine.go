package recovery

import (
	"context"
	"math/rand/v2"
	"time"
)

// Decision identities for the invariants the matrix does not own. They are
// stable, code-owned tokens that ride the evidence beside a rule identity, so
// an operator can tell "the 429 row said fall back" from "the walk stopped
// because the client hung up".
const (
	// RuleIDCommitted is a response that already reached the client.
	RuleIDCommitted = "committed"
	// RuleIDCaller is the client's own cancellation or expired deadline.
	RuleIDCaller = "caller"
	// RuleIDBudgetRequest is the request-wide exchange envelope.
	RuleIDBudgetRequest = "budget-request"
	// RuleIDBudgetCandidate is the current candidate's exchange envelope.
	RuleIDBudgetCandidate = "budget-candidate"
)

// Decision is what the engine concluded about one failed attempt. It is a
// value with no side effects attached: the handler executes it (waits, moves
// on, or stops), the logger records it, and the engine never does either.
type Decision struct {
	// Action is what to do next.
	Action Action
	// RuleID is the identity of what decided it — a matrix rule when the
	// matrix decided, or one of the reserved invariant identities above.
	RuleID string
	// Delay is the bounded wait before a retry. It is zero for every other
	// action, and it has already been capped by the policy's ceilings, the
	// remaining retry window, the exchange envelopes, and the caller's
	// deadline.
	Delay time.Duration
	// Reason is the closed-set failure token the evidence events carry.
	Reason string
}

// Engine is the decision maker: one policy, one budget, one request context,
// evaluated against observations the handler produces.
//
// It holds no I/O of any kind — it cannot dial, sleep, write a response, or
// log — and it does not know what a provider is. What it does own is the
// four invariants no configuration may weaken:
//
//   - commitment is final;
//   - the caller's cancellation is final;
//   - the exchange envelope is final;
//   - the decision for a given (policy, observation, budget state) is a pure
//     function, so the same request replays the same way.
type Engine struct {
	ctx     context.Context
	primary Policy
	policy  Policy
	budget  *Budget
	now     func() time.Time
	draw    func() float64
	entered int
	candAt  time.Time
}

// Option configures an engine's environmental inputs.
type Option func(*Engine)

// WithClock replaces the engine's clock. Production reads the wall clock;
// tests pin it so a scheduled wait is a number to assert on rather than a
// race to wait out.
func WithClock(now func() time.Time) Option {
	return func(e *Engine) {
		if now != nil {
			e.now = now
		}
	}
}

// WithJitterSource replaces the engine's jitter draw, which must return a
// uniform value in [-1, 1].
func WithJitterSource(draw func() float64) Option {
	return func(e *Engine) {
		if draw != nil {
			e.draw = draw
		}
	}
}

// NewEngine starts a walk under a policy. The policy passed here is the
// PRIMARY candidate's effective policy, and it supplies the request-scoped
// knobs: the exchange envelope and the fallback budget. Candidate-scoped
// knobs (the matrix, the retry mechanics, the retry-after policy, the
// candidate envelope) come from whichever candidate is entered.
//
// The context is the request's. It is consulted, never derived from: the
// engine reads it to decide that a dead caller is a terminal condition, and
// never wraps or cancels it.
func NewEngine(ctx context.Context, primary Policy, opts ...Option) *Engine {
	e := &Engine{
		ctx:     ctx,
		primary: primary,
		policy:  primary,
		now:     time.Now,
		draw:    func() float64 { return 2*rand.Float64() - 1 },
	}
	for _, opt := range opts {
		opt(e)
	}
	e.budget = NewBudget(primary.Budget.Request, e.now)
	e.candAt = e.now()
	return e
}

// Now reads the engine's clock. The handler uses it for the facts it puts on
// an observation, so that one request is measured by one clock.
func (e *Engine) Now() time.Time { return e.now() }

// Budget is the request's exchange envelope. The handler hands it to the
// transport (which consumes a unit per real dial) and reads it back for the
// evidence.
func (e *Engine) Budget() *Budget { return e.budget }

// CandidatesEntered is how many chain candidates this walk has entered. One
// is a walk that never fell back.
func (e *Engine) CandidatesEntered() int { return e.entered }

// EnterCandidate starts a candidate: its policy becomes the effective
// policy, its exchange envelope starts fresh, and its retry window opens at
// this instant.
//
// It reports false when the fallback budget cannot reach another candidate,
// in which case the caller MUST NOT attempt anything: the walk has entered
// as many candidates as its policy allows. The first candidate is always
// enterable — a request that could not enter even one would have nothing to
// execute.
func (e *Engine) EnterCandidate(p Policy) bool {
	if !e.canEnter() {
		return false
	}
	e.entered++
	e.policy = p
	e.candAt = e.now()
	e.budget.BeginCandidate(p.Budget.Candidate)
	return true
}

// canEnter is the fallback budget's gate. The reach comes from the PRIMARY
// policy: how far a request may walk is a property of the request, not of
// the candidate it happens to be standing on.
func (e *Engine) canEnter() bool {
	if e.entered == 0 {
		return true
	}
	if !e.primary.Fallback.Enabled {
		return false
	}
	return e.entered < e.primary.Fallback.MaxCandidates
}

// Observe decides what one finished attempt means.
//
// The order of the gates is the order of the invariants, and none of them is
// reachable by configuration:
//
//  1. a committed response ends everything;
//  2. a dead caller ends everything — judged from the request context and
//     from the failure's own class, never from the error text;
//  3. a spent request envelope ends everything, because no candidate can
//     start another exchange;
//  4. the matrix decides; a spent candidate envelope can stop only another
//     retry on THAT candidate, never a fallback to a fresh candidate.
func (e *Engine) Observe(o Observation) Decision {
	reason := Reason(o)
	if o.Committed {
		return Decision{Action: ActionTerminal, RuleID: RuleIDCommitted, Reason: reason}
	}
	if o.Class == FailureCaller || e.ctx.Err() != nil {
		return Decision{Action: ActionTerminal, RuleID: RuleIDCaller, Reason: reason}
	}
	if e.budget.Exhausted() == ExhaustionRequest {
		return Decision{Action: ActionTerminal, RuleID: RuleIDBudgetRequest, Reason: reason}
	}
	action, ruleID := e.policy.Matrix.Match(o)
	switch action {
	case ActionRetry:
		// Candidate envelopes reset on EnterCandidate. Spending this one's
		// envelope therefore forbids only another same-candidate exchange;
		// its retry exhaustion policy still decides whether the walk may move
		// to a fresh candidate while the request-wide envelope has room.
		if e.budget.Exhausted() == ExhaustionCandidate {
			return e.exhaustedRetry(ruleID, reason)
		}
		return e.retry(o, ruleID, reason)
	case ActionFallback:
		return e.move(ruleID, reason)
	default:
		return Decision{Action: ActionTerminal, RuleID: ruleID, Reason: reason}
	}
}

// retry applies the candidate's retry budget to a retryable failure: the
// attempt count first, then the window, then the bounded wait.
func (e *Engine) retry(o Observation, ruleID, reason string) Decision {
	attempts := o.CandidateAttempt
	if attempts < 1 {
		// Defensive: a caller that forgot to count must not be able to turn
		// a bounded retry budget into an unbounded one. The floor is one
		// attempt, which is the truth for any observation.
		attempts = 1
	}
	if attempts > e.policy.Retry.MaxRetries {
		return e.exhaustedRetry(ruleID, reason)
	}
	windowLeft := e.policy.Retry.MaxElapsed - e.now().Sub(e.candAt)
	if windowLeft <= 0 {
		return e.exhaustedRetry(ruleID, reason)
	}
	delay := e.retryDelay(o.RetryAfter, attempts, windowLeft)
	return Decision{Action: ActionRetry, RuleID: ruleID, Delay: delay, Reason: reason}
}

// exhaustedRetry is the tail of a retryable failure whose budget is spent:
// the policy's on-exhausted action, which is a move to the next candidate by
// default and may be a terminal relaying of the answer.
func (e *Engine) exhaustedRetry(ruleID, reason string) Decision {
	if e.policy.Retry.OnExhausted == ActionFallback {
		return e.move(ruleID, reason)
	}
	return Decision{Action: ActionTerminal, RuleID: ruleID, Reason: reason}
}

// move forwards when the fallback budget still reaches a candidate, and ends
// the walk when it does not.
func (e *Engine) move(ruleID, reason string) Decision {
	if e.canEnter() {
		return Decision{Action: ActionFallback, RuleID: ruleID, Reason: reason}
	}
	return Decision{Action: ActionTerminal, RuleID: ruleID, Reason: reason}
}

// retryDelay computes the bounded wait before the next attempt on this
// candidate. Every cap is applied in order, and each one only ever makes the
// wait shorter:
//
//	backoff schedule (jittered)
//	  → raised to the upstream's Retry-After, when the policy honors it
//	  → ceilinged at the backoff maximum
//	  → ceilinged at the retry-after policy's own maximum
//	  → shortened to the remaining retry window
//	  → shortened to the caller's remaining deadline
//
// The result is never negative and never longer than any of the four
// ceilings, so no upstream directive and no configuration can make this
// proxy sleep past a bound the request itself did not grant.
func (e *Engine) retryDelay(retryAfter time.Duration, attempts int, windowLeft time.Duration) time.Duration {
	p := e.policy
	delay := jitteredBackoff(p.Retry.Backoff, attempts, e.draw)
	if p.RetryAfter.Enabled && p.RetryAfter.Mode == RetryAfterMax && retryAfter > delay {
		delay = retryAfter
	}
	if delay > p.Retry.Backoff.Max {
		delay = p.Retry.Backoff.Max
	}
	if p.RetryAfter.Enabled && p.RetryAfter.MaxDelay > 0 && delay > p.RetryAfter.MaxDelay {
		delay = p.RetryAfter.MaxDelay
	}
	if delay > windowLeft {
		delay = windowLeft
	}
	if deadline, ok := e.ctx.Deadline(); ok {
		until := deadline.Sub(e.now())
		if until <= 0 {
			return 0
		}
		if delay > until {
			delay = until
		}
	}
	if delay < 0 {
		return 0
	}
	return delay
}
