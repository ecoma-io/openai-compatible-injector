package recovery

import (
	"errors"
	"fmt"
	"math"
	"time"
)

// RetryPolicy sizes the same-candidate retries the matrix asks for. It never
// decides WHETHER a failure is retryable — that is the matrix's job — only
// how often, how long, and how long to wait.
type RetryPolicy struct {
	// MaxRetries is how many times one candidate may be re-asked after its
	// initial attempt. 0 means every retry-able failure moves straight to
	// the next candidate.
	MaxRetries int
	// MaxElapsed closes a candidate's retry window, measured from its first
	// attempt. The window is checked BEFORE a wait is scheduled, so a sleep
	// never runs past it.
	MaxElapsed time.Duration
	// Backoff is the wait schedule between attempts on one candidate.
	Backoff BackoffPolicy
	// OnExhausted is what happens when a retryable failure arrives with the
	// retry budget spent: fallback (the default) moves to the next
	// candidate, terminal relays the answer. It may not be retry — a
	// retry budget that re-arms itself is not a budget.
	OnExhausted Action
}

// BackoffPolicy is the delay schedule between two attempts on one candidate.
type BackoffPolicy struct {
	// Initial is the delay before the first retry; each further retry
	// doubles it up to Max.
	Initial time.Duration
	// Max is the ceiling the doubling saturates at — and the ceiling an
	// upstream Retry-After can never push past.
	Max time.Duration
	// Jitter spreads each delay by ±Jitter of its value so a recovering
	// provider is not re-hit by a synchronized herd of retries.
	Jitter float64
}

// FallbackPolicy bounds the walk across provider candidates.
type FallbackPolicy struct {
	// Enabled turns the walk past the primary candidate on at all. Disabled,
	// the request is answered by the primary candidate alone (its own
	// same-candidate retries still apply).
	Enabled bool
	// MaxCandidates is how many candidates may be ENTERED during one
	// request, the primary included — not how many the chain happens to
	// list. A chain of eight behind a budget of two walks two candidates.
	MaxCandidates int
	// OnExhausted is what happens when the walk has entered its last
	// reachable candidate and that candidate fails. It is always terminal:
	// there is nowhere further to go.
	OnExhausted Action
}

// Envelope is an absolute ceiling on the real upstream traffic one scope may
// spend. Both members are hard: reaching either one stops the walk, and no
// retry, fallback, or egress fallback may spend past it.
type Envelope struct {
	// MaxExchanges is the number of outbound HTTP exchanges — every real
	// dial, on every path — the scope may start.
	MaxExchanges int
	// MaxElapsed is how long the scope may run before it stops starting new
	// exchanges.
	MaxElapsed time.Duration
}

// BudgetPolicy is the exchange envelope, in two nested scopes: the whole
// request, and each candidate within it.
//
// The nesting is what makes amplification impossible to configure by
// accident. A candidate's retries multiply with the walk's fallbacks, and
// each candidate attempt may itself fan out into an egress pool's fallback
// attempts — three independent knobs whose product is what actually hits the
// internet. The request envelope is the ceiling on that product, and it is
// enforced where the exchanges really happen rather than where they are
// counted.
type BudgetPolicy struct {
	Request   Envelope
	Candidate Envelope
}

// RetryAfterMode selects how an upstream Retry-After directive is combined
// with the backoff schedule.
type RetryAfterMode int

const (
	// RetryAfterMax treats the directive as a floor: the wait is the larger
	// of the backoff and the directive. This is the default, and the
	// behavior this proxy has always had.
	RetryAfterMax RetryAfterMode = iota
	// RetryAfterIgnore discards the directive entirely and sleeps the
	// backoff schedule.
	RetryAfterIgnore
)

// String renders the mode as the config-facing token.
func (m RetryAfterMode) String() string {
	if m == RetryAfterIgnore {
		return "ignore"
	}
	return "max"
}

// ParseRetryAfterMode reads a mode token.
func ParseRetryAfterMode(s string) (RetryAfterMode, error) {
	switch s {
	case "max":
		return RetryAfterMax, nil
	case "ignore":
		return RetryAfterIgnore, nil
	default:
		return RetryAfterMax, errors.New("retry-after mode must be one of ignore, max")
	}
}

// RetryAfterPolicy governs how an upstream's Retry-After header is honored.
// Honoring it is always bounded: the directive can only ever raise a wait up
// to a ceiling that configuration, the backoff schedule, the remaining
// windows, and the caller's deadline all get a veto over. A hostile upstream
// cannot buy itself an arbitrarily long gateway sleep.
type RetryAfterPolicy struct {
	// Enabled honors the directive at all.
	Enabled bool
	// Mode selects floor (max) or discard (ignore).
	Mode RetryAfterMode
	// MaxDelay is this policy's own ceiling on the directive.
	MaxDelay time.Duration
}

// Policy is one complete, resolved recovery policy: everything the engine
// needs to decide, and nothing that depends on the request's live state.
//
// It is a value. Copying it copies the whole policy, the matrix included, so
// a request that holds one is unaffected by any later reload — and the
// resolver produces a NEW Policy for a new config generation rather than
// mutating one in place.
type Policy struct {
	Matrix     Matrix
	Retry      RetryPolicy
	Fallback   FallbackPolicy
	Budget     BudgetPolicy
	RetryAfter RetryAfterPolicy
}

// Absolute runtime safety caps. Configuration is validated against these and
// REJECTED when it exceeds them — never clamped, because a value silently
// reduced to something else (or grown to it) is a policy the operator did
// not write and cannot read back from the file.
const (
	// MaxRetriesCap bounds Retry.MaxRetries.
	MaxRetriesCap = 8
	// MaxCandidatesCap bounds Fallback.MaxCandidates.
	MaxCandidatesCap = 8
	// MaxRequestExchangesCap bounds Budget.Request.MaxExchanges. The default
	// envelope is well under half of it, so the cap only ever bites a policy
	// that deliberately asks for more amplification than the proxy will do.
	MaxRequestExchangesCap = 64
	// MaxCandidateExchangesCap bounds Budget.Candidate.MaxExchanges.
	MaxCandidateExchangesCap = 32
	// MaxRequestElapsedCap bounds Budget.Request.MaxElapsed.
	MaxRequestElapsedCap = 5 * time.Minute
	// MaxCandidateElapsedCap bounds both Budget.Candidate.MaxElapsed and
	// Retry.MaxElapsed.
	MaxCandidateElapsedCap = 2 * time.Minute
	// MaxBackoffCap bounds BackoffPolicy.Max.
	MaxBackoffCap = 2 * time.Minute
	// MinBackoff is the smallest meaningful initial backoff; below it the
	// schedule is indistinguishable from no wait at all.
	MinBackoff = time.Millisecond
)

// Validate rejects an incoherent or over-cap policy. Every rejection is
// fixed text — a policy error must be able to name a position in the file
// without quoting a value, and the config layer prefixes it with the layer
// that produced it.
func (p Policy) Validate() error {
	if p.Retry.MaxRetries < 0 || p.Retry.MaxRetries > MaxRetriesCap {
		return fmt.Errorf("recovery: retries.max-retries must be between 0 and %d", MaxRetriesCap)
	}
	if p.Retry.MaxElapsed < MinBackoff || p.Retry.MaxElapsed > MaxCandidateElapsedCap {
		return errors.New("recovery: retries.max-elapsed is outside the allowed range")
	}
	if p.Retry.Backoff.Initial < MinBackoff {
		return errors.New("recovery: retries.backoff.initial is below the allowed minimum")
	}
	if p.Retry.Backoff.Max < p.Retry.Backoff.Initial {
		return errors.New("recovery: retries.backoff.max must not be smaller than retries.backoff.initial")
	}
	if p.Retry.Backoff.Max > MaxBackoffCap {
		return errors.New("recovery: retries.backoff.max exceeds the allowed maximum")
	}
	// NaN must be named: every comparison against it is false, so a bare
	// range check lets it through and the schedule silently degenerates —
	// `base + base*NaN` converts to the minimum int64 duration and clamps to
	// zero, turning every configured wait into an immediate re-ask.
	if math.IsNaN(p.Retry.Backoff.Jitter) || p.Retry.Backoff.Jitter < 0 || p.Retry.Backoff.Jitter > 1 {
		return errors.New("recovery: retries.backoff.jitter must be between 0 and 1")
	}
	if p.Retry.OnExhausted != ActionFallback && p.Retry.OnExhausted != ActionTerminal {
		return errActionNotAllowed("recovery: retries.on-exhausted", "fallback or terminal")
	}
	if !p.Fallback.Enabled {
		// A disabled walk is a one-candidate walk; carrying any other value
		// would mean the file states a reach the policy does not have.
		if p.Fallback.MaxCandidates != 1 {
			return errActionNotAllowed("recovery: fallback.max-candidates", "1 while fallback is disabled")
		}
	} else if p.Fallback.MaxCandidates < 1 || p.Fallback.MaxCandidates > MaxCandidatesCap {
		return fmt.Errorf("recovery: fallback.max-candidates must be between 1 and %d", MaxCandidatesCap)
	}
	if p.Fallback.OnExhausted != ActionTerminal {
		return errActionNotAllowed("recovery: fallback.on-exhausted", "terminal")
	}
	if p.Budget.Request.MaxExchanges < 1 || p.Budget.Request.MaxExchanges > MaxRequestExchangesCap {
		return fmt.Errorf("recovery: budget.request.max-exchanges must be between 1 and %d", MaxRequestExchangesCap)
	}
	if p.Budget.Request.MaxElapsed < MinBackoff || p.Budget.Request.MaxElapsed > MaxRequestElapsedCap {
		return errors.New("recovery: budget.request.max-elapsed is outside the allowed range")
	}
	if p.Budget.Candidate.MaxExchanges < 1 || p.Budget.Candidate.MaxExchanges > MaxCandidateExchangesCap {
		return fmt.Errorf("recovery: budget.candidate.max-exchanges must be between 1 and %d", MaxCandidateExchangesCap)
	}
	if p.Budget.Candidate.MaxElapsed < MinBackoff || p.Budget.Candidate.MaxElapsed > MaxCandidateElapsedCap {
		return errors.New("recovery: budget.candidate.max-elapsed is outside the allowed range")
	}
	if p.Budget.Candidate.MaxExchanges > p.Budget.Request.MaxExchanges {
		return errors.New("recovery: budget.candidate.max-exchanges must not exceed budget.request.max-exchanges")
	}
	if p.Budget.Candidate.MaxElapsed > p.Budget.Request.MaxElapsed {
		return errors.New("recovery: budget.candidate.max-elapsed must not exceed budget.request.max-elapsed")
	}
	if p.Retry.MaxElapsed > p.Budget.Candidate.MaxElapsed {
		return errors.New("recovery: retries.max-elapsed must not exceed budget.candidate.max-elapsed")
	}
	// A retry budget the exchange envelope cannot fund is a contradiction:
	// the policy asks for attempts that can never happen. Rejecting is the
	// honest answer; clamping would silently deliver a different policy.
	if p.Retry.MaxRetries+1 > p.Budget.Candidate.MaxExchanges {
		return errors.New("recovery: retries.max-retries cannot exceed budget.candidate.max-exchanges minus one")
	}
	if p.RetryAfter.Enabled && p.RetryAfter.MaxDelay < 0 {
		return errors.New("recovery: retry-after.max-delay must not be negative")
	}
	if p.RetryAfter.MaxDelay > MaxCandidateElapsedCap {
		return errors.New("recovery: retry-after.max-delay exceeds the allowed maximum")
	}
	return nil
}
