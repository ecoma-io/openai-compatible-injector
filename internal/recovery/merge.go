package recovery

import (
	"fmt"
	"time"
)

// Partial is one layer's contribution to a policy — a global block, a
// provider override, a model override, or a candidate override. Every field
// is optional and every scalar is a pointer, so "not stated" is
// distinguishable from "stated as the zero value". That distinction is the
// whole point: a layer that says nothing must inherit, and a layer that says
// `jitter: 0` must mean zero.
type Partial struct {
	// Matrix carries rule overrides and the default action.
	Matrix *MatrixPartial
	// Retry, Fallback, Budget and RetryAfter carry the mechanics.
	Retry      *RetryPartial
	Fallback   *FallbackPartial
	Budget     *BudgetPartial
	RetryAfter *RetryAfterPartial
}

// MatrixPartial restates a matrix slice: rules by identity, and the action
// unmatched failures take.
type MatrixPartial struct {
	// Rules replace a rule of the same identity in place, or append a new
	// one. Two rules with the same identity inside ONE layer are rejected —
	// within a layer there is no base to replace, so it can only be a
	// mistake.
	Rules []Rule
	// Default replaces the catch-all action.
	Default *Action
}

// RetryPartial overrides the retry mechanics.
type RetryPartial struct {
	MaxRetries  *int
	MaxElapsed  *time.Duration
	Backoff     *BackoffPartial
	OnExhausted *Action
}

// BackoffPartial overrides the wait schedule.
type BackoffPartial struct {
	Initial *time.Duration
	Max     *time.Duration
	Jitter  *float64
}

// FallbackPartial overrides the walk bound.
type FallbackPartial struct {
	Enabled       *bool
	MaxCandidates *int
	OnExhausted   *Action
}

// BudgetPartial overrides the exchange envelope. A candidate-scoped layer
// may only carry Candidate — the request envelope is a property of the
// request, and the config schema rejects a nested request envelope rather
// than letting two candidates disagree about it.
type BudgetPartial struct {
	Request   *EnvelopePartial
	Candidate *EnvelopePartial
}

// EnvelopePartial overrides one exchange envelope.
type EnvelopePartial struct {
	MaxExchanges *int
	MaxElapsed   *time.Duration
}

// RetryAfterPartial overrides the Retry-After policy.
type RetryAfterPartial struct {
	Enabled  *bool
	Mode     *RetryAfterMode
	MaxDelay *time.Duration
}

// Merge applies one layer over a base policy and returns the result. It is a
// deep semantic merge, not an object replacement:
//
//   - a scalar the layer states replaces the base's; a scalar it omits is
//     inherited, at every depth (a layer that states only jitter keeps the
//     base's initial and max);
//   - a rule restated by identity replaces the base rule of that identity,
//     wherever precedence had put it; a rule with a new identity is added;
//   - the default action is replaced when stated;
//   - nothing is ever removed, and nothing is ever reset to a zero value by
//     omission.
//
// The result is validated. A merge that produces an incoherent policy — a
// retry budget the exchange envelope cannot fund, a caller rule that is not
// terminal, two rules of equal precedence that can both fire — is rejected
// rather than normalized, so the operator sees the contradiction instead of
// a policy they did not write.
func Merge(base Policy, p Partial) (Policy, error) {
	out := base
	if p.Matrix != nil {
		m, err := mergeMatrix(base.Matrix, *p.Matrix)
		if err != nil {
			return Policy{}, err
		}
		out.Matrix = m
	}
	if p.Retry != nil {
		if p.Retry.MaxRetries != nil {
			out.Retry.MaxRetries = *p.Retry.MaxRetries
		}
		if p.Retry.MaxElapsed != nil {
			out.Retry.MaxElapsed = *p.Retry.MaxElapsed
		}
		if p.Retry.Backoff != nil {
			if p.Retry.Backoff.Initial != nil {
				out.Retry.Backoff.Initial = *p.Retry.Backoff.Initial
			}
			if p.Retry.Backoff.Max != nil {
				out.Retry.Backoff.Max = *p.Retry.Backoff.Max
			}
			if p.Retry.Backoff.Jitter != nil {
				out.Retry.Backoff.Jitter = *p.Retry.Backoff.Jitter
			}
		}
		if p.Retry.OnExhausted != nil {
			out.Retry.OnExhausted = *p.Retry.OnExhausted
		}
	}
	if p.Fallback != nil {
		if p.Fallback.Enabled != nil {
			out.Fallback.Enabled = *p.Fallback.Enabled
			if !*p.Fallback.Enabled && p.Fallback.MaxCandidates == nil {
				// Switching the walk off IS a one-candidate walk. Inheriting a
				// parent's larger reach here would leave the policy stating a
				// bound it cannot use, which validation would then reject as a
				// contradiction the operator never wrote.
				out.Fallback.MaxCandidates = 1
			}
		}
		if p.Fallback.MaxCandidates != nil {
			out.Fallback.MaxCandidates = *p.Fallback.MaxCandidates
		}
		if p.Fallback.OnExhausted != nil {
			out.Fallback.OnExhausted = *p.Fallback.OnExhausted
		}
	}
	if p.Budget != nil {
		if p.Budget.Request != nil {
			if p.Budget.Request.MaxExchanges != nil {
				out.Budget.Request.MaxExchanges = *p.Budget.Request.MaxExchanges
			}
			if p.Budget.Request.MaxElapsed != nil {
				out.Budget.Request.MaxElapsed = *p.Budget.Request.MaxElapsed
			}
		}
		if p.Budget.Candidate != nil {
			if p.Budget.Candidate.MaxExchanges != nil {
				out.Budget.Candidate.MaxExchanges = *p.Budget.Candidate.MaxExchanges
			}
			if p.Budget.Candidate.MaxElapsed != nil {
				out.Budget.Candidate.MaxElapsed = *p.Budget.Candidate.MaxElapsed
			}
		}
	}
	if p.RetryAfter != nil {
		if p.RetryAfter.Enabled != nil {
			out.RetryAfter.Enabled = *p.RetryAfter.Enabled
		}
		if p.RetryAfter.Mode != nil {
			out.RetryAfter.Mode = *p.RetryAfter.Mode
		}
		if p.RetryAfter.MaxDelay != nil {
			out.RetryAfter.MaxDelay = *p.RetryAfter.MaxDelay
		}
	}
	if err := out.Validate(); err != nil {
		return Policy{}, err
	}
	return out, nil
}

// mergeMatrix folds a layer's rule list into a base matrix: restated
// identities replace, new identities append, and the result is re-ordered
// and re-validated by NewMatrix.
func mergeMatrix(base Matrix, p MatrixPartial) (Matrix, error) {
	rules := base.Rules()
	at := make(map[string]int, len(rules))
	for i, r := range rules {
		at[r.ID] = i
	}
	seen := make(map[string]int, len(p.Rules))
	for i, r := range p.Rules {
		if prev, dup := seen[r.ID]; dup {
			return Matrix{}, fmt.Errorf("rule %d repeats the identity of rule %d in the same layer", i+1, prev+1)
		}
		seen[r.ID] = i
	}
	for _, r := range p.Rules {
		if i, ok := at[r.ID]; ok {
			rules[i] = r
			continue
		}
		rules = append(rules, r)
	}
	def := base.Default()
	if p.Default != nil {
		def = *p.Default
	}
	return NewMatrix(rules, def)
}

// NewMatrix would fold a same-identity pair inside one layer silently (one
// rule would replace the other), and a layer that names the same row twice
// is a mistake the operator needs to see — reported by position, never by
// identity.
