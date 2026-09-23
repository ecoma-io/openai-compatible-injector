package recovery

import "fmt"

// Layer is one override layer awaiting resolution onto a policy. Name is a
// structural label for error text — "global", "provider", "model",
// "candidate" — and is never operator input, so a rejection can say WHERE
// the contradiction is without quoting anything from the file.
type Layer struct {
	Name    string
	Partial Partial
}

// Resolve is the policy resolver: it folds the override layers, in order,
// onto a base policy and returns the effective one.
//
// The order is the documented hierarchy — global, then provider, then model,
// then candidate — and each layer is merged and VALIDATED before the next
// one is applied. Validating at every step is deliberate: a layer that
// leaves the policy incoherent is rejected even when a later layer would
// have repaired it, because a configuration whose middle state is
// meaningless is a configuration whose author was not describing what they
// thought.
//
// The result is a value: the caller gets a policy it can freeze on a request
// and never sees it change, which is what makes a hot reload invisible to
// work already in flight.
func Resolve(base Policy, layers ...Layer) (Policy, error) {
	out := base
	if err := out.Validate(); err != nil {
		return Policy{}, err
	}
	for _, l := range layers {
		merged, err := Merge(out, l.Partial)
		if err != nil {
			return Policy{}, fmt.Errorf("%s: %w", l.Name, err)
		}
		out = merged
	}
	return out, nil
}
