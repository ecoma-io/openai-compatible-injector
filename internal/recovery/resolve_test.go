package recovery

import (
	"math"
	"testing"
	"time"
)

func action(v Action) *Action             { return &v }
func intp(v int) *int                     { return &v }
func boolp(v bool) *bool                  { return &v }
func durp(v time.Duration) *time.Duration { return &v }
func floatp(v float64) *float64           { return &v }

func TestResolveWithoutLayersIsTheDefault(t *testing.T) {
	got, err := Resolve(Default())
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if got.Retry != Default().Retry || got.Fallback != Default().Fallback ||
		got.Budget != Default().Budget || got.RetryAfter != Default().RetryAfter {
		t.Fatal("resolving no layers changed the policy")
	}
	if got.Matrix.Default() != ActionTerminal {
		t.Fatal("the default action is not terminal")
	}
}

// TestResolveLayerChain exercises the documented hierarchy end to end:
// global → provider → model → candidate, each layer overriding only what it
// states and inheriting the rest.
func TestResolveLayerChain(t *testing.T) {
	provider := Partial{
		Retry: &RetryPartial{MaxRetries: intp(5)},
		Matrix: &MatrixPartial{Rules: []Rule{
			{ID: StatusRuleID(429), Match: Match{Status: 429}, Action: ActionFallback},
			{ID: "quota-exhausted", Match: Match{Status: 429, ProviderErrorCode: "insufficient_quota"}, Action: ActionFallback},
		}},
	}
	model := Partial{
		Retry: &RetryPartial{MaxRetries: intp(3), Backoff: &BackoffPartial{Jitter: floatp(0)}},
		Matrix: &MatrixPartial{
			Default: action(ActionFallback),
		},
	}
	candidate := Partial{
		Budget: &BudgetPartial{Candidate: &EnvelopePartial{MaxExchanges: intp(8)}},
	}

	pol, err := Resolve(Default(),
		Layer{Name: "provider", Partial: provider},
		Layer{Name: "model", Partial: model},
		Layer{Name: "candidate", Partial: candidate},
	)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}

	// Explicit values replace.
	if pol.Retry.MaxRetries != 3 {
		t.Fatalf("model layer did not win the retry count: %d", pol.Retry.MaxRetries)
	}
	if pol.Budget.Candidate.MaxExchanges != 8 {
		t.Fatalf("candidate layer did not win the exchange envelope: %d", pol.Budget.Candidate.MaxExchanges)
	}
	if pol.Matrix.Default() != ActionFallback {
		t.Fatalf("model layer did not win the default action: %v", pol.Matrix.Default())
	}
	// Unstated fields inherit, at depth: the provider's backoff initial and
	// max survive a layer that only stated jitter, and so does the retry
	// window from the built-in defaults.
	if pol.Retry.Backoff.Jitter != 0 {
		t.Fatalf("jitter override lost: %v", pol.Retry.Backoff.Jitter)
	}
	if pol.Retry.Backoff.Initial != DefaultBackoffInitial || pol.Retry.Backoff.Max != DefaultBackoffMax {
		t.Fatalf("backoff siblings were reset by a partial override: %+v", pol.Retry.Backoff)
	}
	if pol.Retry.MaxElapsed != DefaultRetryMaxElapsed {
		t.Fatalf("retry window was reset by omission: %v", pol.Retry.MaxElapsed)
	}
	if pol.RetryAfter != Default().RetryAfter || pol.Fallback != Default().Fallback {
		t.Fatal("an untouched block changed")
	}
	// A rule restated by identity replaces the built-in row.
	if got, id := pol.Matrix.Match(Observation{Class: FailureHTTP, HTTPStatus: 429, StatusClass: StatusClass4xx}); got != ActionFallback || id != "http-429" {
		t.Fatalf("rule override did not replace the row: (%v, %q)", got, id)
	}
	// A rule added by a layer fires when it is the most specific match.
	quota := Observation{Class: FailureHTTP, HTTPStatus: 429, StatusClass: StatusClass4xx, ProviderErrorCode: "insufficient_quota"}
	if got, id := pol.Matrix.Match(quota); got != ActionFallback || id != "quota-exhausted" {
		t.Fatalf("added rule did not fire: (%v, %q)", got, id)
	}
	// The untouched default rows survive the chain.
	if got, id := pol.Matrix.Match(Observation{Class: FailureHTTP, HTTPStatus: 401, StatusClass: StatusClass4xx}); got != ActionFallback || id != "http-401" {
		t.Fatalf("an inherited rule was lost: (%v, %q)", got, id)
	}
}

// TestResolveDoesNotMutateTheBase proves the resolver returns a new value:
// the policy a request already holds can never be reshaped by a later
// reload that resolves from the same base.
func TestResolveDoesNotMutateTheBase(t *testing.T) {
	base := Default()
	before := base.Matrix.Rules()

	if _, err := Resolve(base, Layer{Name: "model", Partial: Partial{
		Retry:  &RetryPartial{MaxRetries: intp(7)},
		Matrix: &MatrixPartial{Rules: []Rule{{ID: "extra", Match: Match{Status: 418}, Action: ActionRetry}}},
	}}); err != nil {
		t.Fatalf("Resolve: %v", err)
	}

	if base.Retry.MaxRetries != DefaultMaxRetries {
		t.Fatalf("the base retry policy was mutated: %d", base.Retry.MaxRetries)
	}
	after := base.Matrix.Rules()
	if len(after) != len(before) {
		t.Fatalf("the base matrix gained rules: %d → %d", len(before), len(after))
	}
	for i := range before {
		if before[i].ID != after[i].ID || before[i].Action != after[i].Action {
			t.Fatalf("the base matrix changed at %d: %+v → %+v", i, before[i], after[i])
		}
	}
	if got, id := base.Matrix.Match(Observation{Class: FailureHTTP, HTTPStatus: 418, StatusClass: StatusClass4xx}); got != ActionTerminal || id != "http-class-4xx" {
		t.Fatalf("the base matrix answered with a merged rule: (%v, %q)", got, id)
	}
}

func TestResolveIsDeterministic(t *testing.T) {
	layer := Layer{Name: "provider", Partial: Partial{
		Retry: &RetryPartial{MaxRetries: intp(4)},
		Matrix: &MatrixPartial{
			Rules: []Rule{
				{ID: "z", Match: Match{Status: 502}, Action: ActionFallback},
				{ID: "a", Match: Match{Status: 503}, Action: ActionFallback},
				{ID: StatusRuleID(429), Match: Match{Status: 429}, Action: ActionTerminal},
			},
			Default: action(ActionRetry),
		},
	}}
	first, err := Resolve(Default(), layer)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	for i := 0; i < 20; i++ {
		again, err := Resolve(Default(), layer)
		if err != nil {
			t.Fatalf("Resolve: %v", err)
		}
		f, g := first.Matrix.Rules(), again.Matrix.Rules()
		if len(f) != len(g) {
			t.Fatalf("run %d produced a different rule count", i)
		}
		for j := range f {
			if f[j].ID != g[j].ID || f[j].Action != g[j].Action {
				t.Fatalf("run %d differs at %d: %+v vs %+v", i, j, f[j], g[j])
			}
		}
	}
}

func TestMergeRejectsAmbiguousRulesAcrossLayers(t *testing.T) {
	// A child layer that adds a rule of the same precedence as an inherited
	// one, with predicates that overlap, is ambiguous and must be rejected
	// rather than silently resolved one way.
	_, err := Merge(Default(), Partial{Matrix: &MatrixPartial{Rules: []Rule{
		{ID: "everything-5xx", Match: Match{StatusClass: StatusClass5xx}, Action: ActionTerminal},
	}}})
	if err == nil {
		t.Fatal("an ambiguous cross-layer rule pair was accepted")
	}
}

func TestMergeRejectsSameIdentityTwiceInOneLayer(t *testing.T) {
	_, err := Merge(Default(), Partial{Matrix: &MatrixPartial{Rules: []Rule{
		{ID: "dup", Match: Match{Status: 502}, Action: ActionRetry},
		{ID: "dup", Match: Match{Status: 503}, Action: ActionRetry},
	}}})
	if err == nil {
		t.Fatal("a layer naming the same rule identity twice was accepted")
	}
}

func TestResolveRejectsContradictoryBudgets(t *testing.T) {
	cases := []struct {
		name  string
		layer Layer
	}{
		{"candidate exchanges exceed the request envelope", Layer{Name: "model", Partial: Partial{
			Budget: &BudgetPartial{
				Request:   &EnvelopePartial{MaxExchanges: intp(4)},
				Candidate: &EnvelopePartial{MaxExchanges: intp(6)},
			},
		}}},
		{"candidate elapsed exceeds the request envelope", Layer{Name: "model", Partial: Partial{
			Budget: &BudgetPartial{
				Request:   &EnvelopePartial{MaxElapsed: durp(10 * time.Second)},
				Candidate: &EnvelopePartial{MaxElapsed: durp(20 * time.Second)},
			},
		}}},
		{"retry window outlives the candidate envelope", Layer{Name: "model", Partial: Partial{
			Retry:  &RetryPartial{MaxElapsed: durp(40 * time.Second)},
			Budget: &BudgetPartial{Candidate: &EnvelopePartial{MaxElapsed: durp(20 * time.Second)}},
		}}},
		{"retry budget the envelope cannot fund", Layer{Name: "provider", Partial: Partial{
			Retry:  &RetryPartial{MaxRetries: intp(5)},
			Budget: &BudgetPartial{Candidate: &EnvelopePartial{MaxExchanges: intp(3)}},
		}}},
		{"exchange envelope below one", Layer{Name: "model", Partial: Partial{
			Budget: &BudgetPartial{Candidate: &EnvelopePartial{MaxExchanges: intp(0)}},
		}}},
		{"fallback disabled while claiming reach", Layer{Name: "model", Partial: Partial{
			Fallback: &FallbackPartial{Enabled: boolp(false), MaxCandidates: intp(3)},
		}}},
		{"fallback on-exhausted that is not terminal", Layer{Name: "model", Partial: Partial{
			Fallback: &FallbackPartial{OnExhausted: action(ActionRetry)},
		}}},
		{"retry on-exhausted that re-arms itself", Layer{Name: "model", Partial: Partial{
			Retry: &RetryPartial{OnExhausted: action(ActionRetry)},
		}}},
		{"backoff ceiling under the initial delay", Layer{Name: "model", Partial: Partial{
			Retry: &RetryPartial{Backoff: &BackoffPartial{Initial: durp(2 * time.Second), Max: durp(time.Second)}},
		}}},
		{"over the absolute retry cap", Layer{Name: "provider", Partial: Partial{
			Retry: &RetryPartial{MaxRetries: intp(MaxRetriesCap + 1)},
		}}},
		{"over the absolute request exchange cap", Layer{Name: "global", Partial: Partial{
			Budget: &BudgetPartial{Request: &EnvelopePartial{MaxExchanges: intp(MaxRequestExchangesCap + 1)}},
		}}},
		{"over the absolute candidate exchange cap", Layer{Name: "model", Partial: Partial{
			Budget: &BudgetPartial{
				Request:   &EnvelopePartial{MaxExchanges: intp(MaxRequestExchangesCap)},
				Candidate: &EnvelopePartial{MaxExchanges: intp(MaxCandidateExchangesCap + 1)},
			},
		}}},
		{"jitter above one", Layer{Name: "model", Partial: Partial{
			Retry: &RetryPartial{Backoff: &BackoffPartial{Jitter: floatp(1.5)}},
		}}},
		// NaN is false against every comparison, so it has to be named
		// explicitly: the range test alone admits it, and it then collapses
		// every wait the policy schedules to zero.
		{"nan jitter", Layer{Name: "model", Partial: Partial{
			Retry: &RetryPartial{Backoff: &BackoffPartial{Jitter: floatp(math.NaN())}},
		}}},
		{"negative retry budget", Layer{Name: "model", Partial: Partial{
			Retry: &RetryPartial{MaxRetries: intp(-1)},
		}}},
	}
	for _, c := range cases {
		if _, err := Resolve(Default(), c.layer); err == nil {
			t.Errorf("%s: an over-cap or contradictory policy was accepted", c.name)
		}
	}
}

// TestMergeDisabledFallbackCollapsesToOneCandidate documents the one place a
// merge derives a value the layer did not state: switching the walk off IS a
// one-candidate walk, and inheriting a parent's larger reach would leave a
// contradiction the operator never wrote.
func TestMergeDisabledFallbackCollapsesToOneCandidate(t *testing.T) {
	out, err := Merge(Default(), Partial{Fallback: &FallbackPartial{Enabled: boolp(false)}})
	if err != nil {
		t.Fatalf("Merge: %v", err)
	}
	if out.Fallback.Enabled || out.Fallback.MaxCandidates != 1 {
		t.Fatalf("disabled fallback resolved to %+v", out.Fallback)
	}
}

// TestMergeNeverClampsToCaps is the fail-closed direction for safety limits:
// a value beyond a cap is a rejection, never a quiet reduction, because a
// policy silently reduced to something else is a policy the operator cannot
// read back from their own file.
func TestMergeNeverClampsToCaps(t *testing.T) {
	_, err := Merge(Default(), Partial{Retry: &RetryPartial{MaxRetries: intp(MaxRetriesCap + 1)}})
	if err == nil {
		t.Fatal("an over-cap retry budget was accepted")
	}
	pol, err := Merge(Default(), Partial{Retry: &RetryPartial{MaxRetries: intp(MaxRetriesCap)}})
	if err != nil {
		t.Fatalf("a cap-valued retry budget was rejected: %v", err)
	}
	if pol.Retry.MaxRetries != MaxRetriesCap {
		t.Fatalf("a value at the cap was changed: %d", pol.Retry.MaxRetries)
	}
}

// TestProviderOverrideByLogicalIdentity shows the shape a provider override
// takes: the domain never learns a provider's name — the config layer
// resolves the name to a layer and hands the layer here.
func TestProviderOverrideByLogicalIdentity(t *testing.T) {
	// global 401 → fallback (the default), provider A 401 → terminal.
	base := Default()
	providerA := Partial{Matrix: &MatrixPartial{Rules: []Rule{
		{ID: StatusRuleID(401), Match: Match{Status: 401}, Action: ActionTerminal},
	}}}
	modelWithA, err := Resolve(base, Layer{Name: "provider", Partial: providerA})
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if got, _ := modelWithA.Matrix.Match(Observation{Class: FailureHTTP, HTTPStatus: 401, StatusClass: StatusClass4xx}); got != ActionTerminal {
		t.Fatalf("provider override did not apply: %v", got)
	}
	// A model on another provider keeps the global row.
	modelWithB, err := Resolve(base, Layer{Name: "provider", Partial: Partial{}})
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if got, _ := modelWithB.Matrix.Match(Observation{Class: FailureHTTP, HTTPStatus: 401, StatusClass: StatusClass4xx}); got != ActionFallback {
		t.Fatalf("an unrelated provider inherited another's override: %v", got)
	}
}
