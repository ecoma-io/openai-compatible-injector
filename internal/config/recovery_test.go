package config

import (
	"context"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"openai-compatible-injector/internal/recovery"
)

// recoveryYAML builds a runtime file around a fixed providers/transports
// table: `extra` is appended to the pa entry (a provider-level override),
// `global` is a top-level block, and `models` is the whole models table. The
// shape is deliberately small so each test's declaration is visible in its
// own body.
func recoveryYAML(global, extra, models string) string {
	return "api-key: unit-test-key\n" +
		"transports:\n  t1:\n    type: direct\n  t2:\n    type: proxy\n    proxy: http://127.0.0.1:9090\n" +
		"providers:\n" +
		"  pa:\n    base-url: https://a.example/v1\n    transport: t1\n" + extra +
		"  pb:\n    base-url: https://b.example/v1\n    transport: t2\n" +
		global +
		"models:\n" + models
}

// recoveryModel resolves one model out of a runtime body, failing the test if
// the file is rejected or the model is missing.
func recoveryModel(t *testing.T, data, name string) Model {
	t.Helper()
	snap, err := LoadRuntime([]byte(data))
	if err != nil {
		t.Fatalf("LoadRuntime: %v", err)
	}
	m, ok := snap.Model(name)
	if !ok {
		t.Fatalf("model not found")
	}
	return m
}

// recoveryRule finds one rule by identity in an effective policy.
func recoveryRule(t *testing.T, p recovery.Policy, id string) recovery.Rule {
	t.Helper()
	for _, r := range p.Matrix.Rules() {
		if r.ID == id {
			return r
		}
	}
	t.Fatalf("rule %q not found in the effective matrix", id)
	return recovery.Rule{}
}

// TestRecoveryShorthandBuildsCanonicalRules pins shorthand expansion: every
// shorthand block produces rules under the canonical identities the domain's
// own helpers generate, with the configured action, and `default` replaces
// the catch-all.
func TestRecoveryShorthandBuildsCanonicalRules(t *testing.T) {
	data := recoveryYAML(
		"recovery:\n"+
			"  matrix:\n"+
			"    http:\n"+
			"      exact:\n"+
			"        \"408\": terminal\n"+
			"        \"429\": retry\n"+
			"        \"500\": terminal\n"+
			"      classes:\n"+
			"        \"3xx\": terminal\n"+
			"        \"5xx\": retry\n"+
			"    transport:\n"+
			"      connection: terminal\n"+
			"      tls: retry\n"+
			"    protocol:\n"+
			"      invalid_response: terminal\n"+
			"    caller:\n"+
			"      canceled: terminal\n"+
			"      deadline: terminal\n"+
			"    default: terminal\n",
		"",
		"  m:\n    provider: pa\n    upstream-model: up-a\n")

	m := recoveryModel(t, data, "m")
	p := m.Recovery

	for _, tc := range []struct {
		id     string
		action recovery.Action
	}{
		{"http-408", recovery.ActionTerminal},
		{"http-429", recovery.ActionRetry},
		{"http-500", recovery.ActionTerminal},
		{"http-class-3xx", recovery.ActionTerminal},
		{"http-class-5xx", recovery.ActionRetry},
		{"transport-class-connection", recovery.ActionTerminal},
		{"transport-cause-tls", recovery.ActionRetry},
		{"protocol-invalid_response", recovery.ActionTerminal},
		{"caller-canceled", recovery.ActionTerminal},
		{"caller-deadline", recovery.ActionTerminal},
	} {
		if got := recoveryRule(t, p, tc.id).Action; got != tc.action {
			t.Errorf("rule %s action = %s, want %s", tc.id, got, tc.action)
		}
	}
	if got := p.Matrix.Default(); got != recovery.ActionTerminal {
		t.Errorf("default action = %s, want terminal", got)
	}
	// The shorthand rows keep the predicate shapes the built-in matrix uses,
	// so replacing one by identity replaces it exactly.
	if r := recoveryRule(t, p, "http-429"); r.Match.Status != 429 || r.Match.Class != recovery.FailureAny {
		t.Errorf("http-429 match = %+v, want an exact status predicate", r.Match)
	}
	if r := recoveryRule(t, p, "transport-class-connection"); r.Match.Class != recovery.FailureTransport || r.Match.TransportClass != recovery.TransportClassConnection {
		t.Errorf("transport-class-connection match = %+v", r.Match)
	}
	if r := recoveryRule(t, p, "caller-canceled"); r.Match.CallerCause != recovery.CallerCanceled {
		t.Errorf("caller-canceled match = %+v, want the canonical caller cause", r.Match)
	}
}

// TestRecoveryCredentialShorthandAndRejections covers the credential layer of
// the matrix schema: the shorthand block expands to the canonical identity,
// the free-form `when.credential-cause` predicate builds, and unknown tokens
// or a mixed cause set are rejected by position, never echoed.
func TestRecoveryCredentialShorthandAndRejections(t *testing.T) {
	data := recoveryYAML(
		"recovery:\n"+
			"  matrix:\n"+
			"    credential:\n"+
			"      cooldown: retry\n",
		"",
		"  m:\n    provider: pa\n    upstream-model: up-a\n")
	m := recoveryModel(t, data, "m")
	r := recoveryRule(t, m.Recovery, "credential-cooldown")
	if r.Action != recovery.ActionRetry {
		t.Errorf("credential-cooldown action = %s, want retry", r.Action)
	}
	if r.Match.Class != recovery.FailureCredential || r.Match.CredentialCause != recovery.CredentialCooldown {
		t.Errorf("credential-cooldown match = %+v", r.Match)
	}

	freeform := recoveryYAML(
		"recovery:\n"+
			"  matrix:\n"+
			"    rules:\n"+
			"      - id: pool-empty\n"+
			// Without `failure:` the class is inferred, so this row sits BELOW
			// the class-pinned default row in precedence instead of colliding
			// with it at equal precedence.
			"        when: {credential-cause: cooldown, retry-index: 1}\n"+
			"        action: fallback\n",
		"",
		"  m:\n    provider: pa\n    upstream-model: up-a\n")
	m2 := recoveryModel(t, freeform, "m")
	if r := recoveryRule(t, m2.Recovery, "pool-empty"); r.Action != recovery.ActionFallback {
		t.Errorf("pool-empty action = %s, want fallback", r.Action)
	}

	const marker = "SECRETMARKER"
	for _, tc := range []struct {
		name string
		yaml string
		want string
	}{
		{
			"bad shorthand key",
			"recovery:\n  matrix:\n    credential: {\"" + marker + "\": retry}\n",
			"credential cause token",
		},
		{
			"bad free-form cause",
			"recovery:\n  matrix:\n    rules:\n      - id: r1\n        when: {credential-cause: " + marker + "}\n        action: retry\n",
			"unknown credential cause token",
		},
		{
			"two cause kinds",
			"recovery:\n  matrix:\n    rules:\n      - id: r1\n        when: {transport-cause: tls, credential-cause: cooldown}\n        action: retry\n",
			"a single cause kind",
		},
		{
			"credential beside status",
			"recovery:\n  matrix:\n    rules:\n      - id: r1\n        when: {status: 429, credential-cause: cooldown}\n        action: retry\n",
			"predicates from a single layer",
		},
	} {
		if _, err := LoadRuntime([]byte(recoveryYAML(tc.yaml, "", "  m:\n    provider: pa\n    upstream-model: up-a\n"))); err == nil {
			t.Errorf("%s: accepted, want %q", tc.name, tc.want)
		} else if !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: error = %v, want it to name %q", tc.name, err, tc.want)
		}
	}
}

// TestRecoveryBlocksAcceptEveryDocumentedField pins the rest of the schema
// the shorthand test does not touch: the free-form rule predicate set, the
// resolved-retry and fallback terminal actions, a candidate-scoped envelope,
// and the Retry-After policy.
func TestRecoveryBlocksAcceptEveryDocumentedField(t *testing.T) {
	data := recoveryYAML(
		"recovery:\n"+
			"  retry-after:\n"+
			"    enabled: true\n"+
			"    mode: max\n"+
			"    max-delay: 3s\n",
		"",
		"  m:\n"+
			"    provider: pa\n"+
			"    upstream-model: up-a\n"+
			"    recovery:\n"+
			"      retries:\n"+
			"        on-exhausted: terminal\n"+
			"      fallback:\n"+
			"        enabled: true\n"+
			"        max-candidates: 3\n"+
			"        on-exhausted: terminal\n"+
			"      budget:\n"+
			"        candidate:\n"+
			"          max-exchanges: 8\n"+
			"          max-elapsed: 12s\n"+
			"      matrix:\n"+
			"        rules:\n"+
			"          - id: provider-throttled\n"+
			"            when: {failure: http, provider-error: {code: rate_limit_exceeded, type: rate_limit_error}}\n"+
			"            action: terminal\n"+
			"          - id: candidate-stall\n"+
			"            when: {protocol-cause: body_timeout, retry-index: 1}\n"+
			"            action: fallback\n"+
			"          - id: streamed-503\n"+
			"            when: {status: 503, streaming: true}\n"+
			"            action: retry\n"+
			"          - id: hold-502\n"+
			"            when: {status: 502, candidate-index: 2}\n"+
			"            action: terminal\n")

	m := recoveryModel(t, data, "m")
	p := m.Recovery
	if p.Retry.OnExhausted != recovery.ActionTerminal {
		t.Errorf("retries.on-exhausted = %s, want terminal", p.Retry.OnExhausted)
	}
	wantFallback := recovery.FallbackPolicy{Enabled: true, MaxCandidates: 3, OnExhausted: recovery.ActionTerminal}
	if !reflect.DeepEqual(p.Fallback, wantFallback) {
		t.Errorf("fallback = %+v, want %+v", p.Fallback, wantFallback)
	}
	wantEnvelope := recovery.Envelope{MaxExchanges: 8, MaxElapsed: 12 * time.Second}
	if !reflect.DeepEqual(p.Budget.Candidate, wantEnvelope) {
		t.Errorf("candidate envelope = %+v, want %+v", p.Budget.Candidate, wantEnvelope)
	}
	if p.Budget.Request.MaxExchanges != recovery.DefaultRequestMaxExchanges {
		t.Errorf("request envelope = %+v, want the inherited default", p.Budget.Request)
	}
	if got := p.RetryAfter; got.Enabled != true || got.Mode != recovery.RetryAfterMax || got.MaxDelay != 3*time.Second {
		t.Errorf("retry-after = %+v", got)
	}

	r := recoveryRule(t, p, "provider-throttled")
	if r.Action != recovery.ActionTerminal || r.Match.ProviderErrorCode != "rate_limit_exceeded" || r.Match.ProviderErrorType != "rate_limit_error" {
		t.Errorf("provider-throttled = %+v", r)
	}
	if r := recoveryRule(t, p, "candidate-stall"); r.Action != recovery.ActionFallback ||
		r.Match.ProtocolCause != recovery.ProtocolBodyTimeout || r.Match.RetryIndex == nil || *r.Match.RetryIndex != 1 {
		t.Errorf("candidate-stall = %+v", r)
	}
	if r := recoveryRule(t, p, "streamed-503"); r.Action != recovery.ActionRetry ||
		r.Match.Status != 503 || r.Match.Streaming == nil || !*r.Match.Streaming {
		t.Errorf("streamed-503 = %+v", r)
	}
	if r := recoveryRule(t, p, "hold-502"); r.Action != recovery.ActionTerminal || r.Match.CandidateIndex != 2 {
		t.Errorf("hold-502 = %+v", r)
	}
}

// TestRecoveryChainPrecedence pins the layer order and the merge semantics:
// the candidate is narrowest and wins every scalar it states, a layer that
// states only a nested field inherits the rest from the layers beneath it,
// a rule restated by identity is replaced wherever it was, and a rule with a
// new identity is added.
func TestRecoveryChainPrecedence(t *testing.T) {
	data := recoveryYAML(
		"recovery:\n"+
			"  retries:\n"+
			"    max-retries: 3\n"+
			"    backoff:\n"+
			"      initial: 1s\n"+
			"  fallback:\n"+
			"    max-candidates: 4\n",
		"    recovery:\n"+
			"      retries:\n"+
			"        max-retries: 5\n"+
			"      matrix:\n"+
			"        default: fallback\n",
		"  m:\n"+
			"    recovery:\n"+
			"      matrix:\n"+
			"        http:\n"+
			"          exact:\n"+
			"            \"429\": terminal\n"+
			"    providers:\n"+
			"      - provider: pa\n"+
			"        upstream-model: up-a\n"+
			"        recovery:\n"+
			"          retries:\n"+
			"            max-retries: 1\n"+
			"            backoff:\n"+
			"              jitter: 0.4\n"+
			"          matrix:\n"+
			"            rules:\n"+
			"              - id: cand-503\n"+
			"                when:\n"+
			"                  status: 503\n"+
			"                action: terminal\n")

	m := recoveryModel(t, data, "m")
	p := m.Recovery

	// Scalar precedence: candidate 1 over provider 5 over global 3.
	if p.Retry.MaxRetries != 1 {
		t.Errorf("max-retries = %d, want the candidate's 1", p.Retry.MaxRetries)
	}
	// Deep merge with inheritance: only jitter is stated at the candidate,
	// so initial comes from the global layer and max from the default.
	wantBackoff := recovery.BackoffPolicy{Initial: time.Second, Max: 2 * time.Second, Jitter: 0.4}
	if !reflect.DeepEqual(p.Retry.Backoff, wantBackoff) {
		t.Errorf("backoff = %+v, want %+v", p.Retry.Backoff, wantBackoff)
	}
	if p.Retry.MaxElapsed != 10*time.Second || p.Retry.OnExhausted != recovery.ActionFallback {
		t.Errorf("untouched retry fields drifted: %+v", p.Retry)
	}
	if !p.Fallback.Enabled || p.Fallback.MaxCandidates != 4 {
		t.Errorf("fallback = %+v, want the global layer's reach", p.Fallback)
	}
	// The provider's default action survives both later layers.
	if got := p.Matrix.Default(); got != recovery.ActionFallback {
		t.Errorf("default action = %s, want the provider's fallback", got)
	}
	// Rule replaced by identity: the model restated http-429.
	if got := recoveryRule(t, p, "http-429").Action; got != recovery.ActionTerminal {
		t.Errorf("http-429 action = %s, want the model's terminal", got)
	}
	// Rule added: the candidate introduced a new identity.
	if got := recoveryRule(t, p, "cand-503").Action; got != recovery.ActionTerminal {
		t.Errorf("cand-503 action = %s, want terminal", got)
	}
	if r := recoveryRule(t, p, "cand-503"); r.Match.Status != 503 {
		t.Errorf("cand-503 match = %+v, want the status predicate", r.Match)
	}
	// The model exposes the primary candidate's policy verbatim.
	if !reflect.DeepEqual(m.Recovery, m.Chain[0].Recovery) {
		t.Error("Model.Recovery does not mirror Chain[0].Recovery")
	}
}

// TestRecoveryProviderOverrideScopedToItsProvider pins the layer's reach and
// its rank: a provider override applies to the models routed through that
// provider and to no others, the global block applies to all of them, and a
// model-level statement still beats the provider it sits under.
func TestRecoveryProviderOverrideScopedToItsProvider(t *testing.T) {
	data := recoveryYAML(
		"recovery:\n  retries:\n    max-retries: 3\n",
		"    recovery:\n      retries:\n        max-retries: 5\n",
		"  on-pa:\n    provider: pa\n    upstream-model: up-a\n"+
			"  on-pb:\n    provider: pb\n    upstream-model: up-b\n"+
			"  on-pa-overridden:\n    provider: pa\n    upstream-model: up-a\n"+
			"    recovery:\n      retries:\n        max-retries: 4\n")

	for _, tc := range []struct {
		model string
		want  int
	}{
		{"on-pa", 5},
		{"on-pb", 3},
		{"on-pa-overridden", 4},
	} {
		m := recoveryModel(t, data, tc.model)
		if m.Recovery.Retry.MaxRetries != tc.want {
			t.Errorf("%s: max-retries = %d, want %d", tc.model, m.Recovery.Retry.MaxRetries, tc.want)
		}
	}
}

// TestRecoveryProviderOverrideRewritesOneMatrixRow is the provider-specific
// disposition scenario: the global matrix sends a 401 to the next candidate,
// and one provider's override makes it terminal for its own candidates only.
// The row keeps its canonical identity, so the override replaces it rather
// than adding a second rule that would have to be disambiguated.
func TestRecoveryProviderOverrideRewritesOneMatrixRow(t *testing.T) {
	data := recoveryYAML(
		"recovery:\n  matrix:\n    http:\n      exact:\n        \"401\": fallback\n",
		"    recovery:\n      matrix:\n        http:\n          exact:\n            \"401\": terminal\n",
		"  on-pa:\n    provider: pa\n    upstream-model: up-a\n"+
			"  on-pb:\n    provider: pb\n    upstream-model: up-b\n")

	if got := recoveryRule(t, recoveryModel(t, data, "on-pa").Recovery, recovery.StatusRuleID(401)).Action; got != recovery.ActionTerminal {
		t.Errorf("provider-a 401 action = %s, want the provider's terminal", got)
	}
	if got := recoveryRule(t, recoveryModel(t, data, "on-pb").Recovery, recovery.StatusRuleID(401)).Action; got != recovery.ActionFallback {
		t.Errorf("provider-b 401 action = %s, want the global fallback", got)
	}
}

// TestRecoveryChainedModelResolvesPerCandidate pins the per-candidate
// resolution: two candidates of one model may end up under different
// policies, because each folds its own provider and its own override, and
// the model still exposes the primary's policy.
func TestRecoveryChainedModelResolvesPerCandidate(t *testing.T) {
	data := recoveryYAML(
		"recovery:\n  retries:\n    max-retries: 3\n",
		"    recovery:\n      retries:\n        max-retries: 5\n",
		"  m:\n"+
			"    providers:\n"+
			"      - provider: pa\n"+
			"        upstream-model: up-a\n"+
			"      - provider: pb\n"+
			"        upstream-model: up-b\n"+
			"        recovery:\n"+
			"          retries:\n"+
			"            max-retries: 0\n"+
			"          matrix:\n"+
			"            default: fallback\n")

	m := recoveryModel(t, data, "m")
	if len(m.Chain) != 2 {
		t.Fatalf("chain length = %d, want 2", len(m.Chain))
	}
	if got := m.Chain[0].Recovery.Retry.MaxRetries; got != 5 {
		t.Errorf("candidate 1 max-retries = %d, want the provider's 5", got)
	}
	if got := m.Chain[1].Recovery.Retry.MaxRetries; got != 0 {
		t.Errorf("candidate 2 max-retries = %d, want the candidate's 0", got)
	}
	if got := m.Chain[1].Recovery.Matrix.Default(); got != recovery.ActionFallback {
		t.Errorf("candidate 2 default action = %s, want the candidate's fallback", got)
	}
	if got := m.Chain[0].Recovery.Matrix.Default(); got != recovery.ActionTerminal {
		t.Errorf("candidate 1 default action = %s, want the default terminal", got)
	}
	if !reflect.DeepEqual(m.Recovery, m.Chain[0].Recovery) {
		t.Error("Model.Recovery does not mirror Chain[0].Recovery")
	}
}

// TestRecoveryLegacyBlocksNormalizeEquivalently pins the compatibility rule:
// the legacy provider-fallback and retries blocks produce exactly the same
// effective policy as the recovery block that states the same values, and a
// file with neither produces the built-in default.
func TestRecoveryLegacyBlocksNormalizeEquivalently(t *testing.T) {
	legacy := recoveryYAML(
		"provider-fallback:\n  enabled: true\n  max-attempts: 3\n",
		"",
		"  m:\n    provider: pa\n    upstream-model: up-a\n"+
			"    retries:\n      max-retries: 2\n      max-elapsed: 5s\n"+
			"      backoff:\n        initial: 100ms\n        max: 1s\n        jitter: 0.2\n")
	modern := recoveryYAML(
		"recovery:\n  fallback:\n    enabled: true\n    max-candidates: 3\n",
		"",
		"  m:\n    provider: pa\n    upstream-model: up-a\n"+
			"    recovery:\n      retries:\n        max-retries: 2\n        max-elapsed: 5s\n"+
			"        backoff:\n          initial: 100ms\n          max: 1s\n          jitter: 0.2\n")

	lm := recoveryModel(t, legacy, "m")
	mm := recoveryModel(t, modern, "m")
	if !reflect.DeepEqual(lm.Recovery, mm.Recovery) {
		t.Errorf("legacy policy = %+v\nrecovery policy = %+v", lm.Recovery, mm.Recovery)
	}
	// The legacy block's defaults are the global layer's defaults.
	absent := recoveryModel(t, recoveryYAML("", "", "  m:\n    provider: pa\n    upstream-model: up-a\n"), "m")
	if !reflect.DeepEqual(absent.Recovery, recovery.Default()) {
		t.Errorf("absent blocks did not resolve to the built-in default: %+v", absent.Recovery)
	}
}

// TestRecoveryLegacyAndBlockAreMutuallyExclusive pins the anti-ambiguity
// rule: a layer that states both spellings of one policy rejects the file
// rather than leaving the effective behavior to whichever the code reads
// first. The two spellings may coexist when they speak about different
// policies.
func TestRecoveryLegacyAndBlockAreMutuallyExclusive(t *testing.T) {
	rejects := []struct {
		name string
		data string
	}{
		{
			"global fallback twice",
			recoveryYAML(
				"provider-fallback:\n  enabled: true\n"+
					"recovery:\n  fallback:\n    max-candidates: 3\n",
				"", "  m:\n    provider: pa\n    upstream-model: up-a\n"),
		},
		{
			"model retries twice",
			recoveryYAML("", "",
				"  m:\n    provider: pa\n    upstream-model: up-a\n"+
					"    retries:\n      max-retries: 2\n"+
					"    recovery:\n      retries:\n        max-retries: 3\n"),
		},
	}
	for _, tc := range rejects {
		_, err := LoadRuntime([]byte(tc.data))
		if err == nil {
			t.Errorf("%s: accepted, want rejection", tc.name)
			continue
		}
		if !strings.Contains(err.Error(), "mutually exclusive") {
			t.Errorf("%s: err = %v, want the mutual-exclusion rejection", tc.name, err)
		}
	}

	// Different policies, different spellings: accepted.
	if _, err := LoadRuntime([]byte(recoveryYAML(
		"provider-fallback:\n  enabled: true\n  max-attempts: 3\n"+
			"recovery:\n  retries:\n    max-retries: 4\n",
		"", "  m:\n    provider: pa\n    upstream-model: up-a\n"))); err != nil {
		t.Errorf("legacy fallback next to recovery.retries rejected: %v", err)
	}
	if _, err := LoadRuntime([]byte(recoveryYAML("", "",
		"  m:\n    provider: pa\n    upstream-model: up-a\n"+
			"    retries:\n      max-retries: 2\n"+
			"    recovery:\n      matrix:\n        default: fallback\n"))); err != nil {
		t.Errorf("legacy retries next to recovery.matrix rejected: %v", err)
	}
}

// TestRecoveryLegacyFallbackSurvivesARecoveryBlock is the compatibility rule
// for the other half of the pair: a legacy provider-fallback stated beside a
// recovery block that does NOT carry a fallback is folded into the layer, not
// discarded. Dropping it would leave the operator's statement silently
// unapplied — the disabled walk still walking, a reach of three becoming the
// default two — which is exactly the invisible behavior the mutual-exclusion
// rule exists to prevent.
func TestRecoveryLegacyFallbackSurvivesARecoveryBlock(t *testing.T) {
	cases := []struct {
		name       string
		legacy     string
		enabled    bool
		candidates int
	}{
		{
			"a disabled legacy walk beside an unrelated recovery block",
			"provider-fallback:\n  enabled: false\n  max-attempts: 5\n" +
				"recovery:\n  retries:\n    max-retries: 4\n",
			false, 1,
		},
		{
			"an enabled legacy reach of three beside an unrelated block",
			"provider-fallback:\n  enabled: true\n  max-attempts: 3\n" +
				"recovery:\n  matrix:\n    default: terminal\n",
			true, 3,
		},
		{
			"a legacy block beside an empty recovery block",
			"provider-fallback:\n  enabled: false\n  max-attempts: 5\n" +
				"recovery: {}\n",
			false, 1,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m := recoveryModel(t, recoveryYAML(tc.legacy, "", "  m:\n    provider: pa\n    upstream-model: up-a\n"), "m")
			if got := m.Recovery.Fallback.Enabled; got != tc.enabled {
				t.Errorf("fallback.enabled = %v, want %v", got, tc.enabled)
			}
			if got := m.Recovery.Fallback.MaxCandidates; got != tc.candidates {
				t.Errorf("fallback.max-candidates = %d, want %d", got, tc.candidates)
			}
		})
	}
}

// TestRecoveryLegacyDisabledWalkIgnoresItsReach pins the one legacy nicety
// that is preserved rather than turned into a rejection: a block that states
// `enabled: false` alongside `max-attempts` ran pinned before the policy
// engine (the count was read only when the walk was on), so it still does. A
// file that worked must not become a startup failure over a number that never
// had an effect.
func TestRecoveryLegacyDisabledWalkIgnoresItsReach(t *testing.T) {
	m := recoveryModel(t, recoveryYAML(
		"provider-fallback:\n  enabled: false\n  max-attempts: 5\n",
		"", "  m:\n    provider: pa\n    upstream-model: up-a\n"), "m")
	if m.Recovery.Fallback.Enabled {
		t.Error("the walk was not disabled")
	}
	if m.Recovery.Fallback.MaxCandidates != 1 {
		t.Errorf("fallback.max-candidates = %d, want the pinned reach 1", m.Recovery.Fallback.MaxCandidates)
	}
}

// TestRecoveryLegacyRetriesRejectNonFiniteJitter pins the same guard on the
// legacy alias, which is where the shipped service enforced it: a jitter the
// arithmetic cannot use is a rejected file on either spelling, never a
// silently different schedule.
func TestRecoveryLegacyRetriesRejectNonFiniteJitter(t *testing.T) {
	for _, jitter := range []string{".nan", ".inf", "2"} {
		data := recoveryYAML("", "", "  m:\n    provider: pa\n    upstream-model: up-a\n"+
			"    retries:\n      backoff:\n        jitter: "+jitter+"\n")
		if _, err := LoadRuntime([]byte(data)); err == nil {
			t.Errorf("jitter %s: accepted, want rejection", jitter)
		}
	}
}

// TestRecoveryRejections pins the whole-file rejections of the recovery
// schema, and the no-echo rule over them: every message is fixed text that
// names the position, and the distinctive marker every case carries never
// appears in the error.
func TestRecoveryRejections(t *testing.T) {
	const marker = "SECRETMARKER"
	simple := "  m:\n    provider: pa\n    upstream-model: up-a\n"
	cases := []struct {
		name  string
		block string
		want  string
	}{
		{"unknown recovery key", "recovery:\n  retry-afterr:\n    enabled: true\n", "line"},
		{"unknown matrix key", "recovery:\n  matrix:\n    http:\n      exacts:\n        \"429\": retry\n", "line"},
		{"unknown retries key", "recovery:\n  retries:\n    max-attempt: 2\n", "line"},
		{"unknown retry-after key", "recovery:\n  retry-after:\n    window: 5s\n", "line"},
		{"bad action", "recovery:\n  matrix:\n    default: " + marker + "\n", "must be one of retry, fallback, terminal"},
		{"bad rule action", "recovery:\n  matrix:\n    rules:\n      - id: r1\n        action: " + marker + "\n", "must be one of retry, fallback, terminal"},
		{"bad status key", "recovery:\n  matrix:\n    http:\n      exact: {\"" + marker + "\": retry}\n", "three-digit HTTP status"},
		{"bad status class", "recovery:\n  matrix:\n    http:\n      classes: {\"" + marker + "\": retry}\n", "status class must be one of"},
		{"bad transport token", "recovery:\n  matrix:\n    transport: {\"" + marker + "\": retry}\n", "must be a transport class"},
		{"bad protocol token", "recovery:\n  matrix:\n    protocol: {\"" + marker + "\": retry}\n", "protocol cause token"},
		{"bad caller token", "recovery:\n  matrix:\n    caller: {\"" + marker + "\": terminal}\n", "caller cause must be one of"},
		{"non-terminal caller rule", "recovery:\n  matrix:\n    caller:\n      canceled: retry\n", "recovery.matrix.caller must be terminal"},
		{"non-terminal free-form caller rule", "recovery:\n  matrix:\n    rules:\n      - id: r1\n        when: {caller-cause: canceled}\n        action: retry\n", "caller rule"},
		{"duplicate rule id", "recovery:\n  matrix:\n    rules:\n      - id: dup\n        when: {status: 400}\n        action: terminal\n      - id: dup\n        when: {status: 401}\n        action: retry\n", "repeats the identity"},
		{"ambiguous rule pair", "recovery:\n  matrix:\n    rules:\n      - id: r1\n        when: {status: 400}\n        action: terminal\n      - id: r2\n        when: {status: 400}\n        action: retry\n", "equal precedence"},
		{"rule without id", "recovery:\n  matrix:\n    rules:\n      - when: {status: 502}\n        action: terminal\n", "rule id"},
		{"rule without action", "recovery:\n  matrix:\n    rules:\n      - id: r1\n        when: {status: 502}\n", "must be one of retry, fallback, terminal"},
		{"invalid rule id", "recovery:\n  matrix:\n    rules:\n      - id: \"" + marker + " x\"\n        action: terminal\n", "rule id"},
		{"reserved rule id", "recovery:\n  matrix:\n    rules:\n      - id: default\n        when: {status: 400}\n        action: terminal\n", "reserved"},
		{"over-cap retries", "recovery:\n  retries:\n    max-retries: 9\n", "retries.max-retries"},
		{"over-cap fallback", "recovery:\n  fallback:\n    max-candidates: 9\n", "fallback.max-candidates"},
		{"over-cap request budget", "recovery:\n  budget:\n    request:\n      max-exchanges: 65\n", "budget.request.max-exchanges"},
		{"over-cap candidate budget", "recovery:\n  budget:\n    candidate:\n      max-exchanges: 33\n", "budget.candidate.max-exchanges"},
		{"candidate envelope above request", "recovery:\n  budget:\n    request:\n      max-exchanges: 4\n    candidate:\n      max-exchanges: 6\n", "budget.candidate.max-exchanges"},
		{"retry window above the envelope", "recovery:\n  retries:\n    max-elapsed: 20s\n  budget:\n    candidate:\n      max-elapsed: 10s\n", "retries.max-elapsed"},
		{"retry budget the envelope cannot fund", "recovery:\n  retries:\n    max-retries: 5\n  budget:\n    candidate:\n      max-exchanges: 3\n", "retries.max-retries"},
		{"bad retries duration", "recovery:\n  retries:\n    max-elapsed: " + marker + "\n", "must be a valid duration"},
		{"bad backoff duration", "recovery:\n  retries:\n    backoff:\n      initial: " + marker + "\n", "must be a valid duration"},
		{"bad budget duration", "recovery:\n  budget:\n    candidate:\n      max-elapsed: " + marker + "\n", "must be a valid duration"},
		{"bad retry-after mode", "recovery:\n  retry-after:\n    mode: " + marker + "\n", "retry-after mode must be one of"},
		{"bad retry-after delay", "recovery:\n  retry-after:\n    max-delay: " + marker + "\n", "must be a valid duration"},
		{"bad failure token", "recovery:\n  matrix:\n    rules:\n      - id: r1\n        when: {failure: " + marker + "}\n        action: retry\n", "must be one of http, transport, protocol, caller"},
		{"bad status predicate", "recovery:\n  matrix:\n    rules:\n      - id: r1\n        when: {status: 99}\n        action: retry\n", "between 100 and 599"},
		{"bad candidate index", "recovery:\n  matrix:\n    rules:\n      - id: r1\n        when: {candidate-index: 0}\n        action: retry\n", "at least 1"},
		{"bad transport-cause token", "recovery:\n  matrix:\n    rules:\n      - id: r1\n        when: {transport-cause: " + marker + "}\n        action: retry\n", "unknown transport cause token"},
		{"bad protocol-cause token", "recovery:\n  matrix:\n    rules:\n      - id: r1\n        when: {protocol-cause: " + marker + "}\n        action: retry\n", "unknown protocol cause token"},
		{"bad caller-cause token", "recovery:\n  matrix:\n    rules:\n      - id: r1\n        when: {caller-cause: " + marker + "}\n        action: retry\n", "caller cause must be one of"},
		{"two cause kinds", "recovery:\n  matrix:\n    rules:\n      - id: r1\n        when: {transport-cause: tls, protocol-cause: body_timeout}\n        action: retry\n", "a single cause kind"},
		// A rule naming no class is held to the one-layer rule by inference:
		// no observation carries a transport cause and an HTTP status at once.
		{"mixed-layer free-form rule", "recovery:\n  matrix:\n    rules:\n      - id: r1\n        when: {status: 429, transport-cause: tls}\n        action: retry\n", "predicates from a single layer"},
		{"disabled fallback with a reach", "recovery:\n  fallback:\n    enabled: false\n    max-candidates: 3\n", "fallback.max-candidates"},
		// NaN survives every comparison, so a bare range check admits it and
		// the spread arithmetic collapses to a zero wait. It must be named.
		{"nan jitter", "recovery:\n  retries:\n    backoff:\n      jitter: .nan\n", "jitter"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := LoadRuntime([]byte(recoveryYAML(tc.block, "", simple)))
			if err == nil {
				t.Fatal("accepted, want rejection")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want containing %q", err, tc.want)
			}
			if strings.Contains(err.Error(), marker) {
				t.Fatalf("rejection echoes operator input: %v", err)
			}
		})
	}
}

// TestRecoveryRejectsRequestBudgetInOverrides pins the position rule: the
// request-scoped envelope belongs to the top-level block only, and an
// override that states one is rejected by position — never silently
// ignored, which would leave two candidates disagreeing about a
// request-wide ceiling.
func TestRecoveryRejectsRequestBudgetInOverrides(t *testing.T) {
	const block = "    recovery:\n      budget:\n        request:\n          max-exchanges: 8\n"
	cases := []struct {
		name string
		data string
	}{
		{"provider", recoveryYAML("", block, "  m:\n    provider: pa\n    upstream-model: up-a\n")},
		{"model", recoveryYAML("", "", "  m:\n    provider: pa\n    upstream-model: up-a\n"+block)},
		{
			"candidate",
			recoveryYAML("", "",
				"  m:\n    providers:\n      - provider: pa\n        upstream-model: up-a\n"+
					"        recovery:\n          budget:\n            request:\n              max-exchanges: 8\n"),
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := LoadRuntime([]byte(tc.data))
			if err == nil {
				t.Fatal("accepted, want rejection")
			}
			if !strings.Contains(err.Error(), "recovery.budget.request") {
				t.Fatalf("err = %v, want the request-envelope position", err)
			}
		})
	}

	// The top-level block accepts the same envelope, and an override may
	// still state the candidate-scoped one.
	snap, err := LoadRuntime([]byte(recoveryYAML(
		"recovery:\n  budget:\n    request:\n      max-exchanges: 20\n",
		"    recovery:\n      budget:\n        candidate:\n          max-exchanges: 4\n",
		"  m:\n    provider: pa\n    upstream-model: up-a\n")))
	if err != nil {
		t.Fatalf("valid envelopes rejected: %v", err)
	}
	m, _ := snap.Model("m")
	if m.Recovery.Budget.Request.MaxExchanges != 20 || m.Recovery.Budget.Candidate.MaxExchanges != 4 {
		t.Fatalf("envelopes did not land: %+v", m.Recovery.Budget)
	}
}

// TestRecoveryFallbackScopeIsTheRequest pins the walk-bound's scope: the
// candidate-walk reach is a property of the REQUEST's primary policy, so a
// provider- or candidate-level `recovery.fallback` block would merge,
// validate, hash, and then do nothing — the engine's reach gate reads only
// the primary policy's Fallback. A file stating one is rejected by position,
// never silently accepted with misleading semantics. The global layer owns
// the walk bound, and a model may state it too, because a model's override
// resolves into that model's own primary candidate and genuinely steers that
// model's walk.
func TestRecoveryFallbackScopeIsTheRequest(t *testing.T) {
	const block = "    recovery:\n      fallback:\n        max-candidates: 4\n"
	rejects := []struct {
		name string
		data string
	}{
		{"provider", recoveryYAML("", block, "  m:\n    provider: pa\n    upstream-model: up-a\n")},
		{
			"candidate",
			recoveryYAML("", "",
				"  m:\n    providers:\n      - provider: pa\n        upstream-model: up-a\n"+
					"        recovery:\n          fallback:\n            max-candidates: 4\n"),
		},
	}
	for _, tc := range rejects {
		t.Run(tc.name, func(t *testing.T) {
			_, err := LoadRuntime([]byte(tc.data))
			if err == nil {
				t.Fatal("accepted, want rejection")
			}
			if !strings.Contains(err.Error(), "recovery.fallback") {
				t.Fatalf("err = %v, want the fallback-scope position", err)
			}
		})
	}

	// The global and model positions accept the same block and it reaches the
	// walk: the model's fallback steers its own primary candidate.
	data := recoveryYAML(
		"recovery:\n  fallback:\n    max-candidates: 4\n",
		"",
		"  m:\n    provider: pa\n    upstream-model: up-a\n"+
			"    recovery:\n      fallback:\n        max-candidates: 6\n")
	m := recoveryModel(t, data, "m")
	if m.Recovery.Fallback.MaxCandidates != 6 {
		t.Fatalf("model fallback = %+v, want the model's walk bound", m.Recovery.Fallback)
	}
	// The global-only chain of the same shape reaches the default reach for
	// the model without a block of its own.
	if got := recoveryModel(t, recoveryYAML("recovery:\n  fallback:\n    max-candidates: 4\n", "", "  m:\n    provider: pa\n    upstream-model: up-a\n"), "m"); got.Recovery.Fallback.MaxCandidates != 4 {
		t.Fatalf("global fallback did not reach the model: %+v", got.Recovery.Fallback)
	}
}

// TestRecoveryRejectedReloadKeepsLastKnownGood pins the reload contract on
// the recovery block: a file whose recovery policy is rejected leaves the
// previous snapshot serving, its generation unchanged, and its resolved
// policy untouched.
func TestRecoveryRejectedReloadKeepsLastKnownGood(t *testing.T) {
	good := recoveryYAML(
		"recovery:\n  retries:\n    max-retries: 4\n",
		"", "  m:\n    provider: pa\n    upstream-model: up-a\n")
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	writeFile(t, path, good)

	store := NewStore(mustSnapshot(t, good))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go NewPoller(store, path, []byte(good), 15*time.Millisecond, testLog(t), nil).Run(ctx)

	// A file carrying an over-cap retry budget is rejected: the last-known-good
	// snapshot keeps serving, policy included.
	writeFile(t, path, recoveryYAML(
		"recovery:\n  retries:\n    max-retries: 9\n",
		"", "  m:\n    provider: pa\n    upstream-model: up-a\n"))
	waitGenStable(t, store, store.Gen(), 150*time.Millisecond)
	got, _ := store.Load().Model("m")
	if got.Recovery.Retry.MaxRetries != 4 {
		t.Fatalf("rejected reload clobbered the policy: %+v", got.Recovery.Retry)
	}

	// A valid change still republishes afterwards.
	writeFile(t, path, recoveryYAML(
		"recovery:\n  retries:\n    max-retries: 6\n",
		"", "  m:\n    provider: pa\n    upstream-model: up-a\n"))
	waitGen(t, store, 1)
	got, _ = store.Load().Model("m")
	if got.Recovery.Retry.MaxRetries != 6 {
		t.Fatalf("valid reload not visible: %+v", got.Recovery.Retry)
	}
}

// TestRecoveryTopLevelKeyIsLegal pins the schema plane: `recovery` is a
// legal top-level key like every other runtime block, and an unknown
// top-level key is still rejected alongside it.
func TestRecoveryTopLevelKeyIsLegal(t *testing.T) {
	if _, err := LoadRuntime([]byte(recoveryYAML(
		"recovery:\n", "", "  m:\n    provider: pa\n    upstream-model: up-a\n"))); err != nil {
		t.Fatalf("null recovery block rejected: %v", err)
	}
	if _, err := LoadRuntime([]byte(recoveryYAML(
		"recovery: {}\n", "", "  m:\n    provider: pa\n    upstream-model: up-a\n"))); err != nil {
		t.Fatalf("empty recovery block rejected: %v", err)
	}
	_, err := LoadRuntime([]byte(recoveryYAML(
		"recoveries: {}\n", "", "  m:\n    provider: pa\n    upstream-model: up-a\n")))
	if err == nil {
		t.Fatal("a near-miss top-level key was accepted")
	}
}

// TestRecoveryHashIsStableAcrossLoadsAndNamesTheData pins the frozen
// policy's data identity at the load boundary: two loads of the same file
// carry the same hash for the same candidate and model, a file whose
// recovery block differs carries a different one, and the flattened
// Model.RecoveryHash always mirrors Chain[0].RecoveryHash.
func TestRecoveryHashIsStableAcrossLoadsAndNamesTheData(t *testing.T) {
	body := recoveryYAML(
		"recovery:\n  retries:\n    max-retries: 3\n",
		"",
		"  m:\n    provider: pa\n    upstream-model: up-a\n")

	first := recoveryModel(t, body, "m")
	if first.RecoveryHash == "" {
		t.Fatal("no hash was frozen onto the model")
	}
	if first.RecoveryHash != first.Chain[0].RecoveryHash {
		t.Errorf("model hash %q does not mirror Chain[0]'s %q", first.RecoveryHash, first.Chain[0].RecoveryHash)
	}
	if want := first.Recovery.Hash(); first.RecoveryHash != want {
		t.Errorf("frozen hash %q does not match the policy's own %q", first.RecoveryHash, want)
	}
	if second := recoveryModel(t, body, "m"); second.RecoveryHash != first.RecoveryHash {
		t.Errorf("two loads of one file hashed differently: %q vs %q", second.RecoveryHash, first.RecoveryHash)
	}

	// A different recovery field is a different policy, and the evidence
	// must be able to tell the two deployments apart.
	changed := recoveryModel(t, recoveryYAML(
		"recovery:\n  retries:\n    max-retries: 4\n",
		"",
		"  m:\n    provider: pa\n    upstream-model: up-a\n"), "m")
	if changed.RecoveryHash == first.RecoveryHash {
		t.Errorf("a changed recovery block kept hash %q", first.RecoveryHash)
	}

	// A candidate-level override is a per-candidate policy, so the chain's
	// entries can differ from each other and from the model's primary.
	perCand := recoveryModel(t, recoveryYAML(
		"", "",
		"  m:\n    providers:\n"+
			"      - provider: pa\n        upstream-model: up-a\n"+
			"      - provider: pb\n        upstream-model: up-b\n"+
			"        recovery:\n          retries:\n            max-retries: 5\n"), "m")
	if len(perCand.Chain) != 2 {
		t.Fatalf("chain = %d candidates, want 2", len(perCand.Chain))
	}
	if perCand.Chain[0].RecoveryHash == perCand.Chain[1].RecoveryHash {
		t.Error("two candidates under different policies share a hash")
	}
	if perCand.RecoveryHash != perCand.Chain[0].RecoveryHash {
		t.Error("model hash does not mirror the primary candidate's")
	}
}
