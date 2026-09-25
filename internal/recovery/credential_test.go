package recovery

import (
	"context"
	"testing"
	"time"
)

// credObs builds the observation the proxy hands the engine when a
// candidate's credential pool has nothing ready: the class carries the
// pool's state, the cause names why, and the wait rides RetryAfter exactly
// as an upstream directive does.
func credObs(attempt int, wait time.Duration) Observation {
	return Observation{
		Class:            FailureCredential,
		CredentialCause:  CredentialCooldown,
		RetryAfter:       wait,
		CandidateIndex:   1,
		CandidateAttempt: attempt,
		RetryIndex:       attempt - 1,
	}
}

// TestDefaultMatrixCredentialCooldownRetries pins the default disposition:
// an undialed attempt whose pool has nothing ready is a retry, not a
// fallback and not a terminal — the candidate did not fail, it must wait.
func TestDefaultMatrixCredentialCooldownRetries(t *testing.T) {
	pol := Default()
	action, id := pol.Matrix.Match(credObs(1, time.Second))
	if action != ActionRetry || id != "credential-cooldown" {
		t.Fatalf("credential cooldown = %v, %q, want retry, credential-cooldown", action, id)
	}
	if got := Reason(credObs(1, time.Second)); got != "credential_cooldown" {
		t.Fatalf("Reason = %q, want credential_cooldown", got)
	}
	// The rule is addressable by its canonical ID: an operator override can
	// restate it without restating the whole matrix.
	found := false
	for _, r := range pol.Matrix.Rules() {
		if r.ID == "credential-cooldown" {
			found = true
			if r.Match.Class != FailureCredential || r.Match.CredentialCause != CredentialCooldown {
				t.Fatalf("credential-cooldown match = %+v", r.Match)
			}
		}
	}
	if !found {
		t.Fatal("the default matrix has no credential-cooldown rule")
	}
}

// TestEngineCredentialCooldownWaitRidesRetryAfter is the seam the feature
// rests on: the pool's earliest ready-at time flows into the engine through
// the SAME RetryAfter channel as a directive, so the raise/cap machinery and
// every existing ceiling bind it without one line of engine change.
func TestEngineCredentialCooldownWaitRidesRetryAfter(t *testing.T) {
	cases := []struct {
		name string
		wait time.Duration
		want time.Duration
	}{
		{"a cooldown above the backoff ceiling is ceilinged", 30 * time.Second, DefaultBackoffMax},
		{"a cooldown between the backoff and its ceiling wins", time.Second, time.Second},
		{"a cooldown below the backoff is only a floor", 50 * time.Millisecond, DefaultBackoffInitial},
		{"no reported ready-at falls to plain backoff", 0, DefaultBackoffInitial},
	}
	for _, c := range cases {
		pol := Default()
		e, _ := newTestEngine(t, context.Background(), pol)
		e.EnterCandidate(pol)
		d := e.Observe(credObs(1, c.wait))
		if d.Action != ActionRetry {
			t.Fatalf("%s: %+v", c.name, d)
		}
		if d.Delay != c.want {
			t.Errorf("%s: delay = %v, want %v", c.name, d.Delay, c.want)
		}
	}
}

// TestEngineCredentialCooldownExhaustsLikeAnyRetry: the wait consumes the
// candidate's real retry budget — no parallel accounting, no free re-asks —
// so a pool that never recovers ends the candidate through the ordinary
// on-exhausted path.
func TestEngineCredentialCooldownExhaustsLikeAnyRetry(t *testing.T) {
	pol := Default()
	e, clk := newTestEngine(t, context.Background(), pol)
	e.EnterCandidate(pol)

	if d := e.Observe(credObs(1, time.Second)); d.Action != ActionRetry {
		t.Fatalf("first cooldown: %+v", d)
	}
	clk.advance(time.Second)
	if d := e.Observe(credObs(2, time.Second)); d.Action != ActionFallback {
		t.Fatalf("second cooldown should exhaust the retry budget: %+v", d)
	}
	if e.CandidatesEntered() != 1 {
		t.Fatalf("candidates entered = %d, want 1 — rotation never enters a candidate", e.CandidatesEntered())
	}

	// With the walk disabled, the same exhaustion is terminal.
	disabled, err := Merge(Default(), Partial{Fallback: &FallbackPartial{Enabled: boolp(false)}})
	if err != nil {
		t.Fatalf("Merge: %v", err)
	}
	e2, _ := newTestEngine(t, context.Background(), disabled)
	e2.EnterCandidate(disabled)
	if d := e2.Observe(credObs(1, time.Second)); d.Action != ActionRetry {
		t.Fatalf("pinned primary first cooldown: %+v", d)
	}
	if d := e2.Observe(credObs(2, time.Second)); d.Action != ActionTerminal {
		t.Fatalf("pinned primary exhausted cooldown: %+v", d)
	}
}

// TestCredentialObservationMatchesNothingElse: the credential class is its
// own layer — no status, transport, protocol, or caller row of the default
// matrix claims it, and its rows claim no other class's observation.
func TestCredentialObservationMatchesNothingElse(t *testing.T) {
	pol := Default()
	// Credential rows leave every other observation to the rest of the
	// matrix: a non-credential observation carries no credential cause, so
	// the only rule that could claim it (credential-cooldown) does not.
	if _, id := pol.Matrix.Match(httpObs(429, 1)); id == "credential-cooldown" {
		t.Fatal("an HTTP 429 was claimed by the credential rule")
	}
	if _, id := pol.Matrix.Match(Observation{Class: FailureTransport, TransportCause: CauseDial}); id == "credential-cooldown" {
		t.Fatal("a transport failure was claimed by the credential rule")
	}
}

// TestCredentialRuleValidation pins the fail-closed grammar: a credential
// rule is one layer among five, so it may not mix predicates with another
// layer or another cause kind.
func TestCredentialRuleValidation(t *testing.T) {
	valid := []Match{
		{Class: FailureCredential, CredentialCause: CredentialCooldown},
		{CredentialCause: CredentialCooldown}, // the class is implied by inference
	}
	for i, m := range valid {
		if _, err := NewMatrix([]Rule{{ID: "r", Match: m, Action: ActionRetry}}, ActionTerminal); err != nil {
			t.Errorf("valid[%d] %+v: %v", i, m, err)
		}
	}
	invalid := []Match{
		// Two cause kinds on one rule.
		{CredentialCause: CredentialCooldown, TransportCause: CauseDial},
		{CredentialCause: CredentialCooldown, CallerCause: CallerCanceled},
		{CredentialCause: CredentialCooldown, ProtocolCause: ProtocolBodyTimeout},
		// A credential predicate beside a status (a different layer).
		{CredentialCause: CredentialCooldown, Status: 429},
		// A class that contradicts the predicate.
		{Class: FailureTransport, CredentialCause: CredentialCooldown},
		// A credential rule that also narrows by a transport class.
		{CredentialCause: CredentialCooldown, TransportClass: TransportClassTimeout},
	}
	for i, m := range invalid {
		if _, err := NewMatrix([]Rule{{ID: "r", Match: m, Action: ActionRetry}}, ActionTerminal); err == nil {
			t.Errorf("invalid[%d] %+v: accepted", i, m)
		}
	}
}

// TestCredentialClassTokens: the class and its cause parse, render, and stay
// inside their closed sets.
func TestCredentialClassTokens(t *testing.T) {
	c, err := ParseFailureClass("credential")
	if err != nil || c != FailureCredential {
		t.Fatalf("ParseFailureClass(credential) = %v, %v", c, err)
	}
	if c.String() != "credential" {
		t.Fatalf("String = %q", c.String())
	}
	if !KnownCredentialCause(CredentialCooldown) {
		t.Fatal("cooldown is not a known credential cause")
	}
	if KnownCredentialCause("expired") || KnownCredentialCause("") {
		t.Fatal("an unknown token passed as a credential cause")
	}
	if got := CredentialCauseRuleID(CredentialCooldown); got != "credential-cooldown" {
		t.Fatalf("CredentialCauseRuleID = %q", got)
	}
	if got := Reason(Observation{Class: FailureCredential}); got != "credential_unavailable" {
		t.Fatalf("bare credential Reason = %q", got)
	}
}

// TestCredentialCauseChangesPolicyHash: two policies differing only in a
// credential row hash differently — the hash covers the whole predicate set,
// which is what lets evidence tell two policies apart.
func TestCredentialCauseChangesPolicyHash(t *testing.T) {
	base := Default()
	other := Default()
	rules := other.Matrix.Rules()
	for i := range rules {
		if rules[i].ID == "credential-cooldown" {
			rules[i].Action = ActionTerminal
		}
	}
	m, err := NewMatrix(rules, other.Matrix.Default())
	if err != nil {
		t.Fatalf("NewMatrix: %v", err)
	}
	other.Matrix = m
	if base.Hash() == other.Hash() {
		t.Fatal("policies differing in the credential row hash identically")
	}
}
