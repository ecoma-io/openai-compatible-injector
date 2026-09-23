package recovery

import (
	"testing"
	"time"
)

// TestPolicyHashIsDeterministic: the same policy hashes to the same token
// every time, in this process and every other. Nothing in the encoding may
// depend on map order, pointer identity, or a formatting accident.
func TestPolicyHashIsDeterministic(t *testing.T) {
	p := Default()
	first := p.Hash()
	for i := 0; i < 8; i++ {
		if got := p.Hash(); got != first {
			t.Fatalf("call %d: hash = %q, want the stable %q", i, got, first)
		}
	}
	// The token has the documented shape: 16 lowercase hex characters.
	if len(first) != 16 {
		t.Fatalf("hash = %q (len %d), want 16 hex characters", first, len(first))
	}
	for _, c := range first {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			t.Fatalf("hash = %q contains %q, want lowercase hex only", first, c)
		}
	}
	// A separately built default hashes identically: the identity belongs to
	// the DATA, not to the value that happens to hold it.
	if again := Default().Hash(); again != first {
		t.Fatalf("a freshly built default hashed %q, want %q", again, first)
	}
}

// TestPolicyHashDistinguishesDifferingPolicies: every block a policy owns
// changes its identity. Two policies that differ in ANY member must not
// collide, or the evidence would claim two populations ran under one policy.
func TestPolicyHashDistinguishesDifferingPolicies(t *testing.T) {
	base := Default()
	want := base.Hash()

	mutations := map[string]func(p *Policy){
		"retry count":     func(p *Policy) { p.Retry.MaxRetries++ },
		"retry window":    func(p *Policy) { p.Retry.MaxElapsed += time.Second },
		"backoff initial": func(p *Policy) { p.Retry.Backoff.Initial += time.Millisecond },
		"backoff max":     func(p *Policy) { p.Retry.Backoff.Max += time.Millisecond },
		"backoff jitter":  func(p *Policy) { p.Retry.Backoff.Jitter += 0.01 },
		"retry on-exhausted": func(p *Policy) {
			p.Retry.OnExhausted = ActionTerminal
		},
		"fallback enabled":  func(p *Policy) { p.Fallback.Enabled = false },
		"fallback budget":   func(p *Policy) { p.Fallback.MaxCandidates++ },
		"fallback default":  func(p *Policy) { p.Fallback.OnExhausted = ActionRetry },
		"request max":       func(p *Policy) { p.Budget.Request.MaxExchanges++ },
		"request window":    func(p *Policy) { p.Budget.Request.MaxElapsed -= time.Second },
		"candidate max":     func(p *Policy) { p.Budget.Candidate.MaxExchanges++ },
		"candidate window":  func(p *Policy) { p.Budget.Candidate.MaxElapsed -= time.Second },
		"retry-after on":    func(p *Policy) { p.RetryAfter.Enabled = false },
		"retry-after mode":  func(p *Policy) { p.RetryAfter.Mode = RetryAfterIgnore },
		"retry-after delay": func(p *Policy) { p.RetryAfter.MaxDelay += time.Second },
	}
	for name, mutate := range mutations {
		p := base
		mutate(&p)
		if got := p.Hash(); got == want {
			t.Errorf("%s: hash %q collides with the unmutated policy", name, got)
		}
	}

	// A matrix difference is a policy difference, and rule ORDER is part of
	// the matrix's data — the identity must follow the stored order.
	added, err := NewMatrix(append(base.Matrix.Rules(), Rule{
		ID:     StatusRuleID(418),
		Match:  Match{Status: 418},
		Action: ActionTerminal,
	}), base.Matrix.Default())
	if err != nil {
		t.Fatalf("NewMatrix: %v", err)
	}
	withRule := base
	withRule.Matrix = added
	if withRule.Hash() == want {
		t.Error("an added matrix rule left the hash unchanged")
	}

	defaultAction := base
	flipped, err := NewMatrix(base.Matrix.Rules(), ActionFallback)
	if err != nil {
		t.Fatalf("NewMatrix: %v", err)
	}
	defaultAction.Matrix = flipped
	if defaultAction.Hash() == want {
		t.Error("a changed matrix default action left the hash unchanged")
	}
}

// TestPolicyHashIsAValueIdentity: the identity belongs to the policy's
// data, not to a particular value or to anything reachable from outside it.
// Two policies holding identical data hash the same — the hash names the
// frozen result, not the route that produced it — and nothing a caller can
// mutate through a returned copy can move it.
func TestPolicyHashIsAValueIdentity(t *testing.T) {
	p := Default()
	copyOfP := p
	if copyOfP.Hash() != p.Hash() {
		t.Fatal("a policy copy hashed differently from its source")
	}
	// Rules are returned as a copy, so mutating what Rules() handed out
	// cannot move the identity.
	rules := p.Matrix.Rules()
	if len(rules) == 0 {
		t.Fatal("the default matrix has no rules")
	}
	rules[0].Action = ActionRetry
	if p.Hash() != copyOfP.Hash() {
		t.Error("mutating the Rules() copy changed the policy hash")
	}
}
