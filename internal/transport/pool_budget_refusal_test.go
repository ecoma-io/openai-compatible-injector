package transport

import (
	"errors"
	"net/http"
	"sync"
	"testing"
)

// The envelope refusal is not an endpoint verdict.
//
// poolDoer refuses a dial when the request's exchange envelope will not fund
// it. That refusal is a fact about THIS PROXY's budget: nothing was dialed, no
// member was reached, and no endpoint exists to blame. The handler reads a
// refusal on the first dial of an Execute as "the pool could not use any
// member" — egress_exhausted / no_eligible_endpoint — and that rewrite is
// correct only because of one condition inside the pool:
//
//	if attempts == 0 { ... return the zero-dial sentinel ... }
//
// The condition is the entire load-bearing link. Drop it and a budget refusal
// is reported as though the pool had found every member ineligible, blaming
// endpoints for a dial this proxy declined to make.
//
// The tests below drive the REAL poolDoer, not a fake that repeats the
// condition. A fake carrying its own copy of `attempts == 0` passes unchanged
// when pool.go's copy is deleted, which is the shape of test that makes a
// guard look covered while leaving it uncovered. Here the only thing under test
// is pool.go's own branch.

// spentBudget refuses every acquisition, and counts how many times it was
// asked. The count matters: a pool that refused the envelope without asking
// would prove nothing.
type spentBudget struct {
	mu    sync.Mutex
	asked int
}

func (b *spentBudget) AcquireExchange() Exchange {
	b.mu.Lock()
	b.asked++
	b.mu.Unlock()
	return Exchange{Granted: false}
}

func (b *spentBudget) count() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.asked
}

// fundingBudget funds exactly the first n acquisitions and refuses the rest,
// so a pool can be walked to a mid-Execute refusal.
type fundingBudget struct {
	mu       sync.Mutex
	asked    int
	granting int
}

func (b *fundingBudget) AcquireExchange() Exchange {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.asked++
	if b.asked <= b.granting {
		return Exchange{Granted: true, Window: unboundedWindow}
	}
	return Exchange{Granted: false}
}

func TestPoolRefusesTheDialWhenTheEnvelopeIsSpent(t *testing.T) {
	// The refusal must reach the handler as a refusal, never as an endpoint
	// error: ErrExhausted would be read as egress_exhausted, and a non-nil
	// error would be a transport failure with a send state nothing earned.
	ep := &stubEndpoint{script: []stubResult{okResult("{}")}}
	pd, _ := newTestPool(poolMembers(Config{}), RoundRobin,
		FallbackPolicy{Enabled: true, MaxAttempts: 3}, HealthPolicy{}, nil, ep)
	budget := &spentBudget{}
	ar := execReq(false, "{}")
	ar.Budget = budget

	resp, info, err := pd.Execute(ar)
	if err != nil {
		t.Fatalf("a spent envelope must refuse rather than report an endpoint failure, got %v", err)
	}
	if errors.Is(err, ErrExhausted) {
		t.Fatal("a budget refusal must never raise the zero-dial exhaustion sentinel")
	}
	if resp != nil {
		t.Fatal("a refused dial must return no response")
	}
	if got := ep.hitCount(); got != 0 {
		t.Fatalf("a refused dial must not reach the member, got %d calls", got)
	}
	if budget.count() == 0 {
		t.Fatal("the pool must claim the envelope before deciding, not skip the claim")
	}
	if info.Attempts != 0 {
		t.Fatalf("a refused dial is not a dialed attempt, got %d", info.Attempts)
	}
	// BudgetExhausted is the flag the handler's own post-walk classification
	// reads. It is the honest report of what happened.
	if !info.BudgetExhausted {
		t.Fatal("a refused exchange must report the budget as exhausted")
	}
	// The endpoint vocabulary is the thing that must stay empty: a refusal
	// names no member, so it can have no failures to report.
	if len(info.Failures) != 0 {
		t.Fatalf("a refused dial blamed %d endpoints, want none", len(info.Failures))
	}
	if info.Kind != "" || info.Target != "" {
		t.Fatalf("a refused dial must report no endpoint, got kind=%q target=%q", info.Kind, info.Target)
	}
}

// The mirror of the case above: once a member HAS been dialed, a later
// refusal on a fallback must not erase that member's failure. The pool that
// could not use every member is a different fact from the pool that dialed
// one, could not dial the next, and is out of budget.
func TestPoolRefusalAfterADialKeepsTheEndpointFailureInForce(t *testing.T) {
	first := &stubEndpoint{script: []stubResult{{err: errors.New("connection refused")}}}
	second := &stubEndpoint{script: []stubResult{okResult("{}")}}
	pd, _ := newTestPool(poolMembers(Config{}, Config{}), RoundRobin,
		FallbackPolicy{Enabled: true, MaxAttempts: 2}, HealthPolicy{}, nil, first, second)
	// Fund the first dial only; the fallback's claim is refused.
	budget := &fundingBudget{granting: 1}
	ar := execReq(false, "{}")
	ar.Budget = budget

	resp, _, err := pd.Execute(ar)
	if resp != nil {
		t.Fatal("no answer is possible when the fallback is refused")
	}
	if err == nil {
		t.Fatal("a dialed-and-failed endpoint must stay the reported failure")
	}
	if errors.Is(err, ErrExhausted) {
		t.Fatal("a refusal after a real dial must not collapse into the exhaustion sentinel")
	}
	if got := first.hitCount(); got != 1 {
		t.Fatalf("the funded member must have been dialed once, got %d", got)
	}
	if got := second.hitCount(); got != 0 {
		t.Fatalf("the refused fallback must not reach its member, got %d", got)
	}
}

// The refused exchange must still give the permit back. The permit was taken
// for a dial that never happens, so keeping it would shrink a capped member's
// capacity for every refusal — an envelope that runs out would quietly
// saturate the pool it refused to dial.
func TestPoolRefusalReturnsTheMemberPermit(t *testing.T) {
	ep := &stubEndpoint{script: []stubResult{okResult("{}")}}
	// A member capped at one in-flight request is fully drained by the first
	// refusal if the permit is not handed back.
	members := poolMembers(Config{})
	members[0].MaxConcurrency = 1
	pd, st := newTestPool(members, RoundRobin,
		FallbackPolicy{Enabled: true, MaxAttempts: 1}, HealthPolicy{}, nil, ep)
	budget := &spentBudget{}
	ar := execReq(false, "{}")
	ar.Budget = budget

	for i := 0; i < 3; i++ {
		if _, _, err := pd.Execute(ar); err != nil {
			t.Fatalf("refusal %d reported an endpoint failure: %v", i, err)
		}
	}
	st.members[0].lim.mu.Lock()
	held := st.members[0].lim.cur
	st.members[0].lim.mu.Unlock()
	if held != 0 {
		t.Fatalf("every refused dial must return its permit, %d still held", held)
	}
}

// The state lease is taken at entry and must be dropped on the refusal path
// too. A state that never returns to zero can never satisfy the registry's
// retire condition, so the generation it belongs to is never torn down.
func TestPoolRefusalReleasesTheStateLease(t *testing.T) {
	ep := &stubEndpoint{script: []stubResult{okResult("{}")}}
	pd, st := newTestPool(poolMembers(Config{}), RoundRobin,
		FallbackPolicy{Enabled: true, MaxAttempts: 1}, HealthPolicy{}, nil, ep)
	budget := &spentBudget{}
	ar := execReq(false, "{}")
	ar.Budget = budget

	if _, _, err := pd.Execute(ar); err != nil {
		t.Fatalf("a refusal reported an endpoint failure: %v", err)
	}
	st.mu.Lock()
	leases := st.leases
	st.mu.Unlock()
	if leases != 0 {
		t.Fatalf("a refused Execute must drop the lease it took, %d outstanding", leases)
	}
}

// A pool that never meters passes no budget, and that is the same code path
// with the claim removed: the dial proceeds. This pins the other branch of the
// same if, so a change to the refusal handling cannot make an unmetered caller
// refuse.
func TestPoolWithoutABudgetDialsAsUsual(t *testing.T) {
	ep := &stubEndpoint{script: []stubResult{okResult(`{"ok":true}`)}}
	pd, _ := newTestPool(poolMembers(Config{}), RoundRobin,
		FallbackPolicy{Enabled: true, MaxAttempts: 1}, HealthPolicy{}, nil, ep)

	resp, info, err := pd.Execute(execReq(false, "{}"))
	if err != nil {
		t.Fatalf("an unmetered caller must not be refused: %v", err)
	}
	if resp == nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("an unmetered caller must reach its member, got %v", resp)
	}
	if info.Attempts != 1 {
		t.Fatalf("one member dialed must count as one attempt, got %d", info.Attempts)
	}
	if info.BudgetExhausted {
		t.Fatal("an unmetered caller must not report an exhausted envelope")
	}
}
