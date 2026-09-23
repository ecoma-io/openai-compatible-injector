package transport

import (
	"sync"
	"testing"
	"time"
)

// The remove→re-add retirement regressions. The defect class: a pool's
// deferred retirement (fired by a lease's last release) ran after a later
// reload re-introduced the same pool identity, and tore the NEW state's
// entries out of the registry maps — or the re-add handed back the retired
// state itself. Both halves are pinned here at the instance level, not by
// identity string: two states of one identity are different generations.

// TestRegistryReaddAfterDeferredRetireIsFresh runs the exact reload
// sequence: activate A, hold a lease, reload removing A, reload re-adding
// A, release the old request. The old release must not touch the new
// state, and the new state must never have been the retired one.
func TestRegistryReaddAfterDeferredRetireIsFresh(t *testing.T) {
	r := NewRegistry()
	cfg := poolCfg(RoundRobin, 3, 0)
	id := cfg.Pool.Identity()
	key := cfg.Key()

	old := r.Doer(cfg).(*poolDoer)
	st1 := old.st
	st1.begin() // a request is executing on the old generation

	r.Retain(nil) // reload drops the pool: teardown deferred to the lease
	if !st1.retired {
		t.Fatal("dropped pool was not marked retired")
	}

	// Move the old state's scheduler before it dies, so a hand-back of st1
	// would be visible as cursor leakage into the new generation.
	r.mu.Lock()
	st1.schedMu.Lock()
	st1.cursor = 1
	st1.schedMu.Unlock()
	r.mu.Unlock()

	fresh := r.Doer(cfg).(*poolDoer) // the reload re-adds A
	st2 := fresh.st
	if st2 == st1 {
		t.Fatal("re-add handed back the retired state")
	}
	if st2.retired {
		t.Fatal("the fresh state was born retired")
	}

	// Give the new generation its own observable runtime state (registry
	// tests drive the scheduler directly — member clients here are real
	// network clients, and no test traffic may leave the process).
	r.mu.Lock()
	st2.schedMu.Lock()
	st2.cursor = 1
	st2.schedMu.Unlock()
	r.mu.Unlock()
	cursor := 1

	st1.end() // the old request finishes; the deferred retirement fires

	// The old retirement must have been a no-op against the new generation.
	r.mu.Lock()
	cur, live := r.pools[id]
	entry, doerLive := r.doers[key]
	r.mu.Unlock()
	if !live || cur != st2 {
		t.Fatalf("old retirement evicted the new state: pools entry = %v", cur)
	}
	if !doerLive {
		t.Fatal("old retirement evicted the new doer entry")
	}
	if pd, ok := entry.(*poolDoer); !ok || pd.st != st2 {
		t.Fatalf("doer entry does not bind the new state: %+v", entry)
	}
	if st2.retired {
		t.Error("the new state was retired by the old generation's callback")
	}
	// The new state's scheduler position survived the old release.
	r.mu.Lock()
	st2.schedMu.Lock()
	after := st2.cursor
	st2.schedMu.Unlock()
	r.mu.Unlock()
	if after != cursor {
		t.Errorf("new state cursor moved without traffic: %d → %d", cursor, after)
	}
}

// TestRegistryReaddWithoutLeaseTearsOldDownImmediately pins the fast path:
// with no lease held, the drop tears the old generation down inline and the
// re-add starts fresh — and a second Retain(nil) is a clean no-op.
func TestRegistryReaddWithoutLeaseTearsOldDownImmediately(t *testing.T) {
	r := NewRegistry()
	cfg := poolCfg(RoundRobin, 3, 0)
	id := cfg.Pool.Identity()

	first := r.Doer(cfg).(*poolDoer)
	r.Retain(nil)
	if _, ok := r.pools[id]; ok {
		t.Fatal("idle pool survived the drop")
	}

	second := r.Doer(cfg).(*poolDoer)
	if second.st == first.st {
		t.Fatal("re-add reused the torn-down state")
	}

	r.Retain(nil) // drop again: no lease, no defer
	r.Retain(nil) // ...and again: nothing left to tear down
	if len(r.pools) != 0 || len(r.doers) != 0 {
		t.Fatalf("maps not empty after two drops: pools=%d doers=%d", len(r.pools), len(r.doers))
	}
}

// TestRegistryConcurrentOldLeasesReleaseAroundReadd hammers the exact race:
// leases from the old generation release while another goroutine re-adds
// the identity, repeatedly. Every interleaving must leave exactly one live,
// unretired state under the identity at the end.
func TestRegistryConcurrentOldLeasesReleaseAroundReadd(t *testing.T) {
	for round := 0; round < 50; round++ {
		r := NewRegistry()
		cfg := poolCfg(RoundRobin, 3, 0)
		id := cfg.Pool.Identity()
		key := cfg.Key()

		st1 := r.Doer(cfg).(*poolDoer).st
		var wg sync.WaitGroup
		for i := 0; i < 4; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				st1.begin()
				// Give the reload a chance to land mid-lease.
				time.Sleep(time.Duration(round%3) * time.Millisecond)
				st1.end()
			}()
		}
		// The reload drops and re-adds while leases are live.
		time.Sleep(time.Millisecond)
		r.Retain(nil)
		readded := r.Doer(cfg).(*poolDoer)
		wg.Wait()

		r.mu.Lock()
		cur, ok := r.pools[id]
		entry := r.doers[key]
		r.mu.Unlock()
		if !ok || cur != readded.st {
			t.Fatalf("round %d: live state is not the re-added generation (ok=%v)", round, ok)
		}
		if readded.st.retired {
			t.Fatalf("round %d: the live state is retired", round)
		}
		if pd, isPd := entry.(*poolDoer); !isPd || pd.st != readded.st {
			t.Fatalf("round %d: doer entry does not bind the live state", round)
		}
		if cur.leases != 0 {
			t.Fatalf("round %d: live state inherited leases: %d", round, cur.leases)
		}
	}
}

// TestRegistryRetainKeepsWarmStateAcrossUnchangedReload pins the direction
// the fixes must not break: an unchanged pool in the Retain set keeps its
// doer, state, and scheduler position across the reload.
func TestRegistryRetainKeepsWarmStateAcrossUnchangedReload(t *testing.T) {
	r := NewRegistry()
	cfg := poolCfg(RoundRobin, 3, 0)

	pd := r.Doer(cfg).(*poolDoer)
	// Move the scheduler one step so warmth is observable.
	r.mu.Lock()
	pd.st.schedMu.Lock()
	pd.st.cursor = 1
	pd.st.schedMu.Unlock()
	r.mu.Unlock()
	r.Retain([]Config{cfg})

	again := r.Doer(cfg).(*poolDoer)
	if again != pd || again.st != pd.st {
		t.Fatal("unchanged reload lost the warm pool doer/state")
	}
	if got := again.st.cursor; got != 1 {
		t.Errorf("warm state cursor = %d, want the pre-reload position 1", got)
	}
}
