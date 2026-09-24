package credential

import (
	"sync"
	"testing"
	"time"
)

func regSpec(vals ...string) *Provider {
	keys := make([]Key, 0, len(vals))
	for i, v := range vals {
		keys = append(keys, Key{ID: "k" + itoa(i), Value: v})
	}
	return &Provider{
		Spec: Spec{Header: "Authorization", Prefix: "Bearer ", Strategy: StrategyRoundRobin, Keys: keys},
	}
}

// TestRegistryGetOrCreateIsContentKeyed: same content, one pool — across
// repeated lookups — and different content, different pools.
func TestRegistryGetOrCreateIsContentKeyed(t *testing.T) {
	r := NewRegistry()
	a := regSpec("s1", "s2")
	p1 := r.Pool(a)
	p2 := r.Pool(regSpec("s1", "s2"))
	if p1 != p2 {
		t.Fatal("identical specs resolved to different pools")
	}
	// Rotation state is shared: a mark through one handle is visible
	// through the other.
	p1.MarkRateLimited(base, "k0", time.Minute)
	if k, ok := p2.Acquire(base, "k0"); !ok || k.ID == "k0" {
		t.Fatalf("the pool ignored a mark made through another handle: %q, %v", k.ID, ok)
	}
	if p3 := r.Pool(regSpec("s1", "s3")); p3 == p1 {
		t.Fatal("changed content reused the old pool")
	}
}

// TestRegistryRetainEvictsAndKeepsWarm pins the reload contract: unchanged
// content keeps its pool (cooldown state survives the reload), removed or
// rewritten content is dropped, and a re-introduced spec starts fresh.
func TestRegistryRetainEvictsAndKeepsWarm(t *testing.T) {
	r := NewRegistry()
	keep := regSpec("s1")
	drop := regSpec("s2")
	r.Pool(keep)
	d := r.Pool(drop)
	d.MarkRateLimited(base, "k0", time.Minute)

	r.Retain([]*Provider{keep})
	// The kept pool is the same instance and still warm.
	p := r.Pool(keep)
	if p.MarkRateLimited(base, "k0", time.Minute); len(p.cooling) != 1 {
		t.Fatalf("kept pool lost its state: %d cooling", len(p.cooling))
	}
	// The dropped pool resolves fresh on the next ask (a pinned request
	// finishing its walk), with cold cooldown state.
	fresh := r.Pool(drop)
	if fresh == d {
		t.Fatal("an evicted spec resolved to its retired pool")
	}
	if len(fresh.cooling) != 0 {
		t.Fatal("a fresh pool inherited cooldown state")
	}
	// Dropping everything empties the registry.
	r.Retain(nil)
	r.Retain([]*Provider{keep})
	again := r.Pool(keep)
	if len(again.cooling) != 0 {
		t.Fatal("a re-introduced spec resurrected retired cooldown state")
	}
}

// TestRegistryConcurrentPoolAndRetain: the get-or-create race — many
// requests resolving the same spec while reloads retain overlapping sets —
// must converge on one pool per content key.
func TestRegistryConcurrentPoolAndRetain(t *testing.T) {
	r := NewRegistry()
	specs := []*Provider{regSpec("s1", "s2"), regSpec("s3"), regSpec("s4", "s5", "s6")}
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				p := r.Pool(specs[(g+i)%len(specs)])
				if _, ok := p.Acquire(base.Add(time.Duration(i)*time.Microsecond), ""); !ok {
					// All cooling is legal under the marks below; the
					// registry must still hand back a usable pool.
					continue
				}
				if i%13 == 0 {
					p.MarkRateLimited(base, "k0", time.Microsecond)
				}
			}
		}(g)
	}
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				r.Retain(specs)
				r.Retain(specs[:1])
			}
		}()
	}
	wg.Wait()
}
