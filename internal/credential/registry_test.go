package credential

import (
	"sync"
	"testing"
	"time"
)

// twoKeys builds a valid spec over the given secret values, ids k0..kn.
func twoKeys(vals ...string) Spec {
	keys := make([]Key, 0, len(vals))
	for i, v := range vals {
		keys = append(keys, Key{ID: "k" + itoa(i), Value: v})
	}
	return Spec{Header: "Authorization", Prefix: "Bearer ", Strategy: StrategyRoundRobin, Keys: keys}
}

// regProvider builds a Provider over twoKeys(vals...) with the default
// loader-shaped rate-limit policy filled in.
func regProvider(id string, vals ...string) *Provider {
	return &Provider{
		Identity:  id,
		Spec:      twoKeys(vals...),
		RateLimit: RateLimit{Cooldown: 2 * time.Second, MaxCooldown: time.Minute},
	}
}

// TestProviderPoolKeyCoversEveryAxis pins the pool identity's contract: it
// is stable for equal providers and differs when ANY axis that shapes
// rotation state differs — the provider identity, the credential content,
// and either rate-limit value. Identity is the axis that makes two
// providers with byte-identical credentials two rotation domains instead
// of one shared cursor.
func TestProviderPoolKeyCoversEveryAxis(t *testing.T) {
	base := regProvider("pa", "s1")
	same := regProvider("pa", "s1")
	if base.PoolKey() != same.PoolKey() {
		t.Fatal("equal providers hashed to different pool keys")
	}
	if base.PoolKey() == regProvider("pb", "s1").PoolKey() {
		t.Fatal("the provider identity is not part of the pool key")
	}
	if base.PoolKey() == regProvider("pa", "s2").PoolKey() {
		t.Fatal("the credential content is not part of the pool key")
	}
	otherCooldown := regProvider("pa", "s1")
	otherCooldown.RateLimit.Cooldown = 3 * time.Second
	if base.PoolKey() == otherCooldown.PoolKey() {
		t.Fatal("the rate-limit cooldown is not part of the pool key")
	}
	otherMax := regProvider("pa", "s1")
	otherMax.RateLimit.MaxCooldown = 2 * time.Minute
	if base.PoolKey() == otherMax.PoolKey() {
		t.Fatal("the rate-limit max-cooldown is not part of the pool key")
	}
}

// TestRegistryGetOrCreateIsPoolKeyed: the same pool identity resolves to
// one pool — across repeated lookups — and a DIFFERENT provider with
// byte-identical credentials resolves to a different one, with no state
// leaking between the two rotation domains.
func TestRegistryGetOrCreateIsPoolKeyed(t *testing.T) {
	r := NewRegistry()
	pa := regProvider("pa", "s1", "s2")
	p1 := r.Pool(pa)
	p2 := r.Pool(regProvider("pa", "s1", "s2"))
	if p1 != p2 {
		t.Fatal("the same pool identity resolved to different pools")
	}
	// Byte-identical credentials under another provider: another pool.
	pb := r.Pool(regProvider("pb", "s1", "s2"))
	if pb == p1 {
		t.Fatal("byte-identical credentials under two providers shared a pool")
	}
	// pa's cooldown must not cool pb's copy of the same accounts...
	p1.MarkRateLimited(base, "k0", time.Minute)
	if k, ok := pb.Acquire(base, ""); !ok || k.ID != "k0" {
		t.Fatalf("pa's cooldown leaked into pb's pool: %q, %v", k.ID, ok)
	}
	// ...and pa's cursor position must not move pb's: pa sits past k0, pb
	// has never been touched.
	if k, ok := p1.Acquire(base, ""); !ok || k.ID == "k0" {
		t.Errorf("pa's own mark did not cool its key: %q, %v", k.ID, ok)
	}
	if changed := r.Pool(regProvider("pa", "s1", "s3")); changed == p1 {
		t.Fatal("changed content reused the old pool")
	}
}

// TestRegistryRetainEvictsAndKeepsWarm pins the reload contract: an
// unchanged pool identity keeps its pool (cooldown state survives the
// reload), a removed or rewritten identity is dropped, and a
// rate-limit-ONLY change — same provider, same credentials — is a new
// identity whose pool starts cold while an in-flight holder keeps the
// retired instance and its state.
func TestRegistryRetainEvictsAndKeepsWarm(t *testing.T) {
	r := NewRegistry()
	keep := regProvider("pa", "s1")
	drop := regProvider("pb", "s2")
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
		t.Fatal("an evicted identity resolved to its retired pool")
	}
	if len(fresh.cooling) != 0 {
		t.Fatal("a fresh pool inherited cooldown state")
	}

	// Rate-limit-only reload: the provider keeps its name and its keys but
	// states a different cooldown. New requests get a pool sized by the NEW
	// policy; the pinned instance keeps the old one's state.
	warm := r.Pool(keep)
	warm.MarkRateLimited(base, "k0", time.Minute)
	retuned := regProvider("pa", "s1")
	retuned.RateLimit.Cooldown = 5 * time.Second
	got := r.Pool(retuned)
	if got == warm {
		t.Fatal("a rate-limit change reused the pool sized by the old policy")
	}
	if len(got.cooling) != 0 {
		t.Fatal("the retuned pool inherited cooldown state")
	}
	if len(warm.cooling) != 1 {
		t.Fatal("the retired pool lost its pinned state")
	}

	// Dropping everything empties the registry.
	r.Retain(nil)
	r.Retain([]*Provider{keep})
	again := r.Pool(keep)
	if len(again.cooling) != 0 {
		t.Fatal("a re-introduced identity resurrected retired cooldown state")
	}
}

// TestRegistryConcurrentPoolAndRetain: the get-or-create race — many
// requests resolving the same pool identities while reloads retain
// overlapping sets — must converge on one pool per pool key.
func TestRegistryConcurrentPoolAndRetain(t *testing.T) {
	r := NewRegistry()
	providers := []*Provider{
		regProvider("pa", "s1", "s2"),
		regProvider("pb", "s3"),
		regProvider("pc", "s4", "s5", "s6"),
	}
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				p := r.Pool(providers[(g+i)%len(providers)])
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
				r.Retain(providers)
				r.Retain(providers[:1])
			}
		}()
	}
	wg.Wait()
}
