package credential

import (
	"sync"
	"testing"
	"time"
)

var base = time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

func threeKeys() Spec {
	return Spec{
		Header:   "Authorization",
		Prefix:   "Bearer ",
		Strategy: StrategyRoundRobin,
		Keys: []Key{
			{ID: "k1", Value: "s1"},
			{ID: "k2", Value: "s2"},
			{ID: "k3", Value: "s3"},
		},
	}
}

func ids(t *testing.T, pool *Pool, now time.Time, n int) []string {
	t.Helper()
	out := make([]string, 0, n)
	for i := 0; i < n; i++ {
		k, ok := pool.Acquire(now, "")
		if !ok {
			t.Fatalf("Acquire %d: not ok, want a ready key", i)
		}
		out = append(out, k.ID)
	}
	return out
}

func TestAcquireOneKey(t *testing.T) {
	p := NewPool(Spec{
		Header: "X-API-Key", Strategy: StrategyRoundRobin,
		Keys: []Key{{ID: "only", Value: "v"}},
	})
	for i := 0; i < 5; i++ {
		k, ok := p.Acquire(base, "")
		if !ok || k.ID != "only" || k.Value != "v" {
			t.Fatalf("Acquire %d = %q,%q,%v", i, k.ID, k.Value, ok)
		}
	}
}

func TestAcquireRoundRobinOrder(t *testing.T) {
	p := NewPool(threeKeys())
	got := ids(t, p, base, 7) // k1 k2 k3 k1 k2 k3 k1
	want := []string{"k1", "k2", "k3", "k1", "k2", "k3", "k1"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("rotation[%d] = %q, want %q (full: %v)", i, got[i], want[i], got)
		}
	}
}

func TestAcquirePreferredReadyKeepsKeyWithoutMovingCursor(t *testing.T) {
	p := NewPool(threeKeys())
	k, ok := p.Acquire(base, "k2")
	if !ok || k.ID != "k2" {
		t.Fatalf("preferred ready Acquire = %q,%v, want k2,true", k.ID, ok)
	}
	// A preferred hit is the same logical attempt continuing, not a rotation:
	// the cursor stays where it was, so the next unbiased pick is still k1.
	if got := ids(t, p, base, 1); got[0] != "k1" {
		t.Fatalf("post-preferred rotation = %q, want k1", got[0])
	}
}

func TestAcquirePreferredCoolingSkipsToNextReady(t *testing.T) {
	p := NewPool(threeKeys())
	p.MarkRateLimited(base, "k1", 30*time.Second)
	k, ok := p.Acquire(base, "k1")
	if !ok || k.ID != "k2" {
		t.Fatalf("preferred-cooling Acquire = %q,%v, want k2,true", k.ID, ok)
	}
}

func TestAcquirePreferredUnknownFallsThroughToRotation(t *testing.T) {
	p := NewPool(threeKeys())
	k, ok := p.Acquire(base, "no-such-key")
	if !ok || k.ID != "k1" {
		t.Fatalf("unknown preferred Acquire = %q,%v, want k1,true", k.ID, ok)
	}
}

func TestMarkRateLimitedRotatesRepeated429s(t *testing.T) {
	p := NewPool(threeKeys())
	now := base

	k, _ := p.Acquire(now, "") // k1
	p.MarkRateLimited(now, k.ID, time.Minute)
	now = now.Add(time.Second)
	k, _ = p.Acquire(now, k.ID) // k1 cooling -> k2
	if k.ID != "k2" {
		t.Fatalf("second pick = %q, want k2", k.ID)
	}
	p.MarkRateLimited(now, k.ID, time.Minute)
	now = now.Add(time.Second)
	k, _ = p.Acquire(now, k.ID) // k2 cooling -> k3
	if k.ID != "k3" {
		t.Fatalf("third pick = %q, want k3", k.ID)
	}
}

func TestAllKeysCooling(t *testing.T) {
	p := NewPool(threeKeys())
	now := base
	for _, id := range []string{"k1", "k2", "k3"} {
		k, ok := p.Acquire(now, "")
		if !ok {
			t.Fatalf("acquire %d unexpectedly failed", 0)
		}
		if k.ID != id {
			t.Fatalf("pick = %q, want %q", k.ID, id)
		}
		p.MarkRateLimited(now, id, time.Minute)
	}
	if k, ok := p.Acquire(now, ""); ok {
		t.Fatalf("acquire with everything cooling = %q, want !ok", k.ID)
	}
	got, ok := p.NextReady(now)
	if !ok || !got.Equal(base.Add(time.Minute)) {
		t.Fatalf("NextReady = %v,%v, want %v,true", got, ok, base.Add(time.Minute))
	}
}

func TestCooldownExpiry(t *testing.T) {
	p := NewPool(threeKeys())
	p.MarkRateLimited(base, "k1", 30*time.Second)
	p.MarkRateLimited(base, "k2", 10*time.Second)

	// Before any expiry: k3 only.
	if k, ok := p.Acquire(base.Add(5*time.Second), ""); !ok || k.ID != "k3" {
		t.Fatalf("Acquire(5s) = %q,%v, want k3,true", k.ID, ok)
	}
	// After k2's cooldown: the scan resumes from the cursor and finds k2.
	if k, ok := p.Acquire(base.Add(11*time.Second), ""); !ok || k.ID != "k2" {
		t.Fatalf("Acquire(11s) = %q,%v, want k2,true", k.ID, ok)
	}
	// After k1's cooldown: k1 is back in rotation.
	if k, ok := p.Acquire(base.Add(31*time.Second), "k1"); !ok || k.ID != "k1" {
		t.Fatalf("Acquire(31s, preferred k1) = %q,%v, want k1,true", k.ID, ok)
	}
}

func TestNextReadyNoneCooling(t *testing.T) {
	p := NewPool(threeKeys())
	if _, ok := p.NextReady(base); ok {
		t.Fatal("NextReady with nothing cooling = ok, want !ok")
	}
	// An expired cooldown is not cooling.
	p.MarkRateLimited(base.Add(-time.Hour), "k1", time.Second)
	if _, ok := p.NextReady(base); ok {
		t.Fatal("NextReady with only expired cooldowns = ok, want !ok")
	}
}

func TestNextReadyEarliestOfSeveral(t *testing.T) {
	p := NewPool(threeKeys())
	p.MarkRateLimited(base, "k1", time.Minute)
	p.MarkRateLimited(base, "k2", 15*time.Second)
	got, ok := p.NextReady(base)
	if !ok || !got.Equal(base.Add(15*time.Second)) {
		t.Fatalf("NextReady = %v,%v, want +15s,true", got, ok)
	}
}

func TestMarkRateLimitedEdgeInputs(t *testing.T) {
	p := NewPool(threeKeys())
	// Non-positive cooldowns are no-ops.
	p.MarkRateLimited(base, "k1", 0)
	p.MarkRateLimited(base, "k1", -time.Second)
	if k, ok := p.Acquire(base, "k1"); !ok || k.ID != "k1" {
		t.Fatalf("zero/negative cooldown marked k1: %q,%v", k.ID, ok)
	}
	// Unknown ids are no-ops and do not corrupt rotation.
	p.MarkRateLimited(base, "ghost", time.Hour)
	if _, ok := p.NextReady(base); ok {
		t.Fatal("unknown-id mark created state")
	}
	// Re-marking shortens nothing: a later deadline replaces, an earlier
	// deadline still replaces too (the last mark wins — the caller observes
	// the freshest upstream answer).
	p.MarkRateLimited(base, "k1", time.Minute)
	p.MarkRateLimited(base.Add(time.Second), "k1", time.Minute)
	got, ok := p.NextReady(base.Add(time.Second))
	if !ok || !got.Equal(base.Add(time.Second+time.Minute)) {
		t.Fatalf("re-mark NextReady = %v,%v", got, ok)
	}
}

func TestNewPoolCopiesSpec(t *testing.T) {
	spec := threeKeys()
	p := NewPool(spec)
	spec.Keys[0].ID = "mutated"
	if k, _ := p.Acquire(base, ""); k.ID != "k1" {
		t.Fatalf("pool aliased the caller's spec: first key = %q", k.ID)
	}
	if got := p.Spec().Keys[0].ID; got != "k1" {
		t.Fatalf("Spec() reflects the caller's mutation: %q", got)
	}
}

func TestNewPoolRejectsEmptyKeyList(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("NewPool with no keys did not panic")
		}
	}()
	NewPool(Spec{Header: "Authorization", Strategy: StrategyRoundRobin})
}

func TestConcurrentAcquireAndMark(t *testing.T) {
	// 256 keys: rotation state under many concurrent writers must stay
	// consistent — every handed-out key comes from the spec, cooldown
	// bookkeeping never deadlocks, and the pool keeps working afterwards.
	spec := threeKeys()
	spec.Keys = make([]Key, 0, 256)
	for i := 0; i < 256; i++ {
		spec.Keys = append(spec.Keys, Key{ID: "k" + itoa(i), Value: "v" + itoa(i)})
	}
	if err := ValidateSpec(spec); err != nil {
		t.Fatalf("256-key spec rejected: %v", err)
	}
	p := NewPool(spec)

	var wg sync.WaitGroup
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			now := base.Add(time.Duration(g) * time.Millisecond)
			for i := 0; i < 500; i++ {
				k, ok := p.Acquire(now, "")
				if !ok {
					// Everything cooling: wait for the reported deadline and
					// go again — the pool must recover by itself.
					until, ok := p.NextReady(now)
					if !ok {
						t.Error("acquire failed with nothing cooling")
						return
					}
					now = until.Add(time.Nanosecond)
					continue
				}
				if k.Value != "v"+k.ID[1:] {
					t.Errorf("key/value mismatch: %+v", k)
					return
				}
				if i%7 == 0 {
					p.MarkRateLimited(now, k.ID, time.Millisecond)
				}
				if i%11 == 0 {
					p.NextReady(now)
				}
				now = now.Add(time.Millisecond)
			}
		}(g)
	}
	wg.Wait()

	// After the storm every cooldown expires and rotation resumes.
	if k, ok := p.Acquire(base.Add(time.Hour), ""); !ok {
		t.Fatalf("pool dead after concurrency: %q,%v", k.ID, ok)
	}
}
