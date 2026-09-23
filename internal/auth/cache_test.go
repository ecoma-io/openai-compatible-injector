package auth

import (
	"encoding/hex"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeClock steps the cache's notion of time so TTL boundaries are
// deterministic instead of sleep-raced. The counter is atomic because the
// concurrency test advances the clock from the same goroutines that read it.
type fakeClock struct{ nanos atomic.Int64 }

func (c *fakeClock) Now() time.Time { return time.Unix(0, c.nanos.Load()) }

func (c *fakeClock) advance(d time.Duration) { c.nanos.Add(int64(d)) }

func newTestCache(capacity int) (*decisionCache, *fakeClock) {
	clock := &fakeClock{}
	clock.nanos.Store(time.Unix(1_700_000_000, 0).UnixNano())
	return newDecisionCache(capacity, clock.Now), clock
}

func hashHexOf(token string) string {
	return hex.EncodeToString(HashToken(token))
}

func TestCachePositiveTTLBounds(t *testing.T) {
	cache, clock := newTestCache(16)
	const token = "t-ok"
	p := Principal{PartnerID: "acme", KeyID: "pak_x"}

	cache.put(hashHexOf(token), p, ReasonOK)

	if principal, reason, ok := cache.get(hashHexOf(token)); !ok || reason != ReasonOK || principal != p {
		t.Fatalf("get = %+v/%v/%v, want the cached principal", principal, reason, ok)
	}

	// The revocation bound: one microsecond before the TTL the decision is
	// still live; at the TTL it is gone.
	clock.advance(positiveTTL - time.Millisecond)
	if _, _, ok := cache.get(hashHexOf(token)); !ok {
		t.Fatal("positive entry expired before its TTL")
	}
	clock.advance(time.Millisecond)
	if _, _, ok := cache.get(hashHexOf(token)); ok {
		t.Fatal("positive entry outlived its TTL — that would be a revocation bypass")
	}
}

func TestCacheNegativeTTLIsShort(t *testing.T) {
	cache, clock := newTestCache(16)
	const token = "t-unknown"
	cache.put(hashHexOf(token), Principal{}, ReasonUnknown)

	if _, _, ok := cache.get(hashHexOf(token)); !ok {
		t.Fatal("negative entry not cached at all")
	}
	clock.advance(negativeTTL)
	if _, _, ok := cache.get(hashHexOf(token)); ok {
		t.Fatal("negative entry outlived its TTL — a newly created key would stay denied past the documented window")
	}
}

func TestCacheRevokedReasonIsCachedNegative(t *testing.T) {
	cache, clock := newTestCache(16)
	cache.put(hashHexOf("t-revoked"), Principal{}, ReasonRevoked)

	if _, reason, ok := cache.get(hashHexOf("t-revoked")); !ok || reason != ReasonRevoked {
		t.Fatalf("get = %v/%v, want cached revoked_key", reason, ok)
	}
	clock.advance(negativeTTL - time.Millisecond)
	if _, _, ok := cache.get(hashHexOf("t-revoked")); !ok {
		t.Fatal("revoked decision expired early")
	}
	clock.advance(time.Millisecond)
	if _, _, ok := cache.get(hashHexOf("t-revoked")); ok {
		t.Fatal("revoked decision outlived the negative TTL")
	}
}

func TestCacheNeverCachesBackendFailures(t *testing.T) {
	cache, _ := newTestCache(16)
	cache.put(hashHexOf("t-backend"), Principal{}, ReasonBackend)
	if _, _, ok := cache.get(hashHexOf("t-backend")); ok {
		t.Fatal("a backend failure was cached — a transient store outage would outlive itself")
	}
}

func TestCacheEvictsLeastRecentlyUsed(t *testing.T) {
	cache, _ := newTestCache(2)

	cache.put(hashHexOf("a"), Principal{PartnerID: "pa"}, ReasonOK)
	cache.put(hashHexOf("b"), Principal{PartnerID: "pb"}, ReasonOK)
	// Touch a: it becomes most recently used, so b is now the LRU victim.
	if _, _, ok := cache.get(hashHexOf("a")); !ok {
		t.Fatal("fresh entry a missing")
	}
	cache.put(hashHexOf("c"), Principal{PartnerID: "pc"}, ReasonOK)

	if _, _, ok := cache.get(hashHexOf("b")); ok {
		t.Fatal("entry b survived eviction — the cache is unbounded")
	}
	for _, k := range []string{"a", "c"} {
		if _, _, ok := cache.get(hashHexOf(k)); !ok {
			t.Fatalf("entry %s missing after evicting a different key", k)
		}
	}
}

func TestCachePutOverwrites(t *testing.T) {
	cache, clock := newTestCache(16)
	key := hashHexOf("t-same")
	cache.put(key, Principal{PartnerID: "before"}, ReasonUnknown)
	// Revoke lands: the same token's decision flips.
	cache.put(key, Principal{}, ReasonRevoked)
	clock.advance(negativeTTL - time.Millisecond)
	if _, reason, ok := cache.get(key); !ok || reason != ReasonRevoked {
		t.Fatalf("get = %v/%v, want the overwritten revoked decision", reason, ok)
	}
}

func TestCacheConcurrentAccess(t *testing.T) {
	// Run under -race: concurrent gets, puts, expiries and evictions on a
	// shared cache must be exactly as boring as the mutex promises.
	cache, clock := newTestCache(64)
	var wg sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				key := hashHexOf(fmt.Sprintf("worker-%d-token-%d", worker, i%32))
				switch i % 3 {
				case 0:
					cache.put(key, Principal{PartnerID: fmt.Sprintf("p%d", worker), KeyID: "pak_x"}, ReasonOK)
				case 1:
					cache.put(key, Principal{}, ReasonUnknown)
				default:
					if principal, _, ok := cache.get(key); ok && principal.PartnerID == "" && principal.KeyID == "" {
						// A hit carrying no identity is only legal for a
						// negative decision; a positive one always names its
						// key. Nothing to assert here beyond race detection —
						// the read must simply not corrupt memory.
						_ = principal
					}
				}
				if i%97 == 0 {
					clock.advance(time.Second)
				}
			}
		}(worker)
	}
	wg.Wait()
}
