package credential

import (
	"strconv"
	"strings"
	"testing"
	"time"
)

// hroSpec builds a valid credential spec with n keys.
func hroSpec(n int) Spec {
	keys := make([]Key, n)
	for i := range keys {
		keys[i] = Key{ID: "key-" + strconv.Itoa(i), Value: "sk-" + strings.Repeat("x", 40) + strconv.Itoa(i)}
	}
	return Spec{Header: "Authorization", Prefix: "Bearer ", Strategy: StrategyRoundRobin, Keys: keys}
}

func hroProvider(n int) *Provider {
	return &Provider{
		Identity:  "openai",
		Spec:      hroSpec(n),
		RateLimit: RateLimit{Cooldown: 30 * time.Second, MaxCooldown: 5 * time.Minute},
	}
}

// BenchmarkHroPoolKey measures the pool identity digest. In the shipped path
// it is computed per candidate per request (handler.go calls
// h.creds.Pool(cand.Cred) -> p.PoolKey() once per candidate entry), so it is
// HOT, and it hashes every key VALUE.
func BenchmarkHroPoolKey(b *testing.B) {
	for _, n := range []int{1, 4, 16, 64, 256} {
		b.Run("keys"+strconv.Itoa(n), func(b *testing.B) {
			p := hroProvider(n)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = p.PoolKey()
			}
		})
	}
}

// BenchmarkHroSpecContentKey measures the spec content digest alone, which
// PoolKey embeds.
func BenchmarkHroSpecContentKey(b *testing.B) {
	for _, n := range []int{1, 16, 256} {
		b.Run("keys"+strconv.Itoa(n), func(b *testing.B) {
			s := hroSpec(n)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = s.ContentKey()
			}
		})
	}
}

// BenchmarkHroRegistryPool measures the registry get-or-create the handler
// performs once per candidate: the PoolKey digest plus a mutex-guarded map
// lookup. This is the per-candidate credential cost, serialized.
func BenchmarkHroRegistryPool(b *testing.B) {
	for _, n := range []int{1, 16} {
		b.Run("keys"+strconv.Itoa(n), func(b *testing.B) {
			r := NewRegistry()
			p := hroProvider(n)
			// Warm the map so the benchmark measures the steady-state
			// lookup, not the create.
			r.Pool(p)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if r.Pool(p) == nil {
					b.Fatal("nil pool")
				}
			}
		})
	}
}

// BenchmarkHroPoolAcquire measures the rotation acquire: a mutex over the
// whole pool, a linear walk of the keys, and one map lookup per key examined.
// With no cooling keys it is one map lookup on an empty map.
func BenchmarkHroPoolAcquire(b *testing.B) {
	for _, n := range []int{1, 4, 16, 64, 256} {
		b.Run("keys"+strconv.Itoa(n), func(b *testing.B) {
			p := NewPool(hroSpec(n))
			now := time.Unix(1_700_000_000, 0)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, ok := p.Acquire(now, ""); !ok {
					b.Fatal("acquire failed")
				}
			}
		})
	}
}

// BenchmarkHroPoolAcquirePreferred measures the sticky-preference acquire a
// same-candidate retry makes: a linear scan for the preferred id, then the
// readiness map lookup.
func BenchmarkHroPoolAcquirePreferred(b *testing.B) {
	for _, n := range []int{1, 16, 256} {
		b.Run("keys"+strconv.Itoa(n), func(b *testing.B) {
			p := NewPool(hroSpec(n))
			now := time.Unix(1_700_000_000, 0)
			// Prefer the LAST key: the worst case for the linear scan.
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, ok := p.Acquire(now, "key-"+strconv.Itoa(n-1)); !ok {
					b.Fatal("acquire failed")
				}
			}
		})
	}
}

// BenchmarkHroPoolMarkRateLimited measures the 429 cooldown write, which
// happens on every path that observed an upstream 429.
func BenchmarkHroPoolMarkRateLimited(b *testing.B) {
	for _, n := range []int{16, 256} {
		b.Run("keys"+strconv.Itoa(n), func(b *testing.B) {
			p := NewPool(hroSpec(n))
			now := time.Unix(1_700_000_000, 0)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				p.MarkRateLimited(now, "key-0", 30*time.Second)
			}
		})
	}
}

// BenchmarkHroPoolAcquireParallel measures the pool mutex under contention:
// every candidate attempt on a candidate with a credential pool takes this
// lock, so it is the credential layer's per-request serialization point.
func BenchmarkHroPoolAcquireParallel(b *testing.B) {
	for _, n := range []int{1, 16} {
		b.Run("keys"+strconv.Itoa(n), func(b *testing.B) {
			p := NewPool(hroSpec(n))
			now := time.Unix(1_700_000_000, 0)
			for _, par := range []int{1, 2, 4, 8, 16, 32} {
				b.Run("par"+strconv.Itoa(par), func(b *testing.B) {
					b.ReportAllocs()
					b.ResetTimer()
					b.RunParallel(func(pb *testing.PB) {
						for pb.Next() {
							if _, ok := p.Acquire(now, ""); !ok {
								b.Fatal("acquire failed")
							}
						}
					})
				})
			}
		})
	}
}

// BenchmarkHroRegistryPoolParallel measures the registry's own mutex: every
// request with a credentialed candidate takes it once per candidate.
func BenchmarkHroRegistryPoolParallel(b *testing.B) {
	for _, par := range []int{1, 2, 4, 8, 16, 32} {
		b.Run("par"+strconv.Itoa(par), func(b *testing.B) {
			r := NewRegistry()
			p := hroProvider(4)
			r.Pool(p)
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					if r.Pool(p) == nil {
						b.Fatal("nil pool")
					}
				}
			})
		})
	}
}
