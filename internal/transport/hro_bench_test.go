package transport

import (
	"context"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// Egress pool: the scheduler, health, and concurrency-limit locks every
// pooled request crosses, plus the registry's content-keyed Doer lookup.
// ---------------------------------------------------------------------------

// hroDirect builds the direct (zero-value) transport config.
func hroDirect() Config { return Config{Kind: Direct} }

// hroProxy builds a proxy transport config pointing at a loopback SOCKS5
// endpoint. The URL is never dialled in these benchmarks: the pool's
// scheduler and the registry's key are what is measured.
func hroProxy(i int) Config {
	u, _ := url.Parse("socks5://user" + strconv.Itoa(i) + ":pass@127.0.0.1:" + strconv.Itoa(1080+i))
	return Config{Kind: Proxy, ProxyURL: u}
}

func hroPoolConfig(members int, strategy Strategy, maxConc int, fallback bool) Config {
	ms := make([]Member, members)
	for i := range ms {
		ms[i] = Member{Endpoint: hroDirect(), Streaming: true, Weight: 1, MaxConcurrency: maxConc}
	}
	p := NewPool(ms, strategy,
		FallbackPolicy{Enabled: fallback, MaxAttempts: members},
		HealthPolicy{Enabled: true, FailureThreshold: 2, Cooldown: time.Second})
	return Config{Kind: EgressPool, Pool: p}
}

// hroPoolDoerFor builds the poolDoer a registry would hand out for c, with
// its shared state installed — i.e. exactly the object the handler resolves.
func hroPoolDoerFor(tb testing.TB, c Config) *poolDoer {
	tb.Helper()
	reg := NewRegistry()
	d, ok := reg.Doer(c).(*poolDoer)
	if !ok {
		tb.Fatalf("registry returned %T, want *poolDoer", reg.Doer(c))
	}
	return d
}

// hroAttempt builds a pooled attempt with the body pre-serialized. The
// member clients in these benchmarks are the real direct client, so a live
// loopback upstream is used where a dial is intended.
func hroAttempt(tb testing.TB, u *url.URL, bodySize int) *AttemptRequest {
	tb.Helper()
	body := make([]byte, bodySize)
	for i := range body {
		body[i] = 'a'
	}
	hdr := make(http.Header, 4)
	hdr.Set("Content-Type", "application/json")
	hdr.Set("Authorization", "Bearer sk-test")
	return &AttemptRequest{
		Ctx:       context.Background(),
		Method:    http.MethodPost,
		URL:       u,
		Header:    hdr,
		Body:      body,
		Streaming: false,
	}
}

// hroLiveUpstream starts a real HTTP server on loopback and returns its base
// URL. Used where the pool must actually dial; the scheduler-only benchmarks
// avoid it entirely.
func hroLiveUpstream(tb testing.TB) *url.URL {
	tb.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatalf("listen: %v", err)
	}
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"id":"x","model":"u","choices":[]}`))
	})}
	go func() { _ = srv.Serve(ln) }()
	tb.Cleanup(func() { _ = srv.Close() })
	u, _ := url.Parse("http://" + ln.Addr().String())
	return u
}

// hroDirectConfig is a direct member config (Kind zero value) — the shape a
// real pool's direct members carry.
func hroDirectConfig() Config { return Config{Kind: Direct} }

// BenchmarkHroPoolSelectInitial measures the scheduler step alone: the
// schedMu critical section plus each candidate's health mutex and limiter
// mutex, with no dial. This is the per-attempt egress bookkeeping, isolated.
func BenchmarkHroPoolSelectInitial(b *testing.B) {
	for _, members := range []int{2, 4, 8, 16, 64} {
		for _, strategy := range []struct {
			name string
			s    Strategy
		}{{"rr", RoundRobin}, {"wrr", WeightedRoundRobin}} {
			b.Run("members"+strconv.Itoa(members)+"/"+strategy.name, func(b *testing.B) {
				c := hroPoolConfig(members, strategy.s, 0, true)
				d := hroPoolDoerFor(b, c)
				st := d.st
				eligible := make([]bool, members)
				for i := range eligible {
					eligible[i] = true
				}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if st.selectInitial(eligible) < 0 {
						b.Fatal("no member selected")
					}
				}
			})
		}
	}
}

// BenchmarkHroPoolSelectInitialConcurrent measures the same selection under
// RunParallel: schedMu is the only outer lock over a member's health and
// limiter mutexes, so this is where shared-pool contention would appear.
func BenchmarkHroPoolSelectInitialConcurrent(b *testing.B) {
	for _, par := range []int{1, 4, 16} {
		b.Run("par"+strconv.Itoa(par), func(b *testing.B) {
			const members = 4
			c := hroPoolConfig(members, RoundRobin, 0, true)
			d := hroPoolDoerFor(b, c)
			st := d.st
			eligible := make([]bool, members)
			for i := range eligible {
				eligible[i] = true
			}
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					if st.selectInitial(eligible) < 0 {
						b.Error("no member selected")
						return
					}
				}
			})
		})
	}
}

// BenchmarkHroPoolExecute measures a full pooled Execute against a live
// loopback upstream: eligibility, scheduling, permit, request build (which
// CLONES the header map and wraps the body in a bytes.Reader), dial, and the
// release body.
func BenchmarkHroPoolExecute(b *testing.B) {
	base := hroLiveUpstream(b)
	for _, members := range []int{1, 4} {
		for _, body := range []int{1 << 10, 64 << 10} {
			b.Run("members"+strconv.Itoa(members)+"/body"+strconv.Itoa(body), func(b *testing.B) {
				ms := make([]Member, members)
				for i := range ms {
					ms[i] = Member{Endpoint: hroDirectConfig(), Streaming: true, Weight: 1}
				}
				p := NewPool(ms, RoundRobin, FallbackPolicy{Enabled: true, MaxAttempts: members}, HealthPolicy{Enabled: true, FailureThreshold: 2, Cooldown: time.Second})
				d := hroPoolDoerFor(b, Config{Kind: EgressPool, Pool: p})
				u, _ := url.Parse(base.String() + "/v1/chat/completions")
				ar := hroAttempt(b, u, body)
				b.SetBytes(int64(body))
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					resp, _, err := d.Execute(ar)
					if err != nil {
						b.Fatalf("execute: %v", err)
					}
					_ = resp.Body.Close()
				}
			})
		}
	}
}

// BenchmarkHroPoolExecuteConcurrent measures a full pooled Execute under
// contention: schedMu, each member's limiter, the registry, and the shared
// http.Transport connection pool.
func BenchmarkHroPoolExecuteConcurrent(b *testing.B) {
	base := hroLiveUpstream(b)
	for _, par := range []int{1, 4, 16} {
		b.Run("par"+strconv.Itoa(par), func(b *testing.B) {
			const members = 4
			ms := make([]Member, members)
			for i := range ms {
				ms[i] = Member{Endpoint: hroDirectConfig(), Streaming: true, Weight: 1}
			}
			p := NewPool(ms, RoundRobin, FallbackPolicy{Enabled: true, MaxAttempts: members}, HealthPolicy{Enabled: true, FailureThreshold: 2, Cooldown: time.Second})
			d := hroPoolDoerFor(b, Config{Kind: EgressPool, Pool: p})
			u, _ := url.Parse(base.String() + "/v1/chat/completions")
			ar := hroAttempt(b, u, 4096)
			b.SetBytes(4096)
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					resp, _, err := d.Execute(ar)
					if err != nil {
						b.Errorf("execute: %v", err)
						return
					}
					_ = resp.Body.Close()
				}
			})
		})
	}
}

// BenchmarkHroConfigKey measures the transport Config content key, the
// registry's get-or-create key. Config.Key is called twice per Registry.Doer
// (once eagerly, once under the lock), so this is HOT on the resolve path.
func BenchmarkHroConfigKey(b *testing.B) {
	b.Run("direct", func(b *testing.B) {
		c := hroDirect()
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = c.Key()
		}
	})
	b.Run("proxy", func(b *testing.B) {
		c := hroProxy(1)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = c.Key()
		}
	})
	b.Run("pool4", func(b *testing.B) {
		c := hroPoolConfig(4, RoundRobin, 0, true)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = c.Key()
		}
	})
	b.Run("pool16", func(b *testing.B) {
		c := hroPoolConfig(16, WeightedRoundRobin, 0, true)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = c.Key()
		}
	})
}

// BenchmarkHroRegistryDoer measures the resolver's get-or-create: two Config
// key computations plus a mutex-guarded map lookup. Every retry and every
// fallback re-runs it (the handler resolves per attempt).
func BenchmarkHroRegistryDoer(b *testing.B) {
	for _, par := range []int{1, 2, 4, 8, 16, 32} {
		b.Run("par"+strconv.Itoa(par), func(b *testing.B) {
			reg := NewRegistry()
			c := hroDirect()
			_ = reg.Doer(c)
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					if reg.Doer(c) == nil {
						b.Error("nil doer")
						return
					}
				}
			})
		})
	}
}

// BenchmarkHroPoolIdentity measures the pool content hash — a load-time cost
// (NewPool), reported for completeness.
func BenchmarkHroPoolIdentity(b *testing.B) {
	for _, members := range []int{4, 16} {
		b.Run("members"+strconv.Itoa(members), func(b *testing.B) {
			ms := make([]Member, members)
			for i := range ms {
				ms[i] = Member{Endpoint: hroDirectConfig(), Streaming: true, Weight: 1}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = NewPool(ms, RoundRobin, FallbackPolicy{Enabled: true, MaxAttempts: members}, HealthPolicy{}).Identity()
			}
		})
	}
}
