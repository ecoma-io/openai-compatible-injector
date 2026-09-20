package e2e_test

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// Throughput and latency harness: the injector is benchmarked against a
// direct fake upstream (the zero-proxy baseline) over the same requests, so
// the proxy's own overhead is the only difference between arms. Benchmarks
// run ONLY under -bench — plain `go test ./e2e/` never executes them. The
// documented invocation (CONTRIBUTING.md):
//
//	go test ./e2e/ -run '^$' -bench BenchmarkThroughput -benchtime 2s
//	go test ./e2e/ -run '^$' -bench BenchmarkLatencyPercentiles -benchtime 1x
//
// Latency percentiles are computed over a fixed 1000-request sample with a
// 100-request warmup, so -benchtime 1x is the intended (and sufficient) run.

// perfMax returns the larger of two ints (a local helper with a perf-scoped
// name to avoid colliding with other files in the package).
func perfMax(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// perfUpstream is a fake upstream tuned for benchmarking: it answers every
// request with a JSON body padded to bodySize bytes, or — when
// streamEvents > 0 — with that many ~200B SSE events, flushed per event and
// never paced (the benchmark measures proxy overhead, not upstream timing).
type perfUpstream struct {
	srv          *httptest.Server
	hits         atomic.Int64
	bodySize     int
	streamEvents int
}

func newPerfUpstream(b *testing.B, bodySize, streamEvents int) *perfUpstream {
	b.Helper()
	u := &perfUpstream{bodySize: bodySize, streamEvents: streamEvents}
	pad := strings.Repeat("x", perfMax(bodySize-90, 1))
	event := `data: {"model":"up-name","choices":[{"delta":{"content":"` + strings.Repeat("y", 100) + `"}}]}` + "\n\n"
	u.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		u.hits.Add(1)
		if u.streamEvents == 0 {
			w.Header().Set("Content-Type", "application/json")
			_, _ = fmt.Fprintf(w, `{"id":"p","model":"up-name","choices":[{"message":{"role":"assistant","content":"%s"}}]}`, pad)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		fl, _ := w.(http.Flusher)
		for i := 0; i < u.streamEvents; i++ {
			_, _ = io.WriteString(w, event)
			if fl != nil {
				fl.Flush()
			}
		}
	}))
	b.Cleanup(u.srv.Close)
	return u
}

// startProcB is the *testing.B counterpart of startSubprocess: newProc and
// the health wait accept testing.TB, so benchmarks share the harness.
func startProcB(b *testing.B, o startOpts) *proc {
	b.Helper()
	p := newProc(b, o)
	p.start()
	b.Cleanup(func() { p.terminate(10 * time.Second) })
	p.waitHealth(b, 10*time.Second)
	return p
}

// benchmarkArm returns the host:port requests should hit for the named arm:
// "direct" is the fake upstream itself (it answers any path); injector arms
// boot the real binary pointed at it, at the named log level.
func benchmarkArm(b *testing.B, u *perfUpstream, arm string) string {
	b.Helper()
	switch arm {
	case "direct":
		return strings.TrimPrefix(u.srv.URL, "http://")
	case "injector_error", "injector_info", "injector_debug":
		p := startProcB(b, startOpts{
			yaml:     runtimeYAML("public", u.srv.URL+"/v1", "up-name", ""),
			logLevel: strings.TrimPrefix(arm, "injector_"),
		})
		return p.addr
	default:
		b.Fatalf("unknown arm %q", arm)
		return ""
	}
}

// perfChatBody builds a chat request whose user message is padded toward
// want bytes.
func perfChatBody(model string, want int) string {
	pad := strings.Repeat("m", perfMax(want-120, 1))
	return fmt.Sprintf(`{"model":%q,"messages":[{"role":"user","content":"%s"}]}`, model, pad)
}

// perfPostAt issues one non-streaming request to the given route and returns
// its duration; it fails the benchmark on any non-200.
func perfPostAt(b *testing.B, addr, path, body string) time.Duration {
	b.Helper()
	start := time.Now()
	resp, err := http.Post("http://"+addr+path, "application/json",
		strings.NewReader(body))
	if err != nil {
		b.Fatalf("post: %v", err)
	}
	got, err := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if err != nil {
		b.Fatalf("read: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		b.Fatalf("status = %d body %s", resp.StatusCode, got)
	}
	return time.Since(start)
}

// perfPost issues one non-streaming chat request and returns its duration.
func perfPost(b *testing.B, addr, body string) time.Duration {
	b.Helper()
	return perfPostAt(b, addr, "/v1/chat/completions", body)
}

// perfStream reads one streaming request to EOF and returns its duration.
func perfStream(b *testing.B, addr, body string) time.Duration {
	b.Helper()
	start := time.Now()
	resp, err := http.Post("http://"+addr+"/v1/chat/completions", "application/json",
		strings.NewReader(body))
	if err != nil {
		b.Fatalf("post: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		b.Fatalf("status = %d", resp.StatusCode)
	}
	n, err := io.Copy(io.Discard, resp.Body)
	if err != nil {
		b.Fatalf("copy: %v", err)
	}
	if n == 0 {
		b.Fatal("empty stream")
	}
	return time.Since(start)
}

// BenchmarkThroughputChat measures single-request overhead end to end: one
// request per iteration, ns/op is the full round trip including the client.
// The size matrix runs at error level; the level axis is pinned on the
// small body.
func BenchmarkThroughputChat(b *testing.B) {
	sizes := []struct {
		name string
		size int
	}{
		{"small_200B", 200},
		{"medium_4KB", 4 << 10},
		{"large_64KB", 64 << 10},
	}
	for _, tc := range sizes {
		arms := []string{"direct", "injector_error"}
		if tc.size == 200 {
			arms = append(arms, "injector_info", "injector_debug")
		}
		for _, arm := range arms {
			b.Run(tc.name+"/"+arm, func(b *testing.B) {
				u := newPerfUpstream(b, tc.size, 0)
				addr := benchmarkArm(b, u, arm)
				body := perfChatBody("public", 128)
				for i := 0; i < 100; i++ {
					perfPost(b, addr, body)
				}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					perfPost(b, addr, body)
				}
			})
		}
	}
}

// BenchmarkThroughputResponses covers the second API surface.
func BenchmarkThroughputResponses(b *testing.B) {
	for _, arm := range []string{"direct", "injector_error"} {
		b.Run("nonstream/"+arm, func(b *testing.B) {
			u := newPerfUpstream(b, 200, 0)
			addr := benchmarkArm(b, u, arm)
			body := `{"model":"public","input":"hello"}`
			for i := 0; i < 100; i++ {
				perfPostAt(b, addr, "/v1/responses", body)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				perfPostAt(b, addr, "/v1/responses", body)
			}
		})
	}
}

// BenchmarkThroughputStream measures streaming overhead at 10/100/1000
// events (~200B each, unpaced): read-to-EOF round trip.
func BenchmarkThroughputStream(b *testing.B) {
	for _, events := range []int{10, 100, 1000} {
		for _, arm := range []string{"direct", "injector_error"} {
			b.Run(fmt.Sprintf("events_%d/%s", events, arm), func(b *testing.B) {
				u := newPerfUpstream(b, 0, events)
				addr := benchmarkArm(b, u, arm)
				body := `{"model":"public","stream":true,"messages":[{"role":"user","content":"hi"}]}`
				for i := 0; i < 100; i++ {
					perfStream(b, addr, body)
				}
				b.ReportAllocs()
				b.SetBytes(int64(events * 200))
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					perfStream(b, addr, body)
				}
			})
		}
	}
}

// BenchmarkLatencyPercentiles reports p50/p95/p99 over a fixed 1000-request
// sample (100 warmups) — report metrics, not per-iteration timings. Run
// with -benchtime 1x.
func BenchmarkLatencyPercentiles(b *testing.B) {
	u := newPerfUpstream(b, 200, 0)
	p := startProcB(b, startOpts{
		yaml:     runtimeYAML("public", u.srv.URL+"/v1", "up-name", ""),
		logLevel: "error",
	})
	body := perfChatBody("public", 128)
	for i := 0; i < 100; i++ {
		perfPost(b, p.addr, body)
	}
	const n = 1000
	sample := make([]time.Duration, 0, n)
	b.ResetTimer()
	for i := 0; i < n; i++ {
		sample = append(sample, perfPost(b, p.addr, body))
	}
	b.StopTimer()

	sort.Slice(sample, func(i, j int) bool { return sample[i] < sample[j] })
	pct := func(p float64) float64 {
		return float64(sample[int(float64(len(sample)-1)*p)].Microseconds()) / 1000.0
	}
	b.ReportMetric(pct(0.50), "p50-ms")
	b.ReportMetric(pct(0.95), "p95-ms")
	b.ReportMetric(pct(0.99), "p99-ms")
	b.Logf("latency over %d requests: p50=%.3fms p95=%.3fms p99=%.3fms",
		n, pct(0.50), pct(0.95), pct(0.99))
}
