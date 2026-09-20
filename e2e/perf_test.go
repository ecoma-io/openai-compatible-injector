package e2e_test

import (
	"bufio"
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
// streamEvents > 0 — with that many SSE events (perfEvent(shape, 200)),
// flushed per event and unpaced unless eventInterval is set (the benchmarks
// measure proxy overhead, not upstream timing).
type perfUpstream struct {
	srv          *httptest.Server
	hits         atomic.Int64
	bodySize     int
	streamEvents int
	// shape selects the SSE envelope the streaming arms emit: "chat"
	// (default) emits bare data: lines; "responses" emits the
	// event:+data: pairs the Responses surface branches on.
	shape string
	// eventInterval, when set, spaces the stream's events that far apart —
	// the pacing a buffering relay would hide (BenchmarkStreamFirstBytePaced).
	eventInterval time.Duration
}

func newPerfUpstream(b *testing.B, bodySize, streamEvents int) *perfUpstream {
	b.Helper()
	u := &perfUpstream{bodySize: bodySize, streamEvents: streamEvents}
	pad := strings.Repeat("x", perfMax(bodySize-90, 1))
	shape := u.shape
	if shape == "" {
		shape = "chat"
	}
	event := perfEvent(shape, 200)
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
			if u.eventInterval > 0 && i > 0 {
				time.Sleep(u.eventInterval)
			}
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
// its duration; it fails the caller on any non-200.
func perfPostAt(tb testing.TB, addr, path, body string) time.Duration {
	tb.Helper()
	start := time.Now()
	resp, err := http.Post("http://"+addr+path, "application/json",
		strings.NewReader(body))
	if err != nil {
		tb.Fatalf("post: %v", err)
	}
	got, err := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if err != nil {
		tb.Fatalf("read: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		tb.Fatalf("status = %d body %s", resp.StatusCode, got)
	}
	return time.Since(start)
}

// perfPost issues one non-streaming chat request and returns its duration.
func perfPost(tb testing.TB, addr, body string) time.Duration {
	tb.Helper()
	return perfPostAt(tb, addr, "/v1/chat/completions", body)
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
//
// What the size axis actually varies: only the upstream RESPONSE body is
// padded — the request body is a fixed ~69 bytes and the injection prompt
// is empty, so a size arm measures the response path (read, validate, model
// rewrite, copy), not inject.Chat at that size. Recorded deltas must be
// read on those terms; the request-side transform is pinned by the
// inject package's own benchmarks.
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
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				perfPostAt(b, addr, "/v1/responses", body)
			}
		})
	}
}

// BenchmarkThroughputStream measures streaming overhead at 10/100/1000
// events (perfEvent's exact byte count feeds SetBytes; unpaced):
// read-to-EOF round trip. No ReportAllocs: these measure the benchmark
// client's heap, not the binary under test.
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
				b.SetBytes(int64(len(perfEvent("chat", 200))) * int64(events))
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

// perfEvent builds one SSE event of the given API shape, ~size bytes.
// Responses envelopes carry event:+data: pairs — the shape the injector's
// path-branching stream reader expects on /v1/responses.
func perfEvent(shape string, size int) string {
	pad := strings.Repeat("y", perfMax(size-60, 1))
	data := `data: {"model":"up-name","delta":{"text":"` + pad + `"}}` + "\n"
	if shape == "responses" {
		return "event: response.output_text.delta\n" + data + "\n"
	}
	return data + "\n"
}

// perfStreamRead reads one streaming request until the first SSE event
// boundary (first-byte proxy) and then to EOF; it returns both durations.
func perfStreamRead(tb testing.TB, addr, path, body string) (ttfb, total time.Duration) {
	tb.Helper()
	start := time.Now()
	resp, err := http.Post("http://"+addr+path, "application/json",
		strings.NewReader(body))
	if err != nil {
		tb.Fatalf("post: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		tb.Fatalf("status = %d", resp.StatusCode)
	}
	br := bufio.NewReader(resp.Body)
	// The first event ends at its blank line; every shape emits one.
	sawBoundary := false
	for !sawBoundary {
		line, err := br.ReadString('\n')
		if err != nil {
			tb.Fatalf("stream read: %v", err)
		}
		if line == "\n" {
			sawBoundary = true
		}
	}
	ttfb = time.Since(start)
	if _, err := io.Copy(io.Discard, br); err != nil {
		tb.Fatalf("copy: %v", err)
	}
	return ttfb, time.Since(start)
}

// BenchmarkThroughputResponsesStream completes the matrix cell the other
// benchmarks leave empty: Responses, streaming, injector vs direct, with the
// upstream emitting the responses SSE shape (event:+data: pairs). The level
// axis rides on the 1000-event cell, as the chat stream pins it on its
// largest arm.
func BenchmarkThroughputResponsesStream(b *testing.B) {
	body := `{"model":"public","stream":true,"input":"hi"}`
	for _, events := range []int{10, 100, 1000} {
		arms := []string{"direct", "injector_error"}
		if events == 1000 {
			arms = append(arms, "injector_info", "injector_debug")
		}
		for _, arm := range arms {
			b.Run(fmt.Sprintf("events_%d/%s", events, arm), func(b *testing.B) {
				u := newPerfUpstream(b, 0, events)
				u.shape = "responses"
				addr := benchmarkArm(b, u, arm)
				for i := 0; i < 100; i++ {
					_, _ = perfStreamRead(b, addr, "/v1/responses", body)
				}
				b.SetBytes(int64(len(perfEvent("responses", 200))) * int64(events))
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					_, _ = perfStreamRead(b, addr, "/v1/responses", body)
				}
			})
		}
	}
}

// BenchmarkStreamFirstBytePercentiles reports first-event (TTFB) p50/p95/p99
// over a fixed 500-request sample (50 warmups), injector vs direct, so the
// streaming interaction cost is visible separately from the full-stream
// duration the throughput benchmarks measure. Run with -benchtime 1x.
func BenchmarkStreamFirstBytePercentiles(b *testing.B) {
	const (
		n      = 500
		warmup = 50
	)
	for _, arm := range []string{"direct", "injector_error"} {
		b.Run(arm, func(b *testing.B) {
			u := newPerfUpstream(b, 0, 20)
			addr := benchmarkArm(b, u, arm)
			body := `{"model":"public","stream":true,"messages":[{"role":"user","content":"hi"}]}`
			for i := 0; i < warmup; i++ {
				_, _ = perfStreamRead(b, addr, "/v1/chat/completions", body)
			}
			sample := make([]time.Duration, 0, n)
			b.ResetTimer()
			for i := 0; i < n; i++ {
				ttfb, _ := perfStreamRead(b, addr, "/v1/chat/completions", body)
				sample = append(sample, ttfb)
			}
			b.StopTimer()

			sort.Slice(sample, func(i, j int) bool { return sample[i] < sample[j] })
			pct := func(p float64) float64 {
				return float64(sample[int(float64(len(sample)-1)*p)].Microseconds()) / 1000.0
			}
			b.ReportMetric(pct(0.50), "ttfb-p50-ms")
			b.ReportMetric(pct(0.95), "ttfb-p95-ms")
			b.ReportMetric(pct(0.99), "ttfb-p99-ms")
			b.Logf("ttfb over %d requests (%s): p50=%.3fms p95=%.3fms p99=%.3fms",
				n, arm, pct(0.50), pct(0.95), pct(0.99))
		})
	}
}

// BenchmarkStreamFirstBytePaced makes buffering measurable where the
// unpaced benchmarks cannot see it: the upstream spaces its events 25ms
// apart, so a pass-through relay delivers the first event at roughly one
// interval while a buffering relay cannot deliver anything until the
// upstream finishes. The reported ttfb-p50 per arm is the instrument — the
// direct-to-injector gap staying near zero is the pass-through evidence.
// Report metrics, not per-iteration timings; run with -benchtime 1x.
func BenchmarkStreamFirstBytePaced(b *testing.B) {
	const (
		events   = 6
		interval = 25 * time.Millisecond
		samples  = 20
		warmup   = 3
	)
	for _, arm := range []string{"direct", "injector_error"} {
		b.Run(arm, func(b *testing.B) {
			u := newPerfUpstream(b, 0, events)
			u.eventInterval = interval
			addr := benchmarkArm(b, u, arm)
			body := `{"model":"public","stream":true,"messages":[{"role":"user","content":"hi"}]}`
			for i := 0; i < warmup; i++ {
				_, _ = perfStreamRead(b, addr, "/v1/chat/completions", body)
			}
			sample := make([]time.Duration, 0, samples)
			b.ResetTimer()
			for i := 0; i < samples; i++ {
				ttfb, _ := perfStreamRead(b, addr, "/v1/chat/completions", body)
				sample = append(sample, ttfb)
			}
			b.StopTimer()

			sort.Slice(sample, func(i, j int) bool { return sample[i] < sample[j] })
			p50 := float64(sample[len(sample)/2].Microseconds()) / 1000.0
			b.ReportMetric(p50, "ttfb-p50-ms")
			b.Logf("paced ttfb over %d requests (%s): p50=%.3fms (pass-through lands near one %v interval)",
				samples, arm, p50, interval)
		})
	}
}

// TestProxyOverheadGuard is the coarse overhead tripwire for the whole proxy
// path: the injector's median round trip must stay within 8x the
// direct-upstream median plus an absolute floor — for a buffered request and
// for a stream's first event. At realistic loopback medians (tens of
// microseconds) the floor dominates the bound, so in practice this behaves
// like an absolute ~5ms ceiling: it catches structural catastrophes
// (per-request config work, per-request dials, synchronous heavy logging),
// not microsecond deltas — and it cannot detect buffering of an unpaced
// upstream stream at all, because an unpaced stream arrives nearly instantly
// either way; BenchmarkStreamFirstBytePaced is the instrument for buffering.
// The multiple only binds if the upstream itself is slow, where scaling with
// the baseline is the fairer bound. Skipped under -short (keeps the suite
// quick) and -race (timings are meaningless with the detector on).
func TestProxyOverheadGuard(t *testing.T) {
	if testing.Short() {
		t.Skip("relative wall-clock guard skipped under -short")
	}
	if raceDetectorOn() {
		t.Skip("relative wall-clock guard skipped under -race")
	}

	u := &perfUpstream{streamEvents: 20}
	event := `data: {"model":"up-name","choices":[{"delta":{"content":"hello"}}]}` + "\n\n"
	u.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqBody, _ := io.ReadAll(r.Body)
		if !strings.Contains(string(reqBody), `"stream":true`) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = fmt.Fprintf(w, `{"id":"p","model":"up-name","choices":[{"message":{"role":"assistant","content":"hi"}}]}`)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		fl, _ := w.(http.Flusher)
		for i := 0; i < u.streamEvents; i++ {
			if u.eventInterval > 0 && i > 0 {
				time.Sleep(u.eventInterval)
			}
			_, _ = io.WriteString(w, event)
			if fl != nil {
				fl.Flush()
			}
		}
	}))
	t.Cleanup(u.srv.Close)
	direct := strings.TrimPrefix(u.srv.URL, "http://")

	p := newProc(t, startOpts{yaml: runtimeYAML("public", u.srv.URL+"/v1", "up-name", "")})
	p.start()
	t.Cleanup(func() { p.terminate(10 * time.Second) })
	p.waitHealth(t, 10*time.Second)

	chatBody := perfChatBody("public", 128)
	streamBody := `{"model":"public","stream":true,"messages":[{"role":"user","content":"hi"}]}`

	// bufferedSample returns the sorted round-trip sample of n buffered POSTs.
	bufferedSample := func(addr string, n int) []time.Duration {
		s := make([]time.Duration, 0, n)
		for i := 0; i < n; i++ {
			s = append(s, perfPostAt(t, addr, "/v1/chat/completions", chatBody))
		}
		sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
		return s
	}
	// ttfbSample returns the sorted first-event sample of n streamed POSTs.
	ttfbSample := func(addr string, n int) []time.Duration {
		s := make([]time.Duration, 0, n)
		for i := 0; i < n; i++ {
			ttfb, _ := perfStreamRead(t, addr, "/v1/chat/completions", streamBody)
			s = append(s, ttfb)
		}
		sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
		return s
	}

	median := func(s []time.Duration) time.Duration { return s[len(s)/2] }

	const (
		n      = 400
		warmup = 50
	)
	// Warm both arms before sampling: first-request connection setup would
	// otherwise sit in the median.
	bufferedSample(direct, warmup)
	bufferedSample(p.addr, warmup)
	ttfbSample(direct, warmup)
	ttfbSample(p.addr, warmup)

	directBuf := median(bufferedSample(direct, n))
	injBuf := median(bufferedSample(p.addr, n))
	directTTFB := median(ttfbSample(direct, n))
	injTTFB := median(ttfbSample(p.addr, n))

	// 8x the direct median plus a 5ms floor. On loopback the direct median
	// is microseconds, so the floor is what binds and the guard is an
	// order-of-magnitude tripwire; the multiple takes over only when the
	// upstream itself is slow, where a scaled bound stays fair.
	const multiple = 8.0
	const floor = 5 * time.Millisecond
	limit := func(direct time.Duration) time.Duration {
		return time.Duration(float64(direct)*multiple) + floor
	}
	t.Logf("buffered: direct p50=%v injector p50=%v (limit %v)", directBuf, injBuf, limit(directBuf))
	t.Logf("stream ttfb: direct p50=%v injector p50=%v (limit %v)", directTTFB, injTTFB, limit(directTTFB))

	if injBuf > limit(directBuf) {
		t.Errorf("buffered median overhead blown: injector %v vs direct %v, limit %v", injBuf, directBuf, limit(directBuf))
	}
	if injTTFB > limit(directTTFB) {
		t.Errorf("stream first-event median overhead blown: injector %v vs direct %v, limit %v", injTTFB, directTTFB, limit(directTTFB))
	}
}
