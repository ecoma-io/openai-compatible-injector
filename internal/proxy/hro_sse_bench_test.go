package proxy

import (
	"bufio"
	"io"
	"runtime"
	"strconv"
	"strings"
	"testing"

	"openai-compatible-injector/internal/inject"
)

// hroBytesPerRun returns the total bytes allocated per run, measured as the
// runtime.MemStats TotalAlloc delta over n runs after a warmup. TotalAlloc
// is cumulative and exact, so the quotient is stable where wall clock is not.
func hroBytesPerRun(n int, op func()) uint64 {
	op()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	for i := 0; i < n; i++ {
		op()
	}
	runtime.ReadMemStats(&after)
	return (after.TotalAlloc - before.TotalAlloc) / uint64(n)
}

// ---------------------------------------------------------------------------
// CopySSE: the streaming hot path. One line read, one gate probe, one
// conditional rewrite, one write, one flush per event boundary.
// ---------------------------------------------------------------------------

// hroCountingReader hands out at most chunk bytes per Read, so the same SSE
// byte stream can be replayed at wire fragmentation levels a real TCP
// connection produces (a 1500-byte MTU is the common case; 1 byte is the
// adversarial one the grammar must survive).
type hroCountingReader struct {
	data  []byte
	chunk int
	off   int
}

func (r *hroCountingReader) Read(p []byte) (int, error) {
	if r.off >= len(r.data) {
		return 0, io.EOF
	}
	n := r.chunk
	if n > len(p) {
		n = len(p)
	}
	if r.off+n > len(r.data) {
		n = len(r.data) - r.off
	}
	copy(p, r.data[r.off:r.off+n])
	r.off += n
	return n, nil
}

func (r *hroCountingReader) reset() { r.off = 0 }

// hroDataLine renders one streamed chat chunk carrying the model key — the
// shape every real provider chunk has, and the one the rewrite gate admits.
func hroDataLine(i, contentBytes int) string {
	return `data: {"id":"chatcmpl-1","object":"chat.completion.chunk","created":1,"model":"upstream-name","choices":[{"index":0,"delta":{"role":"assistant","content":"` +
		strings.Repeat("m", contentBytes) + `"},"finish_reason":null}],"usage":{"prompt_tokens":10,"completion_tokens":2,"total_tokens":12}}` + "\n\n"
}

// hroDeltaOnlyLine is a Responses-shaped event whose data line carries NO
// model and NO usage key — the pure gate-miss fast path, which is most of a
// real stream's lines.
func hroDeltaOnlyLine(i, contentBytes int) string {
	return "event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"item_id\":\"msg_1\",\"output_index\":0,\"content_index\":0,\"delta\":\"" +
		strings.Repeat("t", contentBytes) + "\"}\n\n"
}

// hroSSEStream builds an SSE byte stream of n events, plus the total size.
func hroSSEStream(n, contentBytes int, line func(i, c int) string) ([]byte, int64) {
	var sb strings.Builder
	for i := 0; i < n; i++ {
		sb.WriteString(line(i, contentBytes))
	}
	out := []byte(sb.String())
	return out, int64(len(out))
}

// BenchmarkHroCopySSE measures the full relay — buffered read, per-line
// grammar, gate probe, rewrite, write, flush — across the mandated shapes:
// event count (10 / 100 / 1000 / 10_000), and at a fixed event count the
// fragmentation the wire delivers (1 byte per Read vs. bulk).
//
// The relay is the process's longest-lived per-byte loop: a 10k-event stream
// runs it 30k times, once per line including the blank boundaries.
func BenchmarkHroCopySSE(b *testing.B) {
	rw := sseRewriter("public-name")
	for _, shape := range []struct {
		name         string
		events       int
		contentBytes int
	}{
		{"short_10", 10, 16},
		{"short_100", 100, 16},
		{"long_100", 100, 2048},
		{"high_freq_1000", 1000, 16},
		{"high_freq_10000", 10000, 16},
		{"long_1000_8KB", 1000, 8 << 10},
	} {
		in, total := hroSSEStream(shape.events, shape.contentBytes, hroDataLine)
		src := &hroCountingReader{data: in, chunk: 1 << 20}
		b.Run(shape.name, func(b *testing.B) {
			b.SetBytes(total)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				src.reset()
				st, err := CopySSE(io.Discard, src, rw, func() {}, nil, nil)
				if err != nil {
					b.Fatalf("CopySSE: %v", err)
				}
				if st.Events != shape.events {
					b.Fatalf("events = %d, want %d", st.Events, shape.events)
				}
			}
			b.ReportMetric(float64(shape.events)/float64(b.N), "events/op")
		})
	}
}

// BenchmarkHroCopySSEFragmented replays the SAME stream at a range of wire
// fragmentations. TCP delivers a stream in arbitrary pieces; a relay whose
// cost grows with fragmentation is a relay whose cost the peer controls.
func BenchmarkHroCopySSEFragmented(b *testing.B) {
	rw := sseRewriter("public-name")
	const events = 200
	in, total := hroSSEStream(events, 64, hroDataLine)
	for _, frag := range []int{1, 8, 256, 1500, 65536, 1 << 20} {
		b.Run("read"+strconv.Itoa(frag), func(b *testing.B) {
			src := &hroCountingReader{data: in, chunk: frag}
			b.SetBytes(total)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				src.reset()
				if _, err := CopySSE(io.Discard, src, rw, func() {}, nil, nil); err != nil {
					b.Fatalf("CopySSE: %v", err)
				}
			}
		})
	}
}

// BenchmarkHroCopySSELineRewrite measures the per-line rewrite gate in
// isolation, at the line sizes a stream actually produces: the model-carrying
// chunk (admitted, rewritten), the plain delta (gate miss, no-op), and the
// near-cap line. This is the factor that dominates a 10k-event stream.
func BenchmarkHroCopySSELineRewrite(b *testing.B) {
	rw := sseRewriter("public-name")
	strip := inject.StripPatterns([][]string{{"provider"}, {"service_tier"}})
	for _, tc := range []struct {
		name string
		line []byte
	}{
		{"chat_chunk_with_model", []byte(hroDataLine(0, 16))},
		{"responses_delta_no_keys", []byte(hroDeltaOnlyLine(0, 64))},
		{"comment_ping", []byte(": ping\n\n")},
		{"blank_boundary", []byte("\n")},
		{"done_marker", []byte("data: [DONE]\n\n")},
		{"chunk_8KB", []byte(hroDataLine(0, 8<<10))},
		{"line_near_cap_1MB", []byte(`data: {"model":"upstream-name","c":"` + strings.Repeat("m", 1<<20) + `"}` + "\n")},
	} {
		line := tc.line
		b.Run(tc.name+"/no_strip", func(b *testing.B) {
			b.SetBytes(int64(len(line)))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = rewriteSSELine(line, rw, nil)
			}
		})
		b.Run(tc.name+"/with_strip", func(b *testing.B) {
			b.SetBytes(int64(len(line)))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = rewriteSSELine(line, rw, strip)
			}
		})
	}
}

// BenchmarkHroCopySSEReadLine measures the line reader alone (the CR/LF
// grammar over the bufio buffer), with the rewrite removed from the picture.
func BenchmarkHroCopySSEReadLine(b *testing.B) {
	for _, frag := range []int{1, 1500, 65536} {
		in, _ := hroSSEStream(50, 32, hroDataLine)
		b.Run("read"+strconv.Itoa(frag), func(b *testing.B) {
			src := &hroCountingReader{data: in, chunk: frag}
			b.SetBytes(int64(len(in)))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				src.reset()
				br := bufio.NewReaderSize(src, sseReadBuffer)
				for {
					_, err := readBoundedLine(br)
					if err != nil {
						break
					}
				}
			}
		})
	}
}

// BenchmarkHroCopySSEToBufWriter measures the relay against a growing sink
// rather than io.Discard, so the write path is real. A per-event flush
// against an actual bufio.Writer is the shape the handler's flusher has.
func BenchmarkHroCopySSEToBufWriter(b *testing.B) {
	rw := sseRewriter("public-name")
	in, total := hroSSEStream(500, 64, hroDataLine)
	src := &hroCountingReader{data: in, chunk: 1 << 20}
	dst := bufio.NewWriterSize(io.Discard, 32<<10)
	b.SetBytes(total)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		src.reset()
		dst.Reset(io.Discard)
		if _, err := CopySSE(dst, src, rw, func() { _ = dst.Flush() }, nil, nil); err != nil {
			b.Fatalf("CopySSE: %v", err)
		}
	}
}

// BenchmarkHroCopySSEWithObserver measures the relay with the continuation
// accumulator's observe seam wired in — the shape a deployment with
// stream recovery enabled runs, where every data line is handed to a
// partial-text accumulator before the gate.
func BenchmarkHroCopySSEWithObserver(b *testing.B) {
	rw := sseRewriter("public-name")
	for _, events := range []int{100, 1000} {
		in, total := hroSSEStream(events, 64, hroDataLine)
		src := &hroCountingReader{data: in, chunk: 1 << 20}
		b.Run("events"+strconv.Itoa(events), func(b *testing.B) {
			b.SetBytes(total)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				src.reset()
				// A fresh accumulator per iteration: the point of the row is
				// what one full stream costs, so the previous run's partial
				// text must not carry into this one.
				pt := newPartialText(apiChat, 256<<10)
				if _, err := CopySSE(io.Discard, src, rw, func() {}, nil, pt.Observe); err != nil {
					b.Fatalf("CopySSE: %v", err)
				}
			}
		})
	}
}

// BenchmarkHroCopySSECountingAlloc reports allocs per EVENT and per KB, the
// figure a streaming optimization claim has to beat. b.ReportAllocs already
// gives allocs/op; these custom metrics make the per-event number explicit
// and diffable.
func BenchmarkHroCopySSECountingAlloc(b *testing.B) {
	rw := sseRewriter("public-name")
	for _, events := range []int{100, 1000, 10000} {
		in, total := hroSSEStream(events, 16, hroDataLine)
		src := &hroCountingReader{data: in, chunk: 1 << 20}
		b.Run("events"+strconv.Itoa(events), func(b *testing.B) {
			b.SetBytes(total)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				src.reset()
				if _, err := CopySSE(io.Discard, src, rw, func() {}, nil, nil); err != nil {
					b.Fatalf("CopySSE: %v", err)
				}
			}
			allocs := testing.AllocsPerRun(20, func() {
				src.reset()
				_, _ = CopySSE(io.Discard, src, rw, func() {}, nil, nil)
			})
			bytes := hroBytesPerRun(20, func() {
				src.reset()
				_, _ = CopySSE(io.Discard, src, rw, func() {}, nil, nil)
			})
			b.ReportMetric(allocs/float64(events), "allocs/event")
			b.ReportMetric(float64(bytes)/float64(events), "B/event")
			b.ReportMetric(float64(bytes)/float64(total), "amp")
		})
	}
}
