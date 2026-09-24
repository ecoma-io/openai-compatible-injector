package proxy

import (
	"bytes"
	"fmt"
	"io"
	"strings"
	"testing"
)

// BenchmarkRewriteSSELine measures the per-line rewrite rule in isolation on
// the three shapes a stream actually hits: a data line whose payload carries
// the model key and gets rewritten, a data line without the model key (the
// pure scan fast path), and a single 64 KiB data line (the provider-padding
// case that must stay far below the line cap).
func BenchmarkRewriteSSELine(b *testing.B) {
	tests := []struct {
		name string
		line []byte
	}{
		{
			"with_model",
			[]byte(`data: {"model":"upstream-name","choices":[{"index":0,"delta":{"role":"assistant","content":"hi"},"finish_reason":null}]}` + "\n"),
		},
		{
			"without_model",
			[]byte(`data: {"choices":[{"index":0,"delta":{"role":"assistant","content":"hi"},"finish_reason":null}]}` + "\n"),
		},
		{
			"huge_64KB_line",
			[]byte(`data: {"model":"upstream-name","choices":[{"index":0,"delta":{"role":"assistant","content":"` + strings.Repeat("m", 64<<10) + `"},"finish_reason":null}]}` + "\n"),
		},
	}
	for _, tc := range tests {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(tc.line)))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				rewriteSSELine(tc.line, sseRewriter("public-name"), nil)
			}
		})
	}
}

// sseEvent renders one deterministic SSE event: a data line whose payload is
// the upstream chunk and the trailing blank line. Event boundaries arrive at
// every second line.
func sseEvent(n int) string {
	return fmt.Sprintf(`data: {"model":"upstream-name","i":%d}`+"\n\n", n)
}

func BenchmarkCopySSE(b *testing.B) {
	for _, tc := range []struct {
		name string
		n    int
	}{
		{"events_10", 10},
		{"events_100", 100},
		{"events_1000", 1000},
	} {
		b.Run(tc.name, func(b *testing.B) {
			in, total := buildSSEInput(tc.n, sseEvent)
			var src bytes.Reader
			b.ReportAllocs()
			b.SetBytes(total)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				src.Reset(in) // deterministic: no allocs, no state leak
				if _, err := CopySSE(io.Discard, &src, sseRewriter("public-name"), func() {}, nil); err != nil {
					b.Fatalf("CopySSE: %v", err)
				}
			}
			b.ReportMetric(float64(tc.n)/float64(b.N), "events/op")
		})
	}
}

// BenchmarkCopySSELongLines measures 10 lines of 64 KiB each, with the blank
// event boundary between every pair of data lines. The point is the
// multi-buffer line case: lines far larger than the read buffer, event
// boundaries respected, well below the caps.
func BenchmarkCopySSELongLines(b *testing.B) {
	n := 10
	in, total := buildSSEInput(n, func(i int) string {
		return `data: {"model":"upstream-name","i":` + fmt.Sprint(i) + `,"content":"` +
			strings.Repeat("m", 64<<10) + `"}` + "\n\n"
	})
	var src bytes.Reader
	b.ReportAllocs()
	b.SetBytes(total)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		src.Reset(in)
		if _, err := CopySSE(io.Discard, &src, sseRewriter("public-name"), func() {}, nil); err != nil {
			b.Fatalf("CopySSE: %v", err)
		}
	}
	b.ReportMetric(float64(n)/float64(b.N), "events/op")
}

// buildSSEInput renders n lines via line and returns the concatenated input
// and its total byte size.
func buildSSEInput(n int, line func(int) string) ([]byte, int64) {
	var sb strings.Builder
	for i := 0; i < n; i++ {
		sb.WriteString(line(i))
	}
	out := []byte(sb.String())
	return out, int64(len(out))
}
