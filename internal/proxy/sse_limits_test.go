package proxy

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
)

// fullLine returns a line of exactly n bytes: n-1 'x' plus the "\n"
// terminator. It is not a data line, so it relays byte-identically — the
// size is the only property under test.
func fullLine(n int) string {
	return strings.Repeat("x", n-1) + "\n"
}

// countingReader records how many bytes actually left the source, making
// the stop-reading guarantee after a limit breach observable: a correct
// relay stops within one read buffer of the cap, a runaway one drains the
// whole source.
type countingReader struct {
	r    *strings.Reader
	read int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.read += int64(n)
	return n, err
}

// copySSELimited runs CopySSE over input and returns the exact output bytes,
// the bytes pulled from the source, and the terminal error. Unlike
// copySSEOnce it tolerates — and reports — truncation.
func copySSELimited(t *testing.T, input string) (string, int64, error) {
	t.Helper()
	var buf strings.Builder
	src := &countingReader{r: strings.NewReader(input)}
	stats, err := CopySSE(&buf, src, "public-name", func() {})
	if err == nil && stats.Bytes != int64(buf.Len()) {
		t.Fatalf("stats.Bytes = %d, want %d (output length)", stats.Bytes, buf.Len())
	}
	return buf.String(), src.read, err
}

// assertLimitBreach pins the shared contract of both caps: the sentinel
// survives unwrapping, the failure is NOT marked client-side (the handler's
// truncation phase depends on that distinction), and nothing of the
// offending line reached the client.
func assertLimitBreach(t *testing.T, err error, sentinel error, out, offending string) {
	t.Helper()
	if err == nil {
		t.Fatal("limit breach not detected")
	}
	if !errors.Is(err, sentinel) {
		t.Fatalf("error = %v, want it to wrap %v", err, sentinel)
	}
	var swe *streamWriteError
	if errors.As(err, &swe) {
		t.Errorf("limit breach wrongly marked as client write failure: %v", err)
	}
	if strings.Contains(out, offending) {
		t.Errorf("offending line leaked to the client: %q", truncateForLog(out))
	}
}

func truncateForLog(s string) string {
	if len(s) > 120 {
		return s[:120] + "..."
	}
	return s
}

// TestCopySSELineExactlyAtLimitRelayed: a line of exactly MaxLineBytes is
// inside the cap and must relay byte-identically.
func TestCopySSELineExactlyAtLimitRelayed(t *testing.T) {
	input := fullLine(MaxLineBytes)
	out, _, err := copySSELimited(t, input)
	if err != nil {
		t.Fatalf("line at the exact cap rejected: %v", err)
	}
	if out != input {
		t.Errorf("line at cap corrupted: got %d bytes, want %d identical", len(out), len(input))
	}
}

// TestCopySSELineOneOverLimitTruncates: MaxLineBytes+1 is the first rejected
// line — the offending line is never written and nothing further is read.
func TestCopySSELineOneOverLimitTruncates(t *testing.T) {
	input := strings.Repeat("x", MaxLineBytes) + "\n" + fullLine(64)
	out, read, err := copySSELimited(t, input)
	assertLimitBreach(t, err, ErrSSELineTooLong, out, "xxxx")
	if len(out) != 0 {
		t.Errorf("output = %d bytes, want none (offending line never written)", len(out))
	}
	if read > MaxLineBytes+sseReadBuffer {
		t.Errorf("read %d bytes from the source after crossing the cap; want ≤ %d",
			read, MaxLineBytes+sseReadBuffer)
	}
}

// TestCopySSEHugeUnterminatedLineStopsReading: a multi-megabyte line with no
// newline crosses the cap mid-line. The relay must stop within one read
// buffer of the cap — not drain the 8 MiB source.
func TestCopySSEHugeUnterminatedLineStopsReading(t *testing.T) {
	input := strings.Repeat("x", 8<<20)
	out, read, err := copySSELimited(t, input)
	assertLimitBreach(t, err, ErrSSELineTooLong, out, "xxxx")
	if len(out) != 0 {
		t.Errorf("output = %d bytes, want none (partial line never written)", len(out))
	}
	if read > MaxLineBytes+sseReadBuffer {
		t.Errorf("read %d bytes; want ≤ %d (cap + one read buffer)",
			read, MaxLineBytes+sseReadBuffer)
	}
}

// TestCopySSEEventExactlyAtLimitDispatches: an event whose lines total
// exactly MaxEventBytes is inside the cap, and the blank line that
// dispatches it is excluded from the budget — it must terminate the event,
// not breach it. The second identical event pins that the budget resets at
// the boundary.
func TestCopySSEEventExactlyAtLimitDispatches(t *testing.T) {
	event := fullLine(MaxLineBytes) + fullLine(MaxLineBytes)
	input := event + "\n" + event + "\n"
	out, flushes := copySSEOnce(t, input, "public-name")
	if out != input {
		t.Errorf("event at cap corrupted: got %d bytes, want %d identical", len(out), len(input))
	}
	if flushes != 2 {
		t.Errorf("flushes = %d, want 2 (one per at-cap event)", flushes)
	}
}

// TestCopySSEEventOverLimitTruncatesBeforeOffendingLine: the first line that
// pushes the in-flight event past MaxEventBytes is relayed never — the
// already-relayed prefix stays, nothing after the breach is read.
func TestCopySSEEventOverLimitTruncatesBeforeOffendingLine(t *testing.T) {
	atCap := fullLine(MaxLineBytes) + fullLine(MaxLineBytes)
	crossing := "CROSSyyyyy\n"
	tail := strings.Repeat("z", 2<<20)
	out, read, err := copySSELimited(t, atCap+crossing+tail)
	assertLimitBreach(t, err, ErrSSEEventTooLarge, out, "CROSS")
	if out != atCap {
		t.Errorf("output = %d bytes, want the %d relayed before the breach", len(out), len(atCap))
	}
	if strings.Contains(out, "zzz") {
		t.Errorf("post-breach tail leaked to the client")
	}
	if read > MaxEventBytes+int64(len(crossing))+sseReadBuffer {
		t.Errorf("read %d bytes; want ≤ %d (cap + crossing line + one buffer)",
			read, MaxEventBytes+len(crossing)+sseReadBuffer)
	}
}

// TestCopySSEManySmallEvents: a long well-formed stream stays under every
// cap and dispatches one event per blank line, with the rewrite applied to
// every event.
func TestCopySSEManySmallEvents(t *testing.T) {
	const n = 1000
	var b strings.Builder
	for i := 0; i < n; i++ {
		fmt.Fprintf(&b, "data: {\"model\":\"upstream-name\",\"i\":%d}\n\n", i)
	}
	out, flushes := copySSEOnce(t, b.String(), "public-name")
	if flushes != n {
		t.Errorf("flushes = %d, want %d", flushes, n)
	}
	if got := strings.Count(out, `"model":"public-name"`); got != n {
		t.Errorf("rewritten events = %d, want %d", got, n)
	}
	if strings.Contains(out, "upstream-name") {
		t.Errorf("upstream name leaked past the rewrite")
	}
}

// TestCopySSEPartialFinalLineExactlyAtLimitRelayed: a final line without a
// terminator, exactly at the cap, is still relayed when EOF ends it.
func TestCopySSEPartialFinalLineExactlyAtLimitRelayed(t *testing.T) {
	input := strings.Repeat("x", MaxLineBytes-1)
	out, _, err := copySSELimited(t, input)
	if err != nil {
		t.Fatalf("partial final line at the exact cap rejected: %v", err)
	}
	if out != input {
		t.Errorf("partial final line at cap corrupted: got %d bytes, want %d", len(out), len(input))
	}
}

// TestCopySSEPartialFinalLineOverLimitTruncates: a final unterminated line
// one byte over the cap is rejected like any other oversized line.
func TestCopySSEPartialFinalLineOverLimitTruncates(t *testing.T) {
	input := strings.Repeat("x", MaxLineBytes) + "y"
	out, _, err := copySSELimited(t, input)
	assertLimitBreach(t, err, ErrSSELineTooLong, out, "xxxx")
	if len(out) != 0 {
		t.Errorf("output = %d bytes, want none", len(out))
	}
}

// TestCopySSEBlankLinesAreBudgetFree: blank lines dispatch events and never
// accrue against the event cap — a stream of empty events is legal.
func TestCopySSEBlankLinesAreBudgetFree(t *testing.T) {
	out, flushes := copySSEOnce(t, "\n\n\n", "public-name")
	if out != "\n\n\n" {
		t.Errorf("got %q, want byte-identical", out)
	}
	if flushes != 3 {
		t.Errorf("flushes = %d, want 3 (one per empty event)", flushes)
	}
}

// TestCopySSERewriteNearLimit: a data line far into the multi-buffer range
// still rewrites exactly like a small one — prefix, payload, and terminator
// intact, nothing else touched.
func TestCopySSERewriteNearLimit(t *testing.T) {
	payload := `{"model":"upstream-name","pad":"` + strings.Repeat("x", 512<<10) + `"}`
	input := "data: " + payload + "\n"
	out, _ := copySSEOnce(t, input, "public-name")
	if !strings.HasPrefix(out, `data: {"model":"public-name","pad":"`) {
		t.Fatalf("rewritten prefix wrong: %q", truncateForLog(out))
	}
	if !strings.HasSuffix(out, "\"}\n") {
		t.Errorf("terminator lost: %q", truncateForLog(out[len(out)-16:]))
	}
	var chunk map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSuffix(strings.TrimPrefix(out, "data: "), "\n")), &chunk); err != nil {
		t.Fatalf("rewritten payload not valid JSON: %v", err)
	}
	if chunk["model"] != "public-name" {
		t.Errorf("model = %v, want public-name", chunk["model"])
	}
}

// TestCopySSERewriteWithCRLFAtBoundary: CRLF terminators survive the rewrite
// and dispatch events like bare LF — the budget reset rides the same
// boundary.
func TestCopySSERewriteWithCRLFAtBoundary(t *testing.T) {
	input := "data: {\"model\":\"upstream-name\"}\r\n\r\n"
	out, flushes := copySSEOnce(t, input, "public-name")
	if out != "data: {\"model\":\"public-name\"}\r\n\r\n" {
		t.Errorf("CRLF event corrupted: %q", out)
	}
	if flushes != 1 {
		t.Errorf("flushes = %d, want 1", flushes)
	}
}

// Compile-time guard: countingReader is an io.Reader.
var _ io.Reader = (*countingReader)(nil)
