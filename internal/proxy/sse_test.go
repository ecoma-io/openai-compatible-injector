package proxy

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"
)

// copySSEOnce runs CopySSE over input and returns the exact output bytes and
// the number of flush invocations. The reported stats are checked against
// the output on every call.
func copySSEOnce(t *testing.T, input, public string) (string, int) {
	t.Helper()
	var buf bytes.Buffer
	flushes := 0
	stats, err := CopySSE(&buf, strings.NewReader(input), public, func() { flushes++ })
	if err != nil {
		t.Fatalf("CopySSE: %v", err)
	}
	out := buf.String()
	if stats.Bytes != int64(len(out)) {
		t.Fatalf("stats.Bytes = %d, want %d (output length)", stats.Bytes, len(out))
	}
	if stats.Events != flushes {
		t.Fatalf("stats.Events = %d, want %d (flush count)", stats.Events, flushes)
	}
	return out, flushes
}

func TestCopySSEVerbatimPassthrough(t *testing.T) {
	input := ": keep-alive comment\n" +
		"event: done\n" +
		"data: [DONE]\n" +
		"data: hello world\n" +
		"\n" +
		"id: 42\n"
	out, flushes := copySSEOnce(t, input, "public-name")
	if out != input {
		t.Errorf("verbatim passthrough mismatch:\n got %q\nwant %q", out, input)
	}
	if flushes != 1 {
		t.Errorf("flushes = %d, want 1 (one per event boundary)", flushes)
	}
}

func TestCopySSERewritesModelOnlyInsideDataLines(t *testing.T) {
	input := "data: {\"model\":\"upstream-name\",\"x\":1}\n" +
		"event: {\"model\":\"upstream-name\"}\n" +
		"data: [DONE]\n"
	out, _ := copySSEOnce(t, input, "public-name")
	lines := strings.Split(out, "\n")

	var chunk map[string]any
	if err := json.Unmarshal([]byte(strings.TrimPrefix(lines[0], "data: ")), &chunk); err != nil {
		t.Fatalf("first line payload not valid JSON: %v (%q)", err, lines[0])
	}
	if chunk["model"] != "public-name" {
		t.Errorf("data line model = %v, want public-name", chunk["model"])
	}

	if lines[1] != "event: {\"model\":\"upstream-name\"}" {
		t.Errorf("event line rewritten: %q", lines[1])
	}
	if lines[2] != "data: [DONE]" {
		t.Errorf("[DONE] line changed: %q", lines[2])
	}
}

func TestCopySSEPreservesMissingSpaceSeparator(t *testing.T) {
	input := "data:{\"model\":\"upstream-name\",\"a\":2}\n"
	out, _ := copySSEOnce(t, input, "public-name")
	if strings.HasPrefix(out, "data: ") {
		t.Errorf("separator was inserted where none existed: %q", out)
	}
	line := strings.TrimSuffix(out, "\n")
	var chunk map[string]any
	if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "data:")), &chunk); err != nil {
		t.Fatalf("payload not valid JSON: %v (%q)", err, line)
	}
	if chunk["model"] != "public-name" {
		t.Errorf("model = %v, want public-name", chunk["model"])
	}
}

func TestCopySSEUnparseableDataLineUnchanged(t *testing.T) {
	inputs := []string{
		"data: {truncated\n",
		"data: {\"model\": \"unterminated\n",
		"data: [1,2,3]\n",
		"data: \"a string\"\n",
	}
	for _, input := range inputs {
		out, _ := copySSEOnce(t, input, "public-name")
		if out != input {
			t.Errorf("input %q: got %q, want byte-identical", input, out)
		}
	}
}

func TestCopySSENoModelLineByteIdentical(t *testing.T) {
	input := "data: {\"temperature\":0.7,\"top_p\":1}\n" +
		"retry: 100\n"
	out, _ := copySSEOnce(t, input, "public-name")
	if out != input {
		t.Errorf("got %q, want byte-identical %q", out, input)
	}
}

// TestCopySSEFlushPerEvent pins the flush granularity: one flush per event
// boundary (the blank line that terminates an event) — exactly what SSE
// clients dispatch on. Lines inside an event are written immediately but
// not individually flushed; flushing them buys no earlier client dispatch.
func TestCopySSEFlushPerEvent(t *testing.T) {
	input := "data: a\n\nid: b\ndata: [DONE]\n"
	out, flushes := copySSEOnce(t, input, "public-name")
	if out != input {
		t.Errorf("output corrupted: got %q want %q", out, input)
	}
	if flushes != 1 {
		t.Errorf("flushes = %d, want 1 (single event, flushed at its blank line)", flushes)
	}

	twoEvents := "event: a\ndata: {\"x\":1}\n\nevent: b\ndata: {\"x\":2}\n\n"
	_, flushes = copySSEOnce(t, twoEvents, "public-name")
	if flushes != 2 {
		t.Errorf("flushes = %d, want 2 (one per event)", flushes)
	}
}

func TestCopySSERewritesPayloadTheBufferedPathRewrites(t *testing.T) {
	// The rewrite gate must accept every payload the buffered path accepts:
	// a JSON number that overflows float64 (1e400) is valid JSON and its
	// top-level model is in scope. A full-decode gate would reject it and
	// leak the upstream name where a non-streaming request would not.
	input := "data: {\"model\":\"upstream-name\",\"n\":1e400}\n"
	out, _ := copySSEOnce(t, input, "public-name")
	if out != "data: {\"model\":\"public-name\",\"n\":1e400}\n" {
		t.Errorf("overflow-number payload not rewritten: %q", out)
	}

	// A top-level array is out of rewrite scope on BOTH paths (the rewrite
	// is top-level + response.model); pinned so stream and buffered paths
	// stay consistent.
	arrays := "data: [{\"model\":\"upstream-name\"}]\n"
	out, _ = copySSEOnce(t, arrays, "public-name")
	if out != arrays {
		t.Errorf("array payload must pass through untouched: %q", out)
	}
}

func TestCopySSEPartialFinalLine(t *testing.T) {
	// EOF with no trailing newline: the final partial line is still
	// forwarded (no flush — it is not an event boundary; the response
	// completes and net/http delivers the tail when the handler returns).
	out, flushes := copySSEOnce(t, "data: tail", "public-name")
	if out != "data: tail" {
		t.Errorf("got %q, want %q", out, "data: tail")
	}
	if flushes != 0 {
		t.Errorf("flushes = %d, want 0 (no event boundary seen)", flushes)
	}

	// Rewrite applies to a partial final data line too.
	out, _ = copySSEOnce(t, "data: {\"model\":\"upstream-name\",\"z\":9}", "public-name")
	var chunk map[string]any
	if err := json.Unmarshal([]byte(strings.TrimPrefix(out, "data: ")), &chunk); err != nil {
		t.Fatalf("payload not valid JSON: %v (%q)", err, out)
	}
	if chunk["model"] != "public-name" {
		t.Errorf("model = %v, want public-name", chunk["model"])
	}
}

type failingWriter struct{}

// limitedWriter accepts only limit bytes in total across all writes,
// reporting exactly what it accepted (and an error once the limit is hit).
// It exists to pin the partial-write contract: the bytes a dst actually
// accepted are accounted, and a short write is a stream truncation, not a
// silent success.
type limitedWriter struct {
	limit   int
	written int
}

func (w *limitedWriter) Write(p []byte) (int, error) {
	n := len(p)
	if remaining := w.limit - w.written; n > remaining {
		n = remaining
	}
	w.written += n
	if n < len(p) {
		return n, errors.New("simulated write failure")
	}
	return n, nil
}

// shortWriter succeeds but reports fewer bytes than it was given — the
// io.Writer contract's second failure mode, which must not be treated as a
// complete write.
type shortWriter struct {
	quiet int // writes to swallow before going short
}

func (w *shortWriter) Write(p []byte) (int, error) {
	if w.quiet > 0 {
		w.quiet--
		return len(p), nil
	}
	return len(p) / 2, nil
}

func (failingWriter) Write(p []byte) (int, error) { return 0, errors.New("boom") }

type failingReader struct{}

func (failingReader) Read(p []byte) (int, error) { return 0, errors.New("read boom") }

func TestCopySSEPropagatesErrors(t *testing.T) {
	// A write failure is client-side; the caller logs it as a client
	// disconnect via the *streamWriteError marker.
	_, err := CopySSE(failingWriter{}, strings.NewReader("data: x\n"), "p", func() {})
	if err == nil {
		t.Fatal("write error not propagated")
	}
	var swe *streamWriteError
	if !errors.As(err, &swe) {
		t.Errorf("write error not marked *streamWriteError: %v", err)
	}

	// A read failure is upstream-side and must NOT carry the marker — the
	// truncation phase in the access log depends on the distinction.
	_, err = CopySSE(io.Discard, failingReader{}, "p", func() {})
	if err == nil || err == io.EOF {
		t.Errorf("read error not propagated as-is: %v", err)
	}
	if errors.As(err, &swe) {
		t.Errorf("read error wrongly marked as client write failure: %v", err)
	}
}

// TestCopySSEAccountsPartialWrite pins the stats contract on the failure
// path: bytes dst actually accepted before failing are counted in
// stats.Bytes, not lost — the access log must report what went on the wire,
// and the write is still marked as a client-side failure.
func TestCopySSEAccountsPartialWrite(t *testing.T) {
	w := &limitedWriter{limit: 5}
	stats, err := CopySSE(w, strings.NewReader("data: x\ndata: y\n"), "p", nil)
	if err == nil {
		t.Fatal("write failure not propagated")
	}
	var swe *streamWriteError
	if !errors.As(err, &swe) {
		t.Errorf("partial write not marked *streamWriteError: %v", err)
	}
	if stats.Bytes != 5 {
		t.Errorf("stats.Bytes = %d, want 5 (the bytes dst accepted)", stats.Bytes)
	}
}

// TestCopySSERejectsShortWrite pins the second io.Writer failure mode: a
// write that returns n < len(p) with a nil error is an io.ErrShortWrite and
// must truncate the stream as a client-side failure — continuing past it
// would relay a torn line and overcount the bytes.
func TestCopySSERejectsShortWrite(t *testing.T) {
	stats, err := CopySSE(&shortWriter{}, strings.NewReader("data: x\ndata: y\n"), "p", nil)
	if err == nil {
		t.Fatal("short write not detected")
	}
	var swe *streamWriteError
	if !errors.As(err, &swe) {
		t.Errorf("short write not marked *streamWriteError: %v", err)
	}
	if !errors.Is(err, io.ErrShortWrite) {
		t.Errorf("short write error = %v, want io.ErrShortWrite", err)
	}
	if stats.Bytes+int64(len("data: x\n")) > int64(len("data: x\ndata: y\n")) {
		t.Errorf("stats.Bytes = %d exceeds the accepted input", stats.Bytes)
	}
}
