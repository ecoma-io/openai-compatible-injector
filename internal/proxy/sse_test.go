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
// the number of flush invocations.
func copySSEOnce(t *testing.T, input, public string) (string, int) {
	t.Helper()
	var buf bytes.Buffer
	flushes := 0
	if err := CopySSE(&buf, strings.NewReader(input), public, func() { flushes++ }); err != nil {
		t.Fatalf("CopySSE: %v", err)
	}
	return buf.String(), flushes
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

func (failingWriter) Write(p []byte) (int, error) { return 0, errors.New("boom") }

type failingReader struct{}

func (failingReader) Read(p []byte) (int, error) { return 0, errors.New("read boom") }

func TestCopySSEPropagatesErrors(t *testing.T) {
	if err := CopySSE(failingWriter{}, strings.NewReader("data: x\n"), "p", func() {}); err == nil {
		t.Error("write error not propagated")
	}
	if err := CopySSE(io.Discard, failingReader{}, "p", func() {}); err == nil || err == io.EOF {
		t.Errorf("read error not propagated as-is: %v", err)
	}
}
