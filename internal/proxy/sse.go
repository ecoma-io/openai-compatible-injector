package proxy

import (
	"bufio"
	"bytes"
	"encoding/json"
	"io"

	"openai-compatible-injector/internal/inject"
)

var (
	sseDataPrefix = []byte("data:")
	sseModelKey   = []byte(`"model"`)
)

// CopySSE incrementally passes an upstream SSE stream through to dst,
// rewriting the model field inside data lines from the upstream name to the
// public name. It never buffers the whole stream: lines are read one at a
// time with no size cap (providers pad lines), each line is written out
// immediately, and flush is invoked after every line so events reach the
// client promptly with no aggregation or reordering.
//
// Only lines beginning with "data:" are candidates for rewriting, and only
// when the payload (bytes after "data:" plus one optional space) both
// contains `"model"` and parses as a JSON object; the rewrite itself is
// delegated to inject.RewriteModel and re-emitted with the original
// prefix, separator, and line terminator. Comments, event lines, blank
// lines, and terminators such as [DONE] pass through byte-for-byte.
//
// io.EOF ends the copy with a nil error; any other read error is returned
// so the caller can truncate the stream. Nothing is ever synthesized.
func CopySSE(dst io.Writer, src io.Reader, public string, flush func()) error {
	br := bufio.NewReader(src)
	for {
		line, err := br.ReadBytes('\n')
		if len(line) > 0 {
			if _, werr := dst.Write(rewriteSSELine(line, public)); werr != nil {
				return werr
			}
			if flush != nil {
				flush()
			}
		}
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
	}
}

// rewriteSSELine applies the data-line rewrite rule to a single raw line,
// terminator included. Anything that is not a JSON-object data line carrying
// a model key is returned unchanged.
func rewriteSSELine(line []byte, public string) []byte {
	content, term := splitSSELineTerminator(line)
	rest, ok := bytes.CutPrefix(content, sseDataPrefix)
	if !ok {
		return line
	}
	prefix := content[:len(content)-len(rest)]
	sep := rest[:0]
	payload := rest
	if len(payload) > 0 && payload[0] == ' ' {
		sep = payload[:1]
		payload = payload[1:]
	}
	if !bytes.Contains(payload, sseModelKey) {
		return line
	}
	var obj map[string]any
	if err := json.Unmarshal(payload, &obj); err != nil {
		return line
	}
	out := inject.RewriteModel(payload, public)
	// Capacity is never precomputed as a length sum: that arithmetic is the
	// integer-overflow class CodeQL flags, and the per-line cost of append
	// growth is negligible next to the bufio read and network I/O anyway.
	var buf []byte
	buf = append(buf, prefix...)
	buf = append(buf, sep...)
	buf = append(buf, out...)
	buf = append(buf, term...)
	return buf
}

// splitSSELineTerminator splits a raw line into content and its "\n" or
// "\r\n" terminator. A final partial line has an empty terminator.
func splitSSELineTerminator(line []byte) (content, term []byte) {
	if len(line) >= 2 && line[len(line)-2] == '\r' && line[len(line)-1] == '\n' {
		return line[:len(line)-2], line[len(line)-2:]
	}
	if len(line) >= 1 && line[len(line)-1] == '\n' {
		return line[:len(line)-1], line[len(line)-1:]
	}
	return line, nil
}
