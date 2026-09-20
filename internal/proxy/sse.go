package proxy

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
)

var (
	sseDataPrefix = []byte("data:")
	sseModelKey   = []byte(`"model"`)
)

// Bounded SSE input. Without caps, a single upstream line without a
// terminator — or one event made of many oversized lines — grows the relay's
// memory without bound until the process dies; the caps turn that into a
// clean truncation. They are generous (real provider events are tens of
// bytes; the largest legitimate lines are padded tool-call arguments) and
// exist only so a hostile peer runs into a wall instead of an OOM.
const (
	// MaxLineBytes caps one SSE line, terminator included.
	MaxLineBytes = 1 << 20 // 1 MiB
	// MaxEventBytes caps the bytes of the event currently being relayed:
	// every line since the most recent blank separator (or the start of
	// the stream), terminators included. The blank line that dispatches
	// the event is not part of its budget — it is the completion signal, so
	// an event exactly at the cap still terminates. Lines are relayed one
	// at a time, so this is the worst case one event can pin between reads
	// and the flush that dispatches it.
	MaxEventBytes = 2 << 20 // 2 MiB
)

// sseReadBuffer sizes the bufio.Reader CopySSE reads through. Lines longer
// than one buffer arrive as successive ErrBufferFull chunks, which the
// bounded line reader accumulates — up to MaxLineBytes — before rejecting.
const sseReadBuffer = 64 << 10

var (
	// ErrSSELineTooLong reports an upstream line crossing MaxLineBytes.
	// It is detected mid-line: the relay stops there, and the partial line
	// is never written to the client.
	ErrSSELineTooLong = errors.New("sse line exceeds limit")
	// ErrSSEEventTooLarge reports the event in flight crossing
	// MaxEventBytes. Like the line limit, it is detected before the
	// offending line is relayed.
	ErrSSEEventTooLarge = errors.New("sse event exceeds limit")
)

// CopySSE incrementally passes an upstream SSE stream through to dst,
// rewriting the model field inside data lines via the API-scoped rewriter
// the caller supplies (RewriteChatModel or RewriteResponsesModel — the
// stream must obey its API's rewrite scope exactly like the buffered path).
// It never buffers the whole stream: lines are read one at a time under
// hard caps (MaxLineBytes per line, MaxEventBytes per in-flight event),
// each line is written out immediately, and flush is invoked at every event
// boundary — the blank line that terminates an event, which is exactly what
// SSE clients dispatch on — so events reach the client promptly with no
// aggregation or reordering. Per-line flushing spends a write round trip
// per line without delivering anything a client can act on earlier.
//
// Only lines beginning with "data:" are candidates for rewriting, and only
// when the payload (bytes after "data:" plus one optional space) contains
// `"model"`; payload validation and the rewrite itself are delegated to the
// caller's rewriter, whose acceptance rule is identical to the buffered
// response path — a deliberate parity: a stream and a buffered body with
// the same JSON are rewritten identically. The line is re-emitted with the
// original prefix, separator, and line terminator. Comments, event lines,
// blank lines, and terminators such as [DONE] pass through byte-for-byte.
//
// Limit breaches stop the relay: the offending (partial) line is never
// written, no further reads happen, and the returned error wraps
// ErrSSELineTooLong or ErrSSEEventTooLarge so the caller can log the
// truncation cause. io.EOF ends the copy with a nil error; any other read
// error is returned so the caller can truncate the stream. A failure
// writing to dst is returned wrapped in *streamWriteError — the client side
// went away — so the caller can log the two truncation causes apart.
// Nothing is ever synthesized.
func CopySSE(dst io.Writer, src io.Reader, rewrite func(payload []byte) []byte, flush func()) (StreamStats, error) {
	var stats StreamStats
	br := bufio.NewReaderSize(src, sseReadBuffer)
	// pending counts the bytes of the event in flight — every line since
	// the last blank separator (or the start of the stream), terminators
	// included. The completing blank line never joins the
	// budget: it is the dispatch signal, so an event exactly at the cap
	// still terminates. Lines are relayed one at a time, so this is the
	// worst case one event can pin between reads and the flush.
	var pending int64
	for {
		line, rerr := readBoundedLine(br)
		if errors.Is(rerr, ErrSSELineTooLong) {
			// The line already crossed the cap; whatever remains is
			// unread and unwritten. Stop here — a torn line is worse
			// than a clean truncation.
			return stats, rerr
		}
		if len(line) > 0 {
			boundary := isEventBoundary(line)
			if !boundary {
				pending += int64(len(line))
				if pending > MaxEventBytes {
					return stats, fmt.Errorf("%w: event reached %d bytes, limit %d",
						ErrSSEEventTooLarge, pending, MaxEventBytes)
				}
			}
			out := rewriteSSELine(line, rewrite)
			n, werr := dst.Write(out)
			// Account exactly what dst accepted — on a failed or torn write
			// the stats say how much of the stream actually went out. A
			// short write with a nil error is the io.Writer contract's other
			// failure mode (io.ErrShortWrite); continuing past it would
			// relay a torn line and overcount, so it truncates too — as a
			// client-side failure, since dst is the client.
			stats.Bytes += int64(n)
			if werr != nil {
				return stats, &streamWriteError{err: werr}
			}
			if n < len(out) {
				return stats, &streamWriteError{err: io.ErrShortWrite}
			}
			if boundary {
				// Accounting and budget reset belong to the boundary, not to
				// the flush: stats.Events counts dispatched events and the
				// in-flight budget restarts per event whether or not this
				// caller asked for explicit flushes.
				if flush != nil {
					flush()
				}
				stats.Events++
				pending = 0
			}
		}
		if rerr != nil {
			if errors.Is(rerr, io.EOF) {
				return stats, nil
			}
			return stats, rerr
		}
	}
}

// readBoundedLine returns the next line from br, terminator included,
// mirroring bufio.Reader.ReadBytes: a final line without a terminator is
// returned together with io.EOF, and any other read error is returned as
// -is (with whatever partial line was read). A line that would exceed
// MaxLineBytes stops the read and returns an error wrapping
// ErrSSELineTooLong with the crossed thresholds — the accumulator never
// grows past the cap, so memory stays bounded no matter what the upstream
// sends.
func readBoundedLine(br *bufio.Reader) ([]byte, error) {
	var buf []byte
	for {
		chunk, err := br.ReadSlice('\n')
		if err == nil {
			if int64(len(buf)+len(chunk)) > MaxLineBytes {
				return nil, fmt.Errorf("%w: line reached %d bytes, limit %d",
					ErrSSELineTooLong, int64(len(buf))+int64(len(chunk)), MaxLineBytes)
			}
			return append(buf, chunk...), nil
		}
		if errors.Is(err, bufio.ErrBufferFull) {
			if int64(len(buf)+len(chunk)) > MaxLineBytes {
				return nil, fmt.Errorf("%w: line reached %d bytes, limit %d",
					ErrSSELineTooLong, int64(len(buf))+int64(len(chunk)), MaxLineBytes)
			}
			// chunk aliases bufio's internal buffer; it is invalid after the
			// next read, so it must be copied into the accumulator now.
			buf = append(buf, chunk...)
			continue
		}
		// io.EOF or a real read error; whatever has accumulated (plus the
		// partial chunk, if any) is the final unterminated line — exactly
		// at the cap it is still relayed, over it the line is rejected.
		if len(buf)+len(chunk) > 0 {
			if int64(len(buf)+len(chunk)) > MaxLineBytes {
				return nil, fmt.Errorf("%w: line reached %d bytes, limit %d",
					ErrSSELineTooLong, int64(len(buf))+int64(len(chunk)), MaxLineBytes)
			}
			return append(buf, chunk...), err
		}
		return nil, err
	}
}

// StreamStats reports what a finished CopySSE pass put on the wire: byte
// count and dispatched events. Metadata for the access log only — payloads
// never reach logs at any level.
type StreamStats struct {
	Bytes  int64
	Events int
}

// streamWriteError marks a CopySSE failure that happened writing to the
// client — the connection broke mid-stream — as opposed to a failure
// reading from upstream. The distinction decides the truncation log's
// phase field.
type streamWriteError struct{ err error }

func (e *streamWriteError) Error() string { return "writing SSE stream to client: " + e.err.Error() }
func (e *streamWriteError) Unwrap() error { return e.err }

// isEventBoundary reports whether the raw line (terminator included) is a
// blank line — the terminator that completes an SSE event.
func isEventBoundary(line []byte) bool {
	return len(line) == 1 && line[0] == '\n' ||
		len(line) == 2 && line[0] == '\r' && line[1] == '\n'
}

// rewriteSSELine applies the data-line rewrite rule to a single raw line,
// terminator included. Anything that is not a data line carrying a model
// string is returned unchanged. The rewriter is the API-scoped function the
// caller chose; its no-op contract (input returned unchanged when nothing
// is in scope) is what the pointer-identity shortcut below relies on.
func rewriteSSELine(line []byte, rewrite func(payload []byte) []byte) []byte {
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
	out := rewrite(payload)
	if &out[0] == &payload[0] {
		// The rewriter returned the input slice when nothing was in scope;
		// skip the rebuild for lines that merely mention "model".
		return line
	}
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
