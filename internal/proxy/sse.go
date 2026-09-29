package proxy

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
)

var (
	sseDataPrefix  = []byte("data:")
	sseEventPrefix = []byte("event:")
	sseModelKey    = []byte(`"model"`)
	sseUsageKey    = []byte(`"usage"`)
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
// rewriting data-line payloads via the API-scoped rewriter the caller
// supplies (the model rewrite composed with, when its plan is active, the
// thinking-usage synthesis — the stream must obey its API's rewrite scope
// exactly like the buffered path). stripKeys widens the data-line acceptance
// gate: the stripped keys' first segments, so a line carrying a to-be-excised
// provider field without a "model"/"usage" key is still handed to the
// rewriter. It is the inject package's StripPatterns output; nil or empty is
// today's gate exactly, so an unconfigured deployment is byte-identical.
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
// `"model"` or `"usage"` — the two keys the caller's rewriter owns (the
// model rewrite always; the usage synthesis when its per-request plan is
// active) — or any of stripKeys (the strip rewriter's first-segment keys).
// Payload validation and the rewrite itself are delegated to the
// caller's rewriter, whose acceptance rule is identical to the buffered
// response path — a deliberate parity: a stream and a buffered body with
// the same JSON are rewritten identically. When the synthesis is inactive
// the composed rewriter returns its input unchanged, so a usage-only line
// takes the no-op shortcut and the wire stays byte-identical. The line is
// re-emitted with the original prefix, separator, and line terminator.
// Comments, event lines, blank lines, and terminators such as [DONE] pass
// through byte-for-byte.
//
// observe, when non-nil, is called with the event NAME and the payload of
// every data line — before the acceptance gate above, and therefore before
// the rewrite — and its return value is ignored. That is deliberately a
// different seam from rewriter: the gate is an optimization keyed on the two
// keys the REWRITER owns, and a payload that carries neither is exactly the
// case an observer of, say, assistant text deltas must still see. (A
// Responses `response.output_text.delta` event carries no `model` member at
// all, so composing an observer into rewriter would silently miss every token
// of a Responses stream.) A nil observe costs nothing: the unconfigured path
// does not parse the line twice. observe must not write to dst and must not
// block — it runs inline on the relay's goroutine.
//
// The name is the frame's event: line — the bytes after "event:" — or nil
// when the frame stated none. Nil and empty are different facts and both
// occur: a Chat frame has no event: line at all, while a Responses frame
// states one, and a safety gate refusing on a disagreement has to be able to
// tell "no name was stated" from "a name was stated and it was empty". That
// is why the parameter is a nil-able slice rather than a string, whose empty
// value cannot express the first. The name is a copy, valid for the duration
// of the call only.
//
// Limit breaches stop the relay: the offending (partial) line is never
// written, no further reads happen, and the returned error wraps
// ErrSSELineTooLong or ErrSSEEventTooLarge so the caller can log the
// truncation cause. io.EOF ends the copy with a nil error; any other read
// error is returned so the caller can truncate the stream. A failure
// writing to dst is returned wrapped in *streamWriteError — the client side
// went away — so the caller can log the two truncation causes apart.
func CopySSE(dst io.Writer, src io.Reader, rewrite func(payload []byte) []byte, flush func(), stripKeys [][]byte, observe func(name, payload []byte)) (StreamStats, error) {
	var stats StreamStats
	br := bufio.NewReaderSize(src, sseReadBuffer)
	// pending counts the bytes of the event in flight — every line since
	// the last blank separator (or the start of the stream), terminators
	// included. The completing blank line never joins the
	// budget: it is the dispatch signal, so an event exactly at the cap
	// still terminates. Lines are relayed one at a time, so this is the
	// worst case one event can pin between reads and the flush.
	var pending int64
	// A CR is a complete EventSource line ending in its own right. If its
	// following LF arrives in a later read, the LF is wire data but not a
	// second (blank) line. Remembering that one-byte ambiguity lets a lone CR
	// reach the observer and client immediately without inventing a boundary
	// when it later proves to have been CRLF.
	pendingCRLF := false
	// eventName is the name the CURRENT frame's event: line stated, or nil
	// when it stated none. An SSE frame's event: line precedes its data:
	// lines, so the observer can be handed both halves of the same frame —
	// which is the point: a payload and a name that disagree are a frame
	// with no proven content shape, and the safety gate must be able to see
	// the disagreement rather than only the half that agrees.
	//
	// Copied, never aliased: the line buffer is reused by the next read, and
	// the observer may retain what it is given.
	var eventName []byte
	for {
		line, rerr := readBoundedLine(br)
		if errors.Is(rerr, ErrSSELineTooLong) {
			// The line already crossed the cap; whatever remains is
			// unread and unwritten. Stop here — a torn line is worse
			// than a clean truncation.
			return stats, rerr
		}
		if len(line) > 0 {
			// An immediately following LF belongs to the CR line already written.
			// Relay the byte exactly, but do not run it through line accounting,
			// observation, rewriting, terminal detection, or boundary flushing.
			if pendingCRLF && bytes.Equal(line, []byte("\n")) {
				n, werr := dst.Write(line)
				stats.Bytes += int64(n)
				if werr != nil {
					return stats, &streamWriteError{err: werr}
				}
				if n < len(line) {
					return stats, &streamWriteError{err: io.ErrShortWrite}
				}
				pendingCRLF = false
			} else {
				boundary := isEventBoundary(line)
				if boundary {
					// The frame is over: the next event: line belongs to the
					// next frame, not to a late data line of this one.
					eventName = nil
				} else {
					pending += int64(len(line))
					if pending > MaxEventBytes {
						return stats, fmt.Errorf("%w: event reached %d bytes, limit %d",
							ErrSSEEventTooLarge, pending, MaxEventBytes)
					}
					if name, ok := sseEventName(line); ok {
						// A second event: line in the same frame is the LAST one
						// winning, which is what the EventSource specification
						// says the field means. Copied, never aliased.
						//
						// The copy is into a non-nil empty slice, not
						// append([]byte(nil), ...): a stated but EMPTY name and
						// no name at all are different wire facts, and
						// append(nil) of an empty slice yields nil, which would
						// collapse them into one indistinguishable value at the
						// assignment site — undoing what sseEventName took care
						// to preserve. Today the gate refuses both under the
						// same token, so nothing observable depends on it; the
						// seam is what future callers read, and it should not
						// promise a distinction it cannot deliver.
						eventName = append([]byte{}, name...)
					}
				}
				if observe != nil {
					if payload, ok := sseDataPayload(line); ok {
						observe(eventName, payload)
					}
				}
				out := rewriteSSELine(line, rewrite, stripKeys)
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
				// The terminal fact is recorded on the bytes that actually
				// reached the client — the same buffer the write was handed —
				// never on a line the relay read but could not deliver.
				if isTerminalSSELine(out) {
					stats.Terminal = true
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
				pendingCRLF = len(line) > 0 && line[len(line)-1] == '\r'
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

// readToLineEnd reads one SSE line from br, stopping at CR, LF, or CRLF. It
// is a drop-in for ReadSlice('\n') that recognizes all three line endings the
// SSE grammar admits. The returned slice includes the terminator (one or two
// bytes) exactly as it appeared on the wire; a final partial line returns it
// together with io.EOF, matching ReadSlice's contract for a line that ran out
// mid-read.
//
// Peeking and copying keeps the scanner byte-exact. bufio exposes no CR
// terminator, so the line has to be cut here, and a copy is required because
// the peeked slice is invalidated by the next read. bufio.ErrBufferFull is
// returned unchanged, so readBoundedLine remains the single place a line's
// size against the cap is measured.
func readToLineEnd(br *bufio.Reader) ([]byte, error) {
	// Peek fills the buffer when it is short, so n>=1 guarantees progress
	// and an empty result can only be EOF. n<0 asks for no fill and would
	// spin, so the floor of 1 is load-bearing: the line data is whatever is
	// buffered, plus at least one more byte if the buffer is empty.
	n := br.Buffered()
	if n < 1 {
		n = 1
	}
	b, err := br.Peek(n)
	if err != nil && len(b) == 0 {
		return nil, err
	}
	// An LF anywhere in the buffered window ends the line: every byte
	// before it is line data whatever terminators precede it, so the whole
	// line is one ReadSlice and a CRLF needs no special case.
	if bytes.IndexByte(b, '\n') >= 0 {
		chunk, rerr := br.ReadSlice('\n')
		// ErrBufferFull is impossible here — the window held an LF, and
		// the window is the whole buffer — so any error is the reader's
		// own and propagates.
		return chunk, rerr
	}
	// No LF buffered. A CR is itself a complete line ending. Do not wait for
	// the next byte to learn whether it grows into CRLF: a peer may legally
	// pause forever after a lone CR, and holding that complete line would leave
	// its client-visible event unflushed. CopySSE remembers the ambiguity and
	// treats one later LF as part of this line rather than a new blank line.
	if i := bytes.IndexByte(b, '\r'); i >= 0 {
		chunk := append([]byte(nil), b[:i+1]...)
		if _, err := br.Discard(i + 1); err != nil {
			return chunk, err
		}
		return chunk, nil
	}
	// No terminator in the window: it is all line data. Read it out
	// (bufio returns the error only once its buffer is drained) and let
	// readBoundedLine accumulate; ErrBufferFull is the "still no
	// terminator, keep going" signal, unchanged from ReadSlice.
	chunk := make([]byte, len(b))
	if _, rerr := br.Read(chunk); rerr != nil {
		return nil, rerr
	}
	return chunk, bufio.ErrBufferFull
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
		chunk, err := readToLineEnd(br)
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
// count, dispatched events, and whether the stream was terminated. Metadata
// for the access log only — payloads never reach logs at any level.
type StreamStats struct {
	Bytes  int64
	Events int
	// Terminal reports that one of the two terminal markers reached the
	// client: chat's `data: [DONE]`, responses' `event: response.completed`.
	// It is the fact that separates a stream that ENDED from a stream that
	// was CUT — CopySSE maps io.EOF to a nil error in both cases (an
	// upstream that closes a finished stream and one that closes a dying
	// generation look identical on the wire), so the caller cannot tell them
	// apart from the error alone. Once set it stays set: a stream cannot
	// un-terminate.
	Terminal bool
}

// streamWriteError marks a CopySSE failure that happened writing to the
// client — the connection broke mid-stream — as opposed to a failure
// reading from upstream. The distinction decides the truncation log's
// phase field.
type streamWriteError struct{ err error }

func (e *streamWriteError) Error() string { return "writing SSE stream to client: " + e.err.Error() }
func (e *streamWriteError) Unwrap() error { return e.err }

// isTerminalSSELine reports the two terminal markers this proxy serves.
// Chat's terminal payload is literally [DONE]. Responses identifies the
// terminal envelope with its event name; recognizing it at the event line
// (rather than waiting for its data line or EOF) makes the guarantee
// stronger: no comment can appear in the middle of, or after, the terminal
// event, and the relay can report a terminated stream without waiting for
// the read that follows it.
//
// It lives beside CopySSE rather than beside the heartbeat that first needed
// it, because the relay is where the fact is RECORDED (StreamStats.Terminal)
// and the heartbeat is only where it was first USED — the predicate is a
// property of the wire, not of keep-alive. The bytes are never touched.
func isTerminalSSELine(b []byte) bool {
	content, _ := splitSSELineTerminator(b)
	return bytes.Equal(content, []byte("data: [DONE]")) || bytes.Equal(content, []byte("event: response.completed"))
}

// isEventBoundary reports whether the raw line (terminator included) is a
// blank line — the terminator that completes an SSE event.
func isEventBoundary(line []byte) bool {
	return len(line) == 1 && (line[0] == '\n' || line[0] == '\r') ||
		len(line) == 2 && line[0] == '\r' && line[1] == '\n'
}

// rewriteSSELine applies the data-line rewrite rule to a single raw line,
// terminator included. Anything that is not a data line carrying a model,
// usage, or strip key is returned unchanged. The rewriter is the composed
// function the caller chose; its no-op contract (input returned unchanged
// when nothing is in scope) is what the pointer-identity shortcut below
// relies on.
func rewriteSSELine(line []byte, rewrite func(payload []byte) []byte, stripKeys [][]byte) []byte {
	payload, ok := sseDataPayload(line)
	if !ok {
		return line
	}
	if !bytes.Contains(payload, sseModelKey) && !bytes.Contains(payload, sseUsageKey) && !mentionsAnyKey(payload, stripKeys) {
		return line
	}
	out := rewrite(payload)
	// The pointer comparison identifies the rewriter's no-op contract: the
	// input returned unchanged when nothing was in scope. The length guard
	// keeps the index safe — a zero-length result would panic on &out[0] —
	// and rules out an aliasing coincidence (same pointer, different span)
	// being mistaken for identity.
	if len(out) == len(payload) && &out[0] == &payload[0] {
		// Skip the rebuild for lines that merely mention a gate key.
		return line
	}
	content, term := splitSSELineTerminator(line)
	// head is everything the payload sits behind — "data:" plus the optional
	// separator space — recovered by length so the exact bytes are reused
	// rather than re-derived from a case analysis.
	head := content[:len(content)-len(payload)]
	// Capacity is never precomputed as a length sum: that arithmetic is the
	// integer-overflow class CodeQL flags, and the per-line cost of append
	// growth is negligible next to the bufio read and network I/O anyway.
	var buf []byte
	buf = append(buf, head...)
	buf = append(buf, out...)
	buf = append(buf, term...)
	return buf
}

// sseDataPayload returns the payload of a data line — the bytes after
// "data:" and one optional separator space — and reports whether the line is
// a data line at all. This is the ONE place the SSE data-line prefix is
// parsed: the relay's observer hook, the rewrite, and the terminal check all
// read lines through it or through splitSSELineTerminator, so a change to
// what "a data line" means cannot diverge between them.
func sseDataPayload(line []byte) ([]byte, bool) {
	content, _ := splitSSELineTerminator(line)
	payload, ok := bytes.CutPrefix(content, sseDataPrefix)
	if !ok {
		return nil, false
	}
	if len(payload) > 0 && payload[0] == ' ' {
		payload = payload[1:]
	}
	return payload, true
}

// sseEventName returns the event name a raw "event:" line carries — the bytes
// after "event:" and one optional separator space — and reports whether the
// line is an event line at all. An event line with an empty name is reported
// as an event line carrying an EMPTY name, not as no name at all: the two
// mean different things to the safety gate, and a name that was stated and
// came out empty is a disagreement rather than a silence.
//
// This is the ONE place the SSE event-line prefix is parsed, for the same
// reason sseDataPayload is the one place the data prefix is.
func sseEventName(line []byte) ([]byte, bool) {
	content, _ := splitSSELineTerminator(line)
	name, ok := bytes.CutPrefix(content, sseEventPrefix)
	if !ok {
		return nil, false
	}
	if len(name) > 0 && name[0] == ' ' {
		name = name[1:]
	}
	return name, true
}

// mentionsAnyKey reports whether payload contains any of the strip
// first-segment key bytes. A nil or empty list returns false immediately —
// the length check keeps the unconfigured path to exactly today's two
// Contains probes in rewriteSSELine.
func mentionsAnyKey(payload []byte, keys [][]byte) bool {
	for _, k := range keys {
		if bytes.Contains(payload, k) {
			return true
		}
	}
	return false
}

// splitSSELineTerminator splits a raw line into content and its "\n",
// "\r", or "\r\n" terminator. A final partial line has an empty terminator.
func splitSSELineTerminator(line []byte) (content, term []byte) {
	if len(line) >= 2 && line[len(line)-2] == '\r' && line[len(line)-1] == '\n' {
		return line[:len(line)-2], line[len(line)-2:]
	}
	if len(line) >= 1 && (line[len(line)-1] == '\n' || line[len(line)-1] == '\r') {
		return line[:len(line)-1], line[len(line)-1:]
	}
	return line, nil
}
