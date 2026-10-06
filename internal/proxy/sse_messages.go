package proxy

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"

	"openai-compatible-injector/internal/inject"
)

// CopyMessagesSSE relays the Anthropic Messages stream, which is not a
// passthrough of anything: it re-emits an upstream Chat Completions stream as
// Anthropic events.
//
// It exists beside CopySSE rather than inside it because the two dialects do
// not share a line shape. CopySSE's seam is one data payload in, one payload
// out, with the line's own prefix and terminator preserved — a contract
// OpenAI's stream can satisfy and Anthropic's cannot. One OpenAI chunk can
// owe three Anthropic events (close the open block, start the next, carry the
// delta); every Anthropic event needs an `event:` line this relay has to
// invent; and the terminal is `event: message_stop`, a name the upstream
// never sends. Widening CopySSE's gate would have left all three unreachable.
//
// What IS shared is everything that bounds the relay, and it is shared by
// construction rather than by copy: the same readBoundedLine and the same
// MaxLineBytes/MaxEventBytes caps, so a hostile upstream runs into the same
// wall and produces the same ErrSSELineTooLong / ErrSSEEventTooLarge outcomes;
// the same boundary predicate driving flush-per-event and the in-flight
// budget reset; the same StreamStats with Terminal recorded through the same
// one-byte predicate, so the keep-alive heartbeat and the continuation gate
// learn a Messages stream terminated without knowing this function exists.
//
// The one structural difference from CopySSE's loop is that this relay never
// forwards a raw line. Input lines exist only to be counted against the caps
// and to have their data payload handed to the translator, so there is no
// pendingCRLF bookkeeping here: a lone CR's line terminator cannot leak into
// the output, since the output's terminators are the ones the frame builder
// wrote.
//
// Frames go out ONE LINE PER WRITE, which is not a style choice. pingWriter
// infers the stream's position from the tail of each buffer it is handed
// (endsWithBlankLine) and latches `finished` when a write's bytes are
// terminal, so a multi-line buffer would let one Write hide a terminal event
// from both, and a keep-alive ping could follow message_stop.
func CopyMessagesSSE(dst io.Writer, src io.Reader, rewrite func(payload []byte) []byte, tr *inject.ChatToMessagesStream, flush func(), observe func(name, payload []byte)) (StreamStats, error) {
	var stats StreamStats
	br := bufio.NewReaderSize(src, sseReadBuffer)
	// pending counts the bytes of the INPUT event in flight. The budget
	// bounds what this relay holds, and the only bytes it holds from an
	// upstream event are the line currently being read — so the counter is
	// the input accounting, while stats.Events counts the Anthropic events
	// actually dispatched.
	var pending int64
	// eventName is the CURRENT input frame's event: line, copied for the
	// same reason CopySSE copies it: the read buffer is reused by the next
	// read and the observer may retain what it is given. A Chat stream
	// states none, so this is nil on every real Messages request — but the
	// observer contract is the same one CopySSE offers, and a safety gate
	// needs to be able to see a name and a payload that disagree.
	var eventName []byte

	for {
		line, rerr := readBoundedLine(br)
		if errors.Is(rerr, ErrSSELineTooLong) {
			return stats, rerr
		}
		if len(line) > 0 {
			if isEventBoundary(line) {
				eventName = nil
				pending = 0
			} else {
				pending += int64(len(line))
				if pending > MaxEventBytes {
					return stats, fmt.Errorf("%w: event reached %d bytes, limit %d",
						ErrSSEEventTooLarge, pending, MaxEventBytes)
				}
				if name, ok := sseEventName(line); ok {
					eventName = append([]byte{}, name...)
				}
			}
			if observe != nil {
				if payload, ok := sseDataPayload(line); ok {
					observe(eventName, payload)
				}
			}
			// Only data lines carry a payload the translator can read. An
			// event: line, a comment or a blank line owes nothing, and the
			// upstream's own line terminator is deliberately not forwarded:
			// the Anthropic stream has its own framing.
			if payload, ok := sseDataPayload(line); ok {
				// rewrite is the SAME composed rewriter the buffered path and
				// CopySSE run — the model rename, the thinking-usage synthesis
				// and the field strip, in that order — so a streamed Messages
				// answer is transformed exactly like a buffered one. Strip needs
				// no stripKeys widening here: it runs inside rewriteOut, and the
				// translator never sees a stripped member.
				//
				// The usage capture rides the same wrapper, so metering still
				// reads pre-rewrite upstream bytes — before the rename that
				// makes the model client-facing, and before the translator
				// turns them into Anthropic events.
				if werr := writeFrames(dst, &stats, tr.Frame(rewrite(payload)), flush); werr != nil {
					return stats, werr
				}
			}
		}
		if rerr != nil {
			if errors.Is(rerr, io.EOF) {
				// A CLEAN end of input is the only place Finish() is owed:
				// an upstream that closed a finished stream and one that closed
				// a dying generation look identical on the wire, but only the
				// first earned a terminal. A read error returns above without
				// this, leaving the client's stream exactly as unterminated as
				// it would have been — no synthesized message_stop for a stream
				// that did not earn one.
				if werr := writeFrames(dst, &stats, tr.Finish(), flush); werr != nil {
					return stats, werr
				}
				return stats, nil
			}
			return stats, rerr
		}
	}
}

// writeFrames emits one translated buffer, one line per Write, and keeps the
// three per-line facts CopySSE records: the bytes dst actually accepted, the
// terminal marker on those bytes, and the event dispatch that a trailing
// blank line signals.
//
// Lines are cut by index rather than split and re-joined: every element of a
// Split is a subslice of frames, and appending a terminator onto one would
// write past its length into the shared backing array. Each line here is
// written as it already sits in frames, terminator included, so the output
// bytes are the frame builder's bytes exactly.
//
// Cutting on '\n' is safe because the frame builder encodes every payload
// with the non-HTML-escaping encoder and no JSON encoding contains a raw
// newline — a delta whose text contains one cannot break the wire framing.
func writeFrames(dst io.Writer, stats *StreamStats, frames []byte, flush func()) error {
	for len(frames) > 0 {
		var line []byte
		if i := bytes.IndexByte(frames, '\n'); i >= 0 {
			line, frames = frames[:i+1], frames[i+1:]
		} else {
			line, frames = frames, nil
		}
		n, werr := dst.Write(line)
		stats.Bytes += int64(n)
		if werr != nil {
			return &streamWriteError{err: werr}
		}
		if n < len(line) {
			return &streamWriteError{err: io.ErrShortWrite}
		}
		// Recorded on the bytes that reached the client, never on a line the
		// relay built but could not deliver. Both predicates strip the
		// terminator themselves, so a line is passed whole.
		if isTerminalSSELine(line) {
			stats.Terminal = true
		}
		if isEventBoundary(line) {
			if flush != nil {
				flush()
			}
			stats.Events++
		}
	}
	return nil
}
