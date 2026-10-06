package proxy

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"

	"openai-compatible-injector/internal/inject"
)

// The relay that serves the Anthropic Messages surface: an upstream Chat
// stream in, Anthropic events out. These tests pin the four properties the
// rest of the service reads off it — the event sequence, the stats, the
// per-event flush, and the terminal fact — plus the boundaries it shares with
// CopySSE and the two things that must never appear: a synthesized terminal
// for a stream that did not earn one, and a ping after message_stop.

// upstreamChatStream is a complete, healthy chat SSE stream: a role chunk,
// one text delta, the finish chunk with usage, and [DONE].
const upstreamChatStream = "data: {\"id\":\"chatcmpl-1\",\"model\":\"upstream-name\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\"}}]}\n\n" +
	"data: {\"id\":\"chatcmpl-1\",\"model\":\"upstream-name\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"PONG\"}}]}\n\n" +
	"data: {\"id\":\"chatcmpl-1\",\"model\":\"upstream-name\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":3,\"completion_tokens\":4,\"total_tokens\":7}}\n\n" +
	"data: [DONE]\n\n"

// copyMessages runs CopyMessagesSSE over input with a fresh translator and
// returns the stats, the exact relayed bytes and the error.
func copyMessages(t *testing.T, input string) (StreamStats, string, error) {
	t.Helper()
	var buf bytes.Buffer
	stats, err := CopyMessagesSSE(&buf, strings.NewReader(input), sseRewriter("test-model"),
		inject.NewChatToMessagesStream("test-model"), nil, nil)
	return stats, buf.String(), err
}

// TestCopyMessagesSSETranslatesAWholeStream is the end-to-end shape: the full
// ordered Anthropic event list, with no `data: [DONE]` anywhere (that marker
// belongs to the upstream dialect and has no place in the client's stream).
func TestCopyMessagesSSETranslatesAWholeStream(t *testing.T) {
	stats, out, err := copyMessages(t, upstreamChatStream)
	if err != nil {
		t.Fatalf("CopyMessagesSSE: %v", err)
	}
	if !stats.Terminal {
		t.Fatal("a stream that reached message_stop was not marked terminal")
	}

	wantEvents := []string{
		"message_start",
		"content_block_start",
		"content_block_delta",
		"content_block_stop",
		"message_delta",
		"message_stop",
	}
	var gotEvents []string
	for _, line := range strings.Split(out, "\n") {
		if name, ok := strings.CutPrefix(line, "event: "); ok {
			gotEvents = append(gotEvents, name)
		}
	}
	if strings.Join(gotEvents, ",") != strings.Join(wantEvents, ",") {
		t.Fatalf("events = %v\nwant  = %v\nbody:\n%s", gotEvents, wantEvents, out)
	}
	if strings.Contains(out, "[DONE]") {
		t.Fatalf("the upstream's [DONE] reached the client: %s", out)
	}

	// The public model is the client's own name, and the upstream's model
	// name never survives the rename.
	if !strings.Contains(out, `"model":"test-model"`) {
		t.Fatalf("message_start did not carry the public model: %s", out)
	}
	if strings.Contains(out, "upstream-name") {
		t.Fatalf("the upstream model name reached the client: %s", out)
	}
	// The real counts ride message_delta, not message_start.
	if !strings.Contains(out, `"usage":{"input_tokens":3,"output_tokens":4}`) {
		t.Fatalf("message_delta did not carry the reported usage: %s", out)
	}
	if stats.Bytes != int64(len(out)) {
		t.Fatalf("stats.Bytes = %d, want %d", stats.Bytes, len(out))
	}
	// One dispatched Anthropic event per frame, and the trailing blank line
	// of the last frame counts too.
	if stats.Events != len(wantEvents) {
		t.Fatalf("stats.Events = %d, want %d", stats.Events, len(wantEvents))
	}
}

// Flush happens once per dispatched Anthropic event, not once per upstream
// line: one upstream chunk can owe three events, and the client dispatches
// on the blank line, so the flush must follow the frames.
func TestCopyMessagesSSEFlushesPerAnthropicEvent(t *testing.T) {
	var buf bytes.Buffer
	flushes := 0
	stats, err := CopyMessagesSSE(&buf, strings.NewReader(upstreamChatStream), sseRewriter("test-model"),
		inject.NewChatToMessagesStream("test-model"), func() { flushes++ }, nil)
	if err != nil {
		t.Fatalf("CopyMessagesSSE: %v", err)
	}
	if flushes != stats.Events {
		t.Fatalf("flushes = %d, want %d (one per dispatched event)", flushes, stats.Events)
	}
	if flushes != 6 {
		t.Fatalf("flushes = %d, want 6 (message_start, block start, block delta, block stop, message_delta, message_stop)", flushes)
	}
}

// A nil flush still counts events: the accounting is boundary-driven, not
// flush-driven, exactly as in CopySSE.
func TestCopyMessagesSSENilFlushStillCountsEvents(t *testing.T) {
	stats, _, err := copyMessages(t, upstreamChatStream)
	if err != nil {
		t.Fatalf("CopyMessagesSSE: %v", err)
	}
	if stats.Events != 6 {
		t.Fatalf("stats.Events = %d, want 6", stats.Events)
	}
}

// THE QUIET DIRECTION: a stream the upstream CUT — no finish_reason, then
// EOF — must leave the client with no message_stop. Synthesizing one would
// tell the client an answer finished when it did not, which is the exact lie
// the continuation feature exists to stop. The relay returns a nil error for
// io.EOF (as CopySSE does), so Terminal is the only signal.
func TestCopyMessagesSSETruncatedStreamGetsNoTerminal(t *testing.T) {
	truncated := "data: {\"id\":\"chatcmpl-1\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\"}}]}\n\n" +
		"data: {\"id\":\"chatcmpl-1\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"half\"}}]}\n\n"

	stats, out, err := copyMessages(t, truncated)
	if err != nil {
		t.Fatalf("a clean EOF must map to a nil error, as CopySSE does: %v", err)
	}
	if stats.Terminal {
		t.Fatal("a truncated stream was marked terminal")
	}
	if strings.Contains(out, "message_stop") {
		t.Fatalf("a truncated stream synthesized message_stop: %s", out)
	}
	if !strings.Contains(out, `"text":"half"`) {
		t.Fatalf("the partial answer was not relayed: %s", out)
	}
}

// A read failure is not a clean EOF, so Finish() is not owed — and this is
// the sharpest version of that rule: the finish_reason HAS been stated, so
// the translator is holding a terminal it would emit at the next clean EOF.
// The wire dying instead must leave that terminal unspent, because the
// upstream never said the answer was whole.
func TestCopyMessagesSSEReadFailureGetsNoTerminal(t *testing.T) {
	// finish stated, usage never sent, then the connection resets.
	dying := "data: {\"id\":\"chatcmpl-1\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"half\"}}]}\n\n" +
		"data: {\"id\":\"chatcmpl-1\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n"

	var buf bytes.Buffer
	stats, err := CopyMessagesSSE(&buf, &errBody{data: []byte(dying)},
		sseRewriter("test-model"), inject.NewChatToMessagesStream("test-model"), nil, nil)
	if err == nil {
		t.Fatal("a read failure was not propagated")
	}
	if stats.Terminal {
		t.Fatal("a stream that died on the wire was marked terminal")
	}
	if strings.Contains(buf.String(), "message_stop") {
		t.Fatalf("a read failure spent the held terminal: %s", buf.String())
	}
	if !strings.Contains(buf.String(), `"text":"half"`) {
		t.Fatalf("the bytes relayed before the failure were dropped: %s", buf.String())
	}
}

// The line and event caps are shared with CopySSE, and they apply to the
// INPUT: a hostile upstream runs into the same wall, and the offending line
// is never written.
func TestCopyMessagesSSEEnforcesTheSharedCaps(t *testing.T) {
	t.Run("line cap", func(t *testing.T) {
		huge := "data: {\"x\":\"" + strings.Repeat("a", MaxLineBytes+64) + "\"}\n\n"
		stats, out, err := copyMessages(t, huge)
		if !errors.Is(err, ErrSSELineTooLong) {
			t.Fatalf("err = %v, want ErrSSELineTooLong", err)
		}
		if out != "" {
			t.Fatalf("the offending line was written anyway: %q", out)
		}
		if stats.Terminal {
			t.Fatal("a capped stream was marked terminal")
		}
	})

	t.Run("event cap resets at the boundary", func(t *testing.T) {
		// Two events of two ~900KiB lines each: every line under the line cap,
		// each event under the event cap, the pair over it. Only a per-boundary
		// reset relays the whole stream.
		big := strings.Repeat("a", 900<<10)
		line := "data: {\"x\":\"" + big + "\"}\n"
		input := line + line + "\n" + line + line + "\n"
		stats, _, err := copyMessages(t, input)
		if err != nil {
			t.Fatalf("CopyMessagesSSE: %v (the per-event budget must reset at the boundary)", err)
		}
		// The load-bearing assertion above is that no cap fired. For the
		// event count: each input event is an unknown JSON object, so the
		// first yields message_start and the second yields nothing — one
		// dispatched Anthropic event from two input events.
		if stats.Events != 1 {
			t.Fatalf("stats.Events = %d, want 1 (message_start from the first event only)", stats.Events)
		}
	})

	t.Run("event cap fires across many lines in one event", func(t *testing.T) {
		// Three ~800KiB lines inside ONE event: no single line crosses the
		// line cap, but the event does.
		line := "data: {\"x\":\"" + strings.Repeat("a", 800<<10) + "\"}\n"
		_, _, err := copyMessages(t, line+line+line)
		if !errors.Is(err, ErrSSEEventTooLarge) {
			t.Fatalf("err = %v, want ErrSSEEventTooLarge", err)
		}
	})
}

// observe sees EVERY data line, before the rewrite, with the frame's event
// name — the same contract CopySSE offers. The accumulator behind it reads
// pre-translation upstream bytes: the client holds Anthropic events, but a
// continuation prefix would have to be built from the upstream's own dialect.
func TestCopyMessagesSSEObserveSeesEveryPreRewritePayload(t *testing.T) {
	type frame struct {
		name    string
		payload string
	}
	var got []frame
	var buf bytes.Buffer
	_, err := CopyMessagesSSE(&buf, strings.NewReader(upstreamChatStream), sseRewriter("test-model"),
		inject.NewChatToMessagesStream("test-model"), nil,
		func(name, payload []byte) {
			got = append(got, frame{string(name), string(payload)})
		})
	if err != nil {
		t.Fatalf("CopyMessagesSSE: %v", err)
	}

	// Every data line, including [DONE], and including the usage chunk the
	// translator spends most of its state on. The payloads are the UPSTREAM's
	// bytes: the model still reads upstream-name, because observe runs before
	// the rewrite. A chat stream states no event name, so every name is empty.
	wantPayloads := []string{
		`{"id":"chatcmpl-1","model":"upstream-name","choices":[{"index":0,"delta":{"role":"assistant"}}]}`,
		`{"id":"chatcmpl-1","model":"upstream-name","choices":[{"index":0,"delta":{"content":"PONG"}}]}`,
		`{"id":"chatcmpl-1","model":"upstream-name","choices":[{"index":0,"delta":{},"finish_reason":"stop"}],"usage":{"prompt_tokens":3,"completion_tokens":4,"total_tokens":7}}`,
		`[DONE]`,
	}
	if len(got) != len(wantPayloads) {
		t.Fatalf("observed %d data lines, want %d: %+v", len(got), len(wantPayloads), got)
	}
	for i, want := range wantPayloads {
		if got[i].payload != want {
			t.Fatalf("observed payload %d = %s, want %s", i, got[i].payload, want)
		}
		if got[i].name != "" {
			t.Fatalf("observed payload %d carried name %q; a chat stream states none", i, got[i].name)
		}
	}

	// A frame that DOES state a name carries it to the observer.
	got = nil
	_, err = CopyMessagesSSE(io.Discard,
		strings.NewReader("event: something\ndata: {\"x\":1}\n\n"), sseRewriter("test-model"),
		inject.NewChatToMessagesStream("test-model"), nil,
		func(name, payload []byte) {
			got = append(got, frame{string(name), string(payload)})
		})
	if err != nil {
		t.Fatalf("CopyMessagesSSE: %v", err)
	}
	if len(got) != 1 || got[0].name != "something" {
		t.Fatalf("frame name not carried to the observer: %+v", got)
	}
}

// A write failure is client-side and marked as such, exactly as in CopySSE,
// and the byte count reports what dst actually accepted.
func TestCopyMessagesSSEWriteFailuresAreMarkedClientSide(t *testing.T) {
	t.Run("partial write", func(t *testing.T) {
		w := &limitedWriter{limit: 5}
		stats, err := CopyMessagesSSE(w, strings.NewReader(upstreamChatStream), sseRewriter("p"),
			inject.NewChatToMessagesStream("p"), nil, nil)
		var swe *streamWriteError
		if !errors.As(err, &swe) {
			t.Fatalf("err = %v, want *streamWriteError", err)
		}
		if stats.Bytes != 5 {
			t.Fatalf("stats.Bytes = %d, want 5 (the bytes dst accepted)", stats.Bytes)
		}
	})

	t.Run("short write", func(t *testing.T) {
		stats, err := CopyMessagesSSE(&shortWriter{}, strings.NewReader(upstreamChatStream), sseRewriter("p"),
			inject.NewChatToMessagesStream("p"), nil, nil)
		var swe *streamWriteError
		if !errors.As(err, &swe) {
			t.Fatalf("err = %v, want *streamWriteError", err)
		}
		if !errors.Is(err, io.ErrShortWrite) {
			t.Fatalf("err = %v, want io.ErrShortWrite", err)
		}
		if stats.Bytes == 0 {
			t.Fatal("a short write reported no bytes accepted")
		}
	})
}

// One line per Write is a CONTRACT, not a style choice: pingWriter reads the
// tail of each buffer to decide whether the stream sits at an event boundary
// and latches `finished` when a write's bytes are terminal. A relay that
// batched several lines into one Write would hide both.
func TestCopyMessagesSSEWritesOneLinePerCall(t *testing.T) {
	w := &countingWriter{}
	_, err := CopyMessagesSSE(w, strings.NewReader(upstreamChatStream), sseRewriter("p"),
		inject.NewChatToMessagesStream("p"), nil, nil)
	if err != nil {
		t.Fatalf("CopyMessagesSSE: %v", err)
	}
	if w.calls == 0 {
		t.Fatal("nothing was written")
	}
	for i, b := range w.lines {
		if bytes.ContainsAny(b, "\r\n") && !bytes.HasSuffix(b, []byte("\n")) {
			t.Fatalf("write %d carried an interior newline: %q", i, b)
		}
		if !bytes.HasSuffix(b, []byte("\n")) {
			t.Fatalf("write %d is not a terminated line: %q", i, b)
		}
	}
}

// countingWriter records every Write as a separate line, so a test can prove
// the relay never batched.
type countingWriter struct {
	lines [][]byte
	calls int
}

func (w *countingWriter) Write(p []byte) (int, error) {
	w.calls++
	w.lines = append(w.lines, append([]byte(nil), p...))
	return len(p), nil
}

// Tool calls survive the relay as Anthropic tool_use blocks: the openai
// index does not become the Anthropic one, the id passes through opaque, and
// the argument fragments arrive as input_json_delta events.
func TestCopyMessagesSSERelaysToolCalls(t *testing.T) {
	input := "data: {\"id\":\"chatcmpl-1\",\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call_a\",\"type\":\"function\",\"function\":{\"name\":\"get_weather\",\"arguments\":\"\"}}]}}]}\n\n" +
		"data: {\"id\":\"chatcmpl-1\",\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"function\":{\"arguments\":\"{\\\"city\\\":\\\"Oslo\\\"}\"}}]}}]}\n\n" +
		"data: {\"id\":\"chatcmpl-1\",\"choices\":[{\"delta\":{},\"finish_reason\":\"tool_calls\"}],\"usage\":{\"prompt_tokens\":5,\"completion_tokens\":7,\"total_tokens\":12}}\n\n"

	stats, out, err := copyMessages(t, input)
	if err != nil {
		t.Fatalf("CopyMessagesSSE: %v", err)
	}
	if !stats.Terminal {
		t.Fatal("a tool-call stream that reached message_stop was not marked terminal")
	}
	if !strings.Contains(out, `"type":"tool_use","id":"call_a","name":"get_weather","input":{}`) {
		t.Fatalf("no tool_use block was announced: %s", out)
	}
	if !strings.Contains(out, `"partial_json":"{\"city\":\"Oslo\"}"`) {
		t.Fatalf("the argument fragment did not ride input_json_delta: %s", out)
	}
	if !strings.Contains(out, `"stop_reason":"tool_use"`) {
		t.Fatalf("finish_reason tool_calls did not map to tool_use: %s", out)
	}
}

// An in-band upstream error becomes one static Anthropic error event, with
// this proxy's own message. The provider's text never reaches the client —
// it is the same class of bytes as a prompt or a credential.
func TestCopyMessagesSSERelaysErrorAsAStaticEvent(t *testing.T) {
	input := "data: {\"id\":\"chatcmpl-1\",\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n" +
		"data: {\"error\":{\"message\":\"upstream leaked sk-live-SECRET\",\"type\":\"server_error\"}}\n\n" +
		"data: {\"id\":\"chatcmpl-1\",\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":1,\"completion_tokens\":1}}\n\n"

	stats, out, err := copyMessages(t, input)
	if err != nil {
		t.Fatalf("CopyMessagesSSE: %v", err)
	}
	if !strings.Contains(out, `event: error`) {
		t.Fatalf("no error event was emitted: %s", out)
	}
	if strings.Contains(out, "sk-live-SECRET") {
		t.Fatalf("provider error text reached the client: %s", out)
	}
	// A failed stream never also terminates.
	if stats.Terminal {
		t.Fatal("a stream that failed in band was marked terminal")
	}
	if strings.Contains(out, "message_stop") {
		t.Fatalf("message_stop followed an error event: %s", out)
	}
}

// Byte-by-byte fragmentation must not change the output: the relay reads
// through the same line grammar CopySSE does, so a frame split across
// transport reads reassembles identically.
func TestCopyMessagesSSELineEndingsSurviveReadFragmentation(t *testing.T) {
	_, whole, err := copyMessages(t, upstreamChatStream)
	if err != nil {
		t.Fatalf("CopyMessagesSSE: %v", err)
	}
	var buf bytes.Buffer
	_, err = CopyMessagesSSE(&buf, byteReader{r: strings.NewReader(upstreamChatStream)}, sseRewriter("test-model"),
		inject.NewChatToMessagesStream("test-model"), nil, nil)
	if err != nil {
		t.Fatalf("CopyMessagesSSE (fragmented): %v", err)
	}
	if buf.String() != whole {
		t.Fatalf("fragmented reads changed the output:\n got %q\nwant %q", buf.String(), whole)
	}
}

// The upstream's own line terminators never reach the client: the Anthropic
// stream has its own framing, and a CR-only input stream must not produce
// CR-terminated frames.
func TestCopyMessagesSSEOutputFramingIsLF(t *testing.T) {
	input := "data: {\"id\":\"chatcmpl-1\",\"choices\":[{\"delta\":{\"role\":\"assistant\"}}]}\r\n\r\n" +
		"data: {\"id\":\"chatcmpl-1\",\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":1,\"completion_tokens\":1}}\r\n\r\n"

	stats, out, err := copyMessages(t, input)
	if err != nil {
		t.Fatalf("CopyMessagesSSE: %v", err)
	}
	if !stats.Terminal {
		t.Fatal("a CR-terminated input stream did not reach message_stop")
	}
	if strings.Contains(out, "\r") {
		t.Fatalf("a CR from the input framing leaked into the output: %q", out)
	}
	if !strings.Contains(out, "event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n") {
		t.Fatalf("the terminal frame is not LF-framed: %q", out)
	}
}
