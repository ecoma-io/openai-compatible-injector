package inject

import (
	"bytes"
	"strings"
	"testing"
)

// streamFrame is one synthesized Anthropic SSE frame, split into its event
// name and the exact bytes of its data line.
type streamFrame struct {
	event string
	data  string
}

// splitFrames parses a translator's output the way the relay's writer will:
// each frame is an event line, one data line, then a blank line. Anything
// that does not parse that way is a bug in the frame builder, not in the
// test's expectations, so the parse is strict and fatal.
func splitFrames(t *testing.T, out []byte) []streamFrame {
	t.Helper()
	if len(out) == 0 {
		return nil
	}
	s := string(out)
	if !strings.HasSuffix(s, "\n\n") {
		t.Fatalf("frame stream does not end with a blank line: %q", s)
	}
	var frames []streamFrame
	for _, raw := range strings.Split(strings.TrimSuffix(s, "\n\n"), "\n\n") {
		lines := strings.Split(raw, "\n")
		if len(lines) != 2 {
			t.Fatalf("frame is not exactly two lines: %q", raw)
		}
		name, ok := strings.CutPrefix(lines[0], "event: ")
		if !ok {
			t.Fatalf("frame has no event line: %q", raw)
		}
		data, ok := strings.CutPrefix(lines[1], "data: ")
		if !ok {
			t.Fatalf("frame has no data line: %q", raw)
		}
		frames = append(frames, streamFrame{event: name, data: data})
	}
	return frames
}

func renderFrames(frames []streamFrame) string {
	if len(frames) == 0 {
		return "[]"
	}
	var b strings.Builder
	b.WriteByte('[')
	for i, f := range frames {
		if i > 0 {
			b.WriteString(" | ")
		}
		b.WriteString(f.event)
		b.WriteString(": ")
		b.WriteString(f.data)
	}
	b.WriteByte(']')
	return b.String()
}

// assertFrames pins the full golden sequence: event names AND the exact
// bytes of every data line. Field order is part of the contract here — a
// client that re-parses JSON would not care, but a golden that only checked
// the names could not catch a mangled payload.
func assertFrames(t *testing.T, out []byte, want []streamFrame) {
	t.Helper()
	got := splitFrames(t, out)
	if len(got) != len(want) {
		t.Fatalf("frames = %s\nwant  = %s", renderFrames(got), renderFrames(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("frame %d:\n got  %s:%s\n want %s:%s", i,
				got[i].event, got[i].data, want[i].event, want[i].data)
		}
	}
}

func assertEventNames(t *testing.T, out []byte, want ...string) {
	t.Helper()
	got := splitFrames(t, out)
	names := make([]string, len(got))
	for i, f := range got {
		names[i] = f.event
	}
	if strings.Join(names, ",") != strings.Join(want, ",") {
		t.Fatalf("events = %q, want %q", names, want)
	}
}

func messageStartFrame(id, publicModel string) streamFrame {
	return streamFrame{
		event: "message_start",
		data: `{"type":"message_start","message":{"id":"` + id +
			`","type":"message","role":"assistant","model":"` + publicModel +
			`","content":[],"stop_reason":null,"stop_sequence":null,` +
			`"usage":{"input_tokens":0,"output_tokens":0}}}`,
	}
}

// The opening frame: the first JSON-object payload, whatever else it
// carries. The id is the upstream's passed through, the model is the PUBLIC
// name — the payload here deliberately states the upstream alias so a
// translator that started reading `model` off the chunk instead of from its
// own latch would fail this test.
func TestStreamRoleOnlyFirstChunkEmitsMessageStart(t *testing.T) {
	s := NewChatToMessagesStream("test-model")
	out := s.Frame([]byte(`{"id":"chatcmpl-1","object":"chat.completion.chunk","model":"upstream-name","choices":[{"index":0,"delta":{"role":"assistant"},"finish_reason":null}]}`))
	assertFrames(t, out, []streamFrame{messageStartFrame("chatcmpl-1", "test-model")})

	// Nothing else is owed: no finish_reason, so no terminal.
	if got := s.Finish(); got != nil {
		t.Fatalf("Finish() = %q, want nil", got)
	}
}

// Plain text: one open block, one delta per non-empty chunk, and the
// terminal trio once finish + usage are both known. The text carries HTML
// metacharacters on purpose — marshalJSON must not escape them, or the
// client gets bytes it never sent back.
func TestStreamPlainText(t *testing.T) {
	s := NewChatToMessagesStream("test-model")
	var out []byte
	feed := func(payload string) { out = append(out, s.Frame([]byte(payload))...) }

	feed(`{"id":"chatcmpl-1","choices":[{"delta":{"role":"assistant"}}]}`)
	feed(`{"id":"chatcmpl-1","choices":[{"delta":{"content":"a < b & c"}}]}`)
	feed(`{"id":"chatcmpl-1","choices":[{"delta":{"content":"!"}}]}`)
	feed(`{"id":"chatcmpl-1","choices":[{"delta":{},"finish_reason":"stop"}],"usage":{"prompt_tokens":3,"completion_tokens":4,"total_tokens":7}}`)

	assertFrames(t, out, []streamFrame{
		messageStartFrame("chatcmpl-1", "test-model"),
		{event: "content_block_start", data: `{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}`},
		{event: "content_block_delta", data: `{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"a < b & c"}}`},
		{event: "content_block_delta", data: `{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"!"}}`},
		{event: "content_block_stop", data: `{"type":"content_block_stop","index":0}`},
		{event: "message_delta", data: `{"type":"message_delta","delta":{"stop_reason":"end_turn","stop_sequence":null},"usage":{"input_tokens":3,"output_tokens":4}}`},
		{event: "message_stop", data: `{"type":"message_stop"}`},
	})
	// The golden line above already pins the angle bracket verbatim; this
	// states the same property as its failure mode — the default encoder's
	// < form, which would decode to the same string but is not the
	// same bytes.
	if bytes.Contains(out, []byte("a \\u003c b")) {
		t.Fatalf("encoder HTML-escaped the delta text: %s", out)
	}
}

// Empty, null and absent content deltas must not open a block: a stream
// whose first chunks are role-only stays at message_start until real text
// arrives. Whitespace-only text is a different case — it IS content the
// client can see, so it does open one.
func TestStreamEmptyDeltasDoNotOpenBlocks(t *testing.T) {
	for _, payload := range []string{
		`{"id":"chatcmpl-1","choices":[{"delta":{"content":""}}]}`,
		`{"id":"chatcmpl-1","choices":[{"delta":{"content":null}}]}`,
		`{"id":"chatcmpl-1","choices":[{"delta":{}}]}`,
	} {
		s := NewChatToMessagesStream("test-model")
		out := s.Frame([]byte(payload))
		assertEventNames(t, out, "message_start")
	}

	s := NewChatToMessagesStream("test-model")
	out := s.Frame([]byte(`{"id":"chatcmpl-1","choices":[{"delta":{"content":"   "}}]}`))
	assertEventNames(t, out, "message_start", "content_block_start", "content_block_delta")
}

// Two tool calls arriving sequentially — the shape every real model
// produces, and the shape Anthropic's event stream requires. The second id
// closes the first block before opening the next.
func TestStreamTwoSequentialToolCalls(t *testing.T) {
	s := NewChatToMessagesStream("test-model")
	var out []byte
	feed := func(payload string) { out = append(out, s.Frame([]byte(payload))...) }

	feed(`{"id":"chatcmpl-1","choices":[{"delta":{"role":"assistant"}}]}`)
	feed(`{"id":"chatcmpl-1","choices":[{"delta":{"tool_calls":[{"index":0,"id":"call_a","function":{"name":"get_weather","arguments":""}}]}}]}`)
	feed(`{"id":"chatcmpl-1","choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":"{\"city\":"}}]}}]}`)
	feed(`{"id":"chatcmpl-1","choices":[{"delta":{"tool_calls":[{"index":1,"id":"call_b","function":{"name":"get_time","arguments":""}}]}}]}`)
	feed(`{"id":"chatcmpl-1","choices":[{"delta":{"tool_calls":[{"index":1,"function":{"arguments":"{\"tz\":\"UTC\"}"}}]}}]}`)
	feed(`{"id":"chatcmpl-1","choices":[{"delta":{},"finish_reason":"tool_calls"}],"usage":{"prompt_tokens":5,"completion_tokens":7,"total_tokens":12}}`)

	assertFrames(t, out, []streamFrame{
		messageStartFrame("chatcmpl-1", "test-model"),
		{event: "content_block_start", data: `{"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"call_a","name":"get_weather","input":{}}}`},
		{event: "content_block_delta", data: `{"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"{\"city\":"}}`},
		{event: "content_block_stop", data: `{"type":"content_block_stop","index":0}`},
		{event: "content_block_start", data: `{"type":"content_block_start","index":1,"content_block":{"type":"tool_use","id":"call_b","name":"get_time","input":{}}}`},
		{event: "content_block_delta", data: `{"type":"content_block_delta","index":1,"delta":{"type":"input_json_delta","partial_json":"{\"tz\":\"UTC\"}"}}`},
		{event: "content_block_stop", data: `{"type":"content_block_stop","index":1}`},
		{event: "message_delta", data: `{"type":"message_delta","delta":{"stop_reason":"tool_use","stop_sequence":null},"usage":{"input_tokens":5,"output_tokens":7}}`},
		{event: "message_stop", data: `{"type":"message_stop"}`},
	})

	// Terminated streams owe nothing further — a late [DONE] and another
	// Finish() must both be silent, or the client sees a second message_stop.
	if got := s.Frame([]byte("[DONE]")); len(got) != 0 {
		t.Fatalf("[DONE] after terminate = %s", renderFrames(splitFrames(t, got)))
	}
	if got := s.Finish(); len(got) != 0 {
		t.Fatalf("Finish() after terminate = %s", renderFrames(splitFrames(t, got)))
	}
}

// finish_reason HELD until usage arrives: the terminal trio comes out only
// when both are known, and it carries the real counts.
func TestStreamFinishHeldUntilUsage(t *testing.T) {
	s := NewChatToMessagesStream("test-model")
	s.Frame([]byte(`{"id":"chatcmpl-1","choices":[{"delta":{"content":"Hi"}}]}`))

	// finish alone must not terminate — the usage object has not arrived.
	out := s.Frame([]byte(`{"id":"chatcmpl-1","choices":[{"delta":{},"finish_reason":"stop"}]}`))
	if len(out) != 0 {
		t.Fatalf("finish without usage produced %s", renderFrames(splitFrames(t, out)))
	}

	// The usage-only chunk completes it.
	out = s.Frame([]byte(`{"id":"chatcmpl-1","choices":[],"usage":{"prompt_tokens":11,"completion_tokens":22,"total_tokens":33}}`))
	assertFrames(t, out, []streamFrame{
		{event: "content_block_stop", data: `{"type":"content_block_stop","index":0}`},
		{event: "message_delta", data: `{"type":"message_delta","delta":{"stop_reason":"end_turn","stop_sequence":null},"usage":{"input_tokens":11,"output_tokens":22}}`},
		{event: "message_stop", data: `{"type":"message_stop"}`},
	})
}

// finish but no usage chunk (some upstreams never send one even when asked
// for stream_options): [DONE] still terminates, with zeros — an honest
// "nothing was reported" on the client wire, not a fabricated count.
func TestStreamFinishWithoutUsageThenDone(t *testing.T) {
	s := NewChatToMessagesStream("test-model")
	s.Frame([]byte(`{"id":"chatcmpl-1","choices":[{"delta":{"content":"Hi"}}]}`))
	s.Frame([]byte(`{"id":"chatcmpl-1","choices":[{"delta":{},"finish_reason":"stop"}]}`))

	out := s.Frame([]byte("[DONE]"))
	assertFrames(t, out, []streamFrame{
		{event: "content_block_stop", data: `{"type":"content_block_stop","index":0}`},
		{event: "message_delta", data: `{"type":"message_delta","delta":{"stop_reason":"end_turn","stop_sequence":null},"usage":{"input_tokens":0,"output_tokens":0}}`},
		{event: "message_stop", data: `{"type":"message_stop"}`},
	})
}

// A [DONE] with no finish_reason is a stream that never said it was done.
// It must not be papered over with a synthesized terminal.
func TestStreamDoneWithoutFinishIsSilent(t *testing.T) {
	s := NewChatToMessagesStream("test-model")
	s.Frame([]byte(`{"id":"chatcmpl-1","choices":[{"delta":{"content":"Hi"}}]}`))
	if got := s.Frame([]byte("[DONE]")); len(got) != 0 {
		t.Fatalf("[DONE] without finish_reason = %s", renderFrames(splitFrames(t, got)))
	}
	if got := s.Finish(); len(got) != 0 {
		t.Fatalf("Finish() without finish_reason = %s", renderFrames(splitFrames(t, got)))
	}
}

// Clean EOF after a stated finish_reason terminates; the relay calls Finish
// only on a real io.EOF.
func TestStreamFinishThenCleanEOF(t *testing.T) {
	s := NewChatToMessagesStream("test-model")
	s.Frame([]byte(`{"id":"chatcmpl-1","choices":[{"delta":{"content":"Hi"}}]}`))
	s.Frame([]byte(`{"id":"chatcmpl-1","choices":[{"delta":{},"finish_reason":"length"}]}`))

	out := s.Finish()
	assertFrames(t, out, []streamFrame{
		{event: "content_block_stop", data: `{"type":"content_block_stop","index":0}`},
		{event: "message_delta", data: `{"type":"message_delta","delta":{"stop_reason":"max_tokens","stop_sequence":null},"usage":{"input_tokens":0,"output_tokens":0}}`},
		{event: "message_stop", data: `{"type":"message_stop"}`},
	})
}

// THE QUIET DIRECTION: a truncated stream — EOF with no finish_reason —
// must leave the client's stream exactly as unterminated as it would have
// been. Synthesizing message_stop here would tell the client an answer
// finished when it did not.
func TestStreamTruncatedEOFEmitsNoTerminal(t *testing.T) {
	s := NewChatToMessagesStream("test-model")
	out := s.Frame([]byte(`{"id":"chatcmpl-1","choices":[{"delta":{"role":"assistant"}}]}`))
	out = append(out, s.Frame([]byte(`{"id":"chatcmpl-1","choices":[{"delta":{"content":"half an ans"}}]}`))...)

	if got := s.Finish(); len(got) != 0 {
		t.Fatalf("Finish() on a truncated stream = %s", renderFrames(splitFrames(t, got)))
	}
	if strings.Contains(string(out), "message_stop") {
		t.Fatalf("truncated stream synthesized a terminal: %s", out)
	}
	assertEventNames(t, out, "message_start", "content_block_start", "content_block_delta")
}

// An in-band upstream error is one static frame: this proxy's own message,
// never the provider's text (which is credential- and prompt-shaped), and
// never followed by a message_stop.
func TestStreamUpstreamErrorEmitsStaticFrame(t *testing.T) {
	s := NewChatToMessagesStream("test-model")
	s.Frame([]byte(`{"id":"chatcmpl-1","choices":[{"delta":{"role":"assistant"}}]}`))

	out := s.Frame([]byte(`{"error":{"message":"boom with sk-secret and <prompt>","type":"server_error"}}`))
	assertFrames(t, out, []streamFrame{{
		event: "error",
		data:  `{"type":"error","error":{"type":"api_error","message":"upstream returned an invalid response"}}`,
	}})
	if strings.Contains(string(out), "sk-secret") || strings.Contains(string(out), "<prompt>") {
		t.Fatalf("provider error text leaked into the frame: %s", out)
	}

	// Failed latches: no [DONE] handling, no Finish(), nothing at all.
	if got := s.Frame([]byte(`{"id":"chatcmpl-1","choices":[{"delta":{"content":"late"}}]}`)); len(got) != 0 {
		t.Fatalf("frame after error = %s", renderFrames(splitFrames(t, got)))
	}
	if got := s.Frame([]byte("[DONE]")); len(got) != 0 {
		t.Fatalf("[DONE] after error = %s", renderFrames(splitFrames(t, got)))
	}
	if got := s.Finish(); len(got) != 0 {
		t.Fatalf("Finish() after error = %s", renderFrames(splitFrames(t, got)))
	}
}

// A provider that states "error": null on every healthy chunk must not be
// read as failing: the guard is the member being a JSON object.
func TestStreamNullErrorMemberIsNotAFailure(t *testing.T) {
	s := NewChatToMessagesStream("test-model")
	out := s.Frame([]byte(`{"id":"chatcmpl-1","error":null,"choices":[{"delta":{"content":"ok"}}]}`))
	assertEventNames(t, out, "message_start", "content_block_start", "content_block_delta")
}

// One payload can owe three events: the terminal trio splits into its own
// frames, so the relay's flush-per-boundary accounting sees three dispatches.
func TestStreamThreeEventsFromOnePayload(t *testing.T) {
	s := NewChatToMessagesStream("test-model")
	s.Frame([]byte(`{"id":"chatcmpl-1","choices":[{"delta":{"role":"assistant"}}]}`))
	s.Frame([]byte(`{"id":"chatcmpl-1","choices":[{"delta":{"content":"Hi"}}]}`))

	out := s.Frame([]byte(`{"id":"chatcmpl-1","choices":[{"delta":{},"finish_reason":"stop"}],"usage":{"prompt_tokens":3,"completion_tokens":4,"total_tokens":7}}`))
	assertFrames(t, out, []streamFrame{
		{event: "content_block_stop", data: `{"type":"content_block_stop","index":0}`},
		{event: "message_delta", data: `{"type":"message_delta","delta":{"stop_reason":"end_turn","stop_sequence":null},"usage":{"input_tokens":3,"output_tokens":4}}`},
		{event: "message_stop", data: `{"type":"message_stop"}`},
	})
}

// Text arriving after a tool block closed opens a NEW text block at the
// next index — Anthropic blocks are indexed, never reused.
func TestStreamTextAfterToolOpensNextIndex(t *testing.T) {
	s := NewChatToMessagesStream("test-model")
	s.Frame([]byte(`{"id":"chatcmpl-1","choices":[{"delta":{"role":"assistant"}}]}`))
	s.Frame([]byte(`{"id":"chatcmpl-1","choices":[{"delta":{"tool_calls":[{"index":0,"id":"call_a","function":{"name":"f","arguments":""}}]}}]}`))

	out := s.Frame([]byte(`{"id":"chatcmpl-1","choices":[{"delta":{"content":"then text"}}]}`))
	assertFrames(t, out, []streamFrame{
		{event: "content_block_stop", data: `{"type":"content_block_stop","index":0}`},
		{event: "content_block_start", data: `{"type":"content_block_start","index":1,"content_block":{"type":"text","text":""}}`},
		{event: "content_block_delta", data: `{"type":"content_block_delta","index":1,"delta":{"type":"text_delta","text":"then text"}}`},
	})
}

// Payloads that are not JSON objects owe nothing — comments, keep-alive
// lines shaped oddly, a stray word. They must not open a message, and a
// non-object cannot carry an error either.
func TestStreamIgnoresNonObjectPayloads(t *testing.T) {
	s := NewChatToMessagesStream("test-model")
	for _, payload := range []string{
		``,
		`   `,
		`hello`,
		`[1,2,3]`,
		`"a string"`,
		`null`,
		`{not json`,
		`{"id":` + `truncated}`,
	} {
		if got := s.Frame([]byte(payload)); len(got) != 0 {
			t.Fatalf("payload %q produced %s", payload, renderFrames(splitFrames(t, got)))
		}
	}
	if s.started {
		t.Fatal("non-object payloads latched message_start")
	}
}

// The tool fragment rules: a JSON-string fragment rides through as-is, a
// whole object from a non-conforming provider is accepted as one complete
// fragment, and a null/absent argument emits no delta.
func TestStreamArgumentFragmentShapes(t *testing.T) {
	cases := []struct {
		name string
		raw  string
		want string
	}{
		{name: "string fragment", raw: `"{\"a\":1}"`, want: `{"a":1}`},
		{name: "bare object", raw: `{"a":1}`, want: `{"a":1}`},
		{name: "null is silent", raw: `null`, want: ""},
		{name: "absent is silent", raw: ``, want: ""},
		{name: "unparseable is dropped", raw: `{oops`, want: ""},
		{name: "number is dropped", raw: `42`, want: ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			payload := `{"id":"chatcmpl-1","choices":[{"delta":{"tool_calls":[{"index":0,"id":"call_a","function":{"name":"f","arguments":` +
				tc.raw + `}}]}}]}`
			s := NewChatToMessagesStream("test-model")
			out := s.Frame([]byte(payload))
			frames := splitFrames(t, out)
			var got []string
			for _, f := range frames {
				if f.event == "content_block_delta" {
					got = append(got, f.data)
				}
			}
			if tc.want == "" {
				if len(got) != 0 {
					t.Fatalf("deltas = %q, want none", got)
				}
				return
			}
			want := `{"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"` +
				escapeFrag(tc.want) + `"}}`
			if len(got) != 1 || got[0] != want {
				t.Fatalf("deltas = %q, want [%s]", got, want)
			}
		})
	}
}

// escapeFrag renders a fragment for the partial_json member of a golden
// data line: the JSON encoder's escaping of a Go string, written by hand so
// the test asserts on the same bytes the encoder will produce.
func escapeFrag(s string) string {
	r := strings.NewReplacer(`\`, `\\`, `"`, `\"`)
	return r.Replace(s)
}

// A tool argument announced without a block (first payload carries args but
// no id) has nowhere to land; dropping it keeps the stream well-formed
// rather than inventing a tool_use with an empty id.
func TestStreamOrphanArgumentIsDropped(t *testing.T) {
	s := NewChatToMessagesStream("test-model")
	out := s.Frame([]byte(`{"id":"chatcmpl-1","choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":"{\"a\":1}"}}]}}]}`))
	assertEventNames(t, out, "message_start")
}

// finish_reasons map onto Anthropic's closed vocabulary; an unknown one
// lands on end_turn, the honest reading of "the answer came back whole".
func TestStreamStopReasonMapping(t *testing.T) {
	cases := map[string]string{
		`"stop"`:           `"end_turn"`,
		`"length"`:         `"max_tokens"`,
		`"tool_calls"`:     `"tool_use"`,
		`"function_call"`:  `"tool_use"`,
		`"content_filter"`: `"refusal"`,
		`"weird"`:          `"end_turn"`,
	}
	for finish, want := range cases {
		t.Run(finish, func(t *testing.T) {
			s := NewChatToMessagesStream("test-model")
			s.Frame([]byte(`{"id":"chatcmpl-1","choices":[{"delta":{"content":"x"}}]}`))
			out := s.Frame([]byte(`{"id":"chatcmpl-1","choices":[{"delta":{},"finish_reason":` + finish + `}],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}`))
			if !bytes.Contains(out, []byte(`"stop_reason":`+want)) {
				t.Fatalf("stop_reason %s not present in %s", want, out)
			}
		})
	}
}
