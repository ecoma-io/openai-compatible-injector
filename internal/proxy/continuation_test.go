package proxy

import (
	"strings"
	"testing"
)

// feed pushes a sequence of payloads through a fresh accumulator and returns
// the verdict. Each payload is one admitted data line, exactly as CopySSE
// hands them over.
func feed(api string, limit int, payloads ...string) (string, string) {
	p := newPartialText(api, limit)
	for _, s := range payloads {
		p.Observe([]byte(s))
	}
	return p.Safe()
}

func TestPartialTextAccumulatesChatContent(t *testing.T) {
	prefix, reason := feed(apiChat, 1<<20,
		`{"choices":[{"index":0,"delta":{"role":"assistant"},"finish_reason":null}]}`,
		`{"choices":[{"index":0,"delta":{"content":"Hello"},"finish_reason":null}]}`,
		`{"choices":[{"index":0,"delta":{"content":", "},"finish_reason":null}]}`,
		`{"choices":[{"index":0,"delta":{"content":"world"},"finish_reason":null}]}`,
		`[DONE]`,
	)
	if reason != "" {
		t.Fatalf("a plain text stream was refused: %q", reason)
	}
	if prefix != "Hello, world" {
		t.Fatalf("prefix = %q, want %q", prefix, "Hello, world")
	}
}

func TestPartialTextAccumulatesResponsesContent(t *testing.T) {
	prefix, reason := feed(apiResponses, 1<<20,
		`{"type":"response.created","response":{"id":"r1"}}`,
		`{"type":"response.in_progress","response":{"id":"r1"}}`,
		`{"type":"response.output_item.added","item":{"type":"message","id":"m1"}}`,
		`{"type":"response.content_part.added","part":{"type":"output_text"}}`,
		`{"type":"response.output_text.delta","item_id":"m1","delta":"Once"}`,
		`{"type":"response.output_text.delta","item_id":"m1","delta":" upon a time"}`,
		`{"type":"response.output_text.done","text":"Once upon a time"}`,
		`{"type":"response.content_part.done"}`,
		`{"type":"response.output_item.done","item":{"type":"message"}}`,
		`{"type":"response.completed","response":{"id":"r1"}}`,
	)
	if reason != "" {
		t.Fatalf("a plain Responses stream was refused: %q", reason)
	}
	if prefix != "Once upon a time" {
		t.Fatalf("prefix = %q, want %q", prefix, "Once upon a time")
	}
}

// The safety gate is the feature's fail-closed half: every token below is a
// shape the accumulator refuses to build a continuation request from. Losing
// one of these cases silently is the failure mode that produces duplicated or
// corrupt output.
func TestPartialTextRefusesUnsafeShapes(t *testing.T) {
	cases := []struct {
		name     string
		api      string
		payloads []string
		want     string
	}{
		{
			name: "chat tool_calls",
			api:  apiChat,
			payloads: []string{
				`{"choices":[{"delta":{"content":"Let me check"}}]}`,
				`{"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call_1","function":{"name":"ls"}}]}}]}`,
			},
			want: reasonToolCalls,
		},
		{
			name: "chat legacy function_call",
			api:  apiChat,
			payloads: []string{
				`{"choices":[{"delta":{"function_call":{"name":"ls"}}}]}`,
			},
			want: reasonToolCalls,
		},
		{
			name: "chat finish_reason",
			api:  apiChat,
			payloads: []string{
				`{"choices":[{"delta":{"content":"done"},"finish_reason":"stop"}]}`,
			},
			want: reasonFinishReason,
		},
		{
			name: "chat content filter finish",
			api:  apiChat,
			payloads: []string{
				`{"choices":[{"delta":{},"finish_reason":"content_filter"}]}`,
			},
			want: reasonFinishReason,
		},
		{
			name: "chat error event",
			api:  apiChat,
			payloads: []string{
				`{"error":{"message":"upstream exploded","type":"server_error"}}`,
			},
			want: reasonUpstreamTerminal,
		},
		{
			name:     "chat not json",
			api:      apiChat,
			payloads: []string{`}{`},
			want:     reasonNotObject,
		},
		{
			name:     "chat json but not an object",
			api:      apiChat,
			payloads: []string{`"a string"`},
			want:     reasonNotObject,
		},
		{
			name:     "chat choices is not an array",
			api:      apiChat,
			payloads: []string{`{"choices":{"delta":{"content":"x"}}}`},
			want:     reasonUnknownShape,
		},
		{
			name:     "chat content is not a string",
			api:      apiChat,
			payloads: []string{`{"choices":[{"delta":{"content":[{"type":"text","text":"x"}]}}]}`},
			want:     reasonUnknownShape,
		},
		{
			name:     "chat several choices",
			api:      apiChat,
			payloads: []string{`{"choices":[{"delta":{"content":"a"}},{"delta":{"content":"b"}}]}`},
			want:     reasonUnknownShape,
		},
		{
			name: "responses function call item",
			api:  apiResponses,
			payloads: []string{
				`{"type":"response.output_item.added","item":{"type":"function_call","name":"ls"}}`,
			},
			want: reasonToolCalls,
		},
		{
			name: "responses unknown item type",
			api:  apiResponses,
			payloads: []string{
				`{"type":"response.output_item.added","item":{"type":"web_search_call"}}`,
			},
			want: reasonToolCalls,
		},
		{
			name: "responses argument delta",
			api:  apiResponses,
			payloads: []string{
				`{"type":"response.function_call_arguments.delta","delta":"{\"pa"}`,
			},
			want: reasonToolCalls,
		},
		{
			name: "responses failed",
			api:  apiResponses,
			payloads: []string{
				`{"type":"response.failed","response":{"error":{"code":"server_error"}}}`,
			},
			want: reasonUpstreamTerminal,
		},
		{
			name: "responses incomplete",
			api:  apiResponses,
			payloads: []string{
				`{"type":"response.incomplete","response":{"status":"incomplete"}}`,
			},
			want: reasonUpstreamTerminal,
		},
		{
			name:     "responses unknown event",
			api:      apiResponses,
			payloads: []string{`{"type":"response.some_future_thing","delta":"x"}`},
			want:     reasonUnknownShape,
		},
		{
			name:     "responses missing type",
			api:      apiResponses,
			payloads: []string{`{"delta":"x"}`},
			want:     reasonUnknownShape,
		},
		{
			name:     "responses delta is not a string",
			api:      apiResponses,
			payloads: []string{`{"type":"response.output_text.delta","delta":{"text":"x"}}`},
			want:     reasonUnknownShape,
		},
		{
			name:     "oversize",
			api:      apiChat,
			payloads: []string{`{"choices":[{"delta":{"content":"0123456789"}}]}`},
			want:     reasonOversize,
			// limit 8: the accumulator refuses as soon as it holds >= 8 bytes.
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			limit := 1 << 20
			if tc.want == reasonOversize {
				limit = 8
			}
			prefix, reason := feed(tc.api, limit, tc.payloads...)
			if reason != tc.want {
				t.Fatalf("reason = %q, want %q", reason, tc.want)
			}
			if prefix != "" {
				t.Fatalf("a refused stream still reported a prefix: %q", prefix)
			}
		})
	}
}

// TestPartialTextNoPrefix: a stream that carried no text at all is not
// recoverable. Continuing it would re-ask the original question — a blind
// retry, not a continuation.
func TestPartialTextNoPrefix(t *testing.T) {
	for _, tc := range []struct {
		name     string
		api      string
		payloads []string
	}{
		{"chat with nothing", apiChat, nil},
		{"chat with an empty delta", apiChat, []string{`{"choices":[{"delta":{"role":"assistant"}}]}`}},
		{"chat with null content", apiChat, []string{`{"choices":[{"delta":{"content":null}}]}`}},
		{"responses with only lifecycle", apiResponses, []string{`{"type":"response.created"}`, `{"type":"response.in_progress"}`}},
		{"responses with empty deltas", apiResponses, []string{`{"type":"response.output_text.delta","delta":""}`}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			prefix, reason := feed(tc.api, 1<<20, tc.payloads...)
			if reason != reasonNoPrefix {
				t.Fatalf("reason = %q, want %q", reason, reasonNoPrefix)
			}
			if prefix != "" {
				t.Fatalf("prefix = %q, want empty", prefix)
			}
		})
	}
}

// TestPartialTextFirstRefusalWins: the reason is latched, and a later
// well-formed chunk cannot resurrect a stream the gate already refused. This
// is what makes the gate a gate rather than a running opinion.
func TestPartialTextFirstRefusalWins(t *testing.T) {
	p := newPartialText(apiChat, 1<<20)
	p.Observe([]byte(`{"choices":[{"delta":{"content":"before"}}]}`))
	p.Observe([]byte(`{"choices":[{"delta":{"tool_calls":[{"index":0}]}}]}`))
	p.Observe([]byte(`{"choices":[{"delta":{"content":" after"}}]}`))

	prefix, reason := p.Safe()
	if reason != reasonToolCalls {
		t.Fatalf("reason = %q, want %q", reason, reasonToolCalls)
	}
	if prefix != "" {
		t.Fatalf("prefix = %q, want empty — a refused stream never reports text", prefix)
	}
	// The latched reason survives further calls and further observations.
	p.Observe([]byte(`{"choices":[{"delta":{"content":"more"}}]}`))
	if _, again := p.Safe(); again != reasonToolCalls {
		t.Fatalf("second Safe reason = %q, want the latched %q", again, reasonToolCalls)
	}
	if p.partialBytes() != 0 {
		t.Fatalf("a refused accumulator still holds %d bytes", p.partialBytes())
	}
}

// TestPartialTextDoneIsNotText: chat's [DONE] is a marker, not a payload. It
// must not be treated as an unparseable data line — that would refuse every
// chat stream at the last event — and it must not be accumulated either.
func TestPartialTextDoneIsNotText(t *testing.T) {
	prefix, reason := feed(apiChat, 1<<20,
		`{"choices":[{"delta":{"content":"hi"}}]}`,
		`[DONE]`,
	)
	if reason != "" || prefix != "hi" {
		t.Fatalf("prefix/reason = %q/%q, want %q/\"\"", prefix, reason, "hi")
	}
}

// TestPartialTextNullMembersAreZeroValues: the OpenAI APIs treat a null member
// as its zero value, so a null is never a refusal. A stream that states
// finish_reason explicitly as null is an ordinary in-flight stream.
func TestPartialTextNullMembersAreZeroValues(t *testing.T) {
	prefix, reason := feed(apiChat, 1<<20,
		`{"choices":[{"delta":{"content":"a"},"finish_reason":null}]}`,
		`{"choices":[{"delta":{"content":"b","tool_calls":null,"function_call":null},"finish_reason":null}]}`,
		`{"error":null,"choices":[{"delta":{"content":"c"}}]}`,
	)
	if reason != "" {
		t.Fatalf("a null member was read as a refusal: %q", reason)
	}
	if prefix != "abc" {
		t.Fatalf("prefix = %q, want %q", prefix, "abc")
	}
}

// TestPartialTextReasoningIsNotAssistantText: the reasoning channel is a
// separate output the client renders beside the message, not part of it, so
// reasoning deltas are ignored rather than refused — a stream that thinks
// before answering must still be continuable.
func TestPartialTextReasoningIsNotAssistantText(t *testing.T) {
	prefix, reason := feed(apiResponses, 1<<20,
		`{"type":"response.output_item.added","item":{"type":"reasoning","id":"r"}}`,
		`{"type":"response.reasoning_summary_text.delta","delta":"thinking..."}`,
		`{"type":"response.reasoning_summary_text.done","text":"thinking..."}`,
		`{"type":"response.output_item.done","item":{"type":"reasoning"}}`,
		`{"type":"response.output_item.added","item":{"type":"message","id":"m"}}`,
		`{"type":"response.output_text.delta","delta":"the answer"}`,
	)
	if reason != "" {
		t.Fatalf("a reasoning-then-text stream was refused: %q", reason)
	}
	if prefix != "the answer" {
		t.Fatalf("prefix = %q, want %q", prefix, "the answer")
	}
}

// TestPartialTextOversizeFreesTheBuffer: the bound exists so a recovering
// stream cannot hold an unbounded copy of an answer. Crossing it must both
// refuse the stream and release what was accumulated.
func TestPartialTextOversizeFreesTheBuffer(t *testing.T) {
	p := newPartialText(apiChat, 1<<10)
	chunk := `{"choices":[{"delta":{"content":"` + strings.Repeat("x", 256) + `"}}]}`
	for i := 0; i < 8; i++ {
		p.Observe([]byte(chunk))
	}
	prefix, reason := p.Safe()
	if reason != reasonOversize {
		t.Fatalf("reason = %q, want %q", reason, reasonOversize)
	}
	if prefix != "" || p.partialBytes() != 0 {
		t.Fatalf("the refused accumulator kept %d bytes", p.partialBytes())
	}
}

// TestPartialTextRefusalIsAPrefixOfNothing: the two APIs refuse each other's
// payloads. A chat payload handed to a Responses accumulator has no `type`
// and is refused; the reverse is refused as a chunk with no choices. This is
// the API scoping the response rewrites already have, applied to the gate.
func TestPartialTextIsAPIScoped(t *testing.T) {
	if _, reason := feed(apiResponses, 1<<20, `{"choices":[{"delta":{"content":"x"}}]}`); reason != reasonUnknownShape {
		t.Fatalf("a chat payload under the responses surface = %q, want %q", reason, reasonUnknownShape)
	}
	if prefix, reason := feed(apiChat, 1<<20, `{"type":"response.output_text.delta","delta":"x"}`); reason != reasonNoPrefix || prefix != "" {
		t.Fatalf("a responses payload under the chat surface = %q/%q, want empty/%q", prefix, reason, reasonNoPrefix)
	}
}
