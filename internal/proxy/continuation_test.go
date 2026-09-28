package proxy

import (
	"strings"
	"testing"
)

// feed pushes a sequence of payloads through a fresh accumulator and returns
// its verdict. Each payload is one admitted data line, exactly as CopySSE
// hands them over.
func feed(api string, limit int, payloads ...string) continuationVerdict {
	p := newPartialText(api, limit)
	for _, s := range payloads {
		p.Observe([]byte(s))
	}
	return p.Verdict()
}

// recovered asserts a verdict is the recoverable one and returns its prefix.
func recovered(t *testing.T, v continuationVerdict) string {
	t.Helper()
	if v.Kind != verdictRecoverable {
		t.Fatalf("verdict = %v/%q, want recoverable", v.Kind, v.Reason)
	}
	if v.Text == "" {
		t.Fatal("a recoverable verdict carried no text")
	}
	return v.Text
}

func TestPartialTextAccumulatesChatContent(t *testing.T) {
	v := feed(apiChat, 1<<20,
		`{"choices":[{"index":0,"delta":{"role":"assistant"},"finish_reason":null}]}`,
		`{"choices":[{"index":0,"delta":{"content":"Hello"},"finish_reason":null}]}`,
		`{"choices":[{"index":0,"delta":{"content":", "},"finish_reason":null}]}`,
		`{"choices":[{"index":0,"delta":{"content":"world"},"finish_reason":null}]}`,
		`[DONE]`,
	)
	if got := recovered(t, v); got != "Hello, world" {
		t.Fatalf("prefix = %q, want %q", got, "Hello, world")
	}
}

func TestPartialTextAccumulatesResponsesContent(t *testing.T) {
	v := feed(apiResponses, 1<<20,
		`{"type":"response.created","response":{"id":"r1"}}`,
		`{"type":"response.in_progress","response":{"id":"r1"}}`,
		`{"type":"response.output_item.added","output_index":0,"item":{"type":"message","id":"m1"}}`,
		`{"type":"response.content_part.added","item_id":"m1","output_index":0,"content_index":0,"part":{"type":"output_text"}}`,
		`{"type":"response.output_text.delta","item_id":"m1","output_index":0,"content_index":0,"delta":"Once"}`,
		`{"type":"response.output_text.delta","item_id":"m1","output_index":0,"content_index":0,"delta":" upon a time"}`,
		`{"type":"response.output_text.done","item_id":"m1","output_index":0,"content_index":0,"text":"Once upon a time"}`,
		`{"type":"response.content_part.done","item_id":"m1","output_index":0,"content_index":0}`,
		`{"type":"response.output_item.done","output_index":0,"item":{"type":"message","id":"m1"}}`,
		`{"type":"response.completed","response":{"id":"r1"}}`,
	)
	if got := recovered(t, v); got != "Once upon a time" {
		t.Fatalf("prefix = %q, want %q", got, "Once upon a time")
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
				`{"type":"response.output_item.added","output_index":0,"item":{"type":"function_call","name":"ls"}}`,
			},
			want: reasonToolCalls,
		},
		{
			name: "responses unknown item type",
			api:  apiResponses,
			payloads: []string{
				`{"type":"response.output_item.added","output_index":0,"item":{"type":"web_search_call"}}`,
			},
			want: reasonToolCalls,
		},
		{
			name: "responses argument delta",
			api:  apiResponses,
			payloads: []string{
				`{"type":"response.function_call_arguments.delta","item_id":"fc1","output_index":0,"delta":"{\"pa"}`,
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
			v := feed(tc.api, limit, tc.payloads...)
			if v.Kind != verdictUnsafe || v.Reason != tc.want {
				t.Fatalf("verdict = %v/%q, want unsafe/%q", v.Kind, v.Reason, tc.want)
			}
			if v.Text != "" {
				t.Fatalf("a refused stream still reported a prefix: %q", v.Text)
			}
		})
	}
}

// TestPartialTextFinishReasonIsTerminal: a non-null Chat `finish_reason` says
// the generation is COMPLETE. That forbids recovery exactly as a refusal does
// — there is nothing left to continue — but it is a different fact, and the
// accumulator must not report it as one of the unsafe-content reasons. A
// stream the upstream finished is not a stream this proxy failed to read.
//
// Note what this does NOT do: it never synthesizes the terminal marker the
// client keys on. The wire keeps exactly the bytes the upstream sent, which is
// the whole reason the relay's own `Terminal` fact is tracked separately.
func TestPartialTextFinishReasonIsTerminal(t *testing.T) {
	for _, reason := range []string{"stop", "length", "content_filter", "tool_calls"} {
		t.Run("finish_reason="+reason, func(t *testing.T) {
			v := feed(apiChat, 1<<20,
				`{"choices":[{"index":0,"delta":{"content":"hello"},"finish_reason":null}]}`,
				`{"choices":[{"index":0,"delta":{},"finish_reason":"`+reason+`"}]}`,
			)
			if v.Kind != verdictTerminal {
				t.Fatalf("verdict = %v/%q, want terminal", v.Kind, v.Reason)
			}
			if v.Reason != "" {
				t.Fatalf("a terminal stream reported the reason %q: a declared finish is not unsafe content", v.Reason)
			}
			if v.Text != "" {
				t.Fatalf("a terminal stream still offered a prefix: %q", v.Text)
			}
		})
	}
}

// TestPartialTextTerminalChunkIsStillRead pins the ordering the accumulator
// owes its gate: a chunk carrying BOTH a delta and a finish_reason is read in
// full before the terminal is latched.
//
// The bug this pins is a fail-open one on the unsafe side. The finish_reason
// check used to run FIRST and return, so `{"delta":{"content":"…"},
// "finish_reason":"stop"}` was classified terminal and the content dropped on
// the floor — the stream looked finished when it had not been read at all, and
// a chunk carrying BOTH a tool call and a finish_reason was classified a
// clean logical terminal instead of the hard refusal it is. Either way the
// accumulator decided WITHOUT LOOKING at the payload, which is the one thing
// this gate may never do.
func TestPartialTextTerminalChunkIsStillRead(t *testing.T) {
	// A tool call in the same chunk as the finish_reason is a tool call. The
	// verdict is unsafe/tool_calls, NOT terminal — reading the terminal first
	// would launder a tool call into a clean finish.
	for _, chunk := range []string{
		`{"choices":[{"index":0,"delta":{"content":"calling","tool_calls":[{"index":0,"id":"call_1","function":{"name":"f","arguments":"{}"}}]},"finish_reason":"tool_calls"}]}`,
		`{"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"call_1","function":{"name":"f","arguments":"{}"}}]},"finish_reason":"stop"}]}`,
		`{"choices":[{"index":0,"delta":{"content":"calling","function_call":{"name":"f","arguments":"{}"}},"finish_reason":"function_call"}]}`,
	} {
		t.Run(chunk, func(t *testing.T) {
			v := feed(apiChat, 1<<20, chunk)
			if v.Kind != verdictUnsafe || v.Reason != reasonToolCalls {
				t.Fatalf("verdict = %v/%q, want unsafe/%s — a tool call must fail closed whatever the finish_reason says",
					v.Kind, v.Reason, reasonToolCalls)
			}
		})
	}
	// A content-bearing terminal chunk is READ, then latched: the text is
	// accumulated (so the size bound still applies) and the stream is still
	// terminal, so no continuation is ever built from it.
	for _, chunk := range []string{
		`{"choices":[{"index":0,"delta":{"content":"hello"},"finish_reason":"stop"}]}`,
		`{"choices":[{"index":0,"delta":{"content":"hello"},"finish_reason":"length"}]}`,
	} {
		t.Run(chunk, func(t *testing.T) {
			p := newPartialText(apiChat, 1<<20)
			p.Observe([]byte(chunk))
			if v := p.Verdict(); v.Kind != verdictTerminal {
				t.Fatalf("verdict = %v/%q, want terminal", v.Kind, v.Reason)
			}
			// The delta was read: the accumulator saw the bytes, applied the
			// size bound, and only THEN released them for a terminal. A fix
			// that just reordered the checks without reading the delta would
			// leave the prefix unread — and the size bound unapplied.
			if p.partialBytes() != 0 {
				t.Fatalf("a terminal accumulator still holds %d bytes", p.partialBytes())
			}
		})
	}
	// The size bound applies to a terminal chunk's content too: a terminal
	// chunk carrying an over-limit prefix is still refused, not accepted as a
	// clean finish. This is the proof the content is really being read, not
	// merely skipped.
	p := newPartialText(apiChat, 8)
	p.Observe([]byte(`{"choices":[{"index":0,"delta":{"content":"0123456789"},"finish_reason":"stop"}]}`))
	v := p.Verdict()
	if v.Kind != verdictUnsafe || v.Reason != reasonOversize {
		t.Fatalf("verdict = %v/%q, want unsafe/%s — an over-limit terminal chunk was read as a clean finish",
			v.Kind, v.Reason, reasonOversize)
	}
}

// TestPartialTextFinishReasonIsLatched: the terminal sticks. Content arriving
// after a declared finish — a provider that keeps a socket open, or one whose
// final chunk is followed by another delta — cannot make the stream
// continuable again.
func TestPartialTextFinishReasonIsLatched(t *testing.T) {
	for _, after := range []string{
		`{"choices":[{"index":0,"delta":{"content":" more"},"finish_reason":null}]}`,
		`[DONE]`,
		`{"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"call_1"}]}}]}`,
	} {
		t.Run(after, func(t *testing.T) {
			p := newPartialText(apiChat, 1<<20)
			p.Observe([]byte(`{"choices":[{"index":0,"delta":{"content":"done"},"finish_reason":"stop"}]}`))
			p.Observe([]byte(after))
			if v := p.Verdict(); v.Kind != verdictTerminal {
				t.Fatalf("verdict = %v/%q, want the latched terminal", v.Kind, v.Reason)
			}
			if p.partialBytes() != 0 {
				t.Fatalf("a terminal accumulator still holds %d bytes", p.partialBytes())
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

	v := p.Verdict()
	if v.Kind != verdictUnsafe || v.Reason != reasonToolCalls {
		t.Fatalf("verdict = %v/%q, want unsafe/%q", v.Kind, v.Reason, reasonToolCalls)
	}
	if v.Text != "" {
		t.Fatalf("prefix = %q, want empty — a refused stream never reports text", v.Text)
	}
	// The latched reason survives further calls and further observations, and
	// an unsafe latch outranks a later terminal: first verdict wins.
	p.Observe([]byte(`{"choices":[{"delta":{"content":"more"},"finish_reason":"stop"}]}`))
	if again := p.Verdict(); again.Kind != verdictUnsafe || again.Reason != reasonToolCalls {
		t.Fatalf("second verdict = %v/%q, want the latched unsafe/%q", again.Kind, again.Reason, reasonToolCalls)
	}
	if p.partialBytes() != 0 {
		t.Fatalf("a refused accumulator still holds %d bytes", p.partialBytes())
	}
}

// TestPartialTextDoneIsNotText: chat's [DONE] is a marker, not a payload. It
// must not be treated as an unparseable data line — that would refuse every
// chat stream at the last event — and it must not be accumulated either.
func TestPartialTextDoneIsNotText(t *testing.T) {
	v := feed(apiChat, 1<<20,
		`{"choices":[{"delta":{"content":"hi"}}]}`,
		`[DONE]`,
	)
	if got := recovered(t, v); got != "hi" {
		t.Fatalf("prefix = %q, want %q", got, "hi")
	}
}

// TestPartialTextNullMembersAreZeroValues: the OpenAI APIs treat a null member
// as its zero value, so a null is never a refusal. A stream that states
// finish_reason explicitly as null is an ordinary in-flight stream.
func TestPartialTextNullMembersAreZeroValues(t *testing.T) {
	v := feed(apiChat, 1<<20,
		`{"choices":[{"delta":{"content":"a"},"finish_reason":null}]}`,
		`{"choices":[{"delta":{"content":"b","tool_calls":null,"function_call":null},"finish_reason":null}]}`,
		`{"error":null,"choices":[{"delta":{"content":"c"}}]}`,
	)
	if got := recovered(t, v); got != "abc" {
		t.Fatalf("prefix = %q, want %q", got, "abc")
	}
}

// TestPartialTextNeedsNoPrefix: a stream that carried no text at all is not
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
		{"responses with empty deltas", apiResponses, []string{`{"type":"response.output_text.delta","item_id":"m1","output_index":0,"content_index":0,"delta":""}`}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v := feed(tc.api, 1<<20, tc.payloads...)
			if v.Kind != verdictUnsafe || v.Reason != reasonNoPrefix {
				t.Fatalf("verdict = %v/%q, want unsafe/%q", v.Kind, v.Reason, reasonNoPrefix)
			}
			if v.Text != "" {
				t.Fatalf("prefix = %q, want empty", v.Text)
			}
		})
	}
}

// TestPartialTextReasoningIsNotAssistantText: the reasoning channel is a
// separate output the client renders beside the message, not part of it, so
// reasoning deltas are ignored rather than refused — a stream that thinks
// before answering must still be continuable.
func TestPartialTextReasoningIsNotAssistantText(t *testing.T) {
	v := feed(apiResponses, 1<<20,
		`{"type":"response.output_item.added","output_index":0,"item":{"type":"reasoning","id":"r"}}`,
		`{"type":"response.reasoning_summary_text.delta","item_id":"r","output_index":0,"summary_index":0,"delta":"thinking..."}`,
		`{"type":"response.reasoning_summary_text.done","item_id":"r","output_index":0,"summary_index":0,"text":"thinking..."}`,
		`{"type":"response.output_item.done","output_index":0,"item":{"type":"reasoning","id":"r"}}`,
		`{"type":"response.output_item.added","output_index":1,"item":{"type":"message","id":"m"}}`,
		`{"type":"response.output_text.delta","item_id":"m","output_index":1,"content_index":0,"delta":"the answer"}`,
	)
	if got := recovered(t, v); got != "the answer" {
		t.Fatalf("prefix = %q, want %q", got, "the answer")
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
	v := p.Verdict()
	if v.Kind != verdictUnsafe || v.Reason != reasonOversize {
		t.Fatalf("verdict = %v/%q, want unsafe/%q", v.Kind, v.Reason, reasonOversize)
	}
	if v.Text != "" || p.partialBytes() != 0 {
		t.Fatalf("the refused accumulator kept %d bytes", p.partialBytes())
	}
}

// TestPartialTextIsAPIScoped: the two APIs refuse each other's payloads. A
// chat payload handed to a Responses accumulator has no `type` and is refused;
// the reverse is refused as a chunk with no choices. This is the API scoping
// the response rewrites already have, applied to the gate.
func TestPartialTextIsAPIScoped(t *testing.T) {
	if v := feed(apiResponses, 1<<20, `{"choices":[{"delta":{"content":"x"}}]}`); v.Kind != verdictUnsafe || v.Reason != reasonUnknownShape {
		t.Fatalf("a chat payload under the responses surface = %v/%q, want unsafe/%q", v.Kind, v.Reason, reasonUnknownShape)
	}
	if v := feed(apiChat, 1<<20, `{"type":"response.output_text.delta","delta":"x"}`); v.Kind != verdictUnsafe || v.Reason != reasonNoPrefix {
		t.Fatalf("a responses payload under the chat surface = %v/%q, want unsafe/%q", v.Kind, v.Reason, reasonNoPrefix)
	}
}

// TestPartialTextResponsesIdentityIsOneStream is Task-3's contract at the
// accumulator: the MVP continues plain text from EXACTLY ONE message output's
// EXACTLY ONE content stream, and proves it from the identity every delta
// carries. Two streams' deltas interleave on the wire, so concatenating them
// would hand the model an assistant turn that never existed — a corruption
// the client cannot see, which is strictly worse than the truncation it would
// have replaced.
func TestPartialTextResponsesIdentityIsOneStream(t *testing.T) {
	const (
		m1  = `{"type":"response.output_text.delta","item_id":"m1","output_index":0,"content_index":0,"delta":"hello"}`
		m1b = `{"type":"response.output_text.delta","item_id":"m1","output_index":0,"content_index":0,"delta":" world"}`
		m2  = `{"type":"response.output_text.delta","item_id":"m2","output_index":1,"content_index":0,"delta":"world"}`
	)
	cases := []struct {
		name     string
		payloads []string
		want     string
		recover  bool
	}{
		{
			name:     "single item single stream",
			payloads: []string{m1},
			recover:  true,
		},
		{
			name:     "same identity throughout",
			payloads: []string{m1, m1b},
			recover:  true,
		},
		{
			name:     "item id changes",
			payloads: []string{m1, m2},
			want:     reasonMultipleOutputs,
		},
		{
			name: "output index changes on the same item",
			payloads: []string{m1,
				`{"type":"response.output_text.delta","item_id":"m1","output_index":1,"content_index":0,"delta":"x"}`,
			},
			want: reasonMultipleOutputs,
		},
		{
			name: "content index changes on the same item",
			payloads: []string{m1,
				`{"type":"response.output_text.delta","item_id":"m1","output_index":0,"content_index":1,"delta":"x"}`,
			},
			want: reasonMultipleOutputs,
		},
		{
			name: "channel switches from text to refusal",
			payloads: []string{m1,
				`{"type":"response.refusal.delta","item_id":"m1","output_index":0,"content_index":0,"delta":"I cannot"}`,
			},
			want: reasonMultipleOutputs,
		},
		{
			name: "second message item announced",
			payloads: []string{
				`{"type":"response.output_item.added","output_index":0,"item":{"type":"message","id":"m1"}}`,
				m1,
				`{"type":"response.output_item.added","output_index":1,"item":{"type":"message","id":"m2"}}`,
			},
			want: reasonMultipleOutputs,
		},
		{
			name: "tool call item appears",
			payloads: []string{
				m1,
				`{"type":"response.output_item.added","output_index":1,"item":{"type":"function_call","id":"fc1"}}`,
			},
			want: reasonToolCalls,
		},
		{
			name: "missing item id",
			payloads: []string{
				`{"type":"response.output_text.delta","output_index":0,"content_index":0,"delta":"x"}`,
			},
			want: reasonUnknownShape,
		},
		{
			name: "missing output index",
			payloads: []string{
				`{"type":"response.output_text.delta","item_id":"m1","content_index":0,"delta":"x"}`,
			},
			want: reasonUnknownShape,
		},
		{
			name: "missing content index",
			payloads: []string{
				`{"type":"response.output_text.delta","item_id":"m1","output_index":0,"delta":"x"}`,
			},
			want: reasonUnknownShape,
		},
		{
			name: "identity is not a number",
			payloads: []string{
				`{"type":"response.output_text.delta","item_id":"m1","output_index":"0","content_index":0,"delta":"x"}`,
			},
			want: reasonUnknownShape,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			v := feed(apiResponses, 1<<20, tc.payloads...)
			if tc.recover {
				if v.Kind != verdictRecoverable {
					t.Fatalf("verdict = %v/%q, want recoverable", v.Kind, v.Reason)
				}
				return
			}
			if v.Kind != verdictUnsafe || v.Reason != tc.want {
				t.Fatalf("verdict = %v/%q, want unsafe/%q", v.Kind, v.Reason, tc.want)
			}
			if v.Text != "" {
				t.Fatalf("a refused multi-output stream still offered the prefix %q", v.Text)
			}
		})
	}
}

// TestPartialTextResponsesIdentitySpansHops: the identity contract is about
// the LOGICAL stream, so a hop's deltas must match the prefix's identity
// exactly as the first hop's did. This is the case a per-pass accumulator
// would miss: the second hop reuses the same item and index, which is what an
// upstream continuing its own answer does.
func TestPartialTextResponsesIdentitySpansHops(t *testing.T) {
	p := newPartialText(apiResponses, 1<<20)
	p.Observe([]byte(`{"type":"response.output_text.delta","item_id":"m1","output_index":0,"content_index":0,"delta":"Once"}`))
	// Hop 1 ended here; hop 2's deltas arrive on the same accumulator.
	p.Observe([]byte(`{"type":"response.output_text.delta","item_id":"m1","output_index":0,"content_index":0,"delta":" upon a time"}`))
	if got := recovered(t, p.Verdict()); got != "Once upon a time" {
		t.Fatalf("prefix = %q, want the two hops joined", got)
	}

	// A hop that continues a DIFFERENT item is refused, not appended.
	q := newPartialText(apiResponses, 1<<20)
	q.Observe([]byte(`{"type":"response.output_text.delta","item_id":"m1","output_index":0,"content_index":0,"delta":"Once"}`))
	q.Observe([]byte(`{"type":"response.output_text.delta","item_id":"m9","output_index":0,"content_index":0,"delta":" upon a time"}`))
	if v := q.Verdict(); v.Kind != verdictUnsafe || v.Reason != reasonMultipleOutputs {
		t.Fatalf("verdict = %v/%q, want unsafe/%q", v.Kind, v.Reason, reasonMultipleOutputs)
	}
}
