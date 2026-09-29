package proxy

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

// observe feeds one payload to the accumulator under the frame name its own
// `type` names — which, for these cases, is the name a real relay would have
// seen, so the two halves of the frame agree by construction. The agreement
// check is exercised for real by the refusal tests further down, which hand
// the accumulator a name that deliberately disagrees. Chat frames carry no
// `event:` line at all, so their name is nil, exactly as CopySSE reports it.
func observe(p *partialText, payload string) {
	raw := []byte(payload)
	if p.api == apiChat {
		p.Observe(nil, raw)
		return
	}
	var probe struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(raw, &probe); err != nil {
		p.Observe(nil, raw)
		return
	}
	p.Observe([]byte(probe.Type), raw)
}

// feed pushes a sequence of payloads through a fresh accumulator and returns
// its verdict. Each payload is one admitted data line, exactly as CopySSE
// hands them over.
func feed(api string, limit int, payloads ...string) continuationVerdict {
	p := newPartialText(api, limit)
	for _, s := range payloads {
		observe(p, s)
	}
	return p.Verdict()
}

// feedPasses pushes one or more UPSTREAM RESPONSES through a fresh
// accumulator and returns its verdict. Each pass is one upstream body, and the
// helper calls beginUpstreamStream before it exactly as the relay closure does
// — once per relay invocation, which is once per upstream HTTP response.
func feedPasses(api string, limit int, passes ...[]string) continuationVerdict {
	p := newPartialText(api, limit)
	for _, pass := range passes {
		p.beginUpstreamStream()
		for _, s := range pass {
			observe(p, s)
		}
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

// TestPartialTextAccumulatesResponsesContent proves a cut Responses stream is
// recoverable while its one output-text channel remains in progress.
func TestPartialTextAccumulatesResponsesContent(t *testing.T) {
	v := feed(apiResponses, 1<<20,
		`{"type":"response.created","response":{"id":"r1"}}`,
		`{"type":"response.in_progress","response":{"id":"r1"}}`,
		`{"type":"response.output_item.added","output_index":0,"item":{"type":"message","id":"m1"}}`,
		`{"type":"response.content_part.added","item_id":"m1","output_index":0,"content_index":0,"part":{"type":"output_text"}}`,
		`{"type":"response.output_text.delta","item_id":"m1","output_index":0,"content_index":0,"delta":"Once"}`,
		`{"type":"response.output_text.delta","item_id":"m1","output_index":0,"content_index":0,"delta":" upon a time"}`,
	)
	if got := recovered(t, v); got != "Once upon a time" {
		t.Fatalf("prefix = %q, want %q", got, "Once upon a time")
	}
}

// TestPartialTextResponsesOutputTextDoneIsLogicalTerminal distinguishes the
// content channel's own completed signal from the stream's wire terminal. A
// cut after output_text.done is not safe to continue: the generation ended,
// even if response.completed never arrived.
func TestPartialTextResponsesOutputTextDoneIsLogicalTerminal(t *testing.T) {
	p := newPartialText(apiResponses, 1<<20)
	observe(p, `{"type":"response.output_text.delta","item_id":"m1","output_index":0,"content_index":0,"delta":"Once upon a "}`)
	observe(p, `{"type":"response.output_text.done","item_id":"m1","output_index":0,"content_index":0,"text":"Once upon a time"}`)
	if v := p.Verdict(); v.Kind != verdictUnsafe || v.Reason != reasonUnknownShape {
		t.Fatalf("mismatched output_text.done = %v/%q, want unsafe/%q", v.Kind, v.Reason, reasonUnknownShape)
	}

	p = newPartialText(apiResponses, 1<<20)
	observe(p, `{"type":"response.output_text.delta","item_id":"m1","output_index":0,"content_index":0,"delta":"Once upon a time"}`)
	observe(p, `{"type":"response.output_text.done","item_id":"m1","output_index":0,"content_index":0,"text":"Once upon a time"}`)
	if v := p.Verdict(); v.Kind != verdictTerminal {
		t.Fatalf("matching output_text.done = %v/%q, want terminal", v.Kind, v.Reason)
	}

	p = newPartialText(apiResponses, 1<<20)
	observe(p, `{"type":"response.output_text.done","item_id":"m1","output_index":0,"content_index":0,"text":"final delta"}`)
	if v := p.Verdict(); v.Kind != verdictTerminal {
		t.Fatalf("text-only output_text.done = %v/%q, want terminal", v.Kind, v.Reason)
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
				`{"choices":[{"index":0,"delta":{"content":"Let me check"}}]}`,
				`{"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"call_1","function":{"name":"ls"}}]}}]}`,
			},
			want: reasonToolCalls,
		},
		{
			name: "chat legacy function_call",
			api:  apiChat,
			payloads: []string{
				`{"choices":[{"index":0,"delta":{"function_call":{"name":"ls"}}}]}`,
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
			payloads: []string{`{"choices":[{"index":0,"delta":{"content":[{"type":"text","text":"x"}]}}]}`},
			want:     reasonUnknownShape,
		},
		{
			name:     "chat several choices",
			api:      apiChat,
			payloads: []string{`{"choices":[{"index":0,"delta":{"content":"a"}},{"index":1,"delta":{"content":"b"}}]}`},
			want:     reasonMultipleOutputs,
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
			name: "responses completed has nested error",
			api:  apiResponses,
			payloads: []string{
				`{"type":"response.output_text.delta","item_id":"m1","output_index":0,"content_index":0,"delta":"before"}`,
				`{"type":"response.completed","response":{"id":"r1","error":{"code":"server_error"}}}`,
			},
			want: reasonUpstreamTerminal,
		},
		{
			name: "responses lifecycle response is not object",
			api:  apiResponses,
			payloads: []string{
				`{"type":"response.completed","response":"not an object"}`,
			},
			want: reasonUnknownShape,
		},
		{
			name: "responses refusal done carries text",
			api:  apiResponses,
			payloads: []string{
				`{"type":"response.refusal.done","item_id":"m1","output_index":0,"content_index":0,"refusal":"I can't help with that."}`,
			},
			want: reasonRefusal,
		},
		{
			name: "responses refusal done text is not a string",
			api:  apiResponses,
			payloads: []string{
				`{"type":"response.refusal.done","refusal":{"text":"I can't help"}}`,
			},
			want: reasonUnknownShape,
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
			payloads: []string{`{"choices":[{"index":0,"delta":{"content":"0123456789"}}]}`},
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
			observe(p, chunk)
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
	observe(p, `{"choices":[{"index":0,"delta":{"content":"0123456789"},"finish_reason":"stop"}]}`)
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
			observe(p, `{"choices":[{"index":0,"delta":{"content":"done"},"finish_reason":"stop"}]}`)
			observe(p, after)
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
	observe(p, `{"choices":[{"index":0,"delta":{"content":"before"}}]}`)
	observe(p, `{"choices":[{"index":0,"delta":{"tool_calls":[{"index":0}]}}]}`)
	observe(p, `{"choices":[{"index":0,"delta":{"content":" after"}}]}`)

	v := p.Verdict()
	if v.Kind != verdictUnsafe || v.Reason != reasonToolCalls {
		t.Fatalf("verdict = %v/%q, want unsafe/%q", v.Kind, v.Reason, reasonToolCalls)
	}
	if v.Text != "" {
		t.Fatalf("prefix = %q, want empty — a refused stream never reports text", v.Text)
	}
	// The latched reason survives further calls and further observations, and
	// an unsafe latch outranks a later terminal: first verdict wins.
	observe(p, `{"choices":[{"index":0,"delta":{"content":"more"},"finish_reason":"stop"}]}`)
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
		`{"choices":[{"index":0,"delta":{"content":"hi"}}]}`,
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
		`{"choices":[{"index":0,"delta":{"content":"a"},"finish_reason":null}]}`,
		`{"choices":[{"index":0,"delta":{"content":"b","tool_calls":null,"function_call":null},"finish_reason":null}]}`,
		`{"error":null,"choices":[{"index":0,"delta":{"content":"c"}}]}`,
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
		{"chat with an empty delta", apiChat, []string{`{"choices":[{"index":0,"delta":{"role":"assistant"}}]}`}},
		{"chat with null content", apiChat, []string{`{"choices":[{"index":0,"delta":{"content":null}}]}`}},
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

// TestPartialTextEmptyRefusalDoneIsIgnored is the fail-open half of the
// refusal terminal: a `response.refusal.done` that states no refusal — absent,
// null or empty — is the structural terminator of a content part this stream
// never carried. Treating it as a decline would refuse every stream whose
// provider emits the terminator unconditionally, and binding its identity
// would refuse the text deltas that follow it. The stream stays continuable.
//
// The asymmetry with TestPartialTextRefusalDeltaIsNeverContinuationText — an
// EMPTY refusal DELTA latches the refusal, an empty refusal terminator does
// not — is deliberate. A .done is the structural terminator the API emits for
// every content part a response declares, textless parts included, so a
// textless one is an ordinary shape. A .delta exists only to carry text, so
// its event type alone is the declaration that the model declined.
func TestPartialTextEmptyRefusalDoneIsIgnored(t *testing.T) {
	for _, done := range []string{
		`{"type":"response.refusal.done","item_id":"m1","output_index":0,"content_index":0}`,
		`{"type":"response.refusal.done","item_id":"m1","output_index":0,"content_index":0,"refusal":null}`,
		`{"type":"response.refusal.done","item_id":"m1","output_index":0,"content_index":0,"refusal":""}`,
	} {
		t.Run(done, func(t *testing.T) {
			v := feed(apiResponses, 1<<20,
				`{"type":"response.output_text.delta","item_id":"m1","output_index":0,"content_index":0,"delta":"hello "}`,
				done,
				`{"type":"response.output_text.delta","item_id":"m1","output_index":0,"content_index":0,"delta":"world"}`,
			)
			if got := recovered(t, v); got != "hello world" {
				t.Fatalf("prefix = %q, want %q — an empty refusal terminator must not end the text stream", got, "hello world")
			}
		})
	}
}

// TestPartialTextOversizeFreesTheBuffer: the bound exists so a recovering
// stream cannot hold an unbounded copy of an answer. Crossing it must both
// refuse the stream and release what was accumulated.
func TestPartialTextOversizeFreesTheBuffer(t *testing.T) {
	p := newPartialText(apiChat, 1<<10)
	chunk := `{"choices":[{"index":0,"delta":{"content":"` + strings.Repeat("x", 256) + `"}}]}`
	for i := 0; i < 8; i++ {
		observe(p, chunk)
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
			want: reasonRefusal,
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

// TestPartialTextResponsesIdentityIsPerUpstreamResponse fixes the SCOPE of
// the identity above. A continuation hop is a new upstream response: it
// announces its own output item and emits its own deltas, so its honest
// identity differs from the committed reply's by construction. The identity
// therefore resets at the relay boundary while the prefix joins, because
// judging the hop against the earlier response's item refused every real
// second hop as multiple_outputs — after that hop was already dialed and paid
// for out of the request's own envelope.
//
// The single-response half of the contract is not weakened, only scoped: see
// TestPartialTextResponsesIdentityIsOneStream and the same-pass rows below.
func TestPartialTextResponsesIdentityIsPerUpstreamResponse(t *testing.T) {
	delta := func(item string, outputIndex, contentIndex int, text string) string {
		return fmt.Sprintf(`{"type":"response.output_text.delta","item_id":%q,"output_index":%d,"content_index":%d,"delta":%q}`,
			item, outputIndex, contentIndex, text)
	}
	done := func(item string, outputIndex, contentIndex int, text string) string {
		return fmt.Sprintf(`{"type":"response.output_text.done","item_id":%q,"output_index":%d,"content_index":%d,"text":%q}`,
			item, outputIndex, contentIndex, text)
	}
	added := func(item string, outputIndex int) string {
		return fmt.Sprintf(`{"type":"response.output_item.added","output_index":%d,"item":{"type":"message","id":%q}}`,
			outputIndex, item)
	}
	cases := []struct {
		name    string
		passes  [][]string
		want    string // recoverable prefix
		recover bool
		reason  string
	}{
		{
			name: "a hop's own item id joins the committed prefix",
			passes: [][]string{
				{added("m1", 0), delta("m1", 0, 0, "Once")},
				{added("m2", 0), delta("m2", 0, 0, " upon a time")},
			},
			recover: true,
			want:    "Once upon a time",
		},
		{
			name: "a hop's own output index joins too",
			passes: [][]string{
				{added("m1", 0), delta("m1", 0, 0, "Once")},
				{added("m2", 1), delta("m2", 1, 0, " upon a time")},
			},
			recover: true,
			want:    "Once upon a time",
		},
		{
			name: "a hop that announces nothing still joins by its deltas",
			passes: [][]string{
				{added("m1", 0), delta("m1", 0, 0, "Once")},
				{delta("m2", 0, 0, " upon a time")},
			},
			recover: true,
			want:    "Once upon a time",
		},
		{
			name: "two item ids inside ONE response still refuse",
			passes: [][]string{
				{added("m1", 0), delta("m1", 0, 0, "Once"), delta("m2", 0, 0, " upon a time")},
			},
			reason: reasonMultipleOutputs,
		},
		{
			name: "a second message item inside ONE response still refuses",
			passes: [][]string{
				{added("m1", 0), delta("m1", 0, 0, "Once"), added("m2", 1)},
			},
			reason: reasonMultipleOutputs,
		},
		{
			name: "a content index change inside ONE response still refuses",
			passes: [][]string{
				{added("m1", 0), delta("m1", 0, 0, "Once"), delta("m1", 0, 1, " upon a time")},
			},
			reason: reasonMultipleOutputs,
		},
		{
			name: "a hop's own .done is compared with the hop's own deltas",
			passes: [][]string{
				{added("m1", 0), delta("m1", 0, 0, "Once")},
				{added("m2", 0), delta("m2", 0, 0, " upon a time"), done("m2", 0, 0, " upon a time")},
			},
			reason: "", // terminal, asserted below
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			v := feedPasses(apiResponses, 1<<20, tc.passes...)
			switch {
			case tc.recover:
				if got := recovered(t, v); got != tc.want {
					t.Fatalf("prefix = %q, want %q", got, tc.want)
				}
			case tc.reason == "":
				if v.Kind != verdictTerminal {
					t.Fatalf("verdict = %v/%q, want terminal", v.Kind, v.Reason)
				}
			default:
				if v.Kind != verdictUnsafe || v.Reason != tc.reason {
					t.Fatalf("verdict = %v/%q, want unsafe/%q", v.Kind, v.Reason, tc.reason)
				}
				if v.Text != "" {
					t.Fatalf("a refused stream still offered the prefix %q", v.Text)
				}
			}
		})
	}
}

// TestPartialTextResponsesDoneIsPerUpstreamResponse pins the second facet of
// the same seam: a .done event states the text of the response that emitted
// it, so it is compared with that response's own deltas, never with the whole
// cross-hop prefix. The .done-only row is the one a naive fix gets wrong — its
// cross-hop prefix is non-empty, so testing the wrong region sends a correct
// event into the compare branch and refuses it for being right.
func TestPartialTextResponsesDoneIsPerUpstreamResponse(t *testing.T) {
	const (
		deltaM1 = `{"type":"response.output_text.delta","item_id":"m1","output_index":0,"content_index":0,"delta":"Once"}`
		deltaM2 = `{"type":"response.output_text.delta","item_id":"m2","output_index":0,"content_index":0,"delta":" upon a time"}`
	)

	t.Run("a hop emitting only its own .done terminates", func(t *testing.T) {
		v := feedPasses(apiResponses, 1<<20,
			[]string{deltaM1},
			[]string{`{"type":"response.output_text.done","item_id":"m2","output_index":0,"content_index":0,"text":" upon a time"}`},
		)
		if v.Kind != verdictTerminal {
			t.Fatalf("verdict = %v/%q, want terminal", v.Kind, v.Reason)
		}
	})

	t.Run("a hop's .done disagreeing with its own deltas refuses", func(t *testing.T) {
		v := feedPasses(apiResponses, 1<<20,
			[]string{deltaM1},
			[]string{deltaM2, `{"type":"response.output_text.done","item_id":"m2","output_index":0,"content_index":0,"text":"Once upon a time"}`},
		)
		if v.Kind != verdictUnsafe || v.Reason != reasonUnknownShape {
			t.Fatalf("verdict = %v/%q, want unsafe/%q", v.Kind, v.Reason, reasonUnknownShape)
		}
	})

	t.Run("a within-pass disagreement still refuses", func(t *testing.T) {
		v := feed(apiResponses, 1<<20,
			`{"type":"response.output_text.delta","item_id":"m1","output_index":0,"content_index":0,"delta":"Once upon a "}`,
			`{"type":"response.output_text.done","item_id":"m1","output_index":0,"content_index":0,"text":"Once upon a time"}`,
		)
		if v.Kind != verdictUnsafe || v.Reason != reasonUnknownShape {
			t.Fatalf("verdict = %v/%q, want unsafe/%q", v.Kind, v.Reason, reasonUnknownShape)
		}
	})
}

// TestPartialTextLatchSpansUpstreamResponses is the other half of the scope
// split: the prefix and the latches are logical-stream properties. A later
// response must never release a refusal an earlier one latched — the stream
// stopped being plain text when it did, and a hop's clean deltas cannot make
// the client's answer whole again.
func TestPartialTextLatchSpansUpstreamResponses(t *testing.T) {
	t.Run("a refusal latches across the boundary", func(t *testing.T) {
		v := feedPasses(apiResponses, 1<<20,
			[]string{`{"type":"response.output_text.delta","item_id":"m1","output_index":0,"content_index":0,"delta":"Let me check"}`, `{"type":"response.function_call_arguments.delta","item_id":"fc1","output_index":1,"delta":"{\"path\""}`},
			[]string{`{"type":"response.output_text.delta","item_id":"m2","output_index":0,"content_index":0,"delta":"plain text again"}`},
		)
		if v.Kind != verdictUnsafe || v.Reason != reasonToolCalls {
			t.Fatalf("verdict = %v/%q, want unsafe/%q", v.Kind, v.Reason, reasonToolCalls)
		}
		if v.Text != "" {
			t.Fatalf("a refused stream still offered the prefix %q", v.Text)
		}
	})

	t.Run("a terminal latches across the boundary", func(t *testing.T) {
		// response.completed is the WIRE terminal (StreamStats.Terminal); the
		// accumulator's logical terminal is latched by the content channel's
		// own completion, which is what this row uses. Either way the reset
		// must not release it: a finished generation is not continued because
		// a later response arrived.
		v := feedPasses(apiResponses, 1<<20,
			[]string{
				`{"type":"response.output_text.delta","item_id":"m1","output_index":0,"content_index":0,"delta":"Once"}`,
				`{"type":"response.output_text.done","item_id":"m1","output_index":0,"content_index":0,"text":"Once"}`,
			},
			[]string{`{"type":"response.output_text.delta","item_id":"m2","output_index":0,"content_index":0,"delta":"More"}`},
		)
		if v.Kind != verdictTerminal {
			t.Fatalf("verdict = %v/%q, want terminal", v.Kind, v.Reason)
		}
	})
}

// TestPartialTextChatChoiceIdentityIsPerUpstreamResponse fixes the SCOPE of
// Chat's choice provenance, which is the same scope as the Responses item
// identity: a continuation hop is a NEW upstream response that numbers its own
// choices from scratch, so the binding resets at the relay boundary rather
// than carrying the committed pass's index into the hop.
//
// The reset is behaviorally inert — 0 is the only index that ever binds — and
// this pins that: the hop's own chunk carries index 0 and joins the prefix. It
// also pins the direction that is NOT inert, so the reset cannot be mistaken
// for a licence: a hop stating ANY other index is refused.
func TestPartialTextChatChoiceIdentityIsPerUpstreamResponse(t *testing.T) {
	v := feedPasses(apiChat, 1<<20,
		[]string{`{"choices":[{"index":0,"delta":{"content":"Once"},"finish_reason":null}]}`},
		[]string{`{"choices":[{"index":0,"delta":{"content":" upon a time"},"finish_reason":null}]}`},
	)
	if got := recovered(t, v); got != "Once upon a time" {
		t.Fatalf("prefix = %q, want %q", got, "Once upon a time")
	}

	// A hop that renumbers its choice is a multi-output stream, however
	// cleanly it reset: the reset restarts the binding, it does not widen it.
	w := feedPasses(apiChat, 1<<20,
		[]string{`{"choices":[{"index":0,"delta":{"content":"Once"},"finish_reason":null}]}`},
		[]string{`{"choices":[{"index":1,"delta":{"content":" upon a time"},"finish_reason":null}]}`},
	)
	if w.Kind != verdictUnsafe || w.Reason != reasonMultipleOutputs {
		t.Fatalf("verdict = %v/%q, want unsafe/%q", w.Kind, w.Reason, reasonMultipleOutputs)
	}
	if w.Text != "" {
		t.Fatalf("a refused stream still offered the prefix %q", w.Text)
	}

	// A Chat finish_reason in pass 1 still latches the logical terminal.
	z := feedPasses(apiChat, 1<<20,
		[]string{`{"choices":[{"index":0,"delta":{"content":"Once"},"finish_reason":"stop"}]}`},
		[]string{`{"choices":[{"index":0,"delta":{"content":"More"},"finish_reason":null}]}`},
	)
	if z.Kind != verdictTerminal {
		t.Fatalf("verdict = %v/%q, want terminal", z.Kind, z.Reason)
	}
}

// ---------------------------------------------------------------------------
// The refusal channel
// ---------------------------------------------------------------------------

// TestPartialTextRefusalDeltaIsNeverContinuationText is the defect this family
// of tests exists for. The refusal channel used to be accumulated through the
// same path as output_text, so a refusal the upstream cut before its own
// refusal.done left the model's decline standing as the "committed prefix" —
// and the proxy would then have re-asked the model to continue a sentence it
// had just refused to write.
//
// The two facts pinned here are the whole contract: the stream is refused
// under its own token, and NOT ONE BYTE of it survives as a prefix — neither
// the refusal text nor the assistant text that preceded it, because a stream
// that will not be continued must not be handed to a continuation body.
func TestPartialTextRefusalDeltaIsNeverContinuationText(t *testing.T) {
	const text = `{"type":"response.output_text.delta","item_id":"m1","output_index":0,"content_index":0,"delta":"Let me think."}`

	t.Run("as the stream's first event", func(t *testing.T) {
		p := newPartialText(apiResponses, 1<<20)
		observe(p, `{"type":"response.refusal.delta","item_id":"m1","output_index":0,"content_index":0,"delta":"I can't help with that."}`)
		v := p.Verdict()
		if v.Kind != verdictUnsafe || v.Reason != reasonRefusal {
			t.Fatalf("verdict = %v/%q, want unsafe/%q", v.Kind, v.Reason, reasonRefusal)
		}
		if v.Text != "" || p.partialBytes() != 0 {
			t.Fatalf("the accumulator kept the refusal as a prefix: %q (%d bytes)", v.Text, p.partialBytes())
		}
	})

	t.Run("after committed text", func(t *testing.T) {
		p := newPartialText(apiResponses, 1<<20)
		observe(p, text)
		observe(p, `{"type":"response.refusal.delta","item_id":"m1","output_index":0,"content_index":0,"delta":"No, I won't."}`)
		v := p.Verdict()
		if v.Kind != verdictUnsafe || v.Reason != reasonRefusal {
			t.Fatalf("verdict = %v/%q, want unsafe/%q", v.Kind, v.Reason, reasonRefusal)
		}
		if v.Text != "" || p.partialBytes() != 0 {
			t.Fatalf("a refused stream still offered a prefix: %q (%d bytes)", v.Text, p.partialBytes())
		}
	})

	// The handler is given no payload at all, so a refusal delta latches
	// whatever it carries: nothing, an unreadable shape, an identity that
	// names a different output, or the same identity the text channel used.
	// There is no value-dependent branch for refusal text to slip through,
	// and "the channel was announced" is the whole statement the accumulator
	// needs — a shape no conforming upstream emits is not worth the hole a
	// payload branch would open.
	for _, delta := range []string{
		`{"type":"response.refusal.delta","item_id":"m1","output_index":0,"content_index":0,"delta":""}`,
		`{"type":"response.refusal.delta","item_id":"m1","output_index":0,"content_index":0,"delta":null}`,
		`{"type":"response.refusal.delta"}`,
		`{"type":"response.refusal.delta","item_id":"m1","output_index":0,"content_index":0,"delta":{"nested":"shape"}}`,
		`{"type":"response.refusal.delta","item_id":"m9","output_index":3,"content_index":7,"delta":"elsewhere"}`,
	} {
		t.Run("latches on "+delta, func(t *testing.T) {
			v := feed(apiResponses, 1<<20, text, delta)
			if v.Kind != verdictUnsafe || v.Reason != reasonRefusal {
				t.Fatalf("verdict = %v/%q, want unsafe/%q", v.Kind, v.Reason, reasonRefusal)
			}
			if v.Text != "" {
				t.Fatalf("a refused stream still offered a prefix: %q", v.Text)
			}
		})
	}
}

// TestPartialTextRefusalLatchSpansUpstreamResponses: the refusal is latched
// for the LOGICAL stream. The relay boundary resets identities — a hop's item
// id is its own — but it must never release a refusal an earlier response
// latched, or a hop's clean text deltas would make the client's answer whole
// again by splicing a continuation onto a decline.
func TestPartialTextRefusalLatchSpansUpstreamResponses(t *testing.T) {
	v := feedPasses(apiResponses, 1<<20,
		[]string{
			`{"type":"response.output_text.delta","item_id":"m1","output_index":0,"content_index":0,"delta":"Let me think."}`,
			`{"type":"response.refusal.delta","item_id":"m1","output_index":0,"content_index":0,"delta":" On reflection, no."}`,
		},
		[]string{
			`{"type":"response.output_item.added","output_index":0,"item":{"type":"message","id":"m2"}}`,
			`{"type":"response.output_text.delta","item_id":"m2","output_index":0,"content_index":0,"delta":" perfectly plain text again"}`,
		},
	)
	if v.Kind != verdictUnsafe || v.Reason != reasonRefusal {
		t.Fatalf("verdict = %v/%q, want the latched unsafe/%q", v.Kind, v.Reason, reasonRefusal)
	}
	if v.Text != "" {
		t.Fatalf("a refused stream still offered the prefix %q", v.Text)
	}
}

// TestPartialTextRefusalDoneIsItsOwnReason: a refusal.done that STATES a
// refusal is the refusal channel's terminal. It is refused as reasonRefusal
// rather than upstream_terminal, because the two facts an operator reads are
// different: the upstream did not fail and the generation was not cut short —
// the model declined, and there is no continuation of an answer it declined to
// write. TestPartialTextEmptyRefusalDoneIsIgnored holds the other half.
func TestPartialTextRefusalDoneIsItsOwnReason(t *testing.T) {
	v := feed(apiResponses, 1<<20,
		`{"type":"response.output_text.delta","item_id":"m1","output_index":0,"content_index":0,"delta":"Let me think."}`,
		`{"type":"response.refusal.done","item_id":"m1","output_index":0,"content_index":0,"refusal":"I can't help with that."}`,
	)
	if v.Kind != verdictUnsafe || v.Reason != reasonRefusal {
		t.Fatalf("verdict = %v/%q, want unsafe/%q", v.Kind, v.Reason, reasonRefusal)
	}
	if v.Text != "" {
		t.Fatalf("the refusal text reached a prefix: %q", v.Text)
	}
}

// ---------------------------------------------------------------------------
// Chat choice provenance
// ---------------------------------------------------------------------------

// TestPartialTextChatChoiceIndexIsProvenance is the accumulator's half of the
// Chat identity contract: exactly one choice per chunk, and its `index` must be
// present and must be the one the committed prefix has been read from. The
// accumulator never assumes index 0 — an absent, null, non-integer or negative
// index is unprovable provenance, and any nonzero index (or one that changes
// mid-stream) is a multi-output stream, refused under that token because the
// fix an operator reads off it is a different one.
func TestPartialTextChatChoiceIndexIsProvenance(t *testing.T) {
	// index is a raw JSON fragment so a row can state "absent" as well.
	chunk := func(index, content string) string {
		if index == "" {
			return fmt.Sprintf(`{"choices":[{"delta":{"content":%q},"finish_reason":null}]}`, content)
		}
		return fmt.Sprintf(`{"choices":[{"index":%s,"delta":{"content":%q},"finish_reason":null}]}`, index, content)
	}

	cases := []struct {
		name     string
		payloads []string
		want     string
		prefix   string
		recover  bool
	}{
		{
			name:     "index present and zero",
			payloads: []string{chunk("0", "hello"), chunk("0", " world")},
			prefix:   "hello world",
			recover:  true,
		},
		{
			// No index to read is no provenance to prove: the accumulator
			// must not fall back to 0, because a provider that omits the
			// member could be streaming a choice this proxy would then
			// mislabel.
			name:     "index absent",
			payloads: []string{chunk("", "hello")},
			want:     reasonUnknownShape,
		},
		{name: "index null", payloads: []string{chunk("null", "hello")}, want: reasonUnknownShape},
		{name: "index is a string", payloads: []string{chunk(`"0"`, "hello")}, want: reasonUnknownShape},
		{name: "index is a fraction", payloads: []string{chunk("0.5", "hello")}, want: reasonUnknownShape},
		{name: "index negative", payloads: []string{chunk("-1", "hello")}, want: reasonUnknownShape},
		{
			// A single choice that says it is not the first is still a
			// choice this prefix was not read from.
			name:     "index nonzero on the first chunk",
			payloads: []string{chunk("1", "hello")},
			want:     reasonMultipleOutputs,
		},
		{
			name:     "index changes mid-stream",
			payloads: []string{chunk("0", "hello"), chunk("1", " and more")},
			want:     reasonMultipleOutputs,
		},
		{
			name:     "index disappears after binding",
			payloads: []string{chunk("0", "hello"), chunk("", " and more")},
			want:     reasonUnknownShape,
		},
		{
			// The provenance requirement is not skipped on a chunk that
			// carries no delta: a structural chunk states a choice too.
			name:     "no index on a finish_reason chunk",
			payloads: []string{chunk("0", "hello"), `{"choices":[{"delta":{},"finish_reason":"stop"}]}`},
			want:     reasonUnknownShape,
		},
		{
			// A chunk with no choices carries no assistant output and so
			// states no choice: the usage-only final chunk is still admitted.
			name:     "a chunk with no choices needs no index",
			payloads: []string{chunk("0", "hello"), `{"choices":[],"usage":{"completion_tokens":3}}`},
			prefix:   "hello",
			recover:  true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			v := feed(apiChat, 1<<20, tc.payloads...)
			if tc.recover {
				if got := recovered(t, v); got != tc.prefix {
					t.Fatalf("prefix = %q, want %q", got, tc.prefix)
				}
				return
			}
			if v.Kind != verdictUnsafe || v.Reason != tc.want {
				t.Fatalf("verdict = %v/%q, want unsafe/%q", v.Kind, v.Reason, tc.want)
			}
			// A refused choice never contributes its bytes: the text of the
			// chunk whose provenance failed must not have been accumulated
			// even momentarily.
			if v.Text != "" {
				t.Fatalf("a refused stream still offered a prefix: %q", v.Text)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// The Responses event topology, audited event by event
// ---------------------------------------------------------------------------

// TestPartialTextResponsesBoundEvents audits the events that NAME the text
// content part the prefix was read from while carrying no assistant text of
// their own: content_part.added, content_part.done and
// output_text.annotation.added.
//
// They are admitted — refused would be wrong, because a Responses stream is
// full of them and none of them can carry text — but only under the deltas'
// own identity contract, which is what proves they cannot have come from
// another stream. The refusal rows are the load-bearing half: the moment the
// identity leaves the one stream, or the announced part turns out to be the
// refusal channel, the event is refused.
func TestPartialTextResponsesBoundEvents(t *testing.T) {
	const prefix = `{"type":"response.output_text.delta","item_id":"m1","output_index":0,"content_index":0,"delta":"Once"}`

	cases := []struct {
		name    string
		payload string
		want    string
		recover bool
	}{
		{
			name:    "annotation on the prefix's own stream",
			payload: `{"type":"response.output_text.annotation.added","item_id":"m1","output_index":0,"content_index":0,"annotation_index":0,"annotation":{"type":"url_citation","url":"https://example.test"}}`,
			recover: true,
		},
		{
			name:    "annotation on another item",
			payload: `{"type":"response.output_text.annotation.added","item_id":"m2","output_index":0,"content_index":0,"annotation":{"type":"url_citation"}}`,
			want:    reasonMultipleOutputs,
		},
		{
			name:    "annotation with no identity",
			payload: `{"type":"response.output_text.annotation.added","annotation":{"type":"url_citation"}}`,
			want:    reasonUnknownShape,
		},
		{
			name:    "content part added for the prefix's own stream",
			payload: `{"type":"response.content_part.added","item_id":"m1","output_index":0,"content_index":0,"part":{"type":"output_text","annotations":[]}}`,
			recover: true,
		},
		{
			// A closing part that states the text the deltas already
			// accumulated agrees with the pass, so it changes nothing. This
			// is the case the handler used to ignore for the same reason and
			// still does — read now, not skipped.
			name:    "content part done agreeing with the pass",
			payload: `{"type":"response.content_part.done","item_id":"m1","output_index":0,"content_index":0,"part":{"type":"output_text","text":"Once"}}`,
			recover: true,
		},
		{
			// The gap issue #101 named: a closing part whose text is NOT
			// what the deltas accumulated. The client received text the
			// prefix does not hold, and continuing from the shorter prefix
			// would splice an answer around the hole. Refused with the same
			// token the sibling output_text.done path uses, so one failure
			// has one spelling.
			name:    "content part done disagreeing with the pass",
			payload: `{"type":"response.content_part.done","item_id":"m1","output_index":0,"content_index":0,"part":{"type":"output_text","text":"Once upon a time, and then some more that never arrived"}}`,
			want:    reasonUnknownShape,
		},
		{
			// A closing part that states no text at all is the ordinary
			// shape: its content was carried by deltas the prefix already
			// holds, so there is nothing to compare and nothing lost.
			name:    "content part done with no text member",
			payload: `{"type":"response.content_part.done","item_id":"m1","output_index":0,"content_index":0,"part":{"type":"output_text","annotations":[]}}`,
			recover: true,
		},
		{
			name:    "content part done with a null text",
			payload: `{"type":"response.content_part.done","item_id":"m1","output_index":0,"content_index":0,"part":{"type":"output_text","text":null}}`,
			recover: true,
		},
		{
			// An empty part agrees with any accumulation, and with none.
			name:    "content part done with empty text",
			payload: `{"type":"response.content_part.done","item_id":"m1","output_index":0,"content_index":0,"part":{"type":"output_text","text":""}}`,
			recover: true,
		},
		{
			// A text this build cannot read as a string is a shape it must
			// not wave through: the bytes are there and the meaning is not.
			name:    "content part done with a non-string text",
			payload: `{"type":"response.content_part.done","item_id":"m1","output_index":0,"content_index":0,"part":{"type":"output_text","text":42}}`,
			want:    reasonUnknownShape,
		},
		{
			// `.added` opens a part and states no content of its own, so a
			// text on it is not compared — the event that may state the
			// part's text in full is `.done`, and it is the one held to it.
			// This is the asymmetry the two names have always had, now
			// stated on the test that pins it rather than on a comment.
			name:    "content part added carrying text is not compared",
			payload: `{"type":"response.content_part.added","item_id":"m1","output_index":0,"content_index":0,"part":{"type":"output_text","text":"something the deltas never said"}}`,
			recover: true,
		},
		{
			// The refusal channel announced structurally, with no refusal
			// delta to latch on: refused rather than ignored, so a decline
			// that only ever appears as a part type is still a decline.
			name:    "content part is a refusal",
			payload: `{"type":"response.content_part.added","item_id":"m1","output_index":0,"content_index":0,"part":{"type":"refusal","refusal":""}}`,
			want:    reasonRefusal,
		},
		{
			name:    "content part is a channel this build cannot continue",
			payload: `{"type":"response.content_part.added","item_id":"m1","output_index":0,"content_index":0,"part":{"type":"image"}}`,
			want:    reasonUnknownShape,
		},
		{
			name:    "content part is not an object",
			payload: `{"type":"response.content_part.added","item_id":"m1","output_index":0,"content_index":0,"part":"output_text"}`,
			want:    reasonUnknownShape,
		},
		{
			// A part object that is absent states the identity without
			// stating the channel. There is nothing in that which could have
			// carried text, so the identity is the whole contract.
			name:    "content part with no part member",
			payload: `{"type":"response.content_part.added","item_id":"m1","output_index":0,"content_index":0}`,
			recover: true,
		},
		{
			name:    "content part for a second content index",
			payload: `{"type":"response.content_part.added","item_id":"m1","output_index":0,"content_index":1,"part":{"type":"output_text"}}`,
			want:    reasonMultipleOutputs,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			v := feed(apiResponses, 1<<20, prefix, tc.payload)
			if tc.recover {
				if got := recovered(t, v); got != "Once" {
					t.Fatalf("prefix = %q, want %q", got, "Once")
				}
				return
			}
			if v.Kind != verdictUnsafe || v.Reason != tc.want {
				t.Fatalf("verdict = %v/%q, want unsafe/%q", v.Kind, v.Reason, tc.want)
			}
		})
	}
}

// TestPartialTextContentPartDoneIsPerUpstreamResponse pins the scope of the
// closing-part comparison, which is the same scope observeResponsesTextDone's
// is and for the same reason: a .done states the text of the response that
// emitted it, so a continuation hop's closing part must be compared with that
// hop's own deltas and never with the whole cross-hop prefix. Testing the
// wrong region refuses a correct hop, which is the false "unsafe" the gate is
// supposed to be allowed to pay only when the answer really is wrong.
func TestPartialTextContentPartDoneIsPerUpstreamResponse(t *testing.T) {
	const (
		deltaM1 = `{"type":"response.output_text.delta","item_id":"m1","output_index":0,"content_index":0,"delta":"Once"}`
		deltaM2 = `{"type":"response.output_text.delta","item_id":"m2","output_index":0,"content_index":0,"delta":" upon a time"}`
	)

	t.Run("a hop closing its own part recovers", func(t *testing.T) {
		// The hop's cross-hop prefix is "Once upon a time" while the part it
		// closes states " upon a time" — the pass. Comparing against the
		// whole prefix would refuse this correct hop.
		v := feedPasses(apiResponses, 1<<20,
			[]string{deltaM1},
			[]string{deltaM2, `{"type":"response.content_part.done","item_id":"m2","output_index":0,"content_index":0,"part":{"type":"output_text","text":" upon a time"}}`},
		)
		if got := recovered(t, v); got != "Once upon a time" {
			t.Fatalf("prefix = %q, want %q", got, "Once upon a time")
		}
	})

	t.Run("a hop closing a part that overstates its own deltas refuses", func(t *testing.T) {
		v := feedPasses(apiResponses, 1<<20,
			[]string{deltaM1},
			[]string{deltaM2, `{"type":"response.content_part.done","item_id":"m2","output_index":0,"content_index":0,"part":{"type":"output_text","text":" upon a time, and further"}}`},
		)
		if v.Kind != verdictUnsafe || v.Reason != reasonUnknownShape {
			t.Fatalf("verdict = %v/%q, want unsafe/%q", v.Kind, v.Reason, reasonUnknownShape)
		}
	})

	t.Run("a hop closing a part it never streamed refuses", func(t *testing.T) {
		// A part closed with text where this pass streamed nothing is a cut
		// generation. The text is not adopted — that is output_text.done's
		// call, and the branch that would make it would be a second, weaker
		// path to putting unverified bytes into a continuation body. Here the
		// empty pass agrees with nothing, so the comparison refuses.
		v := feedPasses(apiResponses, 1<<20,
			[]string{deltaM1},
			[]string{`{"type":"response.content_part.done","item_id":"m2","output_index":0,"content_index":0,"part":{"type":"output_text","text":" text no delta stated"}}`},
		)
		if v.Kind != verdictUnsafe || v.Reason != reasonUnknownShape {
			t.Fatalf("verdict = %v/%q, want unsafe/%q", v.Kind, v.Reason, reasonUnknownShape)
		}
	})

	t.Run("a closing part does not latch the terminal", func(t *testing.T) {
		// A content_part.done closes one part of one output; the enclosing
		// output_item.done and the response envelope still follow, so the
		// generation is not over and the stream stays continuable. Only
		// response.output_text.done — the event that states the output is
		// complete — latches terminal.
		v := feedPasses(apiResponses, 1<<20,
			[]string{deltaM1, `{"type":"response.content_part.done","item_id":"m1","output_index":0,"content_index":0,"part":{"type":"output_text","text":"Once"}}`},
			[]string{deltaM2},
		)
		if got := recovered(t, v); got != "Once upon a time" {
			t.Fatalf("prefix = %q, want %q", got, "Once upon a time")
		}
	})
}

// TestPartialTextResponsesItemCompletion: the closing event of an output item
// is classified exactly as the announcing one, minus the "second item" latch —
// it is the SAME item's end, so latching there would refuse every ordinary
// stream at its own item's completion.
func TestPartialTextResponsesItemCompletion(t *testing.T) {
	const prefix = `{"type":"response.output_text.delta","item_id":"m1","output_index":0,"content_index":0,"delta":"Once"}`

	cases := []struct {
		name    string
		payload string
		want    string
		recover bool
	}{
		{
			name:    "the item the deltas belong to",
			payload: `{"type":"response.output_item.done","output_index":0,"item":{"type":"message","id":"m1","content":[{"type":"output_text","text":"Once"}]}}`,
			recover: true,
		},
		{
			name:    "a reasoning item's completion",
			payload: `{"type":"response.output_item.done","output_index":0,"item":{"type":"reasoning","id":"r1"}}`,
			recover: true,
		},
		{
			name:    "another item's completion",
			payload: `{"type":"response.output_item.done","output_index":0,"item":{"type":"message","id":"m2"}}`,
			want:    reasonMultipleOutputs,
		},
		{
			name:    "a message item with no id",
			payload: `{"type":"response.output_item.done","output_index":0,"item":{"type":"message"}}`,
			want:    reasonUnknownShape,
		},
		{
			name:    "a call item's completion",
			payload: `{"type":"response.output_item.done","output_index":1,"item":{"type":"function_call","id":"fc1"}}`,
			want:    reasonToolCalls,
		},
		{
			name:    "an executable call item's completion",
			payload: `{"type":"response.output_item.done","output_index":1,"item":{"type":"code_interpreter_call","id":"ci1"}}`,
			want:    reasonToolCalls,
		},
		{
			name:    "no item at all",
			payload: `{"type":"response.output_item.done","output_index":0}`,
			want:    reasonUnknownShape,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			v := feed(apiResponses, 1<<20, prefix, tc.payload)
			if tc.recover {
				if got := recovered(t, v); got != "Once" {
					t.Fatalf("prefix = %q, want %q", got, "Once")
				}
				return
			}
			if v.Kind != verdictUnsafe || v.Reason != tc.want {
				t.Fatalf("verdict = %v/%q, want unsafe/%q", v.Kind, v.Reason, tc.want)
			}
		})
	}
}

// TestPartialTextResponsesUnknownCallSurfacesFailClosed is the audit's
// negative half, and the one that must never soften. Every call surface other
// than the two argument families named in the switch — a search, an
// interpreter, an image generator, a surface that does not exist yet — reaches
// the accumulator's default arm and is REFUSED. The parser is not an "ignore
// what you do not recognize" pass-through: an event this build cannot classify
// is assumed to be carrying text or opening a call until proven otherwise,
// because a continuation built around an omitted call is a broken call.
func TestPartialTextResponsesUnknownCallSurfacesFailClosed(t *testing.T) {
	const prefix = `{"type":"response.output_text.delta","item_id":"m1","output_index":0,"content_index":0,"delta":"Once"}`

	for _, typ := range []string{
		"response.web_search_call.searching",
		"response.file_search_call.completed",
		"response.code_interpreter_call.in_progress",
		"response.code_interpreter_call_code.delta",
		"response.image_generation_call.generating",
		"response.mcp_call.arguments.delta",
		"response.mcp_list_tools.completed",
		"response.local_shell_call.in_progress",
		"response.audio.delta",
		"response.some_future_thing",
	} {
		t.Run(typ, func(t *testing.T) {
			v := feed(apiResponses, 1<<20, prefix, `{"type":"`+typ+`","item_id":"x","output_index":1,"delta":"y"}`)
			if v.Kind != verdictUnsafe {
				t.Fatalf("verdict = %v/%q, want an unsafe refusal — an unclassified event must fail closed", v.Kind, v.Reason)
			}
			if v.Text != "" {
				t.Fatalf("a refused stream still offered a prefix: %q", v.Text)
			}
		})
	}

	// The two argument families whose deltas are enumerated are refused under
	// the call token itself, in both their delta and their done spelling.
	for _, typ := range []string{
		"response.function_call_arguments.delta",
		"response.function_call_arguments.done",
		"response.custom_tool_call_input.delta",
		"response.custom_tool_call_input.done",
	} {
		t.Run(typ, func(t *testing.T) {
			v := feed(apiResponses, 1<<20, prefix, `{"type":"`+typ+`","item_id":"fc1","output_index":1,"delta":"{\"pa"}`)
			if v.Kind != verdictUnsafe || v.Reason != reasonToolCalls {
				t.Fatalf("verdict = %v/%q, want unsafe/%q", v.Kind, v.Reason, reasonToolCalls)
			}
		})
	}
}

// TestPartialTextHopItemIdentityRegression is the m1/m2 regression suite for
// the hop-scoped identity PR #87 established: a continuation hop is a new
// upstream response with its own output item, and its honest identity differs
// from the committed reply's by construction. Judging the hop against the
// committed item would refuse every real second hop as multiple_outputs, after
// that hop had already been dialed and paid for.
//
// It is written as explicit item ids rather than through the shared helpers so
// the regression is readable at a glance: m1 in the committed pass, m2 in the
// hop, and the SAME pass still refusing two items.
func TestPartialTextHopItemIdentityRegression(t *testing.T) {
	delta := func(item string, index int, text string) string {
		return fmt.Sprintf(`{"type":"response.output_text.delta","item_id":%q,"output_index":%d,"content_index":0,"delta":%q}`, item, index, text)
	}

	t.Run("m1 committed, m2 in the hop", func(t *testing.T) {
		v := feedPasses(apiResponses, 1<<20,
			[]string{
				`{"type":"response.output_item.added","output_index":0,"item":{"type":"message","id":"m1"}}`,
				delta("m1", 0, "Once upon a "),
			},
			[]string{
				`{"type":"response.output_item.added","output_index":0,"item":{"type":"message","id":"m2"}}`,
				delta("m2", 0, "time"),
			},
		)
		if got := recovered(t, v); got != "Once upon a time" {
			t.Fatalf("prefix = %q, want %q", got, "Once upon a time")
		}
	})

	t.Run("m2 and m1 inside one pass still refuse", func(t *testing.T) {
		v := feed(apiResponses, 1<<20,
			`{"type":"response.output_item.added","output_index":0,"item":{"type":"message","id":"m1"}}`,
			delta("m1", 0, "Once upon a "),
			delta("m2", 0, "time"),
		)
		if v.Kind != verdictUnsafe || v.Reason != reasonMultipleOutputs {
			t.Fatalf("verdict = %v/%q, want unsafe/%q", v.Kind, v.Reason, reasonMultipleOutputs)
		}
	})

	t.Run("each hop's .done is compared with its own deltas", func(t *testing.T) {
		v := feedPasses(apiResponses, 1<<20,
			[]string{
				`{"type":"response.output_item.added","output_index":0,"item":{"type":"message","id":"m1"}}`,
				delta("m1", 0, "Once upon a "),
			},
			[]string{
				`{"type":"response.output_item.added","output_index":0,"item":{"type":"message","id":"m2"}}`,
				delta("m2", 0, "time"),
				`{"type":"response.output_text.done","item_id":"m2","output_index":0,"content_index":0,"text":"time"}`,
			},
		)
		// The hop's own .done agrees with the hop's own deltas, so it is the
		// logical terminal — NOT a refusal for disagreeing with the
		// cross-hop prefix it never stated.
		if v.Kind != verdictTerminal {
			t.Fatalf("verdict = %v/%q, want terminal", v.Kind, v.Reason)
		}
	})

	t.Run("a hop's .done disagreeing with its own deltas refuses", func(t *testing.T) {
		v := feedPasses(apiResponses, 1<<20,
			[]string{
				`{"type":"response.output_item.added","output_index":0,"item":{"type":"message","id":"m1"}}`,
				delta("m1", 0, "Once upon a "),
			},
			[]string{
				delta("m2", 0, "time"),
				`{"type":"response.output_text.done","item_id":"m2","output_index":0,"content_index":0,"text":"Once upon a time"}`,
			},
		)
		if v.Kind != verdictUnsafe || v.Reason != reasonUnknownShape {
			t.Fatalf("verdict = %v/%q, want unsafe/%q", v.Kind, v.Reason, reasonUnknownShape)
		}
	})
}

// TestPartialTextFrameNameMustAgreeWithType is the Responses half of the
// frame's two names. A Responses frame states its class twice — the SSE
// `event:` line and the payload's own `type` — and the safety gate may not
// classify on one half while the other says something else. Every case here
// hands the accumulator a frame that a real relay could deliver and that the
// gate must refuse.
func TestPartialTextFrameNameMustAgreeWithType(t *testing.T) {
	const (
		deltaM1 = `{"type":"response.output_text.delta","item_id":"m1","output_index":0,"content_index":0,"delta":"Once upon a "}`
		deltaM2 = `{"type":"response.output_text.delta","item_id":"m1","output_index":0,"content_index":0,"delta":"time"}`
	)

	// named drives the accumulator directly so the frame name and the
	// payload's own `type` can be set independently, which is the whole
	// subject here: no real upstream produces the pairs below, and a helper
	// that derived the name from `type` could not express them.
	named := func(name string, payloads ...string) continuationVerdict {
		p := newPartialText(apiResponses, 1<<20)
		var frame []byte
		if name != "" {
			frame = []byte(name)
		}
		for _, s := range payloads {
			p.Observe(frame, []byte(s))
		}
		return p.Verdict()
	}

	t.Run("agreeing halves recover", func(t *testing.T) {
		// The control: the check must not refuse a well-formed stream, or the
		// fix would be a silent off switch rather than a gate.
		if got := recovered(t, named("response.output_text.delta", deltaM1, deltaM2)); got != "Once upon a time" {
			t.Fatalf("prefix = %q, want %q", got, "Once upon a time")
		}
	})

	t.Run("a frame with no event line refuses", func(t *testing.T) {
		// A Responses stream that stopped naming its frames names none of
		// them here, and the payload's `type` alone is not proof of a shape
		// this build can vouch for. An absent name is LESS evidence than a
		// present one, so it is refused rather than treated as agreement.
		if v := named("", deltaM1, deltaM2); v.Kind != verdictUnsafe || v.Reason != reasonUnknownShape {
			t.Fatalf("verdict = %v/%q, want unsafe/%q", v.Kind, v.Reason, reasonUnknownShape)
		}
	})

	t.Run("a name that disagrees with the type refuses", func(t *testing.T) {
		// The exact shape this issue is about: the payload classifies as a
		// text delta, so the accumulator would admit its text into a
		// continuation body, while the frame says the event is something the
		// gate never learned to handle.
		if v := named("response.output_text.done", deltaM1, deltaM2); v.Kind != verdictUnsafe || v.Reason != reasonUnknownShape {
			t.Fatalf("verdict = %v/%q, want unsafe/%q", v.Kind, v.Reason, reasonUnknownShape)
		}
	})

	t.Run("an unreadable event name refuses", func(t *testing.T) {
		// A name that is not text at all — the relay hands the raw event
		// value over, and a gateway writing `event: {"a":1}` produces this.
		// It cannot equal any `type`, so it refuses under the same token.
		p := newPartialText(apiResponses, 1<<20)
		p.Observe([]byte(`{"a":1}`), []byte(deltaM1))
		if v := p.Verdict(); v.Kind != verdictUnsafe || v.Reason != reasonUnknownShape {
			t.Fatalf("verdict = %v/%q, want unsafe/%q", v.Kind, v.Reason, reasonUnknownShape)
		}
	})

	t.Run("the refusal is permanent and latches before terminal", func(t *testing.T) {
		// A refusal that a later well-formed frame could clear would make the
		// gate's verdict depend on what follows the cut, which is exactly
		// what a caller cannot see when it decides whether to continue.
		p := newPartialText(apiResponses, 1<<20)
		p.Observe([]byte("response.output_text.done"), []byte(deltaM1))
		p.Observe([]byte("response.output_text.delta"), []byte(deltaM2))
		if v := p.Verdict(); v.Kind != verdictUnsafe || v.Reason != reasonUnknownShape {
			t.Fatalf("verdict = %v/%q, want unsafe/%q", v.Kind, v.Reason, reasonUnknownShape)
		}
	})

	t.Run("a disagreement before any text refuses with no prefix", func(t *testing.T) {
		// The refusal must not come with a continuation body: the bytes
		// accumulated before the bad frame are not a proven prefix once one
		// frame of the stream has lied about its own shape.
		if v := named("response.output_text.done", deltaM1); v.Text != "" {
			t.Fatalf("refused verdict carried prefix %q", v.Text)
		}
	})
}

// TestPartialTextChatIsExemptFromFrameName pins the scope of the check. Chat
// carries no `event:` line at all, so a nil name is Chat's NORMAL wire shape
// and not a missing one. Applying the agreement check there would refuse
// every Chat stream, which is why the obligation is Responses-only.
func TestPartialTextChatIsExemptFromFrameName(t *testing.T) {
	p := newPartialText(apiChat, 1<<20)
	p.Observe(nil, []byte(`{"choices":[{"index":0,"delta":{"content":"Hello"},"finish_reason":null}]}`))
	p.Observe(nil, []byte(`{"choices":[{"index":0,"delta":{"content":", world"},"finish_reason":null}]}`))
	if got := recovered(t, p.Verdict()); got != "Hello, world" {
		t.Fatalf("prefix = %q, want %q", got, "Hello, world")
	}
}
