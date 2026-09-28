package proxy

import (
	"bytes"
	"encoding/json"
)

// The two API surfaces, spelled exactly as serve binds them to the request
// logger and to usage.NewCapture. A continuation is API-scoped the same way
// the response rewrites are: the two surfaces stream different envelopes, and
// one accumulator cannot read both.
const (
	apiChat      = "chat"
	apiResponses = "responses"
)

// The closed set of reasons a committed stream cannot be continued, plus the
// end-of-stream reason no payload can describe. They are tokens, never error
// text — the same rule the transport's attempt failures follow — because they
// are logged verbatim and an error string can carry upstream bytes.
//
// Six of them describe what the accumulator SAW; partialText.Safe produces
// reasonNoPrefix for the one case where it saw nothing at all.
const (
	// reasonToolCalls — the stream assembled a tool call. Arguments are
	// accumulated by the CLIENT across deltas, so a continuation that
	// re-asks the model cannot reproduce the exact byte sequence those
	// deltas are opening, and a second call would execute twice.
	reasonToolCalls = "tool_calls"
	// reasonFinishReason — the model declared the answer finished. Recovering
	// a stream whose answer is already complete would ask for more of a
	// finished thing, which is exactly the duplication this feature must
	// never produce.
	reasonFinishReason = "finish_reason"
	// reasonUpstreamTerminal — the upstream declared the stream over in a way
	// the relay's terminal predicate does not recognize: a chat error event,
	// or a Responses response.failed/response.incomplete/response.error. Not
	// recoverable, and — the subtle half — not ignorable either: ignoring one
	// leaves a stream the upstream already finished looking identical to a
	// stream that died mid-generation.
	reasonUpstreamTerminal = "upstream_terminal"
	// reasonNotObject — a data line that is neither chat's [DONE] nor a JSON
	// object. Nothing can be read off it.
	reasonNotObject = "not_object"
	// reasonUnknownShape — parseable JSON whose shape is not one of the
	// recognized text-bearing ones.
	reasonUnknownShape = "unknown_shape"
	// reasonOversize — the committed prefix reached the configured
	// max-partial-bytes. The bound is the operator's, and it exists so a
	// recovering stream cannot hold an unbounded copy of an answer.
	reasonOversize = "oversize"
	// reasonNoPrefix — the stream ended with no committed text at all.
	// Nothing to continue FROM; the hop would be the original request
	// restarted, which is precisely the blind retry this feature refuses.
	reasonNoPrefix = "no_prefix"
)

// partialText accumulates the assistant text a client has already seen from a
// committed SSE stream, and decides — separately from accumulating it —
// whether continuing that stream is safe at all.
//
// It is a fail-closed gate, not a best-effort parser: any shape it does not
// positively recognize as assistant text becomes an unsafe reason and stops
// accumulation for the rest of the stream. The direction is forced by what a
// wrong answer costs. A false "safe" hands the upstream a prefix missing
// whatever the accumulator could not read, and the model then writes a
// continuation around a hole — output the client can see becomes duplicated,
// self-contradictory, or a broken tool call, which is the one outcome the
// feature must never produce. A false "unsafe" costs one truncated stream,
// which is exactly what every deployment sees today.
//
// Two consequences of that rule read as omissions and are not:
//
//   - A stream that carried a tool call is never continued, at any point.
//     Tool validity is a property of the whole call, not of the text so far.
//   - Reasoning-channel and structural lifecycle events are ignored rather
//     than refused — they carry no assistant text, and a Responses stream is
//     mostly made of them.
//
// One partialText observes one logical stream. It is not safe for concurrent
// use: CopySSE calls Observe inline on the relay's own goroutine, and Safe is
// read after that goroutine has stopped.
type partialText struct {
	// api is the request's own surface: apiChat or apiResponses.
	api string
	// limit is the configured max-partial-bytes. Accumulation stops — and
	// the stream becomes unsafe — at or past it.
	limit int
	// text is the committed assistant text so far. Every byte kept here was
	// copied out of the relay's read buffer: nothing aliases upstream memory.
	text []byte
	// unsafe is the latched refusal reason, "" while the prefix is still
	// usable. First refusal wins: a later shape cannot un-refuse a stream.
	unsafe string
}

// newPartialText builds the accumulator for one streamed response. limit is
// the frozen policy's max-partial-bytes; it is read only for the oversize
// bound, so a zero limit refuses at the first appended byte.
func newPartialText(api string, limit int) *partialText {
	return &partialText{api: api, limit: limit}
}

// Observe records one committed data-line payload. It is called once per
// admitted data line — every `data:` line the relay forwarded, before the
// client-facing rewrite — on the relay's goroutine, in wire order. It never
// blocks and never writes.
//
// The payload is the relay's own buffer and is never retained.
func (p *partialText) Observe(payload []byte) {
	if p.unsafe != "" {
		return
	}
	// Chat's terminal payload is not JSON, and it is not text either: it is
	// the marker that ends the stream, so it carries nothing to accumulate.
	if p.api == apiChat && bytes.Equal(payload, []byte("[DONE]")) {
		return
	}
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(payload, &obj); err != nil {
		// Not an object — including valid JSON that is a string, number or
		// array. Both APIs send objects on their text paths.
		p.refuse(reasonNotObject)
		return
	}
	// Both surfaces report a mid-stream failure as a top-level error member
	// (an SSE `error` event). Checked before the surface split because the
	// shape is shared.
	if raw, ok := obj["error"]; ok && !jsonNull(raw) {
		p.refuse(reasonUpstreamTerminal)
		return
	}
	if p.api == apiChat {
		p.observeChatChunk(obj)
		return
	}
	p.observeResponsesEvent(obj)
}

// Safe reports whether the committed prefix can be continued. A non-empty
// reason means it cannot; the token is one of the closed set above and is
// safe to log. The prefix is a snapshot — a copy the caller owns — and is
// empty whenever a reason is returned.
func (p *partialText) Safe() (prefix, reason string) {
	if p.unsafe != "" {
		return "", p.unsafe
	}
	if len(p.text) == 0 {
		return "", reasonNoPrefix
	}
	return string(p.text), ""
}

// partialBytes reports how much text has been committed so far. It is access
// log metadata only — the text itself never reaches a log line.
func (p *partialText) partialBytes() int { return len(p.text) }

// refuse latches the first unsafe reason and releases the accumulated text:
// nothing reads a prefix once the stream is unsafe, and a stream that will
// not be continued must not pin its bytes for the rest of the request.
func (p *partialText) refuse(reason string) {
	if p.unsafe == "" {
		p.unsafe = reason
	}
	p.text = nil
}

// append extends the committed prefix and applies the size bound.
func (p *partialText) append(s string) {
	if s == "" {
		return
	}
	p.text = append(p.text, s...)
	if len(p.text) >= p.limit {
		p.refuse(reasonOversize)
	}
}

// observeChatChunk reads one Chat Completions chunk. The text lives at
// choices[i].delta.content; the two refusals live beside it.
func (p *partialText) observeChatChunk(obj map[string]json.RawMessage) {
	raw, ok := obj["choices"]
	if !ok || jsonNull(raw) {
		// A chunk with no choices carries no assistant output. The shape
		// this admits is the usage-only final chunk and the stream's
		// opening metadata chunk.
		return
	}
	var choices []map[string]json.RawMessage
	if err := json.Unmarshal(raw, &choices); err != nil {
		p.refuse(reasonUnknownShape)
		return
	}
	// n>1 streams several independent answers over one connection. Their
	// deltas interleave on the wire, so accumulating them into one prefix
	// would splice answers together. Refused rather than guessed at: a
	// continuation of a multi-choice stream is not something this proxy can
	// reason about, and the request is one the client can simply repeat.
	if len(choices) > 1 {
		p.refuse(reasonUnknownShape)
		return
	}
	for _, choice := range choices {
		if raw, ok := choice["finish_reason"]; ok && !jsonNull(raw) {
			p.refuse(reasonFinishReason)
			return
		}
		raw, ok := choice["delta"]
		if !ok || jsonNull(raw) {
			continue
		}
		var delta map[string]json.RawMessage
		if err := json.Unmarshal(raw, &delta); err != nil {
			p.refuse(reasonUnknownShape)
			return
		}
		// The legacy function_call spelling is included deliberately: it is
		// the same event class as tool_calls, and an upstream using it would
		// otherwise slip past the gate.
		if raw, ok := delta["tool_calls"]; ok && !jsonNull(raw) {
			p.refuse(reasonToolCalls)
			return
		}
		if raw, ok := delta["function_call"]; ok && !jsonNull(raw) {
			p.refuse(reasonToolCalls)
			return
		}
		content, ok := delta["content"]
		if !ok || jsonNull(content) {
			// The role-only opening delta and the closing chunk both land
			// here, as does a reasoning-bearing delta on providers that
			// stream one beside content.
			continue
		}
		var s string
		if err := json.Unmarshal(content, &s); err != nil {
			// A structured (multi-part) content value is not text this
			// accumulator can splice back into a request.
			p.refuse(reasonUnknownShape)
			return
		}
		p.append(s)
		if p.unsafe != "" {
			return
		}
	}
}

// observeResponsesEvent reads one Responses envelope event. The text arrives
// as output_text/refusal deltas; everything else is a refusal, an item
// announcement, or a lifecycle event.
func (p *partialText) observeResponsesEvent(obj map[string]json.RawMessage) {
	raw, ok := obj["type"]
	if !ok {
		p.refuse(reasonUnknownShape)
		return
	}
	var typ string
	if err := json.Unmarshal(raw, &typ); err != nil {
		p.refuse(reasonUnknownShape)
		return
	}
	switch typ {
	case "response.output_text.delta", "response.refusal.delta":
		// A refusal is client-visible assistant text like any other; leaving
		// it out of the prefix would have the model continue as though it had
		// never declined.
		p.appendDelta(obj)
	case "response.output_item.added":
		p.observeResponseItem(obj)
	case "response.function_call_arguments.delta", "response.custom_tool_call_input.delta":
		p.refuse(reasonToolCalls)
	case "response.failed", "response.incomplete", "response.error":
		p.refuse(reasonUpstreamTerminal)
	case "response.created", "response.queued", "response.in_progress",
		"response.output_item.done",
		"response.content_part.added", "response.content_part.done",
		"response.output_text.done", "response.refusal.done",
		"response.reasoning_summary_part.added", "response.reasoning_summary_part.done",
		"response.reasoning_summary_text.delta", "response.reasoning_summary_text.done",
		"response.reasoning_text.delta", "response.reasoning_text.done",
		"response.completed":
		// Structural lifecycle and reasoning-channel events. None carries
		// client-visible assistant text — the reasoning channel is a
		// separate output the client renders beside the message, not part of
		// it — and a Responses stream is largely made of them, so refusing
		// here would refuse every Responses stream.
	default:
		// An event this build does not know. It is assumed to matter until
		// proven otherwise: an unrecognized event is one that could have
		// carried text, or opened a tool call.
		p.refuse(reasonUnknownShape)
	}
}

// observeResponseItem classifies a response.output_item.added event by the
// item it announces. Message and reasoning items are admitted; every other
// item type is a call of some kind — function_call, custom_tool_call,
// computer_call, web_search_call, file_search_call — and is treated as a tool
// call, because a new surface arriving as a new item type must not slip past
// a gate that only knew to look for function_call.
func (p *partialText) observeResponseItem(obj map[string]json.RawMessage) {
	raw, ok := obj["item"]
	if !ok || jsonNull(raw) {
		p.refuse(reasonUnknownShape)
		return
	}
	var item map[string]json.RawMessage
	if err := json.Unmarshal(raw, &item); err != nil {
		p.refuse(reasonUnknownShape)
		return
	}
	traw, ok := item["type"]
	if !ok {
		p.refuse(reasonUnknownShape)
		return
	}
	var typ string
	if err := json.Unmarshal(traw, &typ); err != nil {
		p.refuse(reasonUnknownShape)
		return
	}
	switch typ {
	case "message", "reasoning":
	default:
		p.refuse(reasonToolCalls)
	}
}

// appendDelta reads the string delta of a Responses text-bearing event.
func (p *partialText) appendDelta(obj map[string]json.RawMessage) {
	raw, ok := obj["delta"]
	if !ok || jsonNull(raw) {
		return
	}
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		p.refuse(reasonUnknownShape)
		return
	}
	p.append(s)
}

// jsonNull reports whether a raw member is JSON null (or absent, which the
// callers model as the same thing). The OpenAI APIs treat a null member as
// its zero value, so a null is never a refusal — only a nonzero shape is.
func jsonNull(raw json.RawMessage) bool {
	return len(raw) == 0 || bytes.Equal(bytes.TrimSpace(raw), []byte("null"))
}
