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
// Six of them describe what the accumulator SAW; partialText.Verdict produces
// reasonNoPrefix for the one case where it saw nothing at all.
const (
	// reasonToolCalls — the stream assembled a tool call. Arguments are
	// accumulated by the CLIENT across deltas, so a continuation that
	// re-asks the model cannot reproduce the exact byte sequence those
	// deltas are opening, and a second call would execute twice.
	reasonToolCalls = "tool_calls"
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
	// reasonMultipleOutputs — a Responses stream whose text does not all come
	// from ONE message output's ONE text content stream: a second message
	// item, a second content part, a switched channel, or a delta whose
	// item_id/output_index/content_index disagrees with the ones the
	// accumulated prefix was read from. It is its own token rather than
	// unknown_shape because the fix an operator reads off it is different: a
	// multi-output stream is a shape this proxy intentionally does not
	// support, not a shape it failed to recognize.
	reasonMultipleOutputs = "multiple_outputs"
	// reasonOversize — the committed prefix reached the configured
	// max-partial-bytes. The bound is the operator's, and it exists so a
	// recovering stream cannot hold an unbounded copy of an answer.
	reasonOversize = "oversize"
	// reasonNoPrefix — the stream ended with no committed text at all.
	// Nothing to continue FROM; the hop would be the original request
	// restarted, which is precisely the blind retry this feature refuses.
	reasonNoPrefix = "no_prefix"
)

// verdictKind is the accumulator's answer about one committed stream. It is a
// closed set of three, and the distinction between the last two is load
// bearing rather than cosmetic: both forbid recovery, but only one of them
// means the upstream produced something this proxy cannot reason about.
type verdictKind int

const (
	// verdictRecoverable — plain assistant text this proxy can re-ask with,
	// and nothing seen so far has forbidden that.
	verdictRecoverable verdictKind = iota
	// verdictTerminal — the upstream declared the answer FINISHED (a non-null
	// Chat `finish_reason`). The generation is complete; there is nothing
	// left to continue, and asking for more of a finished answer is exactly
	// the duplication this feature must never produce. It is NOT an
	// "unsafe content" refusal: nothing about the stream was unreadable or
	// forbidden, it simply ended its own generation without the wire marker
	// the client keys on. Reporting the two alike would send an operator
	// after a parsing problem that does not exist.
	verdictTerminal
	// verdictUnsafe — the stream carried something this proxy must not
	// continue: a tool call, a content-bound refusal, an unreadable shape, a
	// multi-output topology, an upstream-declared failure, or more text than
	// the configured bound.
	verdictUnsafe
)

// continuationVerdict is the accumulator's one answer. Exactly one shape is
// produced: Kind == verdictRecoverable has a non-empty Text and no Reason,
// verdictUnsafe has a Reason and no Text, and verdictTerminal has neither.
type continuationVerdict struct {
	Kind   verdictKind
	Text   string
	Reason string
}

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
// One partialText observes one logical stream, across every hop of it. It is
// not safe for concurrent use: CopySSE calls Observe inline on the relay's
// own goroutine, and Verdict is read after that goroutine has stopped.
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
	// terminal is the latched logical end of the generation. Once set, the
	// stream is over as far as this proxy is concerned: nothing more is
	// accumulated and Verdict reports verdictTerminal.
	terminal bool
	// Responses identity. The MVP continuation contract is deliberately
	// narrow: the committed prefix must come from EXACTLY ONE message output
	// carrying EXACTLY ONE text content stream, with that identity stable for
	// the whole prefix. The deltas of two output items interleave on the wire,
	// so concatenating them would splice two answers into one assistant turn
	// and hand the model a conversation that never happened.
	//
	// The identity is kept in two parts because the wire states it in two
	// places that must agree: the ITEM — its id and the index it occupies in
	// the response, announced by response.output_item.added and repeated on
	// every delta — and the STREAM — which content part of that item, and
	// which channel (text or refusal) the deltas arrived on. A conforming
	// upstream sends both and they match; the two are proven against each
	// other rather than trusting whichever arrived first.
	//
	// itemSeen records that a message item has already been announced, which
	// is how a topology with two of them is refused before any of its deltas
	// arrive.
	itemSeen     bool
	itemBound    bool
	itemID       string
	outputIndex  int
	streamBound  bool
	contentIndex int
	streamKind   string
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
	if p.unsafe != "" || p.terminal {
		// A latched verdict is permanent: a later shape cannot un-refuse a
		// stream, and nothing after the generation's declared end belongs to
		// the answer being continued. Neither costs anything to stop reading
		// here, and both keep the accumulated bytes from growing again.
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

// Verdict reports the accumulator's one answer about the stream so far: the
// committed prefix when the stream is recoverable, the closed-set reason when
// it is not, and the logical-terminal fact when the upstream declared the
// answer finished.
//
// The prefix is a snapshot — a copy the caller owns — and is empty whenever
// the stream is not recoverable.
func (p *partialText) Verdict() continuationVerdict {
	switch {
	case p.unsafe != "":
		return continuationVerdict{Kind: verdictUnsafe, Reason: p.unsafe}
	case p.terminal:
		return continuationVerdict{Kind: verdictTerminal}
	case len(p.text) == 0:
		return continuationVerdict{Kind: verdictUnsafe, Reason: reasonNoPrefix}
	}
	return continuationVerdict{Kind: verdictRecoverable, Text: string(p.text)}
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

// finish latches the logical terminal and releases the accumulated text for
// the same reason refuse does: no continuation will read it.
func (p *partialText) finish() {
	p.terminal = true
	p.text = nil
}

// append extends the committed prefix and applies the size bound.
func (p *partialText) append(s string) {
	if s == "" || p.unsafe != "" || p.terminal {
		return
	}
	p.text = append(p.text, s...)
	if len(p.text) >= p.limit {
		p.refuse(reasonOversize)
	}
}

// observeChatChunk reads one Chat Completions chunk. The text lives at
// choices[i].delta.content; the terminals and refusals live beside it.
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
		raw, ok := choice["delta"]
		if !ok || jsonNull(raw) {
			// A choice with no delta is a structural chunk (role-only
			// opening, or a bare finish_reason). The finish_reason check
			// below handles the terminal case; there is no content to
			// accumulate here.
		} else {
			var delta map[string]json.RawMessage
			if err := json.Unmarshal(raw, &delta); err != nil {
				p.refuse(reasonUnknownShape)
				return
			}
			// Tool calls are a hard refusal — the committed prefix must be
			// plain text. The legacy function_call spelling is included
			// deliberately: it is the same event class as tool_calls, and an
			// upstream using it would otherwise slip past the gate.
			if raw, ok := delta["tool_calls"]; ok && !jsonNull(raw) {
				p.refuse(reasonToolCalls)
				return
			}
			if raw, ok := delta["function_call"]; ok && !jsonNull(raw) {
				p.refuse(reasonToolCalls)
				return
			}
			content, ok := delta["content"]
			if ok && !jsonNull(content) {
				var s string
				if err := json.Unmarshal(content, &s); err != nil {
					// A structured (multi-part) content value is not text
					// this accumulator can splice back into a request.
					p.refuse(reasonUnknownShape)
					return
				}
				p.append(s)
				if p.unsafe != "" {
					return
				}
			}
			// If the delta carried nothing but the finish_reason, the
			// accumulator is unchanged — the logical terminal below will
			// latch without discarding anything.
		}
		// The finish_reason check runs AFTER reading the delta. A chunk
		// carrying BOTH content and finish_reason accumulates the content,
		// then latches the logical terminal — it does NOT discard the
		// prefix. Only an explicit refusal (tool_calls, oversize,
		// unknown_shape) clears the text.
		if raw, ok := choice["finish_reason"]; ok && !jsonNull(raw) {
			p.finish()
			return
		}
		if p.terminal {
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
	case "response.output_text.delta":
		// A text delta is only accumulated once its identity has been proven
		// to match the ONE message output and content stream this prefix has
		// been read from. A refusal delta is held to the same identity: it is
		// a different channel of the SAME content stream, and switching
		// channels mid-prefix would splice a decline into an answer.
		p.appendDelta(typ, obj)
	case "response.refusal.delta":
		p.appendDelta(typ, obj)
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
	case "message":
		// The second message output is where the MVP contract ends: a
		// response with two of them interleaves two answers on one wire, so
		// there is no single assistant turn to continue from. Refused here
		// even before any of its deltas arrive, because the refusal is a
		// property of the topology rather than of the text.
		if p.itemSeen {
			p.refuse(reasonMultipleOutputs)
			return
		}
		p.itemSeen = true
		// The announcement carries the item's OWN identity — its id, and the
		// index it occupies in the response — which the deltas repeat. Where
		// the two disagree the prefix has no single provenance; where the
		// announcement is missing one of them there is nothing to prove the
		// deltas against, and the item is refused rather than trusted.
		id, ok := responsesStringMember(item, "id")
		if !ok {
			p.refuse(reasonUnknownShape)
			return
		}
		index, ok := responsesIndexMember(obj, "output_index")
		if !ok {
			p.refuse(reasonUnknownShape)
			return
		}
		p.bindItemIdentity(id, index)
	case "reasoning":
		// Reasoning items carry no client-visible assistant text and are
		// never accumulated, so their identity is not part of the contract.
	default:
		p.refuse(reasonToolCalls)
	}
}

// appendDelta reads one Responses text-bearing event and accumulates it once
// its identity is proven. kind is the event's own type, which is the channel
// the delta arrived on.
func (p *partialText) appendDelta(kind string, obj map[string]json.RawMessage) {
	raw, ok := obj["delta"]
	if !ok || jsonNull(raw) {
		return
	}
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		p.refuse(reasonUnknownShape)
		return
	}
	if !p.bindTextStream(kind, obj) {
		return
	}
	p.append(s)
}

// bindTextStream proves that one text-bearing delta belongs to the single
// message output and single content stream the committed prefix has been read
// from, binding the identity on the first delta and requiring every later one
// to match it exactly. It reports whether the delta may be accumulated.
//
// Fail-closed is the whole design here. The MVP recovers plain text from one
// output; anything else — a second item, a second content part, a switch
// between the text and refusal channels, or an event that does not state the
// three identity members at all — is refused, because the accumulator cannot
// prove the deltas are one stream and splicing two streams into one assistant
// turn produces a conversation that never happened.
func (p *partialText) bindTextStream(kind string, obj map[string]json.RawMessage) bool {
	itemID, ok := responsesStringMember(obj, "item_id")
	if !ok {
		p.refuse(reasonUnknownShape)
		return false
	}
	outputIndex, ok := responsesIndexMember(obj, "output_index")
	if !ok {
		p.refuse(reasonUnknownShape)
		return false
	}
	contentIndex, ok := responsesIndexMember(obj, "content_index")
	if !ok {
		p.refuse(reasonUnknownShape)
		return false
	}
	if !p.bindItemIdentity(itemID, outputIndex) {
		return false
	}
	if !p.streamBound {
		p.streamBound, p.contentIndex, p.streamKind = true, contentIndex, kind
		return true
	}
	if p.contentIndex != contentIndex || p.streamKind != kind {
		p.refuse(reasonMultipleOutputs)
		return false
	}
	return true
}

// bindItemIdentity proves one item-level identity — the item's own id and the
// index it occupies in the response — against whatever the accumulator already
// knows, binding it when nothing has been bound yet. It reports whether the
// identity holds.
//
// Both sources call it, because a conforming upstream states the identity
// twice: once when it announces the item (response.output_item.added) and
// again on every delta that belongs to it. A stream where the two disagree is
// one whose answer has no single provenance, which is exactly what the MVP
// refuses rather than guesses at.
func (p *partialText) bindItemIdentity(itemID string, outputIndex int) bool {
	if !p.itemBound {
		p.itemBound, p.itemID, p.outputIndex = true, itemID, outputIndex
		return true
	}
	if p.itemID != itemID || p.outputIndex != outputIndex {
		p.refuse(reasonMultipleOutputs)
		return false
	}
	return true
}

// responsesStringMember reads one required string member of an event. An
// absent/null member is not a zero value here as it is elsewhere in this
// package: the identity members are what the contract is MADE of, so a
// missing one is a stream whose provenance cannot be established.
func responsesStringMember(obj map[string]json.RawMessage, key string) (string, bool) {
	raw, ok := obj[key]
	if !ok || jsonNull(raw) {
		return "", false
	}
	var s string
	if err := json.Unmarshal(raw, &s); err != nil || s == "" {
		return "", false
	}
	return s, true
}

// responsesIndexMember reads one required non-negative integer member of an
// event, with the same rule as responsesStringMember: absent is unprovable,
// not zero.
func responsesIndexMember(obj map[string]json.RawMessage, key string) (int, bool) {
	raw, ok := obj[key]
	if !ok || jsonNull(raw) {
		return 0, false
	}
	var n int
	if err := json.Unmarshal(raw, &n); err != nil || n < 0 {
		return 0, false
	}
	return n, true
}

// jsonNull reports whether a raw member is JSON null (or absent, which the
// callers model as the same thing). The OpenAI APIs treat a null member as
// its zero value, so a null is never a refusal — only a nonzero shape is.
func jsonNull(raw json.RawMessage) bool {
	return len(raw) == 0 || bytes.Equal(bytes.TrimSpace(raw), []byte("null"))
}
