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
// Seven of them describe what the accumulator SAW; partialText.Verdict
// produces reasonNoPrefix for the one case where it saw nothing at all.
const (
	// reasonToolCalls — the stream assembled a tool call. Arguments are
	// accumulated by the CLIENT across deltas, so a continuation that
	// re-asks the model cannot reproduce the exact byte sequence those
	// deltas are opening, and a second call would execute twice.
	reasonToolCalls = "tool_calls"
	// reasonRefusal — the stream carried the upstream's refusal CHANNEL: a
	// response.refusal.delta event, or a response.refusal.done whose refusal
	// is non-empty. A refusal is not assistant text (the model declined to
	// answer), so it is never accumulated, and its arrival is its own
	// refusal rather than a generic upstream terminal: the generation was
	// not cut short and the upstream did not fail — the model said no, and
	// no continuation of the message it declined to write exists.
	reasonRefusal = "refusal"
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
	// Chat choice provenance. A Chat stream addresses its answer by
	// choices[i].index — the same identity the Responses surface states as
	// output_index — and the accumulator is held to the same standard for it:
	// the index must be PRESENT on every chunk that carries a choice, and it
	// must be the one the committed prefix has been read from.
	//
	// The value this proxy can continue is the FIRST choice, index 0. It does
	// not assume that: a chunk whose index is absent, null, non-integer or
	// negative proves no provenance and is refused as unknown_shape, and a
	// chunk whose index disagrees with the bound one — or is any nonzero
	// value, which is the same statement about a one-choice prefix — is
	// refused as multiple_outputs, exactly as a second message item on the
	// Responses surface is. So a provider that streams choice 1 alone, or
	// that renumbers its choices mid-stream, is refused rather than spliced.
	//
	// The binding is per upstream response like the Responses identity, but
	// the reset is behaviorally inert: 0 is the only value that ever binds,
	// so a hop's own first chunk binds the same value the committed pass did.
	choiceBound bool
	choiceIndex int
	// passStart is where the CURRENT upstream response's own accumulation
	// begins in text. Everything before it was relayed from an earlier
	// response of the same logical stream, and the identity above describes
	// only the region from passStart on.
	//
	// The two scopes are deliberately different, and that is the whole point
	// of the field. A continuation hop is a new upstream response: it
	// announces its own output item, emits its own deltas and its own
	// response.output_text.done, so its honest identity disagrees with the
	// committed reply's by construction. Judging it against the earlier
	// response's identity refuses every real second hop as multiple_outputs —
	// after that hop has already been dialed and paid for.
	//
	// The prefix and the latches (unsafe, terminal) stay logical-stream-scoped:
	// a later response must never release a refusal an earlier one latched, and
	// the continuation body is built from the whole prefix. Only the identity,
	// and the region a .done event is compared against, are per response.
	// beginUpstreamStream is the only writer.
	passStart int
}

// newPartialText builds the accumulator for one streamed response. limit is
// the frozen policy's max-partial-bytes; it is read only for the oversize
// bound, so a zero limit refuses at the first appended byte.
func newPartialText(api string, limit int) *partialText {
	return &partialText{api: api, limit: limit}
}

// beginUpstreamStream marks the start of a new upstream response being relayed
// into this logical stream: the committed pass, and then every continuation
// hop. The caller invokes it exactly once per upstream body, before that body's
// first line is observed — one relay invocation is one upstream HTTP response.
//
// It resets the Responses identity and only the identity. The identity is a
// property of ONE response: the item a response announces is its own, and every
// real upstream emits a fresh item id for a fresh response, so carrying the
// earlier response's item across the boundary would refuse an honest
// continuation as multiple_outputs. What must NOT be scoped to the response is
// everything the client already has: the accumulated prefix, a latched refusal
// and a latched terminal all live for the logical stream, because a later
// response cannot un-say text the client already read or un-refuse a stream
// that stopped being plain text.
//
// The first pass of an uncontinued stream calls this with an empty text and no
// identity bound, which leaves the accumulator exactly as newPartialText built
// it — the feature-off and single-pass paths are unaffected.
func (p *partialText) beginUpstreamStream() {
	p.passStart = len(p.text)
	p.itemSeen, p.itemBound, p.itemID, p.outputIndex = false, false, "", 0
	p.streamBound, p.contentIndex, p.streamKind = false, 0, ""
	// Chat's choice index is an identity too, and belongs to the response
	// that stated it. The reset is inert in practice — 0 is the only index
	// that ever binds — and is kept so the two surfaces' identities are
	// scoped identically rather than by a per-surface argument.
	p.choiceBound, p.choiceIndex = false, 0
}

// passText returns the region of text this upstream response contributed. It is
// what a per-response check — a .done event's own text — must be compared
// against; the full text belongs to the continuation body, which is a
// logical-stream artifact.
//
// The slice is a window on p.text, not a copy: every caller reads it before the
// next Observe, and Observe is what can grow or release the backing array.
func (p *partialText) passText() []byte { return p.text[p.passStart:] }

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
	if raw, ok := obj["response"]; ok && !jsonNull(raw) {
		var response map[string]json.RawMessage
		if err := json.Unmarshal(raw, &response); err != nil || response == nil {
			p.refuse(reasonUnknownShape)
			return
		}
		if raw, ok := response["error"]; ok && !jsonNull(raw) {
			p.refuse(reasonUpstreamTerminal)
			return
		}
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
	// text is the only thing passStart indexes, and releasing it is the only
	// way the offset could ever point past the end. Reset it here so the
	// invariant holds by construction rather than by the argument that the
	// latched verdict stops Observe before any reader runs.
	p.passStart = 0
}

// finish latches the logical terminal and releases the accumulated text for
// the same reason refuse does: no continuation will read it.
func (p *partialText) finish() {
	p.terminal = true
	p.text = nil
	p.passStart = 0
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
	// would splice answers together. Refused rather than guessed at, and with
	// the token that names the fact: this is a multi-output stream, not an
	// unrecognized shape, and it is refused for the same reason a Responses
	// response carrying two message items is.
	if len(choices) > 1 {
		p.refuse(reasonMultipleOutputs)
		return
	}
	for _, choice := range choices {
		// The delta is read first, so that a tool call is refused under its
		// OWN token: it is the more specific fact about the stream, and a
		// chunk that both calls a tool and mis-states its provenance must not
		// report the weaker of the two.
		var content string
		var hasContent bool
		if raw, ok := choice["delta"]; ok && !jsonNull(raw) {
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
			if raw, ok := delta["content"]; ok && !jsonNull(raw) {
				if err := json.Unmarshal(raw, &content); err != nil {
					// A structured (multi-part) content value is not text
					// this accumulator can splice back into a request.
					p.refuse(reasonUnknownShape)
					return
				}
				hasContent = true
			}
			// If the delta carried nothing but the finish_reason, the
			// accumulator is unchanged — the logical terminal below will
			// latch without discarding anything.
		}
		// Provenance, proven BEFORE any of this chunk's text enters the
		// prefix. A choice that does not state which answer it belongs to (or
		// states one the prefix was not read from) is a stream whose prefix
		// has no single origin, and the text of such a chunk must never be
		// folded into a continuation body even momentarily.
		if !p.bindChoiceIndex(choice) {
			return
		}
		if hasContent {
			p.append(content)
			if p.unsafe != "" {
				return
			}
		}
		// The finish_reason check runs AFTER reading the delta. A chunk
		// carrying BOTH content and finish_reason accumulates the content,
		// then latches the logical terminal — it does NOT discard the
		// prefix. Only an explicit refusal (tool_calls, oversize,
		// unknown_shape, multiple_outputs) clears the text.
		if raw, ok := choice["finish_reason"]; ok && !jsonNull(raw) {
			p.finish()
			return
		}
		if p.terminal {
			return
		}
	}
}

// bindChoiceIndex proves that one Chat choice belongs to the single answer the
// committed prefix has been read from, reading choices[i].index with the same
// rule the Responses identity members follow: a missing, null, non-integer or
// negative index is unprovable provenance, and an index that differs from the
// one already bound — including a nonzero first index, which says the stream
// carries a choice other than the first — is a multi-output stream.
//
// It reports whether the chunk's content may be accumulated. Nothing is
// accumulated when it reports false: the caller returns immediately.
func (p *partialText) bindChoiceIndex(choice map[string]json.RawMessage) bool {
	index, ok := responsesIndexMember(choice, "index")
	if !ok {
		p.refuse(reasonUnknownShape)
		return false
	}
	if !p.choiceBound {
		p.choiceBound, p.choiceIndex = true, index
	}
	if p.choiceIndex != index || p.choiceIndex != 0 {
		p.refuse(reasonMultipleOutputs)
		return false
	}
	return true
}

// observeResponsesEvent reads one Responses envelope event, dispatching it
// into one of the accumulator's event classes. The classification is
// deliberate and exhaustive-by-default: an event belongs to a class only
// because of a property that class states, and anything left over is
// fail-closed.
//
// The classes, and why each event is in the one it is in:
//
//	A. TEXT — the assistant text the client received.
//	   response.output_text.delta / .done. Accumulated, under the identity
//	   contract bindTextStream proves.
//	B. REFUSAL — the model declined. response.refusal.delta / .done. Never
//	   accumulated; a refusal latches reasonRefusal. (See
//	   observeResponsesRefusalDelta and observeResponsesRefusalDone.)
//	C. CALLS — a tool, an interpreter, a search or any other machine-readable
//	   surface. Load-bearing as a whole: their arguments are assembled by the
//	   CLIENT across events, so no prefix of them can be continued. A call
//	   announces itself as response.output_item.added with a non-message item
//	   type, which observeResponseItem refuses before ANY of the call's own
//	   events can arrive; the two argument families whose deltas are listed
//	   here are enumerated so that a stream whose announcement was lost is
//	   still refused under the same token rather than the generic one.
//	D. FAILURE — response.failed / .incomplete / .error: the upstream
//	   declared the generation over without finishing it.
//	E. BOUND — events that name the text content part the prefix was read
//	   from: response.content_part.added / .done and
//	   response.output_text.annotation.added. They are held to the delta
//	   identity contract, and a refusal part is detected rather than ignored.
//	   What makes the IGNORE sound is the shape of the part, not the
//	   identity: the part's `type` is the same claim the deltas make. For
//	   `.added` that is the whole of it, because a part being opened states
//	   no content of its own. For `.done` it is not — the event closes the
//	   part and MAY state its text in full, so a `text` that disagrees with
//	   what this pass accumulated is refused exactly as
//	   observeResponsesTextDone refuses it. See verifyPartText.
//	   (response.output_text.done, by contrast, is class A: its text IS
//	   read, because that event does state the output text in full.)
//	F. METADATA — response.created / .queued / .in_progress / .completed and
//	   the reasoning channel (reasoning_text.*, reasoning_summary_*). The
//	   reasoning channel is a SEPARATE output the client renders beside the
//	   message, not part of it, so its text is not assistant text and is not
//	   accumulated; the envelope's own status events carry no output at all.
//
// Anything else is refused as unknown_shape. That default is the rule, not a
// fallback: this parser must never become an "ignore whatever you do not
// recognize" pass-through, because the events it would ignore are exactly the
// ones a future upstream could use to stream text this proxy would then omit
// from a continuation body.
//
// The event class comes from the DATA PAYLOAD alone, and that is the whole of
// the evidence: the SSE `event:` line that precedes it is not passed to
// `observe` at all, so the class is read from the payload's own `type` member
// and the two are never compared. The relay's terminal predicate makes the
// opposite choice deliberately — it trusts `event: response.completed` and
// explicitly refuses a data payload naming it — because for a terminal marker
// the safe error is to send one more event, while for the safety gate it is
// not. That asymmetry is tracked as issue #100 rather than resolved here:
// refusing on a mismatch is a behaviour change, not a fix, and it belongs to
// the issue.
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
	// A. TEXT
	case "response.output_text.delta":
		// A text delta is only accumulated once its identity has been proven
		// to match the ONE message output and content stream this prefix has
		// been read from.
		p.appendDelta(typ, obj)
	case "response.output_text.done":
		p.observeResponsesTextDone(obj)

	// B. REFUSAL
	case "response.refusal.delta":
		p.observeResponsesRefusalDelta()
	case "response.refusal.done":
		p.observeResponsesRefusalDone(obj)

	// C. CALLS
	case "response.output_item.added":
		p.observeResponseItem(obj)
	case "response.output_item.done":
		p.observeResponseItemDone(obj)
	case "response.function_call_arguments.delta", "response.function_call_arguments.done",
		"response.custom_tool_call_input.delta", "response.custom_tool_call_input.done":
		p.refuse(reasonToolCalls)

	// D. FAILURE
	case "response.failed", "response.incomplete", "response.error":
		p.refuse(reasonUpstreamTerminal)

	// E. BOUND
	case "response.content_part.added":
		p.observeContentPart(obj, false)
	case "response.content_part.done":
		p.observeContentPart(obj, true)
	case "response.output_text.annotation.added":
		p.observeAnnotation(obj)

	// F. METADATA
	case "response.created", "response.queued", "response.in_progress",
		"response.completed",
		"response.reasoning_summary_part.added", "response.reasoning_summary_part.done",
		"response.reasoning_summary_text.delta", "response.reasoning_summary_text.done",
		"response.reasoning_text.delta", "response.reasoning_text.done":
		// Structural lifecycle and reasoning-channel events. None carries
		// client-visible assistant text and a Responses stream is largely
		// made of them, so refusing here would refuse every Responses
		// stream.
	default:
		// An event this build does not know. It is assumed to matter until
		// proven otherwise: an unrecognized event is one that could have
		// carried text, or opened a tool call.
		p.refuse(reasonUnknownShape)
	}
}

// observeResponsesTextDone verifies the output-text stream's final payload.
// A done event can carry text the client received but whose delta was cut off;
// continuing from a shorter prefix would splice an answer around that loss.
// The only safe cases are exact agreement with the accumulated text or a
// text-only done event, whose text becomes the full prefix. Its own identity
// must still agree with the one all accumulated deltas proved.
//
// Both the identity check and the text comparison are scoped to THIS upstream
// response — the region from passStart on. A .done states the text of the
// response that emitted it, so comparing it with the whole cross-hop prefix
// would refuse a hop that is being exactly correct; the deltas it must agree
// with are the hop's own.
func (p *partialText) observeResponsesTextDone(obj map[string]json.RawMessage) {
	if !p.bindTextStream("response.output_text.delta", obj) {
		return
	}
	raw, ok := obj["text"]
	if !ok || jsonNull(raw) {
		p.refuse(reasonUnknownShape)
		return
	}
	var text string
	if err := json.Unmarshal(raw, &text); err != nil {
		p.refuse(reasonUnknownShape)
		return
	}
	// Read the PASS region, not the cross-hop prefix, in both branches. The
	// empty case is load-bearing: a hop that emits only a .done and no deltas
	// has a non-empty cross-hop prefix, so testing p.text here would send a
	// correct event into the compare branch and refuse it for being right.
	pass := p.passText()
	if len(pass) == 0 {
		p.append(text)
	} else if string(pass) != text {
		p.refuse(reasonUnknownShape)
	}
	if p.unsafe == "" {
		// The text output is complete even when the response envelope's final
		// event never makes it over the wire. Re-asking after this point is a
		// duplicate continuation, not recovery from a cut generation.
		p.finish()
	}
}

// observeResponsesRefusalDelta latches the refusal the refusal channel opened.
//
// It takes no payload, and that is the design rather than an accident: the
// delta member of a response.refusal.delta event is the model's decline, which
// must never reach the continuation body — the proxy would be asking the model
// to continue a sentence it refused to write — so the accumulator refuses the
// stream without ever looking at those bytes. There is no branch on the
// payload's value for refusal text to slip through, and an empty or unreadable
// refusal delta, a shape no conforming upstream emits, is not worth the hole
// such a branch would open.
//
// The refusal is latched for the LOGICAL STREAM: beginUpstreamStream resets
// identities, never latches, so a later hop's clean text deltas cannot release
// it. refuse() also releases whatever prefix was accumulated, so no part of
// the answer the client already saw can be put into a continuation body either
// — the whole stream is refused, not just the declined part of it.
func (p *partialText) observeResponsesRefusalDelta() {
	p.refuse(reasonRefusal)
}

// observeResponsesRefusalDone reads the terminal event of the refusal channel.
// Its text member is `refusal` — the deltas carry `delta` — and a refusal can
// arrive with no delta at all, so this event is the only place some declines
// are ever stated.
//
// A non-empty refusal is the upstream's final word on the request: the client
// watched the model decline, and nothing the proxy could ask afterwards is a
// continuation of the assistant text the prefix holds. It is refused as
// reasonRefusal, not as upstream_terminal: the generation was not cut short
// and the upstream did not fail.
//
// An absent, null or empty member declines nothing and is ignored, identity
// and all — and that asymmetry with the delta above is deliberate, not an
// oversight. A .done is the structural terminator the API emits for EVERY
// content part the response declares, including a part that never carried
// text, so a textless one is an ordinary shape and refusing it would refuse
// streams that are still perfectly continuable. A .delta exists only to carry
// text, so its event type alone is the declaration that the model declined.
// Binding the text stream here would refuse a later delta on a stream whose
// terminator was merely structural.
func (p *partialText) observeResponsesRefusalDone(obj map[string]json.RawMessage) {
	raw, ok := obj["refusal"]
	if !ok || jsonNull(raw) {
		return
	}
	var text string
	if err := json.Unmarshal(raw, &text); err != nil {
		// A refusal member that is not a string states a decline this
		// accumulator cannot read. It is unknown_shape rather than refusal:
		// an operator reading the token must be able to tell "the model
		// declined" from "this proxy could not tell".
		p.refuse(reasonUnknownShape)
		return
	}
	if text == "" {
		return
	}
	p.refuse(reasonRefusal)
}

// observeResponseItem classifies a response.output_item.added event by the
// item it announces. Message and reasoning items are admitted; every other
// item type is a call of some kind — function_call, custom_tool_call,
// computer_call, web_search_call, file_search_call — and is treated as a tool
// call, because a new surface arriving as a new item type must not slip past
// a gate that only knew to look for function_call.
//
// This event is also where a call family is stopped before any of its own
// events arrive: a call item is announced before its arguments stream, so the
// refusal is a property of the announced TOPOLOGY rather than of its payload,
// and no argument delta of a known call surface is ever observed after it.
func (p *partialText) observeResponseItem(obj map[string]json.RawMessage) {
	item, typ, ok := responsesItem(obj)
	if !ok {
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
		p.bindMessageItem(item, obj)
	case "reasoning":
		// Reasoning items carry no client-visible assistant text and are
		// never accumulated, so their identity is not part of the contract.
	default:
		p.refuse(reasonToolCalls)
	}
}

// observeResponseItemDone classifies a response.output_item.done event — the
// closing event of an item an earlier announcement opened.
//
// It is held to the announcing event's identity contract rather than being
// ignored as a lifecycle detail, because the event names the same output item
// and must name the SAME one: a completion whose item id or output_index
// disagrees with the prefix's is a second answer on the wire, which is exactly
// what the MVP refuses. It does NOT latch itemSeen: this is the completion of
// the item the announcement already counted, so latching here would refuse
// every ordinary stream at its own item's end.
func (p *partialText) observeResponseItemDone(obj map[string]json.RawMessage) {
	item, typ, ok := responsesItem(obj)
	if !ok {
		p.refuse(reasonUnknownShape)
		return
	}
	switch typ {
	case "message":
		p.bindMessageItem(item, obj)
	case "reasoning":
		// Same rule as the announcement: a reasoning item's identity is none
		// of the text contract's business.
	default:
		p.refuse(reasonToolCalls)
	}
}

// bindMessageItem proves the identity of one message output item: its own id
// and the index it occupies in the response, stated by the announcing and the
// closing event alike and repeated by every delta of it. Where the sources
// disagree the prefix has no single provenance; where a source omits one of
// them there is nothing to prove the deltas against, and the item is refused
// rather than trusted.
func (p *partialText) bindMessageItem(item, obj map[string]json.RawMessage) {
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
}

// responsesItem reads the item object an output_item event announces or
// closes, and its type. ok is false when the event states no readable item, or
// an item with no readable type, and both are the caller's unknown_shape.
func responsesItem(obj map[string]json.RawMessage) (map[string]json.RawMessage, string, bool) {
	raw, ok := obj["item"]
	if !ok || jsonNull(raw) {
		return nil, "", false
	}
	var item map[string]json.RawMessage
	if err := json.Unmarshal(raw, &item); err != nil || item == nil {
		return nil, "", false
	}
	traw, ok := item["type"]
	if !ok || jsonNull(traw) {
		return nil, "", false
	}
	var typ string
	if err := json.Unmarshal(traw, &typ); err != nil {
		return nil, "", false
	}
	return item, typ, true
}

// observeContentPart reads a content-part lifecycle event —
// response.content_part.added or .done. The event announces or closes one
// content part of one output item; the part's text arrives on the deltas.
// done says which of the two it is, because their invariants differ.
//
// It is not ignored as a lifecycle detail, because it names the exact content
// part a delta names and it names the part's CHANNEL: an event whose identity
// does not match the prefix's is a second content stream, and one whose part
// is a refusal is the refusal channel announced structurally. So the identity
// is proven with the deltas' own contract, and the part's type — when the
// event states one — is read: refusal latches reasonRefusal, an output_text
// part is the channel the prefix is made of, and any other part type is a
// shape this accumulator does not know how to continue from.
//
// The two names are handled by one function because they share the identity
// and the channel, but they do NOT share the text: `.added` states a part
// being opened, which by the API's own shape carries no content yet, so
// there is nothing of its own to disagree with the deltas. `.done` closes
// the part and MAY state its text in full, and a text that disagrees with
// what this pass accumulated means the client received text the prefix does
// not hold — the same loss observeResponsesTextDone refuses, for the same
// reason: continuing from a shorter prefix splices an answer around the
// hole. So a `.done` whose part states a text member has it read and
// compared, and only a `.done` that states none is closed on the identity
// alone. The comparison is against the PASS region, exactly as
// observeResponsesTextDone scopes its own: a continuation hop is a new
// upstream response, and the text its part closes is the text that response
// streamed, not the whole cross-hop prefix.
//
// A `.done` is NOT terminal, and the branch deliberately does not finish the
// accumulator. `response.output_text.done` latches terminal because it states
// the output text in full; this event closes one part of one output, which
// the closing content_part and the enclosing output_item still follow. Only
// the comparison is new here.
//
// A part object that is absent is not refused. The provider then states the
// identity without stating the channel, and the identity is what the contract
// is made of: there is nothing here that could have carried text.
func (p *partialText) observeContentPart(obj map[string]json.RawMessage, done bool) {
	if !p.bindTextStream("response.output_text.delta", obj) {
		return
	}
	raw, ok := obj["part"]
	if !ok || jsonNull(raw) {
		return
	}
	var part map[string]json.RawMessage
	if err := json.Unmarshal(raw, &part); err != nil || part == nil {
		p.refuse(reasonUnknownShape)
		return
	}
	traw, ok := part["type"]
	if !ok || jsonNull(traw) {
		p.refuse(reasonUnknownShape)
		return
	}
	var kind string
	if err := json.Unmarshal(traw, &kind); err != nil {
		p.refuse(reasonUnknownShape)
		return
	}
	switch kind {
	case "output_text":
		if done {
			p.verifyPartText(part)
		}
	case "refusal":
		p.refuse(reasonRefusal)
	default:
		p.refuse(reasonUnknownShape)
	}
}

// verifyPartText holds a closing content part to the text this upstream
// response streamed. A `text` member that is absent or null states nothing
// and closes on the identity alone, which is the ordinary shape: the part's
// content was carried by the deltas the prefix already holds. A member that
// IS stated is read, and anything but exact agreement with the pass's own
// accumulation is unknown_shape — the same fact observeResponsesTextDone
// reports on its own event, for the same reason, and the same token so an
// operator reading a log line sees one failure rather than two spellings of
// it.
//
// The comparison is a check and never a source. The text is NOT appended when
// the pass accumulated nothing: a part closed with text where no delta was
// observed is a cut generation, and observeResponsesTextDone handles that
// case by adopting the stated text as the prefix, while the response's own
// output_text.done — the event that says the output is complete — is what
// decides whether it may be. Copying that branch here would make this event
// a second, weaker path to putting unverified bytes into a continuation body.
func (p *partialText) verifyPartText(part map[string]json.RawMessage) {
	raw, ok := part["text"]
	if !ok || jsonNull(raw) {
		return
	}
	var text string
	if err := json.Unmarshal(raw, &text); err != nil {
		p.refuse(reasonUnknownShape)
		return
	}
	if text == "" {
		// An empty part closed textlessly agrees with any accumulation, and
		// with none. There is nothing to compare and nothing that could have
		// been lost, so it is the same shape as a part stating no text.
		return
	}
	if string(p.passText()) != text {
		p.refuse(reasonUnknownShape)
	}
}

// observeAnnotation reads a response.output_text.annotation.added event: a
// citation the client renders alongside the text part it belongs to.
//
// It is the one event class admitted on the strength of its payload rather
// than refused, and the reasoning is narrow enough to state: the event is
// bound to the SAME content part the prefix was read from — it carries the
// same three identity members a delta carries, and is held to the same
// contract here — and its payload is the annotation itself (a url citation, a
// file citation, a span into the text), never assistant text. There is no
// member of it a continuation body could be built from, and the identity
// check is what proves it cannot have come from another stream. Refusing it
// would refuse every citation-bearing answer, which is a shape this proxy has
// no reason to treat as unreadable.
func (p *partialText) observeAnnotation(obj map[string]json.RawMessage) {
	p.bindTextStream("response.output_text.delta", obj)
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
