package inject

import (
	"bytes"
	"encoding/json"
)

// The closed set of reasons a continuation request could not be built. They
// are tokens, never error text: they reach the access log verbatim, and this
// package holds request bodies. Every one of them means the same thing to the
// caller — do not recover this stream — and the token says why.
const (
	// refusalNotObject — the client body is not a JSON object.
	refusalNotObject = "not_object"
	// refusalNoMessages — a Chat body with no usable messages array. There is
	// no turn to continue from.
	refusalNoMessages = "no_messages"
	// refusalUnsupportedShape — a member the builder cannot splice into
	// without guessing: a messages array whose last entry is not an object
	// with a string role, an assistant message whose content is a structured
	// multi-part value rather than text, or a Responses `input` that is
	// neither a string nor an array.
	refusalUnsupportedShape = "unsupported_shape"
	// refusalPreviousResponseID — a Responses body carrying
	// previous_response_id. The upstream would resolve the continuation
	// against a stored response whose generation never finished, and the
	// client body carries no full input to fall back on.
	refusalPreviousResponseID = "previous_response_id"
	// refusalNoOp — the assembled request would be identical to the one that
	// just truncated. Re-sending it is the blind retry this feature exists to
	// avoid, not a continuation.
	refusalNoOp = "no_op"
)

// ContinuationRefusal is the error every builder returns when it declines to
// build a continuation request. It is a typed refusal rather than a plain
// error so the caller can log a closed-set reason token instead of parsing
// error text, and so the fail-closed path is distinguishable from a defect.
type ContinuationRefusal struct{ reason string }

func (e *ContinuationRefusal) Error() string { return "continuation refused: " + e.reason }

// Reason returns the closed-set token naming why the builder declined.
func (e *ContinuationRefusal) Reason() string { return e.reason }

func refuse(reason string) error { return &ContinuationRefusal{reason: reason} }

// continuationInstruction is the one thing an interrupt tells the model. It
// is fixed and not configurable: it carries no operator data, no injection
// prompt, and no request content, and a deployment that could edit it could
// make a continuation duplicate output by accident. Its whole job is to say
// "this is the same answer, keep going" so the model does not restart.
const continuationInstruction = "The previous assistant response was cut off mid-generation. " +
	"Continue it from the exact point where it stopped. " +
	"Do not repeat, rephrase, or summarize any text that was already produced, and do not restart the answer. " +
	"If it was cut off in the middle of a sentence, a word, or a code block, resume from exactly there."

// continuationChatMessage is the assistant message a Chat continuation
// appends to — or rewrites at the end of — the client's own messages array,
// carrying the text the client has already seen.
type continuationChatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// continuationPart is one content part of a Responses input item.
type continuationPart struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

// continuationResponsesItem is one Responses input item. Type is always
// "message" for the items this package builds.
type continuationResponsesItem struct {
	Type    string             `json:"type"`
	Role    string             `json:"role"`
	Content []continuationPart `json:"content"`
}

// BuildContinuationChat returns the body of the one extra upstream request a
// committed Chat Completions stream gets after it dies mid-generation. orig is
// the client's own request body, exactly as it was read; prefix is the
// assistant text the client has already received.
//
// The answer being generated must appear in the conversation as an assistant
// turn, and where it goes depends on how the client's body ends:
//
//   - the last message is already an `assistant` message with string content
//     (a prefill-style request): the streamed text EXTENDS it, so the prefix
//     is appended to that content and NOTHING else about the message changes
//     — its other members, known and unknown alike, are re-emitted from the
//     client's own bytes rather than rebuilt;
//   - any other last message: the assistant turn does not exist yet in the
//     body, so one is appended carrying the prefix.
//
// In both cases the assistant text is the last thing in the body, which is
// what lets the upstream continue it rather than answer it. `stream` is set
// true — the hop is incremental, and a buffered hop would hand the handler a
// whole body where it expects events — and every other member travels exactly
// as the client sent it, unknown and future fields included. The caller then
// runs the body through Chat as usual, so the hop re-applies the candidate's
// model alias and injection and differs from the original attempt in the
// assistant turn and nothing else.
//
// It declines — with a *ContinuationRefusal naming one of the closed-set
// reasons above — rather than guess: there is no safe continuation for a body
// this package cannot read, and a wrong guess here produces duplicated or
// spliced client-visible output. Declining is the correct, expected outcome,
// not an error.
func BuildContinuationChat(orig []byte, prefix string) ([]byte, error) {
	var req map[string]json.RawMessage
	if err := json.Unmarshal(orig, &req); err != nil || req == nil {
		return nil, refuse(refusalNotObject)
	}
	if prefix == "" {
		// Nothing was committed, so there is nothing to continue from: the
		// hop would carry no information the previous attempt did not and is
		// a blind retry wearing a continuation's shape. The accumulator
		// already refuses an empty prefix (no_prefix); refusing again here
		// keeps the invariant this function's own, not the caller's.
		return nil, refuse(refusalNoOp)
	}
	raw, ok := req["messages"]
	if !ok || firstByte(raw) != '[' {
		return nil, refuse(refusalNoMessages)
	}
	var msgs []json.RawMessage
	if err := json.Unmarshal(raw, &msgs); err != nil {
		return nil, refuse(refusalUnsupportedShape)
	}
	if len(msgs) == 0 {
		return nil, refuse(refusalNoMessages)
	}

	last := len(msgs) - 1
	var tail map[string]json.RawMessage
	if err := json.Unmarshal(msgs[last], &tail); err != nil || tail == nil {
		return nil, refuse(refusalUnsupportedShape)
	}
	var role string
	if rawRole, ok := tail["role"]; !ok || json.Unmarshal(rawRole, &role) != nil {
		return nil, refuse(refusalUnsupportedShape)
	}

	if role != "assistant" {
		appended, err := json.Marshal(continuationChatMessage{Role: "assistant", Content: prefix})
		if err != nil {
			return nil, err
		}
		msgs = append(msgs, appended)
	} else {
		var existing string
		// A prefill message with absent or null content is an empty one, not
		// an unreadable one — the OpenAI APIs treat both as "".
		if rawContent, ok := tail["content"]; ok && !jsonIsNull(rawContent) {
			if err := json.Unmarshal(rawContent, &existing); err != nil {
				return nil, refuse(refusalUnsupportedShape)
			}
		}
		// THE MESSAGE IS MUTATED, NOT REBUILT. Only the member this builder
		// owns — `content` — is replaced, and every other member of the
		// prefill message travels exactly as the client sent it: `name`,
		// provider extensions, an opinionated `role`-adjacent field, a field
		// this build has never heard of. Rebuilding the message from the two
		// fields this package knows about would silently drop all of them,
		// and a continuation that quietly rewrites a client's conversation is
		// a corruption the client cannot see — worse than the truncated
		// stream it was meant to repair. Members are re-emitted from the
		// client's own bytes (json.RawMessage), so nothing is normalized.
		extended, err := json.Marshal(existing + prefix)
		if err != nil {
			return nil, err
		}
		tail["content"] = extended
		encodedTail, err := json.Marshal(tail)
		if err != nil {
			return nil, err
		}
		msgs[last] = encodedTail
	}

	encoded, err := json.Marshal(msgs)
	if err != nil {
		return nil, err
	}
	req["messages"] = encoded
	req["stream"] = json.RawMessage("true")
	return json.Marshal(req)
}

// BuildContinuationResponses returns the body of the one extra upstream
// request a committed Responses stream gets after it dies mid-generation.
//
// Responses has no single conversation slot to extend: `input` may be a plain
// string (one user turn) or an array of items, and neither shape can carry an
// assistant turn that continues an answer. Both are therefore normalized into
// the array form, ending with the two items that make a continuation:
//
//   - an assistant `message` item whose `output_text` part is the prefix the
//     client has already received — the answer so far, restated as what it is;
//   - a user `message` item carrying the fixed continuation instruction.
//
// A string `input` is preserved as the leading user item it already meant, so
// nothing the client sent is dropped; an absent `input` contributes no user
// item. `stream` is set true and every other member travels as sent.
//
// It declines — with a *ContinuationRefusal — when `input` is present but
// neither a string nor an array, and when `previous_response_id` is set: the
// upstream would resolve that id against a stored response whose generation
// never completed, and unlike a full `input` there is nothing local to rebuild
// the conversation from.
func BuildContinuationResponses(orig []byte, prefix string) ([]byte, error) {
	var req map[string]json.RawMessage
	if err := json.Unmarshal(orig, &req); err != nil || req == nil {
		return nil, refuse(refusalNotObject)
	}
	if prefix == "" {
		return nil, refuse(refusalNoOp)
	}
	if raw, ok := req["previous_response_id"]; ok && !jsonIsNull(raw) {
		return nil, refuse(refusalPreviousResponseID)
	}

	assistant, err := json.Marshal(continuationResponsesItem{
		Type: "message",
		Role: "assistant",
		Content: []continuationPart{
			{Type: "output_text", Text: prefix},
		},
	})
	if err != nil {
		return nil, err
	}
	instruction, err := json.Marshal(continuationResponsesItem{
		Type: "message",
		Role: "user",
		Content: []continuationPart{
			{Type: "input_text", Text: continuationInstruction},
		},
	})
	if err != nil {
		return nil, err
	}

	var items []json.RawMessage
	switch raw, ok := req["input"]; {
	case !ok || jsonIsNull(raw):
		// Nothing preceded the answer; the continuation is the whole input.
	case firstByte(raw) == '"':
		var existing string
		if err := json.Unmarshal(raw, &existing); err != nil {
			return nil, refuse(refusalUnsupportedShape)
		}
		user, err := json.Marshal(continuationResponsesItem{
			Type: "message",
			Role: "user",
			Content: []continuationPart{
				{Type: "input_text", Text: existing},
			},
		})
		if err != nil {
			return nil, err
		}
		items = append(items, user)
	case firstByte(raw) == '[':
		if err := json.Unmarshal(raw, &items); err != nil {
			return nil, refuse(refusalUnsupportedShape)
		}
	default:
		// A number, object, boolean or null-ish input: not a conversation
		// this builder can extend without inventing structure.
		return nil, refuse(refusalUnsupportedShape)
	}

	items = append(items, assistant, instruction)
	encoded, err := json.Marshal(items)
	if err != nil {
		return nil, err
	}
	req["input"] = encoded
	req["stream"] = json.RawMessage("true")
	return json.Marshal(req)
}

// jsonIsNull reports whether a raw member is JSON null (or absent, which the
// callers model as the same thing). The OpenAI APIs treat a null member as
// its zero value, so null is never a refusal — only a nonzero shape is.
func jsonIsNull(raw json.RawMessage) bool {
	return len(raw) == 0 || bytes.Equal(bytes.TrimSpace(raw), []byte("null"))
}
