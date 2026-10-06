package inject

import (
	"bytes"
	"encoding/json"
)

// chatStreamDone is the Chat Completions stream terminator, as it arrives in
// a data-line payload.
var chatStreamDone = []byte("[DONE]")

// contentBlockKind names the content block the translator currently has
// open, if any. Anthropic's stream is a strict sequence — one block open at
// a time, each closed before the next starts — so the kind is what tells a
// text delta whether it may extend the block already open or has to close it
// and start a new one.
type contentBlockKind uint8

const (
	blockNone contentBlockKind = iota
	blockText
	blockTool
)

// ChatToMessagesStream translates one upstream Chat Completions stream into
// the Anthropic Messages event stream. It is a state machine, not a
// payload-to-payload mapping, because the two dialects do not line up: one
// OpenAI chunk can require three Anthropic events (close the open block,
// start the next, carry the delta), Anthropic frames need their own
// `event:` line and blank-line dispatch, and the terminal marker is a
// different event entirely. That is the seam CopySSE's one-payload-in-
// one-payload-out rewriter structurally cannot express, and the reason the
// Messages relay is a dedicated CopyMessagesSSE rather than a widened gate.
//
// The rules it implements:
//
//   - The first JSON-object payload emits message_start. Its id is the
//     upstream's own, passed through opaquely (both APIs document ids as
//     opaque and the client stores exactly what it was given); its model is
//     the PUBLIC name the client asked for, never the upstream alias.
//   - message_start carries zero usage — the upstream's usage object is the
//     last thing a Chat stream sends, so the counts cannot be known yet.
//     message_delta carries the real input_tokens/output_tokens when they
//     arrive. Metering is untouched by any of this: the capture reads
//     pre-rewrite upstream bytes, not these frames.
//   - First non-empty text opens a text block and carries a text_delta;
//     empty and null deltas are skipped rather than opening a block.
//   - A tool_call delta carrying an id (or a name) closes whatever block is
//     open and starts a tool_use block; argument fragments ride
//     input_json_delta. Parallel tool calls arrive sequentially on the wire
//     — the model generates one call's arguments before the next — so a
//     later id starts the next block, which is exactly Anthropic's order.
//   - finish_reason is HELD, not terminal: the terminal trio (content_block_
//     stop, message_delta, message_stop) fires when BOTH a finish_reason and
//     a usage object are known, or at [DONE]/clean EOF once a finish_reason
//     is known. A stream that ends with neither leaves the client with
//     exactly the unterminated stream it would have had anyway — no
//     terminal is ever synthesized for a stream that did not earn one.
//   - An in-band upstream error emits one static event: error frame and
//     latches `failed`, suppressing message_stop. No provider text is
//     carried: the message is this proxy's own, byte-identical to the
//     buffered path's upstream-invalid envelope.
//
// The translator owns no timers, no goroutines and no I/O: it is a pure
// function of the payload sequence, like every other transform in this
// package.
type ChatToMessagesStream struct {
	publicModel string

	// id is latched from the first payload that stated one. message_start
	// goes out before any delta, so a first chunk with no id means the
	// stream announces an empty id — real OpenAI-compatible streams always
	// carry it on the first chunk.
	id string
	// started/stopped/failed are the three latches. stopped and failed both
	// suppress everything that follows: a stream cannot un-terminate, and an
	// error frame is terminal by construction.
	started bool
	stopped bool
	failed  bool

	// finish holds the raw finish_reason value and usage the raw usage
	// object; both are nil until the upstream states them. The terminal
	// trio needs the first, and prefers having the second.
	finish json.RawMessage
	usage  json.RawMessage

	// nextIndex is the block index the NEXT opened block carries;
	// openIndex/openKind describe the one currently open.
	nextIndex int
	openIndex int
	openKind  contentBlockKind
}

// NewChatToMessagesStream builds the translator for one upstream stream.
// publicModel is the name the CLIENT asked for — the alias, not the
// upstream's — because message_start's model is client-facing and must agree
// with the 404 the same name would have produced.
func NewChatToMessagesStream(publicModel string) *ChatToMessagesStream {
	return &ChatToMessagesStream{publicModel: publicModel}
}

// Frame consumes one upstream data payload and returns the complete
// Anthropic events it owes the client, in wire order: zero or more
// `event:`/`data:` frames, each terminated by a blank line. nil means this
// payload owed nothing.
//
// The payload is the bytes after "data:" and its optional space, after the
// relay's rewrite (the model rename, the thinking-usage synthesis, the field
// strip) — so the translator reads the same bytes the meter's pre-rewrite
// observation already saw, minus the members those transforms own.
func (s *ChatToMessagesStream) Frame(payload []byte) []byte {
	if s.stopped || s.failed {
		return nil
	}
	payload = bytes.TrimSpace(payload)
	if len(payload) == 0 {
		return nil
	}
	if bytes.Equal(payload, chatStreamDone) {
		// A [DONE] after a stated finish_reason is the healthy ending. One
		// without it is a stream that never said it was done, and gets
		// nothing — synthesizing a terminal here is exactly the guessing
		// the refusal builders exist to avoid.
		if s.finish == nil {
			return nil
		}
		return s.appendTerminate(nil)
	}
	if payload[0] != '{' || !json.Valid(payload) {
		return nil
	}
	var doc map[string]json.RawMessage
	if err := json.Unmarshal(payload, &doc); err != nil || doc == nil {
		return nil
	}
	// An in-band failure: one static frame, never the provider's text. The
	// `firstByte` guard keeps a provider that states "error": null on every
	// healthy chunk from reading as a failure.
	if raw, ok := doc["error"]; ok && firstByte(raw) == '{' {
		s.failed = true
		return appendEventFrame(nil, "error", chatStreamErrorEvent{
			Type:  "error",
			Error: chatStreamError{Type: "api_error", Message: "upstream returned an invalid response"},
		})
	}

	var choice chatStreamChoice
	if raw, ok := doc["choices"]; ok && firstByte(raw) == '[' {
		var choices []chatStreamChoice
		if json.Unmarshal(raw, &choices) == nil && len(choices) > 0 {
			choice = choices[0]
		}
	}

	// Latch everything BEFORE emitting anything: a first payload that
	// happens to carry both an id and usage seeds message_start with the
	// real counts instead of the zeros.
	if s.id == "" {
		if raw, ok := doc["id"]; ok && firstByte(raw) == '"' {
			_ = json.Unmarshal(raw, &s.id)
		}
	}
	if raw, ok := doc["usage"]; ok && firstByte(raw) == '{' {
		s.usage = append(json.RawMessage(nil), raw...)
	}
	if raw := choice.FinishReason; firstByte(raw) == '"' {
		s.finish = append(json.RawMessage(nil), raw...)
	}

	var buf []byte
	if !s.started {
		s.started = true
		buf = s.appendMessageStart(buf)
	}
	buf = s.appendContent(buf, choice)
	if s.finish != nil && s.usage != nil {
		buf = s.appendTerminate(buf)
	}
	return buf
}

// Finish returns the frames owed at a CLEAN end of input — the relay calls
// it only when the read ended in io.EOF rather than an error. A stream whose
// finish_reason never arrived owes nothing: it stays unterminated, honestly.
func (s *ChatToMessagesStream) Finish() []byte {
	if s.stopped || s.failed || s.finish == nil {
		return nil
	}
	return s.appendTerminate(nil)
}

// appendMessageStart writes the opening frame. Content is an empty array,
// stop_reason and stop_sequence null, usage zeros — the shape Anthropic's
// own message_start has, with the counts filled in later by message_delta.
func (s *ChatToMessagesStream) appendMessageStart(buf []byte) []byte {
	return appendEventFrame(buf, "message_start", chatStreamMessageStart{
		Type: "message_start",
		Message: chatStreamMessage{
			ID:           s.id,
			Type:         "message",
			Role:         "assistant",
			Model:        s.publicModel,
			Content:      []any{},
			StopReason:   json.RawMessage("null"),
			StopSequence: json.RawMessage("null"),
			Usage:        chatAnswerUsage(nil),
		},
	})
}

// appendContent emits the frames one choice's delta owes: text into the
// open (or a freshly opened) text block, tool calls into a new tool_use
// block each time an id or name announces one.
func (s *ChatToMessagesStream) appendContent(buf []byte, choice chatStreamChoice) []byte {
	if raw := choice.Delta.Content; len(raw) > 0 && firstByte(raw) == '"' {
		var text string
		if json.Unmarshal(raw, &text) == nil && text != "" {
			if s.openKind != blockText {
				buf = s.appendClose(buf)
				s.openIndex, s.openKind, s.nextIndex = s.nextIndex, blockText, s.nextIndex+1
				buf = appendEventFrame(buf, "content_block_start", chatStreamBlockStart{
					Type:         "content_block_start",
					Index:        s.openIndex,
					ContentBlock: chatStreamTextBlock{Type: "text", Text: ""},
				})
			}
			buf = appendEventFrame(buf, "content_block_delta", chatStreamBlockDelta{
				Type:  "content_block_delta",
				Index: s.openIndex,
				Delta: chatStreamTextDelta{Type: "text_delta", Text: text},
			})
		}
	}
	for _, call := range choice.Delta.ToolCalls {
		if call.ID != "" || call.Function.Name != "" {
			buf = s.appendClose(buf)
			s.openIndex, s.openKind, s.nextIndex = s.nextIndex, blockTool, s.nextIndex+1
			buf = appendEventFrame(buf, "content_block_start", chatStreamBlockStart{
				Type:  "content_block_start",
				Index: s.openIndex,
				ContentBlock: chatStreamToolBlock{
					Type:  "tool_use",
					ID:    call.ID,
					Name:  call.Function.Name,
					Input: json.RawMessage("{}"),
				},
			})
		}
		// A fragment without an announced block has nowhere to land: an
		// Anthropic delta is addressed to an open block, and inventing one
		// with an empty id would hand the client a tool_use it cannot
		// answer. Dropping it keeps the stream well-formed.
		if s.openKind != blockTool {
			continue
		}
		if fragment := argumentFragment(call.Function.Arguments); fragment != "" {
			buf = appendEventFrame(buf, "content_block_delta", chatStreamBlockDelta{
				Type:  "content_block_delta",
				Index: s.openIndex,
				Delta: chatStreamJSONDelta{Type: "input_json_delta", PartialJSON: fragment},
			})
		}
	}
	return buf
}

// appendClose closes the open block, if any. Called before a block of a
// different kind opens and once more by the terminal trio.
func (s *ChatToMessagesStream) appendClose(buf []byte) []byte {
	if s.openKind == blockNone {
		return buf
	}
	buf = appendEventFrame(buf, "content_block_stop", chatStreamBlockStop{
		Type:  "content_block_stop",
		Index: s.openIndex,
	})
	s.openKind = blockNone
	return buf
}

// appendTerminate writes the terminal trio — close the open block, the
// message_delta carrying stop_reason and the counts, then message_stop —
// and latches stopped so nothing later can append to a finished stream.
func (s *ChatToMessagesStream) appendTerminate(buf []byte) []byte {
	if s.stopped {
		return buf
	}
	s.stopped = true
	buf = s.appendClose(buf)
	buf = appendEventFrame(buf, "message_delta", chatStreamMessageDelta{
		Type: "message_delta",
		Delta: chatStreamDeltaBody{
			StopReason:   stopReason(s.finish),
			StopSequence: json.RawMessage("null"),
		},
		// chatAnswerUsage reads the member under "usage"; a stream that
		// never stated one yields the same zeros message_start carried,
		// which is an honest "this upstream reported nothing" on the client
		// wire. The usage row is a different surface and keeps its NULL.
		Usage: chatAnswerUsage(map[string]json.RawMessage{"usage": s.usage}),
	})
	return appendEventFrame(buf, "message_stop", chatStreamMessageStop{Type: "message_stop"})
}

// appendEventFrame writes one complete SSE frame: the event line, the data
// line, and the blank line that dispatches it. marshalJSON is the package's
// non-HTML-escaping encoder, so a delta carrying "<" or "&" reaches the
// client as those bytes rather than as an escape.
func appendEventFrame(buf []byte, name string, v any) []byte {
	data, err := marshalJSON(v)
	if err != nil {
		// Every value here is a struct of strings, ints and RawMessages —
		// encoding cannot fail. Skipping rather than emitting a torn frame
		// keeps the invariant that everything Frame returns is complete.
		return buf
	}
	buf = append(buf, "event: "...)
	buf = append(buf, name...)
	buf = append(buf, '\n')
	buf = append(buf, "data: "...)
	buf = append(buf, data...)
	buf = append(buf, '\n', '\n')
	return buf
}

// argumentFragment renders one tool_call's `function.arguments` as the
// partial-JSON string an input_json_delta carries. The wire spells it as a
// JSON string of the fragment; a provider that hands back the whole object
// instead is accepted as one complete fragment; anything else is dropped
// rather than forwarded as something the client cannot concatenate.
func argumentFragment(raw json.RawMessage) string {
	if len(raw) == 0 || firstByte(raw) == 'n' {
		return ""
	}
	var fragment string
	if json.Unmarshal(raw, &fragment) == nil {
		return fragment
	}
	if firstByte(raw) == '{' && json.Valid(raw) {
		return string(raw)
	}
	return ""
}

// The wire shapes below are Anthropic's own, spelled member by member so
// the frame bytes are stable: field order is declaration order, and a golden
// test can assert on them exactly.

type chatStreamChoice struct {
	Delta struct {
		Content   json.RawMessage      `json:"content"`
		ToolCalls []chatStreamToolCall `json:"tool_calls"`
	} `json:"delta"`
	FinishReason json.RawMessage `json:"finish_reason"`
}

type chatStreamToolCall struct {
	ID       string `json:"id"`
	Function struct {
		Name      string          `json:"name"`
		Arguments json.RawMessage `json:"arguments"`
	} `json:"function"`
}

type chatStreamMessageStart struct {
	Type    string            `json:"type"`
	Message chatStreamMessage `json:"message"`
}

type chatStreamMessage struct {
	ID           string          `json:"id"`
	Type         string          `json:"type"`
	Role         string          `json:"role"`
	Model        string          `json:"model"`
	Content      []any           `json:"content"`
	StopReason   json.RawMessage `json:"stop_reason"`
	StopSequence json.RawMessage `json:"stop_sequence"`
	Usage        json.RawMessage `json:"usage"`
}

type chatStreamBlockStart struct {
	Type         string `json:"type"`
	Index        int    `json:"index"`
	ContentBlock any    `json:"content_block"`
}

type chatStreamTextBlock struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

type chatStreamToolBlock struct {
	Type  string          `json:"type"`
	ID    string          `json:"id"`
	Name  string          `json:"name"`
	Input json.RawMessage `json:"input"`
}

type chatStreamBlockDelta struct {
	Type  string `json:"type"`
	Index int    `json:"index"`
	Delta any    `json:"delta"`
}

type chatStreamTextDelta struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

type chatStreamJSONDelta struct {
	Type        string `json:"type"`
	PartialJSON string `json:"partial_json"`
}

type chatStreamBlockStop struct {
	Type  string `json:"type"`
	Index int    `json:"index"`
}

type chatStreamMessageDelta struct {
	Type  string              `json:"type"`
	Delta chatStreamDeltaBody `json:"delta"`
	Usage json.RawMessage     `json:"usage"`
}

type chatStreamDeltaBody struct {
	StopReason   json.RawMessage `json:"stop_reason"`
	StopSequence json.RawMessage `json:"stop_sequence"`
}

type chatStreamMessageStop struct {
	Type string `json:"type"`
}

type chatStreamErrorEvent struct {
	Type  string          `json:"type"`
	Error chatStreamError `json:"error"`
}

type chatStreamError struct {
	Type    string `json:"type"`
	Message string `json:"message"`
}
