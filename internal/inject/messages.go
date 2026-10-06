package inject

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

// marshalJSON encodes with HTML escaping off. The default encoder turns "<",
// ">" and "&" into their \u escapes, which decode back to the same string but
// are not the same BYTES — and this translation's contract with a client is
// that what it sent comes back to it unchanged, most pointedly the model
// name. The proxy's own envelope builder (marshalEnvelopeJSON) already
// encodes this way for the same reason.
func marshalJSON(v any) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return bytes.TrimRight(buf.Bytes(), "\n"), nil
}

// This file translates the Anthropic Messages surface onto Chat Completions:
// MessagesToChat on the way to the upstream, ChatToMessages on the way back.
//
// Unlike the transforms beside it, this one KNOWINGLY gives up the
// byte-preserving discipline the rest of the package is built on. There is
// nothing to preserve: the two dialects do not share a schema, so the body
// is decoded, mapped member by member through an explicit allow-list, and
// re-encoded. What the translation keeps instead of bytes is its
// narrowness — a member the allow-list does not name never reaches the
// upstream, so a field that only means something to an Anthropic client
// (thinking budgets, context management, beta markers) cannot leak into a
// strict OpenAI-compatible provider.

// StripContextMarker removes the "1m" context-window capability suffix a
// Claude client appends to a model name ("claude-sonnet-4-5[1m]"). The
// capability travels on the Anthropic wire as a beta header, not as part of
// the model id, so the name that reaches the model lookup must not carry it.
//
// Matching is anchored to the end and case-insensitive, and a name that does
// NOT match comes back UNCHANGED and untrimmed: trimming a name that had no
// marker would silently resolve a differently-spelled configured model, and
// a miss must never alter the input.
func StripContextMarker(model string) (stripped string, matched bool) {
	trimmed := strings.TrimSpace(model)
	if len(trimmed) < len("[1m]") || !strings.HasSuffix(strings.ToLower(trimmed), "[1m]") {
		return model, false
	}
	return trimmed[:len(trimmed)-len("[1m]")], true
}

// MessagesToChat translates an Anthropic Messages request body into a Chat
// Completions request body.
//
// What survives the translation, and why:
//
//   - "model" is copied through untouched — it is the PUBLIC name, and
//     inject.Chat rewrites it to the configured upstream alias as its own
//     stage, exactly as it does for a native chat request.
//   - "system" becomes one leading OpenAI system message. A block array is
//     joined with a single newline: the blocks are distinct documents rather
//     than fragments of one, and gluing them would merge a sentence ending
//     with the next one's first word.
//   - each turn fans out to zero or more OpenAI messages: tool_result
//     blocks become role:"tool" messages (in source order, ahead of the user
//     text that followed them, which is where OpenAI requires them), text
//     and image blocks become content parts, tool_use blocks become
//     tool_calls, and thinking/redacted_thinking blocks are dropped — an
//     OpenAI upstream has no such content part.
//   - tools map input_schema onto parameters; anything without an object
//     schema (a server-side tool) is dropped, because it has no OpenAI
//     client-tool meaning.
//   - stop_sequences -> stop, metadata.user_id -> user, and the scalar
//     sampling knobs pass through as raw JSON.
//   - "stream" passes through, and a streamed request additionally gains
//     stream_options.include_usage so the upstream reports the counts this
//     proxy otherwise has no way to show a Messages client. That member is
//     client-facing wire shape only: the usage meter reads the upstream's
//     own bytes before any rewrite and is unaffected by it.
//
// Everything else is dropped by construction. The result is deliberately
// NOT byte-identical to the input in any respect — member order and number
// formatting are the encoder's — and an input that is not a JSON object, or
// whose "messages" is not a non-empty JSON array, is an error: the caller
// answers it as a local transform failure, which never falls back to
// another candidate.
func MessagesToChat(body []byte) ([]byte, error) {
	var req map[string]json.RawMessage
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("messages: decode request: %w", err)
	}
	if req == nil {
		return nil, errors.New("messages: request body must be a JSON object")
	}

	rawTurns, ok := req["messages"]
	if !ok || firstByte(rawTurns) != '[' {
		return nil, errors.New(`messages: "messages" must be a JSON array`)
	}
	var turns []json.RawMessage
	if err := json.Unmarshal(rawTurns, &turns); err != nil {
		return nil, fmt.Errorf("messages: decode messages: %w", err)
	}
	if len(turns) == 0 {
		return nil, errors.New(`messages: "messages" must be a non-empty JSON array`)
	}

	out := map[string]json.RawMessage{}

	// The public model name rides through unchanged; inject.Chat owns the
	// rewrite. Copying it here rather than omitting it keeps the request a
	// well-formed chat body even if the caller composes the two stages in
	// the other order.
	if raw, ok := req["model"]; ok {
		out["model"] = raw
	}

	messages := make([]json.RawMessage, 0, len(turns)+1)
	if raw, ok := req["system"]; ok {
		msg, err := chatSystemMessage(raw)
		if err != nil {
			return nil, err
		}
		if msg != nil {
			messages = append(messages, msg)
		}
	}
	for _, turn := range turns {
		translated, err := anthropicTurn(turn)
		if err != nil {
			return nil, err
		}
		messages = append(messages, translated...)
	}
	encoded, err := marshalJSON(messages)
	if err != nil {
		return nil, fmt.Errorf("messages: encode messages: %w", err)
	}
	out["messages"] = encoded

	// Scalar knobs travel as raw JSON: their types are the caller's, and a
	// value the upstream rejects is the upstream's answer, not this
	// translator's to reinterpret.
	for _, key := range []string{"max_tokens", "temperature", "top_p", "stream", "parallel_tool_calls"} {
		if raw, ok := req[key]; ok {
			out[key] = raw
		}
	}

	if raw, ok := req["stop_sequences"]; ok && firstByte(raw) == '[' {
		out["stop"] = raw
	}
	if raw, ok := req["metadata"]; ok && firstByte(raw) == '{' {
		var meta map[string]json.RawMessage
		if json.Unmarshal(raw, &meta) == nil {
			if uid, ok := meta["user_id"]; ok {
				out["user"] = uid
			}
		}
	}
	if raw, ok := req["tools"]; ok {
		if tools, ok := chatTools(raw); ok {
			out["tools"] = tools
		}
	}
	if raw, ok := req["tool_choice"]; ok {
		choice, disableParallel, ok := chatToolChoice(raw)
		if ok {
			out["tool_choice"] = choice
			if disableParallel {
				// disable_parallel_tool_use lives on Anthropic's tool_choice;
				// its OpenAI counterpart is the top-level parallel_tool_calls.
				out["parallel_tool_calls"] = json.RawMessage("false")
			}
		}
	}

	// Asked for after the passthrough so an explicit disable on
	// tool_choice wins over any pass-through value: the two say the same
	// thing and this one is the client's later, narrower intent.
	if raw, ok := req["stream"]; ok {
		var flag *bool
		if json.Unmarshal(raw, &flag) == nil && flag != nil && *flag {
			out["stream_options"] = json.RawMessage(`{"include_usage":true}`)
		}
	}

	return marshalJSON(out)
}

// chatSystemMessage renders Anthropic's "system" — a string or an array of
// text blocks — as one leading OpenAI system message. Blocks of any other
// type are dropped, and an absent or empty system yields no message at all
// rather than an empty one.
func chatSystemMessage(raw json.RawMessage) (json.RawMessage, error) {
	switch firstByte(raw) {
	case '"':
		var text string
		if err := json.Unmarshal(raw, &text); err != nil {
			return nil, fmt.Errorf("messages: decode system: %w", err)
		}
		if text == "" {
			return nil, nil
		}
		return chatMessage("system", text)
	case '[':
		var blocks []json.RawMessage
		if err := json.Unmarshal(raw, &blocks); err != nil {
			return nil, fmt.Errorf("messages: decode system: %w", err)
		}
		parts := make([]string, 0, len(blocks))
		for _, b := range blocks {
			var blk struct {
				Type string `json:"type"`
				Text string `json:"text"`
			}
			if json.Unmarshal(b, &blk) != nil || blk.Type != "text" {
				continue
			}
			parts = append(parts, blk.Text)
		}
		if len(parts) == 0 {
			return nil, nil
		}
		return chatMessage("system", strings.Join(parts, "\n"))
	default:
		return nil, nil
	}
}

// anthropicTurn translates one Anthropic message into the OpenAI messages it
// becomes — more than one when tool results fan out of a user turn, none
// when every block in the turn is one this translation drops.
func anthropicTurn(raw json.RawMessage) ([]json.RawMessage, error) {
	if firstByte(raw) != '{' {
		return nil, nil
	}
	var msg struct {
		Role    string          `json:"role"`
		Content json.RawMessage `json:"content"`
	}
	if err := json.Unmarshal(raw, &msg); err != nil {
		return nil, fmt.Errorf("messages: decode message: %w", err)
	}
	if msg.Role != "user" && msg.Role != "assistant" {
		return nil, nil
	}
	if len(msg.Content) == 0 || firstByte(msg.Content) == 'n' {
		return nil, nil
	}

	// A plain string carries no structure to translate.
	if firstByte(msg.Content) == '"' {
		var text string
		if err := json.Unmarshal(msg.Content, &text); err != nil {
			return nil, fmt.Errorf("messages: decode message content: %w", err)
		}
		if text == "" {
			return nil, nil
		}
		m, err := chatMessage(msg.Role, text)
		if err != nil || m == nil {
			return nil, err
		}
		return []json.RawMessage{m}, nil
	}
	if firstByte(msg.Content) != '[' {
		return nil, nil
	}

	var blocks []json.RawMessage
	if err := json.Unmarshal(msg.Content, &blocks); err != nil {
		return nil, fmt.Errorf("messages: decode message content: %w", err)
	}
	if msg.Role == "assistant" {
		return anthropicAssistantTurn(blocks)
	}
	return anthropicUserTurn(blocks)
}

// anthropicUserTurn translates a user turn. tool_result blocks become tool
// messages FIRST — OpenAI requires a tool message to sit directly after the
// assistant message that requested the call and before any further user
// text, so a tool result that appeared after text in the Anthropic array is
// still emitted ahead of it — then the remaining parts become one user
// message.
func anthropicUserTurn(blocks []json.RawMessage) ([]json.RawMessage, error) {
	var out []json.RawMessage
	var content contentCollector
	for _, b := range blocks {
		blk, err := decodeBlock(b)
		if err != nil {
			return nil, err
		}
		if blk == nil {
			continue
		}
		switch blk.typ {
		case "text":
			if err := content.addText(blk.text); err != nil {
				return nil, err
			}
		case "image":
			content.addImage(blk.raw)
		case "tool_result":
			m, err := toolResultMessage(blk.raw)
			if err != nil {
				return nil, err
			}
			if m != nil {
				out = append(out, m)
			}
		default:
			// thinking, redacted_thinking, and every block type without an
			// OpenAI counterpart are dropped rather than guessed at.
		}
	}
	if !content.empty() {
		m, err := content.message("user")
		if err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, nil
}

// anthropicAssistantTurn translates an assistant turn into a single message
// carrying its text and/or its tool calls. A turn whose blocks all drop
// (thinking only, say) produces no message at all: an OpenAI assistant
// message with neither content nor tool_calls is not a legal message, and
// inventing one would corrupt the history.
func anthropicAssistantTurn(blocks []json.RawMessage) ([]json.RawMessage, error) {
	var texts []string
	var toolCalls []json.RawMessage
	for _, b := range blocks {
		blk, err := decodeBlock(b)
		if err != nil {
			return nil, err
		}
		if blk == nil {
			continue
		}
		switch blk.typ {
		case "text":
			texts = append(texts, blk.text)
		case "tool_use":
			call, err := toolCall(blk.raw)
			if err != nil {
				return nil, err
			}
			if call != nil {
				toolCalls = append(toolCalls, call)
			}
		}
	}
	if len(texts) == 0 && len(toolCalls) == 0 {
		return nil, nil
	}

	msg := map[string]json.RawMessage{"role": json.RawMessage(`"assistant"`)}
	switch {
	case len(texts) > 0:
		content, err := marshalJSON(strings.Join(texts, ""))
		if err != nil {
			return nil, fmt.Errorf("messages: encode assistant content: %w", err)
		}
		msg["content"] = content
	default:
		// Tool calls with no accompanying text: content is explicitly null,
		// which is how a native chat request spells "the model said nothing".
		msg["content"] = json.RawMessage("null")
	}
	if len(toolCalls) > 0 {
		calls, err := marshalJSON(toolCalls)
		if err != nil {
			return nil, fmt.Errorf("messages: encode tool_calls: %w", err)
		}
		msg["tool_calls"] = calls
	}
	encoded, err := marshalJSON(msg)
	if err != nil {
		return nil, fmt.Errorf("messages: encode message: %w", err)
	}
	return []json.RawMessage{encoded}, nil
}

// anthropicBlock is the decoded shape of one content block, reduced to what
// the translation dispatches on. raw is the block's own bytes, kept for the
// members that need a second, type-specific decode (images, tool payloads).
type anthropicBlock struct {
	typ  string
	text string
	raw  json.RawMessage
}

// decodeBlock decodes one block far enough to dispatch on its type. A block
// that is not an object, or carries no string "type", is unrecognizable and
// reported as nil — dropped, never an error: an unknown block in an
// otherwise valid message must not 400 the request.
func decodeBlock(raw json.RawMessage) (*anthropicBlock, error) {
	if firstByte(raw) != '{' {
		return nil, nil
	}
	var probe struct {
		Type string `json:"type"`
		Text string `json:"text"`
	}
	if err := json.Unmarshal(raw, &probe); err != nil {
		return nil, fmt.Errorf("messages: decode content block: %w", err)
	}
	if probe.Type == "" {
		return nil, nil
	}
	return &anthropicBlock{typ: probe.Type, text: probe.Text, raw: raw}, nil
}

func textPart(text string) (json.RawMessage, error) {
	return marshalJSON(map[string]string{"type": "text", "text": text})
}

// imagePart maps an Anthropic image block onto an OpenAI image_url part,
// accepting both of its source forms. A source this translation does not
// recognize yields ok=false and the block is dropped: emitting a part with
// an empty url would be a request the upstream rejects on our invention.
func imagePart(raw json.RawMessage) (json.RawMessage, bool) {
	var blk struct {
		Source struct {
			Type      string `json:"type"`
			MediaType string `json:"media_type"`
			Data      string `json:"data"`
			URL       string `json:"url"`
		} `json:"source"`
	}
	if json.Unmarshal(raw, &blk) != nil {
		return nil, false
	}
	var url string
	switch blk.Source.Type {
	case "base64":
		if blk.Source.Data == "" || blk.Source.MediaType == "" {
			return nil, false
		}
		url = "data:" + blk.Source.MediaType + ";base64," + blk.Source.Data
	case "url":
		if blk.Source.URL == "" {
			return nil, false
		}
		url = blk.Source.URL
	default:
		return nil, false
	}
	part, err := marshalJSON(map[string]any{
		"type":      "image_url",
		"image_url": map[string]string{"url": url},
	})
	if err != nil {
		return nil, false
	}
	return part, true
}

// toolResultMessage maps one tool_result block onto an OpenAI tool message.
// A string content travels as a string; an array of text-only blocks is
// concatenated into one (a plain string is what every OpenAI-compatible
// upstream accepts, and the parts form is only needed once an image is
// present).
func toolResultMessage(raw json.RawMessage) (json.RawMessage, error) {
	var blk struct {
		ToolUseID string          `json:"tool_use_id"`
		Content   json.RawMessage `json:"content"`
	}
	if err := json.Unmarshal(raw, &blk); err != nil {
		return nil, fmt.Errorf("messages: decode tool_result: %w", err)
	}
	if blk.ToolUseID == "" {
		return nil, nil
	}

	var content any = ""
	switch {
	case len(blk.Content) == 0 || firstByte(blk.Content) == 'n':
		content = ""
	case firstByte(blk.Content) == '"':
		var text string
		if err := json.Unmarshal(blk.Content, &text); err != nil {
			return nil, fmt.Errorf("messages: decode tool_result content: %w", err)
		}
		content = text
	case firstByte(blk.Content) == '[':
		collector, err := collectParts(blk.Content)
		if err != nil {
			return nil, err
		}
		value, err := collector.render()
		if err != nil {
			return nil, err
		}
		content = json.RawMessage(value)
	}

	msg, err := marshalJSON(map[string]any{
		"role":         "tool",
		"tool_call_id": blk.ToolUseID,
		"content":      content,
	})
	if err != nil {
		return nil, fmt.Errorf("messages: encode tool result: %w", err)
	}
	return msg, nil
}

// toolCall maps one tool_use block onto an OpenAI tool call. The id and
// name travel unchanged — both APIs document ids as opaque — and the input
// object becomes the arguments STRING the chat shape carries, re-encoded
// from the decoded form because the two spellings are different documents.
func toolCall(raw json.RawMessage) (json.RawMessage, error) {
	var blk struct {
		ID    string          `json:"id"`
		Name  string          `json:"name"`
		Input json.RawMessage `json:"input"`
	}
	if err := json.Unmarshal(raw, &blk); err != nil {
		return nil, fmt.Errorf("messages: decode tool_use: %w", err)
	}
	if blk.ID == "" || blk.Name == "" {
		return nil, nil
	}

	input := blk.Input
	if len(input) == 0 || firstByte(input) != '{' {
		input = json.RawMessage("{}")
	}
	// arguments is the JSON STRING form of the input object — the chat shape
	// carries a string, not the object, and an Anthropic client reads the
	// object back out of it.
	arguments, err := marshalJSON(string(input))
	if err != nil {
		return nil, fmt.Errorf("messages: encode tool arguments: %w", err)
	}

	call, err := marshalJSON(map[string]any{
		"id":   blk.ID,
		"type": "function",
		"function": map[string]json.RawMessage{
			"name":      mustMarshalName(blk.Name),
			"arguments": arguments,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("messages: encode tool call: %w", err)
	}
	return call, nil
}

// mustMarshalName encodes a tool name. json.Marshal on a string can only
// fail for a value the encoder cannot represent, which a decoded string
// never is; the empty RawMessage would be a programming error rather than a
// client one, so the failure is left to the caller's re-marshal.
func mustMarshalName(name string) json.RawMessage {
	encoded, err := marshalJSON(name)
	if err != nil {
		return json.RawMessage(`""`)
	}
	return encoded
}

// chatTools maps an Anthropic tool array onto an OpenAI function-tool array.
// A tool with no object input_schema is a server-side tool (web search and
// the like) with no client-side counterpart, so it is dropped rather than
// emitted with a schema it does not have. ok=false when nothing survives.
func chatTools(raw json.RawMessage) (json.RawMessage, bool) {
	var tools []json.RawMessage
	if err := json.Unmarshal(raw, &tools); err != nil || len(tools) == 0 {
		return nil, false
	}
	out := make([]json.RawMessage, 0, len(tools))
	for _, t := range tools {
		var tool struct {
			Name        string          `json:"name"`
			Description json.RawMessage `json:"description"`
			InputSchema json.RawMessage `json:"input_schema"`
		}
		if json.Unmarshal(t, &tool) != nil || tool.Name == "" {
			continue
		}
		if firstByte(tool.InputSchema) != '{' {
			continue
		}
		fn := map[string]json.RawMessage{
			"name":       mustMarshalName(tool.Name),
			"parameters": tool.InputSchema,
		}
		if len(tool.Description) > 0 && firstByte(tool.Description) == '"' {
			fn["description"] = tool.Description
		}
		entry, err := marshalJSON(map[string]json.RawMessage{
			"type":     json.RawMessage(`"function"`),
			"function": mustMarshalObject(fn),
		})
		if err != nil {
			continue
		}
		out = append(out, entry)
	}
	if len(out) == 0 {
		return nil, false
	}
	encoded, err := marshalJSON(out)
	if err != nil {
		return nil, false
	}
	return encoded, true
}

// mustMarshalObject re-encodes a map whose members are already encoded
// values. The members are all json.RawMessage, so the only failure mode is
// an unencodable key — impossible for the fixed keys used here — and a nil
// return would surface as a dropped member rather than a panic.
func mustMarshalObject(m map[string]json.RawMessage) json.RawMessage {
	encoded, err := marshalJSON(m)
	if err != nil {
		return json.RawMessage("{}")
	}
	return encoded
}

// chatToolChoice maps Anthropic's tool_choice onto OpenAI's, reporting
// whether it also asked to disable parallel tool use (whose OpenAI
// counterpart is the top-level parallel_tool_calls). An object-form
// "auto"/"any"/"none" becomes OpenAI's string form, and a "tool" object
// becomes the function discriminator.
func chatToolChoice(raw json.RawMessage) (out json.RawMessage, disableParallel bool, ok bool) {
	switch firstByte(raw) {
	case '"':
		var name string
		if json.Unmarshal(raw, &name) != nil {
			return nil, false, false
		}
		switch name {
		case "auto", "none", "required":
			encoded, err := marshalJSON(name)
			if err != nil {
				return nil, false, false
			}
			return encoded, false, true
		}
		return nil, false, false
	case '{':
		var choice struct {
			Type                   string `json:"type"`
			Name                   string `json:"name"`
			DisableParallelToolUse *bool  `json:"disable_parallel_tool_use"`
		}
		if json.Unmarshal(raw, &choice) != nil {
			return nil, false, false
		}
		disable := choice.DisableParallelToolUse != nil && *choice.DisableParallelToolUse
		switch choice.Type {
		case "auto", "none":
			encoded, err := marshalJSON(choice.Type)
			if err != nil {
				return nil, disable, false
			}
			return encoded, disable, true
		case "any":
			return json.RawMessage(`"required"`), disable, true
		case "tool":
			if choice.Name == "" {
				return nil, disable, false
			}
			encoded, err := marshalJSON(map[string]any{
				"type":     "function",
				"function": map[string]string{"name": choice.Name},
			})
			if err != nil {
				return nil, disable, false
			}
			return encoded, disable, true
		}
		return nil, disable, false
	}
	return nil, false, false
}

// contentCollector accumulates the OpenAI content parts a run of Anthropic
// blocks becomes, remembering whether an image is among them. An all-text
// run renders as a plain string — the shape every OpenAI-compatible upstream
// accepts — while an image forces the parts array, which is the only form
// that can carry one. Text-only collapse also keeps the common single-text
// message byte-identical to what a native chat client would have sent.
type contentCollector struct {
	parts    []json.RawMessage
	texts    []string
	sawImage bool
}

func (c *contentCollector) addText(text string) error {
	part, err := textPart(text)
	if err != nil {
		return err
	}
	c.parts = append(c.parts, part)
	c.texts = append(c.texts, text)
	return nil
}

func (c *contentCollector) addImage(raw json.RawMessage) {
	if part, ok := imagePart(raw); ok {
		c.parts = append(c.parts, part)
		c.sawImage = true
	}
}

func (c *contentCollector) empty() bool { return len(c.parts) == 0 }

// render encodes the content value: the concatenated text when no image was
// collected, the parts array otherwise. An empty collector still renders —
// "" — because the callers gate on empty() first and this path only guards
// against a caller that forgets.
func (c *contentCollector) render() ([]byte, error) {
	if !c.sawImage {
		return marshalJSON(strings.Join(c.texts, ""))
	}
	return marshalJSON(c.parts)
}

func (c *contentCollector) message(role string) (json.RawMessage, error) {
	content, err := c.render()
	if err != nil {
		return nil, fmt.Errorf("messages: encode content: %w", err)
	}
	msg, err := marshalJSON(map[string]json.RawMessage{
		"role":    mustMarshalName(role),
		"content": content,
	})
	if err != nil {
		return nil, fmt.Errorf("messages: encode message: %w", err)
	}
	return msg, nil
}

// collectParts translates a user-side content-part array into a collector.
func collectParts(raw json.RawMessage) (*contentCollector, error) {
	var blocks []json.RawMessage
	if err := json.Unmarshal(raw, &blocks); err != nil {
		return nil, fmt.Errorf("messages: decode content: %w", err)
	}
	var c contentCollector
	for _, b := range blocks {
		blk, err := decodeBlock(b)
		if err != nil {
			return nil, err
		}
		if blk == nil {
			continue
		}
		switch blk.typ {
		case "text":
			if err := c.addText(blk.text); err != nil {
				return nil, err
			}
		case "image":
			c.addImage(blk.raw)
		}
	}
	return &c, nil
}

// chatMessage builds a single-content message of the given role.
func chatMessage(role, content string) (json.RawMessage, error) {
	encoded, err := marshalJSON(map[string]string{"role": role, "content": content})
	if err != nil {
		return nil, fmt.Errorf("messages: encode message: %w", err)
	}
	return encoded, nil
}

// ChatToMessages translates a buffered Chat Completions answer into an
// Anthropic Message. publicModel is the name the client asked for — the
// upstream's own spelling is its business and never reaches this surface.
//
// ok is false only when the body is not a JSON object: that is an answer
// this translation cannot read, and the caller answers it as an unusable
// upstream response. A well-formed object that is merely missing choices
// degrades instead — empty content, a null stop_reason — because an
// upstream's own oddity must not turn into a local error.
//
// Ids pass through opaquely (chatcmpl-* becomes message.id, call_*
// becomes tool_use.id): both APIs document ids as opaque and the client
// echoes back exactly what it was given, so no mapping state is needed.
// reasoning_content is dropped — an Anthropic content block for it would
// have to be echoed back in the next request's history, which this
// translation does not do.
func ChatToMessages(body []byte, publicModel string) ([]byte, bool) {
	if firstByte(body) != '{' {
		return nil, false
	}
	var doc map[string]json.RawMessage
	if err := json.Unmarshal(body, &doc); err != nil || doc == nil {
		return nil, false
	}

	out := map[string]json.RawMessage{
		"type": json.RawMessage(`"message"`),
		"role": json.RawMessage(`"assistant"`),
	}
	if raw, ok := doc["id"]; ok && firstByte(raw) == '"' {
		out["id"] = raw
	}
	if raw, err := marshalJSON(publicModel); err == nil {
		out["model"] = raw
	}

	var choice struct {
		Message struct {
			Role      string          `json:"role"`
			Content   json.RawMessage `json:"content"`
			ToolCalls []struct {
				ID       string `json:"id"`
				Function struct {
					Name      string          `json:"name"`
					Arguments json.RawMessage `json:"arguments"`
				} `json:"function"`
			} `json:"tool_calls"`
		} `json:"message"`
		FinishReason json.RawMessage `json:"finish_reason"`
	}
	var hasChoice bool
	if raw, ok := doc["choices"]; ok && firstByte(raw) == '[' {
		var choices []json.RawMessage
		if json.Unmarshal(raw, &choices) == nil && len(choices) > 0 {
			if json.Unmarshal(choices[0], &choice) == nil {
				hasChoice = true
			}
		}
	}

	content, err := chatAnswerContent(choice.Message.Content, choice.Message.ToolCalls)
	if err != nil {
		return nil, false
	}
	out["content"] = content

	if hasChoice {
		if choice.Message.Role != "" {
			if raw, err := marshalJSON(choice.Message.Role); err == nil {
				out["role"] = raw
			}
		}
		out["stop_reason"] = stopReason(choice.FinishReason)
	} else {
		out["stop_reason"] = json.RawMessage("null")
	}
	out["stop_sequence"] = json.RawMessage("null")
	out["usage"] = chatAnswerUsage(doc)

	encoded, err := marshalJSON(out)
	if err != nil {
		return nil, false
	}
	return encoded, true
}

// chatAnswerContent builds the Anthropic content block array from a chat
// message: its text becomes one text block (absent when the text is) and
// each tool call becomes a tool_use block whose arguments are decoded back
// into the input object — unparseable arguments degrade to an empty object
// rather than failing an otherwise readable answer.
func chatAnswerContent(raw json.RawMessage, calls []struct {
	ID       string `json:"id"`
	Function struct {
		Name      string          `json:"name"`
		Arguments json.RawMessage `json:"arguments"`
	} `json:"function"`
}) (json.RawMessage, error) {
	blocks := []json.RawMessage{}

	var text string
	if firstByte(raw) == '"' && json.Unmarshal(raw, &text) == nil && text != "" {
		part, err := marshalJSON(map[string]string{"type": "text", "text": text})
		if err != nil {
			return nil, err
		}
		blocks = append(blocks, part)
	}

	for _, call := range calls {
		if call.ID == "" || call.Function.Name == "" {
			continue
		}
		input := json.RawMessage("{}")
		switch {
		case len(call.Function.Arguments) == 0:
			// No arguments member at all: an empty input object.
		case firstByte(call.Function.Arguments) == '"':
			// The chat shape carries arguments as a JSON STRING; the
			// Anthropic shape carries the object itself. A string that does
			// not decode to an object degrades to the empty input.
			var encoded string
			if json.Unmarshal(call.Function.Arguments, &encoded) == nil {
				if obj := json.RawMessage(encoded); json.Valid(obj) && firstByte(obj) == '{' {
					input = obj
				}
			}
		case firstByte(call.Function.Arguments) == '{' && json.Valid(call.Function.Arguments):
			// A non-standard upstream that already sends the object.
			input = call.Function.Arguments
		}
		block, err := marshalJSON(map[string]json.RawMessage{
			"type":  json.RawMessage(`"tool_use"`),
			"id":    mustMarshalName(call.ID),
			"name":  mustMarshalName(call.Function.Name),
			"input": input,
		})
		if err != nil {
			return nil, err
		}
		blocks = append(blocks, block)
	}

	encoded, err := marshalJSON(blocks)
	if err != nil {
		return nil, err
	}
	return encoded, nil
}

// stopReason maps a chat finish_reason onto an Anthropic stop_reason. A
// finish_reason this table does not name — or one that is absent, which a
// buffered 200 should not carry — lands on end_turn, the honest reading of
// "the answer came back whole".
func stopReason(raw json.RawMessage) json.RawMessage {
	var reason string
	if json.Unmarshal(raw, &reason) != nil || reason == "" {
		return json.RawMessage("null")
	}
	switch reason {
	case "stop":
		return json.RawMessage(`"end_turn"`)
	case "length":
		return json.RawMessage(`"max_tokens"`)
	case "tool_calls", "function_call":
		return json.RawMessage(`"tool_use"`)
	case "content_filter":
		return json.RawMessage(`"refusal"`)
	default:
		return json.RawMessage(`"end_turn"`)
	}
}

// chatAnswerUsage maps the chat usage object onto Anthropic's. An upstream
// that stated no usage yields zeros here: this is a buffered answer, so the
// counts exist as a fact about a completed response and the two APIs differ
// only in spelling. That is unlike the usage row, which keeps NULL when the
// upstream stated nothing — the meter reads the upstream's own bytes and
// this is the client's view.
func chatAnswerUsage(doc map[string]json.RawMessage) json.RawMessage {
	usage := map[string]json.RawMessage{
		"input_tokens":  json.RawMessage("0"),
		"output_tokens": json.RawMessage("0"),
	}
	raw, ok := doc["usage"]
	if !ok || firstByte(raw) != '{' {
		encoded, err := marshalJSON(usage)
		if err == nil {
			return encoded
		}
		return json.RawMessage(`{"input_tokens":0,"output_tokens":0}`)
	}
	var wire struct {
		PromptTokens     json.RawMessage `json:"prompt_tokens"`
		CompletionTokens json.RawMessage `json:"completion_tokens"`
	}
	if json.Unmarshal(raw, &wire) == nil {
		if n, ok := integerMember(wire.PromptTokens); ok {
			usage["input_tokens"] = n
		}
		if n, ok := integerMember(wire.CompletionTokens); ok {
			usage["output_tokens"] = n
		}
	}
	encoded, err := marshalJSON(usage)
	if err != nil {
		return json.RawMessage(`{"input_tokens":0,"output_tokens":0}`)
	}
	return encoded
}

// integerMember re-encodes one token count, accepting the shapes
// OpenAI-compatible backends actually emit. A member that is not a count
// leaves ok=false so the caller keeps its default rather than forwarding
// something the client cannot read as a number.
func integerMember(raw json.RawMessage) (json.RawMessage, bool) {
	if len(raw) == 0 || firstByte(raw) == 'n' {
		return nil, false
	}
	var n int64
	if json.Unmarshal(raw, &n) == nil {
		encoded, err := marshalJSON(n)
		if err != nil {
			return nil, false
		}
		return encoded, true
	}
	var f float64
	if json.Unmarshal(raw, &f) == nil {
		encoded, err := marshalJSON(f)
		if err != nil {
			return nil, false
		}
		return encoded, true
	}
	var s string
	if json.Unmarshal(raw, &s) == nil {
		var parsed int64
		if _, err := fmt.Sscan(s, &parsed); err == nil {
			encoded, err := marshalJSON(parsed)
			if err == nil {
				return encoded, true
			}
		}
	}
	return nil, false
}
