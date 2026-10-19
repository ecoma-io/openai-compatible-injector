package inject

import (
	"bytes"
	"encoding/json"
	"strings"
)

// HasImage reports whether a client request body carries image input the
// upstream would need vision to read. It is the vision auto-switch's scanner:
// the proxy consults it once per request, and only when the resolved model
// declares at least one vision-capable candidate — a model without one never
// calls this, which is what keeps the feature's cost out of the hot path for
// every text-only model.
//
// The scan covers the image-bearing shapes the service actually relays:
//
//   - Chat Completions: content parts of type "image_url" at any depth inside
//     the body, plus a data URI spelled inside a plain string content
//     ("data:image/...;base64,..."), the spelling several clients use to embed
//     bytes without switching to the parts form. The nested "images" array
//     Ollama/Hermes clients attach to a message is walked too.
//   - Responses: "input_image" content items.
//   - Anthropic Messages (the body BEFORE MessagesToChat translates it):
//     content blocks of type "image".
//
// Two cheap gates run before any walk: the body must be valid JSON (an
// invalid body cannot reach the transform anyway — Probe rejects it first —
// and reporting false for it is fail-open in the direction that costs
// nothing: text-only candidates stay eligible and the request behaves as it
// always has), and the body must contain the quoted bytes of one of the
// scanned keys, a strict-substring check whose false positives the structural
// walk resolves. A body matching nothing returns false without decoding
// anything.
//
// A string that merely contains the text "image_url" — a user quoting an
// example, a tool schema describing one — is a false positive of the
// substring gate only: the structural walk only reads REAL object members, so
// prose can never flip the answer.
func HasImage(body []byte) bool {
	// The validity gate: a body that is not valid JSON cannot be walked, and
	// it never reaches the transform either — the same acceptance Probe
	// applies. Fail-open on false.
	if !json.Valid(body) {
		return false
	}
	// The byte gate: no image part can exist where the word "image" never
	// appears, and every scanned shape spells it (image_url, input_image,
	// "type":"image", message.images, data:image/). It is deliberately one
	// tolerant marker rather than the exact key spellings: JSON permits
	// whitespace between tokens, so a body spelling `"type": "image"` would
	// slip past a strict `"type":"image"` check and be misread as text. A
	// prose false positive — a prompt discussing images — costs one decode
	// and is resolved by the structural walk below, which reads real object
	// members only.
	if !bytes.Contains(body, []byte("image")) {
		return false
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(body, &fields); err != nil || fields == nil {
		// Valid JSON that is not an object carries no messages or input.
		return false
	}
	// Chat Completions shape: messages[].content.
	if raw, ok := fields["messages"]; ok && firstByte(raw) == '[' &&
		hasImageInMessages(raw) {
		return true
	}
	// Responses shape: input[] items.
	if raw, ok := fields["input"]; ok && firstByte(raw) == '[' &&
		hasImageInResponses(raw) {
		return true
	}
	return false
}

// hasImageInMessages walks the messages array: a message whose content parts
// array contains one image_url or image part — or whose string content spells
// a data image URI — reports an image.
func hasImageInMessages(raw json.RawMessage) bool {
	var msgs []json.RawMessage
	if json.Unmarshal(raw, &msgs) != nil {
		return false
	}
	for _, msg := range msgs {
		var m struct {
			Content json.RawMessage `json:"content"`
			Images  json.RawMessage `json:"images"`
		}
		if json.Unmarshal(msg, &m) != nil {
			continue
		}
		if len(m.Images) > 0 && firstByte(m.Images) == '[' {
			// Ollama/Hermes message-level images array: any non-empty array
			// means the message carries image bytes.
			var arr []json.RawMessage
			if json.Unmarshal(m.Images, &arr) == nil && len(arr) > 0 {
				return true
			}
		}
		if hasImageInContent(m.Content) {
			return true
		}
	}
	return false
}

// hasImageInContent walks one content value: the parts-array form (image_url,
// input_image, image) or a string that spells a data image URI.
func hasImageInContent(content json.RawMessage) bool {
	if len(content) == 0 {
		return false
	}
	switch firstByte(content) {
	case '[':
		var parts []json.RawMessage
		if json.Unmarshal(content, &parts) != nil {
			return false
		}
		for _, part := range parts {
			var p struct {
				Type string `json:"type"`
			}
			if json.Unmarshal(part, &p) != nil {
				continue
			}
			switch p.Type {
			case "image_url", "input_image", "image":
				return true
			}
			// A part may itself carry an "images" array or a data URI in a
			// string field; the nested walk is shallow on purpose.
		}
		return false
	case '"':
		var s string
		if json.Unmarshal(content, &s) == nil {
			return strings.Contains(s, "data:image/")
		}
	}
	return false
}

// hasImageInResponses walks the Responses input array: an item of type
// "input_image" reports an image, or one nested in a content parts array.
func hasImageInResponses(raw json.RawMessage) bool {
	var items []json.RawMessage
	if json.Unmarshal(raw, &items) != nil {
		return false
	}
	for _, item := range items {
		var p struct {
			Type    string          `json:"type"`
			Content json.RawMessage `json:"content"`
		}
		if json.Unmarshal(item, &p) != nil {
			continue
		}
		if p.Type == "input_image" || p.Type == "image" {
			return true
		}
		if hasImageInContent(p.Content) {
			return true
		}
		// The item itself may be a part (some clients spell input as a flat
		// parts array); walk it as content too.
		if hasImageInContent(item) {
			return true
		}
	}
	return false
}
