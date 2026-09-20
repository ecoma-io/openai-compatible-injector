package inject

import (
	"encoding/json"
	"errors"
	"fmt"

	"openai-compatible-injector/internal/config"
)

// responsesDevContent is one content part of the developer message prepended
// to a Responses request's instruction list.
type responsesDevContent struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

// responsesDevItem is the developer message item prepended to the
// "instructions" array of a Responses request.
type responsesDevItem struct {
	Type    string                `json:"type"`
	Role    string                `json:"role"`
	Content []responsesDevContent `json:"content"`
}

// Responses transforms a Responses API request body for a configured model:
// the request's "model" field is set to m.UpstreamModel and the injection
// prompt is merged into the "instructions" field (the system-level
// instruction slot):
//
//   - instructions absent -> set to the prompt string;
//   - instructions string -> prompt + "\n\n" + existing;
//   - instructions array  -> a developer message item
//     {"type":"message","role":"developer","content":[{"type":"input_text","text":prompt}]}
//     prepended to the array;
//   - any other type      -> left untouched.
//
// Every other field keeps its parsed value: values travel as raw JSON and
// are re-marshaled from the decoded form, so unknown and future fields are
// carried through semantically intact — JSON semantics, not input bytes
// (member order, whitespace, and number formatting may be normalized by the
// round trip). The model rewrite is unconditional; when m.InjectionPrompt
// is empty only the instructions merge is skipped.
func Responses(body []byte, m config.Model) ([]byte, error) {
	var req map[string]json.RawMessage
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("responses: decode request: %w", err)
	}
	if req == nil {
		return nil, errors.New("responses: request body must be a JSON object")
	}

	// The model rewrite is unconditional — it is the mapping's core function.
	// The injection prompt is optional, and only the injection is skipped
	// when it is empty.
	upstream, err := json.Marshal(m.UpstreamModel)
	if err != nil {
		return nil, fmt.Errorf("responses: encode upstream model: %w", err)
	}
	req["model"] = upstream

	if m.InjectionPrompt == "" {
		return json.Marshal(req)
	}

	switch raw, ok := req["instructions"]; {
	case !ok:
		prompt, err := json.Marshal(m.InjectionPrompt)
		if err != nil {
			return nil, fmt.Errorf("responses: encode prompt: %w", err)
		}
		req["instructions"] = prompt
	case firstByte(raw) == '"':
		var existing string
		if err := json.Unmarshal(raw, &existing); err == nil {
			merged, err := json.Marshal(m.InjectionPrompt + "\n\n" + existing)
			if err != nil {
				return nil, fmt.Errorf("responses: merge instructions: %w", err)
			}
			req["instructions"] = merged
		}
	case firstByte(raw) == '[':
		var items []json.RawMessage
		if err := json.Unmarshal(raw, &items); err == nil {
			dev, err := json.Marshal(responsesDevItem{
				Type: "message",
				Role: "developer",
				Content: []responsesDevContent{
					{Type: "input_text", Text: m.InjectionPrompt},
				},
			})
			if err != nil {
				return nil, fmt.Errorf("responses: encode developer item: %w", err)
			}
			items = append([]json.RawMessage{dev}, items...)
			out, err := json.Marshal(items)
			if err != nil {
				return nil, fmt.Errorf("responses: encode instructions: %w", err)
			}
			req["instructions"] = out
		}
	}

	return json.Marshal(req)
}
