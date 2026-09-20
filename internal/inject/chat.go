package inject

import (
	"encoding/json"
	"errors"
	"fmt"

	"openai-compatible-injector/internal/config"
)

// chatInjectedMessage is the system-level message prepended to a chat
// request when the model's injection prompt is non-empty.
type chatInjectedMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// Chat transforms a Chat Completions request body for a configured model:
//
//   - the request's "model" field is set to m.UpstreamModel (added when the
//     request has none);
//   - when m.InjectionPrompt is non-empty, a system-level message
//     {"role":"system","content":InjectionPrompt} is inserted at
//     messages[0], preserving the order of every pre-existing message;
//   - every other field survives byte-for-byte via raw JSON re-marshaling,
//     so unknown and future fields are carried through untouched.
//
// Injection is skipped when "messages" is absent or not a JSON array (a
// request is never corrupted); the model rewrite still applies.
func Chat(body []byte, m config.Model) ([]byte, error) {
	var req map[string]json.RawMessage
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("chat: decode request: %w", err)
	}
	if req == nil {
		return nil, errors.New("chat: request body must be a JSON object")
	}

	upstream, err := json.Marshal(m.UpstreamModel)
	if err != nil {
		return nil, fmt.Errorf("chat: encode upstream model: %w", err)
	}
	req["model"] = upstream

	if m.InjectionPrompt != "" {
		if raw, ok := req["messages"]; ok && firstByte(raw) == '[' {
			var msgs []json.RawMessage
			if err := json.Unmarshal(raw, &msgs); err != nil {
				return nil, fmt.Errorf("chat: decode messages: %w", err)
			}
			injected, err := json.Marshal(chatInjectedMessage{Role: "system", Content: m.InjectionPrompt})
			if err != nil {
				return nil, fmt.Errorf("chat: encode injection message: %w", err)
			}
			msgs = append([]json.RawMessage{injected}, msgs...)
			out, err := json.Marshal(msgs)
			if err != nil {
				return nil, fmt.Errorf("chat: encode messages: %w", err)
			}
			req["messages"] = out
		}
	}

	return json.Marshal(req)
}
