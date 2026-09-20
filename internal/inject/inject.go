// Package inject transforms OpenAI-compatible request bodies on their way to
// an upstream provider: it rewrites the requested model name to the
// configured upstream alias and merges a per-model system-level instruction
// prompt into the payload. It also provides Probe, a cheap request-routing
// extractor, and RewriteChatModel/RewriteResponsesModel, byte-preserving
// rewriters with per-API scope used to sanitize streamed chunks before they
// are relayed back to clients.
package inject

import (
	"encoding/json"
	"errors"
	"fmt"
)

// Probe cheaply extracts the fields the proxy needs to route a Chat
// Completions request body without fully decoding it: the requested model
// name and the stream flag. stream is reported as false when absent or
// null (the OpenAI APIs treat a null flag as its zero value, so rejecting
// here would 400 requests the upstream accepts), and as given for
// true/false. Probe returns an error only when body is not valid JSON or
// when the "stream" field is present but is neither a JSON boolean nor
// null. A "model" field that is not a JSON string is ignored without
// error, and a valid JSON body that is not an object reports an empty model
// and stream=false.
func Probe(body []byte) (model string, stream bool, err error) {
	if !json.Valid(body) {
		return "", false, errors.New("probe: body is not valid JSON")
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(body, &fields); err != nil {
		// Valid JSON that is not an object (array, string, number, ...)
		// carries no routeable model or stream.
		return "", false, nil
	}
	if raw, ok := fields["model"]; ok {
		var name string
		if err := json.Unmarshal(raw, &name); err == nil {
			model = name
		}
	}
	if raw, ok := fields["stream"]; ok {
		var flag *bool
		if err := json.Unmarshal(raw, &flag); err != nil {
			return "", false, fmt.Errorf("probe: stream is not a bool: %w", err)
		}
		if flag != nil {
			stream = *flag
		}
	}
	return model, stream, nil
}

// firstByte returns the first non-whitespace byte of raw, or 0 when raw is
// empty or all whitespace.
func firstByte(raw json.RawMessage) byte {
	for _, c := range raw {
		switch c {
		case ' ', '\t', '\n', '\r':
			continue
		default:
			return c
		}
	}
	return 0
}
