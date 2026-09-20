package inject

import (
	"bytes"
	"encoding/json"
)

// RewriteModel replaces the value of every "model" JSON string field in a
// JSON payload with public, preserving ALL other bytes and key order (no
// re-serialization). It covers top-level "model" keys (chat chunks, request
// echoes) and "model" keys nested under an object value of the "response"
// key (Responses envelope events carry model inside response.model); the
// scan applies the same mechanical rule anywhere in the document.
//
// Only JSON string values are replaced ("model":"gpt-5" ->
// "model":"reviewer"); other value types are left untouched, including
// non-string "model" values and "model" text inside string literals. Inputs
// without a "model" field or that fail json.Valid return the input
// UNCHANGED (streamed [DONE] or delta chunks are never corrupted).
func RewriteModel(body []byte, public string) []byte {
	if !json.Valid(body) {
		return body
	}
	quoted, err := json.Marshal(public)
	if err != nil {
		return body // strings always marshal; kept for safety
	}

	// A span is the [start,end) interval of the value's opening quote
	// through its closing quote, in the ORIGINAL body.
	type span struct{ start, end int }
	var spans []span

	key := []byte(`"model"`)
	inString := false
	i := 0
	for i < len(body) {
		if inString {
			switch body[i] {
			case '\\':
				i += 2 // skip the escaped character wholesale
				continue
			case '"':
				inString = false
			}
			i++
			continue
		}
		if body[i] != '"' {
			i++
			continue
		}
		if bytes.HasPrefix(body[i:], key) && isKeyPosition(body, i) {
			j := skipWS(body, i+len(key))
			if j < len(body) && body[j] == ':' {
				j = skipWS(body, j+1)
				switch {
				case j >= len(body):
					i = j
				case body[j] == '"':
					end := valueEnd(body, j)
					spans = append(spans, span{start: j, end: end})
					i = end
				default:
					// Non-string value: keep scanning INSIDE it so nested
					// "model" keys (e.g. response.model) are still found.
					i = j
				}
				continue
			}
		}
		// Any other string (key or value): skip it whole.
		inString = true
		i++
	}

	if len(spans) == 0 {
		return body
	}
	out := make([]byte, 0, len(body))
	prev := 0
	for _, sp := range spans {
		out = append(out, body[prev:sp.start]...)
		out = append(out, quoted...)
		prev = sp.end
	}
	return append(out, body[prev:]...)
}

// isKeyPosition reports whether the byte before i (skipping whitespace) is
// '{' or ',', i.e. body[i] opens an object key rather than a string value or
// array element.
func isKeyPosition(body []byte, i int) bool {
	j := i - 1
	for j >= 0 && isWS(body[j]) {
		j--
	}
	return j >= 0 && (body[j] == '{' || body[j] == ',')
}

// skipWS returns the index of the first non-whitespace byte at or after i.
func skipWS(body []byte, i int) int {
	for i < len(body) && isWS(body[i]) {
		i++
	}
	return i
}

// valueEnd returns the index one past the closing quote of the string whose
// opening quote is at j, honoring backslash escapes. j must point at '"'.
func valueEnd(body []byte, j int) int {
	k := j + 1
	for k < len(body) {
		switch body[k] {
		case '\\':
			k += 2
		case '"':
			return k + 1
		default:
			k++
		}
	}
	return k // unreachable for valid JSON
}

func isWS(c byte) bool {
	return c == ' ' || c == '\t' || c == '\n' || c == '\r'
}
