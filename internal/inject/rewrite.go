package inject

import (
	"encoding/json"
)

// span is the [start,end) interval of a "model" value's opening quote
// through its closing quote, in the ORIGINAL body.
type span struct{ start, end int }

// RewriteModel replaces the value of "model" JSON string fields with public,
// preserving ALL other bytes and key order (no re-serialization). The scope
// is exactly the documented contract: the top-level "model" key (requests,
// chat chunks) and the "model" key directly inside a top-level "response"
// object (Responses envelope events carry model inside response.model).
// Anything deeper — client metadata tags, tool output, usage breakdowns —
// belongs to the caller's payload and passes through untouched.
//
// Only JSON string values are replaced ("model":"gpt-5" ->
// "model":"reviewer"); other value types are left untouched, including
// non-string "model" values and "model" text inside string literals. Inputs
// without an in-scope "model" field or that fail json.Valid return the
// input UNCHANGED (streamed [DONE] or delta chunks are never corrupted).
//
// The scan is byte-level: a key written as "model" decodes to "model"
// but is not matched. Providers emit canonical keys; this is a documented
// limitation, not a corruption risk.
func RewriteModel(body []byte, public string) []byte {
	var spans []span
	scanModelSpans(body, 0, len(body), &spans, 0)

	if len(spans) == 0 {
		return body
	}
	quoted, err := json.Marshal(public)
	if err != nil {
		return body // strings always marshal; kept for safety
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

// scanModelSpans walks the object region [start,end) collecting the string
// values of its direct "model" keys. At depth 0 it also descends into any
// object that is the value of a "response" key; depth-1 regions collect
// their "model" keys only and never descend further. Regions that are not
// objects (arrays, scalars) hold no keys and are skipped whole — the
// narrow, documented scope.
func scanModelSpans(body []byte, start, end int, spans *[]span, depth int) {
	i := skipWS(body, start)
	if i >= end || body[i] != '{' {
		return
	}
	i++
	for {
		i = skipWS(body, i)
		if i >= end {
			return
		}
		if body[i] == '}' {
			return
		}
		if body[i] != '"' {
			return // unreachable for json.Valid input
		}
		keyStart := i
		keyEnd := valueEnd(body, i)
		i = skipWS(body, keyEnd)
		if i >= end || body[i] != ':' {
			return // unreachable for json.Valid input
		}
		i = skipWS(body, i+1)
		if i >= end {
			return
		}
		switch {
		case isKey(body, keyStart, keyEnd, "model") && body[i] == '"':
			valEnd := valueEnd(body, i)
			*spans = append(*spans, span{start: i, end: valEnd})
			i = valEnd
		case depth == 0 && isKey(body, keyStart, keyEnd, "response") && body[i] == '{':
			objEnd := collectionEnd(body, i, end)
			scanModelSpans(body, i, objEnd, spans, 1)
			i = objEnd
		default:
			i = skipValue(body, i, end)
		}
		// Consume the member separator; the loop head re-checks for '}'.
		i = skipWS(body, i)
		if i < end && body[i] == ',' {
			i++
		}
	}
}

// isKey reports whether the raw key string spanning [start,end) — quotes
// included — names the given field.
func isKey(body []byte, start, end int, name string) bool {
	return string(body[start:end]) == `"`+name+`"`
}

// skipValue returns the index just past the JSON value starting at i (a
// string, collection, or scalar), without crossing end.
func skipValue(body []byte, i, end int) int {
	switch body[i] {
	case '"':
		return valueEnd(body, i)
	case '{', '[':
		return collectionEnd(body, i, end)
	default:
		// number, true, false, null: a scalar ends at the next structural
		// delimiter.
		j := i
		for j < end && body[j] != ',' && body[j] != '}' && body[j] != ']' {
			j++
		}
		return j
	}
}

// collectionEnd returns the index just past the closing bracket of the
// object or array whose opening bracket is at i, honoring string state.
func collectionEnd(body []byte, i, end int) int {
	depth := 0
	inString := false
	for ; i < end; i++ {
		if inString {
			switch body[i] {
			case '\\':
				i++ // skip the escaped character wholesale
			case '"':
				inString = false
			}
			continue
		}
		switch body[i] {
		case '"':
			inString = true
		case '{', '[':
			depth++
		case '}', ']':
			depth--
			if depth == 0 {
				return i + 1
			}
		}
	}
	return end
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
