package inject

import (
	"bytes"
	"encoding/json"
	"math"
	"strconv"
	"strings"
)

// usageBytes is the fast-path gate for the synthesizers: the member walk is
// only worth its cost on payloads that mention the key at all. The walk
// itself is key-position-aware, so the bytes appearing inside a string value
// are a false positive for this gate only — never a match.
var usageBytes = []byte(`"usage"`)

// tinyCompletionThreshold is the output-token count at or below which a
// response is assumed to carry no reasoning worth reporting: the synthesized
// count for these is zero, keeping the field present for clients that read
// it without attributing thinking to trivially small answers.
const tinyCompletionThreshold = 10

// maxShareableTokens bounds the completion counts the synthesis will read.
// Above 2^62 no real token count lives, and the guard keeps the
// float-to-int64 conversion free of the silent-overflow trap.
const maxShareableTokens = 1 << 62

// edit is one byte-range splice against the original body: the output is
// body[:start] + repl + body[end:]. start == end is an insertion.
type edit struct {
	start, end int
	repl       []byte
}

// usageShape names the two members an API's usage object carries its output
// token count and its details in.
type usageShape struct {
	completion string // "completion_tokens" (Chat) / "output_tokens" (Responses)
	details    string // "completion_tokens_details" / "output_tokens_details"
}

var (
	chatUsageShape      = usageShape{completion: "completion_tokens", details: "completion_tokens_details"}
	responsesUsageShape = usageShape{completion: "output_tokens", details: "output_tokens_details"}
)

// SynthesizeChatThinkingUsage enriches the top-level "usage" object of a
// Chat Completions response with a synthesized "reasoning_tokens" count when
// plan is active: `usage.completion_tokens_details.reasoning_tokens = n`,
// where n = floor(share × usage.completion_tokens), or 0 for completions of
// ten tokens or fewer. This is the Chat scope — the top-level usage object
// only; a chat payload's nested "response" object (if a client or tool emits
// one) is the caller's data and passes through untouched, mirroring
// RewriteChatModel.
//
// The write is a byte-level splice, not a re-serialization: every member,
// byte of whitespace, and key order of the usage object is preserved except
// for the one field inserted or replaced. An existing details object is
// merged into (sibling members like audio_tokens survive); one present with
// a non-object value (null, say) has that value replaced with the
// synthesized object; a missing one is created as the usage object's first
// member.
//
// Nothing is synthesized when the usage object already reports reasoning
// tokens above zero (directly or inside the details object) — the upstream's
// own number always wins — or when anything is unreadable: no usage key, a
// null or non-object usage, a missing/non-numeric/non-positive completion
// count. Inputs without an in-scope usage object, that are not valid JSON,
// or under an inactive plan return the input UNCHANGED (the same backing
// slice) — streamed chunks are never corrupted and the proxy's no-op
// detection keeps its shortcut.
func SynthesizeChatThinkingUsage(body []byte, plan ThinkingPlan) []byte {
	return synthesizeUsage(body, plan, chatUsageShape, false)
}

// SynthesizeResponsesThinkingUsage is SynthesizeChatThinkingUsage for the
// Responses API shape (`usage.output_tokens_details.reasoning_tokens`,
// share × `usage.output_tokens`) with the Responses scope: the top-level
// usage object AND the usage object directly inside a top-level "response"
// object (envelope events such as response.completed carry it there),
// mirroring RewriteResponsesModel's descent. Anything deeper belongs to the
// caller's payload and passes through untouched; every acceptance rule is
// otherwise SynthesizeChatThinkingUsage's.
func SynthesizeResponsesThinkingUsage(body []byte, plan ThinkingPlan) []byte {
	return synthesizeUsage(body, plan, responsesUsageShape, true)
}

// synthesizeUsage is the shared scan: walk the object region, apply one edit
// per in-scope usage object, and splice the result. descendResponse grants a
// single descent into a top-level "response" object — the same rule the
// model rewriters apply — and the descended region may not descend again.
func synthesizeUsage(body []byte, plan ThinkingPlan, shape usageShape, descendResponse bool) []byte {
	// The validity gate is load-bearing, as in rewriteModel: on invalid input
	// the scan's structural assumptions do not hold.
	if !plan.Active || !json.Valid(body) || !bytes.Contains(body, usageBytes) {
		return body
	}
	var edits []edit
	scanUsageEdits(body, 0, len(body), plan, shape, &edits, descendResponse)
	if len(edits) == 0 {
		return body
	}
	return splice(body, edits)
}

// scanUsageEdits walks the object region [start,end) appending at most one
// edit per "usage" object found, in scan order (left to right, so the edits
// are ordered and non-overlapping). The member loop mirrors
// scanModelSpans exactly; only the recorded work differs.
func scanUsageEdits(body []byte, start, end int, plan ThinkingPlan, shape usageShape, edits *[]edit, descendResponse bool) {
	i := skipWS(body, start)
	if i >= end || body[i] != '{' {
		return
	}
	i++
	for {
		i = skipWS(body, i)
		if i >= end || body[i] == '}' {
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
		case isKey(body, keyStart, keyEnd, "usage") && body[i] == '{':
			objEnd := collectionEnd(body, i, end)
			if e, ok := usageEdit(body, i, objEnd, plan, shape); ok {
				*edits = append(*edits, e)
			}
			i = objEnd
		case descendResponse && isKey(body, keyStart, keyEnd, "response") && body[i] == '{':
			objEnd := collectionEnd(body, i, end)
			scanUsageEdits(body, i, objEnd, plan, shape, edits, false)
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

// usageEdit decides the single edit for one usage object spanning
// [objStart,objEnd) — its opening brace at objStart — and reports whether
// there is one. Reads take the LAST occurrence of a duplicated member, the
// same resolution a JSON decoder gives the object.
func usageEdit(body []byte, objStart, objEnd int, plan ThinkingPlan, shape usageShape) (edit, bool) {
	var completion, direct, details, detailsReasoning, detailsOther *span
	i := skipWS(body, objStart+1)
	for i < objEnd {
		if body[i] != '"' {
			break // the closing brace (or invalid input; either way, done)
		}
		keyStart := i
		keyEnd := valueEnd(body, i)
		i = skipWS(body, keyEnd)
		if i >= objEnd || body[i] != ':' {
			return edit{}, false // unreachable for json.Valid input
		}
		i = skipWS(body, i+1)
		if i >= objEnd {
			return edit{}, false
		}
		switch {
		case isKey(body, keyStart, keyEnd, shape.completion):
			vEnd := skipValue(body, i, objEnd)
			s := span{start: i, end: vEnd}
			completion, i = &s, vEnd
		case isKey(body, keyStart, keyEnd, "reasoning_tokens"):
			vEnd := skipValue(body, i, objEnd)
			s := span{start: i, end: vEnd}
			direct, i = &s, vEnd
		case isKey(body, keyStart, keyEnd, shape.details):
			vEnd := skipValue(body, i, objEnd)
			s := span{start: i, end: vEnd}
			if body[i] == '{' {
				details = &s
				detailsReasoning = memberSpan(body, i, vEnd, "reasoning_tokens")
			} else {
				// Present but not an object (null, say): an explicit "no
				// details" that synthesis fills in, replacing the value in
				// place rather than appending a duplicate key.
				detailsOther = &s
			}
			i = vEnd
		default:
			i = skipValue(body, i, objEnd)
		}
		i = skipWS(body, i)
		if i < objEnd && body[i] == ',' {
			i++
		} else if i < objEnd && body[i] != '}' {
			return edit{}, false // unreachable for json.Valid input
		}
	}
	// The upstream's own number always wins: a reasoning field that parses
	// above zero — or that is present but unreadable, which is not proof of
	// absence — leaves the whole usage object untouched.
	if direct != nil {
		v, ok := parseTokenCount(body[direct.start:direct.end])
		if !ok || v > 0 {
			return edit{}, false
		}
	}
	if detailsReasoning != nil {
		v, ok := parseTokenCount(body[detailsReasoning.start:detailsReasoning.end])
		if !ok || v > 0 {
			return edit{}, false
		}
	}
	if completion == nil {
		return edit{}, false
	}
	comp, ok := parseTokenCount(body[completion.start:completion.end])
	if !ok || comp <= 0 || comp > maxShareableTokens {
		return edit{}, false
	}
	n := int64(0)
	if comp > tinyCompletionThreshold {
		n = int64(math.Floor(plan.Share * comp))
	}
	digits := strconv.AppendInt(nil, n, 10)
	switch {
	case detailsReasoning != nil:
		// The upstream reported an explicit zero; replace its digits in place.
		if bytes.Equal(body[detailsReasoning.start:detailsReasoning.end], digits) {
			return edit{}, false // byte-identical; keep the no-op contract
		}
		return edit{start: detailsReasoning.start, end: detailsReasoning.end, repl: digits}, true
	case detailsOther != nil:
		// The details key carries a non-object value; replace the value with
		// the synthesized object.
		obj := []byte(`{"reasoning_tokens":` + string(digits) + `}`)
		return edit{start: detailsOther.start, end: detailsOther.end, repl: obj}, true
	case details != nil:
		// Merge: insert the member at the head of the existing details object,
		// after its opening brace.
		member := []byte(`"reasoning_tokens":` + string(digits))
		return edit{start: details.start + 1, end: details.start + 1, repl: headMember(body, details.start+1, details.end, member)}, true
	default:
		// No details object: create one as the usage object's first member.
		member := []byte(`"` + shape.details + `":{"reasoning_tokens":` + string(digits) + `}`)
		return edit{start: objStart + 1, end: objStart + 1, repl: headMember(body, objStart+1, objEnd, member)}, true
	}
}

// memberSpan returns the value span of key inside the object spanning
// [objStart,objEnd) — its opening brace at objStart — or nil when the member
// is absent. A duplicated member resolves to its last occurrence, the same
// resolution a JSON decoder gives the object.
func memberSpan(body []byte, objStart, objEnd int, key string) *span {
	var found *span
	i := skipWS(body, objStart+1)
	for i < objEnd {
		if body[i] != '"' {
			break
		}
		keyStart := i
		keyEnd := valueEnd(body, i)
		i = skipWS(body, keyEnd)
		if i >= objEnd || body[i] != ':' {
			return nil // unreachable for json.Valid input
		}
		i = skipWS(body, i+1)
		if i >= objEnd {
			return nil
		}
		vEnd := skipValue(body, i, objEnd)
		if isKey(body, keyStart, keyEnd, key) {
			s := span{start: i, end: vEnd}
			found = &s
		}
		i = vEnd
		i = skipWS(body, i)
		if i < objEnd && body[i] == ',' {
			i++
		}
	}
	return found
}

// headMember renders member for insertion at headPos — just past an object's
// opening brace, the object spanning up to objEnd — appending the comma the
// members already present require. An empty object takes the member alone.
func headMember(body []byte, headPos, objEnd int, member []byte) []byte {
	j := skipWS(body, headPos)
	if j < objEnd && body[j] == '}' {
		return member
	}
	return append(append(make([]byte, 0, len(member)+1), member...), ',')
}

// parseTokenCount reads the raw bytes of a JSON number. Integer literals
// parse exactly; decimal or exponent forms fall back to float. Anything
// else — a non-number, or a number a float64 cannot hold finitely —
// reports ok=false. The raw span may carry trailing whitespace (the scalar
// scan stops at the next structural delimiter, not at whitespace), which a
// JSON number never contains internally, so trimming it is exact.
func parseTokenCount(raw []byte) (float64, bool) {
	s := strings.TrimSpace(string(raw))
	if n, err := strconv.ParseInt(s, 10, 64); err == nil {
		return float64(n), true
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil || math.IsNaN(f) || math.IsInf(f, 0) {
		return 0, false
	}
	return f, true
}

// splice applies ordered, non-overlapping edits to body, copying every byte
// outside the edited ranges verbatim (the same builder discipline as
// rewriteModel).
func splice(body []byte, edits []edit) []byte {
	var out []byte
	prev := 0
	for _, e := range edits {
		out = append(out, body[prev:e.start]...)
		out = append(out, e.repl...)
		prev = e.end
	}
	return append(out, body[prev:]...)
}
