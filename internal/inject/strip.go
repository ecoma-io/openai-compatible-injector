package inject

import (
	"bytes"
	"encoding/json"
)

// StripPatterns precomputes the per-line gate bytes for a strip list: the
// quoted first segment of every path, spelled exactly as canonical JSON keys
// carry it ("provider" for the path provider, "parent name" for
// 'parent name'.child). The SSE relay uses these to decide whether a data
// line is worth handing to the strip rewriter at all, widening its existing
// "model"/"usage" acceptance gate without ever decoding the line. The result
// is nil when paths is empty, and first segments are deduplicated: what the
// gate asks is membership, not how many paths share an entry segment.
func StripPatterns(paths [][]string) [][]byte {
	if len(paths) == 0 {
		return nil
	}
	out := make([][]byte, 0, len(paths))
	seen := make(map[string]struct{}, len(paths))
	for _, p := range paths {
		if len(p) == 0 {
			continue
		}
		pat := quoteStripSegment(p[0])
		if _, dup := seen[string(pat)]; dup {
			continue
		}
		seen[string(pat)] = struct{}{}
		out = append(out, pat)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// quoteStripSegment renders one path segment the way the byte scan compares
// keys: quoted exactly as canonical JSON keys are written, so it becomes a
// bytes.Contains probe for the segment's presence anywhere in a payload.
func quoteStripSegment(seg string) []byte {
	return []byte(`"` + seg + `"`)
}

// StripChatFields excises every member named by one of paths from the
// top-level object of a Chat Completions payload, preserving ALL other bytes
// and key order (no re-serialization): a stripped member's key, colon, value
// and comma are removed in place, and an object that loses every member
// collapses to {}. Provider-added fields like "provider" or "service_tier"
// disappear before the relay without a decode/encode round trip.
//
// A path is an ordered object-key list, one segment per member to match. The
// traversal is object-only: a segment whose value is an array or a scalar
// does not match at that point, and no path descends through an array.
// Duplicate keys in a payload are excised one and all — one left standing is
// one leak. A path validated by config never names model or usage (the
// proxy's own data — the rename and the thinking-usage synthesis); direct
// callers are trusted the same way.
//
// This is the Chat scope: the top-level object only. A chat payload's nested
// "response" object, if a client or tool emits one, is the caller's data and
// is reached only through the paths themselves, never eagerly.
//
// Valid input that matches nothing, and invalid input, return the input
// UNCHANGED (the same backing slice) — the SSE relay's pointer-identity fast
// path depends on it, and streamed chunks are never corrupted.
func StripChatFields(body []byte, paths [][]string) []byte {
	return stripFields(body, paths, false)
}

// StripResponsesFields is StripChatFields for the Responses API scope: the
// top-level object AND the object directly inside a top-level "response"
// envelope (the shape envelope events like response.completed carry their
// provider-added fields in), mirroring the model and usage rewriters' single
// descent. Inside the envelope the whole path list applies again from its
// first segment; the descended region may not descend eagerly a second time.
func StripResponsesFields(body []byte, paths [][]string) []byte {
	return stripFields(body, paths, true)
}

// stripFields is the shared scan: the validity gate and the first-segment
// pattern gate, one member walk per in-scope object, then a single splice
// over the collected removal spans. descendResponse grants the single
// Responses-scope descent into a "response" object.
func stripFields(body []byte, paths [][]string, descendResponse bool) []byte {
	// The validity gate is load-bearing, as in rewriteModel: on invalid input
	// the scan's structural assumptions do not hold. The pattern gate is the
	// SSE fast path's other half — no key can be excised when its exact bytes
	// appear nowhere.
	if !json.Valid(body) {
		return body
	}
	if len(paths) == 0 || !mentionsStripKey(body, paths) {
		return body
	}
	var edits []edit
	stripObject(body, 0, len(body), paths, 0, descendResponse, &edits)
	if len(edits) == 0 {
		return body
	}
	return splice(body, coalesceRemovals(edits))
}

// mentionsStripKey reports whether any path's quoted first segment appears
// anywhere in body. A false answer proves no key can be excised (a matching
// key would carry those bytes, and membership is a strict superset of key
// presence); a true answer is a false positive for the scan only — a string
// value may contain the same byte run, and the scan resolves it.
func mentionsStripKey(body []byte, paths [][]string) bool {
	for _, p := range paths {
		if len(p) > 0 && bytes.Contains(body, quoteStripSegment(p[0])) {
			return true
		}
	}
	return false
}

// stripObject walks the object region [start,end) — its opening brace at
// start — recording a removal edit for every member that completes a path at
// the current depth and recursing once per object value a path continues
// through. descendResponse grants the single Responses-scope descent into a
// "response" object, whose members are rescanned at depth 0 and which may not
// descend again. Regions that are not objects (arrays, scalars) hold no keys
// and are skipped whole — the narrow, documented scope.
func stripObject(body []byte, start, end int, paths [][]string, depth int, descendResponse bool, edits *[]edit) {
	i := skipWS(body, start)
	if i >= end || body[i] != '{' {
		return
	}
	i++
	// prevEnd is the end of the previous SURVIVING member's value: the anchor
	// a last-member removal reaches back to for its preceding comma. It stays
	// -1 while no member has survived, so the first removal is either
	// key-anchored (it carries its own trailing comma) or sole-member.
	prevEnd := -1
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
		valStart := i
		valEnd := skipValue(body, i, end)
		sep := skipWS(body, valEnd)
		hasComma := sep < end && body[sep] == ','

		// Match the key against every path's segment at this depth. A path is
		// completed by excising the member; a path continues by descending an
		// object value.
		excise := false
		var recurse [][]string
		for _, p := range paths {
			if depth >= len(p) {
				continue
			}
			if !isKey(body, keyStart, keyEnd, p[depth]) {
				continue
			}
			if depth == len(p)-1 {
				excise = true
			} else if body[valStart] == '{' {
				recurse = append(recurse, p)
			}
		}
		// The Responses-scope descent is independent of any path: a top-level
		// "response" object is rescanned with every path at its first segment.
		descend := descendResponse && isKey(body, keyStart, keyEnd, "response") && body[valStart] == '{'

		if excise {
			switch {
			case hasComma:
				// A first or middle member: remove it with its trailing comma.
				*edits = append(*edits, edit{start: keyStart, end: sep + 1})
			case prevEnd < 0:
				// The sole surviving member: removing its own span leaves {}.
				*edits = append(*edits, edit{start: keyStart, end: valEnd})
			default:
				// The last member: remove from the previous survivor's value
				// end, carrying that comma with it.
				*edits = append(*edits, edit{start: prevEnd, end: valEnd})
			}
			// An excised value goes away whole; edits into it (a longer path
			// through the same member, or the response descent) are moot and
			// would only overlap the removal.
		} else {
			prevEnd = valEnd
			if len(recurse) > 0 {
				stripObject(body, valStart, valEnd, recurse, depth+1, false, edits)
			}
			if descend {
				stripObject(body, valStart, valEnd, paths, 0, false, edits)
			}
		}

		i = sep
		if hasComma {
			i++
		}
	}
}

// coalesceRemovals merges overlapping or adjacent removal spans, given in scan
// order. A strip list may name a path and one of its prefixes (a and a.b), or
// the Responses descent and a response.* path may reach the same member, and
// both produce edits that overlap or coincide; collapsing them in one pass
// keeps the splice contract — ordered, non-overlapping, exact union removal.
//
// The merged span is the MINIMUM start and MAXIMUM end of its members, not
// the first member's span extended: when two CONSECUTIVE members are both
// excised, the first removal takes the hasComma shape (its own key through
// its own trailing comma) while the second takes the prevEnd-anchored shape
// (the last survivor's value end through its value end). Those spans
// overlap, and the union of the two removals — everything from the last
// survivor's value end to the second member's value end — is what must go;
// keeping the first member's key start would strip the survivor's trailing
// comma into a dangling ",". Min-start also absorbs a nested edit inside a
// member its own removal already covers.
func coalesceRemovals(edits []edit) []edit {
	out := edits[:0]
	for _, e := range edits {
		if len(out) == 0 {
			out = append(out, e)
			continue
		}
		last := &out[len(out)-1]
		if e.start <= last.end {
			if e.start < last.start {
				last.start = e.start
			}
			if e.end > last.end {
				last.end = e.end
			}
			continue
		}
		out = append(out, e)
	}
	return out
}
