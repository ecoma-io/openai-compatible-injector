package inject

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

// ---------------------------------------------------------------------------
// This file closes the GAP recorded under INV-INJ-02 in
// docs/design/behavioral-contract.md, which reads:
//
//   "no property test asserts that a whole corpus of well-formed bodies
//    round-trips byte-identically outside the rewritten member."
//
// The fuzz target pinned the NEGATIVE direction well — invalid input comes
// back unchanged, valid input stays valid, and input with no "model" key
// bytes is untouched — but every one of those assertions either requires the
// input to be invalid or to lack the key entirely. Valid input that DOES
// carry an in-scope "model" string, the case where the rewriter actually
// rewrites, was only covered by hand-written examples: a handful of
// well-formed bodies with the answer written out beside them.
//
// That is the dangerous half to leave unpinned. "Every byte outside the
// replaced value survives" is a property over a corpus, not a list; a
// rewriter that quietly re-serialized, normalized a number, dropped a
// duplicate key, or re-spelled an escape would pass every example above and
// corrupt a client's payload in flight. The mutation that matters most is
// precisely the quiet one, so the quiet direction is the one that gets the
// property test.
//
// The property is checked by DELETION, not by comparison against a recorded
// output. Every "model" string value the rewriter is documented to replace is
// cut out of both the input and the output; what is left must be identical
// byte for byte. Checking the complement rather than the answer means the
// test states the invariant directly, and it cannot be satisfied by a
// rewriter that changes something while also coincidentally matching a
// hand-written expectation.
// ---------------------------------------------------------------------------

// TestRewriteModelPreservesEveryNonValueByte is the corpus test. Each case is
// a well-formed body carrying at least one in-scope "model" string; the
// rewriters must differ from the input ONLY inside the quoted values of those
// keys.
//
// The cases are chosen for what a naive re-serialization would break, one
// hazard per line: number formatting that a float64 round trip rewrites
// (1.0 -> 1, 2.50 -> 2.5, 1e400 -> an overflow error), member order that a
// map decode loses, whitespace and key spacing that only a byte-level scan
// keeps, escape spelling that an encoder rewrites (\u0041 -> A,
// \u00e9 -> é), duplicate keys that a map collapses to the last one, integer
// precision beyond float64's exact range, and nested objects that must be
// descended past rather than flattened.
func TestRewriteModelPreservesEveryNonValueByte(t *testing.T) {
	const public = "public-name"

	// chatOnly would be rewritten under BOTH scopes; the rest are pinned to
	// one scope or the other by whether a top-level "response" object
	// carries a "model".
	cases := []struct {
		name string
		body string
		// responses is the Responses-scope expectation, so a scope that must
		// NOT descend is asserted as such rather than inferred.
		responsesOnly bool
	}{
		{name: "plain", body: `{"model":"gpt-5"}`},
		{name: "whitespace_heavy", body: "{ \n\t\"model\"\t:\r\n \"gpt-5\" ,\n \"n\" : 1 \n}"},
		{name: "number_formatting", body: `{"a":1.0,"model":"gpt-5","b":2.50,"c":1e2,"d":0.0,"e":-0}`},
		{name: "integer_precision", body: `{"big":9007199254740993,"model":"gpt-5","huge":123456789012345678901234567890}`},
		{name: "exponent_forms", body: `{"a":1E+2,"b":1e-2,"c":1.5E10,"model":"gpt-5"}`},
		{name: "escape_spelling", body: `{"u":"Aé\/","model":"gpt-5","t":"tab\there","n":"nl\nhere"}`},
		{name: "unicode_keys", body: `{"mödél":"x","模型":"gpt-5","model":"gpt-5"}`},
		{name: "nested_objects", body: `{"a":{"b":{"c":[1,{"d":"model"}]}},"model":"gpt-5","e":{"f":{}}}`},
		{name: "arrays", body: `{"a":[1,[2,[3,{"model":"nested"}]]],"model":"gpt-5"}`},
		{name: "empty_containers", body: `{"a":[],"b":{},"model":"gpt-5","c":[],"d":{}}`},
		{name: "scalars", body: `{"t":true,"f":false,"n":null,"model":"gpt-5"}`},
		{name: "string_reading_model", body: `{"a":"the \"model\" key","model":"gpt-5"}`},
		{name: "key_containing_model", body: `{"my_model":"x","model_prefix":"y","model":"gpt-5"}`},
		{name: "duplicate_top_level_keys", body: `{"a":1,"model":"gpt-5","a":2,"model":"gpt-9"}`},
		{name: "non_string_model_sibling", body: `{"model":123,"a":1,"b":2}`},
		{name: "model_last", body: `{"a":1,"b":2,"model":"gpt-5"}`},
		{name: "model_first", body: `{"model":"gpt-5","a":1,"b":2}`},
		{name: "long_strings", body: `{"a":"` + strings.Repeat("x", 4096) + `","model":"gpt-5"}`},
		{name: "deep_nesting", body: `{"a":` + strings.Repeat("[", 32) + strings.Repeat("]", 32) + `,"model":"gpt-5"}`},

		// Responses-scope cases: a top-level "response" object whose direct
		// "model" is in scope for the Responses rewriter and out of scope for
		// the chat one.
		{name: "response_envelope", body: `{"type":"response.completed","response":{"id":"r1","model":"gpt-5","status":"completed"},"sequence_number":41}`, responsesOnly: true},
		{name: "response_both_scopes", body: `{"model":"gpt-5","response":{"model":"gpt-5"}}`, responsesOnly: true},
		{name: "response_nested_deeper", body: `{"model":"gpt-5","response":{"wrapper":{"model":"deep"}}}`, responsesOnly: true},
		{name: "response_non_object", body: `{"model":"gpt-5","response":[1,2,3]}`, responsesOnly: true},
		{name: "response_non_string_model", body: `{"response":{"model":7,"id":"r1"}}`, responsesOnly: true},
		{name: "response_duplicate_objects", body: `{"response":{"model":"x"},"response":{"model":"y"}}`, responsesOnly: true},
		{name: "response_whitespace", body: `{"response" : { "model" : "gpt-5" , "id" : "r1" }}`, responsesOnly: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			in := []byte(tc.body)
			if !json.Valid(in) {
				t.Fatalf("fixture is not valid JSON: %s", tc.body)
			}
			// The chat rewriter never descends into "response"; the
			// responses rewriter does. A case with a top-level response
			// object is the one place the two may legitimately differ.
			scopes := []struct {
				name string
				fn   func([]byte, string) []byte
			}{
				{"chat", RewriteChatModel},
				{"responses", RewriteResponsesModel},
			}
			for _, s := range scopes {
				out := s.fn(in, public)
				if !json.Valid(out) {
					t.Fatalf("%s: rewrite produced invalid JSON:\n in  %s\n out %s", s.name, tc.body, out)
				}
				want := deleteModelStringValues(in, s.name == "responses")
				got := deleteModelStringValues(out, s.name == "responses")
				if !bytes.Equal(got, want) {
					t.Fatalf("%s: bytes outside an in-scope \"model\" value changed.\nThe rewriter is byte-preserving: it may replace the value and nothing else.\n in  %s\n out %s\n kept(input)  %q\n kept(output) %q",
						s.name, tc.body, out, want, got)
				}
			}
		})
	}
}

// TestRewriteModelPreservesInScopeValues asserts the other half: the in-scope
// value IS replaced. A byte-preservation test that also passed when the
// rewriter did nothing would be a vacuous guarantee, and "changed nothing" is
// the silent failure this whole file exists to rule out.
func TestRewriteModelPreservesInScopeValues(t *testing.T) {
	const public = "public-name"
	// json.Marshal returns the value WITH its quotes, which is exactly what
	// the rewriter splices in — the span covers the quotes, not the bytes
	// between them.
	quoted, err := json.Marshal(public)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	enc := string(quoted)

	cases := []struct {
		name       string
		body       string
		wantChat   string
		wantResp   string
		bothChange bool
	}{
		// The top-level "model" is in scope under BOTH APIs — the Responses
		// rewriter rewrites everything the chat one does, plus the descent.
		{name: "top_level", body: `{"model":"gpt-5"}`, wantChat: `{"model":` + enc + `}`, wantResp: `{"model":` + enc + `}`, bothChange: true},
		// A body with no top-level "model": only the Responses descent
		// reaches a value, so the chat rewriter must return it untouched.
		{name: "response_envelope", body: `{"response":{"model":"gpt-5"}}`, wantResp: `{"response":{"model":` + enc + `}}`, bothChange: false},
		{name: "both", body: `{"model":"gpt-5","response":{"model":"gpt-5"}}`, wantChat: `{"model":` + enc + `,"response":{"model":"gpt-5"}}`, wantResp: `{"model":` + enc + `,"response":{"model":` + enc + `}}`, bothChange: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			in := []byte(tc.body)
			gotChat := string(RewriteChatModel(in, public))
			gotResp := string(RewriteResponsesModel(in, public))
			wantChat := tc.wantChat
			if wantChat == "" {
				wantChat = tc.body
			}
			wantResp := tc.wantResp
			if wantResp == "" {
				wantResp = tc.body
			}
			if gotChat != wantChat {
				t.Fatalf("chat:\n got  %s\n want %s", gotChat, wantChat)
			}
			if gotResp != wantResp {
				t.Fatalf("responses:\n got  %s\n want %s", gotResp, wantResp)
			}
			// At least one scope must have moved, or the pair of assertions
			// above is testing nothing.
			if !tc.bothChange && gotChat == tc.body && gotResp == tc.body {
				t.Fatalf("neither scope rewrote %s — the fixture has no in-scope value", tc.body)
			}
		})
	}
}

// TestRewriteModelOutOfScopeValueUntouched pins the one asymmetry that is
// easy to lose in a refactor: a top-level "response" object's "model" is the
// Responses envelope's own, and it is the CLIENT's data under the chat scope.
// Rewriting it there mutates a payload the proxy does not own.
func TestRewriteModelOutOfScopeValueUntouched(t *testing.T) {
	body := []byte(`{"model":"gpt-5","response":{"id":"r1","model":"client-owned"}}`)
	got := RewriteChatModel(body, "public-name")
	want := `{"model":"public-name","response":{"id":"r1","model":"client-owned"}}`
	if string(got) != want {
		t.Fatalf("chat scope must not rewrite response.model:\n got  %s\n want %s", got, want)
	}
}

// deleteModelStringValues returns body with the quoted value of every in-scope
// "model" key cut out, so that two bodies differing only in those values
// compare equal. "In scope" means: a direct "model" key of the top-level
// object, plus — when descendResponse is set — a direct "model" key of a
// top-level "response" object.
//
// The deletion is deliberately performed with this package's own scanner
// rather than with encoding/json. That is a real dependency and it is the
// right one here: the property under test is what the rewriter does with the
// bytes AROUND the values, so the oracle must know where the values are by
// the same rule, and an independent JSON-based oracle would be asserting
// something subtly different (it would also, for instance, disagree about
// duplicate keys and escaped key spellings, which the scanner documents as
// out of scope). The scan is validated by TestDeleteModelStringValuesPinsThe
// Spans below, so a scanner change cannot quietly make this oracle vacuous.
func deleteModelStringValues(body []byte, descendResponse bool) []byte {
	if !json.Valid(body) {
		return body
	}
	var spans []span
	scanModelSpans(body, 0, len(body), &spans, descendResponse)
	if len(spans) == 0 {
		return body
	}
	out := make([]byte, 0, len(body))
	prev := 0
	for _, sp := range spans {
		out = append(out, body[prev:sp.start]...)
		prev = sp.end
	}
	return append(out, body[prev:]...)
}

// TestDeleteModelStringValuesPinsTheSpans guards the oracle. If the scanner
// ever stopped finding the values, deleteModelStringValues would become the
// identity function, the corpus test would compare the rewriter against a
// copy of its own input, and every case above would pass no matter what the
// rewriter did. The oracle is only worth what its spans are worth.
//
// Each case states the deleted result as text with the value spliced out as
// a hole, so a span that is off by one quote is visible.
func TestDeleteModelStringValuesPinsTheSpans(t *testing.T) {
	// A span covers the value INCLUDING its quotes, so deleting one leaves
	// the key and its colon behind. The expectations below are the result of
	// that deletion, not of removing the member — the member's comma and key
	// are bytes OUTSIDE the replaced value and the rewriter must keep them.
	cases := []struct {
		name            string
		body            string
		descendResponse bool
		want            string
	}{
		{name: "top_level", body: `{"model":"gpt-5"}`, want: `{"model":}`},
		{name: "no_model", body: `{"a":1}`, want: `{"a":1}`},
		{name: "non_string_value", body: `{"model":123}`, want: `{"model":123}`},
		{name: "key_containing_model", body: `{"my_model":"x","a":1}`, want: `{"my_model":"x","a":1}`},
		{name: "string_value_reading_model", body: `{"a":"model","b":2}`, want: `{"a":"model","b":2}`},
		{name: "both_duplicates", body: `{"model":"a","x":1,"model":"b"}`, want: `{"model":,"x":1,"model":}`},
		{name: "response_not_descended", body: `{"response":{"model":"gpt-5"}}`, want: `{"response":{"model":"gpt-5"}}`},
		{name: "response_descended", body: `{"response":{"model":"gpt-5"}}`, descendResponse: true, want: `{"response":{"model":}}`},
		{name: "response_nested_too_deep", body: `{"response":{"wrapper":{"model":"x"}}}`, descendResponse: true, want: `{"response":{"wrapper":{"model":"x"}}}`},
		{name: "response_non_string", body: `{"response":{"model":7}}`, descendResponse: true, want: `{"response":{"model":7}}`},
		{name: "response_non_object", body: `{"response":[1,2],"model":"gpt-5"}`, descendResponse: true, want: `{"response":[1,2],"model":}`},
		{name: "escaped_key_is_out_of_scope", body: `{"modmodel":"x"}`, want: `{"modmodel":"x"}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := string(deleteModelStringValues([]byte(tc.body), tc.descendResponse))
			if got != tc.want {
				t.Fatalf("oracle spans changed:\n body %s\n got   %s\n want  %s", tc.body, got, tc.want)
			}
		})
	}
}
