package inject

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

// ---------------------------------------------------------------------------
// This file closes the same class of gap for INV-INJ-03 that
// rewrite_property_test.go closes for INV-INJ-02, and it can state its
// property more sharply than the model rewriter's, because excision has a
// property VALUE REPLACEMENT does not have: a strip can only ever delete.
//
// Four properties, each load-bearing for a different way this transform
// could quietly corrupt a payload in flight:
//
//  1. SUBSEQUENCE — the output is a subsequence of the input. Re-serializing
//     through a map reorders members, normalizes numbers and re-spells
//     escapes, none of which is a deletion, so this single check rules out
//     the whole forbidden class at once. It is checked on every corpus case.
//
//  2. IDEMPOTENCE — stripping an already-stripped body changes nothing.
//     Every key the paths name is gone after one pass, so a second pass has
//     no work to do and must not touch the bytes. A splice bug that removed
//     a survivor's comma on the first pass tends to damage the body on the
//     second, which is why this is a separate assertion rather than a
//     corollary of the first.
//
//  3. NO-OP IDENTITY — equal bytes must be the SAME SLICE. INV-INJ-03 makes
//     pointer identity load-bearing for the SSE relay's fast path, and
//     sameSlice is already in the package for exactly this, but it is only
//     reached from the hand-written no-op cases. Here it is asserted over
//     the whole corpus: if a strip ever produced identical bytes through a
//     new allocation, every byte-level test would still pass and the relay
//     would silently lose its fast path.
//
//  4. VALIDITY — a valid body stays valid. The existing suite and fuzz target
//     already cover this; it is kept here so the corpus is self-contained and
//     a failure names the case rather than a shared loop.
// ---------------------------------------------------------------------------

// stripCorpus is the shared body/path matrix. The bodies are chosen to reach
// every branch of the comma-anchoring logic in stripObject — the three
// removal shapes (own trailing comma, sole member, previous survivor's value
// end) — plus the Responses descent, the nested-path recursion, the
// coalescing overlap, and the hazards a re-serialization would introduce.
var stripCorpus = []struct {
	name  string
	body  string
	paths [][]string
}{
	{name: "first_member", body: `{"provider":"x","a":1,"b":2}`, paths: [][]string{{"provider"}}},
	{name: "middle_member", body: `{"a":1,"provider":"x","b":2}`, paths: [][]string{{"provider"}}},
	{name: "last_member", body: `{"a":1,"b":2,"provider":"x"}`, paths: [][]string{{"provider"}}},
	{name: "sole_member", body: `{"provider":"x"}`, paths: [][]string{{"provider"}}},
	{name: "consecutive_members", body: `{"a":1,"provider":"x","service_tier":"s","b":2}`, paths: [][]string{{"provider"}, {"service_tier"}}},
	{name: "all_members_excised", body: `{"provider":"x","service_tier":"s"}`, paths: [][]string{{"provider"}, {"service_tier"}}},
	{name: "duplicates_excised", body: `{"a":1,"provider":"x","provider":"y","b":2}`, paths: [][]string{{"provider"}}},
	{name: "duplicate_only_member", body: `{"provider":"x","provider":"y"}`, paths: [][]string{{"provider"}}},
	{name: "nested_path_leaf", body: `{"a":{"b":{"provider":"x","keep":1}}}`, paths: [][]string{{"a", "b", "provider"}}},
	{name: "nested_path_sole_leaf", body: `{"a":{"b":{"provider":"x"}}}`, paths: [][]string{{"a", "b", "provider"}}},
	{name: "prefix_and_full_path", body: `{"a":{"provider":"x","b":1},"provider":"y","c":2}`, paths: [][]string{{"a", "provider"}, {"provider"}}},
	{name: "overlapping_paths", body: `{"a":{"b":1,"c":2},"z":3}`, paths: [][]string{{"a"}, {"a", "b"}}},
	{name: "response_descent", body: `{"response":{"service_tier":"std","model":"x"},"id":"r1"}`, paths: [][]string{{"service_tier"}}},
	{name: "response_sole_member", body: `{"response":{"service_tier":"std"}}`, paths: [][]string{{"service_tier"}}},
	{name: "response_plus_top_level", body: `{"provider":"x","response":{"provider":"y","keep":1}}`, paths: [][]string{{"provider"}}},
	{name: "whitespace_framing", body: "{ \n\t\"a\" : 1 , \"provider\" : \"x\" , \"b\" : 2 \n}", paths: [][]string{{"provider"}}},
	{name: "whitespace_consecutive", body: "{ \"a\" : 1 , \"provider\" : \"x\" , \"service_tier\" : \"s\" , \"b\" : 2 }", paths: [][]string{{"provider"}, {"service_tier"}}},
	{name: "number_formatting", body: `{"a":1.0,"provider":"x","b":2.50,"c":1e400,"d":0.0,"e":-0}`, paths: [][]string{{"provider"}}},
	{name: "integer_precision", body: `{"big":9007199254740993,"provider":"x"}`, paths: [][]string{{"provider"}}},
	{name: "escape_spelling", body: `{"a":"Aé\/","provider":"x","b":"tab\there"}`, paths: [][]string{{"provider"}}},
	{name: "unicode_keys", body: `{"模型":"keep","provider":"x","mödél":1}`, paths: [][]string{{"provider"}}},
	{name: "member_order_preserved", body: `{"z":1,"y":2,"provider":"x","x":3,"w":4}`, paths: [][]string{{"provider"}}},
	{name: "array_values_skipped", body: `{"a":[1,2,{"provider":"decoy"}],"provider":"x"}`, paths: [][]string{{"provider"}}},
	{name: "string_decoy", body: `{"content":"the \"provider\" key","a":1}`, paths: [][]string{{"provider"}}},
	{name: "key_containing_provider", body: `{"my_provider":"keep","provider":"x"}`, paths: [][]string{{"provider"}}},
	{name: "non_string_values", body: `{"provider":123,"a":true,"b":null,"c":[1]}`, paths: [][]string{{"provider"}}},
	{name: "object_value", body: `{"provider":{"a":1},"b":2}`, paths: [][]string{{"provider"}}},
	{name: "empty_containers", body: `{"a":[],"provider":"x","b":{}}`, paths: [][]string{{"provider"}}},
	{name: "deep_nesting", body: `{"a":` + strings.Repeat("[", 32) + strings.Repeat("]", 32) + `,"provider":"x","b":1}`, paths: [][]string{{"provider"}}},
	{name: "long_value", body: `{"a":"` + strings.Repeat("x", 4096) + `","provider":"x"}`, paths: [][]string{{"provider"}}},

	// No path can match: not one quoted first segment appears in the body, so
	// the mention gate short-circuits and the body must come back untouched.
	{name: "gate_short_circuit_no_key", body: `{"model":"x","a":1}`, paths: [][]string{{"provider"}, {"service_tier"}}},
	{name: "gate_short_circuit_string_decoy_only", body: `{"a":"provider is only a word here"}`, paths: [][]string{{"provider"}}},
	{name: "gate_short_circuit_array_document", body: `[{"provider":"x"}]`, paths: [][]string{{"provider"}}},
	{name: "empty_paths", body: `{"provider":"x","a":1}`, paths: nil},
}

// TestStripOnlyDeletes is the subsequence property: the output is a
// subsequence of the input, which means every output byte came from the input
// in order and nothing was rewritten. This is what rules out re-serialization
// — the change INV-INJ-03 forbids — for every corpus case at once.
func TestStripOnlyDeletes(t *testing.T) {
	for _, tc := range stripCorpus {
		t.Run(tc.name, func(t *testing.T) {
			in := []byte(tc.body)
			if !json.Valid(in) {
				t.Fatalf("fixture is not valid JSON: %s", tc.body)
			}
			for name, got := range map[string][]byte{
				"chat":      StripChatFields(in, tc.paths),
				"responses": StripResponsesFields(in, tc.paths),
			} {
				if !isSubsequence(in, got) {
					t.Fatalf("%s: output is not a subsequence of the input, so something was rewritten rather than excised.\n in  %s\n out %s", name, tc.body, got)
				}
			}
		})
	}
}

// TestStripIsIdempotent asserts a second pass over an already-stripped body is
// a byte-for-byte no-op. Every named key is gone after the first pass, so the
// second has nothing to find; if it changes anything, the first pass removed
// a byte it should not have.
func TestStripIsIdempotent(t *testing.T) {
	for _, tc := range stripCorpus {
		t.Run(tc.name, func(t *testing.T) {
			in := []byte(tc.body)
			for _, s := range []struct {
				name string
				fn   func([]byte, [][]string) []byte
			}{{"chat", StripChatFields}, {"responses", StripResponsesFields}} {
				once := s.fn(in, tc.paths)
				twice := s.fn(once, tc.paths)
				if !bytes.Equal(once, twice) {
					t.Fatalf("%s: second pass changed the body.\n once  %s\n twice %s", s.name, once, twice)
				}
			}
		})
	}
}

// TestStripNoOpReturnsSameSlice pins the pointer-identity half of INV-INJ-03
// across the corpus rather than only at the hand-written no-op cases. Equal
// bytes must be the same backing slice: the SSE relay's fast path skips work
// on the identity, so a strip that returned an equal copy would be correct in
// every byte-level assertion in the package and quietly wrong in the one
// place the relay depends on.
func TestStripNoOpReturnsSameSlice(t *testing.T) {
	for _, tc := range stripCorpus {
		t.Run(tc.name, func(t *testing.T) {
			in := []byte(tc.body)
			for _, s := range []struct {
				name string
				fn   func([]byte, [][]string) []byte
			}{{"chat", StripChatFields}, {"responses", StripResponsesFields}} {
				got := s.fn(in, tc.paths)
				if bytes.Equal(got, in) {
					sameSlice(t, s.name+" (equal bytes)", in, got)
				}
			}
		})
	}
}

// TestStripKeepsOutputValid re-checks validity per case so a failure names the
// corpus entry rather than pointing at a shared loop. The fuzz target already
// covers the general case; this keeps the corpus honest on its own.
func TestStripKeepsOutputValid(t *testing.T) {
	for _, tc := range stripCorpus {
		t.Run(tc.name, func(t *testing.T) {
			in := []byte(tc.body)
			for name, got := range map[string][]byte{
				"chat":      StripChatFields(in, tc.paths),
				"responses": StripResponsesFields(in, tc.paths),
			} {
				if !json.Valid(got) {
					t.Fatalf("%s: strip produced invalid JSON.\n in  %s\n out %s", name, tc.body, got)
				}
			}
		})
	}
}

// TestStripSurplusMemberValuesSurvive is the per-member form of the
// subsequence property. It is a stronger, differently-shaped statement: after
// the strip, the multiset of the SURVIVING members' encoded values is exactly
// the input's multiset minus the excised ones. Unlike the subsequence check it
// would catch a rewriter that preserved byte order while still dropping or
// duplicating a member, and it is the assertion that reads as "nothing
// provider-added leaked, and nothing the proxy does not own was lost".
//
// The comparison is over encoded values rather than a decoded model so a
// number like 2.50 is compared as the bytes the caller sent.
func TestStripSurplusMemberValuesSurvive(t *testing.T) {
	// Only bodies whose every top-level member value is a simple scalar, so
	// the expected survivors can be stated by hand without the test itself
	// becoming a second implementation of the traversal.
	cases := []struct {
		name     string
		body     string
		paths    [][]string
		wantOut  string
		wantChat string
		wantResp string
	}{
		{
			name:    "middle_removed",
			body:    `{"a":"1","provider":"x","b":"2"}`,
			paths:   [][]string{{"provider"}},
			wantOut: `{"a":"1","b":"2"}`,
		},
		{
			name:    "first_removed",
			body:    `{"provider":"x","a":"1","b":"2"}`,
			paths:   [][]string{{"provider"}},
			wantOut: `{"a":"1","b":"2"}`,
		},
		{
			name:    "last_removed",
			body:    `{"a":"1","b":"2","provider":"x"}`,
			paths:   [][]string{{"provider"}},
			wantOut: `{"a":"1","b":"2"}`,
		},
		{
			name:    "sole_removed_collapses",
			body:    `{"provider":"x"}`,
			paths:   [][]string{{"provider"}},
			wantOut: `{}`,
		},
		{
			name:    "two_adjacent_removed",
			body:    `{"a":"1","provider":"x","service_tier":"s","b":"2"}`,
			paths:   [][]string{{"provider"}, {"service_tier"}},
			wantOut: `{"a":"1","b":"2"}`,
		},
		{
			name:    "duplicate_keys_all_removed",
			body:    `{"a":"1","provider":"x","provider":"y","b":"2"}`,
			paths:   [][]string{{"provider"}},
			wantOut: `{"a":"1","b":"2"}`,
		},
		{
			name:    "number_bytes_untouched",
			body:    `{"a":1.0,"provider":"x","b":2.50}`,
			paths:   [][]string{{"provider"}},
			wantOut: `{"a":1.0,"b":2.50}`,
		},
		{
			// The chat scope never descends into "response", so the nested
			// service_tier is the caller's data and must survive there; the
			// Responses scope descends once and excises it. The two scopes
			// are asserted separately below.
			name:     "response_descent_keeps_outer",
			body:     `{"id":"r1","response":{"service_tier":"s","model":"m"}}`,
			paths:    [][]string{{"service_tier"}},
			wantChat: `{"id":"r1","response":{"service_tier":"s","model":"m"}}`,
			wantResp: `{"id":"r1","response":{"model":"m"}}`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			in := []byte(tc.body)
			wantChat := tc.wantOut
			if tc.wantChat != "" {
				wantChat = tc.wantChat
			}
			if got := string(StripChatFields(in, tc.paths)); got != wantChat {
				t.Fatalf("chat:\n got  %s\n want %s", got, wantChat)
			}
			// For every non-response case the two scopes agree (the descent
			// finds nothing), and the response case asserts its own
			// expectation.
			wantResp := wantChat
			if tc.wantResp != "" {
				wantResp = tc.wantResp
			}
			if got := string(StripResponsesFields(in, tc.paths)); got != wantResp {
				t.Fatalf("responses:\n got  %s\n want %s", got, wantResp)
			}
		})
	}
}

// isSubsequence reports whether every byte of sub appears in super in the same
// order, without requiring contiguity. It is the check that distinguishes
// excision from rewriting: a re-serialized payload reorders members and
// reformats numbers, so it fails here even when it stays valid JSON.
func isSubsequence(super, sub []byte) bool {
	i := 0
	for j := 0; j < len(sub); j++ {
		for i < len(super) && super[i] != sub[j] {
			i++
		}
		if i == len(super) {
			return false
		}
		i++
	}
	return true
}
