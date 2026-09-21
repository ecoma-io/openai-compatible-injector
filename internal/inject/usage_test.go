package inject

import (
	"bytes"
	"encoding/json"
	"testing"
)

// activePlan and inactivePlan are the two plans the tables run under. A fixed
// 0.75 share mirrors the config default; 0.75 × 100 = 75 exactly, so expected
// bytes stay hand-checkable.
var (
	activePlan   = ThinkingPlan{Active: true, Share: 0.75}
	inactivePlan = ThinkingPlan{}
)

// sameSlice asserts the load-bearing no-op contract: a payload the
// synthesizer does not touch comes back as the SAME backing slice, which is
// what the SSE path's pointer-identity shortcut depends on.
func sameSlice(t *testing.T, name string, in, out []byte) {
	t.Helper()
	if &out[0] != &in[0] {
		t.Fatalf("%s: no-op returned a different backing slice", name)
	}
}

func TestSynthesizeUsageUntouched(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{"no usage object", `{"id":"x","choices":[]}`},
		{"usage key inside a string value", `{"content":"the \"usage\" object is fake"}`},
		{"null usage", `{"usage":null}`},
		{"non-object usage", `{"usage":42}`},
		{"empty usage object", `{"usage":{}}`},
		{"completion missing", `{"usage":{"prompt_tokens":10}}`},
		{"completion null", `{"usage":{"completion_tokens":null}}`},
		{"completion not a number", `{"usage":{"completion_tokens":"100"}}`},
		{"completion zero", `{"usage":{"completion_tokens":0}}`},
		{"completion negative", `{"usage":{"completion_tokens":-3}}`},
		{"completion float overflow", `{"usage":{"completion_tokens":1e400}`},
		{"completion above the shareable bound", `{"usage":{"completion_tokens":9223372036854775807,"total_tokens":1}}`},
		{"upstream reasoning above zero, direct", `{"usage":{"completion_tokens":100,"reasoning_tokens":40}}`},
		{"upstream reasoning above zero, in details", `{"usage":{"completion_tokens":100,"completion_tokens_details":{"reasoning_tokens":40}}}`},
		{"upstream reasoning unreadable", `{"usage":{"completion_tokens":100,"reasoning_tokens":"many"}}`},
		{"details carrying unreadable reasoning", `{"usage":{"completion_tokens":100,"completion_tokens_details":{"reasoning_tokens":"none"}}}`},
		{"invalid json", `{"usage":{"completion_tokens":100`},
		{"empty body", ``},
		{"array document", `[{"usage":{"completion_tokens":100}}]`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for name, got := range map[string][]byte{
				"chat":      SynthesizeChatThinkingUsage([]byte(tt.body), activePlan),
				"responses": SynthesizeResponsesThinkingUsage([]byte(tt.body), activePlan),
			} {
				if !bytes.Equal(got, []byte(tt.body)) {
					t.Fatalf("%s: body should pass through untouched:\n in  %s\n out %s", name, tt.body, got)
				}
			}
		})
	}
}

// TestSynthesizeUsageInactiveIsIdentity pins the default-off guarantee at the
// pure-function level: an inactive plan returns every input as the same
// backing slice, however rich its usage object.
func TestSynthesizeUsageInactiveIsIdentity(t *testing.T) {
	for _, body := range []string{
		`{"usage":{"completion_tokens":100}}`,
		`{"usage":{"output_tokens":100,"output_tokens_details":{"reasoning_tokens":0}}}`,
		`{"response":{"usage":{"output_tokens":100}}}`,
		`not json`,
	} {
		in := []byte(body)
		for name, got := range map[string][]byte{
			"chat":      SynthesizeChatThinkingUsage(in, inactivePlan),
			"responses": SynthesizeResponsesThinkingUsage(in, inactivePlan),
		} {
			if !bytes.Equal(got, in) || &got[0] != &in[0] {
				t.Fatalf("%s: inactive plan changed the body:\n in  %s\n out %s", name, in, got)
			}
		}
	}
}

func TestSynthesizeChatUsage(t *testing.T) {
	tests := []struct {
		name string
		body string
		want string
	}{
		{
			"details absent",
			`{"id":"x","usage":{"prompt_tokens":10,"completion_tokens":100,"total_tokens":110}}`,
			`{"id":"x","usage":{"completion_tokens_details":{"reasoning_tokens":75},"prompt_tokens":10,"completion_tokens":100,"total_tokens":110}}`,
		},
		{
			"details absent, empty usage braces",
			`{"usage":{"completion_tokens":100}}`,
			`{"usage":{"completion_tokens_details":{"reasoning_tokens":75},"completion_tokens":100}}`,
		},
		{
			"details present without reasoning, siblings preserved",
			`{"usage":{"completion_tokens":100,"completion_tokens_details":{"audio_tokens":2},"total_tokens":110}}`,
			`{"usage":{"completion_tokens":100,"completion_tokens_details":{"reasoning_tokens":75,"audio_tokens":2},"total_tokens":110}}`,
		},
		{
			"details present and empty",
			`{"usage":{"completion_tokens":100,"completion_tokens_details":{}}}`,
			`{"usage":{"completion_tokens":100,"completion_tokens_details":{"reasoning_tokens":75}}}`,
		},
		{
			"details reporting explicit zero",
			`{"usage":{"completion_tokens":100,"completion_tokens_details":{"reasoning_tokens":0}}}`,
			`{"usage":{"completion_tokens":100,"completion_tokens_details":{"reasoning_tokens":75}}}`,
		},
		{
			// An explicit "no details" is filled in by replacing the value in
			// place — never by appending a duplicate key.
			"details null replaced, not duplicated",
			`{"usage":{"completion_tokens":100,"completion_tokens_details":null}}`,
			`{"usage":{"completion_tokens":100,"completion_tokens_details":{"reasoning_tokens":75}}}`,
		},
		{
			"tiny completion synthesizes zero",
			`{"usage":{"completion_tokens":10}}`,
			`{"usage":{"completion_tokens_details":{"reasoning_tokens":0},"completion_tokens":10}}`,
		},
		{
			// The insert lands directly after the usage object's opening
			// brace; every pre-existing whitespace byte stays, right where it
			// was, after the inserted member's comma.
			"whitespace inside usage preserved",
			`{ "usage" : { "completion_tokens" : 100 } }`,
			`{ "usage" : {"completion_tokens_details":{"reasoning_tokens":75}, "completion_tokens" : 100 } }`,
		},
		{
			"exponent-form completion parses",
			`{"usage":{"completion_tokens":1e2}}`,
			`{"usage":{"completion_tokens_details":{"reasoning_tokens":75},"completion_tokens":1e2}}`,
		},
		{
			"share floors, never rounds",
			`{"usage":{"completion_tokens":101}}`,
			`{"usage":{"completion_tokens_details":{"reasoning_tokens":75},"completion_tokens":101}}`,
		},
		{
			// A direct (Claude-shaped) zero does not block synthesis, and the
			// write target stays the API-native details field — the direct
			// member is read as a signal, never rewritten.
			"direct zero reasoning does not block, synthesis lands in details",
			`{"usage":{"completion_tokens":100,"reasoning_tokens":0}}`,
			`{"usage":{"completion_tokens_details":{"reasoning_tokens":75},"completion_tokens":100,"reasoning_tokens":0}}`,
		},
		{
			"chat scope: usage nested in response stays untouched",
			`{"response":{"usage":{"completion_tokens":100}}}`,
			`{"response":{"usage":{"completion_tokens":100}}}`,
		},
		{
			"usage decoy inside a string survives",
			`{"content":"she said \"usage\":{\"completion_tokens\":100} out loud","usage":{"completion_tokens":100}}`,
			`{"content":"she said \"usage\":{\"completion_tokens\":100} out loud","usage":{"completion_tokens_details":{"reasoning_tokens":75},"completion_tokens":100}}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := SynthesizeChatThinkingUsage([]byte(tt.body), activePlan)
			if string(got) != tt.want {
				t.Fatalf("chat:\n in  %s\n out %s\nwant %s", tt.body, got, tt.want)
			}
			var decoded map[string]any
			if err := json.Unmarshal(got, &decoded); err != nil {
				t.Fatalf("output is not valid JSON: %v (%s)", err, got)
			}
		})
	}
}

func TestSynthesizeResponsesUsage(t *testing.T) {
	tests := []struct {
		name string
		body string
		want string
	}{
		{
			"top-level usage uses the responses shape",
			`{"type":"response.completed","usage":{"input_tokens":10,"output_tokens":100,"total_tokens":110}}`,
			`{"type":"response.completed","usage":{"output_tokens_details":{"reasoning_tokens":75},"input_tokens":10,"output_tokens":100,"total_tokens":110}}`,
		},
		{
			"usage inside top-level response object",
			`{"type":"response.completed","response":{"id":"resp_1","usage":{"output_tokens":100,"output_tokens_details":{"reasoning_tokens":0}}}}`,
			`{"type":"response.completed","response":{"id":"resp_1","usage":{"output_tokens":100,"output_tokens_details":{"reasoning_tokens":75}}}}`,
		},
		{
			"both usages in one event are enriched",
			`{"usage":{"output_tokens":100},"response":{"usage":{"output_tokens":40,"output_tokens_details":{"reasoning_tokens":0}}}}`,
			`{"usage":{"output_tokens_details":{"reasoning_tokens":75},"output_tokens":100},"response":{"usage":{"output_tokens":40,"output_tokens_details":{"reasoning_tokens":30}}}}`,
		},
		{
			"deeper nesting stays untouched",
			`{"response":{"output":[{"wrapper":{"usage":{"output_tokens":100}}}]}}`,
			`{"response":{"output":[{"wrapper":{"usage":{"output_tokens":100}}}]}}`,
		},
		{
			"chat-shaped members are ignored in the responses shape",
			`{"usage":{"completion_tokens":100}}`,
			`{"usage":{"completion_tokens":100}}`,
		},
		{
			"tiny completion synthesizes zero",
			`{"usage":{"output_tokens":3}}`,
			`{"usage":{"output_tokens_details":{"reasoning_tokens":0},"output_tokens":3}}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := SynthesizeResponsesThinkingUsage([]byte(tt.body), activePlan)
			if string(got) != tt.want {
				t.Fatalf("responses:\n in  %s\n out %s\nwant %s", tt.body, got, tt.want)
			}
			if !json.Valid(got) {
				t.Fatalf("output is not valid JSON: %s", got)
			}
		})
	}
}

// TestSynthesizeUsageShareRange exercises the synthesized count across the
// share's full range so the floor arithmetic is pinned end to end.
func TestSynthesizeUsageShareRange(t *testing.T) {
	tests := []struct {
		share float64
		want  string
	}{
		{0, `{"usage":{"completion_tokens_details":{"reasoning_tokens":0},"completion_tokens":100}}`},
		{0.5, `{"usage":{"completion_tokens_details":{"reasoning_tokens":50},"completion_tokens":100}}`},
		{0.75, `{"usage":{"completion_tokens_details":{"reasoning_tokens":75},"completion_tokens":100}}`},
		{1, `{"usage":{"completion_tokens_details":{"reasoning_tokens":100},"completion_tokens":100}}`},
	}
	for _, tt := range tests {
		got := SynthesizeChatThinkingUsage([]byte(`{"usage":{"completion_tokens":100}}`), ThinkingPlan{Active: true, Share: tt.share})
		if string(got) != tt.want {
			t.Fatalf("share %v:\n out %s\nwant %s", tt.share, got, tt.want)
		}
	}
}

// TestSynthesizeUsageDuplicates pins duplicate-member resolution: reads take
// the last occurrence (stdlib decoder semantics), and every in-scope usage
// object is enriched — the same all-occurrences rule the model rewriters
// apply.
func TestSynthesizeUsageDuplicates(t *testing.T) {
	tests := []struct {
		name string
		body string
		want string
	}{
		{
			"duplicate completion keys, last wins",
			`{"usage":{"completion_tokens":7,"completion_tokens":100}}`,
			`{"usage":{"completion_tokens_details":{"reasoning_tokens":75},"completion_tokens":7,"completion_tokens":100}}`,
		},
		{
			"duplicate reasoning in details, last wins",
			`{"usage":{"completion_tokens":100,"completion_tokens_details":{"reasoning_tokens":40,"reasoning_tokens":0}}}`,
			`{"usage":{"completion_tokens":100,"completion_tokens_details":{"reasoning_tokens":40,"reasoning_tokens":75}}}`,
		},
		{
			"two top-level usage objects both enriched",
			`{"usage":{"completion_tokens":100},"usage":{"completion_tokens":100}}`,
			`{"usage":{"completion_tokens_details":{"reasoning_tokens":75},"completion_tokens":100},"usage":{"completion_tokens_details":{"reasoning_tokens":75},"completion_tokens":100}}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := SynthesizeChatThinkingUsage([]byte(tt.body), activePlan)
			if string(got) != tt.want {
				t.Fatalf("\n in  %s\n out %s\nwant %s", tt.body, got, tt.want)
			}
		})
	}
}

// TestSynthesizeUsageNoOpKeepsSlice pins the quiet direction the SSE relay
// depends on: a body whose synthesized value is already present, byte for
// byte, must return the input slice itself, not a fresh equal copy.
func TestSynthesizeUsageNoOpKeepsSlice(t *testing.T) {
	in := []byte(`{"usage":{"completion_tokens":100,"completion_tokens_details":{"reasoning_tokens":75}}}`)
	if got := SynthesizeChatThinkingUsage(in, activePlan); &got[0] != &in[0] {
		t.Fatalf("byte-identical no-op returned a different backing slice:\n in  %s\n out %s", in, got)
	}
	// And with no usage key at all the fast gate holds the same contract.
	in = []byte(`{"choices":[{"delta":{"content":"hi"}}]}`)
	if got := SynthesizeChatThinkingUsage(in, activePlan); &got[0] != &in[0] {
		t.Fatalf("no-usage fast gate returned a different backing slice:\n in  %s\n out %s", in, got)
	}
}
