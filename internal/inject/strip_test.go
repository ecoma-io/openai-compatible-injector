package inject

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

// stripPaths is the shared table helper: []string dotted paths →
// [][]string segment lists, the shape the engine takes (config already
// parsed and validated these; the table avoids depending on config).
func stripPaths(raw ...string) [][]string {
	out := make([][]string, 0, len(raw))
	for _, p := range raw {
		out = append(out, splitStripPath(p))
	}
	return out
}

// splitStripPath splits a dotted path with the single-quoted segment spelling
// config.ParseStripPath accepts: 'a.b'.c → ["a.b","c"], 'it”s' → ["it's"],
// unquoted segments split on every dot. Tests trust the inputs; config owns
// validation.
func splitStripPath(s string) []string {
	var segs []string
	for i := 0; i < len(s); {
		switch s[i] {
		case '.':
			i++
		case '\'':
			j := i + 1
			var b strings.Builder
			for j < len(s) {
				if s[j] == '\'' {
					if j+1 < len(s) && s[j+1] == '\'' {
						b.WriteByte('\'')
						j += 2
						continue
					}
					break
				}
				b.WriteByte(s[j])
				j++
			}
			segs = append(segs, b.String())
			i = j + 1
		default:
			j := i
			for j < len(s) && s[j] != '.' && s[j] != '\'' {
				j++
			}
			segs = append(segs, s[i:j])
			i = j
		}
	}
	return segs
}

func TestStripChatFields(t *testing.T) {
	tests := []struct {
		name  string
		body  string
		paths []string
		want  string
	}{
		{
			name:  "single first member",
			body:  `{"provider":"kilo","model":"x"}`,
			paths: []string{"provider"},
			want:  `{"model":"x"}`,
		},
		{
			name:  "middle member",
			body:  `{"model":"x","provider":"kilo","service_tier":"std","choices":[]}`,
			paths: []string{"provider"},
			want:  `{"model":"x","service_tier":"std","choices":[]}`,
		},
		{
			name:  "last member",
			body:  `{"model":"x","choices":[],"service_tier":"std"}`,
			paths: []string{"service_tier"},
			want:  `{"model":"x","choices":[]}`,
		},
		{
			name:  "sole member",
			body:  `{"service_tier":"std"}`,
			paths: []string{"service_tier"},
			want:  `{}`,
		},
		{
			name:  "kilo shape, both junk members in one shot",
			body:  `{"id":"chatcmpl-9","model":"x","provider":"kilo","service_tier":"std","choices":[{"index":0,"delta":{"content":"hi"}}],"usage":{"prompt_tokens":5}}`,
			paths: []string{"provider", "service_tier"},
			want:  `{"id":"chatcmpl-9","model":"x","choices":[{"index":0,"delta":{"content":"hi"}}],"usage":{"prompt_tokens":5}}`,
		},
		{
			name:  "ordered heavy whitespace preserved",
			body:  `{ "provider" : "kilo" , "model" : "x" }`,
			paths: []string{"provider"},
			// Only the member's own bytes are removed (key `:` value `,`); the
			// surrounding whitespace is not the member's and survives, so the
			// two spaces flanking the excised member stay next to each other.
			want: `{  "model" : "x" }`,
		},
		{
			name:  "whitespace-heavy framing",
			body:  "{\n\t\"provider\": \"kilo\",\n\t\"model\"\n\t:\n\"x\"\n}\n",
			paths: []string{"provider"},
			want:  "{\n\t\n\t\"model\"\n\t:\n\"x\"\n}\n",
		},
		{
			name:  "nested path through an object",
			body:  `{"a":{"provider":"kilo"},"provider":"y","model":"x"}`,
			paths: []string{"a.provider"},
			want:  `{"a":{},"provider":"y","model":"x"}`,
		},
		{
			name:  "nested path leaves the parent intact",
			body:  `{"meta":{"provider":"kilo","kind":"x"},"model":"x"}`,
			paths: []string{"meta.provider"},
			want:  `{"meta":{"kind":"x"},"model":"x"}`,
		},
		{
			name:  "deep path",
			body:  `{"a":{"b":{"provider":"kilo","keep":1},"keep":2}}`,
			paths: []string{"a.b.provider"},
			want:  `{"a":{"b":{"keep":1},"keep":2}}`,
		},
		{
			name:  "path does not descend through an array",
			body:  `{"choices":[{"provider":"kilo"}],"model":"x"}`,
			paths: []string{"choices.provider"},
			want:  `{"choices":[{"provider":"kilo"}],"model":"x"}`,
		},
		{
			name:  "array value of a non-terminal segment is skipped",
			body:  `{"a":[{"provider":"kilo"}],"model":"x"}`,
			paths: []string{"a.provider"},
			want:  `{"a":[{"provider":"kilo"}],"model":"x"}`,
		},
		{
			name:  "scalar value of a non-terminal segment is skipped",
			body:  `{"a":5,"model":"x","b":{"provider":"kilo"}}`,
			paths: []string{"a.provider", "b.provider"},
			want:  `{"a":5,"model":"x","b":{}}`,
		},
		{
			name:  "duplicate keys all excised",
			body:  `{"provider":"a","provider":"b","model":"x"}`,
			paths: []string{"provider"},
			want:  `{"model":"x"}`,
		},
		{
			name:  "duplicate nested keys all excised",
			body:  `{"a":{"provider":"p1","provider":"p2"},"model":"x"}`,
			paths: []string{"a.provider"},
			want:  `{"a":{},"model":"x"}`,
		},
		{
			name:  "quoted segment spelling",
			body:  `{"parent name":{"child":"v"},"model":"x"}`,
			paths: []string{"'parent name'.child"},
			want:  `{"parent name":{},"model":"x"}`,
		},
		{
			name:  "escaped quote in segment",
			body:  `{"it's":{"x":1},"model":"x"}`,
			paths: []string{"'it''s'.x"},
			want:  `{"it's":{},"model":"x"}`,
		},
		{
			name:  "quoted dot segment is one key",
			body:  `{"a.b":{"c":1},"model":"x"}`,
			paths: []string{"'a.b'.c"},
			want:  `{"a.b":{},"model":"x"}`,
		},
		{
			name:  "the dotted spelling does not re-split a quoted key",
			body:  `{"a.b":{"c":1},"model":"x"}`,
			paths: []string{"a.b.c"},
			want:  `{"a.b":{"c":1},"model":"x"}`,
		},
		{
			name:  "string value containing the key is untouched",
			body:  `{"content":"the provider said so","provider":"kilo"}`,
			paths: []string{"provider"},
			want:  `{"content":"the provider said so"}`,
		},
		{
			name:  "prefix path and the path itself",
			body:  `{"a":{"provider":"kilo","b":1},"provider":"x"}`,
			paths: []string{"a", "a.provider"},
			want:  `{"provider":"x"}`,
		},
		{
			name:  "unrelated decoy key",
			body:  `{"provider":"kilo","x":{"provider":"other"},"model":"x"}`,
			paths: []string{"provider"},
			want:  `{"x":{"provider":"other"},"model":"x"}`,
		},
		{
			name:  "nested-only path with a top-level decoy",
			body:  `{"provider":"kilo","meta":{"provider":"inner"},"model":"x"}`,
			paths: []string{"meta.provider"},
			want:  `{"provider":"kilo","meta":{},"model":"x"}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := string(StripChatFields([]byte(tt.body), stripPaths(tt.paths...)))
			if got != tt.want {
				t.Fatalf("StripChatFields(%s, %v) =\n  %s\nwant\n  %s", tt.body, tt.paths, got, tt.want)
			}
		})
	}
}

func TestStripResponsesFields(t *testing.T) {
	tests := []struct {
		name  string
		body  string
		paths []string
		want  string
	}{
		{
			name:  "envelope descent",
			body:  `{"type":"response.completed","response":{"id":"r","service_tier":"std","model":"x"},"sequence_number":1}`,
			paths: []string{"service_tier"},
			want:  `{"type":"response.completed","response":{"id":"r","model":"x"},"sequence_number":1}`,
		},
		{
			name:  "envelope descent with a two-segment path",
			body:  `{"type":"response.completed","response":{"meta":{"provider":"kilo"},"id":"r"}}`,
			paths: []string{"meta.provider"},
			want:  `{"type":"response.completed","response":{"meta":{},"id":"r"}}`,
		},
		{
			name:  "response.completed envelope carries the service_tier",
			body:  `{"type":"response.completed","response":{"service_tier":"std","id":"r"}}`,
			paths: []string{"service_tier"},
			want:  `{"type":"response.completed","response":{"id":"r"}}`,
		},
		{
			name:  "top level stripped too",
			body:  `{"provider":"kilo","response":{"service_tier":"std","id":"r"}}`,
			paths: []string{"provider", "service_tier"},
			want:  `{"response":{"id":"r"}}`,
		},
		{
			name:  "duplicate response objects both descended",
			body:  `{"response":{"service_tier":"a","id":"1"},"response":{"service_tier":"b","id":"2"}}`,
			paths: []string{"service_tier"},
			want:  `{"response":{"id":"1"},"response":{"id":"2"}}`,
		},
		{
			name:  "quoted dot segment named a.b stays intact",
			body:  `{"response":{"a.b":{"provider":"kilo"},"id":"r"}}`,
			paths: []string{"'a.b'.provider"},
			want:  `{"response":{"a.b":{},"id":"r"}}`,
		},
		{
			// Inside the descended envelope the full path list applies again
			// from its first segment — the same rule the model/usage
			// rewriters' descents give. A path just deeper than that (three
			// segments to a match below the envelope) is where the scope ends:
			name:  "two-segment path inside the descended envelope",
			body:  `{"response":{"wrapper":{"service_tier":"std"},"id":"r"}}`,
			paths: []string{"wrapper.service_tier"},
			want:  `{"response":{"wrapper":{},"id":"r"}}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := string(StripResponsesFields([]byte(tt.body), stripPaths(tt.paths...)))
			want := tt.want
			if want == "" {
				// Table row marked "" means: must differ from chat (the
				// descent is doing the work).
				chat := string(StripChatFields([]byte(tt.body), stripPaths(tt.paths...)))
				if chat == got {
					t.Fatalf("responses did not descend where chat does not: %s", got)
				}
			}
			if got != want {
				t.Fatalf("StripResponsesFields(%s, %v) =\n  %s\nwant\n  %s", tt.body, tt.paths, got, want)
			}
		})
	}
}

func TestStripFieldsUntouched(t *testing.T) {
	bodies := []string{
		``,                      // empty body
		`{"model":"x"}`,         // no matchable key
		`[{"provider":"kilo"}]`, // array document: no object in scope
		`"just a string"`,       // string document
		`42`,                    // number document
		`{"provider":`,          // truncated JSON
		`not json`,              // garbage
		`{"content":"the provider"}`,
		`{"a":{"provider":"x"}}`, // nested key, path at top level
	}
	for _, body := range bodies {
		t.Run(body, func(t *testing.T) {
			in := []byte(body)
			for name, got := range map[string][]byte{
				"chat":      StripChatFields(in, stripPaths("provider", "service_tier")),
				"responses": StripResponsesFields(in, stripPaths("provider", "service_tier")),
				"no paths":  StripChatFields(in, nil),
			} {
				if !bytes.Equal(got, in) {
					t.Fatalf("%s: body should pass through untouched:\n in  %s\n out %s", name, in, got)
				}
				sameSlice(t, name, in, got)
			}
		})
	}
}

func TestStripFieldsNoPathsIsIdentity(t *testing.T) {
	for _, body := range []string{
		`{"provider":"kilo","service_tier":"std"}`,
		`{"response":{"service_tier":"std"}}`,
		`{"provider":"a","provider":"b"}`,
	} {
		in := []byte(body)
		if got := StripChatFields(in, nil); !bytes.Equal(got, in) {
			t.Fatalf("nil paths mutated a strippable body:\n in  %s\n out %s", in, got)
		}
		sameSlice(t, "chat-no-paths", in, StripChatFields(in, nil))
		sameSlice(t, "resp-no-paths", in, StripResponsesFields(in, nil))
	}
}

func TestStripPatterns(t *testing.T) {
	if got := StripPatterns(nil); got != nil {
		t.Fatalf("StripPatterns(nil) = %v, want nil", got)
	}
	if got := StripPatterns([][]string{}); got != nil {
		t.Fatalf("StripPatterns([]) = %v, want nil", got)
	}
	if got := StripPatterns([][]string{{}}); got != nil {
		t.Fatalf("StripPatterns([[]]) = %v, want nil", got)
	}
	got := StripPatterns(stripPaths("provider", "'parent name'.child", "meta.kind"))
	want := [][]byte{[]byte(`"provider"`), []byte(`"parent name"`), []byte(`"meta"`)}
	if len(got) != len(want) {
		t.Fatalf("StripPatterns = %q, want %q", got, want)
	}
	for i := range want {
		if string(got[i]) != string(want[i]) {
			t.Fatalf("StripPatterns = %q, want %q", got, want)
		}
	}
	// First-segment dedup: two paths through the same first key gate once.
	dedup := StripPatterns(stripPaths("a.b", "a.c"))
	if len(dedup) != 1 || string(dedup[0]) != `"a"` {
		t.Fatalf("StripPatterns did not deduplicate first segments: %q", dedup)
	}
}

// TestStripValidJSONAlwaysValid pins the strongest contract: for any valid
// JSON payload the strip never produces invalid JSON, whichever paths and
// scope are in play. The input/output byte comparison is the table's
// business; here the scan's structural integrity is the contract.
func TestStripValidJSONAlwaysValid(t *testing.T) {
	inputs := []string{
		`{"provider":"kilo","model":"x","service_tier":"std"}`,
		`{ "provider" : { "a" : [1,2], "b" : {"provider":true} }, "model" : [true,false,null] }`,
		`{"a":{"b":{"c":{"d":{"provider":null}}}}}`,
		`{"provider":"x","provider":123,"provider":true}`,
		`{"provider":"..."}`,
		`{"response":{"provider":{"provider":[{"provider":{}}]}}}`, // responses scope, deep nesting
		`[{},{},{"provider":1}]`,
		`{"usage":{"completion_tokens":100},"provider":"kilo"}`,
	}
	var combos []struct {
		name      string
		strip     func([]byte, [][]string) []byte
		paths     []string
		descended bool
	}
	for _, p := range [][]string{{"provider"}, {"service_tier"}, {"a"}, {"a.b"}, {"a.b.c.d"}, {"a.provider"}, {"response"}, {"response.provider"}} {
		combos = append(combos,
			struct {
				name      string
				strip     func([]byte, [][]string) []byte
				paths     []string
				descended bool
			}{"chat", StripChatFields, p, false},
			struct {
				name      string
				strip     func([]byte, [][]string) []byte
				paths     []string
				descended bool
			}{"responses", StripResponsesFields, p, true},
		)
	}
	for _, input := range inputs {
		for _, c := range combos {
			out := c.strip([]byte(input), stripPaths(c.paths...))
			if !json.Valid(out) {
				t.Fatalf("%s strip(%s, %v) produced invalid JSON:\n  out %q", c.name, input, c.paths, out)
			}
		}
	}
}
