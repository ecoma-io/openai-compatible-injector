package inject

import (
	"bytes"
	"slices"
	"strings"
	"testing"

	"openai-compatible-injector/internal/config"
)

// The guard on config's reserved strip set. The strip runs LAST in the
// composed rewriter, so a reserved path that no longer reaches proxy-written
// bytes is worse than useless: the rejection still reads like protection
// while the operator's list quietly carves nothing, and a writer that moves a
// member leaves the entry stale with no signal.
//
// This drives every reserved entry against bodies the REAL writers just
// produced and asserts the strip still excises a member only the proxy could
// have written. Each fixture carries no provider data at any reserved
// location, so its marker has exactly one possible source — the writer — and
// an excision is proof rather than a coincidence of provider data. It runs
// the other direction too: a reserved entry this test cannot account for
// fails, so the set cannot grow silently into decoration.

// The bodies the writers run over: the Responses one carries the model and
// the usage object twice, once top-level and once inside the envelope, which
// is precisely where the two scopes write.
const (
	preChat      = `{"model":"upstream-x","usage":{"completion_tokens":100},"choices":[]}`
	preResponses = `{"model":"upstream-x","usage":{"output_tokens":100},` +
		`"response":{"model":"upstream-x","usage":{"output_tokens":100},"choices":[]}}`
)

const (
	// proxyValue is the renamed model both model writers splice in.
	proxyValue = "public-x"
	// proxyReasoning is the synthesized count itself: floor(0.75 × 100), a
	// number only the proxy computes. Neither body above reports reasoning
	// tokens, so this byte run has exactly one possible source, and it is the
	// marker for every usage case — an excised leaf leaves an empty parent
	// object behind, so the parent's key would prove nothing.
	proxyReasoning = `"reasoning_tokens":75`
)

// chatWritten and responsesWritten are the composed rewriter's output before
// the strip runs: the model rename then the thinking-usage synthesis, the two
// writers whose members the reserved set protects. The strip itself is not
// applied — that step is what this test drives.
func chatWritten() []byte {
	out := RewriteChatModel([]byte(preChat), proxyValue)
	return SynthesizeChatThinkingUsage(out, ThinkingPlan{Active: true, Share: 0.75})
}

func responsesWritten() []byte {
	out := RewriteResponsesModel([]byte(preResponses), proxyValue)
	return SynthesizeResponsesThinkingUsage(out, ThinkingPlan{Active: true, Share: 0.75})
}

// guardCase proves one reserved path against one written body. want is how
// many occurrences of marker must survive the strip.
//
// The counts encode the two mechanics that make the reserved set a union: a
// bare spelling also fires inside the envelope (the Responses strip
// reapplies the whole list there), so "model" and "usage" remove their member
// TWICE and leave none, while their "response"-prefixed spellings reach one
// scope only.
type guardCase struct {
	name   string
	path   string
	chat   bool   // true: strip under the Chat scope; false: Responses
	marker string // the proxy-written bytes the strip must remove
	want   int
}

func TestReservedStripPathsCoverProxyWrites(t *testing.T) {
	cases := []guardCase{
		// The rename.
		{"model (chat)", "model", true, proxyValue, 0},
		{"model (responses, both scopes)", "model", false, proxyValue, 0},
		{"response.model", "response.model", false, proxyValue, 1},
		// The Chat synthesis leaf, alone and through the whole usage object.
		{"usage.completion_tokens_details.reasoning_tokens",
			"usage.completion_tokens_details.reasoning_tokens", true, proxyReasoning, 0},
		{"usage (chat)", "usage", true, proxyReasoning, 0},
		// The Responses synthesis leaf, written to both usage objects.
		{"usage.output_tokens_details.reasoning_tokens (both scopes)",
			"usage.output_tokens_details.reasoning_tokens", false, proxyReasoning, 0},
		{"response.usage.output_tokens_details.reasoning_tokens",
			"response.usage.output_tokens_details.reasoning_tokens", false, proxyReasoning, 1},
		{"usage (responses, both scopes)", "usage", false, proxyReasoning, 0},
		{"response.usage", "response.usage", false, proxyReasoning, 1},
	}

	// Reserved entries with no writer to prove them. This is the envelope
	// spelling of the CHAT synthesis leaf: reaching
	// "response.usage.completion_tokens_details" needs a route that both
	// descends into the envelope and speaks the Chat shape, and no single
	// route does. It stays reserved for uniformity — four proxy-owned members
	// in two spellings each — and that is a deliberate exception, named here
	// so it cannot quietly become the norm.
	uniformityOnly := map[string]bool{
		"response.usage.completion_tokens_details.reasoning_tokens": true,
	}

	reserved := config.ReservedStripPaths()
	covered := make(map[string]bool, len(cases))
	for _, tc := range cases {
		covered[tc.path] = true
	}

	// Direction one: every reserved entry is either proved below or declared.
	// A writer that adds a spelling lands here rather than passing quietly.
	for _, entry := range reserved {
		path := strings.Join(entry, ".")
		if !covered[path] && !uniformityOnly[path] {
			t.Errorf("reserved path %q has no coverage case: add one, or declare it in "+
				"uniformityOnly with the reason it reaches nothing the proxy writes", path)
		}
		if _, err := config.ParseStripPath(path); err == nil {
			t.Errorf("config.ParseStripPath(%q) accepted a reserved path", path)
		} else if !strings.Contains(err.Error(), path) {
			t.Errorf("rejection for %q does not name it: %v", path, err)
		}
	}
	// Direction two: a case for a path config no longer reserves means the
	// guard and the set have drifted apart.
	for path := range covered {
		if !isReserved(reserved, path) {
			t.Errorf("coverage case %q is not in the reserved set", path)
		}
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			segments := strings.Split(tc.path, ".")
			var (
				before, after []byte
			)
			if tc.chat {
				before = chatWritten()
				after = StripChatFields(before, [][]string{segments})
			} else {
				before = responsesWritten()
				after = StripResponsesFields(before, [][]string{segments})
			}
			if got := bytes.Count(before, []byte(tc.marker)); got == tc.want {
				t.Fatalf("fixture is broken: %s occurs %d times before the strip, want more than %d",
					tc.marker, got, tc.want)
			}
			if got := bytes.Count(after, []byte(tc.marker)); got != tc.want {
				t.Fatalf("stripping %q left %d occurrences of %s, want %d:\n before %s\n after  %s",
					tc.path, got, tc.marker, tc.want, before, after)
			}
		})
	}
}

// isReserved is the membership test over the exported set, so the guard can
// check a coverage case from this side.
func isReserved(reserved [][]string, path string) bool {
	segments := strings.Split(path, ".")
	for _, entry := range reserved {
		if slices.Equal(entry, segments) {
			return true
		}
	}
	return false
}
