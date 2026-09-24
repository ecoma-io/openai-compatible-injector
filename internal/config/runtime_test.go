package config

import (
	"fmt"
	"strings"
	"testing"
)

func validRuntime() string {
	return `
api-key: unit-test-key
models:
  gpt-reviewer:
    endpoint: https://api.provider.example/v1
    upstream-model: gpt-5-pro
    injection-prompt: |
      Review this code for bugs.
`
}

func mustSnapshot(t *testing.T, data string) *Snapshot {
	t.Helper()
	s, err := LoadRuntime([]byte(data))
	if err != nil {
		t.Fatalf("LoadRuntime: %v", err)
	}
	return s
}

func TestLoadRuntimeValid(t *testing.T) {
	s := mustSnapshot(t, validRuntime())
	if s.Gen() != 0 {
		t.Errorf("initial Gen() = %d, want 0", s.Gen())
	}
	m, ok := s.Model("gpt-reviewer")
	if !ok {
		t.Fatal("model gpt-reviewer not found")
	}
	if m.Public != "gpt-reviewer" || m.UpstreamModel != "gpt-5-pro" {
		t.Errorf("model = %+v", m)
	}
	if m.Endpoint.Scheme != "https" || m.Endpoint.Host != "api.provider.example" || m.Endpoint.Path != "/v1" {
		t.Errorf("endpoint = %s", m.Endpoint)
	}
	if strings.TrimSpace(m.InjectionPrompt) != "Review this code for bugs." {
		t.Errorf("prompt = %q", m.InjectionPrompt)
	}

	mods := s.Models()
	if len(mods) != 1 || mods["gpt-reviewer"].Public != "gpt-reviewer" {
		t.Errorf("Models() = %+v", mods)
	}
	if _, ok := s.Model("nope"); ok {
		t.Error("unknown model resolved as present")
	}
}

func TestLoadRuntimeInvalidYAML(t *testing.T) {
	if _, err := LoadRuntime([]byte("models: [unclosed")); err == nil {
		t.Error("expected parse error")
	}
}

func TestLoadRuntimeRejectsBootstrapKeys(t *testing.T) {
	// The critical plane rule: runtime YAML must never define bootstrap keys.
	cases := []struct {
		name string
		yaml string
	}{
		{"listen", "listen: :8081\nmodels: {}\n"},
		{"config", "config: {}\nmodels: {}\n"},
		{"poll", "poll-interval: 1s\nmodels: {}\n"},
		{"shutdown", "shutdown-grace: 1s\nmodels: {}\n"},
		{"usage database", "usage-database-url: postgresql://operator:secret@db/usage\nmodels: {}\n"},
		{"unknown key", "bananas: true\nmodels: {}\n"},
		{"bad model key", "models:\n  a:\n    listen: :8080\n    endpoint: https://h/v1\n    upstream-model: m\n"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := LoadRuntime([]byte(tc.yaml)); err == nil {
				t.Fatalf("expected strict-decode rejection of %q", tc.name)
			}
		})
	}
}

func TestLoadRuntimeModelValidation(t *testing.T) {
	cases := []struct {
		name string
		yaml string
		want string
	}{
		{"missing endpoint", "models:\n  a:\n    upstream-model: m\n", "endpoint or provider is required"},
		{"non-http endpoint", "models:\n  a:\n    endpoint: ftp://h\n    upstream-model: m\n", "http(s) with a host"},
		{"hostless endpoint", "models:\n  a:\n    endpoint: https://\n    upstream-model: m\n", "http(s) with a host"},
		{"credentials in endpoint", "models:\n  a:\n    endpoint: https://user:pass@h/v1\n    upstream-model: m\n", "credentials"},
		{"fragment in endpoint", "models:\n  a:\n    endpoint: https://h/v1#page\n    upstream-model: m\n", "fragment"},
		{"missing upstream model", "models:\n  a:\n    endpoint: https://h/v1\n", "upstream-model is required"},
		{"empty model name", "models:\n  \"  \":\n    endpoint: https://h/v1\n    upstream-model: m\n", "empty"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := LoadRuntime([]byte(tc.yaml))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want containing %q", err, tc.want)
			}
		})
	}
}

// TestConfigErrorsDoNotEmbedRawEndpoint pins the credential rule on the
// config plane: validation error text reaches logs verbatim, so it must
// never quote an endpoint's query string or other secret-bearing parts.
func TestConfigErrorsDoNotEmbedRawEndpoint(t *testing.T) {
	cases := []struct {
		name string
		yaml string
	}{
		{"bad scheme with secret query", "models:\n  a:\n    endpoint: ftp://h/v1?api-key=SECRET_ENDPOINT_TOKEN\n    upstream-model: m\n"},
		{"unparseable URL with secret query", "models:\n  a:\n    endpoint: \"ht tp://h/v1?api-key=SECRET_ENDPOINT_TOKEN\"\n    upstream-model: m\n"},
		{"missing host with secret query", "models:\n  a:\n    endpoint: \"/v1?api-key=SECRET_ENDPOINT_TOKEN\"\n    upstream-model: m\n"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := LoadRuntime([]byte(tc.yaml))
			if err == nil {
				t.Fatal("expected rejection")
			}
			if strings.Contains(err.Error(), "SECRET_ENDPOINT_TOKEN") {
				t.Errorf("error text embeds the endpoint query (secret leak): %v", err)
			}
			t.Logf("error text: %v", err)
		})
	}
}

// TestEndpointParseErrorDoesNotQuoteRawBytes pins the two stdlib parse
// causes that quote raw bytes of the endpoint input — the offending escape
// sequence and the rejected host byte (url.EscapeError, url.InvalidHostError).
// Rejection text reaches logs verbatim, so these arrive as the static
// classification instead, mirroring the proxy's sanitizeUpstreamError.
// The port failure keeps its text: the authority substring it quotes is
// inside the scheme+host disclosure set the config plane allows.
func TestEndpointParseErrorDoesNotQuoteRawBytes(t *testing.T) {
	cases := []struct {
		name        string
		endpoint    string
		want        string
		mustNotHave string
	}{
		{"bad escape quotes no bytes", "https://h/v1%zz?api-key=SECRET", "invalid URL escape", "%zz"},
		{"invalid host quotes no bytes", `"https://exa mple.com/v1?api-key=SECRET"`, "invalid host", "exa mple"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := LoadRuntime([]byte("models:\n  a:\n    endpoint: " + tc.endpoint + "\n    upstream-model: m\n"))
			if err == nil {
				t.Fatal("expected rejection")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want containing %q", err, tc.want)
			}
			if strings.Contains(err.Error(), tc.mustNotHave) {
				t.Errorf("error text quotes raw input bytes: %v", err)
			}
		})
	}
}

func TestLoadRuntimeRejectsEmptyModels(t *testing.T) {
	// Zero models serves nothing — worse, an empty file is what a
	// truncate-then-write config edit looks like mid-write. Accepting it
	// would silently drop every model from the live service; rejecting it
	// routes the reload onto the last-known-good path.
	if _, err := LoadRuntime([]byte("models: {}\n")); err == nil {
		t.Fatal("expected rejection of empty models table")
	}
	if _, err := LoadRuntime([]byte("\n")); err == nil {
		t.Fatal("expected rejection of empty file")
	}
}

// TestLoadRuntimeRequiresAPIKey pins the fail-closed rule: a runtime file
// without a usable api-key never becomes a snapshot — at boot that refuses
// to start, on reload it keeps the last-known-good key serving. Absent,
// null, empty, and whitespace-only are the same rejection, and the trimmed
// value is exactly what the auth gate compares against.
func TestLoadRuntimeRequiresAPIKey(t *testing.T) {
	rejects := []struct {
		name string
		yaml string
	}{
		{"absent", "models:\n  a:\n    endpoint: https://h/v1\n    upstream-model: m\n"},
		{"empty", "api-key: \"\"\nmodels:\n  a:\n    endpoint: https://h/v1\n    upstream-model: m\n"},
		{"null", "api-key:\nmodels:\n  a:\n    endpoint: https://h/v1\n    upstream-model: m\n"},
		{"whitespace only", "api-key: \"   \"\nmodels:\n  a:\n    endpoint: https://h/v1\n    upstream-model: m\n"},
		{"embedded space", "api-key: \"unit test key\"\nmodels:\n  a:\n    endpoint: https://h/v1\n    upstream-model: m\n"},
		{"embedded tab", "api-key: \"unit\\ttest-key\"\nmodels:\n  a:\n    endpoint: https://h/v1\n    upstream-model: m\n"},
		{"leading tab", "api-key: \"\\tunit-test-key\"\nmodels:\n  a:\n    endpoint: https://h/v1\n    upstream-model: m\n"},
		{"trailing newline", "api-key: \"unit-test-key\\n\"\nmodels:\n  a:\n    endpoint: https://h/v1\n    upstream-model: m\n"},
		{"disallowed character", "api-key: \"unit:key\"\nmodels:\n  a:\n    endpoint: https://h/v1\n    upstream-model: m\n"},
		{"padding before token end", "api-key: \"unit=test\"\nmodels:\n  a:\n    endpoint: https://h/v1\n    upstream-model: m\n"},
		{"only padding", "api-key: \"===\"\nmodels:\n  a:\n    endpoint: https://h/v1\n    upstream-model: m\n"},
		{"too long", "api-key: \"" + strings.Repeat("a", maxBearerTokenBytes+1) + "\"\nmodels:\n  a:\n    endpoint: https://h/v1\n    upstream-model: m\n"},
	}
	for _, tc := range rejects {
		t.Run(tc.name, func(t *testing.T) {
			_, err := LoadRuntime([]byte(tc.yaml))
			if err == nil {
				t.Fatal("expected rejection")
			}
			want := "api-key is required"
			if tc.name == "embedded space" || tc.name == "embedded tab" || tc.name == "leading tab" || tc.name == "trailing newline" || tc.name == "disallowed character" || tc.name == "padding before token end" || tc.name == "only padding" || tc.name == "too long" {
				want = "api-key must be a valid bearer token"
			}
			if !strings.Contains(err.Error(), want) {
				t.Fatalf("err = %v, want containing %q", err, want)
			}
		})
	}

	s := mustSnapshot(t, "api-key: \"  unit-test-key  \"\nmodels:\n  a:\n    endpoint: https://h/v1\n    upstream-model: m\n")
	if got := s.APIKey(); got != "unit-test-key" {
		t.Fatalf("APIKey() = %q, want the trimmed value", got)
	}
	// The key is a legal top-level citizen: alongside models and log-level
	// it loads fine.
	if _, err := LoadRuntime([]byte("api-key: unit-test-key\nlog-level: debug\nmodels:\n  a:\n    endpoint: https://h/v1\n    upstream-model: m\n")); err != nil {
		t.Fatalf("LoadRuntime rejected a well-formed api-key: %v", err)
	}
}

func TestSnapshotImmutableBetweenPublishes(t *testing.T) {
	// Publishers hand over fresh snapshots; a published snapshot must not
	// alias the previous one (the map is not shared).
	s1 := mustSnapshot(t, validRuntime())
	s2 := mustSnapshot(t, `
api-key: unit-test-key
models:
  other:
    endpoint: http://localhost:9000/v1
    upstream-model: m2
`)
	if len(s1.Models()) != 1 {
		t.Fatalf("s1 = %+v", s1.Models())
	}
	if _, ok := s2.Model("gpt-reviewer"); ok {
		t.Fatal("s2 leaked s1's models")
	}
}

func TestStorePublishGenerations(t *testing.T) {
	store := NewStore(mustSnapshot(t, validRuntime()))
	if store.Gen() != 0 {
		t.Fatalf("initial Gen = %d, want 0", store.Gen())
	}

	next := mustSnapshot(t, validRuntime())
	if err := store.Publish(next); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if store.Gen() != 1 {
		t.Errorf("Gen after first publish = %d, want 1", store.Gen())
	}
	// Active snapshot generation follows the store generation.
	if got := store.Load().Gen(); got != 1 {
		t.Errorf("active Gen = %d, want 1", got)
	}

	if err := store.Publish(mustSnapshot(t, validRuntime())); err != nil {
		t.Fatalf("Publish 2: %v", err)
	}
	if store.Gen() != 2 {
		t.Errorf("Gen after second publish = %d, want 2", store.Gen())
	}

	// Old snapshot pointer remains valid and immutable (used by in-flight streams).
	old := store.Load()
	if err := store.Publish(mustSnapshot(t, validRuntime())); err != nil {
		t.Fatalf("Publish 3: %v", err)
	}
	if got := old.Gen(); got != 2 {
		t.Errorf("retained snapshot Gen = %d, want 2", got)
	}
}

func TestStorePublishNil(t *testing.T) {
	store := NewStore(mustSnapshot(t, validRuntime()))
	if err := store.Publish(nil); err == nil {
		t.Fatal("expected error publishing nil snapshot")
	}
	if store.Gen() != 0 {
		t.Errorf("nil publish must not change generation, got %d", store.Gen())
	}
}

func TestLoadRuntimePreservesModelNameCase(t *testing.T) {
	// viper's map normalization lowercases every key; public model names
	// must reach clients exactly as configured. A configured `MyModel` is
	// reachable as `MyModel` and nothing else.
	s := mustSnapshot(t, `
api-key: unit-test-key
models:
  MyModel:
    endpoint: http://localhost:9000/v1
    upstream-model: m
  mymodel:
    endpoint: http://localhost:9001/v1
    upstream-model: m2
`)
	if got, ok := s.Model("MyModel"); !ok {
		t.Fatal("MyModel not reachable under its configured name")
	} else if got.Endpoint.Host != "localhost:9000" {
		t.Fatalf("MyModel resolved to the wrong entry: %+v", got)
	}
	// The lowercase twin has its own entry: a case-folding decoder (viper)
	// would collapse both keys into one entry and both lookups would land
	// on the same endpoint.
	if got, ok := s.Model("mymodel"); !ok {
		t.Fatal("mymodel not reachable under its configured name")
	} else if got.Endpoint.Host != "localhost:9001" {
		t.Fatalf("mymodel collapsed into MyModel's entry: %+v", got)
	}
}

func TestLoadRuntimeAcceptsDottedModelName(t *testing.T) {
	// viper flattens dotted keys (`gpt-3.5-turbo` -> gpt-3 -> 5-turbo) and
	// then rejects the leftover as an unknown key; a dotted name is legal
	// YAML and must load.
	s := mustSnapshot(t, `
api-key: unit-test-key
models:
  gpt-3.5-turbo:
    endpoint: http://localhost:9000/v1
    upstream-model: m
`)
	if m, ok := s.Model("gpt-3.5-turbo"); !ok {
		t.Fatal("dotted model name not reachable under its configured name")
	} else if m.Endpoint.Host != "localhost:9000" {
		t.Fatalf("dotted model resolved to the wrong entry: %+v", m)
	}
}

func TestLoadRuntimeRejectsUppercaseEntryKeys(t *testing.T) {
	// Entry keys are matched case-sensitively: ENDPOINT is not endpoint.
	if _, err := LoadRuntime([]byte(`
models:
  a:
    ENDPOINT: http://localhost:9000/v1
    upstream-model: m
`)); err == nil {
		t.Fatal("expected rejection of uppercase entry key")
	}
}

func TestLoadRuntimeRejectsTrimCollision(t *testing.T) {
	// `  a` and `a` are distinct YAML keys that trim to the same model
	// name; which one wins must not depend on map iteration order.
	if _, err := LoadRuntime([]byte(`
models:
  "  a":
    endpoint: http://localhost:9000/v1
    upstream-model: m
  a:
    endpoint: http://localhost:9001/v1
    upstream-model: m2
`)); err == nil {
		t.Fatal("expected rejection of colliding trimmed model names")
	}
}

// TestLoadRuntimeDocumentEdges pins the one-document rule's edges: a
// leading separator is part of document one (accepted), a trailing bare
// separator IS a second — null — document (rejected, same as any second
// document), and blank trailing lines are nothing at all (accepted).
// TestLoadRuntimeThinkingUsageValidation pins the thinking-usage block's
// reject matrix: a present block demands a known mode, ratios are finite,
// bounded to [0,1], and ordered, and no rejection text echoes the configured
// values. YAML's .nan and .inf decode straight into float64 and NaN defeats
// every comparison, so those are rejected by name here.
func TestLoadRuntimeThinkingUsageValidation(t *testing.T) {
	entry := func(block string) string {
		return "models:\n  a:\n    endpoint: https://h/v1\n    upstream-model: m\n" + block
	}
	cases := []struct {
		name string
		yaml string
		want string
	}{
		{"empty block", entry("    thinking-usage: {}\n"), "mode is required"},
		{"mode omitted with ratios", entry("    thinking-usage:\n      min-ratio: 0.5\n"), "mode is required"},
		{"unknown mode", entry("    thinking-usage:\n      mode: nonsense-SECRET\n"), "mode must be one of auto, always, off"},
		{"uppercase mode", entry("    thinking-usage:\n      mode: Always\n"), "mode must be one of auto, always, off"},
		{"min below range", entry("    thinking-usage:\n      mode: auto\n      min-ratio: -0.1\n"), "min-ratio must be a number between 0 and 1"},
		{"max above range", entry("    thinking-usage:\n      mode: auto\n      max-ratio: 1.1\n"), "max-ratio must be a number between 0 and 1"},
		{"nan min", entry("    thinking-usage:\n      mode: auto\n      min-ratio: .nan\n"), "min-ratio must be a number between 0 and 1"},
		{"inf max", entry("    thinking-usage:\n      mode: auto\n      max-ratio: .inf\n"), "max-ratio must be a number between 0 and 1"},
		{"negative inf min", entry("    thinking-usage:\n      mode: auto\n      min-ratio: -.inf\n"), "min-ratio must be a number between 0 and 1"},
		{"min above max", entry("    thinking-usage:\n      mode: always\n      min-ratio: 0.9\n      max-ratio: 0.1\n"), "min-ratio must not exceed max-ratio"},
		{"ratios still validated under off", entry("    thinking-usage:\n      mode: off\n      min-ratio: 2\n"), "min-ratio must be a number between 0 and 1"},
		{"unknown nested key", entry("    thinking-usage:\n      mode: auto\n      ratio: 0.5\n"), ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := LoadRuntime([]byte(tc.yaml))
			if err == nil {
				t.Fatal("expected rejection")
			}
			if tc.want != "" && !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want containing %q", err, tc.want)
			}
			if strings.Contains(err.Error(), "nonsense-SECRET") {
				t.Errorf("error text echoes the configured mode value: %v", err)
			}
		})
	}
}

// TestLoadRuntimeThinkingUsageNormalization pins the accept side: absent or
// null is off with a zero-valued config (the response plane's byte-identity
// default), and the share bounds normalize — both unset to the fixed
// default, one set pinned to it, both kept as the [Lo, Hi] range.
func TestLoadRuntimeThinkingUsageNormalization(t *testing.T) {
	entry := func(block string) string {
		return "api-key: unit-test-key\nmodels:\n  a:\n    endpoint: https://h/v1\n    upstream-model: m\n" + block
	}
	cases := []struct {
		name  string
		block string
		mode  ThinkingMode
		lo    float64
		hi    float64
	}{
		{"absent block stays off", "", ThinkingOff, 0, 0},
		{"null block stays off", "    thinking-usage:\n", ThinkingOff, 0, 0},
		{"explicit off", "    thinking-usage:\n      mode: off\n", ThinkingOff, defaultThinkingShare, defaultThinkingShare},
		{"auto default share", "    thinking-usage:\n      mode: auto\n", ThinkingAuto, defaultThinkingShare, defaultThinkingShare},
		{"always default share", "    thinking-usage:\n      mode: always\n", ThinkingAlways, defaultThinkingShare, defaultThinkingShare},
		{"min only pins both", "    thinking-usage:\n      mode: auto\n      min-ratio: 0.6\n", ThinkingAuto, 0.6, 0.6},
		{"max only pins both", "    thinking-usage:\n      mode: auto\n      max-ratio: 0.4\n", ThinkingAuto, 0.4, 0.4},
		{"both keep range", "    thinking-usage:\n      mode: always\n      min-ratio: 0.6\n      max-ratio: 0.9\n", ThinkingAlways, 0.6, 0.9},
		{"equal bounds", "    thinking-usage:\n      mode: auto\n      min-ratio: 0.5\n      max-ratio: 0.5\n", ThinkingAuto, 0.5, 0.5},
		{"zero share", "    thinking-usage:\n      mode: auto\n      min-ratio: 0\n      max-ratio: 0\n", ThinkingAuto, 0, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := mustSnapshot(t, entry(tc.block))
			m, ok := s.Model("a")
			if !ok {
				t.Fatal("model a not found")
			}
			tu := m.ThinkingUsage
			if tu.Mode != tc.mode || tu.Lo != tc.lo || tu.Hi != tc.hi {
				t.Fatalf("ThinkingUsage = %+v, want mode %d lo %v hi %v", tu, tc.mode, tc.lo, tc.hi)
			}
		})
	}
}

func TestLoadRuntimeDocumentEdges(t *testing.T) {
	tests := []struct {
		name string
		yaml string
		want bool // accepted
	}{
		{"plain", validRuntime(), true},
		{"leading separator", "---\n" + validRuntime(), true},
		{"trailing blank lines", validRuntime() + "\n\n", true},
		{"trailing bare separator", validRuntime() + "---\n", false},
		{"two trailing separators", validRuntime() + "---\n---\n", false},
		{"second document with content", validRuntime() + "---\nmodels: {}\n", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := LoadRuntime([]byte(tt.yaml))
			if got := err == nil; got != tt.want {
				t.Fatalf("accepted = %v (err %v), want accepted = %v", got, err, tt.want)
			}
		})
	}
}

// TestParseStripPath pins the dotted-path grammar: unquoted segments split on
// the dot, single-quoted segments carry literal dots and spaces and are
// decoded under the JSON unquoting rules, and every malformed spelling is
// rejected with fixed text (never echoing the path).
func TestParseStripPath(t *testing.T) {
	ok := []struct {
		path string
		want []string
	}{
		{"provider", []string{"provider"}},
		{"service_tier", []string{"service_tier"}},
		{"a.b", []string{"a", "b"}},
		{"a.b.c.d", []string{"a", "b", "c", "d"}},
		{"'parent name'.child", []string{"parent name", "child"}},
		{"'A'.B", []string{"A", "B"}},
		{"'a.b'.c", []string{"a.b", "c"}},
		{"'a''b'.c", []string{"a'b", "c"}},
		{"'single'.'quo ted'", []string{"single", "quo ted"}},
	}
	for _, tc := range ok {
		got, err := ParseStripPath(tc.path)
		if err != nil {
			t.Errorf("ParseStripPath(%q): %v", tc.path, err)
			continue
		}
		if len(got) != len(tc.want) {
			t.Errorf("ParseStripPath(%q) = %v, want %v", tc.path, got, tc.want)
			continue
		}
		for i := range got {
			if got[i] != tc.want[i] {
				t.Errorf("ParseStripPath(%q) = %v, want %v", tc.path, got, tc.want)
				break
			}
		}
	}
	for _, path := range []string{
		"",                        // empty
		".a",                      // leading empty segment
		"a.",                      // trailing empty segment
		"a..b",                    // consecutive dots
		"a.'b",                    // unterminated quote
		"'a.b",                    // unterminated quote (at start)
		"a.b'",                    // stray quote in unquoted segment
		"a' b.c",                  // quote followed by space
		"a.'b'.c'",                // unterminated final quote
		"'a''b",                   // escaped quote then unterminated
		"'b.c'.d.'e'.f.g.h.i.j.k", // too deep
	} {
		if _, err := ParseStripPath(path); err == nil {
			t.Errorf("ParseStripPath(%q) accepted, want rejection", path)
		}
	}
}

// TestStripReservedPaths pins the reserved set from both sides: every path
// the proxy writes is rejected in every spelling it can be written under, and
// the provider-added members strip-fields exists to reach are accepted. Both
// lists are literal on purpose — they are the contract, so a writer that adds
// a member has to come here and extend them, and a rule that quietly widens
// (rejecting provider data) fails just as loudly as one that quietly narrows.
func TestStripReservedPaths(t *testing.T) {
	rejected := []string{
		"model",
		"usage",
		"response.model",
		"response.usage",
		"usage.completion_tokens_details.reasoning_tokens",
		"usage.output_tokens_details.reasoning_tokens",
		"response.usage.completion_tokens_details.reasoning_tokens",
		"response.usage.output_tokens_details.reasoning_tokens",
	}
	for _, path := range rejected {
		if _, err := ParseStripPath(path); err == nil {
			t.Errorf("ParseStripPath(%q) accepted, want rejection", path)
		}
	}
	allowed := []string{
		// The provider-added members inside the usage object — the motivating
		// case: the object is reserved, its children are not.
		"usage.is_byok",
		"usage.cost",
		"usage.cost_details.upstream_inference_cost",
		"usage.prompt_tokens_details.cached_tokens",
		"usage.cache_read_input_tokens",
		// A synthesis leaf's PARENT: allowed, and documented as also excising
		// the provider's own siblings under the same object.
		"usage.completion_tokens_details",
		"usage.output_tokens_details",
		// A bare "model"/"usage" segment away from the proxy's own members.
		"a.model",
		"a.usage",
		"'usage'.cost",
		// The descent happens exactly once: a second "response" level reaches
		// nothing the proxy writes.
		"response.response.model",
		"response.provider",
	}
	for _, path := range allowed {
		if _, err := ParseStripPath(path); err != nil {
			t.Errorf("ParseStripPath(%q) rejected: %v", path, err)
		}
	}
}

// TestLoadRuntimeStripFieldsValidation pins the rejections: an explicit empty
// list, empty or unresolvable segments, duplicates, and the reserved paths
// — all with fixed text that never echoes the offending path.
func TestLoadRuntimeStripFieldsValidation(t *testing.T) {
	entry := func(block string) string {
		return "api-key: unit-test-key\nmodels:\n  a:\n    endpoint: https://h/v1\n    upstream-model: m\n    " + block
	}
	cases := []struct {
		name string
		yaml string
		want string
	}{
		{"empty list", entry("strip-fields: []\n"), "requires at least one path"},
		{"empty path", entry("strip-fields: [\"  \"]\n"), "path must not be empty"},
		{"empty segment", entry("strip-fields: [a..b]\n"), "empty segment"},
		{"stray quote", entry("strip-fields: [\"a'b.c\"]\n"), "quoted before the first quote character"},
		{"duplicate", entry("strip-fields: [a.b, a.b]\n"), "duplicate path"},
		{"duplicate after trim", entry("strip-fields: [\" a \", a]\n"), "duplicate path"},
		{"reserved model", entry("strip-fields: [model]\n"), `must not include "model"`},
		{"reserved usage", entry("strip-fields: [usage]\n"), `must not include "usage"`},
		{"reserved envelope model", entry("strip-fields: [response.model]\n"), `must not include "response.model"`},
		{"reserved envelope usage", entry("strip-fields: [response.usage]\n"), `must not include "response.usage"`},
		{"reserved synthesis leaf", entry("strip-fields: [usage.output_tokens_details.reasoning_tokens]\n"),
			`must not include "usage.output_tokens_details.reasoning_tokens"`},
		{"reserved quoted synthesis leaf",
			entry("strip-fields: [\"'usage'.completion_tokens_details.reasoning_tokens\"]\n"),
			`must not include "usage.completion_tokens_details.reasoning_tokens"`},
		{"double quote in segment", entry("strip-fields: [\"a\\\"b\"]\n"), "must not contain a double quote"},
		{"too many paths", entry("strip-fields: " + manyPaths() + "\n"), "too many paths"},
		{"too deep", entry("strip-fields: [a.b.c.d.e.f.g.h.i]\n"), "descends too deep"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := LoadRuntime([]byte(tc.yaml))
			if err == nil {
				t.Fatal("expected rejection")
			}
			if tc.want != "" && !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want containing %q", err, tc.want)
			}
			if strings.Contains(err.Error(), "SECRET") {
				t.Errorf("error text echoes a value: %v", err)
			}
		})
	}
}

// manyPaths renders seventeen distinct strip paths for the max-count reject.
func manyPaths() string {
	paths := make([]string, 0, 17)
	for i := 1; i <= 17; i++ {
		paths = append(paths, fmt.Sprintf("f%d", i))
	}
	return "[" + strings.Join(paths, ", ") + "]"
}

// TestLoadRuntimeStripFieldsNormalization pins the accept side: absent or
// null is off (nil strip), a list is parsed into ordered segments, and the
// model-override-provider layering — a model with its own list replaces its
// provider's list, a model without one carries each candidate's provider
// list per hop.
func TestLoadRuntimeStripFieldsNormalization(t *testing.T) {
	load := func(yaml string) Model {
		t.Helper()
		s, err := LoadRuntime([]byte(yaml))
		if err != nil {
			t.Fatalf("LoadRuntime: %v", err)
		}
		m, _ := s.Model("a")
		return m
	}
	// The base carries the providers table with strip lists; every model
	// references one of its entries.
	base := "api-key: unit-test-key\ntransports:\n  t1: { type: direct }\n  t2: { type: direct }\nproviders:\n  pa:\n    base-url: https://a/v1\n    transport: t1\n    strip-fields: [provider]\n  pb:\n    base-url: https://b/v1\n    transport: t2\n    strip-fields: [service_tier, 'meta.extra']\n"

	// Absent / null → nil strip, feature off.
	m := load(base + "models:\n  a:\n    provider: pa\n    upstream-model: m\n")
	if m.Strip != nil {
		t.Errorf("absent strip-fields: Strip = %v, want nil", m.Strip)
	}
	m = load(base + "models:\n  a:\n    provider: pa\n    upstream-model: m\n    strip-fields: null\n")
	if m.Strip != nil {
		t.Errorf("null strip-fields: Strip = %v, want nil", m.Strip)
	}

	// A model with its own list replaces the provider's list.
	m = load(base + "models:\n  a:\n    provider: pa\n    upstream-model: m\n    strip-fields: [provider, service_tier]\n")
	if len(m.Strip) != 2 || m.Strip[0].Segments[0] != "provider" || m.Strip[1].Segments[0] != "service_tier" {
		t.Errorf("model-level Strip = %v, want [provider service_tier]", m.Strip)
	}
	for _, c := range m.Chain {
		if len(c.Strip) != 2 {
			t.Errorf("candidate Strip = %v, want the model list on every hop", c.Strip)
		}
	}

	// A model without a list inherits its provider's list per candidate.
	m = load(base + "models:\n  a:\n    provider: pa\n    upstream-model: m\n")
	if m.Strip != nil {
		t.Errorf("inherited model Strip = %v, want nil (per-hop)", m.Strip)
	}
	if len(m.Chain[0].Strip) != 1 || m.Chain[0].Strip[0].Segments[0] != "provider" {
		t.Errorf("chain[0].Strip = %v, want [provider] from pa", m.Chain[0].Strip)
	}

	// A chain through two providers carries each provider's list per hop.
	m = load(base + "models:\n  a:\n    providers:\n      - { provider: pa, upstream-model: x }\n      - { provider: pb, upstream-model: y }\n")
	if m.Strip != nil {
		t.Errorf("chained model Strip = %v, want nil (per-hop)", m.Strip)
	}
	if len(m.Chain[0].Strip) != 1 || m.Chain[0].Strip[0].Segments[0] != "provider" {
		t.Errorf("chain[0].Strip = %v, want [provider]", m.Chain[0].Strip)
	}
	if len(m.Chain[1].Strip) != 2 {
		t.Errorf("chain[1].Strip = %v, want pb's 2 paths", m.Chain[1].Strip)
	}
}
