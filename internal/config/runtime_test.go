package config

import (
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
		{"missing endpoint", "models:\n  a:\n    upstream-model: m\n", "endpoint is required"},
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
	}
	for _, tc := range rejects {
		t.Run(tc.name, func(t *testing.T) {
			_, err := LoadRuntime([]byte(tc.yaml))
			if err == nil {
				t.Fatal("expected rejection")
			}
			want := "api-key is required"
			if tc.name == "embedded space" || tc.name == "embedded tab" || tc.name == "leading tab" || tc.name == "trailing newline" || tc.name == "disallowed character" || tc.name == "padding before token end" || tc.name == "only padding" {
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
