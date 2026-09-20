package config

import (
	"strings"
	"testing"
)

func validRuntime() string {
	return `
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
		{"non-http endpoint", "models:\n  a:\n    endpoint: ftp://h\n    upstream-model: m\n", "http(s) URL"},
		{"hostless endpoint", "models:\n  a:\n    endpoint: https://\n    upstream-model: m\n", "http(s) URL"},
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

func TestSnapshotImmutableBetweenPublishes(t *testing.T) {
	// Publishers hand over fresh snapshots; a published snapshot must not
	// alias the previous one (the map is not shared).
	s1 := mustSnapshot(t, validRuntime())
	s2 := mustSnapshot(t, `
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
