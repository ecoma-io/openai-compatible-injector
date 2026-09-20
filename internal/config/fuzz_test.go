package config

import (
	"strings"
	"testing"

	"github.com/rs/zerolog"
)

// FuzzLoadRuntime pins the reject path's safety property: whatever bytes
// arrive — a truncate-then-write mid-state, a hand-edited file, binary
// garbage — LoadRuntime either returns an error or a fully usable snapshot.
// A snapshot that parses must never be half-built: at least one model (the
// empty table is a reject), every entry named by its own key with a usable
// endpoint and upstream model, a resolvable lookup per entry, and a log
// level from the accepted set. Panics are the target; any error outcome is
// legal.
func FuzzLoadRuntime(f *testing.F) {
	seeds := []string{
		``,             // empty document
		"\n",           // single newline
		"   \n   \n",   // whitespace only
		validRuntime(), // the valid baseline
		strings.Replace(validRuntime(), "gpt-5-pro", "gpt-6-pro", 1),
		strings.Replace(validRuntime(), "gpt-reviewer", "模型", 1), // unicode model name
		`models: {}`,                        // empty table: reject
		`models:`,                           // null table: reject
		`models: [unclosed`,                 // malformed YAML
		"listen: :8081\n" + validRuntime(),  // bootstrap key: reject
		"shutdown-grace: 55s\nmodels: {}\n", // another bootstrap key
		"bananas: true\n" + validRuntime(),  // unknown top-level key
		`models:
  a:
    endpoint: https://h/v1
    upstream-model: m
`, // minimal valid entry
		`models:
  a:
    upstream-model: m
`, // missing endpoint
		`models:
  a:
    endpoint: ftp://h/v1
    upstream-model: m
`, // bad scheme
		`models:
  a:
    endpoint: https://user:pass@h/v1
    upstream-model: m
`, // credentials
		`models:
  a:
    endpoint: https://h/v1#page
    upstream-model: m
`, // fragment
		`models:
  a:
    endpoint: https://h/v1?api-key=SECRET
    upstream-model: m
`, // query string survives, must not reach errors
		`models:
  "  ":
    endpoint: https://h/v1
    upstream-model: m
`, // empty name after trim
		`models:
  "  a":
    endpoint: https://h/v1
    upstream-model: m
  a:
    endpoint: https://h/v1
    upstream-model: m2
`, // trim collision
		`models:
  a:
    ENDPOINT: https://h/v1
    upstream-model: m
`, // case-sensitive entry key
		`models:
  a:
    endpoint: https://h/v1
    upstream-model: m
    extra: true
`, // unknown entry key, strict decode
		"logging:\n  level: debug\n" + strings.TrimPrefix(validRuntime(), "\n"),
		`logging:
  level: banana
models:
  a:
    endpoint: https://h/v1
    upstream-model: m
`, // invalid level
		`logging: null
models:
  a:
    endpoint: https://h/v1
    upstream-model: m
`, // null logging section
		`{"models":{"a":{"endpoint":"https://h/v1","upstream-model":"m"}}}`,                                                                    // JSON is valid YAML
		"models:\n  a:\n    endpoint: https://h/v1\n    upstream-model: m\n    injection-prompt: |\n      " + strings.Repeat("x", 4096) + "\n", // huge prompt value
		"\xff\xfe\x00\x01",       // binary garbage
		"\xef\xbb\xbfmodels: {}", // BOM-prefixed YAML
		"models:\n  a:\n    endpoint: \"ht tp://h\"\n    upstream-model: m\n", // unparseable URL
	}
	for _, s := range seeds {
		f.Add([]byte(s))
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		s, err := LoadRuntime(data)
		if err != nil {
			return
		}
		if s == nil {
			t.Fatal("LoadRuntime returned a nil snapshot with a nil error")
		}
		_ = s.Gen()
		_ = s.LogLevel()
		n := s.Len()
		if n < 1 {
			t.Fatalf("accepted a snapshot with %d models, want at least 1", n)
		}
		mods := s.Models()
		if len(mods) != n {
			t.Fatalf("Len() = %d but Models() returned %d entries", n, len(mods))
		}
		for name, m := range mods {
			if name == "" {
				t.Fatal("accepted a snapshot with an empty model name")
			}
			if m.Public != name {
				t.Fatalf("model %q carries Public %q", name, m.Public)
			}
			if m.Endpoint == nil || m.Endpoint.Host == "" {
				t.Fatalf("model %q has no usable endpoint: %v", name, m.Endpoint)
			}
			if m.UpstreamModel == "" {
				t.Fatalf("model %q has an empty upstream-model", name)
			}
			if _, ok := s.Model(name); !ok {
				t.Fatalf("model %q is not resolvable via Model()", name)
			}
		}
		switch s.LogLevel() {
		case zerolog.DebugLevel, zerolog.InfoLevel, zerolog.WarnLevel, zerolog.ErrorLevel:
		default:
			t.Fatalf("accepted a snapshot carrying log level %v", s.LogLevel())
		}
	})
}
