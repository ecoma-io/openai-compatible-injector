package auth

import (
	"testing"

	"openai-compatible-injector/internal/config"
)

// staticSnapshot builds a minimal runtime snapshot carrying the given
// api-key(s), the way a boot or reload would.
func staticSnapshot(t *testing.T, keys ...string) *config.Snapshot {
	t.Helper()
	var yaml string
	if len(keys) == 0 {
		yaml = "models:\n  m:\n    endpoint: https://up.example/v1\n    upstream-model: u\n"
	} else if len(keys) == 1 {
		yaml = "api-key: " + keys[0] + "\nmodels:\n  m:\n    endpoint: https://up.example/v1\n    upstream-model: u\n"
	} else {
		// build list
		yaml = "api-key:\n"
		for _, k := range keys {
			yaml += "  - " + k + "\n"
		}
		yaml += "models:\n  m:\n    endpoint: https://up.example/v1\n    upstream-model: u\n"
	}
	snap, err := config.LoadRuntime([]byte(yaml))
	if err != nil {
		t.Fatalf("LoadRuntime: %v", err)
	}
	return snap
}
