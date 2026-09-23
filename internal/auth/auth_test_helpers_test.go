package auth

import (
	"testing"

	"openai-compatible-injector/internal/config"
)

// staticSnapshot builds a minimal runtime snapshot carrying the given
// api-key, the way a boot or reload would.
func staticSnapshot(t *testing.T, key string) *config.Snapshot {
	t.Helper()
	snap, err := config.LoadRuntime([]byte("api-key: " + key + "\nmodels:\n  m:\n    endpoint: https://up.example/v1\n    upstream-model: u\n"))
	if err != nil {
		t.Fatalf("LoadRuntime: %v", err)
	}
	return snap
}
