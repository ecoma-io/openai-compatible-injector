package config

import (
	"strings"
	"testing"
)

// Rejection error text reaches logs verbatim — at boot as the fatal
// config_load_failed, on reload as the WARN config_reload_rejected — so an
// operator's botched paste of a credential-bearing endpoint must never be
// echoed back by an error message. These tests plant markers in every
// input-echoing position (values, keys, scalar-in-map positions) and demand
// they stay out of the error text.

const marker = "SECRET_MARKER"

func TestLoadRuntimeErrorsNeverEchoInput(t *testing.T) {
	tests := []struct {
		name string
		yaml string
	}{
		{
			name: "logging.level value carries a credential-bearing URL",
			yaml: "models:\n  a:\n    endpoint: http://h.example/v1\n    upstream-model: m\nlogging:\n  level: https://h.example/v1?" + marker + "=x\n",
		},
		{
			name: "URL pasted as a key inside a model entry",
			yaml: "models:\n  live:\n    https://h.example/v1?" + marker + "=x: \"\"\n    upstream-model: m\n",
		},
		{
			name: "URL pasted as a top-level key",
			yaml: "https://h.example/v1?" + marker + "=x: true\nmodels:\n  a:\n    endpoint: http://h.example/v1\n    upstream-model: m\n",
		},
		{
			name: "secret scalar where a model table is expected",
			yaml: "models: " + marker + "\n",
		},
		{
			name: "secret scalar where an endpoint string is expected",
			yaml: "models:\n  a: " + marker + "\n",
		},
		{
			name: "second document carrying a bootstrap key",
			yaml: "models:\n  a:\n    endpoint: http://h.example/v1\n    upstream-model: m\n---\n" + marker + ": true\n",
		},
		{
			name: "endpoint with an invalid path escape and a secret query",
			yaml: "models:\n  a:\n    endpoint: http://h.example/v1%zz?" + marker + "=x\n    upstream-model: m\n",
		},
		{
			// yaml.v3 rejects duplicate keys with a TypeError that quotes the
			// full key text — a merge tool duplicating a block, or an
			// operator pasting a key twice while iterating, must not have it
			// echoed.
			name: "duplicate top-level key carrying a credential",
			yaml: marker + ": true\nmodels:\n  a:\n    endpoint: http://h.example/v1\n    upstream-model: m\n" + marker + ": false\n",
		},
		{
			name: "duplicate model-entry key whose repeated value carries a credential",
			yaml: "models:\n  a:\n    endpoint: http://h.example/v1\n    endpoint: http://h.example/?" + marker + "=x\n    upstream-model: m\n",
		},
		{
			// A file that is only a scalar cannot be a mapping; the decode
			// error quotes (an elision of) the scalar.
			name: "whole file is a bare scalar",
			yaml: marker + "\n",
		},
		{
			// An undefined alias names its anchor in the error text — a
			// scanner-level failf, not a TypeError.
			name: "undefined alias named by a credential",
			yaml: "models:\n  a:\n    endpoint: *" + marker + "\n    upstream-model: m\n",
		},
		{
			name: "self-referential anchor named by a credential",
			yaml: "models:\n  a: &" + marker + "\n    endpoint: *" + marker + "\n    upstream-model: m\n",
		},
		{
			// An explicit tag that contradicts the value quotes the scalar.
			name: "explicit tag mismatch quoting the value",
			yaml: "models:\n  a:\n    endpoint: !!int " + marker + "\n    upstream-model: m\n",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := LoadRuntime([]byte(tt.yaml))
			if err == nil {
				t.Fatalf("LoadRuntime accepted %q", tt.yaml)
			}
			if strings.Contains(err.Error(), marker) {
				t.Fatalf("rejection error echoes operator input: %q", err.Error())
			}
		})
	}
}

func TestParseLogLevelErrorDoesNotEchoValue(t *testing.T) {
	_, err := ParseLogLevel("https://h.example/v1?" + marker + "=x")
	if err == nil {
		t.Fatal("ParseLogLevel accepted a URL")
	}
	if strings.Contains(err.Error(), marker) {
		t.Fatalf("ParseLogLevel error echoes the raw value: %q", err.Error())
	}
	if !strings.Contains(err.Error(), "debug") {
		t.Errorf("error should still name the legal levels: %q", err.Error())
	}
}
