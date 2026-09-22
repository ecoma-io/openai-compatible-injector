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
			name: "log-level value carries a credential-bearing URL",
			yaml: "models:\n  a:\n    endpoint: http://h.example/v1\n    upstream-model: m\nlog-level: https://h.example/v1?" + marker + "=x\n",
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

// TestModelEntryErrorsLocateByOrdinalNeverByName pins the model-key rule: a
// model entry key is operator input in a pasteable position, so buildModel
// and collision rejections locate the entry by a stable ordinal — never by
// quoting its name, which would carry a botched paste into the logs.
func TestModelEntryErrorsLocateByOrdinalNeverByName(t *testing.T) {
	const nameMarker = "SECRET_MODEL_NAME_SHOULD_NOT_LEAK"
	tests := []struct {
		name string
		yaml string
	}{
		{
			name: "missing endpoint",
			yaml: "models:\n  " + nameMarker + ":\n    upstream-model: m\n",
		},
		{
			name: "bad scheme",
			yaml: "models:\n  " + nameMarker + ":\n    endpoint: ftp://h/v1\n    upstream-model: m\n",
		},
		{
			name: "credentials in endpoint",
			yaml: "models:\n  " + nameMarker + ":\n    endpoint: https://user:SECRET_PASSWORD_VALUE@h/v1\n    upstream-model: m\n",
		},
		{
			name: "fragment in endpoint",
			yaml: "models:\n  " + nameMarker + ":\n    endpoint: https://h/v1#" + nameMarker + "\n    upstream-model: m\n",
		},
		{
			name: "missing upstream model",
			yaml: "models:\n  " + nameMarker + ":\n    endpoint: https://h/v1\n",
		},
		{
			// Distinct YAML keys that trim to the same model name: the
			// collision is a reject, and either key's text must stay out of
			// the error.
			name: "duplicate after trimming",
			yaml: "models:\n  \" " + nameMarker + "\":\n    endpoint: https://h/v1\n    upstream-model: m\n  " + nameMarker + ":\n    endpoint: https://h/v1\n    upstream-model: m\n",
		},
		{
			name: "whitespace-only name",
			yaml: "models:\n  \"  \":\n    endpoint: https://h/v1\n    upstream-model: m\n",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := LoadRuntime([]byte(tt.yaml))
			if err == nil {
				t.Fatalf("LoadRuntime accepted %q", tt.yaml)
			}
			if strings.Contains(err.Error(), nameMarker) {
				t.Fatalf("rejection error echoes the model name: %q", err.Error())
			}
			if !strings.Contains(err.Error(), "model entry") {
				t.Fatalf("rejection error does not locate the entry by ordinal: %q", err.Error())
			}
		})
	}
}

// TestModelEntryOrdinalsAreDeterministic pins the ordinal's stability: the
// entries are validated in sorted-key order, so the same file must reject
// naming the same ordinal on every run — an error that moved between
// entries would be useless for locating the broken one.
func TestModelEntryOrdinalsAreDeterministic(t *testing.T) {
	const yaml = "models:\n  zzz:\n    upstream-model: m\n  aaa:\n    upstream-model: m\n"
	_, err := LoadRuntime([]byte(yaml))
	if err == nil {
		t.Fatal("expected rejection")
	}
	if !strings.Contains(err.Error(), "model entry 1: endpoint is required") {
		t.Fatalf("err = %v, want the first entry in sorted order (aaa) named by ordinal", err)
	}
}

// TestBuildModelErrorMarkersStayRedacted plants the credential-marker
// spellings in value positions a buildModel rejection touches: the scheme
// position (a paste like `Authorization: Bearer ...` parses with the first
// word as its scheme) and query strings that survive into every URL error.
func TestBuildModelErrorMarkersStayRedacted(t *testing.T) {
	tests := []struct {
		name string
		yaml string
	}{
		{
			name: "bad scheme with token query",
			yaml: "models:\n  a:\n    endpoint: ftp://h/v1?token=SECRET_TOKEN_VALUE\n    upstream-model: m\n",
		},
		{
			name: "unparseable URL with password query",
			yaml: "models:\n  a:\n    endpoint: \"ht tp://h/v1?password=SECRET_PASSWORD_VALUE\"\n    upstream-model: m\n",
		},
		{
			name: "hostless with Authorization query",
			yaml: "models:\n  a:\n    endpoint: \"/v1?Authorization=SECRET_AUTH_HEADER\"\n    upstream-model: m\n",
		},
		{
			// A paste like `Authorization: Bearer x` parses with its first
			// word as the URL scheme; the marker here is lowercase because
			// url.Parse lowercases schemes, so the echoed form must still
			// match — a scheme echo of any case shape is a leak.
			name: "auth paste in scheme position",
			yaml: "models:\n  a:\n    endpoint: \"secretbearertoken:x\"\n    upstream-model: m\n",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := LoadRuntime([]byte(tt.yaml))
			if err == nil {
				t.Fatalf("LoadRuntime accepted %q", tt.yaml)
			}
			for _, secret := range []string{
				"SECRET_TOKEN_VALUE", "SECRET_PASSWORD_VALUE",
				"SECRET_AUTH_HEADER", "secretbearertoken",
			} {
				if strings.Contains(err.Error(), secret) {
					t.Errorf("rejection error echoes %q: %q", secret, err.Error())
				}
			}
		})
	}
}
