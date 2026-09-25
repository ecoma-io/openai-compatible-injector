package config

import (
	"strings"
	"testing"
	"time"
)

// authRuntime builds a runtime body whose provider `pa` carries `extra`
// appended, with `models` routing two models at it by default.
func authRuntime(extra, models string) string {
	if models == "" {
		models = "models:\n" +
			"  m1:\n    provider: pa\n    upstream-model: up-a\n" +
			"  m2:\n    providers:\n      - provider: pa\n        upstream-model: up-a\n"
	}
	return "api-key: unit-test-key\n" +
		"transports:\n  t1:\n    type: direct\n" +
		"providers:\n" +
		"  pa:\n    base-url: https://a.example/v1\n    transport: t1\n" + extra +
		models
}

const (
	authSecretMarker = "SECRETMARKER-KEY-VALUE"
	validAuthBlock   = "    auth:\n" +
		"      type: api_key\n" +
		"      header: Authorization\n" +
		"      prefix: \"Bearer \"\n" +
		"      strategy: round_robin\n" +
		"      keys:\n" +
		"        - id: kilo-1\n" +
		"          value: sk-test-one\n" +
		"        - id: kilo-2\n" +
		"          value: sk-test-two\n"
)

// TestProviderAuthBuildsCredential pins the happy path: the block builds a
// validated candidate credential, the chain carries it, and the snapshot's
// retain set dedups by content so two models sharing a provider share one
// pool identity.
func TestProviderAuthBuildsCredential(t *testing.T) {
	s := mustSnapshot(t, authRuntime(validAuthBlock, ""))
	m1, ok := s.Model("m1")
	if !ok {
		t.Fatal("model m1 not found")
	}
	if m1.Chain[0].Cred == nil {
		t.Fatal("candidate cred is nil with an auth block present")
	}
	cred := m1.Chain[0].Cred
	if cred.Spec.Header != "Authorization" || cred.Spec.Prefix != "Bearer " {
		t.Errorf("header surface = %q / %q", cred.Spec.Header, cred.Spec.Prefix)
	}
	if cred.Spec.Strategy != "round_robin" || len(cred.Spec.Keys) != 2 {
		t.Errorf("spec = %+v", cred.Spec)
	}
	if cred.Spec.Keys[0].ID != "kilo-1" || cred.Spec.Keys[0].Value != "sk-test-one" {
		t.Errorf("first key = %+v", cred.Spec.Keys[0])
	}
	// Defaults: the cooldown policy is filled, never zero.
	if cred.RateLimit.Cooldown != 2*time.Second || cred.RateLimit.MaxCooldown != time.Minute {
		t.Errorf("default rate limit = %+v", cred.RateLimit)
	}
	if len(s.Credentials()) != 1 || s.Credentials()[0].ContentKey() != cred.ContentKey() {
		t.Errorf("retain set = %d entries, want 1 identical", len(s.Credentials()))
	}
	// The second model's chain candidate carries the same configuration.
	m2, _ := s.Model("m2")
	if m2.Chain[0].Cred == nil || m2.Chain[0].Cred.ContentKey() != cred.ContentKey() {
		t.Error("m2 candidate lost the shared credential")
	}
}

// TestProviderAuthStatedRateLimit pins the stated-value path and the
// boundary rules: values override the defaults, and a cooldown above the
// ceiling is a rejection rather than a silent clamp.
func TestProviderAuthStatedRateLimit(t *testing.T) {
	s := mustSnapshot(t, authRuntime(
		"    auth:\n"+
			"      type: api_key\n"+
			"      header: X-API-Key\n"+
			"      strategy: round_robin\n"+
			"      keys:\n"+
			"        - id: only\n"+
			"          value: sk-only\n"+
			"      rate-limit:\n"+
			"        cooldown: 5s\n"+
			"        max-cooldown: 90s\n", ""))
	m, _ := s.Model("m1")
	if got := m.Chain[0].Cred.RateLimit.Cooldown; got != 5*time.Second {
		t.Errorf("stated cooldown = %v, want 5s", got)
	}
	if got := m.Chain[0].Cred.RateLimit.MaxCooldown; got != 90*time.Second {
		t.Errorf("stated max-cooldown = %v, want 90s", got)
	}
}

// TestProviderAuthAbsentKeepsCandidateNil pins the byte-identity default: no
// auth block, no credential anywhere on the candidate or in the retain set.
func TestProviderAuthAbsentKeepsCandidateNil(t *testing.T) {
	s := mustSnapshot(t, authRuntime("", ""))
	m, _ := s.Model("m1")
	if m.Chain[0].Cred != nil {
		t.Fatalf("candidate cred = %+v, want nil without an auth block", m.Chain[0].Cred)
	}
	if len(s.Credentials()) != 0 {
		t.Fatalf("retain set = %d entries, want 0", len(s.Credentials()))
	}
}

// TestProviderAuthRejections pins the fail-closed grammar. Every message is
// fixed text naming position and rule; none can ever carry a key value.
func TestProviderAuthRejections(t *testing.T) {
	secretKeys := "      keys:\n        - id: k1\n          value: " + authSecretMarker + "\n"
	cases := []struct {
		name  string
		extra string
		want  string
	}{
		{"missing type", "    auth:\n      header: Authorization\n" + secretKeys, "type must be"},
		{"wrong type", "    auth:\n      type: oauth2\n      header: Authorization\n" + secretKeys, "type must be"},
		{"missing header", "    auth:\n      type: api_key\n" + secretKeys, "header"},
		{"bad header", "    auth:\n      type: api_key\n      header: not a header\n" + secretKeys, "auth header"},
		{"empty keys", "    auth:\n      type: api_key\n      header: Authorization\n      strategy: round_robin\n", "at least one key"},
		{"missing strategy", "    auth:\n      type: api_key\n      header: Authorization\n" + secretKeys, "strategy"},
		{"bad strategy", "    auth:\n      type: api_key\n      header: Authorization\n      strategy: least_connections\n" + secretKeys, "strategy"},
		{"duplicate ids", "    auth:\n      type: api_key\n      header: Authorization\n      strategy: round_robin\n      keys:\n        - id: k1\n          value: v1\n        - id: k1\n          value: v2\n", "unique"},
		{"bad key id", "    auth:\n      type: api_key\n      header: Authorization\n      strategy: round_robin\n      keys:\n        - id: \"bad id\"\n          value: v1\n", "key id"},
		{"empty key value", "    auth:\n      type: api_key\n      header: Authorization\n      strategy: round_robin\n      keys:\n        - id: k1\n          value: \"\"\n", "key value"},
		{"zero cooldown", "    auth:\n      type: api_key\n      header: Authorization\n      strategy: round_robin\n" + secretKeys + "      rate-limit:\n        cooldown: 0s\n", "cooldown must be positive"},
		{"over-cap max-cooldown", "    auth:\n      type: api_key\n      header: Authorization\n      strategy: round_robin\n" + secretKeys + "      rate-limit:\n        max-cooldown: 5m\n", "at most 2m0s"},
		{"cooldown above max", "    auth:\n      type: api_key\n      header: Authorization\n      strategy: round_robin\n" + secretKeys + "      rate-limit:\n        cooldown: 30s\n        max-cooldown: 10s\n", "must not exceed"},
		{"bad duration", "    auth:\n      type: api_key\n      header: Authorization\n      strategy: round_robin\n" + secretKeys + "      rate-limit:\n        cooldown: soon\n", "must be a valid duration"},
	}
	for _, tc := range cases {
		_, err := LoadRuntime([]byte(authRuntime(tc.extra, "")))
		if err == nil {
			t.Errorf("%s: accepted, want %q", tc.name, tc.want)
			continue
		}
		if !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: error = %v, want it to name %q", tc.name, err, tc.want)
		}
		if strings.Contains(err.Error(), authSecretMarker) {
			t.Errorf("%s: rejection echoed the key value", tc.name)
		}
	}
}

// TestProviderAuthValidatedEvenUnreferenced mirrors the rest of the
// providers table: an entry no model references is still validated, so a
// broken key list can never wait for a model to start using it.
func TestProviderAuthValidatedEvenUnreferenced(t *testing.T) {
	data := "api-key: unit-test-key\n" +
		"providers:\n" +
		"  pa:\n    base-url: https://a.example/v1\n" +
		"    auth:\n      type: api_key\n      header: Authorization\n      strategy: round_robin\n" +
		"models:\n" +
		"  m:\n    endpoint: https://b.example/v1\n    upstream-model: up-b\n"
	if _, err := LoadRuntime([]byte(data)); err == nil {
		t.Fatal("an unreferenced provider with a broken auth block was accepted")
	}
}
