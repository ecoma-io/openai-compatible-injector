package config

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"openai-compatible-injector/internal/recovery"
)

// defaultWantedRetry is the retry mechanics every model must carry when its
// runtime entry declares no retries block (or a null one): the status matrix
// applies by default, and the legacy block only sized it — every candidate
// still walks under one same-candidate retry.
var defaultWantedRetry = recovery.RetryPolicy{
	MaxRetries: 1,
	MaxElapsed: 10 * time.Second,
	Backoff: recovery.BackoffPolicy{
		Initial: 250 * time.Millisecond,
		Max:     2 * time.Second,
		Jitter:  0.1,
	},
	OnExhausted: recovery.ActionFallback,
}

// TestLoadRuntimeRetryPolicyDefaults pins the defaults rule: the retries
// block is optional everywhere — absent, null, and empty all select the
// same defaults, and both route forms (chain and legacy) resolve the same
// effective policy onto the model.
func TestLoadRuntimeRetryPolicyDefaults(t *testing.T) {
	for _, tc := range []struct {
		name      string
		modelBody string
	}{
		{"absent", "    provider: pa\n    upstream-model: up-a\n"},
		{"null", "    provider: pa\n    upstream-model: up-a\n    retries:\n"},
		{"empty", "    provider: pa\n    upstream-model: up-a\n    retries: {}\n"},
		{"chain absent", "    providers:\n      - provider: pa\n        upstream-model: up-a\n"},
	} {
		snap, err := LoadRuntime([]byte(chainRuntime(tc.modelBody, "")))
		if err != nil {
			t.Fatalf("%s: LoadRuntime: %v", tc.name, err)
		}
		m, ok := snap.Model("m")
		if !ok {
			t.Fatalf("%s: model missing", tc.name)
		}
		if !reflect.DeepEqual(m.Recovery.Retry, defaultWantedRetry) {
			t.Errorf("%s: retry = %+v, want the defaults %+v", tc.name, m.Recovery.Retry, defaultWantedRetry)
		}
		if m.Recovery.Retry != m.Chain[0].Recovery.Retry {
			t.Errorf("%s: model policy does not mirror Chain[0]", tc.name)
		}
	}
}

// TestLoadRuntimeRetryPolicyOverrides pins field-by-field overrides: each
// set field lands in the effective policy, unset fields keep their defaults,
// and an initial above the omitted max lifts the ceiling to the initial
// (the legacy block's documented floor for a ceiling nobody set).
func TestLoadRuntimeRetryPolicyOverrides(t *testing.T) {
	snap, err := LoadRuntime([]byte(chainRuntime(
		"    provider: pa\n    upstream-model: up-a\n"+
			// 45s is a window the legacy block accepted before the recovery
			// policy existed; it must still load against the unstated envelope
			// defaults, which is what makes this a compatibility test and not
			// just an override test.
			"    retries:\n      max-retries: 3\n      max-elapsed: 45s\n"+
			"      backoff:\n        initial: 500ms\n        max: 5s\n        jitter: 0.5\n", "")))
	if err != nil {
		t.Fatalf("LoadRuntime: %v", err)
	}
	m, _ := snap.Model("m")
	want := recovery.RetryPolicy{
		MaxRetries: 3,
		MaxElapsed: 45 * time.Second,
		Backoff:    recovery.BackoffPolicy{Initial: 500 * time.Millisecond, Max: 5 * time.Second, Jitter: 0.5},
		// The legacy block has no on-exhausted key: it inherits the default.
		OnExhausted: recovery.ActionFallback,
	}
	if !reflect.DeepEqual(m.Recovery.Retry, want) {
		t.Errorf("retry = %+v, want %+v", m.Recovery.Retry, want)
	}

	snap, err = LoadRuntime([]byte(chainRuntime(
		"    provider: pa\n    upstream-model: up-a\n"+
			"    retries:\n      backoff:\n        initial: 3s\n", "")))
	if err != nil {
		t.Fatalf("initial-above-default-max rejected: %v", err)
	}
	m, _ = snap.Model("m")
	if m.Recovery.Retry.Backoff.Max != 3*time.Second || m.Recovery.Retry.Backoff.Initial != 3*time.Second {
		t.Errorf("omitted max with initial 3s = %v, want the initial lifted to the ceiling", m.Recovery.Retry.Backoff)
	}
}

// TestLoadRuntimeRetryPolicyRejections pins the validation matrix: every
// invalid shape rejects the whole file with fixed text (no operator input
// echoed), disabled-value fields included. The bounds themselves belong to
// the recovery domain now, so the messages name the domain's positions.
func TestLoadRuntimeRetryPolicyRejections(t *testing.T) {
	for _, tc := range []struct {
		name string
		yaml string
	}{
		{"negative retries", "    retries:\n      max-retries: -1\n"},
		{"oversized retries", "    retries:\n      max-retries: 9\n"},
		{"bad elapsed duration", "    retries:\n      max-elapsed: soon\n"},
		{"tiny elapsed", "    retries:\n      max-elapsed: 0s\n"},
		{"huge elapsed", "    retries:\n      max-elapsed: 5m\n"},
		{"bad initial duration", "    retries:\n      backoff:\n        initial: fast\n"},
		{"zero initial", "    retries:\n      backoff:\n        initial: 0s\n"},
		{"bad max duration", "    retries:\n      backoff:\n        max: 2seconds\n"},
		{"max below initial", "    retries:\n      backoff:\n        initial: 2s\n        max: 1s\n"},
		{"negative jitter", "    retries:\n      backoff:\n        jitter: -0.1\n"},
		{"jitter above one", "    retries:\n      backoff:\n        jitter: 1.5\n"},
		{"unknown field", "    retries:\n      max-attempts: 2\n"},
		{"unknown backoff field", "    retries:\n      backoff:\n        multiplier: 2\n"},
	} {
		_, err := LoadRuntime([]byte(chainRuntime("    provider: pa\n    upstream-model: up-a\n"+tc.yaml, "")))
		if err == nil {
			t.Errorf("%s: accepted, want rejection", tc.name)
			continue
		}
		if strings.Contains(err.Error(), "soon") || strings.Contains(err.Error(), "fast") || strings.Contains(err.Error(), "2seconds") {
			t.Errorf("%s: rejection echoes operator input: %v", tc.name, err)
		}
	}
}
