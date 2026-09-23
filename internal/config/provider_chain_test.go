package config

import (
	"strings"
	"testing"

	"openai-compatible-injector/internal/recovery"
)

// chainRuntime is the minimal chain harness: providers pa/pb/pc (pa on a
// direct transport, pb on a proxy, pc with none) plus the given model body
// and optional trailing top-level blocks.
func chainRuntime(modelBody, extra string) string {
	return "api-key: unit-test-key\ntransports:\n" +
		"  t1:\n    type: direct\n" +
		"  t2:\n    type: proxy\n    proxy: http://127.0.0.1:9090\n" +
		"providers:\n" +
		"  pa:\n    base-url: https://a.example/v1\n    transport: t1\n" +
		"  pb:\n    base-url: https://b.example/v1\n    transport: t2\n" +
		"  pc:\n    base-url: https://c.example/v1\n" +
		"models:\n  m:\n" + modelBody + extra
}

// TestLoadRuntimeLegacyFormsBuildOneCandidateChain pins the normalization
// rule the handler's uniform walk rests on: both legacy route forms build
// exactly one chain candidate, and the flattened Model fields mirror it.
func TestLoadRuntimeLegacyFormsBuildOneCandidateChain(t *testing.T) {
	snap, err := LoadRuntime([]byte(chainRuntime(
		"    provider: pa\n    upstream-model: up-a\n    injection-prompt: stay honest\n", "")))
	if err != nil {
		t.Fatalf("provider form rejected: %v", err)
	}
	m, ok := snap.Model("m")
	if !ok || len(m.Chain) != 1 {
		t.Fatalf("provider form: chain = %v (ok=%v), want one candidate", m.Chain, ok)
	}
	c := m.Chain[0]
	if c.Provider != "pa" || c.UpstreamModel != "up-a" || c.Endpoint.Host != "a.example" {
		t.Errorf("provider form: candidate = %+v", c)
	}
	if m.Provider != c.Provider || m.Endpoint != c.Endpoint ||
		m.UpstreamModel != c.UpstreamModel || m.Transport.Key() != c.Transport.Key() {
		t.Errorf("provider form: flattened fields do not mirror Chain[0]: %+v vs %+v", m, c)
	}
	if c.Label() != "pa" {
		t.Errorf("provider form: label = %q, want the providers-table name", c.Label())
	}

	snap, err = LoadRuntime([]byte("api-key: unit-test-key\nmodels:\n  m:\n    endpoint: https://inline.example/v1\n    upstream-model: up-i\n"))
	if err != nil {
		t.Fatalf("inline form rejected: %v", err)
	}
	m, ok = snap.Model("m")
	if !ok || len(m.Chain) != 1 {
		t.Fatalf("inline form: chain = %v, want one candidate", m.Chain)
	}
	if m.Chain[0].Provider != "" || m.Chain[0].UpstreamModel != "up-i" {
		t.Errorf("inline form: candidate = %+v", m.Chain[0])
	}
	if got := m.Chain[0].Label(); got != "https://inline.example" {
		t.Errorf("inline form: label = %q, want the endpoint origin", got)
	}
}

// TestLoadRuntimeProviderChainShape pins the chain schema: order preserved,
// per-candidate upstream-model, model-level injection prompt and policy,
// and every candidate's transport in the egress closure — not just the
// primary's.
func TestLoadRuntimeProviderChainShape(t *testing.T) {
	snap, err := LoadRuntime([]byte(chainRuntime(
		"    injection-prompt: stay honest\n"+
			"    providers:\n"+
			"      - provider: pa\n        upstream-model: up-a\n"+
			"      - provider: pb\n        upstream-model: up-b\n"+
			"      - provider: pc\n        upstream-model: up-c\n",
		"provider-fallback:\n  enabled: true\n  max-attempts: 3\n")))
	if err != nil {
		t.Fatalf("LoadRuntime: %v", err)
	}
	m, ok := snap.Model("m")
	if !ok {
		t.Fatal("model missing")
	}
	if len(m.Chain) != 3 {
		t.Fatalf("chain length = %d, want 3", len(m.Chain))
	}
	wantHosts := []string{"a.example", "b.example", "c.example"}
	wantModels := []string{"up-a", "up-b", "up-c"}
	wantLabels := []string{"pa", "pb", "pc"}
	for i, c := range m.Chain {
		if c.Provider != wantLabels[i] || c.UpstreamModel != wantModels[i] || c.Endpoint.Host != wantHosts[i] {
			t.Errorf("chain[%d] = %+v, want provider %s host %s model %s", i, c, wantLabels[i], wantHosts[i], wantModels[i])
		}
		if c.Label() != wantLabels[i] {
			t.Errorf("chain[%d].Label() = %q, want %q", i, c.Label(), wantLabels[i])
		}
	}
	// The proxy transport belongs to the SECOND candidate; the direct one
	// is shared by pa and pc (pc has no transport reference).
	if m.Chain[0].Transport.Kind == m.Chain[1].Transport.Kind {
		t.Errorf("chain[0] and chain[1] resolved to the same transport kind")
	}
	// The flattened primary mirrors chain[0]; injection stays model-level.
	if m.Provider != "pa" || m.UpstreamModel != "up-a" || m.InjectionPrompt != "stay honest" {
		t.Errorf("flattened primary = %+v", m)
	}
	// The egress closure covers every candidate: direct and proxy both
	// present (and deduped — pc's implicit direct collapses into pa's).
	ts := snap.Transports()
	kinds := map[string]int{}
	for _, tc := range ts {
		kinds[tc.Key()]++
	}
	if len(ts) != 2 {
		t.Errorf("transports closure = %d entries (%v), want the deduped direct+proxy pair", len(ts), kinds)
	}

	// The legacy provider-fallback block normalizes into the model's
	// effective fallback policy: enabled, reaching three candidates.
	fb := m.Recovery.Fallback
	if !fb.Enabled || fb.MaxCandidates != 3 || fb.OnExhausted != recovery.ActionTerminal {
		t.Errorf("Recovery.Fallback = %+v, want enabled at 3 candidates", fb)
	}
}

// TestLoadRuntimeProviderChainRejections pins the whole-file rejections of
// the chain schema, and the no-echo rule over them: the provider name (the
// position a pasted credential lands in) never appears in the error.
func TestLoadRuntimeProviderChainRejections(t *testing.T) {
	cases := []struct {
		name      string
		modelBody string
		want      string
	}{
		{"chain next to provider", "    provider: pa\n    upstream-model: x\n    providers:\n      - provider: pb\n        upstream-model: y\n", "mutually exclusive"},
		{"chain next to endpoint", "    endpoint: https://x.example/v1\n    providers:\n      - provider: pb\n        upstream-model: y\n", "mutually exclusive"},
		{"unknown provider ref", "    providers:\n      - provider: ghost\n        upstream-model: y\n", "provider reference is unknown"},
		{"duplicate provider ref", "    providers:\n      - provider: pa\n        upstream-model: y\n      - provider: pa\n        upstream-model: z\n", "duplicate of an earlier candidate"},
		{"candidate without provider", "    providers:\n      - upstream-model: y\n", "provider is required"},
		{"candidate without upstream-model", "    providers:\n      - provider: pa\n", "upstream-model is required"},
		{"empty chain", "    providers: []\n", "requires at least one candidate"},
	}
	for _, tc := range cases {
		_, err := LoadRuntime([]byte(chainRuntime(tc.modelBody, "")))
		if err == nil {
			t.Errorf("%s: accepted", tc.name)
			continue
		}
		if !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: err = %q, want %q", tc.name, err, tc.want)
		}
		if strings.Contains(err.Error(), "ghost") || strings.Contains(err.Error(), "pa") {
			t.Errorf("%s: rejection echoes operator input: %v", tc.name, err)
		}
	}
}

// TestLoadRuntimeProviderFallbackPolicy pins the legacy top-level policy
// block as normalized into the effective fallback policy: absent selects the
// defaults (enabled, two candidates), a present block is validated even when
// it disables the feature, and the candidate reach is bounded. Disabling the
// walk states a one-candidate reach, because a disabled fallback policy that
// still claimed a longer reach would be a contradiction.
func TestLoadRuntimeProviderFallbackPolicy(t *testing.T) {
	cases := []struct {
		name    string
		block   string
		enabled bool
		max     int
		reject  string
	}{
		{name: "absent", block: "", enabled: true, max: 2},
		{name: "null", block: "provider-fallback:\n", enabled: true, max: 2},
		{name: "explicit defaults", block: "provider-fallback:\n  enabled: true\n  max-attempts: 2\n", enabled: true, max: 2},
		{name: "disabled", block: "provider-fallback:\n  enabled: false\n", enabled: false, max: 1},
		{name: "single attempt", block: "provider-fallback:\n  max-attempts: 1\n", enabled: true, max: 1},
		{name: "cap", block: "provider-fallback:\n  max-attempts: 8\n", enabled: true, max: 8},
		{name: "zero attempts", block: "provider-fallback:\n  max-attempts: 0\n", reject: "fallback.max-candidates"},
		{name: "over cap", block: "provider-fallback:\n  max-attempts: 9\n", reject: "fallback.max-candidates"},
		{name: "negative", block: "provider-fallback:\n  max-attempts: -2\n", reject: "fallback.max-candidates"},
		{name: "disabled with a reach", block: "provider-fallback:\n  enabled: false\n  max-attempts: 3\n", reject: "fallback.max-candidates"},
	}
	for _, tc := range cases {
		data := chainRuntime("    provider: pa\n    upstream-model: up-a\n", tc.block)
		snap, err := LoadRuntime([]byte(data))
		if tc.reject != "" {
			if err == nil {
				t.Errorf("%s: accepted", tc.name)
			} else if !strings.Contains(err.Error(), tc.reject) {
				t.Errorf("%s: err = %q, want %q", tc.name, err, tc.reject)
			}
			continue
		}
		if err != nil {
			t.Errorf("%s: rejected: %v", tc.name, err)
			continue
		}
		m, _ := snap.Model("m")
		fb := m.Recovery.Fallback
		if fb.Enabled != tc.enabled || fb.MaxCandidates != tc.max {
			t.Errorf("%s: fallback = %+v, want enabled=%v max-candidates=%d", tc.name, fb, tc.enabled, tc.max)
		}
	}
}
