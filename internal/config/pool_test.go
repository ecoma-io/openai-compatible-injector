package config

import (
	"strings"
	"testing"
	"time"

	"openai-compatible-injector/internal/transport"
)

// poolRuntime embeds a transports entry named `t` (the pool under test)
// into an otherwise valid runtime that routes one model through it. The
// table also carries lan-egress/socks-egress endpoints and another pool —
// the latter sorts before `t`, so a member reference to it exercises the
// pass-2 ordering (an already-built pool must still be refused).
func poolRuntime(poolBody string) string {
	return `
api-key: unit-test-key
transports:
  lan-egress:
    type: proxy
    proxy: http://127.0.0.1:20130
  socks-egress:
    type: proxy
    proxy: "socks5://user:secret@10.0.0.5:30121"
  other-pool:
    type: pool
    members: [lan-egress]
  t:
` + poolBody + `
providers:
  kilo:
    base-url: https://api.kilo.example/v1
    transport: t
models:
  kilo-model:
    provider: kilo
    upstream-model: qwen-3-coder
`
}

func poolModel(t *testing.T, poolBody string) transport.Config {
	t.Helper()
	s := mustSnapshot(t, poolRuntime(poolBody))
	m, ok := s.Model("kilo-model")
	if !ok {
		t.Fatal("model kilo-model not found")
	}
	return m.Transport
}

// TestLoadRuntimePoolFlattensIntoModel pins the full-shape happy path: the
// mapping and bare member forms coexist, per-member eligibility attributes
// land on the right member, and the untouched member carries every default
// (streaming on, weight 1, no caps).
func TestLoadRuntimePoolFlattensIntoModel(t *testing.T) {
	tc := poolModel(t, `    type: pool
    members:
      - transport: lan-egress
        max-body-bytes: 4718592
        max-concurrency: 4
        streaming: false
        weight: 2
      - socks-egress
`)
	if tc.Kind != transport.EgressPool || tc.Pool == nil {
		t.Fatalf("Transport = %+v, want an egress pool", tc)
	}
	p := tc.Pool
	if len(p.Members) != 2 {
		t.Fatalf("members = %d, want 2", len(p.Members))
	}
	first, second := p.Members[0], p.Members[1]
	if first.Endpoint.Kind != transport.Proxy || first.Endpoint.ProxyURL.String() != "http://127.0.0.1:20130" {
		t.Errorf("member 1 endpoint = %+v, want lan-egress", first.Endpoint)
	}
	if first.MaxBodyBytes != 4718592 {
		t.Errorf("member 1 max-body-bytes = %d, want 4718592", first.MaxBodyBytes)
	}
	if first.MaxConcurrency != 4 || first.Streaming || first.Weight != 2 {
		t.Errorf("member 1 = conc %d streaming %v weight %d, want 4/false/2", first.MaxConcurrency, first.Streaming, first.Weight)
	}
	if second.Endpoint.Kind != transport.Proxy || second.Endpoint.ProxyURL.Scheme != "socks5" {
		t.Errorf("member 2 endpoint = %+v, want socks-egress", second.Endpoint)
	}
	if second.Endpoint.ProxyURL.User == nil || second.Endpoint.ProxyURL.User.Username() != "user" {
		t.Error("member 2 proxy userinfo lost")
	}
	if !second.Streaming || second.Weight != 1 || second.MaxBodyBytes != 0 || second.MaxConcurrency != 0 {
		t.Errorf("member 2 defaults = %+v, want streaming/weight1/no caps", second)
	}
	// A pool member endpoint config must equal the standalone transport's
	// content — the registry keys shared clients on it.
	if second.Endpoint.Key() != (transport.Config{Kind: transport.Proxy, ProxyURL: second.Endpoint.ProxyURL}).Key() {
		t.Error("member endpoint key does not match its content")
	}
}

// TestLoadRuntimePoolPolicyDefaults pins the absent-block defaults:
// round_robin, fallback on at 3 distinct dials, health on at 3 strikes per
// 30s cooldown.
func TestLoadRuntimePoolPolicyDefaults(t *testing.T) {
	tc := poolModel(t, `    type: pool
    members: [lan-egress]
`)
	p := tc.Pool
	if p.Strategy != transport.RoundRobin {
		t.Errorf("Strategy = %v, want round_robin", p.Strategy)
	}
	if p.Fallback != (transport.FallbackPolicy{Enabled: true, MaxAttempts: 3}) {
		t.Errorf("Fallback = %+v, want enabled/3", p.Fallback)
	}
	if p.Health != (transport.HealthPolicy{Enabled: true, FailureThreshold: 3, Cooldown: 30 * time.Second}) {
		t.Errorf("Health = %+v, want enabled/3/30s", p.Health)
	}
}

// TestLoadRuntimePoolPolicyOverrides pins the explicit policy forms,
// including the disabled-health normalization: enabled: false zeroes the
// threshold so the runtime cannot misread a configured-but-inert count.
func TestLoadRuntimePoolPolicyOverrides(t *testing.T) {
	tc := poolModel(t, `    type: pool
    members: [lan-egress]
    strategy: weighted_round_robin
    fallback:
      enabled: false
      max-attempts: 5
    health:
      enabled: false
      failure-threshold: 5
      cooldown: 2s
`)
	p := tc.Pool
	if p.Strategy != transport.WeightedRoundRobin {
		t.Errorf("Strategy = %v, want weighted_round_robin", p.Strategy)
	}
	if p.Fallback != (transport.FallbackPolicy{Enabled: false, MaxAttempts: 5}) {
		t.Errorf("Fallback = %+v, want disabled/5", p.Fallback)
	}
	if p.Health != (transport.HealthPolicy{Enabled: false, FailureThreshold: 0, Cooldown: 2 * time.Second}) {
		t.Errorf("Health = %+v, want disabled/0/2s", p.Health)
	}
}

// TestLoadRuntimePoolValidation is the pool rejection matrix: every field
// out of place, reference that cannot resolve, and bound out of range
// rejects the whole file.
func TestLoadRuntimePoolValidation(t *testing.T) {
	cases := []struct {
		name string
		body string
		want string
	}{
		{"pool with proxy", "    type: pool\n    proxy: http://127.0.0.1:8080\n    members: [lan-egress]", "must not set a proxy URL"},
		{"members on direct", "    type: direct\n    members: [lan-egress]", "members are only valid for type pool"},
		{"strategy on proxy", "    type: proxy\n    proxy: http://127.0.0.1:8080\n    strategy: round_robin", "strategy is only valid for type pool"},
		{"fallback on direct", "    type: direct\n    fallback:\n      enabled: false", "fallback is only valid for type pool"},
		{"health on proxy", "    type: proxy\n    proxy: http://127.0.0.1:8080\n    health:\n      enabled: false", "health is only valid for type pool"},
		{"unknown member ref", "    type: pool\n    members: [ghost-egress]", "references an unknown transport"},
		{"member is a pool built earlier", "    type: pool\n    members: [other-pool]", "must reference a direct or proxy transport"},
		{"member is this pool", "    type: pool\n    members: [t]", "must reference a direct or proxy transport"},
		{"duplicate member", "    type: pool\n    members: [lan-egress, lan-egress]", "references the same transport twice"},
		{"empty member ref", "    type: pool\n    members:\n      - \"  \"", "reference is required"},
		{"member is a sequence", "    type: pool\n    members:\n      - [lan-egress]", "must be a transport name or a mapping"},
		{"unknown member field", "    type: pool\n    members:\n      - transport: lan-egress\n        gate: 1", "unknown field"},
		{"weight zero", "    type: pool\n    members:\n      - transport: lan-egress\n        weight: 0", "must be at least 1"},
		{"weight past cap", "    type: pool\n    members:\n      - transport: lan-egress\n        weight: 1000000001", "unreasonably large"},
		{"negative body cap", "    type: pool\n    members:\n      - transport: lan-egress\n        max-body-bytes: -1", "must not be negative"},
		{"negative concurrency", "    type: pool\n    members:\n      - transport: lan-egress\n        max-concurrency: -1", "must not be negative"},
		{"max-attempts zero", "    type: pool\n    members: [lan-egress]\n    fallback:\n      max-attempts: 0", "must be at least 1"},
		{"max-attempts past cap", "    type: pool\n    members: [lan-egress]\n    fallback:\n      max-attempts: 17", "must be at most 16"},
		{"negative threshold", "    type: pool\n    members: [lan-egress]\n    health:\n      failure-threshold: -1", "must not be negative"},
		{"cooldown under 1s", "    type: pool\n    members: [lan-egress]\n    health:\n      cooldown: 500ms", "must be at least 1s"},
		{"cooldown not a duration", "    type: pool\n    members: [lan-egress]\n    health:\n      cooldown: soon", "must be a valid duration"},
		{"unknown strategy", "    type: pool\n    members: [lan-egress]\n    strategy: random", "strategy must be one of round_robin, weighted_round_robin"},
		{"unknown fallback key", "    type: pool\n    members: [lan-egress]\n    fallback:\n      enabled: true\n      retries: 2", "unmarshal errors"},
	}
	for _, tc := range cases {
		_, err := LoadRuntime([]byte(poolRuntime(tc.body)))
		if err == nil {
			t.Errorf("%s: accepted", tc.name)
			continue
		}
		if !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: err = %q, want it to contain %q", tc.name, err.Error(), tc.want)
		}
	}
}

// TestLoadRuntimePoolErrorsDoNotEcho sweeps the pool value positions for
// operator input: a marker planted in each must never surface in the
// rejection error — error text reaches logs verbatim. The value-type
// failures go further and reduce to line numbers only.
func TestLoadRuntimePoolErrorsDoNotEcho(t *testing.T) {
	cases := []struct {
		name   string
		body   string
		marker string
		// lineOnly marks the paths whose rejection must be the redacted
		// line-number form (yaml.TypeError quoting the offending scalar).
		lineOnly bool
	}{
		{"unknown member ref name", "    type: pool\n    members: [s3cr3t-egress]", "s3cr3t-egress", false},
		{"strategy value", "    type: pool\n    members: [lan-egress]\n    strategy: s3cr3t-strategy", "s3cr3t-strategy", false},
		{"cooldown value", "    type: pool\n    members: [lan-egress]\n    health:\n      cooldown: s3cr3t", "s3cr3t", false},
		{"member field key", "    type: pool\n    members:\n      - transport: lan-egress\n        s3cr3t-field: 1", "s3cr3t-field", false},
		{"weight value", "    type: pool\n    members:\n      - transport: lan-egress\n        weight: s3cr3t", "s3cr3t", true},
		{"streaming value", "    type: pool\n    members:\n      - transport: lan-egress\n        streaming: s3cr3t", "s3cr3t", true},
		{"member ref is a mapping", "    type: pool\n    members:\n      - transport:\n          s3cr3t: 1", "s3cr3t", true},
	}
	for _, tc := range cases {
		_, err := LoadRuntime([]byte(poolRuntime(tc.body)))
		if err == nil {
			t.Errorf("%s: accepted", tc.name)
			continue
		}
		msg := err.Error()
		if strings.Contains(msg, tc.marker) {
			t.Errorf("%s: error echoes operator input %q: %q", tc.name, tc.marker, msg)
		}
		if tc.lineOnly && !strings.Contains(msg, "line(s)") {
			t.Errorf("%s: err = %q, want the redacted line-number form", tc.name, msg)
		}
	}
}

// TestLoadRuntimeEgressClosure pins the snapshot's retained transport set:
// the pool itself, every member endpoint, and standalone transports — with
// the shared member deduplicated to one entry. This is the set the registry
// retains on publish.
func TestLoadRuntimeEgressClosure(t *testing.T) {
	s := mustSnapshot(t, `
api-key: unit-test-key
transports:
  relay:
    type: proxy
    proxy: http://127.0.0.1:20130
  socks:
    type: proxy
    proxy: socks5://10.0.0.5:30121
  kilo-pool:
    type: pool
    members: [relay, socks]
providers:
  kilo:
    base-url: https://api.kilo.example/v1
    transport: kilo-pool
  relayed:
    base-url: https://api.relayed.example/v1
    transport: relay
models:
  a:
    provider: kilo
    upstream-model: x
  b:
    provider: relayed
    upstream-model: y
`)
	ts := s.Transports()
	if len(ts) != 3 {
		t.Fatalf("Transports() = %d entries, want 3 (pool + 2 endpoints)", len(ts))
	}
	var poolSeen bool
	proxySeen := map[string]int{}
	for _, tc := range ts {
		switch tc.Kind {
		case transport.EgressPool:
			poolSeen = true
		case transport.Proxy:
			proxySeen[tc.ProxyURL.String()]++
		case transport.Direct:
			t.Errorf("Transports() carries an unexpected direct entry: %+v", tc)
		}
	}
	if !poolSeen {
		t.Error("Transports() lost the pool itself")
	}
	if proxySeen["http://127.0.0.1:20130"] != 1 || proxySeen["socks5://10.0.0.5:30121"] != 1 {
		t.Errorf("member endpoints = %v, want each exactly once", proxySeen)
	}
}

// TestLoadRuntimePoolIdentityAcrossLoads pins the reload identity at the
// config layer: two loads of the same pool YAML agree on the pool's
// identity (content, not pointers), while a strategy change or a member
// reorder produces a different one — fresh state on reload.
func TestLoadRuntimePoolIdentityAcrossLoads(t *testing.T) {
	poolBody := `    type: pool
    members: [lan-egress, socks-egress]
`
	id := func(body string) string {
		return poolModel(t, body).Pool.Identity()
	}
	if first, second := id(poolBody), id(poolBody); first != second {
		t.Errorf("identical pool YAML produced different identities: %q vs %q", first, second)
	}
	changed := strings.Replace(poolBody, "[lan-egress, socks-egress]", "[socks-egress, lan-egress]", 1)
	if id(poolBody) == id(changed) {
		t.Error("member reorder kept the identity")
	}
	weighted := strings.Replace(poolBody, "members:", "strategy: weighted_round_robin\n    members:", 1)
	if id(poolBody) == id(weighted) {
		t.Error("strategy change kept the identity")
	}
}
