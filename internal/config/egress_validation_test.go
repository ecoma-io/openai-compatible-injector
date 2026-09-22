package config

import (
	"fmt"
	"strings"
	"testing"
)

// egressRuntime builds a runtime whose transports table is the given YAML
// body plus one pool `t` whose members are the given list, routed through
// one model — the minimal harness for the pool-member identity tests.
func egressRuntime(transportsBody, membersYAML string) string {
	return "api-key: unit-test-key\ntransports:\n" + transportsBody +
		"  t:\n    type: pool\n    members:\n" + membersYAML +
		"providers:\n  p:\n    base-url: https://api.example/v1\n    transport: t\nmodels:\n  m:\n    provider: p\n    upstream-model: x\n"
}

// TestLoadRuntimePoolRejectsDuplicateResolvedEndpoints pins the member
// identity rule: two members are distinct only if their RESOLVED endpoints
// differ. Two names for the same endpoint — literal duplicates, host-case
// differences, or userinfo spelling differences that decode the same — are
// one endpoint and reject the file; genuinely different endpoints load.
func TestLoadRuntimePoolRejectsDuplicateResolvedEndpoints(t *testing.T) {
	twoDirect := "  a:\n    type: direct\n  b:\n    type: direct\n"
	twoSameProxy := "  a:\n    type: proxy\n    proxy: http://egress.example:8080\n  b:\n    type: proxy\n    proxy: http://egress.example:8080\n"
	hostCase := "  a:\n    type: proxy\n    proxy: http://Egress.example:8080\n  b:\n    type: proxy\n    proxy: http://egress.example:8080\n"
	userinfoSpelling := "  a:\n    type: proxy\n    proxy: 'socks5://us%65r:sec%72et@egress.example:1080'\n  b:\n    type: proxy\n    proxy: 'socks5://user:secret@egress.example:1080'\n"

	for _, tc := range []struct {
		name           string
		transportsBody string
	}{
		{"two direct entries", twoDirect},
		{"same proxy url twice", twoSameProxy},
		{"host case only", hostCase},
		{"userinfo percent-encoding only", userinfoSpelling},
	} {
		data := egressRuntime(tc.transportsBody, "      - a\n      - b\n")
		_, err := LoadRuntime([]byte(data))
		if err == nil {
			t.Errorf("%s: accepted two members resolving to one endpoint", tc.name)
			continue
		}
		if !strings.Contains(err.Error(), "identical to an earlier member") {
			t.Errorf("%s: err = %q, want the duplicate-endpoint rejection", tc.name, err)
		}
		if strings.Contains(err.Error(), "egress.example") || strings.Contains(err.Error(), "secret") {
			t.Errorf("%s: rejection echoes the endpoint: %v", tc.name, err)
		}
	}

	for _, tc := range []struct {
		name           string
		transportsBody string
	}{
		{"socks5 and socks5h differ", "  a:\n    type: proxy\n    proxy: socks5://egress.example:1080\n  b:\n    type: proxy\n    proxy: socks5h://egress.example:1080\n"},
		{"two different proxies", "  a:\n    type: proxy\n    proxy: http://a.example:8080\n  b:\n    type: proxy\n    proxy: http://b.example:8080\n"},
		{"direct and proxy", "  a:\n    type: direct\n  b:\n    type: proxy\n    proxy: http://egress.example:8080\n"},
	} {
		data := egressRuntime(tc.transportsBody, "      - a\n      - b\n")
		if _, err := LoadRuntime([]byte(data)); err != nil {
			t.Errorf("%s: rejected distinct endpoints: %v", tc.name, err)
		}
	}
}

// TestLoadRuntimeSocksCredentialLengthBound pins the RFC 1929 config-plane
// bound: each SOCKS5 credential field is wire-limited to 255 BYTES of
// decoded value — byte count, never rune count, never spelling length.
// Oversized fields reject the whole file; the boundary value loads; http
// proxies have no such bound.
func TestLoadRuntimeSocksCredentialLengthBound(t *testing.T) {
	const marker = "sup3r-s3cret-m4terial"
	cases := []struct {
		name     string
		user     string
		pass     string
		scheme   string
		accepted bool
	}{
		{"user one over", strings.Repeat("u", 256), "", "socks5", false},
		{"password one over", "u", strings.Repeat("p", 256), "socks5h", false},
		{"user at the limit", strings.Repeat("u", 255), "p", "socks5", true},
		{"password at the limit", "u", strings.Repeat("p", 255), "socks5h", true},
		{"multibyte counts as bytes", strings.Repeat("%C3%A9", 130), "", "socks5", false},                             // 130 runes, 260 bytes decoded
		{"multibyte at the limit", strings.Repeat("%C3%A9", 127), "", "socks5", true},                                 // 254 bytes decoded
		{"percent-escapes count decoded", strings.Repeat("%41", 100), "", "socks5", true},                             // 300 chars spelled, 100 bytes decoded
		{"decoded overflow via escapes", strings.Repeat("a", 250) + strings.Repeat("%C3%A9", 4), "", "socks5", false}, // 258 bytes decoded
		{"http proxy unbounded", strings.Repeat("u", 400), strings.Repeat("p", 400), "http", true},
		{"https proxy unbounded", strings.Repeat("u", 400), "", "https", true},
	}
	for _, tc := range cases {
		user := tc.user
		pass := tc.pass
		if !tc.accepted && user != "" && len(user) > 255 {
			user = strings.Replace(user, "u", marker[:1], 1)
		}
		raw := tc.scheme + "://" + user
		if pass != "" {
			raw += ":" + pass
		}
		raw += "@egress.example:8080"
		data := egressRuntime(fmt.Sprintf("  t2:\n    type: proxy\n    proxy: '%s'\n", raw), "      - t2\n")
		_, err := LoadRuntime([]byte(data))
		if tc.accepted {
			if err != nil {
				t.Errorf("%s: rejected: %v", tc.name, err)
			}
			continue
		}
		if err == nil {
			t.Errorf("%s: accepted an oversized credential", tc.name)
			continue
		}
		if !strings.Contains(err.Error(), "RFC 1929") {
			t.Errorf("%s: err = %q, want the RFC 1929 bound rejection", tc.name, err)
		}
		if strings.Contains(err.Error(), marker) || strings.Contains(err.Error(), strings.Repeat("u", 16)) {
			t.Errorf("%s: rejection echoes the credential: %v", tc.name, err)
		}
	}
}
