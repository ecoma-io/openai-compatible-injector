package config

import (
	"strings"
	"testing"
	"time"
)

// keepAliveYAML renders a valid runtime file with the given top-level
// sse-keep-alive block appended verbatim.
func keepAliveYAML(block string) string {
	return validRuntime() + block
}

// TestSSEKeepAliveDefaults pins the default-on contract: this proxy is
// deployed behind Cloudflare, where a silent HTTP/2 stream dies at ~125s,
// so an unconfigured deployment must keep silent streams alive at 15s —
// the quiet direction here is the feature silently off, and a deployment
// that never writes the block must not regress to being cut.
func TestSSEKeepAliveDefaults(t *testing.T) {
	cases := []struct {
		name  string
		yaml  string
		on    bool
		every time.Duration
	}{
		{"block absent", keepAliveYAML(""), true, defaultSSEKeepAliveInterval},
		{"null block", keepAliveYAML("sse-keep-alive:\n"), true, defaultSSEKeepAliveInterval},
		{"empty block", keepAliveYAML("sse-keep-alive: {}\n"), true, defaultSSEKeepAliveInterval},
		{"enabled only", keepAliveYAML("sse-keep-alive:\n  enabled: true\n"), true, defaultSSEKeepAliveInterval},
		{"interval only", keepAliveYAML("sse-keep-alive:\n  interval: 30s\n"), true, 30 * time.Second},
		{"explicit defaults", keepAliveYAML("sse-keep-alive:\n  enabled: true\n  interval: 15s\n"), true, 15 * time.Second},
		{"disabled keeps interval", keepAliveYAML("sse-keep-alive:\n  enabled: false\n  interval: 45s\n"), false, 45 * time.Second},
		{"minute spelling", keepAliveYAML("sse-keep-alive:\n  interval: 1m\n"), true, time.Minute},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := mustSnapshot(t, tc.yaml)
			ka := s.SSEKeepAlive()
			if ka.Enabled != tc.on {
				t.Fatalf("Enabled = %v, want %v", ka.Enabled, tc.on)
			}
			if ka.Interval != tc.every {
				t.Fatalf("Interval = %v, want %v", ka.Interval, tc.every)
			}
		})
	}
}

// TestSSEKeepAliveValidation pins the reject matrix: the interval must be
// a real duration of at least 1s — zero, negative, sub-second, and
// unparseable values reject the whole file, which routes a bad reload onto
// the last-known-good path — and unknown keys inside the block reject via
// strict decoding. No rejection text echoes the configured value: the
// interval position can carry a botched paste of anything, and
// time.ParseDuration errors quote their input.
func TestSSEKeepAliveValidation(t *testing.T) {
	cases := []struct {
		name string
		yaml string
		want string
	}{
		{"zero interval", keepAliveYAML("sse-keep-alive:\n  interval: 0s\n"), "at least 1s"},
		{"negative interval", keepAliveYAML("sse-keep-alive:\n  interval: -5s\n"), "at least 1s"},
		{"sub-second interval", keepAliveYAML("sse-keep-alive:\n  interval: 500ms\n"), "at least 1s"},
		{"unitless zero", keepAliveYAML("sse-keep-alive:\n  interval: \"0\"\n"), "at least 1s"},
		{"unparseable", keepAliveYAML("sse-keep-alive:\n  interval: soon\n"), "valid duration"},
		{"secret-bearing garbage", keepAliveYAML("sse-keep-alive:\n  interval: SECRET_MARKER\n"), "valid duration"},
		{"unknown key in block", keepAliveYAML("sse-keep-alive:\n  intervall: 15s\n"), ""},
		{"still validated when disabled", keepAliveYAML("sse-keep-alive:\n  enabled: false\n  interval: 0s\n"), "at least 1s"},
		{"enabled wrong type", keepAliveYAML("sse-keep-alive:\n  enabled: yes-please\n"), ""},
		{"interval wrong type", keepAliveYAML("sse-keep-alive:\n  interval: [15s]\n"), ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := LoadRuntime([]byte(tc.yaml))
			if err == nil {
				t.Fatal("expected rejection")
			}
			if tc.want != "" && !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want containing %q", err, tc.want)
			}
			if strings.Contains(err.Error(), "SECRET_MARKER") {
				t.Errorf("error text echoes the configured interval: %v", err)
			}
		})
	}
}

// TestSSEKeepAliveTopLevelLegality pins the two planes around the new key:
// sse-keep-alive is a legal runtime-file citizen alongside models, api-key
// and log-level, while bootstrap-plane keys stay rejected wherever they
// appear — including inside the new block, where strict decoding catches
// them as unknown keys.
func TestSSEKeepAliveTopLevelLegality(t *testing.T) {
	if _, err := LoadRuntime([]byte(keepAliveYAML("log-level: debug\n"))); err != nil {
		t.Fatalf("all four legal top-level keys rejected: %v", err)
	}
	if _, err := LoadRuntime([]byte("sse-keep-alive:\n  interval: 15s\n" + validRuntime())); err != nil {
		t.Fatalf("block before models rejected: %v", err)
	}
	if _, err := LoadRuntime([]byte(keepAliveYAML("listen: :8080\n"))); err == nil {
		t.Fatal("bootstrap key still rejected alongside the new legal key")
	}
}
