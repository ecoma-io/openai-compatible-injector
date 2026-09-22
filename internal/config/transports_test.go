package config

import (
	"strings"
	"testing"

	"openai-compatible-injector/internal/transport"
)

// providerRuntime is the full-shape fixture: two named transports, two
// providers (one direct by reference, one proxied), one model per provider
// plus one legacy inline-endpoint model — every form the schema accepts.
func providerRuntime() string {
	return `
api-key: unit-test-key
transports:
  lan-egress:
    type: proxy
    proxy: http://127.0.0.1:8080
  socks-egress:
    type: proxy
    proxy: "socks5h://user:pass@10.0.0.5:1080"
providers:
  opencode:
    base-url: https://api.opencode.example/v1
    transport: lan-egress
  kilo:
    base-url: https://api.kilo.example/v1
    transport: socks-egress
  bare:
    base-url: https://api.bare.example/v1
models:
  reviewer:
    provider: opencode
    upstream-model: gpt-5-pro
  coder:
    provider: socks-provider
    upstream-model: qwen-3-coder
  legacy:
    endpoint: https://api.legacy.example/v1
    upstream-model: legacy-model
`
}

// TestLoadRuntimeProvidersFlattenIntoModels pins the resolution: a model's
// provider reference resolves to the provider's base URL and transport
// with the provider's name carried as metadata; the legacy endpoint form
// stays a direct transport with no provider; a provider without a
// transport reference defaults to direct.
func TestLoadRuntimeProvidersFlattenIntoModels(t *testing.T) {
	data := strings.Replace(providerRuntime(), "socks-provider", "kilo", 1)
	s := mustSnapshot(t, data)

	m, ok := s.Model("reviewer")
	if !ok {
		t.Fatal("model reviewer not found")
	}
	if m.Provider != "opencode" {
		t.Errorf("Provider = %q, want opencode", m.Provider)
	}
	if m.Endpoint.String() != "https://api.opencode.example/v1" {
		t.Errorf("Endpoint = %s, want the provider base-url", m.Endpoint)
	}
	if m.Transport.Kind != transport.Proxy || m.Transport.ProxyURL.String() != "http://127.0.0.1:8080" {
		t.Errorf("Transport = %+v, want the http proxy", m.Transport)
	}

	m, ok = s.Model("coder")
	if !ok {
		t.Fatal("model coder not found")
	}
	if m.Provider != "kilo" || m.Endpoint.String() != "https://api.kilo.example/v1" {
		t.Errorf("coder = %+v", m)
	}
	if m.Transport.Kind != transport.Proxy || m.Transport.ProxyURL.Scheme != "socks5h" {
		t.Errorf("coder Transport = %+v, want socks5h", m.Transport)
	}
	if u := m.Transport.ProxyURL; u.User == nil || u.User.Username() != "user" {
		t.Errorf("socks5h proxy userinfo lost: %v", u.User)
	}

	m, ok = s.Model("legacy")
	if !ok {
		t.Fatal("model legacy not found")
	}
	if m.Provider != "" || m.Transport.Kind != transport.Direct || m.Transport.ProxyURL != nil {
		t.Errorf("legacy = %+v, want no provider and a direct transport", m)
	}

	// Two models share the direct-defaulting provider "bare"? Not
	// referenced here — but the unreferenced table entries were still
	// validated; the load succeeding is that assertion. Snapshot transports
	// must carry exactly the distinct set the models use: lan-egress,
	// socks-egress, direct.
	ts := s.Transports()
	if len(ts) != 3 {
		t.Fatalf("Transports() = %+v, want 3 entries", ts)
	}
	kinds := map[string]bool{}
	for _, tc := range ts {
		switch {
		case tc.Kind == transport.Direct:
			kinds["direct"] = true
		case tc.ProxyURL.String() == "http://127.0.0.1:8080":
			kinds["http"] = true
		case tc.ProxyURL.String() == "socks5h://user:pass@10.0.0.5:1080":
			kinds["socks"] = true
		}
	}
	if !kinds["direct"] || !kinds["http"] || !kinds["socks"] {
		t.Errorf("Transports() = %+v, want direct+http+socks5h", ts)
	}
}

// TestLoadRuntimeSharedProviderSharesTransport pins the dedup: models
// sharing one provider carry the identical transport Config and the
// snapshot's transport set lists it once.
func TestLoadRuntimeSharedProviderSharesTransport(t *testing.T) {
	s := mustSnapshot(t, `
api-key: unit-test-key
transports:
  egress:
    type: proxy
    proxy: http://127.0.0.1:8080
providers:
  opencode:
    base-url: https://api.opencode.example/v1
    transport: egress
models:
  a:
    provider: opencode
    upstream-model: m1
  b:
    provider: opencode
    upstream-model: m2
`)
	ma, _ := s.Model("a")
	mb, _ := s.Model("b")
	if ma.Transport.ProxyURL != mb.Transport.ProxyURL {
		t.Error("models sharing a provider carry distinct proxy URLs")
	}
	if ts := s.Transports(); len(ts) != 1 {
		t.Errorf("Transports() = %+v, want exactly one entry", ts)
	}
}

// TestLoadRuntimeTransportValidation is the transports-table matrix:
// missing type, unknown type, direct with a proxy URL, missing/blank
// proxy URL, malformed URL, unsupported scheme, missing port, and a
// path/query/fragment after the authority. Positive forms (all four
// schemes, userinfo allowed) load.
func TestLoadRuntimeTransportValidation(t *testing.T) {
	transport := func(body string) string {
		return "api-key: unit-test-key\nproviders:\n  p:\n    base-url: https://api.example/v1\n    transport: t\nmodels:\n  m:\n    provider: p\n    upstream-model: x\ntransports:\n  t:\n" + body
	}
	cases := []struct {
		name string
		body string
		want string
	}{
		{"missing type", "    proxy: http://127.0.0.1:8080", "type is required"},
		{"unknown type", "    type: egress", "type must be one of direct, proxy, pool"},
		{"pool without members", "    type: pool", "pool requires at least one member"},
		{"direct with proxy", "    type: direct\n    proxy: http://127.0.0.1:8080", "must not set a proxy URL"},
		{"proxy without url", "    type: proxy", "proxy URL is required"},
		{"proxy blank url", "    type: proxy\n    proxy: \"  \"", "proxy URL is required"},
		{"malformed url", "    type: proxy\n    proxy: 'http://%zz@egress.example:8080'", "invalid URL escape"},
		{"unsupported scheme", "    type: proxy\n    proxy: ftp://127.0.0.1:21", "proxy scheme must be one of http, https, socks5, socks5h"},
		{"missing port", "    type: proxy\n    proxy: http://127.0.0.1", "must include an explicit port"},
		{"path after authority", "    type: proxy\n    proxy: http://127.0.0.1:8080/socks", "nothing after the authority"},
		{"query after authority", "    type: proxy\n    proxy: \"http://127.0.0.1:8080?x=1\"", "nothing after the authority"},
		{"fragment after authority", "    type: proxy\n    proxy: 'http://127.0.0.1:8080#f'", "nothing after the authority"},
		{"empty name", "", "name must not be empty"},
	}
	for _, tc := range cases {
		data := transport(tc.body)
		if tc.name == "empty name" {
			// An entry keyed by whitespace-only name.
			data = strings.Replace(data, "  t:\n", "  \"  \":\n    type: direct\n", 1)
		}
		_, err := LoadRuntime([]byte(data))
		if err == nil {
			t.Errorf("%s: accepted", tc.name)
			continue
		}
		if !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: err = %q, want it to contain %q", tc.name, err, tc.want)
		}
	}
}

// TestLoadRuntimeTransportSchemes pins the four accepted proxy schemes
// and that userinfo (proxy authentication) survives parsing.
func TestLoadRuntimeTransportSchemes(t *testing.T) {
	for _, scheme := range []string{"http", "https", "socks5", "socks5h"} {
		data := "api-key: unit-test-key\nproviders:\n  p:\n    base-url: https://api.example/v1\n    transport: t\nmodels:\n  m:\n    provider: p\n    upstream-model: x\ntransports:\n  t:\n    type: proxy\n    proxy: " + scheme + "://user:pass@egress.example:8080\n"
		s, err := LoadRuntime([]byte(data))
		if err != nil {
			t.Errorf("%s: %v", scheme, err)
			continue
		}
		m, _ := s.Model("m")
		if m.Transport.ProxyURL.Scheme != scheme {
			t.Errorf("%s: parsed scheme = %q", scheme, m.Transport.ProxyURL.Scheme)
		}
		if m.Transport.ProxyURL.User == nil || m.Transport.ProxyURL.User.Username() != "user" {
			t.Errorf("%s: userinfo lost", scheme)
		}
	}
}

// TestLoadRuntimeProviderValidation is the providers-table matrix: missing
// base-url, non-http(s) scheme, userinfo on the base URL, fragment,
// unknown transport reference, empty name, trim collision.
func TestLoadRuntimeProviderValidation(t *testing.T) {
	cases := []struct {
		name    string
		prepare func(data string) string
		want    string
	}{
		{"missing base-url", func(d string) string {
			return strings.Replace(d, "    base-url: https://api.example/v1\n    transport: t\n", "", 1)
		}, "base-url is required"},
		{"ftp base-url", func(d string) string {
			return strings.Replace(d, "https://api.example", "ftp://files.example", 1)
		}, "scheme and host must be http(s)"},
		{"userinfo base-url", func(d string) string {
			return strings.Replace(d, "https://api.example", "https://user:pass@api.example", 1)
		}, "must not contain credentials"},
		{"fragment base-url", func(d string) string {
			return strings.Replace(d, "https://api.example/v1", "https://api.example/v1#frag", 1)
		}, "must not contain a fragment"},
		{"unknown transport", func(d string) string {
			return strings.Replace(d, "transport: t", "transport: nope", 1)
		}, "transport reference is unknown"},
	}
	base := "api-key: unit-test-key\ntransports:\n  t:\n    type: direct\nproviders:\n  p:\n    base-url: https://api.example/v1\n    transport: t\nmodels:\n  m:\n    provider: p\n    upstream-model: x\n"
	for _, tc := range cases {
		_, err := LoadRuntime([]byte(tc.prepare(base)))
		if err == nil {
			t.Errorf("%s: accepted", tc.name)
			continue
		}
		if !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: err = %q, want it to contain %q", tc.name, err, tc.want)
		}
	}

	// Empty and trim-colliding provider names.
	for name, want := range map[string]string{
		"\"  \"": "name must not be empty",
	} {
		data := strings.Replace(base, "  p:\n", "  "+name+":\n", 1)
		_, err := LoadRuntime([]byte(data))
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("provider name %q: err = %v, want %q", name, err, want)
		}
	}
	collide := strings.Replace(base,
		"  p:\n    base-url: https://api.example/v1\n",
		"  p:\n    base-url: https://api.example/v1\n  \" p\":\n    base-url: https://api2.example/v1\n", 1)
	_, err := LoadRuntime([]byte(collide))
	if err == nil || !strings.Contains(err.Error(), "name collides with another entry after trimming whitespace") {
		t.Errorf("provider trim collision: err = %v", err)
	}
}

// TestLoadRuntimeProviderDefaultsToDirect pins that a provider entry
// without a transport reference is a direct transport — the documented
// default, distinct from a broken reference.
func TestLoadRuntimeProviderDefaultsToDirect(t *testing.T) {
	s := mustSnapshot(t, `
api-key: unit-test-key
providers:
  bare:
    base-url: https://api.bare.example/v1
models:
  m:
    provider: bare
    upstream-model: x
`)
	m, _ := s.Model("m")
	if m.Transport.Kind != transport.Direct || m.Transport.ProxyURL != nil {
		t.Errorf("Transport = %+v, want direct", m.Transport)
	}
	if ts := s.Transports(); len(ts) != 1 || ts[0].Kind != transport.Direct {
		t.Errorf("Transports() = %+v, want one direct entry", ts)
	}
}

// TestLoadRuntimeModelProviderExclusivity pins the model-level matrix:
// provider and endpoint together reject, neither rejects, an unknown
// provider reference rejects.
func TestLoadRuntimeModelProviderExclusivity(t *testing.T) {
	base := "api-key: unit-test-key\nproviders:\n  p:\n    base-url: https://api.example/v1\nmodels:\n  m:\n    upstream-model: x\n"
	cases := []struct {
		name string
		body string
		want string
	}{
		{"both", "    provider: p\n    endpoint: https://api2.example/v1", "endpoint and provider are mutually exclusive"},
		{"neither", "", "endpoint or provider is required"},
		{"unknown provider", "    provider: ghost", "provider reference is unknown"},
	}
	for _, tc := range cases {
		data := strings.Replace(base, "    upstream-model: x\n", tc.body+"\n    upstream-model: x\n", 1)
		_, err := LoadRuntime([]byte(data))
		if err == nil {
			t.Errorf("%s: accepted", tc.name)
			continue
		}
		if !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: err = %q, want it to contain %q", tc.name, err, tc.want)
		}
	}
}

// TestLoadRuntimeProviderReferenceIsTrimmed pins that a provider reference
// with outer whitespace still resolves (the name discipline), and that an
// unstyled reference inside the provider table itself trims too.
func TestLoadRuntimeProviderReferenceIsTrimmed(t *testing.T) {
	s := mustSnapshot(t, `
api-key: unit-test-key
transports:
  t:
    type: proxy
    proxy: http://127.0.0.1:8080
providers:
  p:
    base-url: https://api.example/v1
    transport: " t "
models:
  m:
    provider: " p "
    upstream-model: x
`)
	m, _ := s.Model("m")
	if m.Provider != "p" {
		t.Errorf("Provider = %q, want p", m.Provider)
	}
	if m.Transport.Kind != transport.Proxy {
		t.Errorf("Transport = %+v, want the referenced proxy", m.Transport)
	}
}

// TestTransportErrorsNeverEchoProxyInput plants credential-shaped material
// in every rejected proxy URL position and asserts the rejection text
// stays free of it — error text reaches logs verbatim.
func TestTransportErrorsNeverEchoProxyInput(t *testing.T) {
	secret := "sup3r-s3cret-pa55word"
	for _, tc := range []struct {
		name string
		url  string
	}{
		{"bad scheme", "ftp://" + secret + "@egress.example:8080"},
		{"missing port", "http://" + secret + "@egress.example"},
		{"path", "http://egress.example:8080/" + secret},
		{"query", "http://egress.example:8080?" + secret},
		{"malformed", "ht tp://" + secret},
	} {
		data := "api-key: unit-test-key\nproviders:\n  p:\n    base-url: https://api.example/v1\n    transport: t\nmodels:\n  m:\n    provider: p\n    upstream-model: x\ntransports:\n  t:\n    type: proxy\n    proxy: '" + tc.url + "'\n"
		_, err := LoadRuntime([]byte(data))
		if err == nil {
			t.Errorf("%s: accepted", tc.name)
			continue
		}
		if strings.Contains(err.Error(), secret) {
			t.Errorf("%s: rejection echoes the secret: %v", tc.name, err)
		}
	}
}

// TestLoadRuntimeRejectsUnknownTransportEntryKey pins strict decoding one
// level down: an unknown key inside a transports entry is a reject, not a
// silently dropped setting.
func TestLoadRuntimeRejectsUnknownTransportEntryKey(t *testing.T) {
	_, err := LoadRuntime([]byte(`
api-key: unit-test-key
transports:
  t:
    type: direct
    fallback: never
providers:
  p:
    base-url: https://api.example/v1
    transport: t
models:
  m:
    provider: p
    upstream-model: x
`))
	if err == nil {
		t.Fatal("unknown transports-entry key accepted")
	}
}

// TestSnapshotTransportsIsACopy pins immutability: mutating the returned
// slice cannot rewrite the snapshot's retain set.
func TestSnapshotTransportsIsACopy(t *testing.T) {
	s := mustSnapshot(t, `
api-key: unit-test-key
models:
  m:
    endpoint: https://api.example/v1
    upstream-model: x
`)
	ts := s.Transports()
	ts[0] = transport.Config{Kind: transport.Proxy}
	if again := s.Transports(); again[0].Kind != transport.Direct {
		t.Errorf("snapshot transport set mutated through the returned slice: %+v", again)
	}
}
