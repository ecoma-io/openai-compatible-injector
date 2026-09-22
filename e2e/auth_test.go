package e2e_test

import (
	"net/http"
	"strings"
	"testing"
)

// Client-authentication scenarios: the single configured api-key gates both
// API surfaces, the credential never travels upstream, the open surfaces
// (/healthz, method gate, unknown paths) stay open, and the key rides the
// hot-reload plane — a keyless file never disables the gate.

// Byte-exact 401 envelopes (mirrors internal/proxy): static text, no
// interpolation, so neither can echo a fragment of the presented credential.
const (
	authMissingEnvelope = `{"error":{"message":"you must provide an API key in the Authorization header (Bearer <key>)","type":"invalid_request_error","param":null,"code":null}}`
	authInvalidEnvelope = `{"error":{"message":"invalid API key","type":"invalid_request_error","param":null,"code":"invalid_api_key"}}`
)

// TestClientAuthRequired pins the gate on both API surfaces: every
// missing/malformed/wrong credential receives its byte-exact 401 envelope
// BEFORE any upstream I/O, and only the configured key produces a relayed
// request.
func TestClientAuthRequired(t *testing.T) {
	up := newFakeUpstream(t)
	up.setHandler(jsonChatHandler(chatUpstream))
	p := startSubprocess(t, startOpts{
		yaml:     runtimeYAML(chatPublic, up.url()+"/v1", chatUpstream, ""),
		logLevel: "error",
	})

	cases := []struct {
		name string
		auth string // raw Authorization header value
		want string // byte-exact 401 body
	}{
		{"non-bearer scheme", "Basic abc", authMissingEnvelope},
		{"empty token", "Bearer", authMissingEnvelope},
		{"whitespace token", "Bearer    ", authMissingEnvelope},
		{"wrong key", "Bearer not-the-configured-key", authInvalidEnvelope},
		{"key plus suffix", "Bearer " + e2eAPIKey + "-extra", authInvalidEnvelope},
	}
	for _, route := range []struct {
		path string
		body string
	}{
		{"/v1/chat/completions", chatBody},
		{"/v1/responses", `{"model":"chat-public","input":"x"}`},
	} {
		// A truly absent header needs a bare stdlib request: the suite's
		// postJSON auto-presents the default bearer and would mask the case.
		resp, err := http.Post("http://"+p.addr+route.path, "application/json",
			strings.NewReader(route.body))
		if err != nil {
			t.Fatalf("%s no header: post: %v", route.path, err)
		}
		noHdrBody := readBody(t, resp)
		if resp.StatusCode != http.StatusUnauthorized || string(noHdrBody) != authMissingEnvelope {
			t.Fatalf("%s no header: status = %d body %s, want 401 missing envelope", route.path, resp.StatusCode, noHdrBody)
		}
		for _, tc := range cases {
			status, _, body := postJSON(t, p.addr, route.path, route.body,
				map[string]string{"Authorization": tc.auth})
			if status != http.StatusUnauthorized {
				t.Fatalf("%s %s: status = %d, want 401", route.path, tc.name, status)
			}
			if got := string(body); got != tc.want {
				t.Fatalf("%s %s: body:\n got %s\nwant %s", route.path, tc.name, got, tc.want)
			}
		}
		// The scheme match is case-insensitive (RFC 9110) and the token is
		// trimmed — these must authenticate.
		for _, auth := range []string{
			"bearer " + e2eAPIKey,
			"BEARER " + e2eAPIKey,
			"Bearer    " + e2eAPIKey,
		} {
			status, _, _ := postJSON(t, p.addr, route.path, route.body,
				map[string]string{"Authorization": auth})
			if status != http.StatusOK {
				t.Fatalf("%s %q: status = %d, want 200", route.path, auth, status)
			}
		}
	}

	// Every rejection happened before any upstream I/O: the only upstream
	// hits are the three authenticated successes per route — the byte-exact
	// envelope assertions above already pin that no rejected body carried a
	// fragment of the presented credential.
	if n := up.count(); n != 6 {
		t.Fatalf("upstream hits = %d, want 6 (only authenticated requests relay)", n)
	}
}

// TestUpstreamReceivesNoAuthorization: the client credential authenticates
// the client TO the proxy and is consumed there — the upstream request
// carries no Authorization header at all.
func TestUpstreamReceivesNoAuthorization(t *testing.T) {
	up := newFakeUpstream(t)
	up.setHandler(jsonChatHandler(chatUpstream))
	p := startSubprocess(t, startOpts{
		yaml:     runtimeYAML(chatPublic, up.url()+"/v1", chatUpstream, ""),
		logLevel: "error",
	})

	if status, _, _ := postJSON(t, p.addr, "/v1/chat/completions", chatBody,
		map[string]string{"Authorization": "Bearer " + e2eAPIKey}); status != http.StatusOK {
		t.Fatalf("status = %d, want 200", status)
	}
	req, ok := up.last()
	if !ok {
		t.Fatal("upstream recorded no request")
	}
	if got := req.Headers.Get("Authorization"); got != "" {
		t.Fatalf("upstream received Authorization %q — the client credential must be consumed by the proxy", got)
	}
}

// TestHealthzOpenWithoutAuth: the docker probe surface stays unauthenticated
// — a poisoned reload must not be able to fail the container probe.
func TestHealthzOpenWithoutAuth(t *testing.T) {
	up := newFakeUpstream(t)
	p := startSubprocess(t, startOpts{
		yaml:     runtimeYAML(chatPublic, up.url()+"/v1", chatUpstream, ""),
		logLevel: "error",
	})

	resp, err := http.Get("http://" + p.addr + "/healthz")
	if err != nil {
		t.Fatalf("GET /healthz without Authorization: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != 200 {
		t.Fatalf("healthz status = %d, want 200 without credentials", resp.StatusCode)
	}
}

// TestBootWithoutAPIKeyExitsOne: the fail-closed plane — a keyless runtime
// file is invalid, so the process refuses to start rather than serving open.
func TestBootWithoutAPIKeyExitsOne(t *testing.T) {
	code, stderr := startSubprocessExpectExit(t, startOpts{
		yaml:     "models:\n  m:\n    endpoint: http://127.0.0.1:1/v1\n    upstream-model: up\n",
		noAPIKey: true,
	})
	if code != 1 {
		t.Fatalf("keyless boot exit code = %d, want 1", code)
	}
	if !strings.Contains(stderr, "config_load_failed") {
		t.Fatalf("config_load_failed event missing from keyless boot:\n%s", stderr)
	}
	if !strings.Contains(stderr, "api-key is required") {
		t.Fatalf("keyless boot error must name the missing key:\n%s", stderr)
	}
}

// TestReloadToKeylessKeepsServing pins the quiet direction: a keyless file
// arriving mid-flight is REJECTED, never applied — the running key keeps
// working and requests keep reaching the upstream. A regression that treated
// an absent api-key as "auth off" would silently open the service.
func TestReloadToKeylessKeepsServing(t *testing.T) {
	up := newFakeUpstream(t)
	up.setHandler(jsonChatHandler(chatUpstream))
	boot := runtimeYAML(chatPublic, up.url()+"/v1", chatUpstream, "")
	p := startSubprocess(t, startOpts{
		yaml:     boot,
		logLevel: "info",
	})

	if status, _, _ := postJSON(t, p.addr, "/v1/chat/completions", chatBody, nil); status != http.StatusOK {
		t.Fatalf("warm request status = %d, want 200", status)
	}

	keyless := strings.TrimPrefix(boot, "api-key: "+e2eAPIKey+"\n")
	rewriteConfig(t, p.cfgPath, keyless)
	ev := waitForLogEvent(t, p, func(ev logEvent) bool {
		return ev["message"] == "config_reload_rejected"
	}, "config_reload_rejected for the keyless file")
	if errText, _ := ev["error"].(string); !strings.Contains(errText, "api-key is required") {
		t.Fatalf("config_reload_rejected error = %v, want it to name the missing key", ev["error"])
	}

	// The last-known-good snapshot still authenticates and still relays.
	status, _, body := postJSON(t, p.addr, "/v1/chat/completions", chatBody, nil)
	if status != http.StatusOK {
		t.Fatalf("request after keyless reload status = %d, want 200 (body %s)", status, body)
	}
	if n := up.count(); n != 2 {
		t.Fatalf("upstream hits = %d, want 2 (both requests relayed)", n)
	}
}

// TestAPIKeyRotationHotReload: rotating the key is a runtime-file change —
// no restart. The reload lands, the old key stops working, the new one
// relays, and the config_reloaded event carries nothing about the key
// itself (not even a hash: log readers must not be able to confirm guesses
// offline).
func TestAPIKeyRotationHotReload(t *testing.T) {
	up := newFakeUpstream(t)
	up.setHandler(jsonChatHandler(chatUpstream))
	rotate := func(key string) string {
		return strings.Replace(runtimeYAML(chatPublic, up.url()+"/v1", chatUpstream, ""),
			"api-key: "+e2eAPIKey, "api-key: "+key, 1)
	}
	const oldKey = "rotated-key-one"
	const newKey = "rotated-key-two"
	p := startSubprocess(t, startOpts{
		yaml:     rotate(oldKey),
		logLevel: "info",
	})

	oldHdr := map[string]string{"Authorization": "Bearer " + oldKey}
	if status, _, _ := postJSON(t, p.addr, "/v1/chat/completions", chatBody, oldHdr); status != http.StatusOK {
		t.Fatalf("old key status = %d, want 200 before rotation", status)
	}

	rewriteConfig(t, p.cfgPath, rotate(newKey))
	ev := waitForLogEvent(t, p, func(ev logEvent) bool {
		return ev["message"] == "config_reloaded"
	}, "config_reloaded for the rotated key")
	// The reload acknowledgment's contract fields describe the published
	// snapshot; it must contain no key value (or hash) anywhere.
	if got, ok := ev["generation"].(float64); !ok || got != 1 {
		t.Fatalf("config_reloaded generation = %v, want 1", ev["generation"])
	}
	if got, ok := ev["model_count"].(float64); !ok || got != 1 {
		t.Fatalf("config_reloaded model_count = %v, want 1", ev["model_count"])
	}
	if got := ev["log_level"]; got != "info" {
		t.Fatalf("config_reloaded log_level = %v, want info", got)
	}
	if encoded := p.stderr.String(); strings.Contains(encoded, oldKey) || strings.Contains(encoded, newKey) {
		t.Fatal("config reload event carries key material — logging leak")
	}

	status, _, body := postJSON(t, p.addr, "/v1/chat/completions", chatBody, oldHdr)
	if status != http.StatusUnauthorized || string(body) != authInvalidEnvelope {
		t.Fatalf("old key after rotation: status = %d body %s, want 401 invalid envelope", status, body)
	}
	if status, _, _ := postJSON(t, p.addr, "/v1/chat/completions", chatBody,
		map[string]string{"Authorization": "Bearer " + newKey}); status != http.StatusOK {
		t.Fatalf("new key status = %d, want 200 after rotation", status)
	}

	// Neither key value (nor a fragment) reached the logs at any level.
	if strings.Contains(p.stderr.String(), oldKey) || strings.Contains(p.stderr.String(), newKey) {
		t.Fatal("rotated key material reached stderr — logging leak")
	}
}
