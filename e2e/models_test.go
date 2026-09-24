package e2e_test

import (
	"encoding/json"
	"io"
	"net/http"
	"testing"
)

// TestModelsEndpoint exercises the configured model catalog through the real
// binary. It must be snapshot-local (never reaches an upstream), authenticate
// like the model-serving POST routes, retain 405-before-401 ordering, preserve
// the narrow id/created entry shape, and follow a hot-reloaded mapping.
func TestModelsEndpoint(t *testing.T) {
	up := newFakeUpstream(t)
	initial := modelsYAML(up.url()+"/v1", "zebra", "alpha", "mid-model")
	p := startSubprocess(t, startOpts{yaml: initial, logLevel: "info"})

	status, header, body := getModels(t, p.addr, "Bearer "+e2eAPIKey)
	if status != http.StatusOK {
		t.Fatalf("GET /v1/models status = %d, want 200 (body %q)", status, body)
	}
	if got := header.Get("Content-Type"); got != "application/json" {
		t.Errorf("GET /v1/models Content-Type = %q, want application/json", got)
	}
	assertModelsList(t, body, []string{"alpha", "mid-model", "zebra"})
	if got := up.count(); got != 0 {
		t.Fatalf("GET /v1/models reached upstream %d times, want 0", got)
	}

	// The method gate runs before authentication, like the two POST surfaces.
	req, err := http.NewRequest(http.MethodPost, "http://"+p.addr+"/v1/models", nil)
	if err != nil {
		t.Fatalf("new POST /v1/models: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST /v1/models: %v", err)
	}
	got405 := readBody(t, resp)
	if resp.StatusCode != http.StatusMethodNotAllowed || string(got405) != `{"error":{"message":"method not allowed","type":"invalid_request_error","param":null,"code":null}}` {
		t.Fatalf("POST /v1/models = %d %s, want exact 405 envelope", resp.StatusCode, got405)
	}

	// A trailing slash must not silently match the discovery route.
	req, err = http.NewRequest(http.MethodGet, "http://"+p.addr+"/v1/models/", nil)
	if err != nil {
		t.Fatalf("new GET /v1/models/: %v", err)
	}
	req.Header.Set("Authorization", "Bearer "+e2eAPIKey)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET /v1/models/: %v", err)
	}
	got404 := readBody(t, resp)
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("GET /v1/models/ status = %d, want 404 (body %s)", resp.StatusCode, got404)
	}

	// A new request after a successful reload reads the new snapshot, without
	// leaking a stale model catalog.
	rewriteConfig(t, p.cfgPath, modelsYAML(up.url()+"/v1", "beta", "alpha"))
	waitForLogEvent(t, p, func(ev logEvent) bool {
		return ev["message"] == "config_reloaded" && ev["model_count"] == float64(2)
	}, "model-list config reload")

	status, _, body = getModels(t, p.addr, "Bearer "+e2eAPIKey)
	if status != http.StatusOK {
		t.Fatalf("post-reload GET /v1/models status = %d, want 200 (body %q)", status, body)
	}
	assertModelsList(t, body, []string{"alpha", "beta"})
	if got := up.count(); got != 0 {
		t.Fatalf("GET /v1/models reached upstream after reload %d times, want 0", got)
	}
}

// TestModelsEndpointAuthentication ensures discovery does not bypass the
// mandatory bearer gate or disclose why an invalid credential was rejected.
func TestModelsEndpointAuthentication(t *testing.T) {
	up := newFakeUpstream(t)
	p := startSubprocess(t, startOpts{yaml: modelsYAML(up.url()+"/v1", "model")})

	for _, tc := range []struct {
		name, authorization, want string
	}{
		{"missing", "", authMissingEnvelope},
		{"malformed", "Basic not-a-bearer", authMissingEnvelope},
		{"wrong", "Bearer not-the-configured-key", authInvalidEnvelope},
	} {
		t.Run(tc.name, func(t *testing.T) {
			status, _, body := getModels(t, p.addr, tc.authorization)
			if status != http.StatusUnauthorized {
				t.Fatalf("GET /v1/models status = %d, want 401 (body %s)", status, body)
			}
			if got := string(body); got != tc.want {
				t.Errorf("401 body:\n got %s\nwant %s", got, tc.want)
			}
		})
	}
	if got := up.count(); got != 0 {
		t.Fatalf("unauthenticated GET /v1/models reached upstream %d times, want 0", got)
	}
}

// modelsYAML makes a config with public model names mapped to one harmless
// upstream. The endpoint itself must not call that upstream; using it here
// makes a breach directly observable through fakeUpstream's request count.
func modelsYAML(endpoint string, names ...string) string {
	yaml := "api-key: " + e2eAPIKey + "\nmodels:\n"
	for _, name := range names {
		yaml += "  " + name + ":\n    endpoint: " + endpoint + "\n    upstream-model: upstream-" + name + "\n"
	}
	return yaml
}

func getModels(t *testing.T, addr, authorization string) (int, http.Header, []byte) {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, "http://"+addr+"/v1/models", nil)
	if err != nil {
		t.Fatalf("new GET /v1/models: %v", err)
	}
	if authorization != "" {
		req.Header.Set("Authorization", authorization)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET /v1/models: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read GET /v1/models: %v", err)
	}
	return resp.StatusCode, resp.Header.Clone(), body
}

func assertModelsList(t *testing.T, body []byte, wantIDs []string) {
	t.Helper()
	var envelope map[string]any
	if err := json.Unmarshal(body, &envelope); err != nil {
		t.Fatalf("models response is not JSON: %v (%q)", err, body)
	}
	if len(envelope) != 2 || envelope["object"] != "list" {
		t.Fatalf("models response envelope = %v, want exactly object:list + data", envelope)
	}
	data, ok := envelope["data"].([]any)
	if !ok {
		t.Fatalf("models data = %T, want array", envelope["data"])
	}
	if len(data) != len(wantIDs) {
		t.Fatalf("models data length = %d, want %d (%v)", len(data), len(wantIDs), data)
	}
	for i, raw := range data {
		item, ok := raw.(map[string]any)
		if !ok {
			t.Fatalf("models data[%d] = %T, want object", i, raw)
		}
		if len(item) != 2 || item["id"] != wantIDs[i] || item["created"] != float64(0) {
			t.Errorf("models data[%d] = %v, want exactly {id:%q, created:0}", i, item, wantIDs[i])
		}
	}
}
