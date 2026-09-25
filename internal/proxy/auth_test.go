package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/rs/zerolog"

	"openai-compatible-injector/internal/auth"
	"openai-compatible-injector/internal/config"
)

// Partner-mode handler contract: identity rides the request, denials share
// the static 401 discipline, a backend failure denies loudly and never
// leaks driver text, and the provider always receives the request's own
// snapshot.

// stubAuthProvider is the unit-test stand-in for the key store's provider.
// It records every For call so tests can pin the one-snapshot-per-request
// rule at the auth seam, and answers with a canned decision.
type stubAuthProvider struct {
	mu        sync.Mutex
	forCalls  int
	seenSnaps []*config.Snapshot

	principal auth.Principal
	reason    auth.Reason
	err       error
}

func (p *stubAuthProvider) For(snap *config.Snapshot) auth.Authenticator {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.forCalls++
	p.seenSnaps = append(p.seenSnaps, snap)
	return p
}

// Authenticate implements auth.Authenticator (the provider doubles as its
// own snapshot-independent authenticator, like PartnerProvider does).
func (p *stubAuthProvider) Authenticate(_ context.Context, _ string) (auth.Principal, auth.Reason, error) {
	return p.principal, p.reason, p.err
}

// logEvents parses a captured zerolog buffer into per-line maps.
func logEvents(t *testing.T, logs *bytes.Buffer) []map[string]interface{} {
	t.Helper()
	var out []map[string]interface{}
	for _, line := range strings.Split(strings.TrimSpace(logs.String()), "\n") {
		if line == "" {
			continue
		}
		var ev map[string]interface{}
		if err := json.Unmarshal([]byte(line), &ev); err != nil {
			t.Fatalf("log line is not JSON: %v (%q)", err, line)
		}
		out = append(out, ev)
	}
	return out
}

func findEvents(events []map[string]interface{}, slug string) []map[string]interface{} {
	var out []map[string]interface{}
	for _, ev := range events {
		if ev["message"] == slug {
			out = append(out, ev)
		}
	}
	return out
}

func partnerStore(t *testing.T, upstream string) (*config.Store, *config.Snapshot) {
	t.Helper()
	snap, err := config.LoadRuntime([]byte("api-key: " + testAPIKey + "\nmodels:\n  test-model:\n    endpoint: " + upstream + "\n    upstream-model: upstream-name\n"))
	if err != nil {
		t.Fatalf("LoadRuntime: %v", err)
	}
	return config.NewStore(snap), snap
}

func TestPartnerModeIdentityReachesRequestCompleted(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// The client's Authorization is consumed at the gate; the upstream
		// must not receive it in partner mode either.
		if r.Header.Get("Authorization") != "" {
			t.Error("client Authorization leaked upstream")
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"model":"upstream-name","choices":[]}`))
	}))
	defer up.Close()

	store, snap := partnerStore(t, up.URL+"/v1")
	provider := &stubAuthProvider{principal: auth.Principal{PartnerID: "partner-acme", KeyID: "pak_x1"}}
	var logs bytes.Buffer
	h := NewHandler(store, directResolver(), nil, provider, nil, zerolog.New(&logs))

	rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions",
		`{"model":"test-model","messages":[{"role":"user","content":"hi"}]}`, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body: %s)", rec.Code, rec.Body.String())
	}

	// The provider was consulted exactly once, with the request's own
	// snapshot — one snapshot per request, auth included.
	provider.mu.Lock()
	calls, snaps := provider.forCalls, provider.seenSnaps
	provider.mu.Unlock()
	if calls != 1 {
		t.Fatalf("For called %d times, want 1", calls)
	}
	if len(snaps) == 1 && snaps[0] != snap {
		t.Fatal("the provider received a snapshot the request did not load")
	}

	completions := findEvents(logEvents(t, &logs), "request_completed")
	if len(completions) != 1 {
		t.Fatalf("request_completed count = %d, want 1", len(completions))
	}
	ev := completions[0]
	if ev["partner_id"] != "partner-acme" || ev["key_id"] != "pak_x1" {
		t.Fatalf("identity fields = %v/%v, want partner-acme/pak_x1", ev["partner_id"], ev["key_id"])
	}
}

// Every partner-mode denial — unknown, revoked — is byte-identical to the
// static wrong-key 401. Why a key was rejected is enumeration material; it
// never reaches the wire.

func TestPartnerModeDenialsShareTheStaticBody(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		t.Error("a denied request reached the upstream")
	}))
	defer up.Close()

	for name, reason := range map[string]auth.Reason{"unknown": auth.ReasonUnknown, "revoked": auth.ReasonRevoked} {
		t.Run(name, func(t *testing.T) {
			store, _ := partnerStore(t, up.URL+"/v1")
			provider := &stubAuthProvider{reason: reason}
			var logs bytes.Buffer
			h := NewHandler(store, directResolver(), nil, provider, nil, zerolog.New(&logs))

			rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions",
				`{"model":"test-model","messages":[{"role":"user","content":"hi"}]}`, nil)
			if rec.Code != http.StatusUnauthorized {
				t.Fatalf("status = %d, want 401", rec.Code)
			}
			if got := strings.TrimSpace(rec.Body.String()); got != envelopeAuthInvalid {
				t.Fatalf("body = %q, want the static invalid_api_key envelope", got)
			}
			if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
				t.Fatalf("Content-Type = %q, want application/json", ct)
			}
			completions := findEvents(logEvents(t, &logs), "request_completed")
			if len(completions) != 1 || completions[0]["outcome"] != "unauthorized" {
				t.Fatalf("request_completed outcome = %v, want unauthorized", completions[0]["outcome"])
			}
		})
	}
}

// A backend failure denies the request (fail closed), answers with the same
// static 401, and reports exactly one loud event whose only diagnostic is
// the error class — the driver's text can embed infrastructure detail, so
// it never rides the log.

func TestPartnerModeBackendFailureFailsClosed(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		t.Error("a request denied for a store outage reached the upstream")
	}))
	defer up.Close()

	store, _ := partnerStore(t, up.URL+"/v1")
	provider := &stubAuthProvider{reason: auth.ReasonBackend, err: context.DeadlineExceeded}
	var logs bytes.Buffer
	h := NewHandler(store, directResolver(), nil, provider, nil, zerolog.New(&logs))

	rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions",
		`{"model":"test-model","messages":[{"role":"user","content":"hi"}]}`, nil)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401 (fail closed)", rec.Code)
	}
	if got := strings.TrimSpace(rec.Body.String()); got != envelopeAuthInvalid {
		t.Fatalf("body = %q, want the static invalid_api_key envelope", got)
	}

	events := logEvents(t, &logs)
	backend := findEvents(events, "auth_backend_failed")
	if len(backend) != 1 {
		t.Fatalf("auth_backend_failed count = %d, want 1", len(backend))
	}
	if backend[0]["error_class"] != "timeout" {
		t.Fatalf("error_class = %v, want timeout", backend[0]["error_class"])
	}
	for field, forbidden := range map[string]string{
		"error":  "context deadline exceeded",
		"errmsg": "deadline",
	} {
		if v, ok := backend[0][field]; ok && strings.Contains(v.(string), forbidden) {
			t.Fatalf("%s leaked driver text: %v", field, v)
		}
	}
}

// In partner mode the configured YAML api-key is not a wire credential:
// presenting it authenticates against the store, where it was never seeded.

func TestPartnerModeIgnoresTheConfiguredKey(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		t.Error("the configured YAML key authenticated a request in partner mode")
	}))
	defer up.Close()

	store, _ := partnerStore(t, up.URL+"/v1")
	// The static authenticator would accept testAPIKey; the partner store
	// knows no keys at all, so the denial comes from the partner path.
	provider := &stubAuthProvider{reason: auth.ReasonUnknown}
	var logs bytes.Buffer
	h := NewHandler(store, directResolver(), nil, provider, nil, zerolog.New(&logs))

	rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions",
		`{"model":"test-model","messages":[{"role":"user","content":"hi"}]}`, nil)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", rec.Code)
	}
	if got := strings.TrimSpace(rec.Body.String()); got != envelopeAuthInvalid {
		t.Fatalf("body = %q, want the static invalid_api_key envelope", got)
	}
}
