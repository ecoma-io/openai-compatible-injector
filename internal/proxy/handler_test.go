package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"openai-compatible-injector/internal/config"
)

// testAPIKey is the bearer credential every unit-test store configures.
const testAPIKey = "unit-test-key"

func newTestStore(t *testing.T, endpoint string) *config.Store {
	t.Helper()
	yaml := fmt.Sprintf("api-key: %s\nmodels:\n  test-model:\n    endpoint: %s\n    upstream-model: upstream-name\n    injection-prompt: \"\"\n", testAPIKey, endpoint)
	snap, err := config.LoadRuntime([]byte(yaml))
	if err != nil {
		t.Fatalf("LoadRuntime: %v", err)
	}
	return config.NewStore(snap)
}

func newTestHandler(t *testing.T, store *config.Store) http.Handler {
	t.Helper()
	log := zerolog.New(zerolog.TestWriter{T: t}).Level(zerolog.Disabled)
	return NewHandler(store, NewSharedClient(), log)
}

func doRequest(t *testing.T, h http.Handler, method, path, body string, headers map[string]string) *httptest.ResponseRecorder {
	t.Helper()
	var r io.Reader
	if body != "" {
		r = strings.NewReader(body)
	}
	req := httptest.NewRequest(method, path, r)
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	// Default to the configured bearer so the suite's traffic passes the
	// auth gate; a test that passes its own Authorization (right, wrong, or
	// deliberately absent as "") opts out of the default.
	if _, ok := headers["Authorization"]; !ok {
		req.Header.Set("Authorization", "Bearer "+testAPIKey)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

// countingRecorder wraps a ResponseRecorder and counts writes and flushes so
// tests can assert buffered (single write, no flush) vs streaming behavior.
type countingRecorder struct {
	*httptest.ResponseRecorder
	mu      sync.Mutex
	writes  int
	flushes int
}

func (c *countingRecorder) Write(p []byte) (int, error) {
	c.mu.Lock()
	c.writes++
	c.mu.Unlock()
	return c.ResponseRecorder.Write(p)
}

func (c *countingRecorder) Flush() {
	c.mu.Lock()
	c.flushes++
	c.mu.Unlock()
	c.ResponseRecorder.Flush()
}

// TestQueryStringEncoding pins both halves of the query contract. The
// client's query string is dropped: only the operator-configured endpoint
// defines where the request goes, and a client-supplied `?api-key=` must
// never travel — not to the configured upstream, and not into a URL this
// proxy might otherwise log. The endpoint's OWN query is preserved verbatim:
// query-authenticated providers configure their key there, on purpose.
func TestQueryStringEncoding(t *testing.T) {
	var gotQuery muquery
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotQuery.set(r.URL.RawQuery)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"model":"upstream-name","choices":[]}`))
	}))
	defer up.Close()

	t.Run("client query dropped", func(t *testing.T) {
		gotQuery.set("")
		h := newTestHandler(t, newTestStore(t, up.URL+"/v1"))
		rec := doRequest(t, h, http.MethodPost,
			"/v1/chat/completions?api-key=CLIENT_SECRET&x=1",
			`{"model":"test-model","messages":[{"role":"user","content":"hi"}]}`, nil)
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200 (body: %s)", rec.Code, rec.Body.String())
		}
		if q := gotQuery.get(); q != "" {
			t.Fatalf("upstream RawQuery = %q, want empty (client query must not travel)", q)
		}
	})

	t.Run("endpoint query preserved", func(t *testing.T) {
		gotQuery.set("")
		h := newTestHandler(t, newTestStore(t, up.URL+"/v1?api-version=2024-02-01"))
		rec := doRequest(t, h, http.MethodPost,
			"/v1/chat/completions",
			`{"model":"test-model","messages":[{"role":"user","content":"hi"}]}`, nil)
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200 (body: %s)", rec.Code, rec.Body.String())
		}
		if q := gotQuery.get(); q != "api-version=2024-02-01" {
			t.Fatalf("upstream RawQuery = %q, want the endpoint's configured query", q)
		}
	})
}

// muquery is a mutex-guarded string cell for the single-variable capture the
// test above needs.
type muquery struct {
	mu sync.Mutex
	v  string
}

func (q *muquery) set(v string) { q.mu.Lock(); q.v = v; q.mu.Unlock() }
func (q *muquery) get() string  { q.mu.Lock(); defer q.mu.Unlock(); return q.v }

// TestUpstreamFailureLogsRedactEndpoint pins the credential rule: a dial
// failure against an endpoint whose query string carries a secret must not
// put that secret — or the full URL — into logs, in any field. Only the
// scheme+host origin may appear.
func TestUpstreamFailureLogsRedactEndpoint(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close() // refuses connections

	endpoint := "http://" + addr + "/v1?api-key=SECRET_ENDPOINT_TOKEN&deployment=x"
	store := newTestStore(t, endpoint)
	var logs bytes.Buffer
	log := zerolog.New(&logs)
	h := NewHandler(store, NewSharedClient(), log)

	rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions", `{"model":"test-model"}`, nil)
	if rec.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502", rec.Code)
	}

	out := logs.String()
	for _, banned := range []string{"SECRET_ENDPOINT_TOKEN", "api-key", "deployment", "/v1/chat/completions?"} {
		if strings.Contains(out, banned) {
			t.Errorf("log line contains %q:\n%s", banned, out)
		}
	}
	if !strings.Contains(out, "http://"+addr) {
		t.Errorf("log line lost the scheme+host origin:\n%s", out)
	}
}

func TestHealthz(t *testing.T) {
	h := newTestHandler(t, newTestStore(t, "http://127.0.0.1:9/v1"))
	rec := doRequest(t, h, http.MethodGet, "/healthz", "", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "text/plain; charset=utf-8" {
		t.Errorf("Content-Type = %q, want text/plain; charset=utf-8", ct)
	}
	if body := rec.Body.String(); body != "ok\n" {
		t.Errorf("body = %q, want \"ok\\n\"", body)
	}
}

func TestUnknownModel404ExactBody(t *testing.T) {
	h := newTestHandler(t, newTestStore(t, "http://127.0.0.1:9/v1"))
	rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions", `{"model":"does-not-exist"}`, nil)
	want := `{"error":{"message":"The model 'does-not-exist' does not exist or you do not have access to it.","type":"invalid_request_error","param":null,"code":"model_not_found"}}`
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rec.Code)
	}
	if body := rec.Body.String(); body != want {
		t.Errorf("body mismatch:\n got %s\nwant %s", body, want)
	}
}

func TestMissingModel400ExactBody(t *testing.T) {
	h := newTestHandler(t, newTestStore(t, "http://127.0.0.1:9/v1"))
	rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions", `{"messages":[]}`, nil)
	want := `{"error":{"message":"you must provide a model parameter","type":"invalid_request_error","param":null,"code":null}}`
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
	if body := rec.Body.String(); body != want {
		t.Errorf("body mismatch:\n got %s\nwant %s", body, want)
	}
}

func TestInvalidJSON400ExactBody(t *testing.T) {
	h := newTestHandler(t, newTestStore(t, "http://127.0.0.1:9/v1"))
	want := `{"error":{"message":"invalid JSON in request body","type":"invalid_request_error","param":null,"code":null}}`
	for _, body := range []string{`{"model":`, `{"model":"x","stream":"yes"}`, ``} {
		rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions", body, nil)
		if rec.Code != http.StatusBadRequest {
			t.Errorf("body %q: status = %d, want 400", body, rec.Code)
			continue
		}
		if got := rec.Body.String(); got != want {
			t.Errorf("body %q: envelope mismatch:\n got %s\nwant %s", body, got, want)
		}
	}
}

func TestNonStreamRewriteBufferedSingleWrite(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, `{"id":"chatcmpl-1","model":"upstream-name","choices":[{"index":0,"message":{"role":"assistant","content":"hi"}}]}`)
	}))
	defer upstream.Close()

	h := newTestHandler(t, newTestStore(t, upstream.URL+"/v1"))
	rec := httptest.NewRecorder()
	cw := &countingRecorder{ResponseRecorder: rec}
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"test-model","messages":[{"role":"user","content":"hello"}]}`))
	req.Header.Set("Authorization", "Bearer "+testAPIKey)
	h.ServeHTTP(cw, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body %q)", rec.Code, rec.Body.String())
	}
	if cw.writes != 1 {
		t.Errorf("writes = %d, want 1 (fully buffered single write)", cw.writes)
	}
	if cw.flushes != 0 {
		t.Errorf("flushes = %d, want 0 (stream:false must not stream)", cw.flushes)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("response not valid JSON: %v (%q)", err, rec.Body.String())
	}
	if body["model"] != "test-model" {
		t.Errorf("model = %v, want test-model (rewritten from upstream-name)", body["model"])
	}
}

func TestStreamTrueUpstreamIgnoresStreamBuffered(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"model":"upstream-name","choices":[{"delta":{"content":"x"}}],"usage":{"total_tokens":1}}`)
	}))
	defer upstream.Close()

	h := newTestHandler(t, newTestStore(t, upstream.URL+"/v1"))
	rec := httptest.NewRecorder()
	cw := &countingRecorder{ResponseRecorder: rec}
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"test-model","stream":true}`))
	req.Header.Set("Authorization", "Bearer "+testAPIKey)
	h.ServeHTTP(cw, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if cw.writes != 1 || cw.flushes != 0 {
		t.Errorf("writes=%d flushes=%d, want buffered (1, 0) when upstream ignores stream flag",
			cw.writes, cw.flushes)
	}
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("response not valid JSON: %v", err)
	}
	if body["model"] != "test-model" {
		t.Errorf("model = %v, want test-model", body["model"])
	}
}

func TestSSEPassthroughRewritesModel(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: {\"model\":\"upstream-name\",\"role\":\"assistant\",\"content\":\"hi\"}\n\n")
		_, _ = io.WriteString(w, "data: [DONE]\n\n")
	}))
	defer upstream.Close()

	h := newTestHandler(t, newTestStore(t, upstream.URL+"/v1"))
	rec := httptest.NewRecorder()
	cw := &countingRecorder{ResponseRecorder: rec}
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"test-model","stream":true}`))
	req.Header.Set("Authorization", "Bearer "+testAPIKey)
	h.ServeHTTP(cw, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if cw.flushes < 2 {
		t.Errorf("flushes = %d, want >= 2 (flush per event boundary)", cw.flushes)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "text/event-stream" {
		t.Errorf("Content-Type = %q, want text/event-stream", ct)
	}
	lines := strings.Split(rec.Body.String(), "\n")
	var dataLines []string
	for _, ln := range lines {
		if strings.HasPrefix(ln, "data: ") {
			dataLines = append(dataLines, strings.TrimPrefix(ln, "data: "))
		}
	}
	if len(dataLines) != 2 {
		t.Fatalf("data lines = %v, want 2 (rewritten chunk + [DONE])", dataLines)
	}
	var chunk map[string]any
	if err := json.Unmarshal([]byte(dataLines[0]), &chunk); err != nil {
		t.Fatalf("first data line not valid JSON: %v (%q)", err, dataLines[0])
	}
	if chunk["model"] != "test-model" {
		t.Errorf("streamed model = %v, want test-model", chunk["model"])
	}
	if dataLines[1] != "[DONE]" {
		t.Errorf("second data line = %q, want [DONE] passed through untouched", dataLines[1])
	}
}

func TestUpstreamErrorVerbatimPassthrough(t *testing.T) {
	cases := []struct {
		name        string
		status      int
		contentType string
		body        string
	}{
		{"html 503", http.StatusServiceUnavailable, "text/html", "<html>Service Unavailable</html>"},
		{"json 429", http.StatusTooManyRequests, "application/json", `{"error":{"message":"rate limited","type":"rate_limit_error"}}`},
		{"plain 500", http.StatusInternalServerError, "text/plain", "internal error"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", tc.contentType)
				w.Header().Set("X-Secret", "nope")
				w.Header().Set("Cache-Control", "no-store")
				w.Header().Set("X-Request-Id", "req-1")
				w.WriteHeader(tc.status)
				_, _ = io.WriteString(w, tc.body)
			}))
			defer upstream.Close()

			h := newTestHandler(t, newTestStore(t, upstream.URL+"/v1"))
			rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions", `{"model":"test-model"}`, nil)

			if rec.Code != tc.status {
				t.Fatalf("status = %d, want %d", rec.Code, tc.status)
			}
			if body := rec.Body.String(); body != tc.body {
				t.Errorf("body not verbatim:\n got %q\nwant %q", body, tc.body)
			}
			if ct := rec.Header().Get("Content-Type"); ct != tc.contentType {
				t.Errorf("Content-Type = %q, want %q", ct, tc.contentType)
			}
			if got := rec.Header().Get("Cache-Control"); got != "no-store" {
				t.Errorf("Cache-Control = %q, want no-store", got)
			}
			if got := rec.Header().Get("X-Request-Id"); got != "req-1" {
				t.Errorf("X-Request-Id = %q, want req-1", got)
			}
			if got := rec.Header().Get("X-Secret"); got != "" {
				t.Errorf("X-Secret = %q, want empty (not allow-listed)", got)
			}
		})
	}
}

func TestUpstreamRefusedReturns502(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close() // port now refuses connections

	h := newTestHandler(t, newTestStore(t, "http://"+addr+"/v1"))
	rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions", `{"model":"test-model"}`, nil)
	want := `{"error":{"message":"upstream request failed","type":"upstream_error","code":"upstream_unreachable"}}`
	if rec.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502", rec.Code)
	}
	if body := rec.Body.String(); body != want {
		t.Errorf("body mismatch:\n got %s\nwant %s", body, want)
	}
}

func TestUpstreamInvalidResponseReturns502(t *testing.T) {
	for _, body := range []string{"definitely not json", ""} {
		upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = io.WriteString(w, body)
		}))

		h := newTestHandler(t, newTestStore(t, upstream.URL+"/v1"))
		rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions", `{"model":"test-model"}`, nil)
		want := `{"error":{"message":"upstream returned an invalid response","type":"upstream_error","code":"upstream_invalid_response"}}`
		if rec.Code != http.StatusBadGateway {
			upstream.Close()
			t.Fatalf("upstream body %q: status = %d, want 502", body, rec.Code)
		}
		if got := rec.Body.String(); got != want {
			t.Errorf("upstream body %q: envelope mismatch:\n got %s\nwant %s", body, got, want)
		}
		upstream.Close()
	}
}

func TestForwardedHeadersAndUnknownFields(t *testing.T) {
	var (
		mu     sync.Mutex
		gotHdr http.Header
		gotBod []byte
	)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		gotHdr = r.Header.Clone()
		gotBod, _ = io.ReadAll(r.Body)
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"id":"1","model":"upstream-name"}`)
	}))
	defer upstream.Close()

	h := newTestHandler(t, newTestStore(t, upstream.URL+"/v1"))
	rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions",
		`{"model":"test-model","custom_field":"abc123","messages":[{"role":"user","content":"hi"}]}`,
		map[string]string{
			// The client's credential is the configured key — it
			// authenticates the request and must stop there.
			"Authorization": "Bearer " + testAPIKey,
			"Accept":        "text/event-stream",
			"OpenAI-Beta":   "assistants=v2",
			"X-Internal":    "secret",
			"Cookie":        "yum=1",
		})

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	mu.Lock()
	defer mu.Unlock()
	if got := gotHdr.Get("Authorization"); got != "" {
		t.Errorf("Authorization forwarded as %q, want never forwarded upstream", got)
	}
	if got := gotHdr.Get("Accept"); got != "text/event-stream" {
		t.Errorf("Accept = %q, want text/event-stream", got)
	}
	if got := gotHdr.Get("OpenAI-Beta"); got != "assistants=v2" {
		t.Errorf("OpenAI-Beta = %q, want assistants=v2", got)
	}
	if got := gotHdr.Get("Content-Type"); got != "application/json" {
		t.Errorf("Content-Type = %q, want defaulted application/json", got)
	}
	if got := gotHdr.Get("X-Internal"); got != "" {
		t.Errorf("X-Internal forwarded as %q, want dropped", got)
	}
	if got := gotHdr.Get("Cookie"); got != "" {
		t.Errorf("Cookie forwarded as %q, want dropped", got)
	}
	var sent map[string]any
	if err := json.Unmarshal(gotBod, &sent); err != nil {
		t.Fatalf("upstream body not valid JSON: %v (%q)", err, gotBod)
	}
	if sent["custom_field"] != "abc123" {
		t.Errorf("custom_field = %v, want abc123 (unknown field forwarded)", sent["custom_field"])
	}
	if sent["model"] != "upstream-name" {
		t.Errorf("upstream model = %v, want upstream-name (transformed)", sent["model"])
	}
}

// TestAuthRequiredExactBodies pins the auth gate's wire contract on both
// routes: the two static 401 envelopes are byte-exact, the malformed
// presentations (absent header, non-bearer scheme, empty token) share the
// missing envelope, and the scheme match tolerates case and extra spaces per
// RFC 9110. Every 401 lands before any upstream I/O — the upstream hit count
// must not move.
func TestAuthRequiredExactBodies(t *testing.T) {
	var hits atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"id":"1","model":"upstream-name"}`)
	}))
	defer upstream.Close()

	for _, route := range []struct {
		path string
		body string
	}{
		{"/v1/chat/completions", `{"model":"test-model","messages":[{"role":"user","content":"hi"}]}`},
		{"/v1/responses", `{"model":"test-model","input":"hi"}`},
	} {
		t.Run(route.path, func(t *testing.T) {
			h := newTestHandler(t, newTestStore(t, upstream.URL+"/v1"))
			cases := []struct {
				name string
				auth string
				want int
				body string
			}{
				{"no header", "", http.StatusUnauthorized, envelopeAuthMissing},
				{"non-bearer scheme", "Basic abc", http.StatusUnauthorized, envelopeAuthMissing},
				{"bearer with no token", "Bearer", http.StatusUnauthorized, envelopeAuthMissing},
				{"empty bearer token", "Bearer ", http.StatusUnauthorized, envelopeAuthMissing},
				{"token with embedded space", "Bearer unit test-key", http.StatusUnauthorized, envelopeAuthMissing},
				{"token with embedded tab", "Bearer unit\ttest-key", http.StatusUnauthorized, envelopeAuthMissing},
				{"token with trailing tab", "Bearer " + testAPIKey + "\t", http.StatusUnauthorized, envelopeAuthMissing},
				{"disallowed token character", "Bearer unit:key", http.StatusUnauthorized, envelopeAuthMissing},
				{"padding before token end", "Bearer unit=test", http.StatusUnauthorized, envelopeAuthMissing},
				{"only padding", "Bearer ===", http.StatusUnauthorized, envelopeAuthMissing},
				{"wrong key", "Bearer wrong-key", http.StatusUnauthorized, envelopeAuthInvalid},
				{"configured key with suffix", "Bearer " + testAPIKey + "-suffix", http.StatusUnauthorized, envelopeAuthInvalid},
				{"lowercase scheme", "bearer " + testAPIKey, http.StatusOK, ""},
				{"uppercase scheme", "BEARER " + testAPIKey, http.StatusOK, ""},
				{"extra spaces before token", "Bearer   " + testAPIKey, http.StatusOK, ""},
			}
			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					before := hits.Load()
					rec := doRequest(t, h, http.MethodPost, route.path, route.body,
						map[string]string{"Authorization": tc.auth})
					if rec.Code != tc.want {
						t.Fatalf("status = %d, want %d (body %q)", rec.Code, tc.want, rec.Body.String())
					}
					if tc.want == http.StatusUnauthorized {
						if got := rec.Body.String(); got != tc.body {
							t.Errorf("401 body:\n got %s\nwant %s", got, tc.body)
						}
						if after := hits.Load(); after != before {
							t.Errorf("upstream hits moved %d -> %d on an unauthorized request", before, after)
						}
					}
				})
			}
		})
	}
}

// TestAuthUsesSnapshotKey pins the quiet direction of key rotation: the key
// rides the config snapshot, not a process global — publishing a rotated key
// flips subsequent requests (old bearer 401, new 200) and nothing else, so a
// reload can never leave auth bound to a stale key or silently disabled.
func TestAuthUsesSnapshotKey(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"id":"1","model":"upstream-name"}`)
	}))
	defer upstream.Close()

	store := newTestStore(t, upstream.URL+"/v1")
	h := newTestHandler(t, store)
	body := `{"model":"test-model","messages":[{"role":"user","content":"hi"}]}`

	rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions", body,
		map[string]string{"Authorization": "Bearer " + testAPIKey})
	if rec.Code != http.StatusOK {
		t.Fatalf("pre-rotation status = %d, want 200", rec.Code)
	}

	next, err := config.LoadRuntime([]byte(fmt.Sprintf(
		"api-key: rotated-key\nmodels:\n  test-model:\n    endpoint: %s\n    upstream-model: upstream-name\n    injection-prompt: \"\"\n",
		upstream.URL+"/v1")))
	if err != nil {
		t.Fatalf("LoadRuntime: %v", err)
	}
	if err := store.Publish(next); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	rec = doRequest(t, h, http.MethodPost, "/v1/chat/completions", body,
		map[string]string{"Authorization": "Bearer " + testAPIKey})
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("post-rotation old key status = %d, want 401", rec.Code)
	}
	if got := rec.Body.String(); got != envelopeAuthInvalid {
		t.Errorf("post-rotation body:\n got %s\nwant %s", got, envelopeAuthInvalid)
	}
	rec = doRequest(t, h, http.MethodPost, "/v1/chat/completions", body,
		map[string]string{"Authorization": "Bearer rotated-key"})
	if rec.Code != http.StatusOK {
		t.Fatalf("post-rotation new key status = %d, want 200 (body %q)", rec.Code, rec.Body.String())
	}
}

func TestResponsesRoute(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/responses" {
			t.Errorf("upstream path = %q, want /v1/responses", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"id":"resp_1","model":"upstream-name","output":[]}`)
	}))
	defer upstream.Close()

	h := newTestHandler(t, newTestStore(t, upstream.URL+"/v1"))
	rec := doRequest(t, h, http.MethodPost, "/v1/responses", `{"model":"test-model","input":"hi"}`, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("response not valid JSON: %v", err)
	}
	if body["model"] != "test-model" {
		t.Errorf("model = %v, want test-model", body["model"])
	}
}

func TestMethodNotAllowedJSONEnvelope(t *testing.T) {
	h := newTestHandler(t, newTestStore(t, "http://127.0.0.1:9/v1"))
	want := `{"error":{"message":"method not allowed","type":"invalid_request_error","param":null,"code":null}}`
	for _, tc := range []struct{ method, path string }{
		{http.MethodGet, "/v1/chat/completions"},
		{http.MethodDelete, "/v1/chat/completions"},
		{http.MethodGet, "/v1/responses"},
	} {
		rec := doRequest(t, h, tc.method, tc.path, "", nil)
		if rec.Code != http.StatusMethodNotAllowed {
			t.Errorf("%s %s: status = %d, want 405", tc.method, tc.path, rec.Code)
			continue
		}
		if got := rec.Body.String(); got != want {
			t.Errorf("%s %s: body = %s, want %s", tc.method, tc.path, got, want)
		}
	}
}

// TestUpstreamRedirectRelayedVerbatim pins the redirect policy: a 3xx from
// the upstream is relayed verbatim, never followed. Following one would
// silently convert the POST into a body-less GET (301/302/303), replay the
// transformed request body to an arbitrary Location target (307/308), and
// re-attach Authorization to anything on the same hostname.
func TestUpstreamRedirectRelayedVerbatim(t *testing.T) {
	var targetHits int
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		targetHits++
		_, _ = io.WriteString(w, `{}`)
	}))
	defer target.Close()

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Location", target.URL+"/elsewhere")
		w.WriteHeader(http.StatusFound)
	}))
	defer upstream.Close()

	h := newTestHandler(t, newTestStore(t, upstream.URL+"/v1"))
	rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions",
		`{"model":"test-model","messages":[{"role":"user","content":"hi"}]}`, nil)

	if rec.Code != http.StatusFound {
		t.Fatalf("status = %d, want 302 relayed verbatim (body %q)", rec.Code, rec.Body.String())
	}
	if loc := rec.Header().Get("Location"); loc != target.URL+"/elsewhere" {
		t.Errorf("Location = %q, want %q", loc, target.URL+"/elsewhere")
	}
	if targetHits != 0 {
		t.Errorf("redirect target was contacted %d times; redirects must never be followed", targetHits)
	}
}

func TestUpstreamRateLimitHeadersRelayed(t *testing.T) {
	// Operational headers drive client backoff; dropping them makes a 429
	// indistinguishable from any other upstream error to a well-behaved SDK.
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Retry-After", "30")
		w.Header().Set("X-RateLimit-Limit", "100")
		w.Header().Set("X-RateLimit-Remaining", "0")
		w.Header().Set("X-RateLimit-Reset-Tokens", "1.5s")
		w.Header().Set("OpenAI-Request-Id", "req_abc")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = io.WriteString(w, `{"error":{"message":"rate limited"}}`)
	}))
	defer upstream.Close()

	h := newTestHandler(t, newTestStore(t, upstream.URL+"/v1"))
	rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions", `{"model":"test-model"}`, nil)

	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429", rec.Code)
	}
	for name, want := range map[string]string{
		"Retry-After":              "30",
		"X-RateLimit-Limit":        "100",
		"X-RateLimit-Remaining":    "0",
		"X-RateLimit-Reset-Tokens": "1.5s",
		"OpenAI-Request-Id":        "req_abc",
	} {
		if got := rec.Header().Get(name); got != want {
			t.Errorf("%s = %q, want %q", name, got, want)
		}
	}
	if got := rec.Header().Get("X-Secret"); got != "" {
		t.Errorf("X-Secret = %q, want empty (allow-list still holds)", got)
	}
}

func TestUpstreamPathTrailingSlashesNormalized(t *testing.T) {
	// "trailing slashes ignored" must hold for any number of them: an
	// endpoint ending in `//` previously produced `//chat/completions`,
	// which many routers 404 with the cause invisible.
	var gotPath string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"model":"upstream-name"}`)
	}))
	defer upstream.Close()

	h := newTestHandler(t, newTestStore(t, upstream.URL+"//")) // note the double slash
	rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions", `{"model":"test-model"}`, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if gotPath != "/chat/completions" {
		t.Errorf("upstream path = %q, want /chat/completions", gotPath)
	}
}

func TestUpstream204RelayedVerbatim(t *testing.T) {
	// 204 and 304 are defined to have no body; that is a valid upstream
	// answer, not an invalid response, and must not become a 502.
	for _, status := range []int{http.StatusNoContent, http.StatusNotModified} {
		upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(status)
		}))
		h := newTestHandler(t, newTestStore(t, upstream.URL+"/v1"))
		rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions", `{"model":"test-model"}`, nil)
		upstream.Close()
		if rec.Code != status {
			t.Errorf("upstream %d: client status = %d, want verbatim %d", status, rec.Code, status)
		}
	}
}

func TestRequestBodyOverCapRejected413(t *testing.T) {
	// A client (or attacker) must not be able to pin unbounded memory in
	// the proxy with an arbitrarily large body.
	old := maxRequestBodyBytes
	maxRequestBodyBytes = 1 << 20 // 1 MiB for the test
	defer func() { maxRequestBodyBytes = old }()

	h := newTestHandler(t, newTestStore(t, "http://127.0.0.1:9/v1"))
	big := `{"model":"test-model","padding":"` + strings.Repeat("x", 2<<20) + `"}`
	rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions", big, nil)

	want := `{"error":{"message":"request body too large","type":"invalid_request_error","param":null,"code":null}}`
	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want 413", rec.Code)
	}
	if body := rec.Body.String(); body != want {
		t.Errorf("body mismatch:\n got %s\nwant %s", body, want)
	}
}

func TestUpstreamResponseOverCapRejected502(t *testing.T) {
	old := maxBufferedResponseBytes
	maxBufferedResponseBytes = 1 << 20 // 1 MiB for the test
	defer func() { maxBufferedResponseBytes = old }()

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"model":"upstream-name","padding":"`+strings.Repeat("x", 2<<20)+`"}`)
	}))
	defer upstream.Close()

	h := newTestHandler(t, newTestStore(t, upstream.URL+"/v1"))
	rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions", `{"model":"test-model"}`, nil)
	if rec.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502", rec.Code)
	}
	if body := rec.Body.String(); body != envelopeUpInvalid {
		t.Errorf("body mismatch:\n got %s\nwant %s", body, envelopeUpInvalid)
	}
}

// TestStreamAndBufferedRewriteParity pins that a streamed chunk and a
// buffered body carrying the same JSON are rewritten identically — the
// streaming path must never leak the upstream name where the buffered path
// would rewrite it, or vice versa.
func TestStreamAndBufferedRewriteParity(t *testing.T) {
	payload := `{"model":"upstream-name","n":1e400}` // valid JSON, overflow number
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/chat/completions" {
			t.Errorf("upstream path = %q", r.URL.Path)
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: "+payload+"\n\n")
	}))
	defer upstream.Close()

	h := newTestHandler(t, newTestStore(t, upstream.URL+"/v1"))
	rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions", `{"model":"test-model","stream":true}`, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body %q)", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"model":"test-model","n":1e400`) {
		t.Errorf("streamed overflow payload not rewritten: %q", rec.Body.String())
	}
	if strings.Contains(rec.Body.String(), "upstream-name") {
		t.Errorf("upstream name leaked to client: %q", rec.Body.String())
	}
}

// TestUnknownPathsJSON404 pins the catch-all: every path the exact routes do
// not match — unknown paths, trailing slashes, wrong case — answers with the
// OpenAI-compatible JSON envelope, never the mux's plain-text default, so
// SDK clients always receive an error body they can decode.
func TestUnknownPathsJSON404(t *testing.T) {
	h := newTestHandler(t, newTestStore(t, "http://127.0.0.1:9/v1"))
	for _, tc := range []struct{ method, path string }{
		{http.MethodGet, "/nope"},
		{http.MethodGet, "/"},
		{http.MethodPost, "/v1/chat/completions/"},
		{http.MethodPost, "/V1/RESPONSES"},
		{http.MethodPost, "/v1/embeddings"},
	} {
		rec := doRequest(t, h, tc.method, tc.path, `{}`, nil)
		if rec.Code != http.StatusNotFound {
			t.Errorf("%s %s: status = %d, want 404", tc.method, tc.path, rec.Code)
			continue
		}
		if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
			t.Errorf("%s %s: Content-Type = %q, want application/json", tc.method, tc.path, ct)
		}
		var env struct {
			Error struct {
				Message string `json:"message"`
				Type    string `json:"type"`
			} `json:"error"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &env); err != nil {
			t.Errorf("%s %s: body not valid envelope JSON: %v (%q)", tc.method, tc.path, err, rec.Body.String())
			continue
		}
		if !strings.HasPrefix(env.Error.Message, "Invalid URL (") {
			t.Errorf("%s %s: message = %q, want the \"Invalid URL (...)\" shape", tc.method, tc.path, env.Error.Message)
		}
		if env.Error.Type != "invalid_request_error" {
			t.Errorf("%s %s: type = %q, want invalid_request_error", tc.method, tc.path, env.Error.Type)
		}
	}
}

// TestModelNotFoundPreservesNameBytes pins the byte-exact envelope for names
// carrying characters Go's default JSON encoding would HTML-escape (< > &):
// the requested model is interpolated verbatim into the documented shape.
func TestModelNotFoundPreservesNameBytes(t *testing.T) {
	h := newTestHandler(t, newTestStore(t, "http://127.0.0.1:9/v1"))
	rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions", `{"model":"ghost<&>"}`, nil)
	want := `{"error":{"message":"The model 'ghost<&>' does not exist or you do not have access to it.","type":"invalid_request_error","param":null,"code":"model_not_found"}}`
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rec.Code)
	}
	if got := rec.Body.String(); got != want {
		t.Errorf("body mismatch (HTML-escaped or reshaped):\n got %s\nwant %s", got, want)
	}
}

// TestClientCancelBeforeUpstreamAnswer pins the cancel classification: a
// client that goes away while the upstream request is in flight gets the
// client_disconnected outcome at WARN — not a 502 upstream_unreachable — and
// no error envelope is written, so the access log's status stays
// uncommitted.
func TestClientCancelBeforeUpstreamAnswer(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		<-release
	}))
	// Declared after up.Close on purpose: defers run LIFO, so the upstream
	// handler is unblocked before the server joins it.
	defer up.Close()
	defer close(release)

	var logs bytes.Buffer
	h := NewHandler(newTestStore(t, up.URL+"/v1"), NewSharedClient(), zerolog.New(&logs))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions",
		strings.NewReader(`{"model":"test-model","messages":[]}`)).WithContext(ctx)
	req.Header.Set("Authorization", "Bearer "+testAPIKey)
	rec := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		h.ServeHTTP(rec, req)
		close(done)
	}()
	<-entered
	cancel()
	<-done

	if got := rec.Body.String(); got != "" {
		t.Errorf("envelope written for a gone client: %q", got)
	}
	var sawCompleted, sawFailed bool
	for _, line := range strings.Split(strings.TrimSpace(logs.String()), "\n") {
		var m map[string]any
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Fatalf("log line not JSON: %v (%q)", err, line)
		}
		switch m["message"] {
		case "request_completed":
			sawCompleted = true
			if m["outcome"] != "client_disconnected" {
				t.Errorf("outcome = %v, want client_disconnected (%s)", m["outcome"], line)
			}
			if m["status"] != float64(0) {
				t.Errorf("status = %v, want 0 (no response committed)", m["status"])
			}
		case "upstream_request_failed":
			sawFailed = true
			if m["level"] != "warn" {
				t.Errorf("level = %v, want warn", m["level"])
			}
			if m["error_class"] != "client_canceled" {
				t.Errorf("error_class = %v, want client_canceled", m["error_class"])
			}
		}
	}
	if !sawCompleted || !sawFailed {
		t.Fatalf("missing events: request_completed=%v upstream_request_failed=%v\n%s", sawCompleted, sawFailed, logs.String())
	}
}

// TestStreamLineLimitOutcome pins the limit-breach outcome chain, which no
// other test observed: an upstream line crossing MaxLineBytes truncates the
// relay under a committed 200, and the access log reports outcome
// stream_limit_exceeded with the WARN stream_truncated event carrying phase
// upstream_limit — the wall a hostile peer runs into is its own class, never
// folded into an upstream read failure.
func TestStreamLineLimitOutcome(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: {\"model\":\"upstream-name\"}\n\n")
		_, _ = io.WriteString(w, "data: "+strings.Repeat("x", MaxLineBytes+1)+"\n")
	}))
	defer up.Close()

	var logs bytes.Buffer
	h := NewHandler(newTestStore(t, up.URL+"/v1"), NewSharedClient(), zerolog.New(&logs))
	rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions", `{"model":"test-model","stream":true}`, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (status committed before the truncation)", rec.Code)
	}

	var sawCompleted, sawTruncated bool
	for _, line := range strings.Split(strings.TrimSpace(logs.String()), "\n") {
		var m map[string]any
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Fatalf("log line not JSON: %v (%q)", err, line)
		}
		switch m["message"] {
		case "request_completed":
			sawCompleted = true
			if m["outcome"] != "stream_limit_exceeded" {
				t.Errorf("outcome = %v, want stream_limit_exceeded (%s)", m["outcome"], line)
			}
			if m["status"] != float64(http.StatusOK) {
				t.Errorf("status = %v, want 200", m["status"])
			}
		case "stream_truncated":
			sawTruncated = true
			if m["level"] != "warn" {
				t.Errorf("level = %v, want warn", m["level"])
			}
			if m["phase"] != "upstream_limit" {
				t.Errorf("phase = %v, want upstream_limit", m["phase"])
			}
		}
	}
	if !sawCompleted || !sawTruncated {
		t.Fatalf("missing events: request_completed=%v stream_truncated=%v\n%s", sawCompleted, sawTruncated, logs.String())
	}
}

// TestConcurrentReloadAndTraffic pins the snapshot discipline under the race
// detector: a publisher swapping snapshots the way the poller does, while
// concurrent traffic loads and serves. Every request must answer from one
// coherent snapshot — the response model is always the public name that was
// requested (never the upstream alias), whatever the publisher races. CI
// runs this package under -race.
func TestConcurrentReloadAndTraffic(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"model":"upstream-name","choices":[]}`))
	}))
	defer up.Close()

	yaml := fmt.Sprintf("api-key: %s\nmodels:\n  model-a:\n    endpoint: %s\n    upstream-model: upstream-name\n    injection-prompt: \"\"\n  model-b:\n    endpoint: %s\n    upstream-model: upstream-name\n    injection-prompt: \"\"\n", testAPIKey, up.URL+"/v1", up.URL+"/v1")
	snap, err := config.LoadRuntime([]byte(yaml))
	if err != nil {
		t.Fatalf("LoadRuntime: %v", err)
	}
	store := config.NewStore(snap)
	h := NewHandler(store, NewSharedClient(), zerolog.Nop())

	stop := make(chan struct{})
	pubDone := make(chan struct{})
	var publishes atomic.Int64
	go func() {
		defer close(pubDone)
		for {
			select {
			case <-stop:
				return
			default:
			}
			next, err := config.LoadRuntime([]byte(yaml))
			if err != nil {
				t.Errorf("reload: %v", err)
				return
			}
			if err := store.Publish(next); err != nil {
				t.Errorf("publish: %v", err)
				return
			}
			publishes.Add(1)
		}
	}()

	const workers = 8
	const perWorker = 100
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				name := "model-a"
				if (w+i)%2 == 0 {
					name = "model-b"
				}
				rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions",
					`{"model":"`+name+`","messages":[]}`, nil)
				if rec.Code != http.StatusOK {
					t.Errorf("%s: status = %d body %s", name, rec.Code, rec.Body.String())
					return
				}
				var body map[string]any
				if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
					t.Errorf("%s: body not JSON: %v (%q)", name, err, rec.Body.String())
					return
				}
				if body["model"] != name {
					t.Errorf("%s: response model = %v, want the requested public name (alias leak or torn snapshot)", name, body["model"])
					return
				}
			}
		}(w)
	}
	wg.Wait()
	close(stop)
	<-pubDone
	if publishes.Load() == 0 {
		t.Error("publisher never ran; the test pinned nothing")
	}
}

// TestSanitizeUpstreamErrorRedactsNestedURLBytes pins the no-echo rule one
// layer deeper: the nested url errors a *url.Error can carry quote raw
// bytes (the offending escape sequence, the rejected host). They are
// replaced with static text; only the scheme+host origin survives.
func TestSanitizeUpstreamErrorRedactsNestedURLBytes(t *testing.T) {
	endpoint := &url.URL{Scheme: "http", Host: "up.example:1"}

	sanitized := sanitizeUpstreamError(
		&url.Error{Op: "Post", URL: "http://up.example:1/v1?api-key=S", Err: url.EscapeError("%zz")},
		endpoint)
	msg := sanitized.Error()
	if strings.Contains(msg, "%zz") {
		t.Errorf("sanitized error echoes the raw escape bytes: %q", msg)
	}
	if !strings.Contains(msg, "invalid URL escape") {
		t.Errorf("sanitized error lost the failure class: %q", msg)
	}
	if !strings.Contains(msg, "http://up.example:1") {
		t.Errorf("sanitized error lost the origin: %q", msg)
	}

	sanitized = sanitizeUpstreamError(
		&url.Error{Op: "Post", URL: "http://HOSTMARK:1/", Err: url.InvalidHostError("HOSTMARK")},
		endpoint)
	if strings.Contains(sanitized.Error(), "HOSTMARK") {
		t.Errorf("sanitized error echoes the rejected host bytes: %q", sanitized.Error())
	}
}

// TestUpstreamTimeoutClassified pins the timeout branch of the upstream
// failure taxonomy: an upstream that accepts the connection and then goes
// quiet past the header deadline surfaces as the 502 upstream_unreachable
// envelope with error_class timeout — a genuine upstream failure, never a
// client disconnect. The deadline is the client's own ResponseHeaderTimeout
// (a hard local timer, not a race), so the test is deterministic; the shared
// client's zero value is pinned separately (long-lived SSE must not have one).
func TestUpstreamTimeoutClassified(t *testing.T) {
	// Accepts connections, never answers. The handler cannot wait on
	// r.Context(): the proxy forwards a body it never reads, and net/http
	// arms its client-disconnect detector only once the body hits EOF — so
	// the context survives the transport giving up. A test-owned channel
	// releases the handler at teardown instead (deferred LIFO: released
	// before Close waits on it).
	done := make(chan struct{})
	silent := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-done
	}))
	defer silent.Close()
	defer close(done)

	client := NewSharedClient()
	client.Transport.(*http.Transport).ResponseHeaderTimeout = 150 * time.Millisecond

	var logs bytes.Buffer
	h := NewHandler(newTestStore(t, silent.URL+"/v1"), client, zerolog.New(&logs))

	rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions", `{"model":"test-model"}`, nil)
	if rec.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502", rec.Code)
	}
	if body := rec.Body.String(); body != envelopeUpUnreach {
		t.Errorf("body = %s, want the upstream_unreachable envelope", body)
	}
	sawClass := false
	for _, line := range strings.Split(strings.TrimSpace(logs.String()), "\n") {
		var m map[string]any
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Fatalf("log line not JSON: %v (%q)", err, line)
		}
		if m["message"] == "upstream_request_failed" {
			sawClass = true
			if m["error_class"] != "timeout" {
				t.Errorf("error_class = %v, want timeout (%s)", m["error_class"], line)
			}
			if lvl, _ := m["level"].(string); lvl != "error" {
				t.Errorf("level = %v, want error (an upstream timeout is the upstream's failure)", m["level"])
			}
			if m["outcome"] != nil {
				t.Errorf("failure event must not carry the outcome; got %v", m["outcome"])
			}
		}
		if m["message"] == "request_completed" && m["outcome"] != "upstream_unreachable" {
			t.Errorf("outcome = %v, want upstream_unreachable", m["outcome"])
		}
	}
	if !sawClass {
		t.Fatalf("upstream_request_failed not logged:\n%s", logs.String())
	}
}

// TestUpstreamTLSFailureClassified pins the TLS branch: an upstream serving
// a certificate the shared client cannot verify (httptest's self-signed pair
// against the default verifier) fails the handshake and surfaces as the 502
// upstream_unreachable envelope with error_class tls — deterministic, no
// timing involved.
func TestUpstreamTLSFailureClassified(t *testing.T) {
	up := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, `{"model":"upstream-name"}`)
	}))
	defer up.Close()

	var logs bytes.Buffer
	// The shared client does not trust httptest's self-signed certificate.
	h := NewHandler(newTestStore(t, up.URL+"/v1"), NewSharedClient(), zerolog.New(&logs))

	rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions", `{"model":"test-model"}`, nil)
	if rec.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502", rec.Code)
	}
	if body := rec.Body.String(); body != envelopeUpUnreach {
		t.Errorf("body = %s, want the upstream_unreachable envelope", body)
	}
	sawClass := false
	for _, line := range strings.Split(strings.TrimSpace(logs.String()), "\n") {
		var m map[string]any
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Fatalf("log line not JSON: %v (%q)", err, line)
		}
		if m["message"] == "upstream_request_failed" {
			sawClass = true
			if m["error_class"] != "tls" {
				t.Errorf("error_class = %v, want tls (%s)", m["error_class"], line)
			}
			// The credential rule: scheme+host only, never the full URL with
			// any path/query the endpoint carried.
			if strings.Contains(fmt.Sprint(m["upstream"]), "/"+strings.TrimPrefix(up.URL, "http://")) {
				t.Errorf("upstream field carries more than the origin: %v", m["upstream"])
			}
		}
	}
	if !sawClass {
		t.Fatalf("upstream_request_failed not logged:\n%s", logs.String())
	}
}
