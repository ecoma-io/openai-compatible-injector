package proxy

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/rs/zerolog"

	"openai-compatible-injector/internal/config"
)

func newTestStore(t *testing.T, endpoint string) *config.Store {
	t.Helper()
	yaml := fmt.Sprintf("models:\n  test-model:\n    endpoint: %s\n    upstream-model: upstream-name\n    injection-prompt: \"\"\n", endpoint)
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
			"Authorization": "Bearer tok-123",
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
	if got := gotHdr.Get("Authorization"); got != "Bearer tok-123" {
		t.Errorf("Authorization = %q, want Bearer tok-123", got)
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
