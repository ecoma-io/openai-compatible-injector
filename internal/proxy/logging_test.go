package proxy

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"sync"
	"testing"

	"github.com/rs/zerolog"

	"openai-compatible-injector/internal/config"
)

// logBuffer is a mutex-guarded capture target: handler logging happens on
// the request goroutine while the test reads, and -race runs in CI.
type logBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *logBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *logBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// captureLog returns a buffer plus a logger at the given level.
func captureLog(level zerolog.Level) (*logBuffer, zerolog.Logger) {
	b := &logBuffer{}
	return b, zerolog.New(b).Level(level)
}

// events parses every captured line and returns the JSON objects whose
// message equals msg.
func (b *logBuffer) events(t *testing.T, msg string) []map[string]any {
	t.Helper()
	var found []map[string]any
	for _, line := range strings.Split(b.String(), "\n") {
		if line == "" {
			continue
		}
		var ev map[string]any
		if err := json.Unmarshal([]byte(line), &ev); err != nil {
			t.Fatalf("log line is not JSON: %q (%v)", line, err)
		}
		if ev["message"] == msg {
			found = append(found, ev)
		}
	}
	return found
}

// promptStore is a store whose injection prompt embeds a marker: if that
// marker ever reaches a log line, the payload rule is broken.
func promptStore(t *testing.T, endpoint, prompt string) *config.Store {
	t.Helper()
	yaml := fmt.Sprintf("models:\n  test-model:\n    endpoint: %s\n    upstream-model: upstream-name\n    injection-prompt: %q\n", endpoint, prompt)
	snap, err := config.LoadRuntime([]byte(yaml))
	if err != nil {
		t.Fatalf("LoadRuntime: %v", err)
	}
	return config.NewStore(snap)
}

// TestRequestCompletedLogLifecycle pins the access-log contract: exactly one
// INFO request_completed per request, carrying the wire facts — and never
// the payload. The planted markers (request body, injection prompt) must
// appear in no log line at any field.
func TestRequestCompletedLogLifecycle(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"model":"upstream-name","choices":[]}`))
	}))
	defer upstream.Close()

	buf, log := captureLog(zerolog.InfoLevel)
	h := NewHandler(promptStore(t, upstream.URL, "SECRET_PROMPT_VALUE inject me"), NewSharedClient(), log)

	rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions",
		`{"model":"test-model","stream":false,"messages":"SECRET_REQUEST_BODY"}`, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}

	evs := buf.events(t, "request_completed")
	if len(evs) != 1 {
		t.Fatalf("request_completed logged %d times, want exactly 1: %s", len(evs), buf.String())
	}
	ev := evs[0]
	if id, ok := ev["request_id"].(string); !ok || !regexp.MustCompile(`^[0-9a-f]{16}$`).MatchString(id) {
		t.Errorf("request_id = %v, want 16 hex chars", ev["request_id"])
	}
	if ev["api"] != "chat" {
		t.Errorf("api = %v, want chat", ev["api"])
	}
	if ev["status"].(float64) != 200 {
		t.Errorf("status = %v, want 200", ev["status"])
	}
	if ev["outcome"] != "completed" {
		t.Errorf("outcome = %v, want completed", ev["outcome"])
	}
	if ev["public_model"] != "test-model" {
		t.Errorf("public_model = %v, want test-model", ev["public_model"])
	}
	if ev["stream"] != false {
		t.Errorf("stream = %v, want false", ev["stream"])
	}
	if ev["bytes_in"].(float64) != float64(len(`{"model":"test-model","stream":false,"messages":"SECRET_REQUEST_BODY"}`)) {
		t.Errorf("bytes_in = %v", ev["bytes_in"])
	}
	if ev["bytes_out"].(float64) <= 0 {
		t.Errorf("bytes_out = %v, want > 0", ev["bytes_out"])
	}
	if _, ok := ev["duration_ms"]; !ok {
		t.Error("duration_ms missing")
	}
	if _, ok := ev["config_generation"]; !ok {
		t.Error("config_generation missing")
	}

	for _, secret := range []string{"SECRET_REQUEST_BODY", "SECRET_PROMPT_VALUE", "upstream-name"} {
		if strings.Contains(buf.String(), secret) {
			t.Errorf("log output contains %q — payload leak", secret)
		}
	}
}

// TestRequestCompletedOutcomes pins the outcome taxonomy on the early-exit
// paths: each client-visible rejection lands in request_completed with its
// own outcome, so an operator can tell the collapse causes apart.
func TestRequestCompletedOutcomes(t *testing.T) {
	cases := []struct {
		name       string
		body       string
		wantStatus float64
		want       string
	}{
		{"invalid json", `{nope`, 400, "invalid_json"},
		{"missing model", `{"messages":[]}`, 400, "missing_model"},
		{"unmapped model", `{"model":"no-such-model"}`, 404, "model_not_found"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			buf, log := captureLog(zerolog.InfoLevel)
			h := NewHandler(newTestStore(t, "http://127.0.0.1:1/v1"), NewSharedClient(), log)

			rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions", tc.body, nil)
			if rec.Code != int(tc.wantStatus) {
				t.Fatalf("status = %d, want %d", rec.Code, int(tc.wantStatus))
			}
			evs := buf.events(t, "request_completed")
			if len(evs) != 1 {
				t.Fatalf("request_completed logged %d times, want 1", len(evs))
			}
			if evs[0]["outcome"] != tc.want {
				t.Errorf("outcome = %v, want %q", evs[0]["outcome"], tc.want)
			}
			if evs[0]["status"].(float64) != tc.wantStatus {
				t.Errorf("status = %v, want %v", evs[0]["status"], tc.wantStatus)
			}
		})
	}
}

// errBody is a response body that yields some bytes, then a read error —
// an upstream that dies mid-answer.
type errBody struct {
	data []byte
	used bool
}

func (b *errBody) Read(p []byte) (int, error) {
	if b.used {
		return 0, errors.New("upstream connection reset")
	}
	b.used = true
	n := copy(p, b.data)
	return n, nil
}

func (b *errBody) Close() error { return nil }

// stubTransport answers every request with a canned response whose body is
// supplied by the test, without any network.
type stubTransport struct {
	resp *http.Response
}

func (s *stubTransport) RoundTrip(*http.Request) (*http.Response, error) {
	return s.resp, nil
}

// TestStreamTruncationPhaseLogging pins the truncation contract: a client
// write failure mid-stream logs WARN stream_truncated with phase
// client_write, an upstream read failure with phase upstream_read — and
// neither level nor payload ever changes the bytes on the wire.
func TestStreamTruncationPhaseLogging(t *testing.T) {
	t.Run("client_write", func(t *testing.T) {
		upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/event-stream")
			flusher(w)()
			for i := 0; i < 20; i++ {
				_, _ = io.WriteString(w, "data: {\"model\":\"upstream-name\",\"i\":"+fmt.Sprint(i)+"}\n\n")
				flusher(w)()
			}
		}))
		defer upstream.Close()

		// Info level: both the WARN truncation and the INFO completion line
		// must appear at the default level.
		buf, log := captureLog(zerolog.InfoLevel)
		h := NewHandler(newTestStore(t, upstream.URL), NewSharedClient(), log)

		// A recorder that stops accepting writes mid-stream — the client
		// disconnected.
		dying := &dyingRecorder{limit: 2}
		req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions",
			strings.NewReader(`{"model":"test-model","stream":true}`))
		h.ServeHTTP(dying, req)

		evs := buf.events(t, "stream_truncated")
		if len(evs) != 1 {
			t.Fatalf("stream_truncated logged %d times, want 1: %s", len(evs), buf.String())
		}
		if evs[0]["phase"] != "client_write" {
			t.Errorf("phase = %v, want client_write", evs[0]["phase"])
		}
		if evs[0]["public_model"] != "test-model" {
			t.Errorf("public_model = %v, want test-model", evs[0]["public_model"])
		}
		completed := buf.events(t, "request_completed")
		if len(completed) != 1 || completed[0]["outcome"] != "stream_truncated" {
			t.Fatalf("request_completed outcome = %v, want stream_truncated", completed)
		}
	})

	t.Run("upstream_read", func(t *testing.T) {
		buf, log := captureLog(zerolog.InfoLevel)
		client := &http.Client{Transport: &stubTransport{resp: &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": {"text/event-stream"}},
			Body:       &errBody{data: []byte("data: {\"model\":\"upstream-name\",\"x\":1}\n\n")},
			Request:    &http.Request{Method: http.MethodPost},
		}}}
		h := NewHandler(newTestStore(t, "http://127.0.0.1:1/v1"), client, log)

		rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions",
			`{"model":"test-model","stream":true}`, nil)
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200 (committed before the truncation)", rec.Code)
		}

		evs := buf.events(t, "stream_truncated")
		if len(evs) != 1 {
			t.Fatalf("stream_truncated logged %d times, want 1: %s", len(evs), buf.String())
		}
		if evs[0]["phase"] != "upstream_read" {
			t.Errorf("phase = %v, want upstream_read", evs[0]["phase"])
		}
		// The rewritten prefix must have reached the client before the read
		// error: truncation never switches the response to an error body.
		if !strings.Contains(rec.Body.String(), "test-model") ||
			strings.Contains(rec.Body.String(), "upstream-name") {
			t.Errorf("rewritten events lost on truncation: %q", rec.Body.String())
		}
	})
}

// dyingRecorder accepts limit writes, then fails like a vanished client.
type dyingRecorder struct {
	limit   int
	writes  int
	Header0 http.Header
}

func (d *dyingRecorder) Header() http.Header {
	if d.Header0 == nil {
		d.Header0 = http.Header{}
	}
	return d.Header0
}

func (d *dyingRecorder) Write(p []byte) (int, error) {
	if d.writes >= d.limit {
		return 0, errors.New("client connection reset")
	}
	d.writes++
	return len(p), nil
}

func (d *dyingRecorder) WriteHeader(int) {}
func (d *dyingRecorder) Flush()          {}

// TestRelayCopyFailureLogged pins the verbatim branch's copy failure: a
// body that dies mid-relay is logged (ERROR — not a client cancel), while
// the response status stays whatever upstream committed.
func TestRelayCopyFailureLogged(t *testing.T) {
	buf, log := captureLog(zerolog.InfoLevel)
	client := &http.Client{Transport: &stubTransport{resp: &http.Response{
		StatusCode: http.StatusTeapot,
		Header:     http.Header{"Content-Type": {"text/plain"}},
		Body:       &errBody{data: []byte("partial")},
		Request:    &http.Request{Method: http.MethodPost},
	}}}
	h := NewHandler(newTestStore(t, "http://127.0.0.1:1/v1"), client, log)

	rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions",
		`{"model":"test-model"}`, nil)
	if rec.Code != http.StatusTeapot {
		t.Fatalf("status = %d, want 418 (relayed verbatim)", rec.Code)
	}

	evs := buf.events(t, "relay_copy_failed")
	if len(evs) != 1 {
		t.Fatalf("relay_copy_failed logged %d times, want 1", len(evs))
	}
	completed := buf.events(t, "request_completed")
	if len(completed) != 1 || completed[0]["outcome"] != "relayed" {
		t.Fatalf("request_completed = %v, want outcome relayed", completed)
	}
}

// TestMethodNotAllowedOutsideLifecycle pins the 405 path: it never loads a
// snapshot and emits no request_completed — a wrong method is not proxy
// traffic.
func TestMethodNotAllowedOutsideLifecycle(t *testing.T) {
	buf, log := captureLog(zerolog.InfoLevel)
	h := NewHandler(newTestStore(t, "http://127.0.0.1:1/v1"), NewSharedClient(), log)

	rec := doRequest(t, h, http.MethodGet, "/v1/chat/completions", "", nil)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want 405", rec.Code)
	}
	if evs := buf.events(t, "request_completed"); len(evs) != 0 {
		t.Errorf("request_completed logged for a 405: %s", buf.String())
	}
}

// TestBodyTooLargeOutcome pins the 413 path's outcome label.
func TestBodyTooLargeOutcome(t *testing.T) {
	oldCap := maxRequestBodyBytes
	maxRequestBodyBytes = 16
	defer func() { maxRequestBodyBytes = oldCap }()

	buf, log := captureLog(zerolog.InfoLevel)
	h := NewHandler(newTestStore(t, "http://127.0.0.1:1/v1"), NewSharedClient(), log)

	rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions",
		`{"model":"test-model","overflow":"xxxxxxxxxxxxxxxxxxxxxxxx"}`, nil)
	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want 413", rec.Code)
	}
	evs := buf.events(t, "request_completed")
	if len(evs) != 1 || evs[0]["outcome"] != "body_too_large" {
		t.Fatalf("request_completed = %v, want outcome body_too_large", evs)
	}
}
