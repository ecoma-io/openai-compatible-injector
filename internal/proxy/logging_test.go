package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"sync"
	"syscall"
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
		if len(completed) != 1 || completed[0]["outcome"] != "client_disconnected" {
			t.Fatalf("request_completed outcome = %v, want client_disconnected", completed)
		}
		if evs[0]["public_model"] == nil {
			t.Errorf("truncation event missing public_model")
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

	t.Run("client_cancel_via_read", func(t *testing.T) {
		// The client went away mid-stream and the canceled request context
		// surfaced through the next upstream READ instead of a write: the
		// classification must still be the client's disconnect, never a
		// stream_truncated blamed on the upstream.
		buf, log := captureLog(zerolog.InfoLevel)
		client := &http.Client{Transport: &stubTransport{resp: &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": {"text/event-stream"}},
			Body:       &canceledBody{},
			Request:    &http.Request{Method: http.MethodPost},
		}}}
		h := NewHandler(newTestStore(t, "http://127.0.0.1:1/v1"), client, log)

		rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions",
			`{"model":"test-model","stream":true}`, nil)
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200 (committed before the disconnect)", rec.Code)
		}

		evs := buf.events(t, "stream_truncated")
		if len(evs) != 1 {
			t.Fatalf("stream_truncated logged %d times, want 1: %s", len(evs), buf.String())
		}
		if evs[0]["phase"] != "client_write" {
			t.Errorf("phase = %v, want client_write (the client canceled, not the upstream)", evs[0]["phase"])
		}
		completed := buf.events(t, "request_completed")
		if len(completed) != 1 || completed[0]["outcome"] != "client_disconnected" {
			t.Fatalf("request_completed outcome = %v, want client_disconnected", completed)
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
// body that dies mid-relay is logged as an upstream read (ERROR — not a
// client cancel), the outcome says upstream_read_failed — never "relayed",
// which would report a truncated body as a finished one — while the
// response status stays whatever upstream committed.
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
	if evs[0]["phase"] != "upstream_read" {
		t.Errorf("phase = %v, want upstream_read", evs[0]["phase"])
	}
	completed := buf.events(t, "request_completed")
	if len(completed) != 1 || completed[0]["outcome"] != "upstream_read_failed" {
		t.Fatalf("request_completed = %v, want outcome upstream_read_failed", completed)
	}
}

// canceledBody is an upstream body whose read fails with context.Canceled —
// what the transport surfaces when the client request context is canceled
// (the client went away mid-answer).
type canceledBody struct{ used bool }

func (b *canceledBody) Read(p []byte) (int, error) {
	if b.used {
		return 0, context.Canceled
	}
	b.used = true
	s := "partial"
	return copy(p, s), nil
}

func (b *canceledBody) Close() error { return nil }

// shortResponseWriter reports fewer bytes written than it was given, with a
// nil error — the io.Writer contract's second failure mode.
type shortResponseWriter struct {
	Header0 http.Header
}

func (s *shortResponseWriter) Header() http.Header {
	if s.Header0 == nil {
		s.Header0 = http.Header{}
	}
	return s.Header0
}
func (s *shortResponseWriter) Write(p []byte) (int, error) { return len(p) / 2, nil }
func (s *shortResponseWriter) WriteHeader(int)             {}

// TestRelayOutcomeClassification pins the verbatim and buffered branches'
// failure taxonomy: a write-side failure of any shape (error, short write,
// broken pipe) and a canceled context surface as WARN client-side failures
// with outcome client_disconnected — never as a silent completion, and
// never as an upstream failure.
func TestRelayOutcomeClassification(t *testing.T) {
	verbatim := func(body io.ReadCloser) (buf *logBuffer, h http.Handler) {
		buf, log := captureLog(zerolog.InfoLevel)
		client := &http.Client{Transport: &stubTransport{resp: &http.Response{
			StatusCode: http.StatusTeapot,
			Header:     http.Header{"Content-Type": {"text/plain"}},
			Body:       body,
			Request:    &http.Request{Method: http.MethodPost},
		}}}
		return buf, NewHandler(newTestStore(t, "http://127.0.0.1:1/v1"), client, log)
	}
	expectDisconnected := func(t *testing.T, buf *logBuffer, slug string) {
		t.Helper()
		completed := buf.events(t, "request_completed")
		if len(completed) != 1 {
			t.Fatalf("request_completed logged %d times, want 1: %s", len(completed), buf.String())
		}
		if completed[0]["outcome"] != "client_disconnected" {
			t.Fatalf("outcome = %v, want client_disconnected", completed[0]["outcome"])
		}
		evs := buf.events(t, slug)
		if len(evs) != 1 {
			t.Fatalf("%s logged %d times, want 1: %s", slug, len(evs), buf.String())
		}
		if evs[0]["phase"] != "client_write" {
			t.Errorf("phase = %v, want client_write", evs[0]["phase"])
		}
		if lvl, ok := evs[0]["level"].(string); !ok || lvl != "warn" {
			t.Errorf("level = %v, want warn (a disconnect is operational, not an error)", evs[0]["level"])
		}
	}

	t.Run("verbatim_write_error", func(t *testing.T) {
		buf, h := verbatim(io.NopCloser(strings.NewReader("body bytes")))
		dying := &dyingRecorder{limit: 0}
		req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"test-model"}`))
		h.ServeHTTP(dying, req)
		expectDisconnected(t, buf, "relay_copy_failed")
	})

	t.Run("verbatim_broken_pipe", func(t *testing.T) {
		buf, h := verbatim(io.NopCloser(strings.NewReader("body bytes")))
		pipe := &pipeRecorder{}
		req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"test-model"}`))
		h.ServeHTTP(pipe, req)
		expectDisconnected(t, buf, "relay_copy_failed")
	})

	t.Run("verbatim_short_write", func(t *testing.T) {
		buf, h := verbatim(io.NopCloser(strings.NewReader("body bytes")))
		short := &shortResponseWriter{}
		req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"test-model"}`))
		h.ServeHTTP(short, req)
		expectDisconnected(t, buf, "relay_copy_failed")
	})

	t.Run("verbatim_canceled_context", func(t *testing.T) {
		// The canceled request context surfaces through the upstream READ —
		// io.Copy's error is context.Canceled with no write wrapper. It is
		// still a client-side disconnect: the context only cancels when the
		// client goes away.
		buf, h := verbatim(&canceledBody{})
		rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions", `{"model":"test-model"}`, nil)
		if rec.Code != http.StatusTeapot {
			t.Fatalf("status = %d, want 418", rec.Code)
		}
		expectDisconnected(t, buf, "relay_copy_failed")
	})

	t.Run("buffered_read_failure", func(t *testing.T) {
		// An upstream that dies mid-body on the buffered path: its own
		// outcome (upstream_read_failed), a WARN, and the same client-502
		// envelope as an unparseable body — the wire contract is unchanged,
		// the log distinguishes the causes.
		buf, log := captureLog(zerolog.InfoLevel)
		client := &http.Client{Transport: &stubTransport{resp: &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": {"application/json"}},
			Body:       &errBody{data: []byte(`{"model":"upstream-name","partial`)},
			Request:    &http.Request{Method: http.MethodPost},
		}}}
		h := NewHandler(newTestStore(t, "http://127.0.0.1:1/v1"), client, log)

		rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions", `{"model":"test-model"}`, nil)
		if rec.Code != http.StatusBadGateway {
			t.Fatalf("status = %d, want 502", rec.Code)
		}
		completed := buf.events(t, "request_completed")
		if len(completed) != 1 || completed[0]["outcome"] != "upstream_read_failed" {
			t.Fatalf("request_completed = %v, want outcome upstream_read_failed", completed)
		}
		if evs := buf.events(t, "upstream_body_read_failed"); len(evs) != 1 {
			t.Fatalf("upstream_body_read_failed logged %d times, want 1: %s", len(evs), buf.String())
		}
	})

	t.Run("buffered_write_error", func(t *testing.T) {
		upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"model":"upstream-name","choices":[]}`))
		}))
		defer upstream.Close()

		buf, log := captureLog(zerolog.InfoLevel)
		h := NewHandler(promptStore(t, upstream.URL, "prompt"), NewSharedClient(), log)
		dying := &dyingRecorder{limit: 0}
		req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"test-model"}`))
		h.ServeHTTP(dying, req)

		completed := buf.events(t, "request_completed")
		if len(completed) != 1 || completed[0]["outcome"] != "client_disconnected" {
			t.Fatalf("request_completed = %v, want outcome client_disconnected", completed)
		}
		if evs := buf.events(t, "client_write_failed"); len(evs) != 1 {
			t.Fatalf("client_write_failed logged %d times, want 1: %s", len(evs), buf.String())
		}
	})

	t.Run("buffered_canceled_context", func(t *testing.T) {
		// The canceled request context surfaces through the upstream READ
		// on the buffered path: the outcome is the client's disconnect, the
		// upstream is never blamed (no upstream_body_read_failed), and no
		// 502 envelope write is attempted for a client that is gone.
		buf, log := captureLog(zerolog.InfoLevel)
		client := &http.Client{Transport: &stubTransport{resp: &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": {"application/json"}},
			Body:       &canceledBody{},
			Request:    &http.Request{Method: http.MethodPost},
		}}}
		h := NewHandler(newTestStore(t, "http://127.0.0.1:1/v1"), client, log)

		rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions",
			`{"model":"test-model"}`, nil)
		if rec.Body.Len() != 0 {
			t.Errorf("envelope written for a disconnected client: %q", rec.Body.String())
		}
		expectDisconnected(t, buf, "relay_copy_failed")
		if evs := buf.events(t, "upstream_body_read_failed"); len(evs) != 0 {
			t.Errorf("upstream blamed for a client cancel: %s", buf.String())
		}
	})
}

// TestDebugLifecycleChain pins the DEBUG checkpoint sequence for one
// buffered request: every lifecycle checkpoint fires exactly once, in flow
// order, and the chain carries metadata only — the planted markers (request
// body, injection prompt, upstream query secret) appear in no event at any
// field even at maximum verbosity.
func TestDebugLifecycleChain(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.Header.Get("Authorization"), "SECRET") {
			t.Errorf("upstream did not receive the forwarded Authorization — forward rule broken, audit test invalid")
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"model":"upstream-name","choices":[]}`))
	}))
	defer upstream.Close()

	buf, log := captureLog(zerolog.DebugLevel)
	h := NewHandler(promptStore(t, upstream.URL, "SECRET_PROMPT_VALUE inject me"), NewSharedClient(), log)

	rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions",
		`{"model":"test-model","messages":"SECRET_REQUEST_BODY"}`,
		map[string]string{"Authorization": "Bearer SECRET_AUTH_VALUE"})
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}

	chain := []string{
		"request_received", "probe_completed", "model_resolved",
		"request_transform_started", "request_transform_completed",
		"upstream_request_started", "upstream_response_received",
		"response_transform_started", "response_transform_completed",
		"client_write_completed", "request_completed",
	}
	seen := make(map[string]int, len(chain))
	all := parseEvents(t, buf)
	for i, ev := range all {
		msg, _ := ev["message"].(string)
		if _, dup := seen[msg]; !dup {
			seen[msg] = i
		}
	}
	prev := -1
	for _, msg := range chain {
		idx, ok := seen[msg]
		if !ok {
			t.Fatalf("lifecycle checkpoint %q missing at debug level:\n%s", msg, buf.String())
		}
		if countMessage(all, msg) != 1 {
			t.Errorf("%s fired %d times, want exactly 1", msg, countMessage(all, msg))
		}
		if idx <= prev {
			t.Errorf("%s out of order (index %d, previous checkpoint at %d)", msg, idx, prev)
		}
		prev = idx
	}

	// The resolution checkpoint carries the routing facts an operator needs,
	// and nothing else.
	resolved := all[seen["model_resolved"]]
	if resolved["upstream_model"] != "upstream-name" {
		t.Errorf("model_resolved upstream_model = %v", resolved["upstream_model"])
	}
	if !strings.HasPrefix(resolved["upstream"].(string), "http://127.0.0.1:") {
		t.Errorf("model_resolved upstream = %v, want scheme+host origin only", resolved["upstream"])
	}
	if resolved["request_id"] == nil {
		t.Error("model_resolved missing request_id correlation")
	}

	// The upstream response checkpoint reports the status and content type.
	received := all[seen["upstream_response_received"]]
	if received["status"].(float64) != 200 {
		t.Errorf("upstream_response_received status = %v", received["status"])
	}

	// upstream_model is deliberately metadata (operator-authored config),
	// but the request body, the prompt, the credentials, and the upstream
	// response body's own bytes ("choices" only exists inside the relayed
	// payload) must appear nowhere.
	for _, secret := range []string{"SECRET_REQUEST_BODY", "SECRET_PROMPT_VALUE", "SECRET_AUTH_VALUE", `"choices"`} {
		if strings.Contains(buf.String(), secret) {
			t.Errorf("debug log output contains %q — payload leak", secret)
		}
	}
}

// TestStreamProgressHeartbeat pins the stream progress equivalent: the
// flush boundary doubles as the event counter, so a stream longer than the
// heartbeat interval emits periodic DEBUG stream_event_progress events with
// running counts — and a shorter one emits none.
func TestStreamProgressHeartbeat(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		flusher(w)()
		for i := 0; i < 300; i++ {
			_, _ = io.WriteString(w, "data: {\"model\":\"upstream-name\",\"i\":"+fmt.Sprint(i)+"}\n\n")
			flusher(w)()
		}
	}))
	defer upstream.Close()

	buf, log := captureLog(zerolog.DebugLevel)
	h := NewHandler(newTestStore(t, upstream.URL), NewSharedClient(), log)

	rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions",
		`{"model":"test-model","stream":true}`, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}

	all := parseEvents(t, buf)
	progress := filterMessage(all, "stream_event_progress")
	if len(progress) != 1 {
		t.Fatalf("stream_event_progress fired %d times, want 1 (300 events, heartbeat every %d):\n%s", len(progress), sseProgressEvery, buf.String())
	}
	if progress[0]["events"].(float64) != sseProgressEvery {
		t.Errorf("progress events = %v, want %d", progress[0]["events"], sseProgressEvery)
	}
	// The relayed event payloads (`"i":N` markers) appear nowhere; counts
	// only.
	if strings.Contains(buf.String(), `"i":`) {
		t.Error("stream log carries event payload — leak")
	}
}

// parseEvents decodes every captured line into objects, failing on
// non-JSON output.
func parseEvents(t *testing.T, b *logBuffer) []map[string]any {
	t.Helper()
	var evs []map[string]any
	for _, line := range strings.Split(b.String(), "\n") {
		if line == "" {
			continue
		}
		var ev map[string]any
		if err := json.Unmarshal([]byte(line), &ev); err != nil {
			t.Fatalf("log line is not JSON: %q (%v)", line, err)
		}
		evs = append(evs, ev)
	}
	return evs
}

func countMessage(evs []map[string]any, msg string) int {
	return len(filterMessage(evs, msg))
}

func filterMessage(evs []map[string]any, msg string) []map[string]any {
	var found []map[string]any
	for _, ev := range evs {
		if ev["message"] == msg {
			found = append(found, ev)
		}
	}
	return found
}

// pipeRecorder fails its first write with EPIPE — the errno a real client
// disconnect produces on the write side.
type pipeRecorder struct {
	Header0 http.Header
}

func (p *pipeRecorder) Header() http.Header {
	if p.Header0 == nil {
		p.Header0 = http.Header{}
	}
	return p.Header0
}
func (p *pipeRecorder) Write(b []byte) (int, error) { return 0, syscall.EPIPE }
func (p *pipeRecorder) WriteHeader(int)             {}

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

// TestErrorEnvelopeWriteFailureOutcome pins outcome fidelity on the
// locally generated envelopes: when the client has gone away before the
// envelope can land, the request is accounted client_disconnected with a
// WARN client_write_failed — never with the envelope's own classification,
// which would report a decision as delivered when it reached nobody. Every
// rejection path is exercised: the write dies on the first attempt, so the
// classification outcome on the old code would be the one reported.
func TestErrorEnvelopeWriteFailureOutcome(t *testing.T) {
	cases := []struct {
		name        string
		store       func(t *testing.T) *config.Store
		client      func(t *testing.T) *http.Client
		bodyCap     int64
		body        string
		wantAttempt int
	}{
		{
			name:        "invalid_json",
			store:       func(t *testing.T) *config.Store { return newTestStore(t, "http://127.0.0.1:1/v1") },
			body:        `{nope`,
			wantAttempt: http.StatusBadRequest,
		},
		{
			name:        "missing_model",
			store:       func(t *testing.T) *config.Store { return newTestStore(t, "http://127.0.0.1:1/v1") },
			body:        `{"messages":[]}`,
			wantAttempt: http.StatusBadRequest,
		},
		{
			name:        "model_not_found",
			store:       func(t *testing.T) *config.Store { return newTestStore(t, "http://127.0.0.1:1/v1") },
			body:        `{"model":"no-such-model"}`,
			wantAttempt: http.StatusNotFound,
		},
		{
			name:        "body_too_large",
			store:       func(t *testing.T) *config.Store { return newTestStore(t, "http://127.0.0.1:1/v1") },
			bodyCap:     16,
			body:        `{"model":"test-model","overflow":"xxxxxxxxxxxxxxxxxxxxxxxx"}`,
			wantAttempt: http.StatusRequestEntityTooLarge,
		},
		{
			name:        "upstream_unreachable",
			store:       func(t *testing.T) *config.Store { return newTestStore(t, "http://127.0.0.1:1/v1") },
			body:        `{"model":"test-model"}`,
			wantAttempt: http.StatusBadGateway,
		},
		{
			name: "upstream_invalid_response",
			store: func(t *testing.T) *config.Store {
				return newTestStore(t, "http://stub.invalid/v1")
			},
			client: func(t *testing.T) *http.Client {
				return &http.Client{Transport: &stubTransport{resp: &http.Response{
					StatusCode: http.StatusOK,
					Header:     http.Header{"Content-Type": {"application/json"}},
					Body:       io.NopCloser(strings.NewReader("definitely not json")),
					Request:    &http.Request{Method: http.MethodPost},
				}}}
			},
			body:        `{"model":"test-model"}`,
			wantAttempt: http.StatusBadGateway,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.bodyCap != 0 {
				old := maxRequestBodyBytes
				maxRequestBodyBytes = tc.bodyCap
				defer func() { maxRequestBodyBytes = old }()
			}
			buf, log := captureLog(zerolog.InfoLevel)
			client := NewSharedClient()
			if tc.client != nil {
				client = tc.client(t)
			}
			h := NewHandler(tc.store(t), client, log)

			// The client died before anything could be written: every
			// envelope write attempt fails on the spot.
			dying := &dyingRecorder{limit: 0}
			req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions",
				strings.NewReader(tc.body))
			h.ServeHTTP(dying, req)

			completed := buf.events(t, "request_completed")
			if len(completed) != 1 {
				t.Fatalf("request_completed logged %d times, want 1: %s", len(completed), buf.String())
			}
			if completed[0]["outcome"] != "client_disconnected" {
				t.Errorf("outcome = %v, want client_disconnected (the envelope never reached the client)", completed[0]["outcome"])
			}
			if completed[0]["status"].(float64) != float64(tc.wantAttempt) {
				t.Errorf("status = %v, want %d (the attempted envelope's status)", completed[0]["status"], tc.wantAttempt)
			}
			failed := buf.events(t, "client_write_failed")
			if len(failed) != 1 {
				t.Fatalf("client_write_failed logged %d times, want 1: %s", len(failed), buf.String())
			}
			if lvl, ok := failed[0]["level"].(string); !ok || lvl != "warn" {
				t.Errorf("level = %v, want warn", failed[0]["level"])
			}
		})
	}
}
