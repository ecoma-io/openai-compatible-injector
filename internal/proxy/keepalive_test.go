package proxy

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"openai-compatible-injector/internal/config"
)

// newKeepAliveStore builds a store for the "test-model" mapping plus a
// verbatim top-level sse-keep-alive block ("" = block absent, defaults).
func newKeepAliveStore(t *testing.T, endpoint, block string) *config.Store {
	t.Helper()
	yaml := fmt.Sprintf("api-key: %s\nmodels:\n  test-model:\n    endpoint: %s\n    upstream-model: upstream-name\n%s",
		testAPIKey, endpoint, block)
	snap, err := config.LoadRuntime([]byte(yaml))
	if err != nil {
		t.Fatalf("LoadRuntime: %v", err)
	}
	return config.NewStore(snap)
}

// chatEvent renders one chat SSE data line for the upstream model; the
// relay rewrites the model back to the public name, byte-preserving the
// rest.
func chatEvent(public, content string) string {
	return fmt.Sprintf("data: {\"id\":\"s1\",\"model\":%q,\"choices\":[{\"index\":0,\"delta\":{\"content\":%q}}]}\n\n", public, content)
}

// runStream serves one streaming chat request against h and returns the
// full client body.
func runStream(t *testing.T, h http.Handler) (int, string) {
	t.Helper()
	rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions",
		`{"model":"test-model","messages":[{"role":"user","content":"hi"}],"stream":true}`, nil)
	return rec.Code, rec.Body.String()
}

// countPings reports how many keep-alive comment events the client body
// carries.
func countPings(body string) int { return strings.Count(body, string(ssePing)) }

// TestPingWriterBoundaryRules pins the heartbeat's injection rule at the
// writer itself, with a synthetic clock: a ping goes out only when the
// configured silence has elapsed AND the stream sits at an event boundary —
// never mid-event, never twice in one interval, never after the stream
// finishes, and a relay write resets the silence clock.
func TestPingWriterBoundaryRules(t *testing.T) {
	interval := 20 * time.Millisecond
	var buf syncBuffer
	p := newPingWriter(&buf, func() {}, interval, time.Now())
	still := func(t *testing.T, at time.Time, wantPing bool) {
		t.Helper()
		before := countPings(buf.String())
		if !p.maybePing(at) {
			t.Fatal("maybePing reported the client gone on a healthy writer")
		}
		if got := countPings(buf.String()) > before; got != wantPing {
			t.Fatalf("at %v: ping written = %v, want %v", at, got, wantPing)
		}
	}
	base := time.Now()
	// Before the interval elapses: nothing.
	still(t, base.Add(interval/2), false)
	// At the interval, at the (still pristine) boundary: the first ping.
	still(t, base.Add(interval), true)
	if got := buf.String(); got != string(ssePing) {
		t.Fatalf("first ping = %q, want %q", got, ssePing)
	}
	// Too soon after the ping itself: nothing — the ping is traffic too.
	still(t, base.Add(interval+interval/2), false)
	still(t, base.Add(2*interval), true)

	// A relay write lands the stream mid-event; silence alone is no longer
	// enough. The write resets the clock regardless.
	if _, err := p.Write([]byte("data: {\"model\":\"x\"}\n")); err != nil {
		t.Fatalf("relay write: %v", err)
	}
	still(t, base.Add(time.Minute), false)
	// The completing blank line reopens the boundary.
	if _, err := p.Write([]byte("\n")); err != nil {
		t.Fatalf("boundary write: %v", err)
	}
	still(t, base.Add(2*time.Minute), true)

	// The chat terminal marker closes heartbeat eligibility immediately,
	// before the next upstream read sees EOF. A ticker racing that EOF must
	// not append a comment after [DONE].
	if _, err := p.Write([]byte("data: [DONE]\n")); err != nil {
		t.Fatalf("terminal write: %v", err)
	}
	still(t, base.Add(3*time.Hour), false)

	// A finished stream never pings again, and a fresh clock cannot revive
	// it.
	p.start()
	p.stopAndWait()
	still(t, base.Add(4*time.Hour), false)
}

// syncBuffer is a mutex-guarded byte buffer standing in for the client
// writer.
type syncBuffer struct {
	mu  sync.Mutex
	b   []byte
	err error
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return 0, s.err
	}
	s.b = append(s.b, p...)
	return len(p), nil
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return string(s.b)
}

// TestPingWriterStopsOnClientGone pins the heartbeat's other exit: a ping
// write that fails means the client is gone, and maybePing tells the runner
// to stop instead of hammering a dead connection once per interval.
func TestPingWriterStopsOnClientGone(t *testing.T) {
	buf := &syncBuffer{err: errors.New("connection reset")}
	p := newPingWriter(buf, func() {}, time.Millisecond, time.Now())
	if p.maybePing(time.Now().Add(2 * time.Millisecond)) {
		t.Fatal("maybePing kept running after a failed ping write")
	}
	if p.pingCount() != 0 {
		t.Fatalf("failed ping counted: %d", p.pingCount())
	}
}

// TestRunKeepAliveJoinsOnStop pins the no-leak contract at the runner
// level: stopAndWait returns only after the ticker goroutine has exited,
// so a handler that has returned has no writer in flight. The ticker fires
// throughout; a goroutine that ignored stop would hang this test.
func TestRunKeepAliveJoinsOnStop(t *testing.T) {
	var buf syncBuffer
	p := newPingWriter(&buf, func() {}, 2*time.Millisecond, time.Now())
	p.start()
	deadline := time.Now().Add(2 * time.Second)
	p.stopAndWait()
	if time.Now().After(deadline) {
		t.Fatal("stopAndWait blocked past the deadline")
	}
	select {
	case <-p.exited:
	default:
		t.Fatal("heartbeat goroutine still running after stopAndWait")
	}
	// No ping may follow the join, even well past the interval.
	n := countPings(buf.String())
	time.Sleep(10 * time.Millisecond)
	if got := countPings(buf.String()); got != n {
		t.Fatalf("ping written after stopAndWait: %d -> %d", n, got)
	}
}

// TestSSEKeepAlivePingsDuringUpstreamSilence is the headline behavior: a
// silent upstream gets one comment ping per interval at event boundaries,
// and the real events are still relayed byte-exactly (model rewrite
// included) once they arrive. Interval 1s is the config floor; the 15s
// default is pinned separately below.
func TestSSEKeepAlivePingsDuringUpstreamSilence(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		fl := w.(http.Flusher)
		_, _ = io.WriteString(w, chatEvent("upstream-name", "chunk1"))
		fl.Flush()
		time.Sleep(3200 * time.Millisecond) // several intervals of silence
		_, _ = io.WriteString(w, chatEvent("upstream-name", "chunk2"))
		fl.Flush()
		_, _ = io.WriteString(w, "data: [DONE]\n\n")
		fl.Flush()
	}))
	defer up.Close()

	h := newTestHandler(t, newKeepAliveStore(t, up.URL+"/v1", "sse-keep-alive:\n  interval: 1s\n"))
	status, body := runStream(t, h)
	if status != http.StatusOK {
		t.Fatalf("status = %d, want 200", status)
	}
	pings := countPings(body)
	// The ticker's phase starts at the header commit, the silence clock at
	// the first forwarded byte, so pings land in [interval, 2*interval)
	// after the last write — over 3.2s of silence that is 1-3 pings
	// depending on phasing. Anything outside means the heartbeat fired on
	// activity or never fired.
	if pings < 1 || pings > 3 {
		t.Fatalf("pings during 3.2s of silence = %d, want 1-3", pings)
	}
	// The relayed events survive byte-exact once the ignorable comments
	// are stripped — ping bytes and nothing else were added.
	want := chatEvent("test-model", "chunk1") + chatEvent("test-model", "chunk2") + "data: [DONE]\n\n"
	if got := strings.ReplaceAll(body, string(ssePing), ""); got != want {
		t.Fatalf("body with pings stripped =\n%q\nwant\n%q", got, want)
	}
}

// TestSSEKeepAliveQuietWhenUpstreamActive pins the other half of the reset
// rule: an upstream that never lets the client go silent must produce zero
// pings — a heartbeat on active traffic would be pure noise.
func TestSSEKeepAliveQuietWhenUpstreamActive(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		fl := w.(http.Flusher)
		for i := range 6 {
			_, _ = io.WriteString(w, chatEvent("upstream-name", fmt.Sprintf("chunk%d", i)))
			fl.Flush()
			time.Sleep(300 * time.Millisecond)
		}
		_, _ = io.WriteString(w, "data: [DONE]\n\n")
		fl.Flush()
	}))
	defer up.Close()

	h := newTestHandler(t, newKeepAliveStore(t, up.URL+"/v1", "sse-keep-alive:\n  interval: 1s\n"))
	status, body := runStream(t, h)
	if status != http.StatusOK {
		t.Fatalf("status = %d, want 200", status)
	}
	if pings := countPings(body); pings != 0 {
		t.Fatalf("active stream got %d pings, want 0", pings)
	}
	want := &strings.Builder{}
	for i := range 6 {
		want.WriteString(chatEvent("test-model", fmt.Sprintf("chunk%d", i)))
	}
	want.WriteString("data: [DONE]\n\n")
	if body != want.String() {
		t.Fatalf("active-stream body drifted:\n%q", body)
	}
}

// TestSSEKeepAliveDisabled pins the opt-out: enabled: false leaves a
// silent stream exactly as an unconfigured deployment would relay it — no
// ping bytes at all, however long the silence.
func TestSSEKeepAliveDisabled(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		fl := w.(http.Flusher)
		_, _ = io.WriteString(w, chatEvent("upstream-name", "chunk1"))
		fl.Flush()
		time.Sleep(1500 * time.Millisecond)
		_, _ = io.WriteString(w, "data: [DONE]\n\n")
		fl.Flush()
	}))
	defer up.Close()

	h := newTestHandler(t, newKeepAliveStore(t, up.URL+"/v1", "sse-keep-alive:\n  enabled: false\n"))
	status, body := runStream(t, h)
	if status != http.StatusOK {
		t.Fatalf("status = %d, want 200", status)
	}
	if pings := countPings(body); pings != 0 {
		t.Fatalf("disabled heartbeat wrote %d pings", pings)
	}
	want := chatEvent("test-model", "chunk1") + "data: [DONE]\n\n"
	if body != want {
		t.Fatalf("body =\n%q\nwant\n%q", body, want)
	}
}

// TestSSEKeepAliveDefaultOnAt15s pins the deployment-driven default at the
// behavior level: no block in the config, and a silence that crosses the
// default 15s interval still earns a ping. The quiet direction of a broken
// default is exactly the Cloudflare cut this feature exists to prevent.
func TestSSEKeepAliveDefaultOnAt15s(t *testing.T) {
	if testing.Short() {
		t.Skip("16s of real silence under -short")
	}
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		fl := w.(http.Flusher)
		// Flush the bare headers: without a byte or a flush, net/http holds
		// the upstream response off the wire and the whole "silence" happens
		// before the proxy has even committed its own headers — the
		// documented pre-first-byte window this feature does not cover.
		fl.Flush()
		// No event first: the silence window is the thinking phase before
		// any event byte, where the ticker's phase and the silence clock
		// start together — the first ping lands at the default 15s.
		time.Sleep(16 * time.Second)
		_, _ = io.WriteString(w, chatEvent("upstream-name", "chunk1"))
		fl.Flush()
		_, _ = io.WriteString(w, "data: [DONE]\n\n")
		fl.Flush()
	}))
	defer up.Close()

	// No sse-keep-alive block: the store's snapshot carries the defaults.
	h := newTestHandler(t, newKeepAliveStore(t, up.URL+"/v1", ""))
	status, body := runStream(t, h)
	if status != http.StatusOK {
		t.Fatalf("status = %d, want 200", status)
	}
	if pings := countPings(body); pings < 1 {
		t.Fatalf("default heartbeat produced %d pings over 16s of silence, want >= 1", pings)
	}
	if want := chatEvent("test-model", "chunk1") + "data: [DONE]\n\n"; strings.ReplaceAll(body, string(ssePing), "") != want {
		t.Fatalf("body with pings stripped drifted:\n%q", body)
	}
}

// TestSSEKeepAliveNonStreamingUntouched pins the scope rule: keep-alive
// applies only to client-facing SSE. A buffered JSON response is
// byte-identical to an unconfigured deployment — no ping bytes, no extra
// write, same rewrite.
func TestSSEKeepAliveNonStreamingUntouched(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"model":"upstream-name","choices":[{"message":{"role":"assistant","content":"hi"}}]}`))
	}))
	defer up.Close()

	// Default config: the heartbeat is on, and must not show up anywhere.
	h := newTestHandler(t, newKeepAliveStore(t, up.URL+"/v1", ""))
	rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions",
		`{"model":"test-model","messages":[{"role":"user","content":"hi"}]}`, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	want := `{"model":"test-model","choices":[{"message":{"role":"assistant","content":"hi"}}]}`
	if rec.Body.String() != want {
		t.Fatalf("buffered body = %q, want %q", rec.Body.String(), want)
	}
	if strings.Contains(rec.Body.String(), ": ping") {
		t.Fatal("ping bytes in a non-streaming response")
	}
}

// TestSSEKeepAliveClientDisconnectStopsHeartbeat pins the no-leak
// contract end to end: a client that walks away mid-silence must end the
// heartbeat with the handler — ServeHTTP returns (which requires the
// goroutine join in stopAndWait), the outcome is the disconnect, and the
// last state on the wire was a ping, proving the ticker was live until
// then.
func TestSSEKeepAliveClientDisconnectStopsHeartbeat(t *testing.T) {
	release := make(chan struct{})
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		fl := w.(http.Flusher)
		_, _ = io.WriteString(w, chatEvent("upstream-name", "chunk1"))
		fl.Flush()
		<-release // silent forever, from the client's point of view
	}))
	// LIFO: release the upstream handler before the server close waits on
	// it, or a failed assertion deadlocks the binary in cleanup.
	t.Cleanup(up.Close)
	t.Cleanup(func() { close(release) })

	h := newTestHandler(t, newKeepAliveStore(t, up.URL+"/v1", "sse-keep-alive:\n  interval: 1s\n"))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	var body syncBuffer
	go func() {
		defer close(done)
		req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions",
			strings.NewReader(`{"model":"test-model","messages":[{"role":"user","content":"hi"}],"stream":true}`)).
			WithContext(ctx)
		req.Header.Set("Authorization", "Bearer "+testAPIKey)
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req) // the statusWriter's writes land in rec
		body.mu.Lock()
		body.b = append([]byte(nil), rec.Body.Bytes()...)
		body.mu.Unlock()
	}()

	// Let at least one ping land — the first tick's phasing can miss by the
	// header-to-first-event gap, so wait past two ticks — then disconnect.
	time.Sleep(2600 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("handler did not return within 10s of the client disconnect — heartbeat not joined")
	}
	if pings := countPings(body.String()); pings < 1 {
		t.Fatalf("heartbeat was silent before the disconnect (%d pings), want >= 1", pings)
	}
}

// TestSSEKeepAliveResponsesPath pins the second streaming surface: the
// Responses API's event:/data: pairs get the same heartbeat during
// silence, with the envelope's nested model still rewritten — the ping
// must not disturb either.
func TestSSEKeepAliveResponsesPath(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		fl := w.(http.Flusher)
		_, _ = io.WriteString(w, "event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"model\":\"upstream-name\",\"delta\":\"hi\"}\n\n")
		fl.Flush()
		time.Sleep(3200 * time.Millisecond)
		_, _ = io.WriteString(w, "event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"model\":\"upstream-name\"}}\n\n")
		fl.Flush()
	}))
	defer up.Close()

	h := newTestHandler(t, newKeepAliveStore(t, up.URL+"/v1", "sse-keep-alive:\n  interval: 1s\n"))
	rec := doRequest(t, h, http.MethodPost, "/v1/responses",
		`{"model":"test-model","input":"hi","stream":true}`, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	body := rec.Body.String()
	if pings := countPings(body); pings < 1 || pings > 3 {
		t.Fatalf("pings during responses-stream silence = %d, want 1-3", pings)
	}
	want := "event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"model\":\"test-model\",\"delta\":\"hi\"}\n\n" +
		"event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"model\":\"test-model\"}}\n\n"
	if got := strings.ReplaceAll(body, string(ssePing), ""); got != want {
		t.Fatalf("responses body with pings stripped =\n%q\nwant\n%q", got, want)
	}
}
