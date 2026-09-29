package proxy

// Regression tests for the exchange envelope's reach into the BUFFERED 2xx
// path, and for the ownership of the timeout it produces.
//
// The defect these pin is the one the reviewers found in the two-phase
// handoff: the phase decision was made from the REQUEST's `stream` flag before
// any upstream header existed, so a `stream: true` request answered with
// `200 application/json` had its exchange window disarmed at headers while the
// body was read as buffered JSON — inside the walk, before commitment, where
// the candidate's absolute `max-elapsed` still owns it. The body was drawn
// from the envelope's budget but was no longer bounded by it, the exact hole
// issue #96 closed.
//
// The quiet direction is the whole point: with the old flag, these stalls
// passed every existing test (the header-only tests never read the body, and
// the buffered tests never claimed a window). Only a test that claims a real
// envelope, streams the request, and answers with buffered JSON can see the
// window being dropped.
//
// These tests deliberately do NOT call stubRetryTiming. A frozen policy clock
// freezes the envelope's own clock — recovery.WithClock(retryClock.Now) — so a
// frozen walk could never spend a 60 ms window, and the reproduction would
// hang on the frozen instant instead of on the peer. Time has to be real here;
// the wait seam stays stubbed so no test sleeps a backoff.

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"openai-compatible-injector/internal/config"
	"openai-compatible-injector/internal/transport"
)

// realRetryTiming stubs only the walk's sleep, leaving the wall clock in place.
// The envelope under test is measured on that clock, so it must be real.
func realRetryTiming(t *testing.T) {
	t.Helper()
	origWait := retryWait
	t.Cleanup(func() { retryWait = origWait })
	retryWait = func(ctx context.Context, _ time.Duration) bool { return ctx.Err() == nil }
}

// streamChatBody is a chat payload that declares streaming. The point of
// these tests is that the REQUEST's stream flag is not the RESPONSE's shape.
const streamChatBody = `{"model":"chain-model","stream":true,"messages":[{"role":"user","content":"hi"}]}`

// tightEnvelope is the recovery block every test here loads. The retry window
// has to be stated too: config rejects a candidate envelope narrower than the
// retry window it would have to fund (retries.max-elapsed <= candidate
// max-elapsed), and the shipped default retry window is ten seconds.
const tightEnvelope = `
recovery:
  retries:
    max-elapsed: 50ms
  budget:
    request:
      max-elapsed: 60ms
    candidate:
      max-elapsed: 60ms
`

// envelopeStalledUpstream answers every exchange with a stalled body on a real
// connection: headers flushed immediately under the given status and content
// type, then a body that never delivers another byte. The exchange envelope's
// window is the only thing that can end the read.
type envelopeStalledUpstream struct {
	srv *httptest.Server
}

func newEnvelopeStalledUpstream(t *testing.T, status int, contentType string) *envelopeStalledUpstream {
	t.Helper()
	var done chan struct{}
	s := &envelopeStalledUpstream{}
	s.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", contentType)
		w.WriteHeader(status)
		w.(http.Flusher).Flush()
		// The connection stays live; the body never produces a byte. Hold it
		// until cleanup so no goroutine outlives the test.
		<-done
	}))
	done = make(chan struct{})
	t.Cleanup(func() { close(done); s.srv.Close() })
	return s
}

// Do is the direct Doer a test hands the resolver: the real direct client, so
// the response body is a real net/http body carrying the derived context.
func (s *envelopeStalledUpstream) Do(req *http.Request) (*http.Response, error) {
	return transport.NewDirectClient().Do(req)
}

// streamTrueBufferedChain builds a store whose model walks exactly one
// candidate at the given base URL, with the given recovery block. The model
// has no injection prompt, so the body reached the read unchanged.
//
// The base URL is the stalled server's own: the injected Doer is a real
// direct client that honours the request's URL, so a placeholder host would
// send the walk to DNS instead of the stall these tests are about.
func streamTrueBufferedChain(t *testing.T, baseURL, recoveryBlock string) *config.Store {
	t.Helper()
	snap, err := config.LoadRuntime([]byte("api-key: " + testAPIKey + "\n" + recoveryBlock + `
transports:
  t1:
    type: direct
providers:
  pa:
    base-url: ` + baseURL + `
    transport: t1
models:
  chain-model:
    injection-prompt: ""
    providers:
      - provider: pa
        upstream-model: up-a
`))
	if err != nil {
		t.Fatalf("LoadRuntime: %v", err)
	}
	return config.NewStore(snap)
}

// serveStalledChat runs one streaming chat request against a stalled upstream
// and returns the recorder once the walk has returned, failing the test if it
// never does.
func serveStalledChat(t *testing.T, h http.Handler) *httptest.ResponseRecorder {
	t.Helper()
	req := buildRequest(t, http.MethodPost, "/v1/chat/completions", streamChatBody, map[string]string{"stream": "true"})
	rec := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		h.ServeHTTP(rec, req)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("the walk never returned: the buffered answer was not bounded by the candidate envelope")
	}
	return rec
}

// TestBufferedAnswerToStreamRequestHonoursTheEnvelope pins the reviewer's
// exact reproduction: a `stream: true` request answered with buffered JSON.
// The answer is not committed, so the candidate's absolute `max-elapsed` must
// still bound the body read — the request's own stream flag has no say in what
// the upstream sent.
//
// The envelope is tiny (candidate max-elapsed 60 ms) and the peer stalls its
// JSON body far past it. On the defective implementation the window was
// disarmed at headers, so the walk would sit in the buffered read until the
// client's own (background, unbounded) context — the request would hang
// forever. On the fixed one the read surfaces the transport's typed exchange
// timeout, the walk finalizes promptly inside the envelope, and the outcome
// names the envelope, never the peer.
func TestBufferedAnswerToStreamRequestHonoursTheEnvelope(t *testing.T) {
	realRetryTiming(t)
	up := newEnvelopeStalledUpstream(t, http.StatusOK, "application/json")
	store := streamTrueBufferedChain(t, up.srv.URL+"/v1", tightEnvelope)
	h := NewHandler(store, fixedDoer{up}, nil, nil, nil, zerolog.Nop())

	rec := serveStalledChat(t, h)

	if rec.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502 (the buffered answer read failed inside its envelope)", rec.Code)
	}
	if body := rec.Body.String(); body != envelopeUpInvalid {
		t.Errorf("body:\n got %s\nwant %s", body, envelopeUpInvalid)
	}
}

// TestBufferedAnswerExchangeTimeoutOwnsTheCause pins the ownership of the
// timeout the walk just surfaced: a body cut by THIS proxy's exchange envelope
// must be observed as the protocol body timeout with failure_origin envelope —
// never as a read failure of the peer's, which is what the pre-fix handler did
// (it folded the transport's typed error into ProtocolBodyReadFailed).
func TestBufferedAnswerExchangeTimeoutOwnsTheCause(t *testing.T) {
	realRetryTiming(t)
	logBuf, log := captureLog(zerolog.InfoLevel)
	up := newEnvelopeStalledUpstream(t, http.StatusOK, "application/json")
	store := streamTrueBufferedChain(t, up.srv.URL+"/v1", tightEnvelope)
	h := NewHandler(store, fixedDoer{up}, nil, nil, nil, log)

	serveStalledChat(t, h)

	// The evidence event must say the envelope did it, not the peer.
	failed := logBuf.events(t, "upstream_body_read_failed")
	if len(failed) != 1 {
		t.Fatalf("upstream_body_read_failed events = %d, want 1\n%s", len(failed), logBuf.String())
	}
	ev := failed[0]
	if ev["error_class"] != "upstream_error" {
		t.Errorf("error_class = %v, want upstream_error", ev["error_class"])
	}
	if ev["error_cause"] != "exchange_elapsed" {
		t.Errorf("error_cause = %v, want exchange_elapsed", ev["error_cause"])
	}
	if ev["failure_origin"] != "envelope" {
		t.Errorf("failure_origin = %v, want envelope", ev["failure_origin"])
	}
	// The error field must stay the proxy's own sanitized constant: the typed
	// exchange timeout's own text is allowed, but nothing from the peer is.
	if ev["error"] != "upstream transport error" {
		t.Errorf("error = %v, want the sanitized %q", ev["error"], "upstream transport error")
	}
	// And the recovery reason token the matrix read must agree.
	if ev["reason"] != "upstream_body_timeout" {
		t.Errorf("reason = %v, want upstream_body_timeout", ev["reason"])
	}
}

// TestErrorBodyStalledPastTheCandidateEnvelope pins the second reviewer
// reproduction: a 500 whose error-body capture stalls past the candidate
// envelope. The error body did not finish inside its bound, so the
// observation is the protocol body timeout with failure_origin envelope and
// error_class upstream_error_body_timeout — never a read failure of the peer's.
// The retained answer is the same timeout flavor, so a finalize reports
// upstream_invalid_response with reason upstream_body_timeout.
func TestErrorBodyStalledPastTheCandidateEnvelope(t *testing.T) {
	realRetryTiming(t)
	// The capture's own internal deadline is far longer than the envelope, so
	// only the envelope can cut the read — otherwise this test would trip the
	// capture deadline instead and prove nothing about the envelope.
	old := upstreamErrorCaptureTimeout
	upstreamErrorCaptureTimeout = 30 * time.Second
	defer func() { upstreamErrorCaptureTimeout = old }()

	logBuf, log := captureLog(zerolog.InfoLevel)
	up := newEnvelopeStalledUpstream(t, http.StatusInternalServerError, "application/json")
	store := streamTrueBufferedChain(t, up.srv.URL+"/v1", tightEnvelope)
	h := NewHandler(store, fixedDoer{up}, nil, nil, nil, log)

	rec := serveStalledChat(t, h)
	if body := rec.Body.String(); body != envelopeUpInvalid {
		t.Errorf("body:\n got %s\nwant %s", body, envelopeUpInvalid)
	}

	// The evidence event must name the envelope, not the peer.
	failed := logBuf.events(t, "upstream_body_read_failed")
	if len(failed) != 1 {
		t.Fatalf("upstream_body_read_failed events = %d, want 1\n%s", len(failed), logBuf.String())
	}
	ev := failed[0]
	if ev["error_class"] != "upstream_error_body_timeout" {
		t.Errorf("error_class = %v, want upstream_error_body_timeout", ev["error_class"])
	}
	if ev["error_cause"] != "exchange_elapsed" {
		t.Errorf("error_cause = %v, want exchange_elapsed", ev["error_cause"])
	}
	if ev["failure_origin"] != "envelope" {
		t.Errorf("failure_origin = %v, want envelope", ev["failure_origin"])
	}
	if ev["reason"] != "upstream_body_timeout" {
		t.Errorf("reason = %v, want upstream_body_timeout", ev["reason"])
	}
	// And the completion record keeps the invalid-response cause rather than a
	// read failure.
	completed := findLogEvent(t, logBuf.String(), "request_completed")
	if completed["outcome"] != "upstream_invalid_response" {
		t.Errorf("outcome = %v, want upstream_invalid_response", completed["outcome"])
	}
}
