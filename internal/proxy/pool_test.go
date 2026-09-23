package proxy

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/rs/zerolog"

	"openai-compatible-injector/internal/config"
	"openai-compatible-injector/internal/transport"
)

// newPoolStore builds a store whose kilo model routes through a pool
// transport, next to a legacy direct model — the two handler paths side by
// side.
func newPoolStore(t *testing.T) *config.Store {
	t.Helper()
	snap, err := config.LoadRuntime([]byte(`
api-key: unit-test-key
transports:
  relay:
    type: proxy
    proxy: http://127.0.0.1:20130
  pool-egress:
    type: pool
    members: [relay]
providers:
  kilo:
    base-url: https://api.kilo.example/v1
    transport: pool-egress
models:
  pool-model:
    provider: kilo
    upstream-model: pm
  direct-model:
    endpoint: https://direct.example/v1
    upstream-model: dm
`))
	if err != nil {
		t.Fatalf("LoadRuntime: %v", err)
	}
	return config.NewStore(snap)
}

// singleDoerResolver answers every config with one fixed Doer.
type singleDoerResolver struct{ d transport.Doer }

func (r *singleDoerResolver) Doer(transport.Config) transport.Doer { return r.d }

// kindDoerResolver answers pool configs with one Doer and everything else
// with another — the handler's two seam branches side by side in one store.
type kindDoerResolver struct {
	pool   transport.Doer
	direct transport.Doer
}

func (r *kindDoerResolver) Doer(c transport.Config) transport.Doer {
	if c.Kind == transport.EgressPool {
		return r.pool
	}
	return r.direct
}

// stubExecutor is the pool side of the seam stand-in: it records the
// AttemptRequest it was handed and answers with the canned result. Do
// panics — after this PR the handler must use Execute for a pool Doer, and
// a regression back to Do must be loud.
type stubExecutor struct {
	got      *transport.AttemptRequest
	err      error
	info     transport.AttemptInfo
	doCalled bool
}

func (e *stubExecutor) Execute(ar *transport.AttemptRequest) (*http.Response, transport.AttemptInfo, error) {
	e.got = ar
	// A real pool claims the request's exchange envelope once per dial; the
	// stand-in does the same, so the handler's exchange accounting sees the
	// traffic the stub stands for. A refusal is the pool's own exhausted
	// shape: nothing dialed, the budget flag set and no answer to relay.
	if ar.Budget != nil && !ar.Budget.ConsumeExchange() {
		return nil, transport.AttemptInfo{BudgetExhausted: true}, errors.New("exchange budget exhausted")
	}
	if e.err == nil {
		// A nil error always means a response on this seam; default to a
		// fresh minimal 200 JSON answer when the test pinned none — fresh
		// per call, since the handler consumes each body.
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body:       io.NopCloser(strings.NewReader(`{"id":"x","choices":[]}`)),
		}, e.info, nil
	}
	return nil, e.info, e.err
}

func (e *stubExecutor) Do(*http.Request) (*http.Response, error) {
	e.doCalled = true
	panic("handler called Do on an Executor-capable doer")
}

const poolChatBody = `{"model":"pool-model","messages":[{"role":"user","content":"hi"}]}`

// TestHandlerPoolExecutesThroughExecutor pins the seam branch: a pool
// transport's Doer is driven through Execute, handed the request facts
// eligibility needs (the request context, the outgoing URL, cloned headers,
// the post-injection body, the client-declared stream flag), and its
// response is relayed exactly like a plain Doer's.
func TestHandlerPoolExecutesThroughExecutor(t *testing.T) {
	store := newPoolStore(t)
	ex := &stubExecutor{
		info: transport.AttemptInfo{Attempts: 1, Kind: "proxy", Target: "http://127.0.0.1:20130"},
	}
	h := NewHandler(store, &singleDoerResolver{d: ex}, nil, nil, zerolog.New(zerolog.TestWriter{T: t}).Level(zerolog.Disabled))

	rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions", poolChatBody, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body %s", rec.Code, rec.Body.String())
	}
	if ex.doCalled {
		t.Fatal("handler fell back to Do")
	}
	ar := ex.got
	if ar == nil {
		t.Fatal("Execute was never handed a request")
	}
	if ar.Method != http.MethodPost {
		t.Errorf("AttemptRequest.Method = %q", ar.Method)
	}
	if ar.URL == nil || ar.URL.String() != "https://api.kilo.example/v1/chat/completions" {
		t.Errorf("AttemptRequest.URL = %v", ar.URL)
	}
	if ar.Streaming {
		t.Error("non-stream body probed as streaming")
	}
	if string(ar.Body) != `{"messages":[{"role":"user","content":"hi"}],"model":"pm"}` {
		// The outgoing body: the client's bytes after the transform — model
		// renamed to the upstream name, no injection prompt configured.
		t.Errorf("AttemptRequest.Body = %q, want the outgoing body", ar.Body)
	}
	if ar.Header.Get("Content-Type") == "" {
		t.Error("forwarded headers lost")
	}

	// The streamed variant carries the probed stream flag.
	rec = doRequest(t, h, http.MethodPost, "/v1/chat/completions",
		`{"model":"pool-model","stream":true,"messages":[]}`, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("stream status = %d", rec.Code)
	}
	if !ex.got.Streaming {
		t.Error("stream body probed as non-streaming")
	}
}

// TestHandlerPoolExhaustionIs502UpstreamUnreachable pins exhaustion's
// landing: zero dials is not a crash and not a provider answer — the
// canonical upstream_unreachable 502, with the pool's static sentinel as
// the sanitized log error.
func TestHandlerPoolExhaustionIs502UpstreamUnreachable(t *testing.T) {
	store := newPoolStore(t)
	ex := &stubExecutor{
		err:  errors.New("egress pool: no eligible endpoint available"),
		info: transport.AttemptInfo{Attempts: 0, Exhausted: true},
	}
	h := NewHandler(store, &singleDoerResolver{d: ex}, nil, nil, zerolog.New(zerolog.TestWriter{T: t}).Level(zerolog.Disabled))

	rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions", poolChatBody, nil)
	if rec.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502", rec.Code)
	}
	if got := strings.TrimSpace(rec.Body.String()); got != envelopeUpUnreach {
		t.Errorf("body = %q, want the canonical upstream_unreachable envelope", got)
	}
}

// TestHandlerClientCancelBeatsPoolExhaustion pins the precedence: a
// cancellation that surfaces after the pool stopped on exhaustion (or
// alongside it) is the client's event — outcome client_disconnected, no
// 502 envelope attempted on a connection that is already gone. The request
// context arrives genuinely canceled (doDisconnectedRequest): under the
// ownership rule the context, not the error shape, decides.
func TestHandlerClientCancelBeatsPoolExhaustion(t *testing.T) {
	store := newPoolStore(t)
	ex := &stubExecutor{
		err:  context.Canceled,
		info: transport.AttemptInfo{Attempts: 0, Exhausted: true},
	}
	buf, logger := captureLog(zerolog.InfoLevel)
	h := NewHandler(store, &singleDoerResolver{d: ex}, nil, nil, logger)

	rec := doDisconnectedRequest(t, h, http.MethodPost, "/v1/chat/completions", poolChatBody, nil)
	if rec.Code == http.StatusBadGateway {
		t.Error("cancellation surfaced as a 502 envelope")
	}
	if rec.Body.Len() != 0 {
		t.Errorf("body = %q, want no envelope after a disconnect", rec.Body.String())
	}
	for _, ev := range buf.events(t, "request_completed") {
		if ev["outcome"] != "client_disconnected" {
			t.Errorf("outcome = %v, want client_disconnected", ev["outcome"])
		}
	}
}

// TestHandlerPoolLogFields pins the egress attempt report on the access
// log: attempts, the last endpoint's kind and scheme+host target, and the
// exhaustion flag ride request_completed — and the plain single-endpoint
// path carries none of them.
func TestHandlerPoolLogFields(t *testing.T) {
	store := newPoolStore(t)
	ex := &stubExecutor{
		info: transport.AttemptInfo{Attempts: 2, Kind: "socks5", Target: "socks5://10.0.0.5:30121"},
	}
	buf, logger := captureLog(zerolog.InfoLevel)
	h := NewHandler(store, &kindDoerResolver{pool: ex, direct: &stubDoer{code: http.StatusOK, body: `{"id":"x"}`}}, nil, nil, logger)

	rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions", poolChatBody, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	evs := buf.events(t, "request_completed")
	if len(evs) != 1 {
		t.Fatalf("request_completed events = %d, want 1", len(evs))
	}
	ev := evs[0]
	if ev["egress_attempts"].(float64) != 2 {
		t.Errorf("egress_attempts = %v, want 2", ev["egress_attempts"])
	}
	if ev["egress_kind"] != "socks5" {
		t.Errorf("egress_kind = %v", ev["egress_kind"])
	}
	if ev["egress_target"] != "socks5://10.0.0.5:30121" {
		t.Errorf("egress_target = %v", ev["egress_target"])
	}
	if ev["egress_exhausted"] != false {
		t.Errorf("egress_exhausted = %v", ev["egress_exhausted"])
	}

	// The single-endpoint path reports no egress fields at all.
	buf.buf.Reset()
	rec = doRequest(t, h, http.MethodPost, "/v1/chat/completions",
		`{"model":"direct-model","messages":[{"role":"user","content":"hi"}]}`, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("direct status = %d", rec.Code)
	}
	for _, ev := range buf.events(t, "request_completed") {
		for _, key := range []string{"egress_attempts", "egress_kind", "egress_target", "egress_exhausted"} {
			if _, ok := ev[key]; ok {
				t.Errorf("plain path carried %s on request_completed", key)
			}
		}
	}
}

// TestHandlerPoolExhaustionLoggedWithClass pins the failure event: an
// exhausted pool logs upstream_request_failed at ERROR with the
// egress_exhausted class token and the static sentinel — and never the
// member proxy's credentials (the pool config embeds userinfo that must
// stay out of every line).
func TestHandlerPoolExhaustionLoggedWithClass(t *testing.T) {
	store := newPoolStore(t)
	ex := &stubExecutor{
		err:  errors.New("egress pool: no eligible endpoint available"),
		info: transport.AttemptInfo{Exhausted: true},
	}
	buf, logger := captureLog(zerolog.ErrorLevel)
	h := NewHandler(store, &singleDoerResolver{d: ex}, nil, nil, logger)

	rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions", poolChatBody, nil)
	if rec.Code != http.StatusBadGateway {
		t.Fatalf("status = %d", rec.Code)
	}
	evs := buf.events(t, "upstream_request_failed")
	if len(evs) != 1 {
		t.Fatalf("upstream_request_failed events = %d, want 1", len(evs))
	}
	ev := evs[0]
	if ev["error_class"] != "egress_exhausted" {
		t.Errorf("error_class = %v, want egress_exhausted", ev["error_class"])
	}
	if ev["egress_exhausted"] != true {
		t.Errorf("egress_exhausted = %v, want true", ev["egress_exhausted"])
	}
	if msg, _ := ev["error"].(string); !strings.Contains(msg, "no eligible endpoint") {
		t.Errorf("error = %q, want the pool's static sentinel", msg)
	}
	out := buf.String()
	for _, secret := range []string{"s3cr3t", "unit-test-key"} {
		if strings.Contains(out, secret) {
			t.Errorf("log line echoes %q: %s", secret, out)
		}
	}
}

// TestClassifyAttemptTypedProxyErrors pins the classification ladder's
// typed entries against REAL typed errors, under a live caller context: a
// CONNECT-answering-407 proxy produces the proxy_auth surface, a proxy that
// accepts and dies mid-handshake produces proxy_connect — both through the
// url.Error wrap net/http adds, and both with sanitizer text that carries no
// proxy output and no URL.
func TestClassifyAttemptTypedProxyErrors(t *testing.T) {
	// A proxy answering 407 to CONNECT: the auth-refusal surface.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusProxyAuthRequired)
	})}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { _ = srv.Close() })

	authReq, err := http.NewRequest(http.MethodPost, "https://origin.example/v1/x", strings.NewReader(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	reg := transport.NewRegistry()
	pu, err := url.Parse("http://user:s3cr3t@" + ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	_, authErr := reg.Doer(transport.Config{Kind: transport.Proxy, ProxyURL: pu}).Do(authReq)
	if authErr == nil {
		t.Fatal("407 CONNECT tunneled; want a transport error")
	}
	if got := transport.ClassifyAttempt(context.Background(), authErr); got.Class.String() != "proxy_auth" || got.Cause != "proxy_auth" || got.CallerTerminated {
		t.Errorf("auth classification = %+v, want proxy_auth/proxy_auth, not caller-owned", got)
	}
	if got := transportErrorTextSafe(errors.Unwrap(authErr)); !got {
		t.Errorf("inner auth error deemed unsafe: %v", authErr)
	}
	if sanitized := sanitizeUpstreamError(authErr, &url.URL{Scheme: "https", Host: "origin.example"}); strings.Contains(sanitized.Error(), "s3cr3t") {
		t.Errorf("sanitized auth error leaks proxy credentials: %v", sanitized)
	}

	// A proxy that accepts the TCP connection and dies before the SOCKS
	// handshake completes: the connect-failure surface.
	dead, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		for {
			c, err := dead.Accept()
			if err != nil {
				return
			}
			_ = c.Close()
		}
	}()
	t.Cleanup(func() { _ = dead.Close() })

	du, err := url.Parse("socks5://127.0.0.1:" + strings.Split(dead.Addr().String(), ":")[1])
	if err != nil {
		t.Fatal(err)
	}
	connReq, err := http.NewRequest(http.MethodPost, "http://origin.example/v1/x", strings.NewReader(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	_, connErr := reg.Doer(transport.Config{Kind: transport.Proxy, ProxyURL: du}).Do(connReq)
	if connErr == nil {
		t.Fatal("dead proxy answered; want a transport error")
	}
	if got := transport.ClassifyAttempt(context.Background(), connErr); got.Class.String() != "proxy_connect" || got.Cause != "proxy_connect" || got.CallerTerminated {
		t.Errorf("connect classification = %+v, want proxy_connect/proxy_connect, not caller-owned", got)
	}
}

// TestHandlerEgressAttemptFailureEvents pins the per-attempt evidence on
// the log: one WARN egress_attempt_failed per dialed-and-failed endpoint —
// kind, scheme+host target, typed class, 1-based attempt number, public
// model — emitted even when a later member serves the request, and never
// carrying the error text or any credential material.
func TestHandlerEgressAttemptFailureEvents(t *testing.T) {
	store := newPoolStore(t)

	// Success after two failed dials: the evidence events fire AND the
	// request still completes 200 — the events are evidence, not outcomes.
	ex := &stubExecutor{
		info: transport.AttemptInfo{
			Attempts: 3, Kind: "direct", Target: "direct",
			Failures: []transport.AttemptFailure{
				{Kind: "socks5h", Target: "socks5h://10.0.0.5:1080", Class: "proxy_connect"},
				{Kind: "socks5h", Target: "socks5h://10.0.0.6:1080", Class: "proxy_auth"},
			},
		},
	}
	buf, logger := captureLog(zerolog.InfoLevel)
	h := NewHandler(store, &kindDoerResolver{pool: ex, direct: &stubDoer{code: http.StatusOK, body: `{"id":"x"}`}}, nil, nil, logger)
	rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions", poolChatBody, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 through the serving member", rec.Code)
	}
	evs := buf.events(t, "egress_attempt_failed")
	if len(evs) != 2 {
		t.Fatalf("egress_attempt_failed events = %d, want 2", len(evs))
	}
	for i, ev := range evs {
		if ev["public_model"] != "pool-model" {
			t.Errorf("event %d public_model = %v", i, ev["public_model"])
		}
		if ev["egress_kind"] != "socks5h" {
			t.Errorf("event %d egress_kind = %v, want socks5h", i, ev["egress_kind"])
		}
		if ev["egress_target"] != fmt.Sprintf("socks5h://10.0.0.%d:1080", i+5) {
			t.Errorf("event %d egress_target = %v", i, ev["egress_target"])
		}
	}
	if evs[0]["error_class"] != "proxy_connect" || evs[1]["error_class"] != "proxy_auth" {
		t.Errorf("classes = %v/%v, want proxy_connect/proxy_auth", evs[0]["error_class"], evs[1]["error_class"])
	}
	if evs[0]["attempt"].(float64) != 1 || evs[1]["attempt"].(float64) != 2 {
		t.Errorf("attempts = %v/%v, want 1/2", evs[0]["attempt"], evs[1]["attempt"])
	}

	// The plain single-endpoint path emits no attempt evidence (there are
	// no attempts to report).
	buf.buf.Reset()
	rec = doRequest(t, h, http.MethodPost, "/v1/chat/completions",
		`{"model":"direct-model","messages":[{"role":"user","content":"hi"}]}`, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("direct status = %d", rec.Code)
	}
	if evs := buf.events(t, "egress_attempt_failed"); len(evs) != 0 {
		t.Errorf("plain path emitted %d attempt events", len(evs))
	}
}
