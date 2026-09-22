package transport

import (
	"bufio"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"
)

// recordingProxy is a forward HTTP proxy for tests: http targets arrive in
// absolute-form and are relayed to the origin; https targets arrive as
// CONNECT and get a raw pipe. It counts what traversed it — the assertion
// anchor for "the request went through the proxy".
type recordingProxy struct {
	srv *httptest.Server
	mu  sync.Mutex
	hit int
}

func newRecordingProxy(t *testing.T) *recordingProxy {
	t.Helper()
	rp := &recordingProxy{}
	hop := &http.Client{Timeout: 10 * time.Second}
	rp.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodConnect {
			dst, err := net.Dial("tcp", r.Host)
			if err != nil {
				w.WriteHeader(http.StatusBadGateway)
				return
			}
			hj, ok := w.(http.Hijacker)
			if !ok {
				_ = dst.Close()
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			conn, buf, err := hj.Hijack()
			if err != nil {
				_ = dst.Close()
				return
			}
			_, _ = conn.Write([]byte("HTTP/1.1 200 Connection Established\r\n\r\n"))
			// Either side finishing tears down BOTH conns, so a tunnel
			// whose far end is gone cannot strand the other direction.
			var once sync.Once
			teardown := func() { _ = conn.Close(); _ = dst.Close() }
			go func() { defer once.Do(teardown); _, _ = io.Copy(dst, buf) }()
			go func() { defer once.Do(teardown); _, _ = io.Copy(conn, dst) }()
			return
		}
		rp.mu.Lock()
		rp.hit++
		rp.mu.Unlock()
		out, err := http.NewRequest(r.Method, r.URL.String(), r.Body)
		if err != nil {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		out.Header = r.Header.Clone()
		resp, err := hop.Do(out)
		if err != nil {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		defer func() { _ = resp.Body.Close() }()
		for k, vv := range resp.Header {
			for _, v := range vv {
				w.Header().Add(k, v)
			}
		}
		w.WriteHeader(resp.StatusCode)
		_, _ = io.Copy(w, resp.Body)
	}))
	t.Cleanup(rp.srv.Close)
	return rp
}

func (rp *recordingProxy) hits() int {
	rp.mu.Lock()
	defer rp.mu.Unlock()
	return rp.hit
}

func (rp *recordingProxy) config() Config {
	u, err := url.Parse(rp.srv.URL)
	if err != nil {
		panic(err)
	}
	return Config{Kind: Proxy, ProxyURL: u}
}

// proxyCfg builds a proxy Config for a raw URL string, failing the test on
// a bad fixture.
func proxyCfg(t *testing.T, raw string) Config {
	t.Helper()
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("fixture URL %q: %v", raw, err)
	}
	return Config{Kind: Proxy, ProxyURL: u}
}

// ---- direct ----

// TestDirectClientPreservesTheExchange pins the direct matrix: method,
// path, query, headers, and body reach the server; status, headers, and
// body reach the caller.
func TestDirectClientPreservesTheExchange(t *testing.T) {
	var gotPath, gotQuery, gotMethod, gotBody string
	var gotHdr http.Header
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		gotPath, gotQuery, gotMethod, gotBody, gotHdr = r.URL.Path, r.URL.RawQuery, r.Method, string(b), r.Header.Clone()
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Origin-Marker", "kept")
		w.WriteHeader(http.StatusTeapot)
		_, _ = io.WriteString(w, `{"answer":42}`)
	}))
	defer srv.Close()

	req, err := http.NewRequest(http.MethodPost, srv.URL+"/v1/chat/completions?api-version=2024-02-01", strings.NewReader(`{"model":"m"}`))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "text/event-stream")
	resp, err := NewDirectClient().Do(req)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if gotMethod != http.MethodPost || gotPath != "/v1/chat/completions" || gotQuery != "api-version=2024-02-01" {
		t.Errorf("server saw %s %s?%s", gotMethod, gotPath, gotQuery)
	}
	if gotBody != `{"model":"m"}` {
		t.Errorf("server body = %q", gotBody)
	}
	if gotHdr.Get("Content-Type") != "application/json" || gotHdr.Get("Accept") != "text/event-stream" {
		t.Errorf("server headers = %v", gotHdr)
	}
	if resp.StatusCode != http.StatusTeapot {
		t.Errorf("status = %d, want 418", resp.StatusCode)
	}
	if resp.Header.Get("X-Origin-Marker") != "kept" {
		t.Errorf("response header lost: %v", resp.Header)
	}
	body, _ := io.ReadAll(resp.Body)
	if string(body) != `{"answer":42}` {
		t.Errorf("client body = %q", body)
	}
}

// TestDirectClientTransportTuning pins the tuning carried over from the
// historical shared client — the regression net for "preserve existing
// behavior": no overall timeout (SSE), no response-header timeout, pooled
// idles, eager HTTP/2.
func TestDirectClientTransportTuning(t *testing.T) {
	c := NewDirectClient()
	tr, ok := c.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("transport is %T, want *http.Transport", c.Transport)
	}
	if tr.TLSHandshakeTimeout != 10*time.Second {
		t.Errorf("TLSHandshakeTimeout = %v, want 10s", tr.TLSHandshakeTimeout)
	}
	if tr.ResponseHeaderTimeout != 0 {
		t.Errorf("ResponseHeaderTimeout = %v, want 0 (long-lived SSE streams)", tr.ResponseHeaderTimeout)
	}
	if tr.IdleConnTimeout != 90*time.Second {
		t.Errorf("IdleConnTimeout = %v, want 90s", tr.IdleConnTimeout)
	}
	if tr.MaxIdleConns != 100 || tr.MaxIdleConnsPerHost != 100 {
		t.Errorf("idle pooling = %d/%d, want 100/100", tr.MaxIdleConns, tr.MaxIdleConnsPerHost)
	}
	if !tr.ForceAttemptHTTP2 {
		t.Error("ForceAttemptHTTP2 = false, want true")
	}
	if c.Timeout != 0 {
		t.Errorf("client.Timeout = %v, want 0 (no overall request deadline)", c.Timeout)
	}
	if tr.Proxy == nil {
		t.Error("direct transport lost the ambient env-proxy hook (historical behavior)")
	}
}

// ---- proxy (http scheme) ----

// TestHTTPProxyTraversal pins the load-bearing traversal property: a
// request through a type: proxy transport reaches the origin only by
// traversing the configured proxy, and the answer returns unchanged —
// while the direct transport never touches the proxy. The quiet direction
// is a proxy config silently ignored (direct egress from the wrong IP).
func TestHTTPProxyTraversal(t *testing.T) {
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"model":"public-name"}`)
	}))
	defer origin.Close()
	proxy := newRecordingProxy(t)

	do := func(cfg Config) (*http.Response, error) {
		req, err := http.NewRequest(http.MethodPost, origin.URL+"/v1/chat/completions", strings.NewReader(`{"model":"m"}`))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		return NewRegistry().Doer(cfg).Do(req)
	}

	resp, err := do(proxy.config())
	if err != nil {
		t.Fatalf("Do via proxy: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK || string(body) != `{"model":"public-name"}` {
		t.Errorf("via proxy: status %d body %q", resp.StatusCode, body)
	}
	if proxy.hits() != 1 {
		t.Fatalf("proxy traversed %d times, want 1", proxy.hits())
	}

	resp, err = do(Config{})
	if err != nil {
		t.Fatalf("Do direct: %v", err)
	}
	_ = resp.Body.Close()
	if proxy.hits() != 1 {
		t.Errorf("direct request traversed the proxy (%d hits) — proxy config leaked into the direct path", proxy.hits())
	}
}

// TestHTTPProxyPreservesExchangeDetails pins the full wire matrix through
// the proxy hop: the origin sees the method, path, headers, and body the
// caller sent, and the caller sees the origin's status, headers, and body.
func TestHTTPProxyPreservesExchangeDetails(t *testing.T) {
	var gotPath, gotMethod, gotBody string
	var gotHdr http.Header
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		gotPath, gotMethod, gotBody, gotHdr = r.URL.Path, r.Method, string(b), r.Header.Clone()
		w.Header().Set("Retry-After", "7")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = io.WriteString(w, `{"error":"slow down"}`)
	}))
	defer origin.Close()
	proxy := newRecordingProxy(t)

	req, err := http.NewRequest(http.MethodPost, origin.URL+"/v1/chat/completions", strings.NewReader(`{"model":"m"}`))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Client-Marker", "through")
	resp, err := NewRegistry().Doer(proxy.config()).Do(req)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if gotMethod != http.MethodPost || gotPath != "/v1/chat/completions" {
		t.Errorf("origin saw %s %s", gotMethod, gotPath)
	}
	if gotBody != `{"model":"m"}` {
		t.Errorf("origin body = %q", gotBody)
	}
	if gotHdr.Get("Content-Type") != "application/json" || gotHdr.Get("X-Client-Marker") != "through" {
		t.Errorf("origin headers = %v", gotHdr)
	}
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Errorf("status = %d, want 429", resp.StatusCode)
	}
	if resp.Header.Get("Retry-After") != "7" {
		t.Errorf("Retry-After lost: %v", resp.Header)
	}
	body, _ := io.ReadAll(resp.Body)
	if string(body) != `{"error":"slow down"}` {
		t.Errorf("client body = %q", body)
	}
}

// TestHTTPProxyServesConnectTunnelsForHTTPS covers the https-target path:
// the proxy sees CONNECT (not the request), tunnels blindly, and the
// TLS answer returns end-to-end — the production shape for https
// providers behind an http egress proxy.
func TestHTTPProxyServesConnectTunnelsForHTTPS(t *testing.T) {
	origin := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"tunneled":true}`)
	}))
	defer origin.Close()
	proxy := newRecordingProxy(t)

	doer := NewRegistry().Doer(proxy.config()).(*http.Client)
	// The origin's certificate is self-signed; the test only exercises the
	// tunnel, so verification is relaxed before the client is ever used.
	// (The cloned base transport carries no TLSClientConfig of its own.)
	doer.Transport.(*http.Transport).TLSClientConfig = &tls.Config{InsecureSkipVerify: true}

	resp, err := doer.Get(origin.URL + "/v1/models")
	if err != nil {
		t.Fatalf("Get through CONNECT tunnel: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK || string(body) != `{"tunneled":true}` {
		t.Errorf("status %d body %q", resp.StatusCode, body)
	}
	if proxy.hits() != 0 {
		t.Errorf("https target must ride CONNECT, not absolute-form (%d forwarded hits)", proxy.hits())
	}
}

// ---- semantics: HTTP errors are responses, network failures are errors ----

// TestProviderHTTPStatusesAreResponsesNotErrors is the architecture's
// load-bearing seam: a provider answering 429/500/503 through the proxy is
// an answer (response, nil error) for the caller to interpret; PR1's error
// normalization depends on this never becoming a transport error.
func TestProviderHTTPStatusesAreResponsesNotErrors(t *testing.T) {
	for _, status := range []int{429, 500, 502, 503, 404, 401} {
		origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(status)
			_, _ = io.WriteString(w, fmt.Sprintf(`{"status":%d}`, status))
		}))
		proxy := newRecordingProxy(t)

		req, err := http.NewRequest(http.MethodPost, origin.URL+"/v1/chat/completions", strings.NewReader(`{}`))
		if err != nil {
			t.Fatal(err)
		}
		resp, err := NewRegistry().Doer(proxy.config()).Do(req)
		if err != nil {
			t.Errorf("status %d: Do returned err = %v, want nil (upstream answer, not transport failure)", status, err)
			origin.Close()
			continue
		}
		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode != status {
			t.Errorf("status %d: caller saw %d", status, resp.StatusCode)
		}
		if !strings.Contains(string(body), fmt.Sprintf(`"status":%d`, status)) {
			t.Errorf("status %d: body = %q", status, body)
		}
		origin.Close()
	}
}

// TestProxyConnectionFailureIsTransportError pins the other half of the
// seam: a proxy endpoint that refuses connections is a transport error —
// never a silently-direct request and never a synthetic response.
func TestProxyConnectionFailureIsTransportError(t *testing.T) {
	// A reserved-then-closed port: connections are refused, not hung.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()

	for _, scheme := range []string{"http", "socks5", "socks5h"} {
		req, rerr := http.NewRequest(http.MethodPost, "http://origin.example/v1/chat/completions", strings.NewReader(`{}`))
		if rerr != nil {
			t.Fatal(rerr)
		}
		_, err := NewRegistry().Doer(proxyCfg(t, scheme+"://"+addr)).Do(req)
		if err == nil {
			t.Errorf("%s proxy: Do returned nil error on refused proxy connection", scheme)
		}
	}
}

// ---- streaming ----

// TestProxyTransportStreamsBodiesLive pins the no-buffering contract where
// it would bite: the caller receives the first SSE event while the origin
// is still holding the second — a buffered transport cannot pass this.
func TestProxyTransportStreamsBodiesLive(t *testing.T) {
	firstFlushed := make(chan struct{})
	release := make(chan struct{})
	var unblock sync.Once
	letFinish := func() { unblock.Do(func() { close(release) }) }
	// A deadlock here would hang the suite: if the client cannot read the
	// first event, the watchdog lets the origin finish so the test fails on
	// the assertion instead of timing out.
	time.AfterFunc(5*time.Second, letFinish)

	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		f := w.(http.Flusher)
		_, _ = io.WriteString(w, "data: first\n\n")
		f.Flush()
		close(firstFlushed)
		<-release
		_, _ = io.WriteString(w, "data: [DONE]\n\n")
		f.Flush()
	}))
	defer origin.Close()
	proxy := newRecordingProxy(t)

	req, err := http.NewRequest(http.MethodPost, origin.URL+"/v1/chat/completions", strings.NewReader(`{"model":"m"}`))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Accept", "text/event-stream")
	resp, err := NewRegistry().Doer(proxy.config()).Do(req)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	br := bufio.NewReader(resp.Body)
	select {
	case <-firstFlushed:
	case <-time.After(5 * time.Second):
		t.Fatal("origin never flushed the first event")
	}
	line, err := br.ReadString('\n')
	if err != nil {
		letFinish()
		t.Fatalf("reading the first event: %v (stream was buffered?)", err)
	}
	if line != "data: first\n" {
		letFinish()
		t.Fatalf("first line = %q, want %q", line, "data: first\n")
	}
	letFinish()
	for {
		line, err = br.ReadString('\n')
		if err != nil {
			t.Fatalf("reading the rest: %v", err)
		}
		if line == "data: [DONE]\n" {
			break
		}
	}
}

// ---- request immutability ----

// TestTransportLeavesRequestIdentityAlone pins the caller-owns-the-request
// contract: Do must not rewrite the method, URL, or headers of the request
// object it was handed — the handler may yet need that request as it built
// it (and PR1's normalization reads it after Do returns). A fresh
// identical request must also succeed on the same doer: no hidden
// one-shot state.
func TestTransportLeavesRequestIdentityAlone(t *testing.T) {
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "{}")
	}))
	defer origin.Close()
	proxy := newRecordingProxy(t)

	do := func(cfg Config) {
		t.Helper()
		body := `{"model":"m"}`
		req, err := http.NewRequest(http.MethodPost, origin.URL+"/v1/chat/completions", strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		before := struct {
			method string
			url    string
			header http.Header
		}{req.Method, req.URL.String(), req.Header.Clone()}

		resp, err := NewRegistry().Doer(cfg).Do(req)
		if err != nil {
			t.Fatalf("Do: %v", err)
		}
		_ = resp.Body.Close()

		if req.Method != before.method || req.URL.String() != before.url {
			t.Errorf("request identity rewritten: %s %s -> %s %s", before.method, before.url, req.Method, req.URL)
		}
		if len(req.Header) != len(before.header) {
			t.Errorf("request header set changed: %v -> %v", before.header, req.Header)
		}
		for k, vv := range before.header {
			got := req.Header[k]
			if len(got) != len(vv) {
				t.Errorf("header %q changed: %v -> %v", k, vv, got)
			}
		}
	}
	do(Config{})
	do(proxy.config())
}
