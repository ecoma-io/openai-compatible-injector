package transport

import (
	"bufio"
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// These tests drive the REAL HTTP stack, because the send-state axis is a
// claim about the wire and a hand-built *net.OpError can only pin the
// mapping, never the shapes production actually produces. Each case asserts
// the state that the pool's replay gate reads.

// upstream closes the connection after reading the whole request body,
// without answering. The request was delivered; the failure is a bare EOF
// with no wire op to read, which is exactly the shape a class-based mapping
// would mislabel as definitely-not-sent.
func TestSendStateEOFAfterBodyIsUnknown(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.ReadAll(r.Body)
		hj, ok := w.(http.Hijacker)
		if !ok {
			return
		}
		c, _, err := hj.Hijack()
		if err == nil {
			_ = c.Close()
		}
	}))
	defer srv.Close()

	f := classifyAgainst(t, srv.URL)
	if f.Class != ClassConnection {
		t.Errorf("class = %v, want connection (the premise of this test)", f.Class)
	}
	if f.SendState != SendStateUnknown {
		t.Errorf("send state = %v, want send_unknown: the upstream read the whole body", f.SendState)
	}
}

// upstream reads the body then resets the connection. The error is a
// read-op *net.OpError on an ESTABLISHED connection — the class bucket says
// "connection", but a connection demonstrably carried the request.
func TestSendStateResetAfterBodyIsUnknown(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.ReadAll(r.Body)
		hj, ok := w.(http.Hijacker)
		if !ok {
			return
		}
		c, _, err := hj.Hijack()
		if err == nil {
			if tc, ok := c.(*net.TCPConn); ok {
				_ = tc.SetLinger(0)
			}
			_ = c.Close()
		}
	}))
	defer srv.Close()

	f := classifyAgainst(t, srv.URL)
	if f.SendState != SendStateUnknown {
		t.Errorf("send state = %v, want send_unknown: a reset arrived on an established connection", f.SendState)
	}
}

// upstream answers partially and dies mid-headers. Partial response bytes
// are positive proof the request was delivered.
func TestSendStateTruncatedResponseIsUnknown(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.ReadAll(r.Body)
		hj, ok := w.(http.Hijacker)
		if !ok {
			return
		}
		c, rw, err := hj.Hijack()
		if err != nil {
			return
		}
		// A complete status line and a half-written header, then the socket
		// goes away: the client sees a transport-broken unexpected EOF.
		_, _ = rw.WriteString("HTTP/1.1 200 OK\r\nContent-Len")
		_ = rw.Flush()
		_ = c.Close()
	}))
	defer srv.Close()

	f := classifyAgainst(t, srv.URL)
	if f.SendState != SendStateUnknown {
		t.Errorf("send state = %v, want send_unknown: a partial answer proves delivery", f.SendState)
	}
}

// A blackholed egress never accepts the connection, so the connect times
// out with nothing on the wire. The class is timeout — but the state is
// definitely-not-sent, which is what keeps a dead egress path
// fallback-eligible instead of turning it into a client-visible 502.
func TestSendStateDialTimeoutIsNotSent(t *testing.T) {
	tr := newBaseTransport()
	tr.DialContext = (&net.Dialer{Timeout: 300 * time.Millisecond}).DialContext
	cl := &http.Client{Transport: tr, CheckRedirect: relayRedirects}

	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost,
		"http://10.255.255.1:81/v1/chat/completions", strings.NewReader("{}"))
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	_, derr := cl.Do(req)
	if derr == nil {
		t.Skip("the blackhole address answered; nothing to classify")
	}
	f := ClassifyAttempt(context.Background(), derr)
	if f.Class != ClassTimeout {
		t.Errorf("class = %v, want timeout (the premise of this test)", f.Class)
	}
	if f.SendState != SendStateNotSent {
		t.Errorf("send state = %v, want definitely_not_sent: the dial never completed", f.SendState)
	}
}

// A refused connect never carried a request, whichever class it lands in.
func TestSendStateRefusedIsNotSent(t *testing.T) {
	cl := &http.Client{Transport: &http.Transport{
		DialContext: (&net.Dialer{Timeout: time.Second}).DialContext,
	}, CheckRedirect: relayRedirects}
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost,
		"http://127.0.0.1:1/v1/chat/completions", strings.NewReader("{}"))
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	_, derr := cl.Do(req)
	if derr == nil {
		t.Fatal("expected a refused connection")
	}
	f := ClassifyAttempt(context.Background(), derr)
	if f.SendState != SendStateNotSent {
		t.Errorf("send state = %v, want definitely_not_sent", f.SendState)
	}
}

// An HTTP forward proxy that refuses the CONNECT tunnel fails before any
// request byte: the value is typed, not read from text.
func TestSendStateProxyTunnelFailureIsNotSent(t *testing.T) {
	// A listener that accepts and immediately closes stands in for a proxy
	// that cannot form the tunnel.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer func() { _ = ln.Close() }()
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			_, _ = bufio.NewReader(c).ReadString('\n')
			_ = c.Close()
		}
	}()

	f := ClassifyAttempt(context.Background(), &ProxyConnectError{msg: "socks5 connect refused"})
	if f.SendState != SendStateNotSent {
		t.Errorf("proxy connect failure = %v, want definitely_not_sent", f.SendState)
	}
	f = ClassifyAttempt(context.Background(), &ProxyAuthError{msg: "proxy authentication rejected"})
	if f.SendState != SendStateNotSent {
		t.Errorf("proxy auth failure = %v, want definitely_not_sent", f.SendState)
	}
}

// A TLS certificate that fails verification fails the HANDSHAKE, and the
// handshake completes before any request byte exists. This is the one TLS
// failure this package can PROVE pre-send, because the Go stack reports it
// through a type of its own — and it is therefore the only TLS shape mapped
// to definitely_not_sent. The assertion on the type is the premise of the
// test: if a future Go release stops surfacing it, this fails here rather
// than silently turning a dead certificate into a duplicated request.
func TestSendStateTLSCertificateRejectedIsNotSent(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("a handshake that failed verification reached the handler")
	}))
	defer srv.Close()

	// NewDirectClient trusts the system roots, not the test server's
	// self-signed certificate.
	cl := NewDirectClient()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, srv.URL, strings.NewReader("{}"))
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	_, derr := cl.Do(req)
	if derr == nil {
		t.Fatal("expected a certificate verification failure")
	}
	var cv *tls.CertificateVerificationError
	if !errors.As(derr, &cv) {
		t.Fatalf("error = %T (%v), want a *tls.CertificateVerificationError", derr, derr)
	}
	f := ClassifyAttempt(context.Background(), derr)
	if f.Class != ClassConnection {
		t.Errorf("class = %v, want connection (the premise of this test)", f.Class)
	}
	if f.SendState != SendStateNotSent {
		t.Errorf("send state = %v, want definitely_not_sent: the handshake never completed", f.SendState)
	}
}

// The mirror image, and the reason the mapping is narrow: a TLS connection
// that completed its handshake carries the request, so a failure while the
// answer was awaited is send-unknown even though it arrives as a TLS record
// error or a bare EOF. Widening the handshake clause to "any TLS failure"
// would replay exactly this request.
func TestSendStateTLSEstablishedConnectionDroppedIsUnknown(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.ReadAll(r.Body)
		hj, ok := w.(http.Hijacker)
		if !ok {
			return
		}
		c, _, err := hj.Hijack()
		if err == nil {
			_ = c.Close()
		}
	}))
	defer srv.Close()

	cl := srv.Client()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, srv.URL, strings.NewReader("{}"))
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	_, derr := cl.Do(req)
	if derr == nil {
		t.Fatal("expected the dropped connection to fail the request")
	}
	f := ClassifyAttempt(context.Background(), derr)
	if f.SendState != SendStateUnknown {
		t.Errorf("send state = %v, want send_unknown: the handshake completed, so the request was carried", f.SendState)
	}
}

// A fatal alert during the handshake is ALSO pre-send — nothing was
// requested of the peer beyond the handshake itself — but Go reports it
// through shapes it also produces after the handshake (an op "remote error"
// wrap, or a bare EOF), and none of them is typed as a handshake error. It
// stays send_unknown deliberately: the conservative direction costs one
// ineligible fallback, the other direction duplicates a request.
func TestSendStateTLSHandshakeAlertIsUnknown(t *testing.T) {
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	// The server accepts nothing newer than TLS 1.2; the client demands TLS
	// 1.3, so the server answers the ClientHello with a fatal alert.
	srv.TLS = &tls.Config{MaxVersion: tls.VersionTLS12}
	srv.StartTLS()
	defer srv.Close()

	cl := srv.Client()
	tr := cl.Transport.(*http.Transport).Clone()
	tc := tr.TLSClientConfig.Clone()
	tc.MinVersion = tls.VersionTLS13
	tr.TLSClientConfig = tc
	cl.Transport = tr

	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, srv.URL, strings.NewReader("{}"))
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	_, derr := cl.Do(req)
	if derr == nil {
		t.Fatal("expected the version alert to fail the request")
	}
	// The SHAPE is the reason the mapping is narrow, so it is pinned beside
	// the answer: the alert arrives as a *net.OpError whose op is "remote
	// error" — not one of the connection-establishment ops the mapping trusts
	// ("dial", "proxyconnect") — over types this package cannot name. If Go
	// ever re-shapes it into something typed and dial-like, this premise fails
	// loudly instead of the prose in errors.go quietly becoming false.
	var oe *net.OpError
	if !errors.As(derr, &oe) || oe.Op != "remote error" {
		t.Fatalf("error = %T (%v), want a *net.OpError with op \"remote error\"", derr, derr)
	}
	f := ClassifyAttempt(context.Background(), derr)
	if f.SendState != SendStateUnknown {
		t.Errorf("send state = %v, want send_unknown for %T (%v): the alert's shape is not provably a handshake failure", f.SendState, derr, derr)
	}
}

// A peer that answers a TLS hello with plaintext HTTP fails before the
// request too, but net/http renders it as an untyped string error, so no
// type-based mapping can reach it without matching error text — which this
// package never does. It stays send_unknown, and this test pins that the
// conservative answer survives contact with the real stack.
func TestSendStateTLSPlaintextPeerIsUnknown(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer func() { _ = ln.Close() }()
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer func() { _ = c.Close() }()
				_, _ = bufio.NewReader(c).ReadString('\n')
				_, _ = io.WriteString(c, "HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\n\r\n")
			}(c)
		}
	}()

	f := classifyAgainst(t, "https://"+ln.Addr().String()+"/v1/chat/completions")
	if f.SendState != SendStateUnknown {
		t.Errorf("send state = %v, want send_unknown: a plaintext peer is an untyped failure", f.SendState)
	}
}

func classifyAgainst(t *testing.T, url string) Failure {
	t.Helper()
	cl := NewDirectClient()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, url, strings.NewReader("{}"))
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	_, derr := cl.Do(req)
	if derr == nil {
		t.Fatal("expected a transport failure")
	}
	return ClassifyAttempt(context.Background(), derr)
}
