package transport

import (
	"bufio"
	"context"
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
