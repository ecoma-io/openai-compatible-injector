package transport

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// socks5Stub is a minimal SOCKS5 server (RFC 1928 greeting/negotiation,
// RFC 1929 auth, CONNECT) for tests. It records the address TYPE and value
// of the CONNECT target — the assertion anchor for the socks5-vs-socks5h
// DNS contract — and relays the tunnel to whatever target it resolves.
type socks5Stub struct {
	ln net.Listener

	// auth, when set, demands RFC 1929 username/password and rejects
	// everything else with NO ACCEPTABLE METHODS.
	auth     bool
	user     string
	pass     string
	forceRep byte // non-zero: reply code to send instead of connecting
	stall    bool // read the greeting, then never answer

	mu     sync.Mutex
	conns  []net.Conn
	atyp   byte // address type of the last CONNECT (1 IP4, 3 domain, 4 IP6)
	target string
	port   int
}

func newSocks5Stub(t *testing.T, cfg func(*socks5Stub)) *socks5Stub {
	t.Helper()
	s := &socks5Stub{}
	if cfg != nil {
		cfg(s)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s.ln = ln
	t.Cleanup(func() {
		_ = ln.Close()
		s.mu.Lock()
		for _, c := range s.conns {
			_ = c.Close()
		}
		s.mu.Unlock()
	})
	go s.accept()
	return s
}

// dualStackOrigin is an httptest server bound on the wildcard address so
// that both 127.0.0.1 and ::1 reach it — the local resolver's answer for
// "localhost" is machine-dependent, and the socks5 local-resolve test must
// work with either. It returns the PORT the client should target by
// hostname.
func dualStackOrigin(t *testing.T, h http.HandlerFunc) int {
	t.Helper()
	srv := &http.Server{Handler: h}
	ln, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = srv.Close() })
	go func() { _ = srv.Serve(ln) }()
	_, port, err := net.SplitHostPort(ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	n, err := strconv.Atoi(port)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func (s *socks5Stub) accept() {
	for {
		conn, err := s.ln.Accept()
		if err != nil {
			return
		}
		s.mu.Lock()
		s.conns = append(s.conns, conn)
		s.mu.Unlock()
		go s.handle(conn)
	}
}

func (s *socks5Stub) handle(conn net.Conn) {
	if err := s.handleConn(conn); err != nil {
		_ = conn.Close()
	}
}

func (s *socks5Stub) handleConn(conn net.Conn) error {
	head := make([]byte, 2)
	if _, err := io.ReadFull(conn, head); err != nil {
		return err
	}
	methods := make([]byte, int(head[1]))
	if _, err := io.ReadFull(conn, methods); err != nil {
		return err
	}
	if s.stall {
		select {} // never answer: the client must abort on its own
	}
	var chosen byte = 0xff
	for _, m := range methods {
		if (s.auth && m == 0x02) || (!s.auth && m == 0x00) {
			chosen = m
		}
	}
	if err := writeAll(conn, []byte{0x05, chosen}); err != nil {
		return err
	}
	if chosen == 0xff {
		return nil
	}
	if chosen == 0x02 {
		vl := make([]byte, 2)
		if _, err := io.ReadFull(conn, vl); err != nil {
			return err
		}
		uname := make([]byte, int(vl[1]))
		if _, err := io.ReadFull(conn, uname); err != nil {
			return err
		}
		pl := make([]byte, 1)
		if _, err := io.ReadFull(conn, pl); err != nil {
			return err
		}
		passwd := make([]byte, int(pl[0]))
		if _, err := io.ReadFull(conn, passwd); err != nil {
			return err
		}
		status := byte(0x00)
		if string(uname) != s.user || string(passwd) != s.pass {
			status = 0x01
		}
		if err := writeAll(conn, []byte{0x01, status}); err != nil {
			return err
		}
		if status != 0x00 {
			return nil
		}
	}
	hdr := make([]byte, 4)
	if _, err := io.ReadFull(conn, hdr); err != nil {
		return err
	}
	if hdr[1] != 0x01 { // only CONNECT is spoken here
		return nil
	}
	var target string
	switch hdr[3] {
	case 0x01:
		b := make([]byte, 4)
		if _, err := io.ReadFull(conn, b); err != nil {
			return err
		}
		target = net.IP(b).String()
	case 0x03:
		l := make([]byte, 1)
		if _, err := io.ReadFull(conn, l); err != nil {
			return err
		}
		d := make([]byte, int(l[0]))
		if _, err := io.ReadFull(conn, d); err != nil {
			return err
		}
		target = string(d)
	case 0x04:
		b := make([]byte, 16)
		if _, err := io.ReadFull(conn, b); err != nil {
			return err
		}
		target = net.IP(b).String()
	default:
		return nil
	}
	pb := make([]byte, 2)
	if _, err := io.ReadFull(conn, pb); err != nil {
		return err
	}
	port := int(pb[0])<<8 | int(pb[1])

	s.mu.Lock()
	s.atyp, s.target, s.port = hdr[3], target, port
	s.mu.Unlock()

	if s.forceRep != 0 {
		return writeAll(conn, append([]byte{0x05, s.forceRep, 0x00, 0x01}, make([]byte, 6)...))
	}
	dst, err := net.Dial("tcp", net.JoinHostPort(target, strconv.Itoa(port)))
	if err != nil {
		return writeAll(conn, append([]byte{0x05, 0x04, 0x00, 0x01}, make([]byte, 6)...))
	}
	if err := writeAll(conn, append([]byte{0x05, 0x00, 0x00, 0x01}, make([]byte, 6)...)); err != nil {
		_ = dst.Close()
		return err
	}
	// Either side finishing tears down BOTH conns: closing only the peer
	// that finished leaves the other copy goroutine — and the raw readers
	// behind it — blocked on a tunnel whose far end is gone forever.
	var once sync.Once
	teardown := func() { _ = conn.Close(); _ = dst.Close() }
	go func() { defer once.Do(teardown); _, _ = io.Copy(dst, conn) }()
	go func() { defer once.Do(teardown); _, _ = io.Copy(conn, dst) }()
	return nil
}

// lastConnect returns the recorded address type and target of the last
// CONNECT the stub served.
func (s *socks5Stub) lastConnect() (byte, string, int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.atyp, s.target, s.port
}

// config builds the proxy Config pointing at this stub, with the scheme
// and optional userinfo under test.
func (s *socks5Stub) config(t *testing.T, scheme, userinfo string) Config {
	t.Helper()
	raw := scheme + "://"
	if userinfo != "" {
		raw += userinfo + "@"
	}
	raw += s.ln.Addr().String()
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	return Config{Kind: Proxy, ProxyURL: u}
}

// roundTrip does one POST through the given config against originPort by
// hostname, returning status and body.
func roundTrip(t *testing.T, cfg Config, originPort int) (int, string, error) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost,
		"http://localhost:"+strconv.Itoa(originPort)+"/v1/chat/completions",
		strings.NewReader(`{"model":"m"}`))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := NewRegistry().Doer(cfg).Do(req)
	if err != nil {
		return 0, "", err
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return resp.StatusCode, "", err
	}
	return resp.StatusCode, string(body), nil
}

// ---- the DNS contract: socks5 vs socks5h ----

// TestSocks5LocalResolveSendsIPLiteral pins the socks5:// half: the
// UPSTREAM hostname is resolved locally and the proxy receives an IP
// literal (ATYP 1 or 4) — never the hostname. The quiet direction is a
// socks5 that silently behaves like socks5h and leaks upstream hostnames
// into the local resolver's place of hiding.
func TestSocks5LocalResolveSendsIPLiteral(t *testing.T) {
	port := dualStackOrigin(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, `{"via":"socks5"}`)
	})
	stub := newSocks5Stub(t, nil)

	status, body, err := roundTrip(t, stub.config(t, "socks5", ""), port)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	if status != http.StatusOK || body != `{"via":"socks5"}` {
		t.Errorf("status %d body %q", status, body)
	}
	atyp, target, gotPort := stub.lastConnect()
	if atyp == 0x03 {
		t.Fatalf("socks5 sent the hostname (%q) to the proxy — that is socks5h's contract", target)
	}
	if net.ParseIP(target) == nil {
		t.Fatalf("socks5 sent non-IP target %q (atyp %#x)", target, atyp)
	}
	if gotPort != port {
		t.Errorf("CONNECT port = %d, want %d", gotPort, port)
	}
}

// TestSocks5HRemoteResolveSendsDomainForm pins the socks5h:// half: the
// hostname itself travels in domain form (ATYP 3) and the proxy resolves
// it — remote DNS.
func TestSocks5HRemoteResolveSendsDomainForm(t *testing.T) {
	port := dualStackOrigin(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, `{"via":"socks5h"}`)
	})
	stub := newSocks5Stub(t, nil)

	status, body, err := roundTrip(t, stub.config(t, "socks5h", ""), port)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	if status != http.StatusOK || body != `{"via":"socks5h"}` {
		t.Errorf("status %d body %q", status, body)
	}
	atyp, target, gotPort := stub.lastConnect()
	if atyp != 0x03 {
		t.Fatalf("socks5h sent an IP literal (atyp %#x, %q) — that is socks5's contract", atyp, target)
	}
	if target != "localhost" {
		t.Errorf("CONNECT domain = %q, want localhost", target)
	}
	if gotPort != port {
		t.Errorf("CONNECT port = %d, want %d", gotPort, port)
	}
}

// ---- auth ----

// TestSocks5UsernamePasswordAuth exercises RFC 1929 end to end: correct
// credentials tunnel (percent-encoded userinfo decodes — the space is %20,
// never a bare +), and wrong ones fail as transport errors that never echo
// the credential material.
func TestSocks5UsernamePasswordAuth(t *testing.T) {
	port := dualStackOrigin(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, `{"ok":true}`)
	})
	stub := newSocks5Stub(t, func(s *socks5Stub) {
		s.auth, s.user, s.pass = true, "Aladdin", "open sesame"
	})

	status, body, err := roundTrip(t, stub.config(t, "socks5h", "Aladdin:open%20sesame"), port)
	if err != nil {
		t.Fatalf("Do with correct credentials: %v", err)
	}
	if status != http.StatusOK || body != `{"ok":true}` {
		t.Errorf("status %d body %q", status, body)
	}

	_, _, err = roundTrip(t, stub.config(t, "socks5h", "Aladdin:wrong-pass"), port)
	if err == nil {
		t.Fatal("wrong password tunneled; want transport error")
	}
	if strings.Contains(err.Error(), "wrong-pass") || strings.Contains(err.Error(), "Aladdin") {
		t.Errorf("auth error echoes credentials: %v", err)
	}
}

// TestSocks5AuthDemandedButNotConfigured pins the NO ACCEPTABLE METHODS
// path: an auth-demanding proxy and a credential-less config fail loudly.
func TestSocks5AuthDemandedButNotConfigured(t *testing.T) {
	port := dualStackOrigin(t, func(w http.ResponseWriter, r *http.Request) {})
	stub := newSocks5Stub(t, func(s *socks5Stub) {
		s.auth, s.user, s.pass = true, "u", "p"
	})

	_, _, err := roundTrip(t, stub.config(t, "socks5", ""), port)
	if err == nil {
		t.Fatal("no-credentials config against auth-demanding proxy succeeded")
	}
	if !strings.Contains(err.Error(), "no acceptable authentication method") {
		t.Errorf("err = %v, want the no-acceptable-method rejection", err)
	}
}

// ---- failure surfaces ----

// TestSocks5ReplyCodeSurfacesAsTransportError maps proxy reply codes to
// their RFC 1928 §6 meaning instead of a generic blob.
func TestSocks5ReplyCodeSurfacesAsTransportError(t *testing.T) {
	cases := map[byte]string{
		0x01: "general failure",
		0x03: "network unreachable",
		0x05: "connection refused",
		0x07: "command not supported",
	}
	for rep, want := range cases {
		port := dualStackOrigin(t, func(w http.ResponseWriter, r *http.Request) {})
		stub := newSocks5Stub(t, func(s *socks5Stub) { s.forceRep = rep })

		_, _, err := roundTrip(t, stub.config(t, "socks5h", ""), port)
		if err == nil {
			t.Errorf("reply %#x: Do returned nil error", rep)
			continue
		}
		if !strings.Contains(err.Error(), want) {
			t.Errorf("reply %#x: err = %v, want it to mention %q", rep, err, want)
		}
	}
}

// TestSocks5HandshakeCancellationAborts pins that cancelling the request
// context mid-handshake tears the dial down promptly (the request's
// context is the only cancellation lever — there is no overall timeout).
func TestSocks5HandshakeCancellationAborts(t *testing.T) {
	stub := newSocks5Stub(t, func(s *socks5Stub) { s.stall = true })

	ctx, cancel := context.WithCancel(context.Background())
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		"http://localhost:1/v1/chat/completions", strings.NewReader(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		time.Sleep(150 * time.Millisecond)
		cancel()
	}()
	start := time.Now()
	_, err = NewRegistry().Doer(stub.config(t, "socks5", "")).Do(req)
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("Do succeeded against a stalled proxy")
	}
	if elapsed > 5*time.Second {
		t.Fatalf("cancellation took %v to abort the handshake", elapsed)
	}
	if !errors.Is(err, context.Canceled) {
		t.Logf("note: abort surfaced as %v (context cancellation propagates as a transport error)", err)
	}
}

// TestSocks5HandshakeDeadlineBoundsStall pins the whole-handshake deadline
// directly: a proxy that accepts and then stalls must fail within the
// dialer's bound, not strand the dial goroutine.
func TestSocks5HandshakeDeadlineBoundsStall(t *testing.T) {
	stub := newSocks5Stub(t, func(s *socks5Stub) { s.stall = true })
	u, err := url.Parse("socks5h://" + stub.ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	d := newSocks5Dialer(u)
	d.timeout = 150 * time.Millisecond
	d.dialer.Timeout = 150 * time.Millisecond

	start := time.Now()
	_, err = d.DialContext(context.Background(), "tcp", "origin.example:443")
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("DialContext succeeded against a stalled proxy")
	}
	if elapsed > 5*time.Second {
		t.Fatalf("deadline took %v to fire (bound was 150ms)", elapsed)
	}
}

// TestSocks5DeadlineClearsAfterHandshake pins the quiet half of the
// deadline logic: once the tunnel is up, the connection carries no
// leftover deadline — a slow SSE stream hours later must not be killed by
// the handshake bound. The conn is used raw here: sleep past the bound,
// then speak HTTP over it. A lingering deadline would fail the write.
func TestSocks5DeadlineClearsAfterHandshake(t *testing.T) {
	// A raw origin, not an http.Server: the answer must close the
	// connection so the raw read below sees EOF.
	ln, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	go func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		defer func() { _ = c.Close() }()
		_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))
		_, _ = io.CopyN(io.Discard, c, 1) // wait for the request's first byte
		_, _ = c.Write([]byte("HTTP/1.0 200 OK\r\nContent-Length: 2\r\n\r\nhi"))
	}()
	_, portStr, err := net.SplitHostPort(ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	stub := newSocks5Stub(t, nil)
	u, err := url.Parse("socks5://" + stub.ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	d := newSocks5Dialer(u)
	d.timeout = 200 * time.Millisecond
	d.dialer.Timeout = 200 * time.Millisecond

	conn, err := d.DialContext(context.Background(), "tcp", "localhost:"+portStr)
	if err != nil {
		t.Fatalf("DialContext: %v", err)
	}
	defer func() { _ = conn.Close() }()
	time.Sleep(350 * time.Millisecond) // outlive the handshake bound
	if _, err := conn.Write([]byte("GET /v1/x HTTP/1.0\r\nHost: localhost\r\n\r\n")); err != nil {
		t.Fatalf("write on live tunnel failed (leftover handshake deadline?): %v", err)
	}
	resp, err := io.ReadAll(conn)
	if err != nil {
		t.Fatalf("read on live tunnel failed: %v", err)
	}
	if !strings.Contains(string(resp), "200 OK") {
		t.Fatalf("tunnel answer = %q", resp)
	}
}
