package transport

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/url"
	"strconv"
	"time"
)

// socks5HandshakeTimeout bounds the TCP dial to the proxy plus the whole
// SOCKS5 greeting/auth/CONNECT exchange. It matches the dial timeout the
// direct transport applies to a plain TCP connect: the HTTP transport's
// ResponseHeaderTimeout never starts before Client.Do returns, so without
// this bound a proxy that accepts the TCP connection and then stalls would
// strand the dial goroutine indefinitely.
const socks5HandshakeTimeout = 30 * time.Second

// socks5Dialer implements a minimal SOCKS5 CONNECT dialer (RFC 1928 §3-4)
// with username/password auth (RFC 1929), hand-rolled on the stdlib. The
// protocol is small enough that a dependency would cost more than the
// bytes — and both candidates treat the DNS semantics identically: stdlib
// net/http maps socks5 and socks5h onto the same hostname CONNECT, and
// x/net/proxy does the same, which would silently erase the distinction
// the two schemes exist to carry. Here they differ, deliberately:
//
//	socks5://  resolves the UPSTREAM hostname LOCALLY and CONNECTs the IP
//	           literal — the proxy never sees the hostname;
//	socks5h:// CONNECTs the hostname itself (ATYP=3, RFC 1928 §5) and the
//	           proxy resolves it — remote DNS, the leak-free form.
//
// Either way the PROXY host is dialed locally: it must be reachable before
// any tunnel exists. Ported from the organisation's opencode-free-proxy
// (same hand-rolled lineage), minus its health-class error taxonomy —
// this package has no fallback machinery to feed.
type socks5Dialer struct {
	proxy   *url.URL
	dialer  net.Dialer
	timeout time.Duration // bounds dial + handshake; overridable in tests
}

func newSocks5Dialer(proxy *url.URL) *socks5Dialer {
	return &socks5Dialer{
		proxy: proxy,
		dialer: net.Dialer{
			Timeout:   socks5HandshakeTimeout,
			KeepAlive: 30 * time.Second,
		},
		timeout: socks5HandshakeTimeout,
	}
}

// DialContext establishes the tunnel for addr ("host:port" of the upstream
// provider). The returned conn is ready for the HTTP transport's traffic.
func (d *socks5Dialer) DialContext(ctx context.Context, _ string, addr string) (net.Conn, error) {
	conn, err := d.dialer.DialContext(ctx, "tcp", d.proxy.Host)
	if err != nil {
		// The proxy endpoint is scheme+host — the same class of detail the
		// upstream's scheme+host is — and never carries the credentials
		// (userinfo never dials). Typed so the pool classifies the hop
		// failure; the message is byte-identical to the historical
		// fmt.Errorf wrap.
		return nil, &ProxyConnectError{msg: "socks5: dial proxy", cause: err}
	}
	// Bound the WHOLE handshake: the net.Dialer timeout covers only the TCP
	// connect, and a proxy that accepts then never answers would otherwise
	// strand the dial goroutine past any caller deadline (the HTTP
	// transport's ResponseHeaderTimeout never starts — Client.Do has not
	// returned yet). Reads below have no ctx of their own, hence the
	// ctx-abort: cancellation mid-handshake closes the conn, aborting the
	// in-flight ReadFull. The deadline clears once the tunnel is up.
	_ = conn.SetDeadline(time.Now().Add(d.timeout))
	// context.AfterFunc, not a select-watcher goroutine: a watcher selecting
	// between ctx.Done() and a handshake-done channel may pick ctx.Done() —
	// and Close() the just-returned LIVE tunnel — when both channels are
	// ready simultaneously (Go's select picks randomly). AfterFunc arms only
	// on ctx cancellation and stop() disarms it deterministically, so a
	// completed handshake can never race its own teardown; stop() runs on
	// every return below, so there is no watcher leak either.
	stopWatcher := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stopWatcher()
	if err := d.negotiate(conn); err != nil {
		_ = conn.Close()
		return nil, err
	}
	if err := d.connect(ctx, conn, addr); err != nil {
		_ = conn.Close()
		return nil, err
	}
	_ = conn.SetDeadline(time.Time{}) // caller owns the conn from here
	return conn, nil
}

// hopErr types an I/O failure on the proxy connection itself — the proxy
// died or went unreachable mid-handshake. Typed so both the pool's fallback
// decision and the access log's error_class see the proxy hop, not a
// generic dial failure; the cause rides the Unwrap chain unchanged.
func hopErr(cause error) error {
	return &ProxyConnectError{msg: "socks5: proxy handshake failed", cause: cause}
}

// negotiate picks the auth method (RFC 1928 §3) and, if the proxy demands
// it, authenticates (RFC 1929 §2). Credentials come from the proxy URL
// userinfo; nothing here ever formats them into an error or a log.
func (d *socks5Dialer) negotiate(conn net.Conn) error {
	hasAuth := d.proxy.User != nil
	methods := []byte{0x00} // no-auth is always offered
	if hasAuth {
		methods = append(methods, 0x02) // username/password
	}
	greet := append([]byte{0x05, byte(len(methods))}, methods...)
	if err := writeAll(conn, greet); err != nil {
		return hopErr(err)
	}
	buf := make([]byte, 2)
	if _, err := io.ReadFull(conn, buf); err != nil {
		return hopErr(err)
	}
	if buf[0] != 0x05 {
		return &ProxyConnectError{msg: fmt.Sprintf("socks5: proxy replied with version %d", buf[0])}
	}
	switch buf[1] {
	case 0x00:
		return nil
	case 0x02:
		if !hasAuth {
			return &ProxyAuthError{msg: "socks5: proxy demanded auth but none configured"}
		}
		user := d.proxy.User.Username()
		pass, _ := d.proxy.User.Password()
		// Defensive duplicate of the config plane's RFC 1929 length bound
		// (255 bytes per field): a value that slipped past LoadRuntime must
		// still fail as a typed proxy error — classified by type for the
		// pool's fallback decision and the log's error_class, never as a
		// bare fmt.Errorf — with static text, no credential material in it.
		if len(user) > 255 || len(pass) > 255 {
			return &ProxyAuthError{msg: "socks5: credentials exceed the RFC 1929 length limit"}
		}
		p := []byte{0x01, byte(len(user))}
		p = append(p, user...)
		p = append(p, byte(len(pass)))
		p = append(p, pass...)
		if err := writeAll(conn, p); err != nil {
			return hopErr(err)
		}
		if _, err := io.ReadFull(conn, buf); err != nil {
			return hopErr(err)
		}
		// RFC 1929 §2: version 0x01 + status 0x00 is success; anything else
		// is an auth failure. A malformed reply cannot be distinguished from
		// a rejection at 2 bytes, and this package has no downstream
		// machinery that would care about the difference — both are the same
		// transport error.
		if buf[0] != 0x01 || buf[1] != 0x00 {
			return &ProxyAuthError{msg: "socks5: proxy authentication failed"}
		}
		return nil
	case 0xff:
		// NO ACCEPTABLE METHODS (RFC 1928 §3): the proxy rejected every
		// method we offered.
		return &ProxyAuthError{msg: "socks5: no acceptable authentication method"}
	default:
		return &ProxyConnectError{msg: fmt.Sprintf("socks5: proxy chose unknown method %d", buf[1])}
	}
}

// connect sends CONNECT for addr and validates the reply, consuming the
// variable-length bind address (RFC 1928 §4, §6). The CONNECT address form
// follows the proxy URL's scheme: socks5h:// carries the hostname (ATYP=3
// domain form — 1 length byte + FQDN) and the proxy resolves it; socks5://
// resolves LOCALLY and sends the IP literal. An IP-literal target keeps its
// literal form (ATYP=1/4) in both modes — it needs no DNS, and ATYP=3 is
// defined for FQDNs, not dotted quads. A failed remote-resolve CONNECT
// fails exactly like any other CONNECT error: there is no silent fallback
// to local resolution and no retry.
func (d *socks5Dialer) connect(ctx context.Context, conn net.Conn, addr string) error {
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("socks5: bad target %q: %w", addr, err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return fmt.Errorf("socks5: bad target port %q: %w", portStr, err)
	}
	var atyp byte
	var addrBytes []byte
	switch ip := net.ParseIP(host); {
	case ip != nil:
		// Literal target: no resolution on either side of the tunnel.
		if v4 := ip.To4(); v4 != nil {
			atyp, addrBytes = 0x01, v4
		} else {
			atyp, addrBytes = 0x04, ip.To16()
		}
	case d.proxy.Scheme == "socks5h":
		// REMOTE resolution: the proxy sees the hostname and does the
		// lookup — the form providers that refuse IP-literal CONNECT
		// targets require, and the form that keeps upstream hostnames out
		// of the local resolver's view. Domain form (RFC 1928 §5): 1
		// length byte, then the FQDN.
		if len(host) > 255 {
			return fmt.Errorf("socks5: hostname exceeds the RFC 1928 domain-form limit of 255 bytes")
		}
		atyp, addrBytes = 0x03, append([]byte{byte(len(host))}, host...)
	default:
		// LOCAL resolution: the proxy never sees the hostname. This is the
		// socks5:// contract.
		addrs, err := net.DefaultResolver.LookupIPAddr(ctx, host)
		if err != nil || len(addrs) == 0 {
			if err == nil {
				err = fmt.Errorf("no addresses")
			}
			return &ProxyConnectError{msg: fmt.Sprintf("socks5: resolve %q locally", host), cause: err}
		}
		ip := addrs[0].IP
		if v4 := ip.To4(); v4 != nil {
			atyp, addrBytes = 0x01, v4
		} else {
			atyp, addrBytes = 0x04, ip.To16()
		}
	}
	req := []byte{0x05, 0x01, 0x00, atyp}
	req = append(req, addrBytes...)
	req = append(req, byte(port>>8), byte(port))
	if err := writeAll(conn, req); err != nil {
		return hopErr(err)
	}
	head := make([]byte, 4)
	if _, err := io.ReadFull(conn, head); err != nil {
		return hopErr(err)
	}
	if head[0] != 0x05 {
		return fmt.Errorf("socks5: bad reply version %d", head[0])
	}
	if head[1] != 0x00 {
		return replyErr(head[1])
	}
	var bound int
	switch head[3] {
	case 0x01:
		bound = 4
	case 0x04:
		bound = 16
	case 0x03:
		l := make([]byte, 1)
		if _, err := io.ReadFull(conn, l); err != nil {
			return hopErr(err)
		}
		bound = int(l[0])
	default:
		return &ProxyConnectError{msg: fmt.Sprintf("socks5: bad bind address type %d", head[3])}
	}
	if _, err := io.CopyN(io.Discard, conn, int64(bound+2)); err != nil {
		return hopErr(err)
	}
	return nil
}

// replyErr maps a SOCKS5 reply code (RFC 1928 §6) onto its error. Every
// reply is a failure of the proxy hop to establish the tunnel, so each is
// typed ProxyConnectError — the pool's fallback and health machinery reads
// the type, never the text. Texts are unchanged from their historical
// forms.
func replyErr(rep byte) error {
	var e *ProxyConnectError
	switch rep {
	case 0x01:
		e = &ProxyConnectError{msg: "socks5: general failure"}
	case 0x02:
		e = &ProxyConnectError{msg: "socks5: connection not allowed by ruleset"}
	case 0x03:
		e = &ProxyConnectError{msg: "socks5: network unreachable"}
	case 0x04:
		e = &ProxyConnectError{msg: "socks5: host unreachable"}
	case 0x05:
		e = &ProxyConnectError{msg: "socks5: connection refused"}
	case 0x06:
		e = &ProxyConnectError{msg: "socks5: ttl expired"}
	case 0x07:
		e = &ProxyConnectError{msg: "socks5: command not supported"}
	case 0x08:
		e = &ProxyConnectError{msg: "socks5: address type not supported"}
	default:
		e = &ProxyConnectError{msg: fmt.Sprintf("socks5: unknown reply %d", rep)}
	}
	return e
}

func writeAll(conn net.Conn, b []byte) error {
	for len(b) > 0 {
		n, err := conn.Write(b)
		if err != nil {
			return err
		}
		b = b[n:]
	}
	return nil
}
