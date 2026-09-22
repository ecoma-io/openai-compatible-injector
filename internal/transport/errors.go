package transport

import (
	"context"
	"errors"
	"net"
	"os"
)

// ProxyAuthError reports an egress proxy that refused this client's
// authentication: an RFC 1929 username/password rejection, a proxy that
// demanded a method we cannot offer, or an HTTP CONNECT answered 407. The
// text is always our own static wording — credential material and proxy
// output never enter it — so the value is safe to log wherever the handler
// already logs sanitized transport errors.
type ProxyAuthError struct{ msg string }

func (e *ProxyAuthError) Error() string { return e.msg }

// ProxyConnectError reports a failure establishing or using the proxy hop
// itself: the TCP dial to the proxy endpoint, a SOCKS5 CONNECT reply code,
// a socks5h remote resolution failure. cause, when set, keeps the underlying
// error reachable through errors.Is/As (a dial aborted by the caller's
// context must still classify as canceled, never as a proxy failure).
type ProxyConnectError struct {
	msg   string
	cause error
}

func (e *ProxyConnectError) Error() string {
	if e.cause != nil {
		return e.msg + ": " + e.cause.Error()
	}
	return e.msg
}

func (e *ProxyConnectError) Unwrap() error { return e.cause }

// Class buckets a transport failure for the fallback decision and the
// access log's error_class field. It is derived from typed errors and
// stdlib sentinels only — never from message text.
type Class int

const (
	// ClassNone means err carried no transport failure (nil, or a non-error
	// classification of an already-returned response).
	ClassNone Class = iota
	// ClassConnection is any pre-response transport failure that is not
	// more specifically typed: refused connections, TLS, reset sockets.
	ClassConnection
	// ClassProxyConnect is a failure establishing or using the proxy hop.
	ClassProxyConnect
	// ClassProxyAuth is the proxy refusing our credentials.
	ClassProxyAuth
	// ClassTimeout is a deadline the transport hit (dial, handshake, or an
	// exhausted context deadline).
	ClassTimeout
	// ClassCanceled is the caller's own cancellation — never a fault of the
	// endpoint, never a fallback trigger, never a health strike.
	ClassCanceled
)

// String renders the class as the log field's token.
func (c Class) String() string {
	switch c {
	case ClassConnection:
		return "connection"
	case ClassProxyConnect:
		return "proxy_connect"
	case ClassProxyAuth:
		return "proxy_auth"
	case ClassTimeout:
		return "timeout"
	case ClassCanceled:
		return "canceled"
	default:
		return "none"
	}
}

// Classify buckets a Do error. Every class except canceled is
// fallback-eligible: the request provably got no response. The canceled
// check comes first so a cancellation racing a proxy failure classifies as
// the caller's event, not the endpoint's.
func Classify(err error) Class {
	if err == nil {
		return ClassNone
	}
	if errors.Is(err, context.Canceled) {
		return ClassCanceled
	}
	var pa *ProxyAuthError
	if errors.As(err, &pa) {
		return ClassProxyAuth
	}
	var pc *ProxyConnectError
	if errors.As(err, &pc) {
		return ClassProxyConnect
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, os.ErrDeadlineExceeded) {
		return ClassTimeout
	}
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		return ClassTimeout
	}
	return ClassConnection
}
