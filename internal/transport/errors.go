package transport

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"net/url"
	"os"
	"syscall"
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

// Classify buckets a Do error. The request provably got no response —
// that is what a Do error means — but "no response" is not "never sent":
// only the connection-class and proxy-class buckets establish that no
// connection ever carried the request. The canceled check comes first so a
// cancellation racing a proxy failure classifies as the caller's event,
// not the endpoint's.
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

// Failure is the context-aware classification of one failed outbound
// attempt: the canonical class the fallback decision reads, a bounded
// cause token for the evidence log, whether the caller's own request
// ended (cancellation or deadline), and whether the request could have
// reached the wire. CallerTerminated is terminal at every fallback layer —
// there is nobody left to answer — and SendStateUnknown forbids a replay
// below the layer that owns retry policy, because a request that may have
// been processed can never be repeated behind the operator's back.
type Failure struct {
	Class            Class
	Cause            string
	CallerTerminated bool
	SendState        SendState
}

// SendState records whether a failed attempt provably never left the
// client. Only connection-establishment failures are definitely-not-sent:
// no TCP connection, TLS handshake, or proxy tunnel ever existed to carry
// the request bytes. Every other failure — a timeout on an established
// connection above all — is send-unknown, because the upstream may already
// be processing a request whose answer never came back.
//
// The zero value is the conservative one: an unclassified failure is
// treated as possibly-transmitted, never as provably-unsent.
type SendState int

const (
	// SendStateUnknown means the request may have reached the upstream.
	// No layer below the policy owner may silently replay it.
	SendStateUnknown SendState = iota
	// SendStateNotSent means the failure provably preceded any request
	// byte — the connection was never established — so replaying it on
	// another egress carries no duplicate-request risk.
	SendStateNotSent
)

// String renders the send state as the closed-set evidence token.
func (s SendState) String() string {
	if s == SendStateNotSent {
		return "definitely_not_sent"
	}
	return "send_unknown"
}

// sendStateOf maps a classified failure to its send state. The cause
// vocabulary already carries the axis: connection-class causes
// (connection_refused / tls / dial) and proxy-class causes are
// connection-establishment failures, while timeout-class failures are the
// ones that can arrive after the request went out. REFUSED and TLS are the
// load-bearing cases — a refused TCP connect and a failed TLS handshake
// both provably precede the HTTP request — so a refused or unreachable
// egress still falls back while a timeout never does.
func sendStateOf(class Class) SendState {
	switch class {
	case ClassConnection, ClassProxyConnect, ClassProxyAuth:
		return SendStateNotSent
	default:
		return SendStateUnknown
	}
}

// Cause tokens. Closed set, code-owned, never derived from error text —
// they carry the detail the single Class token deliberately flattens
// (refused vs TLS vs dial) while staying bounded by construction.
const (
	// connection-class causes.
	CauseConnectionRefused = "connection_refused"
	CauseTLS               = "tls"
	CauseDial              = "dial"
	// proxy-class causes.
	CauseProxyConnect = "proxy_connect"
	CauseProxyTimeout = "proxy_timeout"
	// timeout-class causes.
	CauseDeadlineExceeded = "deadline_exceeded"
	CauseNetworkTimeout   = "network_timeout"
	// canceled-class causes.
	CauseCallerCanceled         = "caller_canceled"
	CauseCallerDeadlineExceeded = "caller_deadline_exceeded"
)

// ClassifyAttempt buckets a failed attempt WITH the context the attempt
// ran under. Error-chain inspection alone cannot establish deadline
// ownership — a caller deadline and a transport timer both surface as
// context.DeadlineExceeded — so the caller's context decides: a context
// that is already done owns the failure (canceled, terminal), and only a
// still-live context classifies the error on its own terms, where a
// timeout-shaped failure is the endpoint's and stays fallback-eligible.
func ClassifyAttempt(ctx context.Context, err error) Failure {
	if err == nil {
		return Failure{Class: ClassNone}
	}
	// Caller-owned wins over whatever the error chain carries: a
	// cancellation racing a proxy failure is the caller's event, and a
	// caller deadline that fired mid-attempt must never read as the
	// endpoint's timeout.
	switch ctx.Err() {
	case context.Canceled:
		return Failure{Class: ClassCanceled, Cause: CauseCallerCanceled, CallerTerminated: true, SendState: SendStateUnknown}
	case context.DeadlineExceeded:
		return Failure{Class: ClassCanceled, Cause: CauseCallerDeadlineExceeded, CallerTerminated: true, SendState: SendStateUnknown}
	}
	class := Classify(err)
	f := Failure{Class: class, SendState: sendStateOf(class)}
	switch class {
	case ClassProxyAuth:
		f.Cause = "proxy_auth"
	case ClassProxyConnect:
		f.Cause = CauseProxyConnect
		if isTimeoutErr(err) {
			f.Cause = CauseProxyTimeout
		}
	case ClassTimeout:
		f.Cause = CauseNetworkTimeout
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, os.ErrDeadlineExceeded) {
			f.Cause = CauseDeadlineExceeded
		}
	case ClassConnection:
		f.Cause = causeOfConnection(err)
	}
	return f
}

// isTimeoutErr reports whether the error chain carries a timeout marker
// (stdlib deadline sentinels or a net.Error timeout), independent of the
// class the typed proxy errors contributed.
func isTimeoutErr(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, os.ErrDeadlineExceeded) {
		return true
	}
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout()
}

// causeOfConnection narrows the connection class to its code-owned cause:
// refused, TLS verification, or the generic dial/transport bucket.
func causeOfConnection(err error) string {
	var oe *net.OpError
	if errors.As(err, &oe) && errors.Is(oe.Err, syscall.ECONNREFUSED) {
		return CauseConnectionRefused
	}
	var te *tls.CertificateVerificationError
	if errors.As(err, &te) {
		return CauseTLS
	}
	var ue *url.Error
	if errors.As(err, &ue) {
		return causeOfConnection(ue.Err)
	}
	if errors.Is(err, syscall.ECONNREFUSED) {
		return CauseConnectionRefused
	}
	return CauseDial
}
