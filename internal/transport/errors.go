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

// RequestBuildError reports a request this process could not CONSTRUCT: no
// socket was touched, no member was chosen, and nothing reached any wire, so
// the failure belongs to the caller that asked for the request rather than to
// an endpoint — the handler reports it under its own local vocabulary rather
// than as a transport failure, exactly as the single-endpoint path reports
// its own build failure.
//
// The text is our own static wording. The cause (http.NewRequest's error,
// which quotes the request URL inside its message) stays reachable through
// errors.Is/As and is never printed: an endpoint URL can carry credentials in
// its query string, and this value reaches a log line.
type RequestBuildError struct{ cause error }

func (e *RequestBuildError) Error() string { return "transport: request build failed" }
func (e *RequestBuildError) Unwrap() error { return e.cause }

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
// this bucket is deliberately coarse (ClassConnection is the catch-all), so
// it must not be read as a statement about the wire. sendStateOf carries
// that axis separately, from the failing operation rather than the class.
// The canceled check comes first so a cancellation racing a proxy failure
// classifies as the caller's event, not the endpoint's.
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
// client. Only a failure in the CONNECTION-ESTABLISHMENT phase that is
// ALSO provable as such is definitely-not-sent: no TCP connection, proxy
// tunnel, or TLS certificate verification ever completed. Every other
// failure — a timeout or a reset on an established connection above all —
// is send-unknown, because the upstream may already be processing a request
// whose answer never came back.
//
// "Provable" is the operative word, and it is why the mapping is narrower
// than the phase: most of what fails during a TLS handshake arrives in the
// same shapes Go produces AFTER the handshake, so the phase cannot be read
// off the error. See sendStateOf.
//
// The zero value is the conservative one: a failure that is not proven
// pre-send is treated as possibly-transmitted, never as provably-unsent.
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

// sendStateOf reports whether a failed attempt provably preceded any
// request byte, judged from the failing WIRE OPERATION rather than from the
// class.
//
// The class is deliberately not the input: ClassConnection is the
// classifier's catch-all, and it reaches failures on an ESTABLISHED
// connection — a socket reset while the answer was awaited, an EOF after
// the body was written, an HTTP/1.x transport broken mid-headers. Those
// prove the opposite of what the bucket's name suggests: the request was
// delivered and may already have been processed. Reading the bucket as
// "never connected" would replay exactly the requests this axis exists to
// protect.
//
// The positive evidence of "never sent" is one of:
//
//   - a proxy tunnel failure (typed auth or connect error) — the tunnel
//     precedes the request;
//   - a TLS CERTIFICATE VERIFICATION failure (typed) — the client rejects
//     the peer's certificate during the handshake, which completes before
//     the first request byte;
//   - a *net.OpError whose op is "dial" or "proxyconnect" — the failure is
//     a failure to ESTABLISH the connection, whatever class it landed in.
//     A blackholed egress belongs here: its error is timeout-shaped, but
//     nothing was sent, so it is both fallback-eligible and safe to replay;
//   - a bare refused syscall — only a connect attempt can produce it.
//
// Certificate verification is the ONLY TLS failure this mapping can prove,
// and the narrowness is deliberate. A TLS handshake that fails for any
// other reason is still pre-send — no request byte exists yet — but Go
// reports those failures through shapes it also produces on an ESTABLISHED
// connection, and none of them is typed as a handshake failure:
//
//   - a fatal alert from the peer (a version or cipher mismatch, a missing
//     client certificate) arrives as `*net.OpError{Op: "remote error"}`
//     wrapping an unexported `*tls.permanentError` and an unexported
//     `tls.alert` — no exported type to match;
//   - a peer that answers the hello with plaintext HTTP arrives as an
//     untyped `errors.errorString` ("http: server gave HTTP response to
//     HTTPS client"), reachable only by matching its text, which this
//     package never does;
//   - a peer that closes at accept arrives as a bare EOF, exactly like an
//     established connection dying before it answered.
//
// Those stay send-unknown, and the two errors are not symmetric: calling a
// delivered request unsent duplicates it upstream, while calling an unsent
// failure unknown only makes that egress member ineligible for this one
// attempt — the provider walk above still covers the request by policy. So
// the mapping takes the conservative side wherever the phase is not
// provable from a type, and gives up a fallback rather than a request.
//
// A "read"/"write" op, an error with no op to read at all (a bare EOF, an
// HTTP/2 stream error, a shape this package does not know), and every other
// failure are send-unknown: the conservative answer, and the only one
// defensible without evidence.
func sendStateOf(err error) SendState {
	var pa *ProxyAuthError
	var pc *ProxyConnectError
	var te *tls.CertificateVerificationError
	if errors.As(err, &pa) || errors.As(err, &pc) || errors.As(err, &te) {
		return SendStateNotSent
	}
	var oe *net.OpError
	if errors.As(err, &oe) {
		switch oe.Op {
		case "dial", "proxyconnect":
			return SendStateNotSent
		}
		return SendStateUnknown
	}
	if errors.Is(err, syscall.ECONNREFUSED) {
		return SendStateNotSent
	}
	return SendStateUnknown
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
	f := Failure{Class: class, SendState: sendStateOf(err)}
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
