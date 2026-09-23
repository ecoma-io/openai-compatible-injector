package recovery

import (
	"errors"
	"strconv"
	"time"
)

// FailureClass names the layer one failure came from — the axis the matrix
// switches on before anything else.
type FailureClass int

const (
	// FailureAny constrains nothing. It is the zero value of a MATCH
	// predicate (a rule that does not care which layer failed) and never the
	// class of an observation: the handler always sets one of the four
	// concrete classes below.
	FailureAny FailureClass = iota
	// FailureHTTP is a received upstream HTTP status in 4xx..599. Statuses
	// outside that range are answers and never reach the matrix at all.
	FailureHTTP
	// FailureTransport is an attempt that provably produced no response: a
	// refused connection, a TLS failure, a proxy rejection, an egress pool
	// with no eligible member, a transport timeout.
	FailureTransport
	// FailureProtocol is a response that arrived but cannot be used: an
	// unparseable or oversized 200 body, a body read that failed mid-answer,
	// an error body whose bounded capture stalled.
	FailureProtocol
	// FailureCaller is the client's own cancellation or expired deadline. It
	// is decided from the request context, never from the wire, and it is
	// terminal by construction.
	FailureCaller
)

// String renders the class as the config-facing token.
func (c FailureClass) String() string {
	switch c {
	case FailureHTTP:
		return "http"
	case FailureTransport:
		return "transport"
	case FailureProtocol:
		return "protocol"
	case FailureCaller:
		return "caller"
	default:
		return "any"
	}
}

// ParseFailureClass reads a class token from configuration.
func ParseFailureClass(s string) (FailureClass, error) {
	switch s {
	case "http":
		return FailureHTTP, nil
	case "transport":
		return FailureTransport, nil
	case "protocol":
		return FailureProtocol, nil
	case "caller":
		return FailureCaller, nil
	default:
		return FailureAny, errors.New("failure must be one of http, transport, protocol, caller")
	}
}

// StatusClass buckets an HTTP status. The observation only ever carries 4xx
// or 5xx — every other range is an answer, and an answer is never a failure —
// but the full range is part of the vocabulary so that a configuration
// naming an unreachable bucket is a validated, visible no-op rather than an
// unknown key.
type StatusClass int

const (
	// StatusClassNone constrains nothing: the zero value of a match
	// predicate.
	StatusClassNone StatusClass = iota
	StatusClass1xx
	StatusClass2xx
	StatusClass3xx
	StatusClass4xx
	StatusClass5xx
)

// String renders the class as the config-facing token ("4xx"); the
// unconstrained zero value renders as the empty string, which is not a
// legal configuration token.
func (c StatusClass) String() string {
	switch c {
	case StatusClass1xx:
		return "1xx"
	case StatusClass2xx:
		return "2xx"
	case StatusClass3xx:
		return "3xx"
	case StatusClass4xx:
		return "4xx"
	case StatusClass5xx:
		return "5xx"
	default:
		return ""
	}
}

// ParseStatusClass reads a status-class token from configuration.
func ParseStatusClass(s string) (StatusClass, error) {
	switch s {
	case "1xx":
		return StatusClass1xx, nil
	case "2xx":
		return StatusClass2xx, nil
	case "3xx":
		return StatusClass3xx, nil
	case "4xx":
		return StatusClass4xx, nil
	case "5xx":
		return StatusClass5xx, nil
	default:
		return StatusClassNone, errors.New("status class must be one of 1xx, 2xx, 3xx, 4xx, 5xx")
	}
}

// StatusClassOf buckets a concrete HTTP status.
func StatusClassOf(status int) StatusClass {
	switch {
	case status >= 100 && status < 200:
		return StatusClass1xx
	case status >= 200 && status < 300:
		return StatusClass2xx
	case status >= 300 && status < 400:
		return StatusClass3xx
	case status >= 400 && status < 500:
		return StatusClass4xx
	case status >= 500 && status <= 599:
		return StatusClass5xx
	default:
		return StatusClassNone
	}
}

// IsUpstreamError reports whether a status is one this proxy normalizes: every
// 4xx and every 5xx up to 599. Outside the range the status is an answer —
// 2xx, 3xx (never followed), 204, 304, and anything a broken peer emits above
// 599 — and an answer is committed, never decided.
func IsUpstreamError(status int) bool {
	return status >= 400 && status <= 599
}

// TransportClass is the coarse bucket of a transport failure. It mirrors the
// transport package's Class vocabulary; the finer-grained Cause below is the
// token the log evidence carries.
type TransportClass int

const (
	// TransportClassNone constrains nothing.
	TransportClassNone TransportClass = iota
	TransportClassConnection
	TransportClassTimeout
	TransportClassProxyConnect
	TransportClassProxyAuth
)

// String renders the class as the config-facing token.
func (c TransportClass) String() string {
	switch c {
	case TransportClassConnection:
		return "connection"
	case TransportClassTimeout:
		return "timeout"
	case TransportClassProxyConnect:
		return "proxy_connect"
	case TransportClassProxyAuth:
		return "proxy_auth"
	default:
		return ""
	}
}

// ParseTransportClass reads a transport-class token from configuration.
func ParseTransportClass(s string) (TransportClass, error) {
	switch s {
	case "connection":
		return TransportClassConnection, nil
	case "timeout":
		return TransportClassTimeout, nil
	case "proxy_connect":
		return TransportClassProxyConnect, nil
	case "proxy_auth":
		return TransportClassProxyAuth, nil
	default:
		return TransportClassNone, errors.New("transport class must be one of connection, timeout, proxy_connect, proxy_auth")
	}
}

// Transport cause tokens. The closed set the transport layer can produce for
// a dialed-and-failed endpoint, plus two this layer owns: no_eligible_endpoint
// (an egress pool that dialed nothing, so no endpoint can be blamed) and
// caller_* (which the handler reclassifies under FailureCaller before the
// matrix ever sees them, and which are listed here so a configuration naming
// them is recognized rather than silently inert).
const (
	CauseConnectionRefused  = "connection_refused"
	CauseTLS                = "tls"
	CauseDial               = "dial"
	CauseNetworkTimeout     = "network_timeout"
	CauseDeadlineExceeded   = "deadline_exceeded"
	CauseProxyConnect       = "proxy_connect"
	CauseProxyTimeout       = "proxy_timeout"
	CauseProxyAuth          = "proxy_auth"
	CauseNoEligibleEndpoint = "no_eligible_endpoint"
	CauseCallerCanceled     = "caller_canceled"
	CauseCallerDeadline     = "caller_deadline_exceeded"
)

// transportCauses is the closed cause vocabulary, with the class each cause
// belongs to. The mapping is what lets a class-only rule and a cause rule
// order against each other without the configuration having to state it.
var transportCauses = map[string]TransportClass{
	CauseConnectionRefused:  TransportClassConnection,
	CauseTLS:                TransportClassConnection,
	CauseDial:               TransportClassConnection,
	CauseNetworkTimeout:     TransportClassTimeout,
	CauseDeadlineExceeded:   TransportClassTimeout,
	CauseProxyConnect:       TransportClassProxyConnect,
	CauseProxyTimeout:       TransportClassProxyConnect,
	CauseProxyAuth:          TransportClassProxyAuth,
	CauseNoEligibleEndpoint: TransportClassConnection,
}

// KnownTransportCause reports whether a token is part of the closed set.
func KnownTransportCause(s string) bool {
	_, ok := transportCauses[s]
	return ok
}

// TransportClassOfCause returns the class a cause belongs to. An unknown
// cause belongs to no class, which is what makes an unknown token match a
// cause rule but never a class rule.
func TransportClassOfCause(s string) TransportClass {
	return transportCauses[s]
}

// Protocol cause tokens: a response arrived, but it cannot be used. They are
// split from the HTTP causes on purpose — a 200 that is not JSON has no
// status row in the matrix, and an error body whose capture stalled never
// really arrived, so neither can be classified by code.
const (
	ProtocolInvalidResponse   = "invalid_response"
	ProtocolOversizedResponse = "oversized_response"
	ProtocolBodyReadFailed    = "body_read_failed"
	ProtocolBodyTimeout       = "body_timeout"
)

// KnownProtocolCause reports whether a token is part of the closed set.
func KnownProtocolCause(s string) bool {
	switch s {
	case ProtocolInvalidResponse, ProtocolOversizedResponse,
		ProtocolBodyReadFailed, ProtocolBodyTimeout:
		return true
	default:
		return false
	}
}

// Caller cause tokens. The canonical spellings match the log evidence's
// cause vocabulary exactly; configuration may use the shorter forms, which
// the config layer canonicalizes before building a rule.
const (
	CallerCanceled = "caller_canceled"
	CallerDeadline = "caller_deadline_exceeded"
)

// CauseExchangeBudget names the exchange envelope itself as the reason a walk
// stopped: the request had real exchanges left in principle, but not one more
// the envelope would fund, so the next outbound HTTP request was refused
// before it was dialed.
//
// It is deliberately NOT a transport cause. The transport causes blame a
// network path — a refused socket, a TLS handshake, a proxy — and this token
// blames nothing: no endpoint was reached, no member was struck, and the pool
// that refused the claim had already handed its permit back. It sits outside
// transportCauses so a class rule can never claim it, and it is reported
// under the reserved RuleIDBudgetRequest / RuleIDBudgetCandidate identities
// the engine attaches to its own envelope decision.
const CauseExchangeBudget = "exchange_budget"

// KnownCallerCause reports whether a token is part of the closed set.
func KnownCallerCause(s string) bool {
	switch s {
	case CallerCanceled, CallerDeadline:
		return true
	default:
		return false
	}
}

// Observation is one failed attempt, reduced to the bounded facts a policy
// may match on. It is the ONLY input the matrix accepts, which is what keeps
// policy evaluation free of HTTP, of response bodies, and of arbitrary
// expressions: there is no field here that can carry provider text.
//
// Every provider-supplied value on it has already been through the evidence
// parser's token gate (printable ASCII, bounded length) before it arrives.
type Observation struct {
	// Class is the layer the failure came from. The zero value (FailureAny)
	// matches no rule but a general one, which is the safe direction.
	Class FailureClass

	// HTTPStatus and StatusClass describe a FailureHTTP: the received status
	// and its bucket. StatusClass is always derived from HTTPStatus.
	HTTPStatus  int
	StatusClass StatusClass

	// TransportClass and TransportCause describe a FailureTransport. The
	// cause is the finer token (tls, connection_refused, ...); the class is
	// its coarse bucket, so a class rule and a cause rule can both be
	// configured and still be ordered deterministically.
	TransportClass TransportClass
	TransportCause string

	// ProtocolCause describes a FailureProtocol.
	ProtocolCause string

	// CallerCause describes a FailureCaller. It is present for the evidence
	// and the decision's reason token; the engine hard-stops on the class
	// itself rather than on any rule.
	CallerCause string

	// ProviderErrorType and ProviderErrorCode are the upstream error
	// object's own type/code members, when the bounded capture found an
	// OpenAI-shaped error object and the values passed the printable-token
	// gate. They are the only provider-supplied values policy may match on;
	// the provider's message is never captured, so it can never be matched.
	ProviderErrorType string
	ProviderErrorCode string

	// Streaming reports whether the client asked for a stream. It lets a
	// policy treat a streamed failure differently from a buffered one.
	Streaming bool

	// Committed reports whether a response has already reached the client.
	// The engine hard-stops on it: after commitment there is no retry and no
	// fallback, whatever the matrix says.
	Committed bool

	// CandidateIndex is the one-based chain position of the candidate that
	// failed, CandidateAttempt its one-based attempt number on that
	// candidate, and RetryIndex CandidateAttempt−1.
	CandidateIndex   int
	CandidateAttempt int
	RetryIndex       int

	// Elapsed is how long the attempt took to reach its outcome. It rides
	// along for the evidence; no rule predicate reads it.
	Elapsed int64

	// RetryAfter is the failed response's parsed Retry-After directive, 0
	// when absent, invalid, or already past. It is not a predicate — the
	// Retry-After policy decides whether it may raise a wait at all, and the
	// engine caps whatever it raises before anything sleeps.
	RetryAfter time.Duration
}

// Reason returns the closed-set reason token the evidence events carry for an
// observation. It is derived from the observation alone — never from error
// text — and its vocabulary is the one the log contract already documents:
// the three retryable statuses name themselves, other statuses carry their
// number, 5xx collapses to http_5xx, and the unusable-answer and transport
// cases carry their typed cause.
func Reason(o Observation) string {
	switch o.Class {
	case FailureHTTP:
		switch o.HTTPStatus {
		case 408:
			return "http_408"
		case 425:
			return "http_425"
		case 429:
			return "http_429"
		}
		if o.HTTPStatus >= 500 && o.HTTPStatus <= 599 {
			return "http_5xx"
		}
		return "http_" + strconv.Itoa(o.HTTPStatus)
	case FailureTransport:
		if o.TransportCause != "" {
			return o.TransportCause
		}
		return "transport_error"
	case FailureProtocol:
		return protocolReason(o.ProtocolCause)
	case FailureCaller:
		if o.CallerCause != "" {
			return o.CallerCause
		}
		return "caller_ended"
	default:
		return "unknown"
	}
}

// protocolReason maps a protocol cause to the event token. The three
// pre-existing tokens keep their spelling; the oversized-response case shares
// upstream_invalid_response with the unparseable case exactly as it did
// before this domain existed — the RULE ID is what distinguishes the two
// policies now, not the reason token.
func protocolReason(cause string) string {
	switch cause {
	case ProtocolBodyReadFailed:
		return "upstream_body_read_failed"
	case ProtocolBodyTimeout:
		return "upstream_body_timeout"
	default:
		return "upstream_invalid_response"
	}
}
