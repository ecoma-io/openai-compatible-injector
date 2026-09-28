package transport

import (
	"net"
	"net/http"
	"time"
)

// NewDirectClient builds the client direct requests execute through: the
// normal Go HTTP stack with the service's tuning, no added middleware. It
// preserves the historical shared client exactly — including the ambient
// HTTP_PROXY/HTTPS_PROXY/NO_PROXY environment variables inherited from
// http.DefaultTransport, so a deployment that relied on them keeps working.
func NewDirectClient() *http.Client {
	return &http.Client{
		Transport: newBaseTransport(),
		// Redirects are relayed verbatim, never followed. Go's default
		// policy would convert the POST into a body-less GET on 301/302/303
		// and replay the transformed request body to whatever Location names
		// on 307/308. (No credential could leak through a followed redirect
		// anyway — the upstream request carries no Authorization at all.)
		// An OpenAI-compatible API does not redirect; an unexpected 3xx is
		// the upstream's answer and the client's to judge.
		CheckRedirect: relayRedirects,
	}
}

// relayRedirects is the redirect policy every transport's client shares:
// never follow, hand the response to the caller.
func relayRedirects(*http.Request, []*http.Request) error {
	return http.ErrUseLastResponse
}

// newBaseTransport clones http.DefaultTransport's settings and then applies
// the service's tuning: a 30s dial timeout, a 10s TLS handshake timeout,
// generous idle pooling, and — deliberately — no ResponseHeaderTimeout.
// HTTP/2 is attempted eagerly. The clone keeps http.DefaultTransport's
// Proxy (ProxyFromEnvironment); callers that own their proxy hop override
// Proxy or DialContext.
//
// ResponseHeaderTimeout is left at zero because this transport is shared by
// every path and a fixed constant here would be a policy no operator wrote:
// a provider that queues a request behind its own load legitimately takes
// minutes to send a status line, and nothing in the config plane states a
// "time to first header" budget for the walk. The tuning that DOES bound a
// blocked exchange lives where the operator's own numbers are:
//
//   - the request's context. A client that hangs up or a caller that set a
//     deadline cancels the exchange, headers or body, through net/http's own
//     handling of a canceled request context.
//   - internal/proxy's recovery window, for the post-commitment continuation
//     loop: it cancels each hop's request context at the frozen
//     `stream.max-elapsed` instant, which is why a continuation hop cannot
//     park in Do waiting for headers nobody will send. That bound is derived
//     from configuration and scoped to the one path whose budget states it,
//     rather than applied to every dial the process makes.
func newBaseTransport() *http.Transport {
	var tr *http.Transport
	if base, ok := http.DefaultTransport.(*http.Transport); ok && base != nil {
		tr = base.Clone()
	} else {
		tr = &http.Transport{}
	}
	tr.DialContext = (&net.Dialer{Timeout: 30 * time.Second, KeepAlive: 30 * time.Second}).DialContext
	tr.TLSHandshakeTimeout = 10 * time.Second
	tr.ResponseHeaderTimeout = 0
	tr.IdleConnTimeout = 90 * time.Second
	tr.MaxIdleConns = 100
	tr.MaxIdleConnsPerHost = 100
	tr.ForceAttemptHTTP2 = true
	return tr
}
