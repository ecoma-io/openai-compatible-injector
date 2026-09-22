package proxy

import (
	"net"
	"net/http"
	"time"
)

// NewSharedClient builds the single HTTP client shared by every proxied
// request. The transport clones http.DefaultTransport's settings and then
// applies the proxy's tuning: a 30s dial timeout, a 10s TLS handshake
// timeout, generous idle pooling, and — deliberately — no
// ResponseHeaderTimeout, because SSE streams stay open far longer than any
// sane header deadline. HTTP/2 is attempted eagerly.
func NewSharedClient() *http.Client {
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
	return &http.Client{
		Transport: tr,
		// Redirects are relayed verbatim, never followed. Go's default
		// policy would convert the POST into a body-less GET on 301/302/303
		// and replay the transformed request body to whatever Location names
		// on 307/308. (No credential could leak through a followed redirect
		// anyway — the upstream request carries no Authorization at all.)
		// An OpenAI-compatible API does not redirect; an unexpected 3xx is
		// the upstream's answer and the client's to judge.
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}
