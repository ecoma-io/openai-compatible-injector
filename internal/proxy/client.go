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
	return &http.Client{Transport: tr}
}
