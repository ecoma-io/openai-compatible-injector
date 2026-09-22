package transport

import (
	"net/http"
	"net/url"
)

// newProxyClient builds the client that routes every request through one
// configured proxy endpoint. It is NOT a pool: no rotation, no health
// checks, no cooldown, no fallback, no retry — the client answers for
// exactly the URL validation accepted, and an upstream HTTP error of any
// status comes back as a response, never an error.
//
// http:// and https:// proxies ride net/http's native support (absolute-form
// requests for http targets, CONNECT tunnels for https targets, TLS to the
// proxy for the https scheme, Proxy-Authorization from the URL userinfo).
// socks5:// and socks5h:// ride the hand-rolled dialer in socks5.go —
// stdlib's own SOCKS5 support maps both schemes onto remote DNS, which
// would silently erase the local/remote resolve distinction the schemes
// exist to carry.
func newProxyClient(proxy *url.URL) *http.Client {
	tr := newBaseTransport()
	switch proxy.Scheme {
	case "http", "https":
		tr.Proxy = http.ProxyURL(proxy)
	case "socks5", "socks5h":
		// The dialer is the proxy hop; the cloned transport's ambient
		// ProxyFromEnvironment would put a second, unconfigured proxy in
		// front of it, so it is dropped outright rather than left to the
		// process environment.
		tr.Proxy = nil
		tr.DialContext = newSocks5Dialer(proxy).DialContext
	default:
		// Unreachable: validation accepts exactly the schemes above.
		panic("transport: unsupported proxy scheme " + proxy.Scheme)
	}
	return &http.Client{Transport: tr, CheckRedirect: relayRedirects}
}
