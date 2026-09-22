// Package transport owns how an outbound request reaches its provider.
// The proxy layer decides which provider serves a request (the model →
// provider mapping); this package decides the path: direct, or through one
// configured proxy endpoint. The seam is deliberately one method —
//
//	Do(req) → (*http.Response, error)
//
// with a contract the provider layer depends on: an HTTP response — any
// status, 429 and 5xx included — is an answer, never an error; only
// transport-level failures (dial, TLS, handshake, context cancellation
// before headers) return an error, and the response body is never read or
// buffered here, so a 200 text/event-stream reaches the caller as a live
// io.ReadCloser. Upstream error interpretation, model rewriting, and prompt
// injection stay outside this package.
package transport

import (
	"net/http"
	"net/url"
)

// Doer executes one outbound HTTP exchange. *http.Client satisfies it; the
// interface exists so the request handler depends on the seam, not on a
// single global client — and so tests can stand in for it.
type Doer interface {
	Do(req *http.Request) (*http.Response, error)
}

// Resolver maps a validated transport config onto its Doer. The request
// handler resolves the model's provider transport through this, once per
// request; *Registry is the production implementation.
type Resolver interface {
	Doer(Config) Doer
}

// Kind selects the outbound path.
type Kind int

const (
	// Direct is the zero value: no configured proxy hop. It preserves the
	// historical shared client's behavior exactly, ambient
	// HTTP_PROXY/HTTPS_PROXY/NO_PROXY environment included.
	Direct Kind = iota
	// Proxy routes every request through the one configured proxy endpoint
	// (Config.ProxyURL). Exactly one endpoint — pools, rotation, health
	// checks, cooldown, and fallback are later work, not this type.
	Proxy
)

// Config is the validated shape of one named transport as it rides a config
// snapshot: the outbound kind plus, for Proxy, the parsed proxy endpoint.
// Userinfo in the URL is allowed — it is the proxy's authentication — and
// is credential material: it never reaches logs or error text. The zero
// value is the direct transport, which is also what a provider with no
// transport reference resolves to; validation guarantees ProxyURL is
// non-nil whenever Kind is Proxy.
type Config struct {
	Kind     Kind
	ProxyURL *url.URL
}

// key identifies a Config by content — the registry's pool-sharing
// identity: two Configs with the same key are the same transport and share
// one connection pool, so a config reload that does not change a
// transport's settings keeps its warm pool. The proxy URL string carries
// userinfo; it lives in memory as a map key only and is never logged.
func (c Config) key() string {
	if c.Kind == Proxy {
		if c.ProxyURL == nil {
			// Unreachable through config validation (a proxy transport
			// without a URL rejects the file); failing loud beats silently
			// routing a proxy request direct.
			panic("transport: proxy config without a proxy URL")
		}
		return "proxy " + c.ProxyURL.String()
	}
	return "direct"
}
