package transport

import "sync"

// Registry owns the process's long-lived transport clients, keyed by
// validated Config content. Doer is a get-or-create: the first request for
// a transport builds its client — with its connection pool — and every
// later request for the same config reuses it, across config reloads, so
// pools survive unchanged reloads warm and no per-request client or pool
// is ever built. Entries are bounded by configuration, not traffic.
type Registry struct {
	mu    sync.Mutex
	doers map[string]Doer
}

// NewRegistry returns an empty registry.
func NewRegistry() *Registry {
	return &Registry{doers: make(map[string]Doer)}
}

// Doer returns the Doer for the config, creating it on first use. A request
// resolves its Doer once and holds the returned value for its whole
// lifetime, so an in-flight request (or stream) keeps its pool even across
// a reload that evicts the entry.
func (r *Registry) Doer(c Config) Doer {
	k := c.key() // panics on a proxy config without a URL: unreachable via validation
	r.mu.Lock()
	defer r.mu.Unlock()
	if d, ok := r.doers[k]; ok {
		return d
	}
	var d Doer
	switch c.Kind {
	case Direct:
		d = NewDirectClient()
	case Proxy:
		d = newProxyClient(c.ProxyURL)
	default:
		panic("transport: unknown kind") // unreachable: Kind is set by validation alone
	}
	r.doers[k] = d
	return d
}

// Retain keeps doers only for the listed configs and evicts the rest:
// closing their idle pooled connections and dropping the entries, so a
// reload that removes or rewrites a transport does not leave its pool
// behind. Eviction is safe for in-flight work — CloseIdleConnections
// touches only idle connections; a request already executing on the doer
// keeps its live connection and finishes normally. The poller's onPublish
// hook calls this with the new snapshot's transport set.
func (r *Registry) Retain(active []Config) {
	keys := make(map[string]struct{}, len(active))
	for _, c := range active {
		keys[c.key()] = struct{}{}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for k, d := range r.doers {
		if _, ok := keys[k]; ok {
			continue
		}
		closeIdle(d)
		delete(r.doers, k)
	}
}

// CloseIdleConnections closes every current doer's idle pooled
// connections — the shutdown-time drain the server calls before exiting.
func (r *Registry) CloseIdleConnections() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, d := range r.doers {
		closeIdle(d)
	}
}

func closeIdle(d Doer) {
	if c, ok := d.(interface{ CloseIdleConnections() }); ok {
		c.CloseIdleConnections()
	}
}
