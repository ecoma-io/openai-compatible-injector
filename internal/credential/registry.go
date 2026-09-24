package credential

import "sync"

// Registry owns the process's credential pools, keyed by validated spec
// content. Pool is a get-or-create: the first request for a credential
// configuration builds its rotation state and every later request for the
// same content reuses it — across config reloads — so an unchanged pool
// keeps its rotation cursor and cooldown state warm and no per-request
// state is ever rebuilt. Entries are bounded by configuration, not traffic.
// The shape mirrors transport.Registry, one layer up: a pool is to a
// provider's credentials what a client (with its connection pool) is to a
// transport endpoint.
//
// Eviction needs no deferred teardown and no instance bookkeeping, unlike
// the transport registry: a Pool is pure state with nothing to close and
// nothing to revoke. Dropping the map entry can never break a request that
// already holds the *Pool — the pointer stays alive until the request
// finishes, then the state is simply garbage. A request that resolves a
// pool for a spec the CURRENT registry no longer retains (pinned before a
// reload that removed it) gets a fresh pool through the same get-or-create
// a transport-resolving request gets a fresh client: it finishes its walk
// under the configuration it pinned, cold where that configuration has
// been retired.
type Registry struct {
	mu    sync.Mutex
	pools map[string]*Pool
}

// NewRegistry returns an empty registry.
func NewRegistry() *Registry {
	return &Registry{pools: make(map[string]*Pool)}
}

// Pool returns the rotation state for the provider's credential
// configuration, creating it on first use. A request resolves its pool once
// per candidate and holds the returned value for the candidate's whole
// walk, so an in-flight request keeps its rotation and cooldown state even
// across a reload that evicts or rewrites the entry. The spec must have
// passed ValidateSpec (NewPool panics otherwise; unreachable through the
// loader).
func (r *Registry) Pool(p *Provider) *Pool {
	k := p.ContentKey()
	r.mu.Lock()
	defer r.mu.Unlock()
	if pool, ok := r.pools[k]; ok {
		return pool
	}
	pool := NewPool(p.Spec)
	r.pools[k] = pool
	return pool
}

// Retain keeps entries only for the listed providers and drops the rest: a
// reload that removes or rewrites a provider's credentials does not leave
// its pool behind. In-flight requests holding an evicted pool are untouched
// — see the type comment. The poller's onPublish hook calls this with the
// new snapshot's credential set.
func (r *Registry) Retain(active []*Provider) {
	keys := make(map[string]struct{}, len(active))
	for _, p := range active {
		keys[p.ContentKey()] = struct{}{}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for k := range r.pools {
		if _, ok := keys[k]; !ok {
			delete(r.pools, k)
		}
	}
}
