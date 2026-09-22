package transport

import (
	"sync"
	"time"
)

// Registry owns the process's long-lived transport clients, keyed by
// validated Config content. Doer is a get-or-create: the first request for
// a transport builds its client — with its connection pool — and every
// later request for the same config reuses it, across config reloads, so
// pools survive unchanged reloads warm and no per-request client or pool
// is ever built. Entries are bounded by configuration, not traffic.
//
// EgressPool configs add a second layer: per-identity pool state (scheduler
// position, health, concurrency permits, in-flight leases) shared by every
// poolDoer of the same identity, so an unchanged pool keeps its warm state
// across reloads while a changed policy starts fresh. Member endpoint
// clients resolve through the same content-keyed map, so a pool member and
// a standalone transport pointing at the same endpoint share one client.
type Registry struct {
	mu    sync.Mutex
	doers map[string]Doer
	pools map[string]*poolState
}

// NewRegistry returns an empty registry.
func NewRegistry() *Registry {
	return &Registry{doers: make(map[string]Doer), pools: make(map[string]*poolState)}
}

// Doer returns the Doer for the config, creating it on first use. A request
// resolves its Doer once and holds the returned value for its whole
// lifetime, so an in-flight request (or stream) keeps its pool even across
// a reload that evicts the entry.
func (r *Registry) Doer(c Config) Doer {
	_ = c.Key() // panics on a config missing its URL/policy: unreachable via validation
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.doerLocked(c)
}

// doerLocked is Doer with r.mu already held (pool creation resolves member
// endpoint clients on the same lock).
func (r *Registry) doerLocked(c Config) Doer {
	k := c.Key()
	if d, ok := r.doers[k]; ok {
		return d
	}
	var d Doer
	switch c.Kind {
	case Direct:
		d = NewDirectClient()
	case Proxy:
		d = newProxyClient(c.ProxyURL)
	case EgressPool:
		d = r.newPoolDoerLocked(c.Pool)
	default:
		panic("transport: unknown kind") // unreachable: Kind is set by validation alone
	}
	r.doers[k] = d
	return d
}

// newPoolDoerLocked builds the pool's Doer against the shared per-identity
// state, creating that state (and every member's endpoint client) on first
// use. r.mu must be held.
func (r *Registry) newPoolDoerLocked(p *Pool) *poolDoer {
	id := p.Identity()
	st, ok := r.pools[id]
	if !ok {
		st = r.newPoolStateLocked(p)
		r.pools[id] = st
	}
	return &poolDoer{pool: p, st: st}
}

func (r *Registry) newPoolStateLocked(p *Pool) *poolState {
	threshold := p.Health.FailureThreshold
	if !p.Health.Enabled {
		threshold = 0
	}
	clock := time.Now
	st := &poolState{
		pool:    p,
		key:     "pool " + p.Identity(),
		members: make([]memberState, len(p.Members)),
		clock:   clock,
		cw:      make([]int, len(p.Members)),
	}
	for i, m := range p.Members {
		st.members[i] = memberState{
			client: r.doerLocked(m.Endpoint),
			health: &endpointHealth{
				threshold: threshold,
				cooldown:  p.Health.Cooldown,
				clock:     clock,
			},
			lim: &limiter{max: m.MaxConcurrency},
		}
	}
	return st
}

// Retain keeps entries only for the listed configs and evicts the rest:
// closing their idle pooled connections and dropping the entries, so a
// reload that removes or rewrites a transport does not leave its pool
// behind. The active set is the snapshot's egress closure — every plain
// transport, every pool, and every pool's member endpoints.
//
// Eviction is safe for in-flight work: CloseIdleConnections touches only
// idle connections, and a pool state still leased by an executing request
// defers its teardown to the lease's release (the request keeps executing
// on the old state; the teardown then closes what is by then idle). The
// poller's onPublish hook calls this with the new snapshot's transport set.
func (r *Registry) Retain(active []Config) {
	keys := make(map[string]struct{}, len(active))
	ids := make(map[string]struct{}, len(active))
	for _, c := range active {
		keys[c.Key()] = struct{}{}
		if c.Kind == EgressPool {
			ids[c.Pool.Identity()] = struct{}{}
		}
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
	for id, st := range r.pools {
		if _, ok := ids[id]; ok {
			continue
		}
		r.retirePoolLocked(id, st)
	}
}

// retirePoolLocked drops one pool's entries unless a lease still holds it —
// then retirement is marked and deferred to the lease's last release.
// r.mu must be held.
func (r *Registry) retirePoolLocked(id string, st *poolState) {
	st.mu.Lock()
	if st.leases > 0 {
		if !st.retired {
			st.retired = true
			st.onRetire = func() { r.retirePool(id, st) }
		}
		st.mu.Unlock()
		return
	}
	st.mu.Unlock()
	r.finishRetireLocked(id, st)
}

// retirePool is the deferred teardown the lease release fires (no locks
// held by the caller).
func (r *Registry) retirePool(id string, st *poolState) {
	r.mu.Lock()
	r.finishRetireLocked(id, st)
	r.mu.Unlock()
}

// finishRetireLocked closes the pool doer's and every member's idle
// connections and drops both map entries. r.mu must be held. members is
// immutable after construction, so reading it needs only r.mu's protection
// of the map entries.
func (r *Registry) finishRetireLocked(id string, st *poolState) {
	if d, ok := r.doers[st.key]; ok {
		closeIdle(d)
		delete(r.doers, st.key)
	}
	delete(r.pools, id)
	for i := range st.members {
		closeIdle(st.members[i].client)
	}
}

// CloseIdleConnections closes every current doer's idle pooled
// connections — the shutdown-time drain the server calls before exiting.
// Pool doers forward into their members, so pooled endpoints drain too.
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
