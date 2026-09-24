package credential

import (
	"sync"
	"time"
)

// Pool is one provider's credential rotation state: a round-robin cursor over
// the spec's keys plus per-key cooldown deadlines. It is runtime state keyed
// by the spec's content (see Registry) and shared by every request whose
// snapshot carries the same credential configuration — rotation is a
// process-wide property, exactly like transport pool scheduler position.
//
// The pool is deliberately inert: Acquire reads state and moves a cursor,
// MarkRateLimited writes a deadline, NextReady reads the earliest deadline.
// It performs no I/O, spawns no goroutine, holds no timer, and never waits —
// expiry is evaluated lazily against the caller's clock, so cooldown
// bookkeeping costs one map lookup per key examined and costs nothing when
// every key is ready. Concurrency is one mutex over the whole pool; no
// network I/O or callback ever runs under it.
//
// A Pool is safe for concurrent use.
type Pool struct {
	spec Spec

	mu      sync.Mutex
	cursor  int
	cooling map[string]time.Time // key id -> ready-at deadline
}

// NewPool builds the rotation state for a validated spec. The spec must have
// passed ValidateSpec (at least one key); the pool copies it, so the caller's
// Spec is never aliased.
func NewPool(spec Spec) *Pool {
	if len(spec.Keys) == 0 {
		// Unreachable through the config loader (ValidateSpec rejects an
		// empty key list); a static panic keeps the invariant honest without
		// inventing an error path callers would have to handle.
		panic("credential: pool requires at least one key")
	}
	keys := make([]Key, len(spec.Keys))
	copy(keys, spec.Keys)
	spec.Keys = keys
	return &Pool{
		spec:    spec,
		cooling: make(map[string]time.Time),
	}
}

// Spec returns the immutable configuration this pool rotates over.
func (p *Pool) Spec() Spec {
	return p.spec
}

// Acquire returns the credential the next attempt carries.
//
// preferred names the key the caller's previous attempt on this candidate
// used, or "" on a first attempt. A preferred key that is ready is handed
// back unchanged — that is what makes a 500/408/425/5xx/protocol retry keep
// its key, and it deliberately does not move the rotation cursor (a retry is
// the same logical attempt, not a rotation). A preferred key that is cooling
// is skipped like any other cooling key — that is what makes the re-ask
// after a 429 land on the next ready account. With no preference, the walk
// resumes at the pool's cursor over configuration order, skipping keys in
// cooldown, and the cursor advances past the key it hands out so concurrent
// requests rotate instead of stampeding one account.
//
// ok is false when every key is cooling; the pool then reports state through
// NextReady and the CALLER's recovery policy decides what happens next. The
// pool itself never waits and never retries.
func (p *Pool) Acquire(now time.Time, preferred string) (Key, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()

	n := len(p.spec.Keys)
	if preferred != "" {
		for _, k := range p.spec.Keys {
			if k.ID == preferred && p.readyLocked(k.ID, now) {
				return k, true
			}
		}
	}
	for off := 0; off < n; off++ {
		idx := (p.cursor + off) % n
		k := p.spec.Keys[idx]
		if p.readyLocked(k.ID, now) {
			p.cursor = (idx + 1) % n
			return k, true
		}
	}
	return Key{}, false
}

// MarkRateLimited puts a key into cooldown until now+cooldown. The proxy
// calls it on every path that observed an upstream 429 for the key — the
// mark describes the UPSTREAM's view of the account, so it happens even when
// the request itself goes on to be cancelled or its error body cannot be
// read. A non-positive cooldown is a no-op (a rate limit that expires before
// now is not a rate limit). Marking an unknown id is a no-op: an in-flight
// request's pool is pinned, so under normal operation the id exists, and a
// defensive no-op beats inventing an error the walk would have to handle.
func (p *Pool) MarkRateLimited(now time.Time, keyID string, cooldown time.Duration) {
	if cooldown <= 0 {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, k := range p.spec.Keys {
		if k.ID == keyID {
			p.cooling[keyID] = now.Add(cooldown)
			return
		}
	}
}

// NextReady reports the earliest time a currently-cooling key becomes ready.
// ok is false when nothing is cooling (every key is ready) — the distinction
// matters to the caller: false means an Acquire failure cannot be fixed by
// waiting, true hands the recovery policy a deadline it may wait for within
// its existing windows.
func (p *Pool) NextReady(now time.Time) (time.Time, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	var earliest time.Time
	found := false
	for _, until := range p.cooling {
		if !now.Before(until) {
			continue // already expired; lazy expiry, nothing to delete
		}
		if !found || until.Before(earliest) {
			earliest, found = until, true
		}
	}
	return earliest, found
}

// readyLocked reports whether a key is out of cooldown at now. Expired
// deadlines are left in place (the map is bounded by the key count; lazily
// ignoring them is cheaper than pruning and cannot race a concurrent read).
func (p *Pool) readyLocked(keyID string, now time.Time) bool {
	until, cooling := p.cooling[keyID]
	return !cooling || !now.Before(until)
}
