package auth

import (
	"container/list"
	"sync"
	"time"
)

// Cache tuning. The positive TTL is the documented bound on how long a
// revocation can lag behind its own database write: worst case, a key is
// denied for up to positiveTTL after `keys revoke` returns. The negative
// TTL is the matching bound for newly created keys — a token minted and
// used immediately can hit a cached unknown for at most negativeTTL. Both
// exist so the store sees a bounded request rate, not so it can be
// bypassed: cache entries hold decisions the store already made, never an
// answer the store did not give.
const (
	positiveTTL     = 60 * time.Second
	negativeTTL     = 5 * time.Second
	defaultCacheCap = 4096
)

// PositiveTTL exposes the revocation propagation bound for operator-facing
// text (the keys CLI states it after a successful revoke); it is a fact
// about the cache, not a tuning knob.
func PositiveTTL() time.Duration { return positiveTTL }

// cacheEntry is one decided authentication outcome, carrying the cache key
// it lives under so LRU eviction can remove it from the map without a
// second lookup.
type cacheEntry struct {
	hashHex   string
	principal Principal
	reason    Reason
	expiresAt time.Time
}

// decisionCache is a bounded LRU of authentication decisions keyed by the
// SHA-256 hex of the presented token — the digest, never the token itself,
// so the cache cannot become a second plaintext store. Capacity is the
// memory bound: an attacker spraying random tokens fills the cache and
// evicts real entries instead of growing the process.
type decisionCache struct {
	mu       sync.Mutex
	capacity int
	now      func() time.Time
	entries  map[string]*list.Element
	order    *list.List // front = most recently used
}

func newDecisionCache(capacity int, now func() time.Time) *decisionCache {
	if capacity <= 0 {
		capacity = defaultCacheCap
	}
	if now == nil {
		now = time.Now
	}
	return &decisionCache{
		capacity: capacity,
		now:      now,
		entries:  make(map[string]*list.Element, capacity),
		order:    list.New(),
	}
}

// get returns a cached decision. An expired entry is removed on sight — a
// stale hit would be a revocation (or a new key) living past its bound.
func (c *decisionCache) get(hashHex string) (Principal, Reason, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	el, ok := c.entries[hashHex]
	if !ok {
		return Principal{}, ReasonUnknown, false
	}
	entry := el.Value.(*cacheEntry)
	if !c.now().Before(entry.expiresAt) {
		c.order.Remove(el)
		delete(c.entries, hashHex)
		return Principal{}, ReasonUnknown, false
	}
	c.order.MoveToFront(el)
	return entry.principal, entry.reason, true
}

// put records a definitive decision. Backend failures are refused here —
// they are not judgments about a key, and caching one would turn a
// transient store outage into denial of otherwise-valid callers even after
// the store recovers. Positive decisions get the long TTL; negatives (a
// key is unknown or revoked) expire quickly so both lifecycle transitions
// — create and revoke — land inside their documented windows.
func (c *decisionCache) put(hashHex string, principal Principal, reason Reason) {
	if reason == ReasonBackend {
		return
	}
	ttl := negativeTTL
	if reason == ReasonOK {
		ttl = positiveTTL
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.entries[hashHex]; ok {
		el.Value = &cacheEntry{hashHex: hashHex, principal: principal, reason: reason, expiresAt: c.now().Add(ttl)}
		c.order.MoveToFront(el)
		return
	}
	c.entries[hashHex] = c.order.PushFront(&cacheEntry{
		hashHex:   hashHex,
		principal: principal,
		reason:    reason,
		expiresAt: c.now().Add(ttl),
	})
	for len(c.entries) > c.capacity {
		oldest := c.order.Back()
		if oldest == nil {
			break
		}
		delete(c.entries, oldest.Value.(*cacheEntry).hashHex)
		c.order.Remove(oldest)
	}
}
