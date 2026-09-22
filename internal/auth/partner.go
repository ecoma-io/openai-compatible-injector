package auth

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"database/sql/driver"
	"encoding/hex"
	"errors"
	"net"
	"time"

	"openai-compatible-injector/internal/config"
)

// PartnerProvider resolves tokens against the key store, fronted by the
// bounded decision cache. It is deliberately snapshot-independent: unlike
// static mode, where a reload swaps the key, partner credentials are
// security state in PostgreSQL, and their lifecycle (create, revoke) is
// governed by the cache TTLs — documented in cache.go — never by config
// immutability.
type PartnerProvider struct {
	store KeyStore
	cache *decisionCache
}

// NewPartnerProvider composes the store-backed authenticator. The cache is
// always on: its capacity is the memory bound against token-spraying, its
// TTLs are the documented revocation/creation propagation bounds.
func NewPartnerProvider(store KeyStore) *PartnerProvider {
	return &PartnerProvider{store: store, cache: newDecisionCache(defaultCacheCap, nil)}
}

// For implements Provider. The snapshot parameter is accepted — every
// request binds to exactly one — but partner identity does not vary with
// config, so the same authenticator serves every snapshot.
func (p *PartnerProvider) For(*config.Snapshot) Authenticator { return p }

// Authenticate implements Authenticator. Fail-closed shape: every path that
// is not an affirmative lookup returns a non-OK reason with a nil error
// (definitive negative, cacheable) or a non-nil error (backend failure,
// never cached) — the handler denies on both, identically on the wire.
func (p *PartnerProvider) Authenticate(ctx context.Context, token string) (Principal, Reason, error) {
	// A malformed token never reaches store or cache: same static denial
	// as unknown, none of the cost.
	if !ValidTokenFormat(token) {
		return Principal{}, ReasonUnknown, nil
	}
	digest := sha256.Sum256([]byte(token))
	hashHex := hex.EncodeToString(digest[:])

	if principal, reason, ok := p.cache.get(hashHex); ok {
		return principal, reason, nil
	}

	rec, err := p.store.LookupByHash(ctx, digest[:])
	switch {
	case err == nil:
		// fall through to the status check below
	case errors.Is(err, ErrKeyNotFound):
		principal := Principal{}
		p.cache.put(hashHex, principal, ReasonUnknown)
		return principal, ReasonUnknown, nil
	default:
		// Backend failure: deny, log-worthy, and never cached — a store
		// outage must not outlive itself through the cache.
		return Principal{}, ReasonBackend, err
	}

	switch rec.Status {
	case StatusActive:
		principal := Principal{PartnerID: rec.PartnerID, KeyID: rec.KeyID}
		p.cache.put(hashHex, principal, ReasonOK)
		p.store.TouchLastUsed(rec.KeyID, time.Now())
		return principal, ReasonOK, nil
	case StatusRevoked:
		p.cache.put(hashHex, Principal{}, ReasonRevoked)
		return Principal{}, ReasonRevoked, nil
	default:
		// A status the CHECK constraint forbids anyway; treated as a
		// definitive negative rather than trusted.
		p.cache.put(hashHex, Principal{}, ReasonUnknown)
		return Principal{}, ReasonUnknown, nil
	}
}

// StoreErrorClass renders an authentication backend failure as a stable
// snake_case token for the auth_backend_failed event. The raw error never
// rides the log event: driver text can embed infrastructure detail, and the
// credential rule is level-independent.
func StoreErrorClass(err error) string {
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	case errors.Is(err, context.Canceled):
		return "canceled"
	case errors.Is(err, sql.ErrConnDone) || errors.Is(err, driver.ErrBadConn):
		return "unavailable"
	default:
		var netErr net.Error
		if errors.As(err, &netErr) {
			return "unavailable"
		}
		return "query_failed"
	}
}
