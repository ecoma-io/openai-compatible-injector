package auth

import (
	"context"
	"errors"
	"time"
)

// Lifecycle statuses stored in the key table. The CHECK constraint on the
// column is the authority; these mirror it.
const (
	StatusActive  = "active"
	StatusRevoked = "revoked"
)

// ErrKeyNotFound is returned by LookupByHash when no key row carries the
// presented hash. It is a definitive answer, not an error condition: the
// caller maps it to ReasonUnknown, and it is cacheable.
var ErrKeyNotFound = errors.New("no key matches the presented credential")

// KeyRecord is a stored credential's identity and lifecycle state. The
// token hash deliberately lives outside this struct: it is an internal
// implementation detail of storage, and nothing that consumes a record
// (logs, the list CLI, usage metering) has a reason to see it.
type KeyRecord struct {
	KeyID      string
	PartnerID  string
	Status     string
	CreatedAt  time.Time
	RevokedAt  *time.Time
	LastUsedAt *time.Time
}

// KeyStore is the persistence seam behind partner-mode authentication. All
// methods fail closed by their signature: only ErrKeyNotFound is a definitive
// negative, every other error is a backend failure the caller must deny on.
type KeyStore interface {
	// LookupByHash resolves a presented token's digest to its record.
	// ErrKeyNotFound when nothing matches.
	LookupByHash(ctx context.Context, hash []byte) (KeyRecord, error)
	// CreateKey stores a freshly minted credential. The plaintext token is
	// never passed here — only its digest — so no call path in this
	// package can persist or log a secret.
	CreateKey(ctx context.Context, rec KeyRecord, hash []byte) error
	// ListKeys returns every key, newest first, for the management CLI.
	// Hashes are excluded by construction (see KeyRecord).
	ListKeys(ctx context.Context) ([]KeyRecord, error)
	// RevokeKey flips a key to revoked. It reports whether the key existed
	// and was active; revoking an already-revoked key is a no-op, not an
	// error.
	RevokeKey(ctx context.Context, keyID string) (bool, error)
	// TouchLastUsed records a key's latest observed use. Best-effort by
	// contract: implementations may drop touches under load, and a lost
	// touch never affects any request.
	TouchLastUsed(keyID string, at time.Time)
	// Close releases the store's resources, flushing what TouchLastUsed
	// buffered.
	Close(ctx context.Context) error
}
