// Package auth implements client identity for the injector: who is calling,
// resolved from the presented bearer token. Authentication produces a
// Principal — an identity, not a boolean — so consumption can be attributed
// to a partner and a specific key, and so a single caller's key can be
// revoked without touching anyone else's.
//
// Two modes share one seam. Static mode is the historical shared api-key
// from the runtime YAML (still the default when no auth database is
// configured); partner mode resolves keys from the PostgreSQL key store,
// with a bounded cache in front of it. The handler never compares tokens
// itself: it asks the Provider for the request's Authenticator — bound to
// the request's snapshot in static mode, snapshot-independent in partner
// mode, because revocation is security state governed by the documented
// cache TTLs, not by config immutability.
package auth

import (
	"context"
	"crypto/subtle"

	"openai-compatible-injector/internal/config"
)

// Principal is the authenticated identity of a caller. In static mode the
// zero value is the success result: one shared key, no per-caller identity.
// In partner mode both fields are set.
type Principal struct {
	// PartnerID names the caller's organization (opaque to this service).
	PartnerID string
	// KeyID names the specific credential presented. It is a public
	// identifier (the secret is the token itself) and is safe to log.
	KeyID string
}

// Reason distinguishes authentication outcomes beyond success. It exists
// for logs and metrics only — the wire response is always the same static
// 401 body, because telling a caller WHY a key was rejected (unknown vs
// revoked) is enumeration material.
type Reason int

const (
	// ReasonOK means the token authenticated.
	ReasonOK Reason = iota
	// ReasonUnknown means no key matches the presented token (or the
	// presented credential is malformed — the handler has already
	// classified those separately).
	ReasonUnknown
	// ReasonRevoked means the key exists but is no longer active.
	ReasonRevoked
	// ReasonBackend means the credential store could not be reached or
	// answered with an error. The request is denied — fail closed — but
	// the denial is an infrastructure failure, not a key judgment, and is
	// never cached.
	ReasonBackend
)

// String is the log token for a Reason. Stable snake_case, like every
// other structured field value this service emits.
func (r Reason) String() string {
	switch r {
	case ReasonOK:
		return "ok"
	case ReasonUnknown:
		return "unknown_key"
	case ReasonRevoked:
		return "revoked_key"
	case ReasonBackend:
		return "backend_unavailable"
	default:
		return "unknown_reason"
	}
}

// Authenticator resolves a presented bearer token to an identity. A nil
// error with a non-OK Reason is a definitive negative answer (unknown,
// revoked); a non-nil error is a backend failure — the caller must deny
// the request either way, but the two are accounted differently.
type Authenticator interface {
	Authenticate(ctx context.Context, token string) (Principal, Reason, error)
}

// Provider hands out the Authenticator a request must use, bound to the
// request's config snapshot. Every request calls For exactly once with the
// snapshot it loaded — one snapshot per request, auth included.
type Provider interface {
	For(snap *config.Snapshot) Authenticator
}

// StaticProvider is the default mode: one shared key from the runtime
// YAML. Authentication is a constant-time comparison against the
// snapshot's own key, so a key rotation lands on the next request after
// the reload — the snapshot the request bound to decides.
type StaticProvider struct{}

// For returns the snapshot-bound authenticator.
func (StaticProvider) For(snap *config.Snapshot) Authenticator {
	return staticAuthenticator{configured: snap.APIKey()}
}

// staticAuthenticator compares the presented token against one configured
// key. The comparison is constant-time: both byte slices are padded to the
// same fixed cap so the comparison exposes neither a mismatch position nor
// the configured key's length.
type staticAuthenticator struct{ configured string }

// maxTokenBytes bounds the credential material held per comparison. The
// permitted b64token syntax bounds real tokens far below this; the cap
// exists so the padded buffers are fixed-size and the comparison time
// carries no length signal.
const maxTokenBytes = 4 << 10

// Authenticate implements Authenticator. The context is accepted for seam
// parity; the static comparison does no I/O and cannot be cancelled.
func (a staticAuthenticator) Authenticate(_ context.Context, token string) (Principal, Reason, error) {
	if tokenMatches(token, a.configured) {
		// The zero Principal is the shared identity: static mode has no
		// per-caller attribution to report.
		return Principal{}, ReasonOK, nil
	}
	return Principal{}, ReasonUnknown, nil
}

// tokenMatches compares two tokens in constant time. Their permitted b64token
// syntax bounds their length; pad both byte slices to the same fixed cap so
// the constant-time comparison exposes neither a mismatch position nor the
// configured token's length.
func tokenMatches(presented, configured string) bool {
	if configured == "" || len(presented) > maxTokenBytes || len(configured) > maxTokenBytes {
		// No snapshot carries an empty key (LoadRuntime rejects it); fail
		// closed anyway rather than ever match an empty presentation.
		return false
	}
	var presentedBuf, configuredBuf [maxTokenBytes]byte
	copy(presentedBuf[:], presented)
	copy(configuredBuf[:], configured)
	return subtle.ConstantTimeCompare(presentedBuf[:], configuredBuf[:]) == 1 &&
		subtle.ConstantTimeEq(int32(len(presented)), int32(len(configured))) == 1
}
