package credential

import "time"

// RateLimit is one pool's 429-cooldown policy: how long a key that received
// an upstream 429 stays out of rotation. It is ACQUISITION policy, not
// recovery policy — the recovery engine never reads it, and the engine's
// retry/fallback decisions are untouched by every value here. The loader
// fills both fields (stated value or default), so the zero value never
// reaches the proxy.
type RateLimit struct {
	// Cooldown is the mark duration applied when the 429 carried no usable
	// Retry-After directive (or the proxy could not read one).
	Cooldown time.Duration
	// MaxCooldown is the ceiling applied to a Retry-After-derived cooldown:
	// an upstream asking for an hour does not hold a key out for an hour.
	MaxCooldown time.Duration
}

// Provider couples a validated credential spec with its cooldown policy —
// everything the proxy needs to acquire keys for one provider and to cool
// them down. It is built once at config load, frozen onto the snapshot
// through Candidate.Cred, and never mutated afterwards. A nil *Provider on
// a candidate means the provider takes no upstream credential and every
// request byte stays exactly as it was before this feature existed.
type Provider struct {
	Spec      Spec
	RateLimit RateLimit
}

// ContentKey is the pool-identity the registry keys on: the spec's content
// digest. Two providers whose credentials are byte-identical share one pool
// (and one rotation cursor) deliberately — they are the same upstream
// accounts.
func (p *Provider) ContentKey() string { return p.Spec.ContentKey() }
