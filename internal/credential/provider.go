package credential

import (
	"crypto/sha256"
	"encoding/hex"
	"strconv"
	"time"
)

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
	// Identity is the providers-table name the auth block was declared
	// under. Rotation state belongs to the provider that declared it: two
	// providers configuring byte-identical keys are two independent
	// rotation domains (one provider's 429 cooldown must never stall the
	// other's traffic), so the identity takes part in the pool key below.
	// It is operator-chosen input like any other — the pool key digests it
	// and this package never logs it.
	Identity string
	// Spec is the validated credential block: header surface and key list.
	Spec Spec
	// RateLimit is the resolved 429-cooldown policy for the pool.
	RateLimit RateLimit
}

// PoolKey is the pool identity the Registry keys on: a sha256 over the
// provider's identity, the credential block's content, and the RESOLVED
// rate-limit policy — every axis that shapes rotation state. Identity,
// because two providers with byte-identical credentials are two rotation
// domains, never one; content, because changed keys are different accounts;
// rate-limit policy, because a reload that changes a cooldown must not
// leave state sized by the old policy (an in-flight request keeps the pool
// instance it pinned; new requests get the fresh one). The digest is a map
// key only: it is derived from secret material and never logged, never
// echoed, and never leaves the process.
func (p *Provider) PoolKey() string {
	h := sha256.New()
	write := func(v string) {
		// Length-prefix so field boundaries survive concatenation.
		h.Write([]byte(strconv.Itoa(len(v))))
		h.Write([]byte{0})
		h.Write([]byte(v))
	}
	// A version tag so a future shape change cannot collide with keys built
	// by an older binary in the same process lifetime.
	write("credential-pool-key-v1")
	write(p.Identity)
	write(p.Spec.ContentKey())
	write(strconv.FormatInt(int64(p.RateLimit.Cooldown), 10))
	write(strconv.FormatInt(int64(p.RateLimit.MaxCooldown), 10))
	return hex.EncodeToString(h.Sum(nil))
}
