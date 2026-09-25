package proxy

import (
	"time"

	"openai-compatible-injector/internal/credential"
)

// CredentialResolver is the handler's seam to the process's credential
// pools — transport.Resolver one layer up. The handler resolves a
// credential-bearing candidate's pool once per candidate, before the
// candidate's first attempt, and holds the returned *Pool for the
// candidate's whole walk, so rotation and cooldown state is never rebuilt
// mid-request even across a reload. A nil resolver — or a candidate whose
// configuration carries no auth block — disables the seam entirely: such
// requests run the historical no-credential path byte for byte.
//
// The resolver hands out rotation STATE, never credential material to the
// wire machinery: the pool returns a whole Key at Acquire and the header
// value is composed in the handler alone, onto the request the transport
// is already about to send. Nothing credential-shaped reaches the
// transport layer — pools live beside the transports, never inside them.
type CredentialResolver interface {
	Pool(*credential.Provider) *credential.Pool
}

// credentialCooldown folds an upstream Retry-After directive into the
// provider's cooldown policy for a 429 mark. The mark is an account fact
// — the provider said this key is rate-limited — and is taken whatever
// the recovery matrix later does with the request's wait: a policy that
// ignores Retry-After for its own sleeps must not also unmark the key.
// The key cools for the directive when one is usable, capped by the
// provider's max-cooldown (an upstream cannot pin a key out of rotation
// indefinitely by shouting in a header), and for the provider's configured
// default cooldown when no usable directive arrived.
func credentialCooldown(directive time.Duration, rl credential.RateLimit) time.Duration {
	if directive <= 0 {
		return rl.Cooldown
	}
	if directive > rl.MaxCooldown {
		return rl.MaxCooldown
	}
	return directive
}
