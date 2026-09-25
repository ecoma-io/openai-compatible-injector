package config

import (
	"errors"
	"fmt"
	"time"

	"openai-compatible-injector/internal/credential"
)

// The provider `auth` block: per-provider upstream credential configuration.
// Its shape and the discipline of its rejections mirror the rest of the
// providers table — every value is operator input and error text reaches
// logs verbatim, so a rejection names the POSITION and the violated RULE,
// never the input. Key values are upstream secret material: they appear in
// no error, no log, and no snapshot accessor.

// runtimeAuth mirrors one provider's `auth` block. Present means the
// provider requires an upstream credential; absent means the request bytes
// are untouched (the default for every deployment that does not state it).
type runtimeAuth struct {
	// Type is the credential scheme. Phase one accepts exactly api_key —
	// an explicit discriminator, so a future scheme (oauth, query param)
	// can never silently parse as the one that exists today.
	Type string `yaml:"type"`
	// Header is the RFC 7230 field-name the credential is sent in
	// ("Authorization", "X-API-Key", ...).
	Header string `yaml:"header"`
	// Prefix is placed before the key value in the header value
	// ("Bearer " or ""). Empty is legal.
	Prefix string `yaml:"prefix"`
	// Strategy selects how the pool walks ready keys. Required and
	// explicitly stated: a file never inherits a rotation strategy it did
	// not name, so a future default swap cannot silently change deployed
	// files.
	Strategy string `yaml:"strategy"`
	// Keys is the pool. At least one; ids unique; values bounded.
	Keys []runtimeCredentialKey `yaml:"keys"`
	// RateLimit is the optional 429-cooldown policy for the pool.
	RateLimit *runtimeAuthRateLimit `yaml:"rate-limit"`
}

// runtimeCredentialKey mirrors one `auth.keys` entry.
type runtimeCredentialKey struct {
	ID    string `yaml:"id"`
	Value string `yaml:"value"`
}

// runtimeAuthRateLimit mirrors the `auth.rate-limit` block.
type runtimeAuthRateLimit struct {
	// Cooldown is the mark duration when a 429 carries no usable
	// Retry-After directive.
	Cooldown string `yaml:"cooldown"`
	// MaxCooldown is the ceiling on a Retry-After-derived cooldown.
	MaxCooldown string `yaml:"max-cooldown"`
}

// authTypeAPIKey is the one accepted credential scheme.
const authTypeAPIKey = "api_key"

// The cooldown defaults and their ceiling. The default mark sits at the
// default backoff ceiling (recovery.DefaultBackoffMax): under the shipped
// policy a 429's re-ask waits at most that long, so a marked key is out of
// rotation for at least one re-ask — which is exactly what makes the retry
// land on the next account instead of the same one. The ceiling exists so a
// hostile or broken Retry-After cannot hollow out a pool for an hour; a
// stated value above it rejects the file rather than being silently
// clamped, because a clamped value would make the file lie about the
// behavior it asked for.
const (
	defaultAuthCooldown    = 2 * time.Second
	defaultAuthMaxCooldown = time.Minute
	maxAuthCooldownCap     = 2 * time.Minute
)

// buildAuth translates and validates one provider's `auth` block. A nil
// block yields (nil, nil) — no credential, unchanged request bytes — and a
// malformed block rejects the whole file with a position-prefixed,
// fixed-text error. name is the entry's trimmed providers-table name and
// becomes the credential's Identity: rotation state belongs to the
// provider that declared the block, so the name takes part in the pool key
// and two providers with byte-identical auth stay independent. The key
// VALUES are validated by the credential domain and never echoed anywhere,
// including here.
func buildAuth(ra *runtimeAuth, name string, ordinal int) (*credential.Provider, error) {
	if ra == nil {
		return nil, nil
	}
	prefix := fmt.Sprintf("provider entry %d: auth", ordinal)
	if ra.Type != authTypeAPIKey {
		return nil, fmt.Errorf("%s: type must be %q", prefix, authTypeAPIKey)
	}
	spec := credential.Spec{
		Header:   ra.Header,
		Prefix:   ra.Prefix,
		Strategy: credential.Strategy(ra.Strategy),
	}
	if len(ra.Keys) > 0 {
		spec.Keys = make([]credential.Key, 0, len(ra.Keys))
		for _, rk := range ra.Keys {
			spec.Keys = append(spec.Keys, credential.Key{ID: rk.ID, Value: rk.Value})
		}
	}
	if err := credential.ValidateSpec(spec); err != nil {
		return nil, fmt.Errorf("%s: %w", prefix, err)
	}
	rl := credential.RateLimit{
		Cooldown:    defaultAuthCooldown,
		MaxCooldown: defaultAuthMaxCooldown,
	}
	if ra.RateLimit != nil {
		if ra.RateLimit.Cooldown != "" {
			d, err := parseRecoveryDuration(ra.RateLimit.Cooldown, prefix+" rate-limit cooldown")
			if err != nil {
				return nil, err
			}
			if d <= 0 {
				return nil, errors.New(prefix + " rate-limit cooldown must be positive")
			}
			rl.Cooldown = d
		}
		if ra.RateLimit.MaxCooldown != "" {
			d, err := parseRecoveryDuration(ra.RateLimit.MaxCooldown, prefix+" rate-limit max-cooldown")
			if err != nil {
				return nil, err
			}
			if d <= 0 {
				return nil, errors.New(prefix + " rate-limit max-cooldown must be positive")
			}
			if d > maxAuthCooldownCap {
				return nil, fmt.Errorf("%s rate-limit max-cooldown must be at most %s", prefix, maxAuthCooldownCap)
			}
			rl.MaxCooldown = d
		}
		if rl.Cooldown > rl.MaxCooldown {
			return nil, fmt.Errorf("%s rate-limit cooldown must not exceed the effective max-cooldown (configured %s, default %s)", prefix, rl.Cooldown, maxAuthCooldownCap)
		}
	}
	return &credential.Provider{Identity: name, Spec: spec, RateLimit: rl}, nil
}
