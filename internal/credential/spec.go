// Package credential owns per-provider upstream credential configuration and
// rotation state: which API key the next attempt of a provider carries, and
// how long a rate-limited key stays out of rotation.
//
// The package is deliberately free of I/O and of HTTP. It never dials, never
// sleeps, never retries a request, and never increments a recovery counter —
// the recovery engine remains the only retry/fallback/terminal authority and
// the transport layer never learns that a key exists. A credential is part of
// the outgoing request ABOVE the transport seam, so egress fallback inside
// one attempt keeps the same key by construction.
//
// Secrets discipline: a Key.Value is upstream provider secret material. It is
// carried in memory only, applied to the outgoing request header by the
// proxy, and never appears in an error, a log field, or any package here.
// Key identifiers are bounded to a fixed safe charset (ValidateID) so an
// operator-chosen id can never become a log-injection vector.
package credential

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
)

// Strategy names how a pool walks its ready keys. Phase one ships exactly
// one strategy; anything else is a config rejection, never a fallback to a
// default the file did not state.
type Strategy string

// StrategyRoundRobin walks the ready keys in configuration order, resuming
// after the key last handed out. It is the zero-worst default and the only
// accepted value.
const StrategyRoundRobin Strategy = "round_robin"

// parseStrategy validates the `strategy` field's spelling. The error text is
// a fixed literal: it names the accepted value, never the input.
func parseStrategy(s string) (Strategy, error) {
	if Strategy(s) == StrategyRoundRobin {
		return StrategyRoundRobin, nil
	}
	return "", fmt.Errorf("credential: unknown auth strategy %q (accepted: %q)", redact(s), StrategyRoundRobin)
}

// redact reduces an operator-supplied spelling to a bounded marker so no
// validation path can echo file bytes into an error. Values land here only
// from the config loader, which already rejects at the line level; this is
// defense in depth for anything that calls the package validators directly.
func redact(string) string {
	return "(input redacted)"
}

// Bounds for the auth block's string fields. A value over its bound rejects
// the whole file; the rejection names the bound, never the input.
const (
	// MaxHeaderBytes bounds the credential header's field name (RFC 7230
	// field-name tokens; 128 covers every real header many times over).
	MaxHeaderBytes = 128
	// MaxPrefixBytes bounds the prefix placed before the key value in the
	// header value ("Bearer " is 7 bytes).
	MaxPrefixBytes = 128
	// MaxIDBytes bounds an operator-chosen key id.
	MaxIDBytes = 64
	// MaxValueBytes bounds one key's secret material — the same order as the
	// client api-key's bearer-token bound.
	MaxValueBytes = 4096
	// MaxKeys bounds one pool's key list. Rotation state is O(keys) memory,
	// so the file states the ceiling instead of trusting the file size.
	MaxKeys = 256
)

// Key is one upstream credential. Value is secret material: memory-only,
// never logged, never embedded in an error, and never carried anywhere but
// the outgoing request header the proxy sets.
type Key struct {
	ID    string
	Value string
}

// Spec is the immutable, validated per-provider credential configuration. It
// is built once at config load, bound to a request through its snapshot, and
// never mutated afterwards — rotation state lives in the Pool, not here.
type Spec struct {
	// Header is the RFC 7230 field-name the credential is sent in
	// ("Authorization", "X-API-Key", ...).
	Header string
	// Prefix is placed before the key value in the header value
	// ("Bearer " or "").
	Prefix string
	// Strategy selects the rotation walk over ready keys.
	Strategy Strategy
	// Keys are the pool's credentials, in configuration order.
	Keys []Key
}

// ContentKey returns a memory-only identity for the spec's exact content
// (header, prefix, strategy, and every key id and value, in order). The
// registry uses it to key pool instances the way the transport registry keys
// clients: unchanged content across a reload keeps its runtime state, changed
// content starts fresh. The digest never reaches a log — the key VALUES are
// hashed into it.
func (s Spec) ContentKey() string {
	h := sha256.New()
	writeField := func(v string) {
		// Length-prefix so field boundaries survive concatenation.
		h.Write([]byte(strconv.Itoa(len(v))))
		h.Write([]byte{0})
		h.Write([]byte(v))
	}
	writeField(s.Header)
	writeField(s.Prefix)
	writeField(string(s.Strategy))
	writeField(strconv.Itoa(len(s.Keys)))
	for _, k := range s.Keys {
		writeField(k.ID)
		writeField(k.Value)
	}
	return hex.EncodeToString(h.Sum(nil))
}

// ErrInvalidCredentialConfig marks every auth-block validation failure. The
// config loader maps position information onto it; the messages below are
// fixed literals that never carry operator input.
var ErrInvalidCredentialConfig = errors.New("invalid provider auth configuration")

func invalid(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalidCredentialConfig, fmt.Sprintf(format, args...))
}

// ValidHeader enforces the credential header's field-name shape: RFC 7230
// token characters, non-empty, bounded. The error is a fixed literal.
func ValidHeader(h string) error {
	if h == "" {
		return invalid("auth header must not be empty")
	}
	if len(h) > MaxHeaderBytes {
		return invalid("auth header must be at most %d bytes", MaxHeaderBytes)
	}
	for i := 0; i < len(h); i++ {
		if !isTokenByte(h[i]) {
			return invalid("auth header must contain only RFC 7230 token characters")
		}
	}
	return nil
}

// ValidPrefix enforces the prefix's shape: bounded, and free of control
// characters (a header value must never smuggle a CR/LF). Empty is valid.
func ValidPrefix(p string) error {
	if len(p) > MaxPrefixBytes {
		return invalid("auth prefix must be at most %d bytes", MaxPrefixBytes)
	}
	for i := 0; i < len(p); i++ {
		if p[i] < 0x20 || p[i] == 0x7f {
			return invalid("auth prefix must not contain control characters")
		}
	}
	return nil
}

// ValidID enforces an operator-chosen key id: non-empty, bounded, and drawn
// from a fixed safe charset ([A-Za-z0-9._:-]) so the id can travel through
// JSON log lines and never read as anything but a name. The error is a fixed
// literal that never quotes the id.
func ValidID(id string) error {
	if id == "" {
		return invalid("credential key id must not be empty")
	}
	if len(id) > MaxIDBytes {
		return invalid("credential key id must be at most %d bytes", MaxIDBytes)
	}
	for i := 0; i < len(id); i++ {
		b := id[i]
		switch {
		case b >= 'a' && b <= 'z', b >= 'A' && b <= 'Z', b >= '0' && b <= '9':
		case b == '.' || b == '_' || b == '-' || b == ':':
		default:
			return invalid("credential key id must contain only [A-Za-z0-9._:-]")
		}
	}
	return nil
}

// ValidValue enforces one key's secret material: non-empty and bounded. The
// error is a fixed literal; the length is named, the value never is.
func ValidValue(v string) error {
	if v == "" {
		return invalid("credential key value must not be empty")
	}
	if len(v) > MaxValueBytes {
		return invalid("credential key value must be at most %d bytes", MaxValueBytes)
	}
	for i := 0; i < len(v); i++ {
		if v[i] < 0x20 || v[i] == 0x7f {
			return invalid("credential key value must not contain control characters")
		}
	}
	return nil
}

// ValidateSpec enforces the whole block's internal consistency: exactly the
// values ParseStrategy/Valid* accept, at least one key, no duplicate ids
// (ids are rotation and log identity), and the key-count bound.
func ValidateSpec(s Spec) error {
	if err := ValidHeader(s.Header); err != nil {
		return err
	}
	if err := ValidPrefix(s.Prefix); err != nil {
		return err
	}
	if s.Strategy != StrategyRoundRobin {
		return invalid("auth strategy must be %q", StrategyRoundRobin)
	}
	if len(s.Keys) == 0 {
		return invalid("auth must declare at least one key")
	}
	if len(s.Keys) > MaxKeys {
		return invalid("auth must declare at most %d keys", MaxKeys)
	}
	seen := make(map[string]struct{}, len(s.Keys))
	for _, k := range s.Keys {
		if err := ValidID(k.ID); err != nil {
			return err
		}
		if err := ValidValue(k.Value); err != nil {
			return err
		}
		if _, dup := seen[k.ID]; dup {
			return invalid("credential key ids must be unique within a provider")
		}
		seen[k.ID] = struct{}{}
	}
	return nil
}

// isTokenByte reports whether b may appear in an RFC 7230 field-name token:
// tchar = "!" / "#" / "$" / "%" / "&" / "'" / "*" / "+" / "-" / "." /
// "^" / "_" / "`" / "|" / "~" / DIGIT / ALPHA.
func isTokenByte(b byte) bool {
	switch {
	case b >= 'a' && b <= 'z', b >= 'A' && b <= 'Z', b >= '0' && b <= '9':
		return true
	}
	switch b {
	case '!', '#', '$', '%', '&', '\'', '*', '+', '-', '.', '^', '_', '`', '|', '~':
		return true
	}
	return false
}
