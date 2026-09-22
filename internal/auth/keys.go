package auth

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"strings"
)

// Partner key material. The token is the secret clients present; the key ID
// is its public name (safe to log, safe to store alongside the hash); the
// hash is the only form of the token that is ever persisted.

const (
	// tokenPrefix makes a presented partner token recognizable on sight —
	// and, more usefully, lets the authenticator reject garbage cheaply
	// before paying a store lookup or a cache entry.
	tokenPrefix = "oaicr_"
	// tokenEntropy is 32 crypto-random bytes: ~256 bits of entropy, so
	// brute force is not a lifecycle concern and a hash on its own cannot
	// be reversed to a token within any relevant horizon.
	tokenEntropy = 32
	// keyIDPrefix names the key ("pak_" = partner API key).
	keyIDPrefix = "pak_"
	// keyIDEntropy is 12 bytes: 16 base64url characters — collision-proof
	// for any realistic key population without wasting column width.
	keyIDEntropy = 12
	// tokenLength is the exact length of a well-formed token:
	// "oaicr_" + base64url.RawURLEncoding of 32 bytes (43 chars, no padding).
	tokenLength = len(tokenPrefix) + 43
)

// GenerateToken mints a new partner token: "oaicr_" + 32 crypto-random
// bytes, base64url without padding. The returned string is the only copy of
// the secret that will ever exist — the caller (the keys CLI) prints it once
// and the store keeps only its SHA-256 digest.
func GenerateToken() (string, error) {
	buf := make([]byte, tokenEntropy)
	if _, err := rand.Read(buf); err != nil {
		// crypto/rand failing is a process-level emergency; the message
		// carries no material to leak.
		return "", err
	}
	return tokenPrefix + base64.RawURLEncoding.EncodeToString(buf), nil
}

// GenerateKeyID mints a key identifier: "pak_" + 12 crypto-random bytes,
// base64url without padding. Key IDs are public — they ride logs and usage
// records — so they are random rather than derived from the partner name,
// keeping even the identifier unlinkable to a guessable scheme.
func GenerateKeyID() (string, error) {
	buf := make([]byte, keyIDEntropy)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return keyIDPrefix + base64.RawURLEncoding.EncodeToString(buf), nil
}

// HashToken derives the storable form of a token: a bare SHA-256 digest.
// Plain SHA-256 (not bcrypt/argon2) is deliberate: the input is 256 bits of
// crypto-random material, so a precomputation or dictionary attack has
// nothing to chew on — the digest only has to keep the plaintext out of the
// database, out of logs, and out of backups.
func HashToken(token string) []byte {
	sum := sha256.Sum256([]byte(token))
	return sum[:]
}

// ValidTokenFormat reports whether a presented credential is even shaped
// like a partner token. It is a cheap pre-filter, not a judgment: a
// malformed token is answered exactly like an unknown one (static 401),
// it just never reaches the store.
func ValidTokenFormat(token string) bool {
	if len(token) != tokenLength || !strings.HasPrefix(token, tokenPrefix) {
		return false
	}
	_, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(token, tokenPrefix))
	return err == nil
}
