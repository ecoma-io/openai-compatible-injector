package auth

import (
	"bytes"
	"encoding/base64"
	"strings"
	"testing"
)

// Token material: high-entropy, recognizable, hashed-only at rest.

func TestGenerateTokenShape(t *testing.T) {
	token, err := GenerateToken()
	if err != nil {
		t.Fatalf("GenerateToken: %v", err)
	}
	if !strings.HasPrefix(token, "oaicr_") {
		t.Fatalf("token %q lacks the oaicr_ prefix", "redacted")
	}
	if len(token) != tokenLength {
		t.Fatalf("token length = %d, want %d", len(token), tokenLength)
	}
	raw, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(token, "oaicr_"))
	if err != nil {
		t.Fatalf("token body is not base64url: %v", err)
	}
	if len(raw) != tokenEntropy {
		t.Fatalf("decoded entropy = %d bytes, want %d", len(raw), tokenEntropy)
	}
	// And the format gate must accept the generator's own output.
	if !ValidTokenFormat(token) {
		t.Fatal("ValidTokenFormat rejected a freshly generated token")
	}
}

func TestGenerateTokenUniqueness(t *testing.T) {
	seen := make(map[string]struct{}, 256)
	for i := 0; i < 256; i++ {
		token, err := GenerateToken()
		if err != nil {
			t.Fatalf("GenerateToken: %v", err)
		}
		if _, dup := seen[token]; dup {
			t.Fatal("GenerateToken produced a duplicate token")
		}
		seen[token] = struct{}{}
	}
}

func TestGenerateKeyIDShape(t *testing.T) {
	id, err := GenerateKeyID()
	if err != nil {
		t.Fatalf("GenerateKeyID: %v", err)
	}
	if !strings.HasPrefix(id, "pak_") || len(id) != len("pak_")+16 {
		t.Fatalf("key id shape wrong (len %d)", len(id))
	}
}

func TestValidTokenFormatRejectsGarbage(t *testing.T) {
	token, err := GenerateToken()
	if err != nil {
		t.Fatalf("GenerateToken: %v", err)
	}
	cases := []string{
		"",
		"garbage",
		"sk-proj-anything",
		token + "x",                              // length off
		token[:len(token)-1],                     // truncated body
		strings.ToUpper(token),                   // base64url is case-sensitive
		"oaicr_" + strings.Repeat("A", 43) + "=", // padded is not the raw alphabet shape
		"bearer " + token,
	}
	for _, c := range cases {
		if ValidTokenFormat(c) {
			t.Fatalf("ValidTokenFormat accepted a malformed token (len %d)", len(c))
		}
	}
}

func TestHashTokenIsDigestOnly(t *testing.T) {
	token, err := GenerateToken()
	if err != nil {
		t.Fatalf("GenerateToken: %v", err)
	}
	h := HashToken(token)
	if len(h) != 32 {
		t.Fatalf("hash length = %d, want 32 (SHA-256)", len(h))
	}
	if bytes.Contains(h, []byte(token)) {
		t.Fatal("hash contains the token bytes")
	}
	if !bytes.Equal(h, HashToken(token)) {
		t.Fatal("hashing is not deterministic")
	}
	// A different token hashes differently — the digest is the only
	// collision surface and it must actually discriminate.
	other, err := GenerateToken()
	if err != nil {
		t.Fatalf("GenerateToken: %v", err)
	}
	if bytes.Equal(h, HashToken(other)) {
		t.Fatal("two distinct tokens hashed identically")
	}
}
