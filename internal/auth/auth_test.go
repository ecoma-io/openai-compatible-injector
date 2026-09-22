package auth

import (
	"context"
	"strings"
	"testing"
)

// The static authenticator is the historical shared key: match exactly,
// deny everything else, leak nothing by timing.

func TestStaticAuthenticatorAcceptsExactKey(t *testing.T) {
	const key = "sk-static-key-value"
	a := StaticProvider{}.For(staticSnapshot(t, key))

	p, reason, err := a.Authenticate(context.Background(), key)
	if err != nil {
		t.Fatalf("Authenticate returned error: %v", err)
	}
	if reason != ReasonOK {
		t.Fatalf("reason = %v, want ok", reason)
	}
	// The zero Principal is the shared identity: static mode has no
	// per-caller attribution, and nothing may be invented.
	if p.PartnerID != "" || p.KeyID != "" {
		t.Fatalf("static Principal = %+v, want zero value", p)
	}
}

func TestStaticAuthenticatorRejectsEverythingElse(t *testing.T) {
	const key = "sk-static-key-value"
	a := StaticProvider{}.For(staticSnapshot(t, key))

	for _, presented := range []string{"", key + "x", key[:len(key)-1], strings.ToUpper(key), "another-key"} {
		_, reason, err := a.Authenticate(context.Background(), presented)
		if err != nil {
			t.Fatalf("Authenticate(%q) returned error: %v", presented, err)
		}
		if reason != ReasonUnknown {
			t.Fatalf("Authenticate(%q) reason = %v, want unknown_key", presented, reason)
		}
	}
}

func TestStaticAuthenticatorBindsToItsSnapshot(t *testing.T) {
	old := StaticProvider{}.For(staticSnapshot(t, "old-key"))
	newSnap := StaticProvider{}.For(staticSnapshot(t, "new-key"))

	if _, reason, _ := old.Authenticate(context.Background(), "new-key"); reason == ReasonOK {
		t.Fatal("the new key authenticated against the old snapshot — the authenticator must be snapshot-bound")
	}
	if _, reason, _ := newSnap.Authenticate(context.Background(), "new-key"); reason != ReasonOK {
		t.Fatal("the new key did not authenticate against its own snapshot")
	}
}

func TestReasonStringsAreStable(t *testing.T) {
	// These tokens reach structured logs and are pinned: they are part of
	// the observable surface, like every other snake_case field value.
	for r, want := range map[Reason]string{
		ReasonOK:      "ok",
		ReasonUnknown: "unknown_key",
		ReasonRevoked: "revoked_key",
		ReasonBackend: "backend_unavailable",
		Reason(99):    "unknown_reason",
	} {
		if got := r.String(); got != want {
			t.Errorf("Reason(%d).String() = %q, want %q", int(r), got, want)
		}
	}
}
