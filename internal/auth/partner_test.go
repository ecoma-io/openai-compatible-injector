package auth

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// fakeKeyStore is an in-memory KeyStore for provider-level tests. It counts
// lookups so tests can prove what the cache absorbed, and a settable
// lookupErr to drive backend failures.
type fakeKeyStore struct {
	mu        sync.Mutex
	byHash    map[string]KeyRecord // keyed by the raw token digest bytes
	lookupErr error
	lookups   int
	touches   map[string]int
	closed    bool
}

func newFakeKeyStore() *fakeKeyStore {
	return &fakeKeyStore{byHash: make(map[string]KeyRecord), touches: make(map[string]int)}
}

func (s *fakeKeyStore) seed(token string, rec KeyRecord) {
	s.byHash[string(HashToken(token))] = rec
}

func (s *fakeKeyStore) LookupByHash(_ context.Context, hash []byte) (KeyRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lookups++
	if s.lookupErr != nil {
		return KeyRecord{}, s.lookupErr
	}
	rec, ok := s.byHash[string(hash)]
	if !ok {
		return KeyRecord{}, ErrKeyNotFound
	}
	return rec, nil
}

func (s *fakeKeyStore) CreateKey(_ context.Context, rec KeyRecord, hash []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.byHash[string(hash)] = rec
	return nil
}

func (s *fakeKeyStore) ListKeys(_ context.Context) ([]KeyRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]KeyRecord, 0, len(s.byHash))
	for _, rec := range s.byHash {
		out = append(out, rec)
	}
	return out, nil
}

func (s *fakeKeyStore) RevokeKey(_ context.Context, keyID string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for h, rec := range s.byHash {
		if rec.KeyID == keyID {
			if rec.Status != StatusActive {
				return false, nil
			}
			rec.Status = StatusRevoked
			now := time.Now()
			rec.RevokedAt = &now
			s.byHash[h] = rec
			return true, nil
		}
	}
	return false, nil
}

func (s *fakeKeyStore) TouchLastUsed(keyID string, _ time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.touches[keyID]++
}

func (s *fakeKeyStore) Close(context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	return nil
}

func (s *fakeKeyStore) lookupCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lookups
}

func (s *fakeKeyStore) touchCount(keyID string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.touches[keyID]
}

func seedActive(t *testing.T, store *fakeKeyStore, token string) KeyRecord {
	t.Helper()
	rec := KeyRecord{KeyID: "pak_" + token[len(token)-4:], PartnerID: "partner-" + token[len(token)-4:], Status: StatusActive, CreatedAt: time.Now().UTC()}
	store.seed(token, rec)
	return rec
}

// The affirmative path: an active key resolves to its identity, and the
// store learns about the use.

func TestPartnerAuthenticatesActiveKey(t *testing.T) {
	store := newFakeKeyStore()
	token, err := GenerateToken()
	if err != nil {
		t.Fatalf("GenerateToken: %v", err)
	}
	want := seedActive(t, store, token)

	p := NewPartnerProvider(store)
	got, reason, err := p.Authenticate(context.Background(), token)
	if err != nil || reason != ReasonOK {
		t.Fatalf("Authenticate = %v/%v/err=%v, want ok", got, reason, err)
	}
	if got.PartnerID != want.PartnerID || got.KeyID != want.KeyID {
		t.Fatalf("Principal = %+v, want %+v", got, want)
	}
	if store.touchCount(want.KeyID) != 1 {
		t.Fatalf("TouchLastUsed count = %d, want 1", store.touchCount(want.KeyID))
	}

	// The second presentation is served from the cache: still ok, and the
	// store saw no new lookup (and no duplicate touch worth accounting —
	// the touch rides only fresh lookups by design).
	before := store.lookupCount()
	if _, reason, err := p.Authenticate(context.Background(), token); err != nil || reason != ReasonOK {
		t.Fatalf("cached Authenticate = %v/err=%v, want ok", reason, err)
	}
	if store.lookupCount() != before {
		t.Fatalf("cached presentation hit the store %d extra times", store.lookupCount()-before)
	}
}

// The negative paths: definitive answers, cached, never an error.

func TestPartnerUnknownKeyIsDefinitiveNegative(t *testing.T) {
	store := newFakeKeyStore()
	p := NewPartnerProvider(store)
	token, err := GenerateToken()
	if err != nil {
		t.Fatalf("GenerateToken: %v", err)
	}

	_, reason, err := p.Authenticate(context.Background(), token)
	if err != nil {
		t.Fatalf("unknown key must not be a backend error: %v", err)
	}
	if reason != ReasonUnknown {
		t.Fatalf("reason = %v, want unknown_key", reason)
	}
	// Second presentation is absorbed by the negative cache.
	if _, _, _ = p.Authenticate(context.Background(), token); store.lookupCount() != 1 {
		t.Fatalf("lookups = %d, want 1 (the negative decision must be cached)", store.lookupCount())
	}
}

func TestPartnerRevokedKeyIsDenied(t *testing.T) {
	store := newFakeKeyStore()
	token, err := GenerateToken()
	if err != nil {
		t.Fatalf("GenerateToken: %v", err)
	}
	rec := seedActive(t, store, token)
	rec.Status = StatusRevoked
	now := time.Now()
	rec.RevokedAt = &now
	store.seed(token, rec)

	p := NewPartnerProvider(store)
	_, reason, err := p.Authenticate(context.Background(), token)
	if err != nil {
		t.Fatalf("revoked key must be a definitive negative, not %v", err)
	}
	if reason != ReasonRevoked {
		t.Fatalf("reason = %v, want revoked_key", reason)
	}
	if _, reason, _ := p.Authenticate(context.Background(), token); reason != ReasonRevoked {
		t.Fatalf("second presentation reason = %v, want revoked_key", reason)
	}
}

func TestPartnerMalformedTokenNeverReachesTheStore(t *testing.T) {
	store := newFakeKeyStore()
	p := NewPartnerProvider(store)
	for _, token := range []string{"", "x", "oaicr_short", "sk-proj-whatever", "oaicr_!!!"} {
		_, reason, err := p.Authenticate(context.Background(), token)
		if err != nil || reason != ReasonUnknown {
			t.Fatalf("Authenticate(%q) = %v/err=%v, want unknown/nil", "redacted", reason, err)
		}
	}
	if store.lookupCount() != 0 {
		t.Fatalf("garbage tokens caused %d store lookups, want 0", store.lookupCount())
	}
}

// The fail-closed heart: a backend failure denies the request, is never
// cached, and the very next call retries the store — a recovered store is
// trusted again immediately, and an outage can never be frozen into the
// cache.

func TestPartnerBackendFailureFailsClosedAndIsNeverCached(t *testing.T) {
	store := newFakeKeyStore()
	token, err := GenerateToken()
	if err != nil {
		t.Fatalf("GenerateToken: %v", err)
	}
	seedActive(t, store, token)

	p := NewPartnerProvider(store)

	// In flight: the store is down.
	store.mu.Lock()
	store.lookupErr = errors.New("connection refused")
	store.mu.Unlock()
	_, reason, err := p.Authenticate(context.Background(), token)
	if err == nil {
		t.Fatal("backend failure must surface an error for the auth_backend_failed event")
	}
	if reason != ReasonBackend {
		t.Fatalf("reason = %v, want backend_unavailable", reason)
	}

	// Still down: the failure was not cached — the store is consulted (and
	// fails) again.
	_, reason, _ = p.Authenticate(context.Background(), token)
	if reason != ReasonBackend {
		t.Fatalf("second in-flight reason = %v, want backend_unavailable (the failure must not be cached)", reason)
	}

	// Recovered: the very next presentation succeeds, and the key is
	// remembered positively from there.
	store.mu.Lock()
	store.lookupErr = nil
	store.mu.Unlock()
	if _, reason, _ := p.Authenticate(context.Background(), token); reason != ReasonOK {
		t.Fatalf("post-recovery reason = %v, want ok", reason)
	}
}

// Partner identity is security state, not config state: the authenticator
// handed out for one snapshot is the same one for any other.

func TestPartnerProviderForIgnoresTheSnapshot(t *testing.T) {
	store := newFakeKeyStore()
	p := NewPartnerProvider(store)
	snap := staticSnapshot(t, "irrelevant-shared-key")

	a1 := p.For(snap)
	a2 := p.For(staticSnapshot(t, "another-shared-key"))
	if a1 != Authenticator(a2) {
		t.Fatal("partner mode must not fork authenticators per snapshot — revocation cannot become snapshot-scoped")
	}
	// And the YAML api-key has no power in partner mode: it is not seeded
	// in the store, so presenting it denies.
	_, reason, err := a1.Authenticate(context.Background(), "irrelevant-shared-key")
	if err != nil || reason != ReasonUnknown {
		t.Fatalf("YAML key in partner mode = %v/err=%v, want unknown/nil", reason, err)
	}
}

func TestStoreErrorClassClassification(t *testing.T) {
	if got := StoreErrorClass(context.DeadlineExceeded); got != "timeout" {
		t.Errorf("deadline = %q, want timeout", got)
	}
	if got := StoreErrorClass(context.Canceled); got != "canceled" {
		t.Errorf("canceled = %q, want canceled", got)
	}
	if got := StoreErrorClass(ErrKeyNotFound); got != "query_failed" {
		t.Errorf("sentinel = %q, want query_failed", got)
	}
	if got := StoreErrorClass(errors.New("connection refused")); got != "query_failed" {
		t.Errorf("plain error = %q, want query_failed", got)
	}
}
