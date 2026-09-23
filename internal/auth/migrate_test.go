package auth

import (
	"context"
	"database/sql"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"
)

// Structure tests run without a database: the migration set itself must be
// well-formed before any SQL touches a server.

func TestEmbeddedMigrationsAreWellFormed(t *testing.T) {
	migrations, err := loadMigrations()
	if err != nil {
		t.Fatalf("loadMigrations: %v", err)
	}
	if len(migrations) == 0 {
		t.Fatal("no migrations embedded")
	}
	previous := 0
	for _, m := range migrations {
		if m.Version <= previous {
			t.Errorf("migration %s is not strictly ascending (previous %d)", m.Name, previous)
		}
		if strings.TrimSpace(m.Body) == "" {
			t.Errorf("migration %s has an empty body", m.Name)
		}
		previous = m.Version
	}
}

// Everything below drives a real PostgreSQL/TimescaleDB and is opt-in:
// set OAICR_TEST_DATABASE_URL to a dedicated, disposable test database.
// Unset (the default, and CI) skips.

func integrationDB(t *testing.T) string {
	t.Helper()
	rawURL := os.Getenv("OAICR_TEST_DATABASE_URL")
	if rawURL == "" {
		t.Skip("OAICR_TEST_DATABASE_URL not set; skipping PostgreSQL integration test")
	}
	return isolatedTestURL(t, rawURL, "auth")
}

// isolatedTestURL separates this package's disposable PostgreSQL fixtures
// from other package test binaries, which Go runs concurrently by default.
func isolatedTestURL(t *testing.T, rawURL, schema string) string {
	t.Helper()
	u, err := url.Parse(rawURL)
	if err != nil {
		t.Fatalf("parse OAICR_TEST_DATABASE_URL: %v", err)
	}
	name := "oaicr_" + schema + "_test"
	admin, err := sql.Open("pgx", rawURL)
	if err != nil {
		t.Fatalf("open test database: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := admin.ExecContext(ctx, `CREATE SCHEMA IF NOT EXISTS `+name); err != nil {
		_ = admin.Close()
		t.Fatalf("create test schema: %v", err)
	}
	if err := admin.Close(); err != nil {
		t.Fatalf("close test database: %v", err)
	}
	q := u.Query()
	q.Set("search_path", name)
	u.RawQuery = q.Encode()
	return u.String()
}

func resetSchema(t *testing.T, url string) {
	t.Helper()
	db, err := sql.Open("pgx", url)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for _, table := range []string{"partner_api_keys", "schema_migrations"} {
		if _, err := db.ExecContext(ctx, "DROP TABLE IF EXISTS "+table); err != nil {
			t.Fatalf("drop %s: %v", table, err)
		}
	}
}

func TestPGStoreLifecycle(t *testing.T) {
	url := integrationDB(t)
	resetSchema(t, url)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	store, err := NewPGStore(ctx, url)
	if err != nil {
		t.Fatalf("NewPGStore: %v", err)
	}

	token, err := GenerateToken()
	if err != nil {
		t.Fatalf("GenerateToken: %v", err)
	}
	keyID, err := GenerateKeyID()
	if err != nil {
		t.Fatalf("GenerateKeyID: %v", err)
	}
	created := time.Now().UTC().Truncate(time.Microsecond)
	rec := KeyRecord{KeyID: keyID, PartnerID: "partner-acme", Status: StatusActive, CreatedAt: created}
	if err := store.CreateKey(ctx, rec, HashToken(token)); err != nil {
		t.Fatalf("CreateKey: %v", err)
	}

	// Lookup by the presented token's digest resolves the identity.
	got, err := store.LookupByHash(ctx, HashToken(token))
	if err != nil {
		t.Fatalf("LookupByHash: %v", err)
	}
	if got.KeyID != keyID || got.PartnerID != "partner-acme" || got.Status != StatusActive {
		t.Fatalf("LookupByHash = %+v, want the seeded record", got)
	}
	if got.RevokedAt != nil || got.LastUsedAt != nil {
		t.Fatalf("fresh record carries timestamps: %+v", got)
	}

	// An unknown token is the sentinel, never a generic error.
	if _, err := store.LookupByHash(ctx, HashToken("oaicr_"+strings.Repeat("A", 43))); err != ErrKeyNotFound {
		t.Fatalf("unknown lookup = %v, want ErrKeyNotFound", err)
	}

	// Revocation flips exactly once; a second call is a no-op false.
	if ok, err := store.RevokeKey(ctx, keyID); err != nil || !ok {
		t.Fatalf("RevokeKey = %v/%v, want true/nil", ok, err)
	}
	if ok, err := store.RevokeKey(ctx, keyID); err != nil || ok {
		t.Fatalf("second RevokeKey = %v/%v, want false/nil", ok, err)
	}
	if ok, err := store.RevokeKey(ctx, "pak_does_not_exist"); err != nil || ok {
		t.Fatalf("unknown RevokeKey = %v/%v, want false/nil", ok, err)
	}
	got, err = store.LookupByHash(ctx, HashToken(token))
	if err != nil {
		t.Fatalf("post-revoke lookup: %v", err)
	}
	if got.Status != StatusRevoked || got.RevokedAt == nil {
		t.Fatalf("post-revoke record = %+v, want revoked with a timestamp", got)
	}

	// ListKeys shows the key and structurally cannot leak a hash (the
	// record type carries none).
	keys, err := store.ListKeys(ctx)
	if err != nil {
		t.Fatalf("ListKeys: %v", err)
	}
	if len(keys) != 1 || keys[0].KeyID != keyID {
		t.Fatalf("ListKeys = %+v, want exactly the created key", keys)
	}

	// TouchLastUsed flushes on Close: advisory, but the one batch we can
	// demand deterministically is the final one.
	store.TouchLastUsed(keyID, time.Now().UTC())
	closeCtx, closeCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer closeCancel()
	if err := store.Close(closeCtx); err != nil {
		t.Fatalf("Close: %v", err)
	}

	verify, err := sql.Open("pgx", url)
	if err != nil {
		t.Fatalf("verify open: %v", err)
	}
	defer func() { _ = verify.Close() }()
	var lastUsed sql.NullTime
	if err := verify.QueryRowContext(ctx,
		`SELECT last_used_at FROM partner_api_keys WHERE key_id = $1`, keyID).Scan(&lastUsed); err != nil {
		t.Fatalf("verify query: %v", err)
	}
	if !lastUsed.Valid {
		t.Fatal("last_used_at was never flushed")
	}
}

func TestEnsureSchemaIsIdempotent(t *testing.T) {
	url := integrationDB(t)
	resetSchema(t, url)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for run := 0; run < 2; run++ {
		store, err := NewPGStore(ctx, url)
		if err != nil {
			t.Fatalf("run %d: NewPGStore: %v", run, err)
		}
		if err := store.Close(ctx); err != nil {
			t.Fatalf("run %d: Close: %v", run, err)
		}
	}

	// The bookkeeping table recorded every migration exactly once.
	verify, err := sql.Open("pgx", url)
	if err != nil {
		t.Fatalf("verify open: %v", err)
	}
	defer func() { _ = verify.Close() }()
	rows, err := verify.QueryContext(ctx, `SELECT version FROM schema_migrations WHERE module = 'auth'`)
	if err != nil {
		t.Fatalf("verify query: %v", err)
	}
	defer func() { _ = rows.Close() }()
	seen := make(map[int]int)
	for rows.Next() {
		var v int
		if err := rows.Scan(&v); err != nil {
			t.Fatalf("scan: %v", err)
		}
		seen[v]++
	}
	migrations, err := loadMigrations()
	if err != nil {
		t.Fatalf("loadMigrations: %v", err)
	}
	for _, m := range migrations {
		if seen[m.Version] != 1 {
			t.Fatalf("migration %d recorded %d times, want exactly 1", m.Version, seen[m.Version])
		}
	}
}

func TestValidateSchemaFailsClosedOnForeignSchema(t *testing.T) {
	url := integrationDB(t)
	resetSchema(t, url)

	// A table that shares the name but not the contract: manually mutated,
	// or half-migrated by someone else's tool. Startup must refuse.
	setup, err := sql.Open("pgx", url)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := setup.ExecContext(ctx, `CREATE TABLE partner_api_keys (key_id text)`); err != nil {
		t.Fatalf("create foreign table: %v", err)
	}
	_ = setup.Close()

	if _, err := NewPGStore(ctx, url); err == nil {
		t.Fatal("NewPGStore accepted a foreign partner_api_keys schema — startup must fail closed")
	}
}

func TestEnsureSchemaLeavesUnknownVersionsAlone(t *testing.T) {
	url := integrationDB(t)
	resetSchema(t, url)

	// A newer binary migrated here first: its bookkeeping rows are forward
	// history this binary must never undo or re-run.
	setup, err := sql.Open("pgx", url)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := setup.ExecContext(ctx, `
		CREATE TABLE schema_migrations (
			module     varchar(63) NOT NULL,
			version    integer     NOT NULL,
			name       text        NOT NULL,
			applied_at timestamptz NOT NULL DEFAULT now(),
			PRIMARY KEY (module, version));
		INSERT INTO schema_migrations (module, version, name) VALUES ('auth', 9999, '0009_from_the_future.sql');`); err != nil {
		t.Fatalf("seed future history: %v", err)
	}
	_ = setup.Close()

	store, err := NewPGStore(ctx, url)
	if err != nil {
		t.Fatalf("NewPGStore: %v", err)
	}
	if err := store.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}

	verify, err := sql.Open("pgx", url)
	if err != nil {
		t.Fatalf("verify open: %v", err)
	}
	defer func() { _ = verify.Close() }()
	var n int
	if err := verify.QueryRowContext(ctx,
		`SELECT count(*) FROM schema_migrations WHERE version = 9999`).Scan(&n); err != nil {
		t.Fatalf("verify query: %v", err)
	}
	if n != 1 {
		t.Fatalf("future migration row count = %d, want 1 (forward-only: never rewritten)", n)
	}
}
