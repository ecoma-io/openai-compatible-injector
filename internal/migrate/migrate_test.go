package migrate

import (
	"context"
	"database/sql"
	"net/url"
	"os"
	"testing"
	"testing/fstest"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib" // registers the "pgx" database/sql driver
)

// Structure tests run without a database: the parser's contract is what
// keeps a broken migration set from ever reaching a server.

func TestParseAcceptsAWellFormedSet(t *testing.T) {
	valid := fstest.MapFS{
		"migrations/0001_first.sql":  &fstest.MapFile{Data: []byte("SELECT 1;")},
		"migrations/0002_second.sql": &fstest.MapFile{Data: []byte("SELECT 2;")},
		"migrations/notes.txt":       &fstest.MapFile{Data: []byte("ignored, not a migration")},
	}
	got, err := Parse(valid, "migrations")
	if err != nil {
		t.Fatalf("valid set rejected: %v", err)
	}
	if len(got) != 2 || got[0].Version != 1 || got[1].Version != 2 {
		t.Fatalf("Parse = %+v, want versions 1 then 2", got)
	}
}

func TestParseRejectsBrokenSets(t *testing.T) {
	cases := map[string]fstest.MapFS{
		"duplicate version": {
			"migrations/0001_a.sql": &fstest.MapFile{Data: []byte("SELECT 1;")},
			"migrations/0001_b.sql": &fstest.MapFile{Data: []byte("SELECT 1;")},
		},
		"bad name": {
			"migrations/first.sql": &fstest.MapFile{Data: []byte("SELECT 1;")},
		},
		"non-numeric version": {
			"migrations/one_first.sql": &fstest.MapFile{Data: []byte("SELECT 1;")},
		},
		"zero version": {
			"migrations/0000_first.sql": &fstest.MapFile{Data: []byte("SELECT 1;")},
		},
		"negative version": {
			"migrations/-001_first.sql": &fstest.MapFile{Data: []byte("SELECT 1;")},
		},
		"empty body": {
			"migrations/0001_empty.sql": &fstest.MapFile{Data: []byte("  \n")},
		},
		"missing directory": {},
	}
	for name, fsys := range cases {
		if _, err := Parse(fsys, "migrations"); err == nil {
			t.Errorf("%s: broken migration set accepted", name)
		}
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
	return isolatedTestURL(t, rawURL, "migrate")
}

// isolatedTestURL gives each package test binary a private schema. Go runs
// package tests concurrently, and the individual suites deliberately create
// and drop their own schema-migrations table; sharing public would make those
// correct tests destroy each other's fixtures. The schema is temporary test
// infrastructure, not application behavior.
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

func resetBookkeeping(t *testing.T, db *sql.DB) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := db.ExecContext(ctx, `DROP TABLE IF EXISTS schema_migrations, migrate_probe`); err != nil {
		t.Fatalf("reset migration test tables: %v", err)
	}
}

func TestEnsureSchemaIsIdempotentPerModule(t *testing.T) {
	url := integrationDB(t)
	db, err := sql.Open("pgx", url)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	resetBookkeeping(t, db)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	set := []Migration{
		{Version: 1, Name: "0001_probe.sql", Body: `CREATE TABLE migrate_probe (id integer NOT NULL, payload text NOT NULL)`},
	}
	columns := []string{"id", "payload"}
	for run := 0; run < 2; run++ {
		if err := EnsureSchema(ctx, db, "probe-a", set, "migrate_probe", columns); err != nil {
			t.Fatalf("run %d: EnsureSchema: %v", run, err)
		}
	}

	// Two modules with overlapping version numbers share the bookkeeping
	// table without colliding: each owns its version space.
	setB := []Migration{
		{Version: 1, Name: "0001_other.sql", Body: `SELECT 1`},
		{Version: 2, Name: "0002_other.sql", Body: `SELECT 2`},
	}
	if err := EnsureSchema(ctx, db, "probe-b", setB, "migrate_probe", columns); err != nil {
		t.Fatalf("second module: %v", err)
	}

	var n int
	if err := db.QueryRowContext(ctx,
		`SELECT count(*) FROM schema_migrations WHERE module = 'probe-a' AND version = 1`).Scan(&n); err != nil {
		t.Fatalf("count probe-a: %v", err)
	}
	if n != 1 {
		t.Fatalf("probe-a version 1 recorded %d times, want exactly 1", n)
	}
	if err := db.QueryRowContext(ctx,
		`SELECT count(*) FROM schema_migrations WHERE module = 'probe-b'`).Scan(&n); err != nil {
		t.Fatalf("count probe-b: %v", err)
	}
	if n != 2 {
		t.Fatalf("probe-b recorded %d rows, want 2", n)
	}
}

func TestEnsureSchemaConcurrentBoot(t *testing.T) {
	url := integrationDB(t)
	seed, err := sql.Open("pgx", url)
	if err != nil {
		t.Fatalf("open seed: %v", err)
	}
	resetBookkeeping(t, seed)
	if err := seed.Close(); err != nil {
		t.Fatalf("close seed: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	set := []Migration{{Version: 1, Name: "0001_probe.sql", Body: `CREATE TABLE migrate_probe (id integer NOT NULL)`}}
	errs := make(chan error, 2)
	for range 2 {
		go func() {
			db, err := sql.Open("pgx", url)
			if err == nil {
				err = EnsureSchema(ctx, db, "probe", set, "migrate_probe", []string{"id"})
				_ = db.Close()
			}
			errs <- err
		}()
	}
	for range 2 {
		if err := <-errs; err != nil {
			t.Fatalf("concurrent EnsureSchema: %v", err)
		}
	}

	db, err := sql.Open("pgx", url)
	if err != nil {
		t.Fatalf("open verify: %v", err)
	}
	defer func() { _ = db.Close() }()
	var n int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM schema_migrations WHERE module = 'probe' AND version = 1`).Scan(&n); err != nil {
		t.Fatalf("read history: %v", err)
	}
	if n != 1 {
		t.Fatalf("migration rows = %d, want exactly 1", n)
	}
}

func TestEnsureSchemaUpgradesLegacyAuthBookkeeping(t *testing.T) {
	url := integrationDB(t)
	db, err := sql.Open("pgx", url)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	resetBookkeeping(t, db)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	// This is precisely the ledger shape that partner-key deployments used
	// before usage metering introduced independently versioned modules.
	if _, err := db.ExecContext(ctx, `
		CREATE TABLE schema_migrations (
			version integer NOT NULL PRIMARY KEY,
			name text NOT NULL,
			applied_at timestamptz NOT NULL DEFAULT now());
		INSERT INTO schema_migrations (version, name) VALUES (1, '0001_partner_api_keys.sql')`); err != nil {
		t.Fatalf("seed legacy bookkeeping: %v", err)
	}

	set := []Migration{{Version: 1, Name: "0001_probe.sql", Body: `CREATE TABLE migrate_probe (id integer NOT NULL)`}}
	if err := EnsureSchema(ctx, db, "usage", set, "migrate_probe", []string{"id"}); err != nil {
		t.Fatalf("EnsureSchema upgrade: %v", err)
	}

	var module string
	if err := db.QueryRowContext(ctx, `SELECT module FROM schema_migrations WHERE version = 1 AND name = '0001_partner_api_keys.sql'`).Scan(&module); err != nil {
		t.Fatalf("read preserved auth history: %v", err)
	}
	if module != "auth" {
		t.Fatalf("legacy history module = %q, want auth", module)
	}
	var n int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM schema_migrations WHERE module = 'usage' AND version = 1`).Scan(&n); err != nil {
		t.Fatalf("read usage history: %v", err)
	}
	if n != 1 {
		t.Fatalf("usage migration rows = %d, want 1", n)
	}
}

func TestEnsureSchemaLeavesUnknownVersionsAlone(t *testing.T) {
	url := integrationDB(t)
	db, err := sql.Open("pgx", url)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	resetBookkeeping(t, db)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := db.ExecContext(ctx,
		`INSERT INTO schema_migrations (module, version, name) VALUES ('probe', 9999, '0009_from_the_future.sql')`); err != nil {
		// The bookkeeping table may not exist yet on a fresh database.
		if _, err := db.ExecContext(ctx, `
			CREATE TABLE schema_migrations (
				module     varchar(63) NOT NULL,
				version    integer     NOT NULL,
				name       text        NOT NULL,
				applied_at timestamptz NOT NULL DEFAULT now(),
				PRIMARY KEY (module, version))`); err != nil {
			t.Fatalf("bootstrap bookkeeping: %v", err)
		}
		if _, err := db.ExecContext(ctx,
			`INSERT INTO schema_migrations (module, version, name) VALUES ('probe', 9999, '0009_from_the_future.sql')`); err != nil {
			t.Fatalf("seed future history: %v", err)
		}
	}

	set := []Migration{{Version: 1, Name: "0001_probe.sql", Body: `CREATE TABLE migrate_probe (id integer NOT NULL)`}}
	if err := EnsureSchema(ctx, db, "probe", set, "migrate_probe", []string{"id"}); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}

	var n int
	if err := db.QueryRowContext(ctx,
		`SELECT count(*) FROM schema_migrations WHERE module = 'probe' AND version = 9999`).Scan(&n); err != nil {
		t.Fatalf("verify query: %v", err)
	}
	if n != 1 {
		t.Fatalf("future migration row count = %d, want 1 (forward-only: never rewritten)", n)
	}
}

func TestValidateSchemaFailsClosedOnForeignSchema(t *testing.T) {
	url := integrationDB(t)
	db, err := sql.Open("pgx", url)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	resetBookkeeping(t, db)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	// A table that shares the name but not the contract: manually mutated,
	// or half-migrated by someone else's tool. Startup must refuse.
	if _, err := db.ExecContext(ctx, `CREATE TABLE migrate_probe (id integer)`); err != nil {
		t.Fatalf("create foreign table: %v", err)
	}
	set := []Migration{{Version: 1, Name: "0001_probe.sql", Body: `SELECT 1`}}
	if err := EnsureSchema(ctx, db, "probe", set, "migrate_probe", []string{"id", "payload"}); err == nil {
		t.Fatal("EnsureSchema accepted a foreign table schema — startup must fail closed")
	}
}
