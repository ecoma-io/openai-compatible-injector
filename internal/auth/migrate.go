package auth

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"io/fs"
	"sort"
	"strconv"
	"strings"
)

// SQL-first, versioned, forward-only migrations. Every change to the key
// schema is a numbered file under migrations/, applied inside a
// transaction and recorded in schema_migrations. There is no down path —
// schema history is append-only, like the config snapshots this service
// already treats as immutable once published. Startup runs EnsureSchema and
// fails closed if the database cannot be brought to (or verified at) the
// schema this binary speaks; the request path never issues DDL.
//
//go:embed migrations/*.sql
var migrationsFS embed.FS

// requiredColumns is the schema contract this binary's queries assume.
// ValidateSchema checks every entry against information_schema so a
// manually mutated or partially migrated database is a startup failure
// (fail closed) instead of a 500-shaped surprise on the first lookup.
var requiredColumns = []string{
	"key_id", "partner_id", "key_hash", "status", "created_at", "revoked_at", "last_used_at",
}

type migration struct {
	version int
	name    string
	body    string
}

// loadMigrations parses the embedded migration set.
func loadMigrations() ([]migration, error) {
	return loadMigrationsFrom(migrationsFS)
}

// loadMigrationsFrom parses a migration directory: names follow the
// NNNN_name.sql convention, versions are unique and applied ascending, and
// bodies are non-empty. A violation is a process-shaped failure — the
// binary refuses to guess at schema state.
func loadMigrationsFrom(fsys fs.FS) ([]migration, error) {
	entries, err := fs.ReadDir(fsys, "migrations")
	if err != nil {
		return nil, err
	}
	var out []migration
	seen := make(map[int]string, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}
		base := strings.TrimSuffix(entry.Name(), ".sql")
		dash := strings.Index(base, "_")
		if dash <= 0 {
			return nil, fmt.Errorf("migration file %s does not follow the NNNN_name.sql convention", entry.Name())
		}
		version, err := strconv.Atoi(base[:dash])
		if err != nil {
			return nil, fmt.Errorf("migration file %s does not follow the NNNN_name.sql convention", entry.Name())
		}
		if prev, dup := seen[version]; dup {
			return nil, fmt.Errorf("migration version %d is claimed by both %s and %s", version, prev, entry.Name())
		}
		seen[version] = entry.Name()
		body, err := fs.ReadFile(fsys, "migrations/"+entry.Name())
		if err != nil {
			return nil, err
		}
		if len(strings.TrimSpace(string(body))) == 0 {
			return nil, fmt.Errorf("migration file %s is empty", entry.Name())
		}
		out = append(out, migration{version: version, name: entry.Name(), body: string(body)})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].version < out[j].version })
	return out, nil
}

// EnsureSchema brings the database up to the embedded schema and verifies
// the result. Applied-but-unknown-to-this-binary versions (a newer binary
// ran here first) are left alone — forward-only means never guessing at
// what a newer file did, and the column contract below is still checked.
func EnsureSchema(ctx context.Context, db *sql.DB) error {
	migrations, err := loadMigrations()
	if err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version    integer     NOT NULL PRIMARY KEY,
			name       text        NOT NULL,
			applied_at timestamptz NOT NULL DEFAULT now()
		)`); err != nil {
		return fmt.Errorf("schema_migrations bootstrap failed: %w", err)
	}
	applied := make(map[int]bool, len(migrations))
	rows, err := db.QueryContext(ctx, `SELECT version FROM schema_migrations`)
	if err != nil {
		return fmt.Errorf("schema_migrations read failed: %w", err)
	}
	for rows.Next() {
		var version int
		if err := rows.Scan(&version); err != nil {
			_ = rows.Close()
			return fmt.Errorf("schema_migrations read failed: %w", err)
		}
		applied[version] = true
	}
	_ = rows.Close()
	if err := rows.Err(); err != nil {
		return fmt.Errorf("schema_migrations read failed: %w", err)
	}

	for _, m := range migrations {
		if applied[m.version] {
			continue
		}
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("migration %d transaction failed: %w", m.version, err)
		}
		if _, err := tx.ExecContext(ctx, m.body); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("migration %d failed: %w", m.version, err)
		}
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO schema_migrations (version, name) VALUES ($1, $2)`, m.version, m.name); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("migration %d bookkeeping failed: %w", m.version, err)
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("migration %d commit failed: %w", m.version, err)
		}
	}
	return ValidateSchema(ctx, db)
}

// ValidateSchema checks the column contract against information_schema.
// Missing columns are named — they are this codebase's own identifiers, not
// operator input, so the no-echo rule is not at stake.
func ValidateSchema(ctx context.Context, db *sql.DB) error {
	if _, err := loadMigrations(); err != nil {
		return err
	}
	var missing []string
	for _, col := range requiredColumns {
		var exists bool
		err := db.QueryRowContext(ctx, `
			SELECT EXISTS (
				SELECT 1 FROM information_schema.columns
				WHERE table_schema = current_schema()
				  AND table_name   = 'partner_api_keys'
				  AND column_name  = $1
			)`, col).Scan(&exists)
		if err != nil {
			return fmt.Errorf("schema validation query failed: %w", err)
		}
		if !exists {
			missing = append(missing, col)
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf("partner_api_keys is missing required columns: %s", strings.Join(missing, ", "))
	}
	return nil
}
