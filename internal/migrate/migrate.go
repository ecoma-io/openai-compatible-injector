// Package migrate is the shared SQL-first migration runner for the modules
// that own database schemas (the partner key store, the usage-event store).
//
// Migrations are embedded .sql files following the NNNN_name.sql
// convention, applied in ascending version order inside a transaction and
// recorded in one shared schema_migrations bookkeeping table keyed by
// (module, version) — each module owns its own version space. There is no
// down path: schema history is append-only, like the config snapshots this
// service already treats as immutable once published. After applying, the
// caller's column contract is validated against information_schema, so a
// manually mutated or partially migrated database is a startup failure
// (fail closed) instead of a query-time surprise. The request path never
// issues DDL — EnsureSchema runs once at construction, and that is the only
// DDL a store ever issues.
package migrate

import (
	"context"
	"database/sql"
	"fmt"
	"io/fs"
	"sort"
	"strconv"
	"strings"
)

// Migration is one parsed migration file.
type Migration struct {
	Version int
	Name    string
	Body    string
}

// Parse reads a migration directory from fsys: names follow the
// NNNN_name.sql convention, versions are unique and returned ascending, and
// bodies are non-empty. Versions start at 1. A violation is a process-shaped
// failure — the binary refuses to guess at schema state.
func Parse(fsys fs.FS, dir string) ([]Migration, error) {
	entries, err := fs.ReadDir(fsys, dir)
	if err != nil {
		return nil, err
	}
	var out []Migration
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
		if err != nil || version < 1 {
			return nil, fmt.Errorf("migration file %s does not follow the NNNN_name.sql convention", entry.Name())
		}
		if prev, dup := seen[version]; dup {
			return nil, fmt.Errorf("migration version %d is claimed by both %s and %s", version, prev, entry.Name())
		}
		seen[version] = entry.Name()
		body, err := fs.ReadFile(fsys, dir+"/"+entry.Name())
		if err != nil {
			return nil, err
		}
		if len(strings.TrimSpace(string(body))) == 0 {
			return nil, fmt.Errorf("migration file %s is empty", entry.Name())
		}
		out = append(out, Migration{Version: version, Name: entry.Name(), Body: string(body)})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Version < out[j].Version })
	return out, nil
}

// EnsureSchema brings the database up to the parsed schema for one module
// and verifies the result. Applied-but-unknown-to-this-binary versions (a
// newer binary ran here first) are left alone — forward-only means never
// guessing at what a newer file did, and the column contract below is still
// checked. table and requiredColumns are validated via ValidateSchema.
func EnsureSchema(ctx context.Context, db *sql.DB, module string, migrations []Migration, table string, requiredColumns []string) error {
	if err := ensureBookkeeping(ctx, db); err != nil {
		return err
	}
	applied := make(map[int]bool, len(migrations))
	rows, err := db.QueryContext(ctx, `SELECT version FROM schema_migrations WHERE module = $1`, module)
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
		if applied[m.Version] {
			continue
		}
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("migration %s/%d transaction failed: %w", module, m.Version, err)
		}
		// Serialize competing replicas and re-check after waiting. A process
		// that loses the race must observe the winner's bookkeeping row before
		// it can execute the migration body.
		if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtext('openai-compatible-injector:schema_migrations'))`); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("migration %s/%d lock failed: %w", module, m.Version, err)
		}
		var alreadyApplied bool
		if err := tx.QueryRowContext(ctx,
			`SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE module = $1 AND version = $2)`, module, m.Version).Scan(&alreadyApplied); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("migration %s/%d recheck failed: %w", module, m.Version, err)
		}
		if alreadyApplied {
			_ = tx.Rollback()
			continue
		}
		if _, err := tx.ExecContext(ctx, m.Body); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("migration %s/%d failed: %w", module, m.Version, err)
		}
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO schema_migrations (module, version, name) VALUES ($1, $2, $3)`, module, m.Version, m.Name); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("migration %s/%d bookkeeping failed: %w", module, m.Version, err)
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("migration %s/%d commit failed: %w", module, m.Version, err)
		}
	}
	return ValidateSchema(ctx, db, table, requiredColumns)
}

// ensureBookkeeping creates the module-scoped ledger, or upgrades the
// pre-module ledger released with partner keys. That older table has a global
// version primary key, and every historical row belongs to auth: at that time
// auth was the service's only schema owner. The upgrade happens atomically;
// it preserves all auth history before changing the key to (module, version),
// so a deployed database never reruns 0001_partner_api_keys.sql.
func ensureBookkeeping(ctx context.Context, db *sql.DB) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("schema_migrations bootstrap transaction failed: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	// CREATE TABLE IF NOT EXISTS is not itself race-free when two fresh
	// connections issue it at once: PostgreSQL can conflict on catalog type
	// creation before it sees the other's table. A transaction-scoped advisory
	// lock turns that into one bootstrap path, and also serializes legacy-ledger
	// adoption.
	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtext('openai-compatible-injector:schema_migrations_bootstrap'))`); err != nil {
		return fmt.Errorf("schema_migrations bootstrap lock failed: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			module     varchar(63) NOT NULL,
			version    integer     NOT NULL,
			name       text        NOT NULL,
			applied_at timestamptz NOT NULL DEFAULT now(),
			PRIMARY KEY (module, version)
		)`); err != nil {
		return fmt.Errorf("schema_migrations bootstrap failed: %w", err)
	}
	var hasModule bool
	if err := tx.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.columns
			WHERE table_schema = current_schema()
			  AND table_name = 'schema_migrations'
			  AND column_name = 'module'
		)`).Scan(&hasModule); err != nil {
		return fmt.Errorf("schema_migrations inspect failed: %w", err)
	}
	if !hasModule {
		if _, err := tx.ExecContext(ctx, `ALTER TABLE schema_migrations ADD COLUMN module varchar(63)`); err != nil {
			return fmt.Errorf("schema_migrations upgrade failed: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `UPDATE schema_migrations SET module = 'auth' WHERE module IS NULL`); err != nil {
			return fmt.Errorf("schema_migrations upgrade failed: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `ALTER TABLE schema_migrations ALTER COLUMN module SET NOT NULL`); err != nil {
			return fmt.Errorf("schema_migrations upgrade failed: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `ALTER TABLE schema_migrations DROP CONSTRAINT schema_migrations_pkey`); err != nil {
			return fmt.Errorf("schema_migrations upgrade failed: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `ALTER TABLE schema_migrations ADD PRIMARY KEY (module, version)`); err != nil {
			return fmt.Errorf("schema_migrations upgrade failed: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("schema_migrations bootstrap commit failed: %w", err)
	}
	return nil
}

// ValidateSchema checks the column contract against information_schema.
// Missing columns are named — they are this codebase's own identifiers, not
// operator input, so the no-echo rule is not at stake.
func ValidateSchema(ctx context.Context, db *sql.DB, table string, requiredColumns []string) error {
	var missing []string
	for _, col := range requiredColumns {
		var exists bool
		err := db.QueryRowContext(ctx, `
			SELECT EXISTS (
				SELECT 1 FROM information_schema.columns
				WHERE table_schema = current_schema()
				  AND table_name   = $1
				  AND column_name  = $2
			)`, table, col).Scan(&exists)
		if err != nil {
			return fmt.Errorf("schema validation query failed: %w", err)
		}
		if !exists {
			missing = append(missing, col)
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf("%s is missing required columns: %s", table, strings.Join(missing, ", "))
	}
	return nil
}
