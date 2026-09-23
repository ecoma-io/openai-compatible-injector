package auth

import (
	"context"
	"database/sql"
	"embed"

	"openai-compatible-injector/internal/migrate"
)

// SQL-first, versioned, forward-only migrations, run by the shared
// internal/migrate runner (own version space under module "auth", recorded
// in the shared schema_migrations table). Every change to the key schema is
// a numbered file under migrations/, applied inside a transaction. Startup
// runs EnsureSchema and fails closed if the database cannot be brought to
// (or verified at) the schema this binary speaks; the request path never
// issues DDL.
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

// loadMigrations parses the embedded migration set.
func loadMigrations() ([]migrate.Migration, error) {
	return migrate.Parse(migrationsFS, "migrations")
}

// EnsureSchema brings the key store's database up to the embedded schema
// and verifies the column contract.
func EnsureSchema(ctx context.Context, db *sql.DB) error {
	migrations, err := loadMigrations()
	if err != nil {
		return err
	}
	return migrate.EnsureSchema(ctx, db, "auth", migrations, "partner_api_keys", requiredColumns)
}

// ValidateSchema checks the key table's column contract against
// information_schema.
func ValidateSchema(ctx context.Context, db *sql.DB) error {
	if _, err := loadMigrations(); err != nil {
		return err
	}
	return migrate.ValidateSchema(ctx, db, "partner_api_keys", requiredColumns)
}
