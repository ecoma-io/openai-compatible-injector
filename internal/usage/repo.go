package usage

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"embed"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib" // registers the "pgx" database/sql driver

	"openai-compatible-injector/internal/migrate"
)

// PostgreSQL/TimescaleDB storage for usage events, over database/sql +
// pgx. SQL-first like the key store: no ORM, explicit statements. This file
// holds both halves of the repository seam — ingestion (InsertEvents, the
// only method the pipeline sees) and reporting (Summary) — but nothing
// couples them: the pipeline depends on InsertRepository, reporting
// consumers depend on the query methods, and the two surfaces can evolve
// apart.
//
// The DSN lives in bootstrap env (OAICR_USAGE_DATABASE_URL) and is never
// logged, in line with every other connection string this service holds.

//go:embed migrations/*.sql
var migrationsFS embed.FS

// requiredColumns is the column contract this binary's statements assume.
var requiredColumns = []string{
	"event_id", "occurred_at", "partner_id", "key_id", "request_id", "config_generation",
	"public_model", "provider", "upstream_model", "api", "stream", "http_status", "outcome",
	"prompt_tokens", "completion_tokens", "total_tokens",
	"bytes_in", "bytes_out", "provider_attempts", "egress_attempts", "egress_kind", "latency_ms",
}

// parseUsageMigrations parses the embedded migration set.
func parseUsageMigrations() ([]migrate.Migration, error) {
	return migrate.Parse(migrationsFS, "migrations")
}

// EnsureSchema applies the embedded migrations and validates the column
// contract, under the usage module's own version space.
func EnsureSchema(ctx context.Context, db *sql.DB) error {
	migrations, err := parseUsageMigrations()
	if err != nil {
		return err
	}
	return migrate.EnsureSchema(ctx, db, "usage", migrations, "usage_events", requiredColumns)
}

// PGRepository reads and writes usage events in PostgreSQL/TimescaleDB.
type PGRepository struct {
	db *sql.DB
}

// NewPGRepository opens the pool and brings the schema up. A pool that
// cannot be reached or brought to the expected schema is a construction
// failure — the operator asked for metering, and metering into a database
// that is not there would be silent loss from the first request. This is
// the only DDL the repository ever issues.
func NewPGRepository(ctx context.Context, databaseURL string) (*PGRepository, error) {
	db, err := sql.Open("pgx", databaseURL)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(4)
	db.SetMaxIdleConns(2)
	db.SetConnMaxIdleTime(5 * time.Minute)
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	if err := EnsureSchema(ctx, db); err != nil {
		_ = db.Close()
		return nil, err
	}
	return &PGRepository{db: db}, nil
}

// InsertEvents implements InsertRepository: one multi-row INSERT per
// batch. The batch is the pipeline's flush unit, so a busy service pays
// one round trip per batch, not per request. NULL token counts ride as
// nils, preserving "the upstream said nothing" against the zero value's
// "the upstream said zero".
func (r *PGRepository) InsertEvents(ctx context.Context, events []Event) error {
	if len(events) == 0 {
		return nil
	}
	const cols = 22
	var sb strings.Builder
	sb.WriteString(`INSERT INTO usage_events (
	event_id, occurred_at, partner_id, key_id, request_id, config_generation,
	public_model, provider, upstream_model, api, stream, http_status, outcome,
	prompt_tokens, completion_tokens, total_tokens,
	bytes_in, bytes_out, provider_attempts, egress_attempts, egress_kind, latency_ms) VALUES `)
	args := make([]any, 0, len(events)*cols)
	for i, ev := range events {
		if i > 0 {
			sb.WriteByte(',')
		}
		base := i * cols
		fmt.Fprintf(&sb, `($%d::uuid,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d)`,
			base+1, base+2, base+3, base+4, base+5, base+6, base+7, base+8, base+9, base+10,
			base+11, base+12, base+13, base+14, base+15, base+16, base+17, base+18, base+19, base+20,
			base+21, base+22)
		args = append(args,
			ev.EventID, ev.OccurredAt, ev.PartnerID, ev.KeyID, ev.RequestID, int64(ev.ConfigGeneration),
			ev.PublicModel, ev.Provider, ev.UpstreamModel, ev.API, ev.Stream, ev.HTTPStatus, ev.Outcome,
			ev.PromptTokens, ev.CompletionTokens, ev.TotalTokens,
			ev.BytesIn, ev.BytesOut, ev.ProviderAttempts, ev.EgressAttempts, ev.EgressKind, ev.LatencyMS)
	}
	if _, err := r.db.ExecContext(ctx, sb.String(), args...); err != nil {
		return fmt.Errorf("usage insert failed: %w", err)
	}
	return nil
}

// Filter selects the reporting window. Zero-value fields are wildcards;
// From/To bound occurred_at (To exclusive).
type Filter struct {
	PartnerID   string
	KeyID       string
	Provider    string
	PublicModel string
	API         string
	From        time.Time
	To          time.Time
}

// Summary aggregates one window: request and success/error counts plus the
// token totals. Success is a fully delivered answer — a 2xx/3xx status and
// one of the two delivery outcomes ("completed" for a buffered body or a
// stream that ran to its end, "relayed" for verbatim 3xx/204/304).
// Everything else partitions into failed: upstream errors, unreachable
// providers, and the 200-status rows whose stream the client never actually
// received whole (disconnect, truncation, relay cap), which a status-only
// partition would miscount as successes. Token sums cover the rows that
// stated them.
type Summary struct {
	Requests         int64
	Succeeded        int64
	Failed           int64
	PromptTokens     int64
	CompletionTokens int64
	TotalTokens      int64
}

// Summary implements the reporting seam. It exists for internal consumers
// (a future operator surface); nothing in the request path touches it.
func (r *PGRepository) Summary(ctx context.Context, f Filter) (Summary, error) {
	var sb strings.Builder
	sb.WriteString(`SELECT
	count(*),
	count(*) FILTER (WHERE http_status >= 200 AND http_status < 400 AND outcome IN ('completed', 'relayed')),
	count(*) FILTER (WHERE NOT (http_status >= 200 AND http_status < 400 AND outcome IN ('completed', 'relayed'))),
	COALESCE(sum(prompt_tokens), 0),
	COALESCE(sum(completion_tokens), 0),
	COALESCE(sum(total_tokens), 0)
	FROM usage_events`)
	var args []any
	add := func(cond string, v any) {
		if len(args) == 0 {
			sb.WriteString(" WHERE ")
		} else {
			sb.WriteString(" AND ")
		}
		sb.WriteString(cond)
		args = append(args, v)
	}
	if f.PartnerID != "" {
		add(fmt.Sprintf("partner_id = $%d", len(args)+1), f.PartnerID)
	}
	if f.KeyID != "" {
		add(fmt.Sprintf("key_id = $%d", len(args)+1), f.KeyID)
	}
	if f.Provider != "" {
		add(fmt.Sprintf("provider = $%d", len(args)+1), f.Provider)
	}
	if f.PublicModel != "" {
		add(fmt.Sprintf("public_model = $%d", len(args)+1), f.PublicModel)
	}
	if f.API != "" {
		add(fmt.Sprintf("api = $%d", len(args)+1), f.API)
	}
	if !f.From.IsZero() {
		add(fmt.Sprintf("occurred_at >= $%d", len(args)+1), f.From)
	}
	if !f.To.IsZero() {
		add(fmt.Sprintf("occurred_at < $%d", len(args)+1), f.To)
	}

	var s Summary
	err := r.db.QueryRowContext(ctx, sb.String(), args...).
		Scan(&s.Requests, &s.Succeeded, &s.Failed,
			&s.PromptTokens, &s.CompletionTokens, &s.TotalTokens)
	if err != nil {
		return Summary{}, fmt.Errorf("usage summary failed: %w", err)
	}
	return s, nil
}

// Close releases the pool.
func (r *PGRepository) Close() error {
	return r.db.Close()
}

// StoreErrorClass reduces a repository error to a stable, log-safe token —
// the driver's error text can embed hosts, DSNs, and query fragments, so
// events carry the class only. Classified by type, never message text; the
// same vocabulary the key store applies to its own backend errors.
func StoreErrorClass(err error) string {
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	case errors.Is(err, context.Canceled):
		return "canceled"
	case errors.Is(err, sql.ErrConnDone), errors.Is(err, driver.ErrBadConn):
		return "unavailable"
	default:
		var ne net.Error
		if errors.As(err, &ne) {
			return "unavailable"
		}
		return "query_failed"
	}
}
