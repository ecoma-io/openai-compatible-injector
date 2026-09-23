package usage

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"
)

// Structure test, no database: the embedded usage migration set is
// well-formed before any SQL touches a server.

func TestEmbeddedUsageMigrationsAreWellFormed(t *testing.T) {
	migrations, err := parseUsageMigrations()
	if err != nil {
		t.Fatalf("parseUsageMigrations: %v", err)
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
	return isolatedTestURL(t, rawURL, "usage")
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

func resetUsageSchema(t *testing.T, url string) {
	t.Helper()
	db, err := sql.Open("pgx", url)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for _, table := range []string{"usage_events", "schema_migrations"} {
		if _, err := db.ExecContext(ctx, "DROP TABLE IF EXISTS "+table); err != nil {
			t.Fatalf("drop %s: %v", table, err)
		}
	}
}

func TestPGUsageRoundTrip(t *testing.T) {
	url := integrationDB(t)
	resetUsageSchema(t, url)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	repo, err := NewPGRepository(ctx, url)
	if err != nil {
		t.Fatalf("NewPGRepository: %v", err)
	}
	defer func() { _ = repo.Close() }()

	base := time.Now().UTC().Add(-time.Hour)
	batch := []Event{
		{EventID: NewEventID(), OccurredAt: base, PartnerID: "acme", KeyID: "pak_a", RequestID: "r1",
			ConfigGeneration: 7, PublicModel: "public-xl", Provider: "prov-a", UpstreamModel: "up-a",
			API: "chat", Stream: false, HTTPStatus: 200, Outcome: "completed",
			PromptTokens: i64(100), CompletionTokens: i64(50), TotalTokens: i64(150),
			BytesIn: 1200, BytesOut: 3400, ProviderAttempts: 1, EgressAttempts: 2, EgressKind: "socks5", LatencyMS: 120},
		{EventID: NewEventID(), OccurredAt: base.Add(time.Minute), PartnerID: "acme", KeyID: "pak_a", RequestID: "r2",
			ConfigGeneration: 7, PublicModel: "public-mini", Provider: "prov-b", UpstreamModel: "up-b",
			API: "responses", Stream: true, HTTPStatus: 429, Outcome: "upstream_http_error",
			BytesIn: 900, BytesOut: 130, ProviderAttempts: 1, EgressAttempts: 2, EgressKind: "http", LatencyMS: 45},
		{EventID: NewEventID(), OccurredAt: base.Add(2 * time.Minute), PartnerID: "other", KeyID: "pak_z", RequestID: "r3",
			ConfigGeneration: 9, PublicModel: "public-xl", Provider: "prov-a", UpstreamModel: "up-a",
			API: "chat", Stream: false, HTTPStatus: 502, Outcome: "upstream_unreachable",
			BytesIn: 800, BytesOut: 96, ProviderAttempts: 2, EgressAttempts: 3, EgressKind: "socks5", LatencyMS: 500},
		// A committed 200 whose stream the client never received whole, and
		// a client cancel before any header: neither is a success, whatever
		// a status-only partition would say.
		{EventID: NewEventID(), OccurredAt: base.Add(3 * time.Minute), PartnerID: "acme", KeyID: "pak_a", RequestID: "r4",
			ConfigGeneration: 7, PublicModel: "public-mini", Provider: "prov-b", UpstreamModel: "up-b",
			API: "chat", Stream: true, HTTPStatus: 200, Outcome: "client_disconnected",
			BytesIn: 700, BytesOut: 512, ProviderAttempts: 1, EgressAttempts: 1, EgressKind: "direct", LatencyMS: 900},
		{EventID: NewEventID(), OccurredAt: base.Add(4 * time.Minute), PartnerID: "other", KeyID: "pak_z", RequestID: "r5",
			ConfigGeneration: 9, PublicModel: "public-xl", Provider: "prov-a", UpstreamModel: "up-a",
			API: "chat", Stream: false, HTTPStatus: 0, Outcome: "client_disconnected",
			ProviderAttempts: 1, EgressAttempts: 0, LatencyMS: 5},
	}
	if err := repo.InsertEvents(ctx, batch); err != nil {
		t.Fatalf("InsertEvents: %v", err)
	}

	// Everything: only the fully relayed answer is a success — the
	// disconnect rows (one committed 200, one status-less) are failures.
	s, err := repo.Summary(ctx, Filter{})
	if err != nil {
		t.Fatalf("Summary(all): %v", err)
	}
	if s.Requests != 5 || s.Succeeded != 1 || s.Failed != 4 {
		t.Fatalf("Summary(all) counts = %d/%d/%d, want 5/1/4", s.Requests, s.Succeeded, s.Failed)
	}
	if s.PromptTokens != 100 || s.CompletionTokens != 50 || s.TotalTokens != 150 {
		t.Fatalf("Summary(all) tokens = %d/%d/%d, want only the stated usage", s.PromptTokens, s.CompletionTokens, s.TotalTokens)
	}

	// By partner.
	s, err = repo.Summary(ctx, Filter{PartnerID: "acme"})
	if err != nil {
		t.Fatalf("Summary(acme): %v", err)
	}
	if s.Requests != 3 || s.Succeeded != 1 || s.Failed != 2 {
		t.Fatalf("Summary(acme) counts = %d/%d/%d, want 3/1/2", s.Requests, s.Succeeded, s.Failed)
	}

	// By model.
	s, err = repo.Summary(ctx, Filter{PublicModel: "public-xl"})
	if err != nil {
		t.Fatalf("Summary(model): %v", err)
	}
	if s.Requests != 3 {
		t.Fatalf("Summary(model) requests = %d, want 3", s.Requests)
	}

	// By provider and API.
	s, err = repo.Summary(ctx, Filter{Provider: "prov-a", API: "chat"})
	if err != nil {
		t.Fatalf("Summary(provider): %v", err)
	}
	if s.Requests != 3 {
		t.Fatalf("Summary(provider) requests = %d, want 3", s.Requests)
	}

	// Time window (To exclusive).
	s, err = repo.Summary(ctx, Filter{From: base.Add(30 * time.Second), To: base.Add(90 * time.Second)})
	if err != nil {
		t.Fatalf("Summary(window): %v", err)
	}
	if s.Requests != 1 {
		t.Fatalf("Summary(window) requests = %d, want 1", s.Requests)
	}

	// Absent usage must read back as NULL, not zero — verify at the SQL
	// level where the distinction lives.
	verify, err := sql.Open("pgx", url)
	if err != nil {
		t.Fatalf("verify open: %v", err)
	}
	defer func() { _ = verify.Close() }()
	var prompt sql.NullInt64
	if err := verify.QueryRowContext(ctx,
		`SELECT prompt_tokens FROM usage_events WHERE request_id = 'r2'`).Scan(&prompt); err != nil {
		t.Fatalf("verify query: %v", err)
	}
	if prompt.Valid {
		t.Fatalf("absent usage read back as %d, want NULL", prompt.Int64)
	}

	// The whole row round-trips: identity, walk and egress facts, wire
	// sizes, timing — every column the insert statement writes is one a
	// report will read.
	var (
		partnerID, keyID, reqID        string
		gen                            int64
		publicModel, provider, upModel string
		apiVal                         string
		stream                         bool
		status                         int
		outcomeVal, egressKind         string
		statedPrompt                   sql.NullInt64
		bytesIn, bytesOut, latency     int64
		provAtt, egrAtt                int
	)
	if err := verify.QueryRowContext(ctx, `
		SELECT partner_id, key_id, request_id, config_generation,
		       public_model, provider, upstream_model, api, stream,
		       http_status, outcome, prompt_tokens,
		       bytes_in, bytes_out, provider_attempts, egress_attempts,
		       egress_kind, latency_ms
		FROM usage_events WHERE request_id = 'r1'`).Scan(
		&partnerID, &keyID, &reqID, &gen,
		&publicModel, &provider, &upModel, &apiVal, &stream,
		&status, &outcomeVal, &statedPrompt,
		&bytesIn, &bytesOut, &provAtt, &egrAtt,
		&egressKind, &latency); err != nil {
		t.Fatalf("row read-back: %v", err)
	}
	if partnerID != "acme" || keyID != "pak_a" || reqID != "r1" || gen != 7 ||
		publicModel != "public-xl" || provider != "prov-a" || upModel != "up-a" ||
		apiVal != "chat" || stream || status != 200 || outcomeVal != "completed" ||
		!statedPrompt.Valid || statedPrompt.Int64 != 100 ||
		bytesIn != 1200 || bytesOut != 3400 ||
		provAtt != 1 || egrAtt != 2 || egressKind != "socks5" || latency != 120 {
		t.Fatalf("row round-trip mismatch: %+v", map[string]any{
			"partner": partnerID, "key": keyID, "request": reqID, "gen": gen,
			"model": publicModel, "provider": provider, "upstream": upModel,
			"api": apiVal, "stream": stream, "status": status, "outcome": outcomeVal,
			"prompt": statedPrompt, "bytes_in": bytesIn, "bytes_out": bytesOut,
			"provider_attempts": provAtt, "egress_attempts": egrAtt,
			"egress_kind": egressKind, "latency_ms": latency,
		})
	}
}

func TestPGUsageSchemaIsIdempotent(t *testing.T) {
	url := integrationDB(t)
	resetUsageSchema(t, url)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for run := 0; run < 2; run++ {
		repo, err := NewPGRepository(ctx, url)
		if err != nil {
			t.Fatalf("run %d: NewPGRepository: %v", run, err)
		}
		if err := repo.Close(); err != nil {
			t.Fatalf("run %d: Close: %v", run, err)
		}
	}

	// The auth module's version space is untouched by usage bookkeeping —
	// and vice versa: one table, independent version spaces.
	verify, err := sql.Open("pgx", url)
	if err != nil {
		t.Fatalf("verify open: %v", err)
	}
	defer func() { _ = verify.Close() }()
	var modules []string
	rows, err := verify.QueryContext(ctx, `SELECT DISTINCT module FROM schema_migrations`)
	if err != nil {
		t.Fatalf("verify query: %v", err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var m string
		if err := rows.Scan(&m); err != nil {
			t.Fatalf("scan: %v", err)
		}
		modules = append(modules, m)
	}
	if len(modules) != 1 || modules[0] != "usage" {
		t.Fatalf("bookkeeping modules = %v, want exactly [usage]", modules)
	}
}

func TestPGUsageRepositoryFailsClosedOnForeignSchema(t *testing.T) {
	url := integrationDB(t)
	resetUsageSchema(t, url)

	setup, err := sql.Open("pgx", url)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := setup.ExecContext(ctx, `CREATE TABLE usage_events (event_id uuid)`); err != nil {
		t.Fatalf("create foreign table: %v", err)
	}
	_ = setup.Close()

	if _, err := NewPGRepository(ctx, url); err == nil {
		t.Fatal("NewPGRepository accepted a foreign usage_events schema — startup must fail closed")
	}
}

// StoreErrorClass reduces driver-shaped failures to log-safe tokens. The
// classification is by type, never message text — the same vocabulary the
// key store applies.

func TestStoreErrorClass(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want string
	}{
		{"deadline", context.DeadlineExceeded, "timeout"},
		{"wrapped deadline", fmt.Errorf("insert: %w", context.DeadlineExceeded), "timeout"},
		{"canceled", context.Canceled, "canceled"},
		{"bad conn", driver.ErrBadConn, "unavailable"},
		{"conn done", sql.ErrConnDone, "unavailable"},
		{"network", &net.OpError{Op: "dial", Err: errors.New("connection refused")}, "unavailable"},
		{"other", errors.New("syntax error at or near \"usage\""), "query_failed"},
	}
	for _, tc := range cases {
		if got := StoreErrorClass(tc.err); got != tc.want {
			t.Errorf("%s: StoreErrorClass = %q, want %q", tc.name, got, tc.want)
		}
	}
}
