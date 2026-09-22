package auth

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib" // registers the "pgx" database/sql driver
)

// PostgreSQL storage for partner keys, over database/sql + pgx. The store
// is SQL-first: no ORM, explicit statements, and every failure surfaced as
// an error the caller must deny on — there is no read path in this file
// that can return a success-shaped answer from a failed query.
//
// last_used_at is deliberately off the request path: LookupByHash never
// writes. A buffered channel feeds one flusher goroutine that batches
// touches; under pressure the channel drops touches (never requests), so
// metering pressure can never become proxy latency.

const (
	// touchQueueCap bounds the pending-touch buffer. Full means "drop the
	// touch", not "block the request": last_used_at is advisory.
	touchQueueCap = 1024
	// flushEvery is the batch window for the flusher.
	flushEvery = time.Second
	// flushBatch is how many distinct keys one flush writes at most.
	flushBatch = 256
)

// PGStore implements KeyStore against PostgreSQL/TimescaleDB.
type PGStore struct {
	db      *sql.DB
	touches chan touch
	done    chan struct{}
	close   sync.Once
}

// touch is one pending last_used_at update.
type touch struct {
	keyID string
	at    time.Time
}

// NewPGStore opens the connection pool, pings it, brings the schema up to
// the embedded migrations, and validates the column contract. A pool that
// cannot be reached or brought to the expected schema is a construction
// failure, not a degrade-and-warn: partner mode without a reachable,
// correct store would deny everything anyway, so the process refuses to
// start instead (fail closed). This is the only DDL the store ever issues —
// the request path is queries only.
func NewPGStore(ctx context.Context, databaseURL string) (*PGStore, error) {
	db, err := sql.Open("pgx", databaseURL)
	if err != nil {
		return nil, err
	}
	// The request path issues one lookup per authentication; a small pool
	// is plenty, and bounding it keeps a slow database from ballooning
	// connections.
	db.SetMaxOpenConns(8)
	db.SetMaxIdleConns(4)
	db.SetConnMaxIdleTime(5 * time.Minute)
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	if err := EnsureSchema(ctx, db); err != nil {
		_ = db.Close()
		return nil, err
	}
	s := &PGStore{
		db:      db,
		touches: make(chan touch, touchQueueCap),
		done:    make(chan struct{}),
	}
	go s.flushLoop()
	return s, nil
}

// LookupByHash implements KeyStore.
func (s *PGStore) LookupByHash(ctx context.Context, hash []byte) (KeyRecord, error) {
	var rec KeyRecord
	var revoked, lastUsed sql.NullTime
	err := s.db.QueryRowContext(ctx, `
		SELECT key_id, partner_id, status, created_at, revoked_at, last_used_at
		FROM partner_api_keys
		WHERE key_hash = $1`, hash).
		Scan(&rec.KeyID, &rec.PartnerID, &rec.Status, &rec.CreatedAt, &revoked, &lastUsed)
	if errors.Is(err, sql.ErrNoRows) {
		return KeyRecord{}, ErrKeyNotFound
	}
	if err != nil {
		return KeyRecord{}, err
	}
	if revoked.Valid {
		t := revoked.Time
		rec.RevokedAt = &t
	}
	if lastUsed.Valid {
		t := lastUsed.Time
		rec.LastUsedAt = &t
	}
	return rec, nil
}

// CreateKey implements KeyStore.
func (s *PGStore) CreateKey(ctx context.Context, rec KeyRecord, hash []byte) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO partner_api_keys (key_id, partner_id, key_hash, status, created_at)
		VALUES ($1, $2, $3, $4, $5)`,
		rec.KeyID, rec.PartnerID, hash, rec.Status, rec.CreatedAt)
	if err != nil {
		return fmt.Errorf("key store rejected the insert: %w", err)
	}
	return nil
}

// ListKeys implements KeyStore. No hash column is selected — the secret's
// only storable form never leaves the table.
func (s *PGStore) ListKeys(ctx context.Context) ([]KeyRecord, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT key_id, partner_id, status, created_at, revoked_at, last_used_at
		FROM partner_api_keys
		ORDER BY created_at DESC, key_id`)
	if err != nil {
		return nil, fmt.Errorf("key store listing failed: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []KeyRecord
	for rows.Next() {
		var rec KeyRecord
		var revoked, lastUsed sql.NullTime
		if err := rows.Scan(&rec.KeyID, &rec.PartnerID, &rec.Status, &rec.CreatedAt, &revoked, &lastUsed); err != nil {
			return nil, fmt.Errorf("key store listing failed: %w", err)
		}
		if revoked.Valid {
			t := revoked.Time
			rec.RevokedAt = &t
		}
		if lastUsed.Valid {
			t := lastUsed.Time
			rec.LastUsedAt = &t
		}
		out = append(out, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("key store listing failed: %w", err)
	}
	return out, nil
}

// RevokeKey implements KeyStore. The UPDATE is conditional on the key being
// currently active, so the boolean answers the CLI's question honestly:
// true = this call flipped the key, false = it was already revoked or never
// existed.
func (s *PGStore) RevokeKey(ctx context.Context, keyID string) (bool, error) {
	tag, err := s.db.ExecContext(ctx, `
		UPDATE partner_api_keys
		SET status = $1, revoked_at = COALESCE(revoked_at, now())
		WHERE key_id = $2 AND status = $3`,
		StatusRevoked, keyID, StatusActive)
	if err != nil {
		return false, fmt.Errorf("key store revocation failed: %w", err)
	}
	n, err := tag.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("key store revocation failed: %w", err)
	}
	return n > 0, nil
}

// TouchLastUsed implements KeyStore: fire-and-forget, drop on full. The
// latest touch per key wins in the flusher's batch, so a hot key produces
// one UPDATE per window, not one per request.
func (s *PGStore) TouchLastUsed(keyID string, at time.Time) {
	select {
	case s.touches <- touch{keyID: keyID, at: at}:
	default:
	}
}

// flushLoop batches pending touches. It drains what is queued when the
// window elapses or the batch fills, writing GREATEST(existing, incoming)
// so an out-of-order flush can never move last_used_at backwards.
func (s *PGStore) flushLoop() {
	defer close(s.done)
	ticker := time.NewTicker(flushEvery)
	defer ticker.Stop()
	pending := make(map[string]time.Time, flushBatch)
	flush := func() {
		if len(pending) == 0 {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		for keyID, at := range pending {
			_, err := s.db.ExecContext(ctx, `
				UPDATE partner_api_keys
				SET last_used_at = GREATEST(COALESCE(last_used_at, to_timestamp(0)), $1)
				WHERE key_id = $2`, at, keyID)
			if err != nil {
				// Advisory column: a failed touch is dropped, never retried
				// into the request path, and never fatal to the flusher.
				break
			}
		}
		cancel()
		clear(pending)
	}
	for {
		select {
		case t, ok := <-s.touches:
			if !ok {
				flush()
				return
			}
			pending[t.keyID] = t.at
			if len(pending) >= flushBatch {
				flush()
			}
		case <-ticker.C:
			flush()
		}
	}
}

// Close implements KeyStore: stops the flusher, waits for the final batch,
// then closes the pool.
func (s *PGStore) Close(ctx context.Context) error {
	var err error
	s.close.Do(func() {
		close(s.touches)
		select {
		case <-s.done:
		case <-ctx.Done():
		}
		err = s.db.Close()
	})
	return err
}
