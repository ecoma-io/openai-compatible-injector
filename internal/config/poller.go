package config

import (
	"context"
	"crypto/sha256"
	"os"
	"time"

	"github.com/rs/zerolog"
)

// Poller watches the runtime config file and republishes validated snapshots.
// It polls by content hash every PollInterval: an unchanged file is skipped
// without re-parsing, and a changed file is fully re-validated before being
// published. Any read, parse, or validation failure keeps the
// last-known-good snapshot serving and is logged on the transition into
// failure only — a persistently broken file would otherwise emit one
// identical error line per tick, 86,400 lines a day at the default
// interval; while a failure persists, a debug-level line documents each
// retry, and recovery is logged again. onPublish, when non-nil, runs after
// every successful publish — the hook that applies the snapshot's
// hot-reloadable log level.
type Poller struct {
	store     *Store
	path      string
	interval  time.Duration
	log       zerolog.Logger
	onPublish func(*Snapshot)
}

// NewPoller constructs a poller. interval must be positive; onPublish may
// be nil.
func NewPoller(store *Store, path string, interval time.Duration, log zerolog.Logger, onPublish func(*Snapshot)) *Poller {
	return &Poller{store: store, path: path, interval: interval, log: log, onPublish: onPublish}
}

// Run polls until ctx is cancelled. It starts by recording the hash of the
// current file without republishing, so a file that never changes keeps the
// generation it was loaded with at startup.
func (p *Poller) Run(ctx context.Context) {
	var last [sha256.Size]byte
	if data, err := os.ReadFile(p.path); err == nil {
		last = sha256.Sum256(data)
	} else {
		p.log.Warn().Err(err).Str("file", p.path).Msg("config_initial_read_failed")
	}

	failing := false
	t := time.NewTicker(p.interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}

		data, err := os.ReadFile(p.path)
		if err != nil {
			if !failing {
				failing = true
				p.log.Warn().Err(err).Str("file", p.path).Msg("config_file_unreadable")
			} else {
				p.log.Debug().Err(err).Str("file", p.path).Msg("config_still_unreadable")
			}
			continue
		}
		h := sha256.Sum256(data)
		if h == last {
			if failing {
				// The file is back — and byte-identical to the last-known-good
				// content, so there is nothing to republish.
				failing = false
				p.log.Info().Str("file", p.path).Msg("config_file_recovered")
			} else {
				p.log.Debug().Msg("config_unchanged")
			}
			continue
		}

		next, err := LoadRuntime(data)
		if err != nil {
			if !failing {
				failing = true
				p.log.Warn().Err(err).Str("file", p.path).Msg("config_reload_rejected")
			} else {
				p.log.Debug().Err(err).Str("file", p.path).Msg("config_reload_still_rejected")
			}
			continue
		}
		if err := p.store.Publish(next); err != nil {
			// Unreachable: Publish rejects only a nil snapshot.
			p.log.Error().Err(err).Msg("config_publish_failed")
			continue
		}
		last = h
		failing = false
		p.log.Info().
			Uint64("generation", next.Gen()).
			Int("model_count", next.Len()).
			Str("log_level", next.LogLevel().String()).
			Msg("config_reloaded")
		if p.onPublish != nil {
			p.onPublish(next)
		}
	}
}
