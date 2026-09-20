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
// published. The hash baseline is the boot content itself, seeded by the
// caller — Run never re-reads the file to establish it, so a file rewritten
// or made unreadable in the window between boot load and Run is still
// observed as a change rather than swallowed into the baseline.
//
// Any read, parse, or validation failure keeps the last-known-good snapshot
// serving and is logged on the transition into failure only — a persistently
// broken file would otherwise emit one identical error line per tick,
// 86,400 lines a day at the default interval. The failure is tracked by
// kind (unreadable vs rejected), so a file that starts failing in a new way
// warns again; while one kind persists, a debug-level line documents each
// retry, and recovery is logged again. onPublish, when non-nil, runs after
// every successful publish — the hook that applies the snapshot's
// hot-reloadable log level.
type Poller struct {
	store     *Store
	path      string
	seed      []byte
	interval  time.Duration
	log       zerolog.Logger
	onPublish func(*Snapshot)
}

// Failure kinds — the states a poll cycle can be in between healthy ticks.
// An empty string is the healthy state.
const (
	failingUnreadable = "unreadable"
	failingRejected   = "rejected"
)

// NewPoller constructs a poller. seed is the exact runtime-file content the
// caller loaded at startup: the hash baseline the first ticks compare
// against. interval must be positive; onPublish may be nil.
func NewPoller(store *Store, path string, seed []byte, interval time.Duration, log zerolog.Logger, onPublish func(*Snapshot)) *Poller {
	return &Poller{store: store, path: path, seed: seed, interval: interval, log: log, onPublish: onPublish}
}

// Run polls until ctx is cancelled. The first ticks compare against the
// boot content's hash, so a file that never changes keeps the generation it
// was loaded with at startup.
func (p *Poller) Run(ctx context.Context) {
	last := sha256.Sum256(p.seed)

	failingKind := ""
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
			p.enterFailure(&failingKind, failingUnreadable, err)
			continue
		}
		h := sha256.Sum256(data)
		if h == last {
			if failingKind != "" {
				// The file is back — and byte-identical to the last-known-good
				// content, so there is nothing to republish.
				p.log.Info().Str("file", p.path).Msg("config_file_recovered")
			} else {
				p.log.Debug().Msg("config_unchanged")
			}
			failingKind = ""
			continue
		}

		next, err := LoadRuntime(data)
		if err != nil {
			p.enterFailure(&failingKind, failingRejected, err)
			continue
		}
		if err := p.store.Publish(next); err != nil {
			// Unreachable: Publish rejects only a nil snapshot.
			p.log.Error().Err(err).Msg("config_publish_failed")
			continue
		}
		last = h
		failingKind = ""
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

// enterFailure handles one failed poll cycle of the given kind: a WARN when
// the failure is new (healthy, or a different kind than the previous
// cycle), a debug heartbeat when the same kind persists. The slugs carry
// the kind so an operator can tell a vanished file from a rejected one.
func (p *Poller) enterFailure(kind *string, newKind string, err error) {
	if *kind != newKind {
		*kind = newKind
		p.log.Warn().Err(err).Str("file", p.path).Msg(p.failureSlug(newKind, false))
		return
	}
	p.log.Debug().Err(err).Str("file", p.path).Msg(p.failureSlug(newKind, true))
}

// failureSlug maps a failure kind and persistence onto its event slug.
func (p *Poller) failureSlug(kind string, still bool) string {
	if still {
		if kind == failingUnreadable {
			return "config_still_unreadable"
		}
		return "config_reload_still_rejected"
	}
	if kind == failingUnreadable {
		return "config_file_unreadable"
	}
	return "config_reload_rejected"
}
