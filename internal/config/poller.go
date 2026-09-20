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
// published. Any read, parse, or validation failure is logged and the
// last-known-good snapshot keeps serving. A temporarily missing file is
// treated as a reload failure, never as a reset.
type Poller struct {
	store    *Store
	path     string
	interval time.Duration
	log      zerolog.Logger
}

// NewPoller constructs a poller. interval must be positive.
func NewPoller(store *Store, path string, interval time.Duration, log zerolog.Logger) *Poller {
	return &Poller{store: store, path: path, interval: interval, log: log}
}

// Run polls until ctx is cancelled. It starts by recording the hash of the
// current file without republishing, so a file that never changes keeps the
// generation it was loaded with at startup.
func (p *Poller) Run(ctx context.Context) {
	var last [sha256.Size]byte
	if data, err := os.ReadFile(p.path); err == nil {
		last = sha256.Sum256(data)
	} else {
		p.log.Warn().Err(err).Str("file", p.path).Msg("config poll: initial read failed; will retry")
	}

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
			p.log.Error().Err(err).Str("file", p.path).Msg("config reload: file unreadable; keeping last-known-good")
			continue
		}
		h := sha256.Sum256(data)
		if h == last {
			continue
		}

		next, err := LoadRuntime(data)
		if err != nil {
			p.log.Error().Err(err).Msg("config reload: invalid file; keeping last-known-good")
			continue
		}
		if err := p.store.Publish(next); err != nil {
			p.log.Error().Err(err).Msg("config reload: publish failed; keeping last-known-good")
			continue
		}
		last = h
		p.log.Info().Uint64("generation", next.Gen()).Msg("config reloaded")
	}
}
