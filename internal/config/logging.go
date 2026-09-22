package config

import (
	"fmt"

	"github.com/rs/zerolog"
)

// DefaultLogLevel is the effective level when the runtime file declares no
// log-level key.
const DefaultLogLevel = zerolog.InfoLevel

// LogLevelHook returns the poller's onPublish hook that applies each
// published snapshot's log level process-wide and acknowledges the change.
//
// The ack is emitted after the global level swaps, at the new level — the
// only severity guaranteed visible under the level it announces. zerolog
// drops events below the global level, so an ack emitted at a fixed
// severity would vanish on exactly the transitions an operator needs
// confirmed: anything->error drops an info or warn ack, warn->info drops a
// debug ack. config_reloaded (logged before the swap, at info) is likewise
// suppressed while the old level is warn or above — with the ack at the new
// level, every successful transition still yields at least one visible
// confirmation. previous_level names what the swap replaced, so the full
// transition reads from one line.
func LogLevelHook(log zerolog.Logger) func(*Snapshot) {
	return func(next *Snapshot) {
		prev := zerolog.GlobalLevel()
		zerolog.SetGlobalLevel(next.LogLevel())
		log.WithLevel(next.LogLevel()).
			Uint64("generation", next.Gen()).
			Str("previous_level", prev.String()).
			Str("log_level", next.LogLevel().String()).
			Msg("log_level_applied")
	}
}

// ParseLogLevel maps the runtime file's top-level log-level value onto a
// zerolog level. Accepted exactly (same spelling and strictness as the org's
// other Go services — no alias, no case folding, no whitespace trimming):
// debug, info, warn, error; empty selects the default. Anything else is a
// reject — the whole runtime file is invalid and last-known-good keeps
// serving, so a typo can never silently silence the service. The rejection
// message names the legal levels and the value's length, never the value
// itself: error text reaches logs verbatim and a botched paste into this
// position may hold credentials.
func ParseLogLevel(s string) (zerolog.Level, error) {
	switch s {
	case "":
		return DefaultLogLevel, nil
	case "debug":
		return zerolog.DebugLevel, nil
	case "info":
		return zerolog.InfoLevel, nil
	case "warn":
		return zerolog.WarnLevel, nil
	case "error":
		return zerolog.ErrorLevel, nil
	default:
		return DefaultLogLevel, fmt.Errorf("log-level has %d characters and must be one of debug, info, warn, error", len(s))
	}
}
