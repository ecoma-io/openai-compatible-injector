package config

import (
	"fmt"
	"strings"

	"github.com/rs/zerolog"
)

// DefaultLogLevel is the effective level when the runtime file declares no
// logging section.
const DefaultLogLevel = zerolog.InfoLevel

// ParseLogLevel maps the runtime file's logging.level value onto a zerolog
// level. Accepted (case-insensitive, surrounding whitespace ignored):
// debug, info, warn, warning (alias for warn), error; empty selects the
// default. Anything else is a reject — the whole runtime file is invalid and
// last-known-good keeps serving, so a typo can never silently silence the
// service. The rejection message names the legal levels and the value's
// length, never the value itself: error text reaches logs verbatim and a
// botched paste into this position may hold credentials.
func ParseLogLevel(s string) (zerolog.Level, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "":
		return DefaultLogLevel, nil
	case "debug":
		return zerolog.DebugLevel, nil
	case "info":
		return zerolog.InfoLevel, nil
	case "warn", "warning":
		return zerolog.WarnLevel, nil
	case "error":
		return zerolog.ErrorLevel, nil
	default:
		return DefaultLogLevel, fmt.Errorf("logging.level has %d characters and must be one of debug, info, warn, warning, error", len(s))
	}
}
