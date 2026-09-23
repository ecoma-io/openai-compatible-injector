// Package config implements the two configuration planes of the injector:
//
//   - Bootstrap settings, read once from the environment and immutable for
//     the lifetime of the process (listen address, config file path, poll
//     interval, shutdown grace). Every variable carries the OAICR_ prefix;
//     the service reads no unprefixed names of its own.
//   - Runtime settings, validated from a YAML file and hot-reloaded into
//     immutable snapshots that are atomically swapped.
package config

import (
	"errors"
	"fmt"
	"time"
)

// Bootstrap holds process-lifetime settings. Every field is read exclusively
// from the environment (OAICR_-prefixed variables only); none of them are
// part of the runtime YAML schema. Changing any of them requires a process
// restart.
type Bootstrap struct {
	// Listen is the TCP address the HTTP server binds, e.g. ":8080".
	Listen string
	// ConfigFile is the path of the runtime YAML configuration.
	ConfigFile string
	// PollInterval is how often the config file is checked for changes.
	PollInterval time.Duration
	// ShutdownGrace is how long http.Server.Shutdown waits for in-flight
	// requests (including active SSE streams) before force-closing them.
	ShutdownGrace time.Duration
	// AuthDatabaseURL is the PostgreSQL/TimescaleDB connection string of
	// the partner key store. Empty (the default) keeps static mode: the
	// runtime YAML's api-key authenticates every client. Non-empty opts the
	// process into partner mode, where /v1 requests authenticate against
	// hashed per-partner keys and the YAML api-key is not honored on the
	// wire. Infrastructure, not policy: it never hot-reloads, and the
	// connection string itself is never logged.
	AuthDatabaseURL string
	// UsageDatabaseURL is the PostgreSQL/TimescaleDB connection string of
	// the usage-event store. Empty (the default) turns metering off
	// entirely: no events, no connection, byte-identical traffic. Non-empty
	// opts into async usage metering — one factual row per request that
	// reaches the provider path, written off the response path. Like the
	// auth DSN: infrastructure, never hot-reloads, never logged.
	UsageDatabaseURL string
}

// Default bootstrap values.
const (
	DefaultListen        = ":8080"
	DefaultConfigFile    = "/config/config.yaml"
	DefaultPollInterval  = time.Second
	DefaultShutdownGrace = 55 * time.Second
)

// LoadBootstrap reads and validates bootstrap settings from the environment.
// Absent variables fall back to defaults; present-but-invalid values are a
// startup error — the process must not start with a half-understood listen
// address or interval.
func LoadBootstrap(env func(string) string) (Bootstrap, error) {
	var b Bootstrap
	var errs []error

	b.Listen = env("OAICR_LISTEN")
	if b.Listen == "" {
		b.Listen = DefaultListen
	}

	b.ConfigFile = env("OAICR_CONFIG_FILE")
	if b.ConfigFile == "" {
		b.ConfigFile = DefaultConfigFile
	}

	if v := env("OAICR_CONFIG_POLL_INTERVAL"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			// time.ParseDuration quotes the raw input; error text reaches
			// logs verbatim, so the value is not echoed (length only).
			errs = append(errs, fmt.Errorf("OAICR_CONFIG_POLL_INTERVAL is not a duration (%d characters; e.g. 500ms, 5s)", len(v)))
		} else if d <= 0 {
			errs = append(errs, errors.New("OAICR_CONFIG_POLL_INTERVAL must be a positive duration"))
		} else {
			b.PollInterval = d
		}
	} else {
		b.PollInterval = DefaultPollInterval
	}

	if v := env("OAICR_SHUTDOWN_GRACE"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			errs = append(errs, fmt.Errorf("OAICR_SHUTDOWN_GRACE is not a duration (%d characters; e.g. 55s)", len(v)))
		} else if d <= 0 {
			// Zero silently disables the graceful drain — in-flight streams
			// would be force-closed the moment shutdown starts. That must be
			// a startup error, not a surprise.
			errs = append(errs, errors.New("OAICR_SHUTDOWN_GRACE must be a positive duration"))
		} else {
			b.ShutdownGrace = d
		}
	} else {
		b.ShutdownGrace = DefaultShutdownGrace
	}

	// No format validation here: a malformed connection string fails at
	// store open during startup — the one place a broken auth backend can
	// stop the process instead of silently degrading it. The value itself
	// is never echoed, not even its length: a DSN embeds a password, and
	// the credential rule is level-independent. The usage DSN follows the
	// same discipline.
	b.AuthDatabaseURL = env("OAICR_AUTH_DATABASE_URL")
	b.UsageDatabaseURL = env("OAICR_USAGE_DATABASE_URL")

	return b, errors.Join(errs...)
}
