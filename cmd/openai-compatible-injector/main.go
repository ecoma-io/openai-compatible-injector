// Command openai-compatible-injector is the CLI entrypoint of an
// OpenAI-compatible API proxy that rewrites model names and injects prompts
// based on runtime configuration that can be hot-reloaded.
package main

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/rs/zerolog"

	"openai-compatible-injector/internal/config"
	"openai-compatible-injector/internal/server"
)

// version is the build version, overridable at link time with
// -ldflags "-X main.version=<tag>".
var version = "dev"

func main() {
	if len(os.Args) > 1 {
		switch os.Args[1] {
		case "version":
			fmt.Println(version)
			return
		case "healthcheck":
			os.Exit(healthcheck())
		default:
			usage()
			os.Exit(2)
		}
	}
	os.Exit(run())
}

func usage() {
	fmt.Fprintf(os.Stderr, "usage: %s [version|healthcheck]\n", os.Args[0])
}

// healthcheck probes the running service's /healthz endpoint without reading
// any configuration: a bad reload must never turn a healthy process into a
// failing probe.
func healthcheck() int {
	log := zerolog.New(os.Stderr).With().Timestamp().Logger()

	addr := os.Getenv("LISTEN")
	if addr == "" {
		addr = config.DefaultListen
	}
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		log.Error().Err(err).Str("listen", addr).Msg("healthcheck_invalid_listen")
		return 1
	}
	// A wildcard or hostless address binds all interfaces; probe loopback.
	if host == "" || host == "::" || host == "0.0.0.0" {
		host = "127.0.0.1"
	}
	url := "http://" + net.JoinHostPort(host, port) + "/healthz"

	// An empty Transport ignores HTTP_PROXY and friends: the probe must
	// reach this process directly, never detour through a proxy that may be
	// configured in the environment.
	client := &http.Client{Timeout: 2 * time.Second, Transport: &http.Transport{}}
	resp, err := client.Get(url)
	if err != nil {
		log.Error().Err(err).Msg("healthcheck_probe_failed")
		return 1
	}
	defer func() { _ = resp.Body.Close() }()
	// Bounded read: /healthz answers "ok\n", but a probe aimed at the wrong
	// port can hit anything, and its answer is never logged — only its
	// status and size.
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		log.Error().Err(err).Msg("healthcheck_body_read_failed")
		return 1
	}
	if resp.StatusCode != http.StatusOK || string(body) != "ok\n" {
		log.Error().Int("status", resp.StatusCode).Int("body_len", len(body)).Msg("healthcheck_unhealthy")
		return 1
	}
	return 0
}

// newLogger builds the stderr JSON logger. The base logger is maximally
// permissive (TraceLevel): zerolog consults both the logger level and the
// global level per event, so the global level — seeded from the runtime
// file's logging.level and re-applied on every config reload — acts as the
// single live control. LOG_LEVEL does not exist: the runtime YAML is
// mandatory at boot, so an environment variable had no legitimate window
// and two sources of truth for one setting.
func newLogger() zerolog.Logger {
	return zerolog.New(os.Stderr).With().Timestamp().Logger().Level(zerolog.TraceLevel)
}

func run() int {
	log := newLogger()

	b, err := config.LoadBootstrap(os.Getenv)
	if err != nil {
		log.Fatal().Err(err).Msg("bootstrap_load_failed")
	}

	data, err := os.ReadFile(b.ConfigFile)
	if err != nil {
		log.Fatal().Err(err).Str("file", b.ConfigFile).Msg("config_file_read_failed")
	}
	snap, err := config.LoadRuntime(data)
	if err != nil {
		log.Fatal().Err(err).Str("file", b.ConfigFile).Msg("config_load_failed")
	}
	zerolog.SetGlobalLevel(snap.LogLevel())

	store := config.NewStore(snap)
	log.Info().
		Str("version", version).
		Str("listen", b.Listen).
		Str("config_file", b.ConfigFile).
		Dur("poll_interval", b.PollInterval).
		Dur("shutdown_grace", b.ShutdownGrace).
		Int("model_count", snap.Len()).
		Str("log_level", snap.LogLevel().String()).
		Msg("service_started")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// One signal channel owns the whole lifecycle. Both signals are
	// registered before anything can deliver one, so no window exists where
	// a signal is swallowed: the first SIGINT/SIGTERM starts the graceful
	// drain, a second forces an immediate exit 1 regardless of in-flight
	// work, and once the drain has finished the signals are ignored outright
	// — the process is committed to its exit code and must not be killed by
	// a late duplicate into the shell's 143.
	sig := make(chan os.Signal, 2)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)

	drainDone := make(chan struct{})
	go func() {
		select {
		case <-sig: // the signal that starts the drain
			cancel()
		case <-drainDone: // the server stopped without a signal
			return
		}
		select {
		case <-sig:
			// A duplicate forces exit only while the drain is still running.
			// A select with two ready cases picks at random, so once the
			// drain has finished, a buffered latecomer must lose
			// deterministically instead of racing the exit code.
			select {
			case <-drainDone:
				return
			default:
			}
			log.Warn().Msg("second_signal_forced_exit")
			os.Exit(1)
		case <-drainDone: // the drain finished within grace
		}
	}()

	// onPublish applies the reloaded snapshot's log level process-wide: the
	// global level is an atomic int32 that zerolog consults per event, so a
	// reload swaps it without locks and in-flight events race only to the
	// old/new boundary, never around a mutex. The poller is seeded with the
	// exact bytes loaded above: its hash baseline is the boot content, not a
	// fresh read of a file that may have changed in between.
	go config.NewPoller(store, b.ConfigFile, data, b.PollInterval, log, func(next *config.Snapshot) {
		zerolog.SetGlobalLevel(next.LogLevel())
		log.Debug().Uint64("generation", next.Gen()).
			Str("log_level", next.LogLevel().String()).Msg("log_level_applied")
	}).Run(ctx)

	err = server.New(store, b.Listen, b.ShutdownGrace, log).Run(ctx)
	// Ignore before announcing the drain done: from the instant Run returns
	// the process is committed to its exit code, and a duplicate signal must
	// fall on the ignored disposition, not the default handler's 143.
	// Already-buffered signals stay readable in the channel, and the
	// drain-wins check in the watcher makes them deterministic no-ops.
	// signal.Stop is deliberately not deferred — it would re-arm the default
	// disposition at return, briefly reopening exactly this window.
	signal.Ignore(os.Interrupt, syscall.SIGTERM)
	close(drainDone)
	if err != nil {
		log.Error().Err(err).Msg("server_failed")
		return 1
	}
	log.Info().Msg("shutdown_complete")
	return 0
}
