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
		log.Error().Err(err).Str("url", url).Msg("healthcheck_probe_failed")
		return 1
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Error().Err(err).Msg("healthcheck_body_read_failed")
		return 1
	}
	if resp.StatusCode != http.StatusOK || string(body) != "ok\n" {
		log.Error().Int("status", resp.StatusCode).Str("body", string(body)).Msg("healthcheck_unhealthy")
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

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// A second signal forces an immediate exit regardless of in-flight work.
	// The watcher channel is registered NOW rather than after ctx.Done:
	// between the first signal's cancel and a later registration, a second
	// signal would reach only NotifyContext's channel, whose goroutine has
	// already returned — swallowed, and the drain would run its full grace
	// budget. Registered up front, every signal lands in this channel too;
	// the first read (the signal that started the drain) is discarded and
	// the second read forces the exit.
	sig := make(chan os.Signal, 2)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(sig)
	go func() {
		<-ctx.Done()
		<-sig // the signal that started the drain — discarded
		<-sig // any second signal — force exit
		log.Warn().Msg("second_signal_forced_exit")
		os.Exit(1)
	}()

	// onPublish applies the reloaded snapshot's log level process-wide: the
	// global level is an atomic int32 that zerolog consults per event, so a
	// reload swaps it without locks and in-flight events race only to the
	// old/new boundary, never around a mutex.
	go config.NewPoller(store, b.ConfigFile, b.PollInterval, log, func(next *config.Snapshot) {
		zerolog.SetGlobalLevel(next.LogLevel())
		log.Debug().Uint64("generation", next.Gen()).
			Str("log_level", next.LogLevel().String()).Msg("log_level_applied")
	}).Run(ctx)

	if err := server.New(store, b.Listen, b.ShutdownGrace, log).Run(ctx); err != nil {
		log.Error().Err(err).Msg("server_failed")
		return 1
	}
	log.Info().Msg("shutdown_complete")
	return 0
}
