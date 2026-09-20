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
	addr := os.Getenv("LISTEN")
	if addr == "" {
		addr = config.DefaultListen
	}
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "healthcheck: invalid LISTEN address %q: %v\n", addr, err)
		return 1
	}
	// A wildcard/hostless address binds all interfaces; probe loopback.
	if host == "" || host == "::" {
		host = "127.0.0.1"
	}
	url := "http://" + net.JoinHostPort(host, port) + "/healthz"

	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		fmt.Fprintf(os.Stderr, "healthcheck: %v\n", err)
		return 1
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		fmt.Fprintf(os.Stderr, "healthcheck: read body: %v\n", err)
		return 1
	}
	if resp.StatusCode != http.StatusOK || string(body) != "ok\n" {
		fmt.Fprintf(os.Stderr, "healthcheck: status %d body %q\n", resp.StatusCode, body)
		return 1
	}
	return 0
}

// newLogger builds the stderr JSON logger honouring LOG_LEVEL (default info;
// accepts debug/info/warn/error; anything else falls back to info).
func newLogger(levelEnv string) zerolog.Logger {
	level := zerolog.InfoLevel
	switch levelEnv {
	case "debug":
		level = zerolog.DebugLevel
	case "info":
		level = zerolog.InfoLevel
	case "warn":
		level = zerolog.WarnLevel
	case "error":
		level = zerolog.ErrorLevel
	}
	return zerolog.New(os.Stderr).With().Timestamp().Logger().Level(level)
}

func run() int {
	log := newLogger(os.Getenv("LOG_LEVEL"))

	b, err := config.LoadBootstrap(os.Getenv)
	if err != nil {
		log.Fatal().Err(err).Msg("load bootstrap")
	}

	data, err := os.ReadFile(b.ConfigFile)
	if err != nil {
		log.Fatal().Err(err).Str("file", b.ConfigFile).Msg("read config file")
	}
	snap, err := config.LoadRuntime(data)
	if err != nil {
		log.Fatal().Err(err).Str("file", b.ConfigFile).Msg("load runtime config")
	}

	store := config.NewStore(snap)
	log.Info().Str("file", b.ConfigFile).Dur("interval", b.PollInterval).Msg("config poller started")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// A second signal forces an immediate exit regardless of in-flight work.
	// Wait for the first signal (ctx.Done) before arming: signal.Notify
	// delivers every occurrence to every registered channel, so a channel
	// registered up front would also catch the first signal and race the
	// graceful shutdown.
	go func() {
		<-ctx.Done()
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
		<-sig
		log.Warn().Msg("second signal received; forcing exit")
		os.Exit(1)
	}()

	go config.NewPoller(store, b.ConfigFile, b.PollInterval, log).Run(ctx)

	if err := server.New(store, b.Listen, b.ShutdownGrace, log).Run(ctx); err != nil {
		log.Error().Err(err).Msg("server failed")
		return 1
	}
	log.Info().Msg("shutdown complete")
	return 0
}
