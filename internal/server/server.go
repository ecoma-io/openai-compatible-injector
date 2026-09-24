package server

import (
	"context"
	"errors"
	"net"
	"net/http"
	"time"

	"github.com/rs/zerolog"

	"openai-compatible-injector/internal/auth"
	"openai-compatible-injector/internal/config"
	"openai-compatible-injector/internal/credential"
	"openai-compatible-injector/internal/proxy"
	"openai-compatible-injector/internal/transport"
	"openai-compatible-injector/internal/usage"
)

// Server owns the HTTP server and the outbound transport registry. Its
// surface is deliberately minimal: New + Run only, nothing else is exported,
// no status endpoint. Lifecycle (os.Exit, signal handling) stays with the
// caller.
type Server struct {
	http  *http.Server
	doers *transport.Registry
	grace time.Duration
	log   zerolog.Logger
}

// New builds a Server with the proxy handler bound to the given store and
// transport registry, served on addr. grace is the shutdown drain deadline:
// after it elapses, in-flight requests are force-closed. authProvider is
// passed through to the handler unchanged (nil = static mode), as is meter
// (nil = metering off) and creds (nil = no upstream credentials; the
// credential registry needs no shutdown handling — a Pool is pure state
// with nothing to close, and in-flight requests hold their pool pointers
// through the drain).
func New(store *config.Store, doers *transport.Registry, creds *credential.Registry, authProvider auth.Provider, meter usage.Ingest, addr string, grace time.Duration, log zerolog.Logger) *Server {
	return &Server{
		http: &http.Server{
			Addr:              addr,
			Handler:           proxy.NewHandler(store, doers, creds, authProvider, meter, log),
			ReadHeaderTimeout: 10 * time.Second,
			// Without an IdleTimeout a client that opens a keep-alive
			// connection and goes quiet pins a goroutine and a file
			// descriptor for the process's whole life. Two minutes
			// comfortably exceeds any client's keep-alive pooling window
			// while bounding the leak.
			IdleTimeout: 120 * time.Second,
		},
		doers: doers,
		grace: grace,
		log:   log,
	}
}

// Run serves until ctx is cancelled or the HTTP server fails on its own.
//
// On ctx cancellation it drains in-flight requests for up to grace via
// http.Server.Shutdown; if the deadline passes with connections still active,
// the listener and every remaining connection are force-closed so grace is a
// hard upper bound. Idle upstream connections are always closed before
// returning. Run never calls os.Exit.
func (s *Server) Run(ctx context.Context) error {
	ln, err := net.Listen("tcp", s.http.Addr)
	if err != nil {
		return err
	}
	s.log.Debug().Str("addr", ln.Addr().String()).Msg("listener_ready")

	serveDone := make(chan error, 1)
	go func() {
		serveDone <- s.http.Serve(ln)
	}()

	select {
	case err := <-serveDone:
		// The server terminated on its own (or the listener died).
		s.doers.CloseIdleConnections()
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
	}

	s.log.Info().Dur("grace", s.grace).Msg("drain_started")
	drainCtx, cancel := context.WithTimeout(context.Background(), s.grace)
	defer cancel()
	if err := s.http.Shutdown(drainCtx); err != nil {
		s.log.Warn().Err(err).Dur("grace", s.grace).Msg("drain_deadline_exceeded")
		// Shutdown failed (typically context deadline exceeded): force-close
		// every remaining connection so the drain deadline is enforced.
		_ = s.http.Close()
	}

	err = <-serveDone
	s.doers.CloseIdleConnections()
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}
