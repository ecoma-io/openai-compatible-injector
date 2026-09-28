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

// Server owns the HTTP server, the outbound transport registry, and the
// process's readiness state. Its surface is deliberately minimal: New + Run
// only, nothing else is exported. Lifecycle beyond the listener (os.Exit,
// signal handling) stays with the caller.
type Server struct {
	http  *http.Server
	doers *transport.Registry
	// lc is the readiness state machine /readyz reports. Run owns every
	// transition; nothing else may move it.
	lc    *lifecycle
	grace time.Duration
	log   zerolog.Logger
}

// New builds a Server with the proxy handler bound to the given store and
// transport registry, served on addr. grace is the whole post-signal
// shutdown budget: the readiness head start and the drain for in-flight
// requests are drawn from it (see Run), so the time from signal to exit never
// exceeds grace. authProvider is passed through to the handler unchanged
// (nil = static mode), as is meter (nil = metering off) and creds (nil = no
// upstream credentials; the credential registry needs no shutdown handling —
// a Pool is pure state with nothing to close, and in-flight requests hold
// their pool pointers through the drain).
//
// The handler served is the API plus one lifecycle path: GET /readyz, the
// process's readiness, answered by the state machine here rather than by the
// proxy. Liveness (GET /healthz) stays with the proxy beside its other
// unauthenticated path.
func New(store *config.Store, doers *transport.Registry, creds *credential.Registry, authProvider auth.Provider, meter usage.Ingest, addr string, grace time.Duration, log zerolog.Logger) *Server {
	lc := newLifecycle()
	return &Server{
		http: &http.Server{
			Addr:    addr,
			Handler: lifecycleRouter{lc: lc, api: proxy.NewHandler(store, doers, creds, authProvider, meter, log)},
			// Bounds how long a client may take to send its request headers,
			// which is the slowloris shape. A slow request BODY is not this
			// field's job: it is bounded where the body is read, by the
			// handler, which knows how large the body is allowed to be.
			ReadHeaderTimeout: 10 * time.Second,
			// Without an IdleTimeout a client that opens a keep-alive
			// connection and goes quiet pins a goroutine and a file
			// descriptor for the process's whole life. Two minutes
			// comfortably exceeds any client's keep-alive pooling window
			// while bounding the leak.
			IdleTimeout: 120 * time.Second,
			// Deliberately no WriteTimeout: a streamed response is one write
			// for as long as the model keeps producing, so any overall write
			// deadline would cut legitimate SSE mid-flight. The keep-alive
			// ticker bounds silence instead, and the drain bounds the whole
			// request's life.
		},
		doers: doers,
		lc:    lc,
		grace: grace,
		log:   log,
	}
}

// Run serves until ctx is cancelled or the HTTP server fails on its own.
//
// On ctx cancellation it stops advertising readiness, keeps the listener
// answering for a bounded head start so whatever routes here can notice, and
// only then drains in-flight requests via http.Server.Shutdown; if the drain
// deadline passes with connections still active, the listener and every
// remaining connection are force-closed. Idle upstream connections are always
// closed before returning, and the readiness state ends stopped. Run never
// calls os.Exit.
//
// The head start and the drain are both drawn from grace, so the total time
// from cancellation to return never exceeds it — a deployment's
// stop_grace_period needs no adjustment for the readiness transition.
//
// The ordering is the point, and it is the one thing this sequence may not
// trade away: readiness must go false while the listener is still accepting,
// because a probe that has not yet noticed cannot be told anything by a
// closed port.
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

	// Ready by the time any byte can be served: the listener exists and the
	// accept loop is running, so a probe arriving now is answered by a
	// process that is certainly serving. A context already cancelled at
	// entry — the process was signalled the instant it started — is never
	// advertised as ready at all, so no probe can observe a readiness this
	// run never intended to keep.
	if ctx.Err() == nil {
		s.lc.markReady()
		s.log.Debug().Msg("readiness_ready")
	}

	select {
	case err := <-serveDone:
		// The server terminated on its own (or the listener died).
		s.lc.markStopped()
		s.doers.CloseIdleConnections()
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
	}

	// Unready FIRST, and before the listener is touched: from this instant
	// /readyz answers 503 while /healthz keeps answering 200 and the API
	// keeps serving, which is the window a load balancer needs.
	head := s.propagation()
	s.lc.beginDraining()
	s.log.Info().Dur("grace", s.grace).Dur("propagation", head).Msg("readiness_unready")

	if head > 0 {
		timer := time.NewTimer(head)
		select {
		case <-timer.C:
		case err := <-serveDone:
			// The listener died on its own during the head start; there is
			// nothing left to drain.
			timer.Stop()
			s.lc.markStopped()
			s.doers.CloseIdleConnections()
			if errors.Is(err, http.ErrServerClosed) {
				return nil
			}
			return err
		}
	}

	budget := s.grace - head
	s.log.Info().Dur("grace", s.grace).Dur("drain", budget).Msg("drain_started")
	drainCtx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()
	if err := s.http.Shutdown(drainCtx); err != nil {
		s.log.Warn().Err(err).Dur("grace", s.grace).Dur("drain", budget).Msg("drain_deadline_exceeded")
		// Shutdown failed (typically context deadline exceeded): force-close
		// every remaining connection so the drain deadline is enforced.
		_ = s.http.Close()
	}

	err = <-serveDone
	s.lc.markStopped()
	s.doers.CloseIdleConnections()
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}
