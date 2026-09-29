package server

import (
	"io"
	"net/http"
	"sync/atomic"
	"time"
)

// The serving lifecycle, and the one endpoint that reports it.
//
// A process that is going away has two different things to say, and they must
// not be the same thing:
//
//   - liveness — "this process is alive". That is GET /healthz, owned by the
//     proxy handler beside its other unauthenticated path, and it stays true
//     for as long as the listener exists. A liveness probe that failed during
//     a drain would tell an operator to restart a process that is stopping
//     correctly, or worse, tell an orchestrator to kill it mid-drain.
//   - readiness — "send me traffic". That is GET /readyz, owned here by the
//     component that owns the listener, and it goes false BEFORE the listener
//     closes. This is the ordering a draining instance depends on: a load
//     balancer that still believes the instance is ready must be told
//     otherwise while the socket it is routing to is still answering, and
//     only then may that socket go away.
//
// Readiness is a statement about this process alone. It is deliberately not
// derived from the config snapshot, from an upstream provider, or from the
// databases: a provider outage is not a reason to stop routing to an instance
// that can still serve every model it has left, and a database blip must not
// empty a load balancer's whole pool. The machine below reads nothing but its
// own state, and the type gives it nothing to read.

// state is one point in the lifecycle. The values are ordered: a process only
// ever moves forward through them, and a transition that would move it
// backward is refused rather than obeyed.
type state int32

const (
	// stateStarting is the zero value: the process has not yet reached the
	// point where it can serve. Nothing in the current boot sequence
	// advertises readiness before the listener exists, so this state is not
	// observable over HTTP — it exists so that "ready" is never the zero
	// value of a field, where a missed transition would silently mean
	// healthy.
	stateStarting state = iota
	stateReady
	stateDraining
	stateStopped
)

// String is the token /readyz reports for the state, and the only vocabulary
// an operator sees for it. It carries no detail by construction: there is no
// provider, database, or config fact in it to leak.
func (s state) String() string {
	switch s {
	case stateStarting:
		return "starting"
	case stateReady:
		return "ready"
	case stateDraining:
		return "draining"
	case stateStopped:
		return "stopped"
	}
	return "unknown"
}

// lifecycle is the readiness state machine. It is safe for concurrent use by
// construction — a single atomic integer, no lock, no callback, nothing that
// can block a probe behind a request — because the readers are probe
// goroutines that must never queue behind the data path, and the writer is
// the shutdown path that must never wait for them.
type lifecycle struct {
	state atomic.Int32
}

func newLifecycle() *lifecycle { return &lifecycle{} }

// load reports the current state.
func (l *lifecycle) load() state { return state(l.state.Load()) }

// ready reports whether the process is currently advertising readiness. It is
// exactly the /readyz success condition.
func (l *lifecycle) ready() bool { return l.load() == stateReady }

// advance moves the state forward, and only forward. It reports whether the
// move happened: a call that would take the process backward — ready after
// draining, say — is refused, so no late or duplicated call can re-advertise
// an instance that has already told its load balancer to stop. The
// compare-and-swap is what makes that guarantee hold against a concurrent
// writer rather than merely in the common case.
func (l *lifecycle) advance(from, to state) bool {
	return l.state.CompareAndSwap(int32(from), int32(to))
}

// markReady advertises readiness when the listener is up and the process is
// still starting or already ready (a repeated call is a no-op that keeps the
// guarantee above intact).
func (l *lifecycle) markReady() bool {
	return l.advance(stateStarting, stateReady) || l.advance(stateReady, stateReady)
}

// beginDraining stops advertising readiness from the instant it is called,
// whether or not the process ever advertised it: a process stopped during
// startup has just as much reason to answer "not ready" as one stopped while
// serving. A repeated call is the same state, not a backward move.
func (l *lifecycle) beginDraining() bool {
	return l.advance(stateStarting, stateDraining) || l.advance(stateReady, stateDraining) ||
		l.advance(stateDraining, stateDraining)
}

// markStopped is the terminal transition, taken once the listener can no
// longer accept anything. Like the others it is idempotent: the several paths
// out of the serve loop may each reach it.
func (l *lifecycle) markStopped() bool {
	return l.advance(stateDraining, stateStopped) || l.advance(stateReady, stateStopped) ||
		l.advance(stateStarting, stateStopped) || l.advance(stateStopped, stateStopped)
}

// readyPath is the readiness endpoint. It is matched exactly: "/readyz/" is
// not this endpoint and falls through to the API handler's catch-all, the
// same rule GET /v1/models/ follows.
const readyPath = "/readyz"

// readyBody is the success body, matching GET /healthz's shape so an operator
// reads one convention instead of two.
const readyBody = "ok\n"

// serveReadyz answers the readiness probe: 200 "ok\n" while the process is
// advertising, 503 with the state token otherwise. A probe reads only the
// status code; the body exists so a human running curl learns why.
//
// A non-GET is 405 with an Allow header rather than the proxy's JSON method
// envelope: /readyz is not part of the OpenAI-compatible surface, so it makes
// no promise about OpenAI-shaped error bodies.
//
// The response is explicitly uncacheable. A cached 200 replayed after the
// process began draining would route traffic into a socket that is about to
// close — the exact failure this endpoint exists to prevent.
func (l *lifecycle) serveReadyz(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusMethodNotAllowed)
		_, _ = io.WriteString(w, "method not allowed\n")
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	st := l.load()
	if st == stateReady {
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, readyBody)
		return
	}
	w.WriteHeader(http.StatusServiceUnavailable)
	_, _ = io.WriteString(w, st.String()+"\n")
}

// readinessPropagation is the head start the process gives whatever routes to
// it, between "this instance is no longer ready" and "this instance stops
// accepting connections". Without it the two happen at the same instant, and
// every health check that had already been scheduled, every in-flight probe,
// and every load-balancer view that is one interval out of date lands on a
// closed port — a connection refused where a 503 or a served request was
// available.
//
// Five seconds is derived from the probe cadence the deployment ships: a 2s
// interval plus a 2s timeout means the worst-case notification is 4s, plus
// slack for scheduling. It is drawn from the shutdown grace rather than added
// to it (see Server.propagation), so the total time from signal to exit is
// unchanged and a container's stop_grace_period needs no adjustment.
const readinessPropagation = 5 * time.Second

// propagation is the readiness head start this server will take, capped at
// half the drain budget so the drain keeps the majority of it. The cap is what
// makes the window safe to spend on every deployment: a server configured with
// a short grace (a test, or an operator who wants a fast stop) takes a short
// head start instead of one that leaves nothing for in-flight work. A budget
// that cannot carry a head start at all — zero, or a negative no configuration
// produces — takes none, which is the previous behavior exactly.
func (s *Server) propagation() time.Duration {
	half := s.grace / 2
	if half <= 0 {
		return 0
	}
	if readinessPropagation > half {
		return half
	}
	return readinessPropagation
}

// lifecycleRouter is the top-level handler: the one lifecycle path this
// package owns, and the API handler for everything else. Wrapping the API
// rather than registering into it keeps the readiness answer structurally
// independent of the proxy: this handler can reach the lifecycle state and
// nothing else, so no future edit to the proxy can make readiness depend on a
// provider, a snapshot, or a database.
type lifecycleRouter struct {
	lc  *lifecycle
	api http.Handler
}

func (rt lifecycleRouter) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == readyPath {
		rt.lc.serveReadyz(w, r)
		return
	}
	rt.api.ServeHTTP(w, r)
}
