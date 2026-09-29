package proxy

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"openai-compatible-injector/internal/config"
	"openai-compatible-injector/internal/credential"
	"openai-compatible-injector/internal/recovery"
	"openai-compatible-injector/internal/transport"
)

// The closed set of reasons the continuation loop stopped short of a terminal
// stream. Like every other token this service logs they are fixed strings from
// a typed value, never error text — and these ones are the vocabulary an
// operator reads to tell "the deployment ran out of budget" from "the model's
// stream was not safe to continue".
const (
	// recoveryBudgetSpent — an exchange envelope is spent. A continuation is
	// real outbound traffic, and the envelope bounds traffic rather than the
	// intent to make it.
	recoveryBudgetSpent = "budget_spent"
	// recoveryMaxRecoveries — the configured reach was used up.
	recoveryMaxRecoveries = "max_recoveries"
	// recoveryMaxElapsed — the upstream has been silent for the configured
	// maximum. One moving window spans the whole logical stream, not one hop:
	// source bytes and a timely continuation header move it forward, while the
	// watchdogs still close a stalled relay body and cancel a hop waiting for
	// headers. Thus a healthy long stream survives, but neither kind of silence
	// can outlive the bound (recoveryWindow).
	//
	// It is also the PHASE a hop cut by that window reports, and the one dial
	// phase that blames no endpoint: nothing was refused by a member, nothing
	// was dialed for a dial that never happened, and the owner is this proxy.
	// A hop reads the same token in `stream_recovery_failed` as the phase of
	// the stop and in `stream_recovery_exhausted` as the reason for it,
	// because it is the same fact.
	recoveryMaxElapsed = "max_elapsed"
	// recoveryLogicalTerminal — the upstream declared the answer FINISHED
	// (a non-null Chat `finish_reason`) without forwarding the terminal
	// marker the client keys on. There is nothing left to continue, so no hop
	// was made — and this is NOT `unsafe_content`: the stream was neither
	// unreadable nor forbidden, its generation had simply ended. The wire
	// fact the client experiences — a stream with no marker — is unchanged
	// and unrepairable from here, because the proxy synthesizes no marker.
	recoveryLogicalTerminal = "logical_terminal"
	// recoveryUnsafeContent — the stream is not one this proxy may continue:
	// it carried a tool call, it declared itself finished, the upstream
	// declared it failed, the text accumulated past its bound, or the
	// request body cannot express a continuation. The `unsafe_reason` field
	// carries the gate's own token.
	recoveryUnsafeContent = "unsafe_content"
)

// continuationHop is one continuation attempt's inputs. Everything here is a
// fact about the committed candidate captured before the hop — the hop is the
// same candidate re-asked, not a new walk step, and every one of these is what
// makes that true: the candidate's own transport, its own endpoint, its own
// rotation pool, and the request's own exchange envelope.
type continuationHop struct {
	// ctx is the REQUEST's context. A hop dies with the client exactly like
	// every other dial this request makes, and a canceled context aborts the
	// hop before it is built.
	ctx context.Context
	// window is the logical stream's ONE recovery bound. The hop reads it
	// three ways — to refuse a dial the window has already closed, to derive
	// the context that bounds its wait for response headers, and to say whose
	// bound ended it — and it is the same window every other hop of the same
	// stream ran under, never a fresh one.
	window *recoveryWindow
	// client is the original client request: its headers are forwarded onto
	// the hop exactly as the walk forwards them, and it is never mutated.
	client *http.Request
	// cand is the committed candidate.
	cand config.Candidate
	// pool and credKey are the committed candidate's rotation pool and the
	// key it went out with. The hop asks for that same key first — sticky, so
	// a continuation re-asks with the credential that produced the prefix —
	// and only takes another when the pool has marked it rate-limited, which
	// is exactly when rotating is the right answer.
	pool    *credential.Pool
	credKey string
	// transform and model are the candidate view the walk used: the hop
	// re-applies the same upstream model alias and the same injection, so the
	// two attempts differ only in the conversation they carry.
	transform transformFunc
	model     config.Model
	// body is the assembled continuation body, before transform.
	body []byte
	// suffix is the route the walk appends to the endpoint.
	suffix string
	// budget is the request's own exchange envelope — the same one the walk
	// spent, never a fresh one: the walk is over and nothing is starved by a
	// continuation paying its way.
	budget *recovery.Budget
	// now is the engine's clock reading for this hop's credential acquire.
	// The recovery window owns its own synchronized reads and timer schedule;
	// do not reuse this timestamp for its idle accounting.
	now time.Time
}

// continuationDial is one hop's outcome. Exactly one of phase/err describes a
// failure: a non-empty phase names the stage that refused, and err carries the
// underlying cause when there was one (a credential or budget refusal has no
// error — nothing was dialed and no endpoint is at fault).
type continuationDial struct {
	// resp is non-nil EXACTLY when phase is empty and err is nil. Every
	// refusal this function makes — a build failure, a cooled-off pool, a
	// spent envelope — leaves it nil and names a phase instead, so a caller
	// may read a status only after checking the phase.
	resp  *http.Response
	info  transport.AttemptInfo
	err   error
	phase string
	// upstream is the hop's own request URL. Log-safe surfaces only
	// (scheme+host) — the same discipline the walk's events follow.
	upstream *url.URL
	// credKey is the key the hop actually went out with, empty on a candidate
	// without a pool. It is the NEXT hop's preferred key, so rotation state
	// moves forward with the flow instead of being re-derived.
	credKey string
	// callerGone reports that the client left, read from the REQUEST's own
	// context and never from an error chain. It is set at most once, on the
	// one classification the error text alone cannot make: a hop that failed
	// under a canceled context, which is the hop that would otherwise be
	// recorded as a peer refusing a connection. The flag is what lets the
	// loop's final record name the disconnect — the loop's own gate already
	// stopped the NEXT hop from being dialed for a reader who is gone, so
	// without it a reader who left during a hop is reported as a truncated
	// stream the upstream caused.
	callerLeft bool
}

// callerGone reports whether the client left, once, for the caller to read.
func (d *continuationDial) callerGone() bool { return d.callerLeft }

// dialContinuation builds and sends one continuation request. It is
// deliberately a near-copy of the walk's own attempt path — same transform,
// same header forwarding, same credential binding, same resolver, same
// budget — because the hop must be indistinguishable to the upstream from an
// ordinary attempt on that candidate, apart from the conversation body.
//
// It performs no recovery of its own: one hop is one exchange, and what a hop
// that fails MEANS is the caller's decision, not this function's.
//
// Two bounds make a hop unable to outlive the stream's recovery window, and
// both are that window (recoveryWindow), never a timer of the hop's own:
//
//   - the window is consulted before anything is built or dialed, so a hop the
//     loop was about to make into an already-shut window never reaches the
//     wire and never spends an exchange. That refusal is reported as the
//     `max_elapsed` phase, which is the one phase names no endpoint.
//   - the request runs under a context derived from the window (bind), which is
//     what bounds the RESPONSE-HEADER wait. Without it a peer that accepts the
//     TCP connection and then answers nothing parks this call in Do until the
//     client's own context dies — the transport has no ResponseHeaderTimeout
//     on purpose, since an SSE body legitimately outlives any fixed header
//     deadline. That context's watchdog is released the moment the dial
//     returns, but the CONTEXT lives on until the hop's body is closed: a
//     canceled request context aborts the body read that is still to come, so
//     releasing it here would truncate the very bytes this loop exists to
//     deliver (boundBody).
func (h *injectorHandler) dialContinuation(hop continuationHop) continuationDial {
	var dial continuationDial
	dial.credKey = hop.credKey
	// The window first: it is the cheapest gate, it is this proxy's own bound,
	// and a hop that has already outlived it must not build a body, acquire a
	// credential or claim an exchange on its way to being refused.
	if !hop.window.dialable() {
		dial.phase = recoveryMaxElapsed
		return dial
	}
	// The endpoint normalized exactly as the walk normalizes it: trailing
	// slashes trimmed, RawPath cleared so EscapedPath cannot percent-decode
	// the endpoint path behind the caller's back.
	upstream := *hop.cand.Endpoint
	upstream.Path = strings.TrimRight(upstream.Path, "/") + hop.suffix
	upstream.RawPath = ""
	dial.upstream = &upstream

	bounds, ok := hop.window.bind(hop.ctx)
	if !ok {
		dial.phase = recoveryMaxElapsed
		return dial
	}
	// handedOff records that an answer left this function with its body unread,
	// so the context now belongs to that body. It is set on exactly the one path
	// that wraps the body, and read by the release rule below.
	handedOff := false
	// ONE rule for the derived context, on every path out of this function: the
	// header watchdog is released as soon as the dial is over, and the context
	// is released here — the moment nothing can read a body — unless a body was
	// handed off, in which case that body's own Close releases it.
	defer func() {
		if !handedOff {
			bounds.release()
		}
	}()

	out, terr := hop.transform(hop.body, hop.model)
	if terr != nil {
		dial.phase, dial.err = "build", terr
		return dial
	}
	req, rerr := http.NewRequestWithContext(bounds.ctx, http.MethodPost, upstream.String(), bytes.NewReader(out))
	if rerr != nil {
		dial.phase, dial.err = "build", rerr
		return dial
	}
	copyForwardHeaders(req.Header, hop.client.Header)

	// THE CREDENTIAL SEAM, for the hop. The header is composed here and only
	// here; the value never reaches a log, an error, or the transport layer.
	if hop.pool != nil {
		k, ok := hop.pool.Acquire(hop.now, hop.credKey)
		if !ok {
			// Every key is cooling. A continuation is not worth a wait: the
			// client is holding an open stream, and the pool's cooldown exists
			// to stop a provider being hammered, not to be waited out inside a
			// response. The stream truncates, which is what it would do with
			// the feature off.
			dial.phase = "credential"
			return dial
		}
		dial.credKey = k.ID
		req.Header.Set(hop.cand.Cred.Spec.Header, hop.cand.Cred.Spec.Prefix+k.Value)
	}

	d := h.doers.Doer(hop.cand.Transport)
	if ex, pooled := d.(transport.Executor); pooled {
		// The pool owns the egress loop and claims its own exchanges; the hop
		// hands it the same facts the walk does, including the request's
		// envelope, so a continuation pays for its dials out of the same
		// budget rather than around it. No stream flag: whether the answer is
		// an event stream is read from its own content type at
		// acceptHopAnswer, never assumed from the request that asked for it.
		dial.resp, dial.info, dial.err = ex.Execute(&transport.AttemptRequest{
			Ctx:       bounds.ctx,
			Method:    http.MethodPost,
			URL:       &upstream,
			Header:    req.Header.Clone(),
			Body:      out,
			Streaming: true,
			Budget:    hop.budget,
		})
	} else {
		// One dial, claimed here exactly as the walk claims it: the envelope
		// counts real exchanges, so a direct hop is one unit and a refused
		// claim is a hop that never reached the wire.
		//
		// The hop does NOT take the envelope's own time bound, and the
		// asymmetry is deliberate. This dial runs under the recovery window's
		// context (bounds.ctx), which already cuts a hop that stalls before
		// headers — at the bound derived from `stream.max-elapsed`, the one
		// number that path's configuration actually states. Applying the
		// request envelope's elapsed half on top would bound a continuation
		// with a number the continuation's own policy does not name, and
		// would report that cut as the endpoint's timeout when it is
		// this proxy's own `max_elapsed` (the window's flag, checked first at
		// the failure read below). One bound per hop, owned by the window.
		if !hop.budget.AcquireExchange().Granted {
			dial.phase = "budget"
			return dial
		}
		dial.resp, dial.err = d.Do(req)
	}
	if dial.err != nil {
		// Whose failure this is, checked before anything else. A dial that
		// came back failed because the WINDOW canceled it is this proxy's own
		// bound and must never be recorded as a peer that misbehaved: the
		// watchdog stores `closed` before it cancels, so the flag is already
		// visible here, and during a dial this window's own watchdog is the
		// only writer of it — every other pass has released its body watchdog
		// before the loop could reach a hop, and a shut window ends the loop
		// rather than dialing through it.
		//
		// The reader is the second owner, and it is read from the REQUEST
		// context rather than from the error chain: this hop ran under a
		// context derived from that one, so a cancellation surfacing here is
		// indistinguishable from a canceled dial by shape — net/http reports
		// both the same way, and a cancellation this window itself raised to
		// stop a stalled hop reaches it too. A reader who hung up must never
		// appear as an upstream that refused a connection, so the hop says so
		// and the caller classifies the request as a disconnect. A hop that
		// failed because the client hung up is still a failed hop and is still
		// recorded; what changes is the owner it is recorded under, and
		// `callerLeft` is what lets the loop's final record name the same
		// cause once more, so one cause keeps one owner across both events.
		// The window is checked FIRST because a deadline this proxy set is
		// the more specific fact when both fired.
		switch {
		case hop.window.shut():
			dial.phase = recoveryMaxElapsed
		case hop.ctx.Err() != nil:
			dial.phase, dial.callerLeft = "client_write", true
		default:
			dial.phase = "dial"
		}
	} else if dial.resp == nil {
		// The pool's zero-dial budget refusal is the ONE Execute outcome that
		// is neither an answer nor an error: a nil response with a nil error,
		// raised when the envelope would not fund this attempt's first dial
		// (internal/transport/pool.go). It is a refusal by this proxy, so it
		// names the budget phase and blames no endpoint — the same reading the
		// walk gives it. Naming the phase here is what keeps the caller off
		// dial.resp: an answer that never arrived must never be dereferenced.
		dial.phase = "budget"
	} else if !bounds.promote(dial.resp.Body) {
		// A response which wins the network race after the idle deadline is not a
		// live continuation. Its headers are upstream activity only while they
		// arrive before the bound; after that, drop the body and retain the
		// proxy-owned timeout rather than relaying a late answer.
		_ = dial.resp.Body.Close()
		dial.resp = nil
		dial.phase = recoveryMaxElapsed
	} else {
		// An answer is in hand and its body has not been read yet — the caller
		// relays it. The context this dial ran under must therefore outlive this
		// call, and the hop's body is what says when it may be released.
		dial.resp.Body = &boundBody{ReadCloser: dial.resp.Body, release: bounds.release}
		handedOff = true
	}
	return dial
}

// recoveryWindow is ONE logical stream's recovery bound: the maximum silence
// this proxy tolerates from an upstream before it stops trying to finish the
// answer. The bound is idle time, never total stream runtime: every accepted
// upstream byte moves it forward by one interval, so a healthy long generation
// remains live while a peer that goes quiet is still bounded.
//
// The clock and timer scheduler are one seam. Production uses time.Now and
// time.AfterFunc, whose monotonic readings make deadline arithmetic immune to
// wall-clock steps. Tests replace BOTH halves together; a fake policy clock can
// therefore never disagree with a real watchdog timer.
//
// One mutex owns the deadline, irreversible timeout latch, and the one active
// blocked-operation guard. A relay read and a continuation header wait cannot
// overlap in this sequential loop, so one guard is sufficient. Its callback
// only acts while it is still the current guard; a stale callback after progress
// or a header-to-body handoff can never close the new body or cancel its context.
type recoveryWindow struct {
	clock recoveryClock
	idle  time.Duration

	mu       sync.Mutex
	deadline time.Time
	closed   bool
	guard    *windowGuard
}

// windowLease identifies one blocked operation across timer resets. Progress
// replaces a guard's timer, but the relay or hop that owns the operation must
// still be able to disarm its replacement when it finishes.
type windowLease struct{}

type windowGuard struct {
	lease *windowLease
	timer recoveryTimer
	trip  func()
}

// newRecoveryWindow opens the moving idle window at stream commitment.
func newRecoveryWindow(clock recoveryClock, maxIdle time.Duration) *recoveryWindow {
	now := clock.Now()
	return &recoveryWindow{clock: clock, idle: maxIdle, deadline: now.Add(maxIdle)}
}

// expireLocked latches a spent bound and removes its active guard. The caller
// must stop the returned timer and invoke the returned trip function after
// releasing w.mu: both may race with I/O.
func (w *recoveryWindow) expireLocked(now time.Time) (recoveryTimer, func()) {
	if w.closed || now.Before(w.deadline) {
		return nil, nil
	}
	w.closed = true
	g := w.guard
	w.guard = nil
	if g == nil {
		return nil, nil
	}
	return g.timer, g.trip
}

// armLocked installs a guard for the current deadline. The caller holds w.mu
// and has already established that the window is live. The callback proves its
// identity under that same mutex before it can end an operation.
func (w *recoveryWindow) armLocked(now time.Time, lease *windowLease, trip func()) *windowGuard {
	g := &windowGuard{lease: lease, trip: trip}
	w.guard = g
	g.timer = w.clock.AfterFunc(w.deadline.Sub(now), func() { w.fire(g) })
	return g
}

func (w *recoveryWindow) fire(g *windowGuard) {
	w.mu.Lock()
	if w.guard != g || w.closed {
		w.mu.Unlock()
		return
	}
	now := w.clock.Now()
	if now.Before(w.deadline) {
		// A scheduler may wake a timer early. Keep the same operation guarded
		// until the moving deadline actually arrives.
		w.guard = nil
		w.armLocked(now, g.lease, g.trip)
		w.mu.Unlock()
		return
	}
	w.closed = true
	w.guard = nil
	trip := g.trip
	w.mu.Unlock()
	trip()
}

// disarm makes a lease's current guard stale before asking its timer to stop.
// Timer.Stop alone cannot prevent an already-runnable callback from firing
// after a successful handoff; comparing the stable lease also makes relay
// cleanup disarm a timer progress has replaced.
func (w *recoveryWindow) disarm(lease *windowLease) {
	if lease == nil {
		return
	}
	w.mu.Lock()
	g := w.guard
	if g != nil && g.lease == lease {
		w.guard = nil
	} else {
		g = nil
	}
	w.mu.Unlock()
	if g != nil {
		g.timer.Stop()
	}
}

// progress accepts one source-body read as upstream activity. A byte at the
// exact deadline is late: the live interval is [last upstream activity,
// last upstream activity+idle), consistently for relay reads, header waits,
// loop gates and dial refusal. False means the byte must not reach CopySSE or
// the client; it cannot resurrect a timeout whose boundary already arrived.
func (w *recoveryWindow) progress() bool {
	w.mu.Lock()
	now := w.clock.Now()
	if timer, trip := w.expireLocked(now); timer != nil || trip != nil || w.closed {
		w.mu.Unlock()
		if timer != nil {
			timer.Stop()
		}
		if trip != nil {
			trip()
		}
		return false
	}
	w.deadline = now.Add(w.idle)
	old := w.guard
	if old != nil {
		// Publish the new identity before stopping the old timer. If its callback
		// has already become runnable, it observes that it is stale. Preserve the
		// stable lease, so completion disarms this replacement rather than only
		// the guard that happened to be armed at pass start.
		w.guard = nil
		w.armLocked(now, old.lease, old.trip)
	}
	w.mu.Unlock()
	if old != nil {
		old.timer.Stop()
	}
	return true
}

// expired reports and irreversibly latches whether the idle interval is spent.
func (w *recoveryWindow) expired() bool {
	w.mu.Lock()
	timer, trip := w.expireLocked(w.clock.Now())
	expired := w.closed
	w.mu.Unlock()
	if timer != nil {
		timer.Stop()
	}
	if trip != nil {
		trip()
	}
	return expired
}

// shut reports whether this proxy's own idle bound has already won.
func (w *recoveryWindow) shut() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.closed
}

// armBody binds the current moving deadline to a response body. Body.Read has
// no context, so Close is the only lever that can release a stalled read.
func (w *recoveryWindow) armBody(body io.Closer) func() {
	w.mu.Lock()
	now := w.clock.Now()
	if timer, trip := w.expireLocked(now); timer != nil || trip != nil || w.closed {
		w.mu.Unlock()
		if timer != nil {
			timer.Stop()
		}
		if trip != nil {
			trip()
		} else {
			_ = body.Close()
		}
		return func() {}
	}
	lease := &windowLease{}
	w.armLocked(now, lease, func() { _ = body.Close() })
	w.mu.Unlock()
	return func() { w.disarm(lease) }
}

// dialable atomically refuses a hop at or after the idle boundary.
func (w *recoveryWindow) dialable() bool { return !w.expired() }

// hopBounds owns the context while a continuation is awaiting headers, then
// transfers the one moving watchdog to its body. The request context is only
// canceled once the body is closed, because net/http ties body lifetime to it.
type hopBounds struct {
	ctx    context.Context
	cancel context.CancelFunc
	window *recoveryWindow
	lease  *windowLease
}

// bind starts the header-wait watchdog. It returns false once the exact
// deadline is spent, before a hop can build a body or claim an exchange.
func (w *recoveryWindow) bind(parent context.Context) (*hopBounds, bool) {
	ctx, cancel := context.WithCancel(parent)
	w.mu.Lock()
	now := w.clock.Now()
	if timer, trip := w.expireLocked(now); timer != nil || trip != nil || w.closed {
		w.mu.Unlock()
		if timer != nil {
			timer.Stop()
		}
		if trip != nil {
			trip()
		}
		cancel()
		return nil, false
	}
	lease := &windowLease{}
	w.armLocked(now, lease, cancel)
	w.mu.Unlock()
	return &hopBounds{ctx: ctx, cancel: cancel, window: w, lease: lease}, true
}

// promote turns a successful upstream response header into progress, then
// atomically replaces its header waiter with a body watchdog. HTTP response
// headers are upstream wire activity; without this handoff a valid response
// received just before the inherited deadline could have its body closed before
// its first byte. Downstream writes and proxy pings never call this method.
func (b *hopBounds) promote(body io.Closer) bool {
	w := b.window
	w.mu.Lock()
	g := w.guard
	if g == nil || g.lease != b.lease || w.closed {
		w.mu.Unlock()
		return false
	}
	now := w.clock.Now()
	if timer, trip := w.expireLocked(now); timer != nil || trip != nil || w.closed {
		w.mu.Unlock()
		if timer != nil {
			timer.Stop()
		}
		if trip != nil {
			trip()
		}
		return false
	}
	w.deadline = now.Add(w.idle)
	w.guard = nil
	w.armLocked(now, b.lease, func() { _ = body.Close() })
	w.mu.Unlock()
	g.timer.Stop()
	return true
}

// release clears the active guard and cancels the hop context only after no
// response body can still be read. It is safe to call repeatedly.
func (b *hopBounds) release() {
	b.window.disarm(b.lease)
	b.cancel()
}

// boundBody releases a hop's derived context when the hop's body is closed: the
// one moment nothing can be reading it any more. It exists because net/http
// aborts an unread response body the instant the request's context is canceled
// (a cancel after Do returns loses every byte the transport had not already
// buffered with the headers), so the release cannot happen at the dial, and
// wrapping the body is what keeps it from happening anywhere later than it must.
type boundBody struct {
	io.ReadCloser
	release func()
	once    sync.Once
}

func (b *boundBody) Close() error {
	err := b.ReadCloser.Close()
	b.once.Do(b.release)
	return err
}

// Unwrap exposes the body this wrapper was placed on. It changes nothing about
// how the body reads or closes — the release still rides Close, and only
// Close — and it exists for one caller: transport.HandoffStream, which needs
// to reach the exchange window underneath to disarm it once a hop's answer is
// confirmed to be an event stream. A hop through a POOLED member carries a
// window from the transport under the pool's own permit wrapper, and this is
// the third layer above it; without this method the handoff would stop at the
// hop and the window would cut a stream the client already holds.
func (b *boundBody) Unwrap() io.ReadCloser { return b.ReadCloser }

// continuationEligible reports whether a finished relay pass ended in the one
// way a continuation can answer: the stream stopped without a terminal marker,
// for a reason that is not one of the three that make continuing wrong.
//
// The three exclusions are not judgements about the upstream's health — they
// are statements about the CLIENT's stream. A terminal marker already reached
// it, so the stream is over and a hop would append to a finished answer. The
// client connection is gone, so no hop can deliver anything. Or a bounded-relay
// cap stopped the passthrough by this proxy's own policy, and re-dialing would
// replay the same oversized line into the same wall.
func continuationEligible(stats StreamStats, err error) bool {
	if stats.Terminal {
		return false
	}
	if err == nil {
		return true
	}
	var swe *streamWriteError
	switch {
	case errors.As(err, &swe), clientSide(err):
		return false
	case errors.Is(err, ErrSSELineTooLong), errors.Is(err, ErrSSEEventTooLarge):
		return false
	}
	return true
}
