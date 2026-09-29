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
	"sync/atomic"
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
	// recoveryMaxElapsed — the configured window since the stream began is
	// over. It bounds the whole recovery effort, not one hop, and it is a HARD
	// bound on each of the two ways this proxy can be left waiting: the
	// window's watchdogs close the upstream body a relay is blocked on AND
	// cancel the context a hop is waiting for response headers under, so
	// neither a stream that stops producing bytes nor a peer that accepts a
	// connection and answers nothing can outlive it (recoveryWindow).
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
	// now is the engine's clock reading for this hop: the credential acquire
	// is stamped with it, and the dial's own watchdog is armed for the
	// distance from it to the window's deadline. One reading, so the hop
	// cannot measure itself against a different "now" than the gates that
	// admitted it.
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
	if !hop.window.dialable(hop.now) {
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

	bounds := hop.window.bind(hop.ctx, hop.now)
	// handedOff records that an answer left this function with its body unread,
	// so the context now belongs to that body. It is set on exactly the one path
	// that wraps the body, and read by the release rule below.
	handedOff := false
	// ONE rule for the derived context, on every path out of this function: the
	// header watchdog is released as soon as the dial is over, and the context
	// is released here — the moment nothing can read a body — unless a body was
	// handed off, in which case that body's own Close releases it.
	defer func() {
		bounds.stop()
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
		// budget rather than around it.
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
		if !hop.budget.ConsumeExchange() {
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
	} else {
		// An answer is in hand and its body has not been read yet — the caller
		// relays it. The context this dial ran under must therefore outlive this
		// call, and the hop's body is what says when it may be released.
		dial.resp.Body = &boundBody{ReadCloser: dial.resp.Body, release: bounds.release}
		handedOff = true
	}
	return dial
}

// recoveryWindow is ONE logical stream's recovery bound: the silence this
// proxy tolerates from an upstream before it stops trying to finish the
// answer.
//
// It is a value rather than a handful of locals because the bound has to be
// the SAME measure for four different mechanisms, and the defect this type
// replaces was that it was not: the loop's own gate read one clock while the
// hard deadline was computed from another, and the hop dial was bounded by
// neither.
//
//   - the loop's own gate, read on the request's clock (expired);
//   - the body watchdog that closes an upstream body the relay is parked on
//     (armBody) — Body.Read takes no context, so closing the body is the only
//     lever that unblocks a read the upstream is not finishing;
//   - the dial watchdog that cancels a hop's request context while it waits
//     for response headers (bind) — the shared transport deliberately has no
//     ResponseHeaderTimeout, because an SSE body legitimately outlives any
//     fixed header deadline, so the only header bound a hop can have is the
//     one the operator's own max-elapsed already implies;
//   - the hop's refusal to dial at all once the measure is spent (dialable).
//
// The bound is IDLE time, not total time. Every upstream byte the relay
// accepts moves it forward by a full interval, so a generation that is
// healthy for an hour is never cut, while an upstream that goes quiet stops
// costing the client after one interval of silence. The previous absolute
// deadline could not tell those two apart: it killed long healthy answers
// (production measured 73s, 83s and 106s streams completing normally) with
// the same event it used for a genuinely stalled peer.
//
// Progress is counted from UPSTREAM bytes only. The keep-alive ping is
// written to the client, never read from the peer, so it cannot reset the
// window — a proxy that kept itself alive by talking to its own client would
// report a dead upstream as a live one.
//
// The clock is the caller's, deliberately: this type never reads time itself.
// Every caller passes a reading of the REQUEST's one clock — the engine's, the
// same source the walk, the budget and the frozen policy were timed by — so a
// recovery session cannot drift between two notions of "now", and so a test
// can drive the whole session through the same seam it already drives the
// engine through. In production that seam is time.Now, so the instant carries
// Go's monotonic reading and every comparison below survives a wall-clock
// step; a fake clock only ever replaces it in tests.
type recoveryWindow struct {
	// idle is how much silence the bound permits. It is fixed for the life
	// of the window; only the deadline below moves.
	idle time.Duration
	// deadline is the instant this silence ends, and it MOVES: every
	// accepted upstream byte pushes it a full `idle` into the future, so it
	// answers "when has this upstream last said anything, plus the bound"
	// rather than "when did this request start". It is read and written
	// under mu, because the relay's progress hook and the window's own
	// gate run concurrently — the relay goroutine records progress while
	// the loop below decides whether that progress was enough.
	deadline time.Time
	// mu guards deadline and the live body watchdog. It is never held across a
	// body close, a context cancel or any other call that could block: the
	// watchdog stores `closed` and closes its lever OUTSIDE it, so a read parked
	// in Body.Read can never be waiting on this mutex.
	mu sync.Mutex
	// closed records that the bound has been REACHED. It is set by whichever
	// watchdog gets there first, or by a caller that finds the silence
	// already spent, and it is the single authority on "this proxy's own
	// bound ended that pass" as opposed to "the upstream cut it" or "the
	// caller left". It is stored BEFORE the body is closed or the context
	// canceled, so a caller that reads it after an unblocked read or an
	// aborted dial can never miss it and never misattribute the stop to a
	// peer.
	closed atomic.Bool
	// armed is the live body watchdog, or nil when no pass is inside a read.
	// A moving deadline needs a MOVING timer: an AfterFunc armed at the pass's
	// start still fires at the instant it was armed for, so extending the
	// bound without resetting it would leave the relay parked on a body this
	// proxy had already promised to keep reading. armedUntil is the physical
	// timer deadline, separate from the injected-clock deadline above: the
	// timer runs on real time while tests drive the policy clock. It lets a
	// callback that raced a Reset see that progress already moved the timer
	// before it closes the body.
	//
	// Both are guarded by mu alongside deadline, and are cleared by the same
	// stop() the relay defers, so the timer cannot outlive the pass that armed
	// it.
	armed      *time.Timer
	armedUntil time.Time
}

// newRecoveryWindow opens the window: start is the instant the committed
// stream began relaying, maxIdle the frozen policy's tolerated silence.
func newRecoveryWindow(start time.Time, maxIdle time.Duration) *recoveryWindow {
	return &recoveryWindow{idle: maxIdle, deadline: start.Add(maxIdle)}
}

// progress records that the upstream produced bytes at `now`, extending the
// silence to a full `idle` from here. It is the one function that moves the
// deadline, and it is called at the SOURCE read boundary — not after a client
// write and not only after an SSE event boundary. A peer can legitimately send
// one large, fragmented event for longer than the interval; the bytes prove it
// is alive even before the parser sees its blank line. Conversely, a slow
// client and this proxy's keep-alive ping are downstream facts, not upstream
// progress, and must never revive the bound.
//
// A byte that arrives at or after the current deadline does NOT revive the
// window: by then a watchdog has already closed the body, and re-opening the
// bound from under it would leave a stream no longer bound at all. The
// deadline is also never moved once closed is set, so a late byte and a bound
// that already fired agree on the outcome.
//
// The armed body watchdog is reset with the deadline. That is the whole reason
// this function exists rather than a plain field write: `max-elapsed` used to
// be an instant, so a timer armed once for it stayed right for the life of the
// pass. An idle bound is not an instant, and a timer that outlives the
// extension would cut exactly the healthy stream the extension exists to
// protect. The timer is reset rather than re-created so the relay holds no
// second handle and stop() stays the single release.
func (w *recoveryWindow) progress(now time.Time) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed.Load() {
		return
	}
	next := now.Add(w.idle)
	if next.After(w.deadline) {
		w.deadline = next
		if w.armed != nil {
			w.armedUntil = time.Now().Add(w.idle)
			w.armed.Reset(w.idle)
		}
	}
}

// remaining reports how much silence is left at a reading of the request's
// clock, and whether any is left at all. It is the single place the moving
// deadline is read, so the loop's gate and both watchdogs cannot disagree
// about when the bound falls.
func (w *recoveryWindow) remaining(now time.Time) time.Duration {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.deadline.Sub(now)
}

// expired reports whether the tolerated silence is spent at a reading of the
// request's clock. It reads the moving deadline through remaining, so it takes
// the same lock progress writes under and the four mechanisms below can never
// disagree about when the bound falls.
//
// The instant itself is not yet spent: a bound that fired on the tick rather
// than after it would cut a stream one nanosecond early, and the boundary
// belongs here rather than in each caller.
func (w *recoveryWindow) expired(now time.Time) bool { return w.remaining(now) < 0 }

// shut reports whether a watchdog has already reached the window.
func (w *recoveryWindow) shut() bool { return w.closed.Load() }

// armBody arms this window's hard bound against one relay pass: after the
// upstream has been silent for the tolerated interval the returned watchdog
// closes the body the pass is reading from, which is what unblocks a relay
// sitting inside a read.
//
// Closing the body is the only lever a Go response body offers: Body.Read
// takes no context, so a peer that sends a partial event and then holds the
// TCP connection open blocks the relay indefinitely, and `max-elapsed` would
// otherwise be checked only after the read it was supposed to bound. The same
// lever, for the same reason, is what the upstream error capture uses
// (upstream_error.go) — and the same cost is accepted: closing a body
// mid-read discards that connection rather than returning it to the pool,
// which is a deliberate casualty of cutting a stalled stream.
//
// Three properties make this safe to arm around every pass:
//
//   - It is a plain timer, NOT a derivation of the request context. A client
//     disconnect is not the window closing, and the two must stay
//     distinguishable in the record: the request context already aborts the
//     read on its own, so a watchdog hung off it would be unable to tell
//     "the operator's bound elapsed" from "the reader left".
//   - It creates no goroutine of its own. time.AfterFunc runs its function on
//     the runtime's timer goroutine, and the caller's stop() — deferred by
//     the relay closure — releases the timer on every path, including the one
//     where the pass ended long before the silence fell.
//   - The distance it arms for is measured from the caller's own reading of
//     the request's clock to the window's deadline, so arming it per pass
//     shares one window rather than restarting it. Upstream progress MOVES
//     that deadline and resets this timer with it, so a pass that keeps
//     receiving events is never cut by a timer armed for an earlier one.
func (w *recoveryWindow) armBody(now time.Time, body io.Closer) (stop func()) {
	left := w.remaining(now)
	if left <= 0 {
		// The silence is already spent — the only way a pass can start here
		// is a pass that was itself idle past the bound. Close the body now:
		// the pass returns immediately and the loop reports the bound instead
		// of relaying bytes past a deadline the operator set.
		w.closed.Store(true)
		_ = body.Close()
		return func() {}
	}
	// Reset cannot retract a callback already scheduled by the runtime, so the
	// callback re-checks the physical timer deadline under mu before it closes
	// the body: a source read racing an old deadline either moves the timer
	// first (the old callback becomes a no-op), or loses the race honestly and
	// sees the body close.
	var timer *time.Timer
	w.mu.Lock()
	w.armedUntil = time.Now().Add(left)
	timer = time.AfterFunc(left, func() {
		w.mu.Lock()
		if w.armed != timer || time.Now().Before(w.armedUntil) {
			w.mu.Unlock()
			return
		}
		w.closed.Store(true)
		w.mu.Unlock()
		_ = body.Close()
	})
	// Published under mu so progress cannot reset a timer it has not been
	// told about: a byte accepted between the AfterFunc above and this store
	// would otherwise move the deadline with no timer following it, and the
	// pass would be cut at the instant the byte should have moved.
	w.armed = timer
	w.mu.Unlock()
	return func() {
		timer.Stop()
		// Cleared under mu so a progress arriving after the pass ends cannot
		// reach in and reset a timer this stop has already released.
		w.mu.Lock()
		if w.armed == timer {
			w.armed = nil
			w.armedUntil = time.Time{}
		}
		w.mu.Unlock()
	}
}

// dialable reports whether a hop may still be dialed at a reading of the
// request's clock, marking the window shut when it may not. A refusal here is
// this proxy's own bound, never an endpoint's: nothing is dialed, no exchange
// is claimed, and no member is blamed — which is why dialContinuation turns it
// into the `max_elapsed` phase rather than a dial failure.
func (w *recoveryWindow) dialable(now time.Time) bool {
	if !w.expired(now) {
		return true
	}
	w.closed.Store(true)
	return false
}

// hopBounds is the context ONE hop runs under and the two levers that end it.
// They are separate because they bound different waits, and conflating them was
// a defect: the timer cancels the context a hop is PARKED IN while it waits for
// response headers, while the hop's body, once those headers arrive, is bounded
// by the window's body watchdog (armBody).
type hopBounds struct {
	ctx    context.Context
	timer  *time.Timer
	cancel context.CancelFunc
}

// stop releases the header watchdog without touching the context, and a hop
// calls it the moment its dial returns — headers or error. The wait it bounds
// is over at that point, and leaving it armed would be worse than useless: the
// watchdog writes `closed` and cancels the context, and net/http ties the
// LIFETIME of a response body to the context of the request that produced it,
// so a timer firing during the hop's own body read would abort that read with
// a canceled context — reported as the client leaving, or as an upstream read
// failure, when the owner was this proxy's own bound.
func (b *hopBounds) stop() { b.timer.Stop() }

// release stops the watchdog and cancels the derived context. Nothing may be
// reading the hop's body when it is called, which is exactly why it is NOT
// called when the dial returns: a successful dial's context is released by the
// hop's own Body.Close (see boundBody), and every other path here has no body
// to outlive this call.
func (b *hopBounds) release() {
	b.timer.Stop()
	b.cancel()
}

// bind derives the context ONE hop runs under: the caller's context — the
// client's request context, so a hop still dies with the reader exactly as
// every other dial this request makes — plus a cancel at this window's
// current deadline. It is called only after dialable has agreed, so there is
// silence left to bound here by construction and the watchdog always has a
// positive distance to arm for.
func (w *recoveryWindow) bind(parent context.Context, now time.Time) *hopBounds {
	ctx, cancel := context.WithCancel(parent)
	timer := time.AfterFunc(w.remaining(now), func() {
		w.closed.Store(true)
		cancel()
	})
	return &hopBounds{ctx: ctx, timer: timer, cancel: cancel}
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
}

func (b *boundBody) Close() error {
	err := b.ReadCloser.Close()
	b.release()
	return err
}

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
