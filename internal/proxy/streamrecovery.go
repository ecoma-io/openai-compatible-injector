package proxy

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
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
}

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
		if hop.window.shut() {
			dial.phase = recoveryMaxElapsed
		} else {
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

// recoveryWindow is ONE logical stream's recovery bound: the single absolute
// instant, computed once when the committed stream begins relaying, past which
// this proxy stops trying to finish the answer.
//
// It is a value rather than a handful of locals because the bound has to be
// the SAME instant for four different mechanisms, and the defect this type
// replaces was that it was not: the loop's "is the window over" gate read one
// clock while the hard deadline was computed from another, and the hop dial
// was bounded by neither.
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
//   - the hop's refusal to dial at all once the instant has passed (dialable).
//
// A window is never reset and no hop re-opens it. Every pass and every hop
// arms its own watchdog — a body must be closed to unblock a read and a
// context must be canceled to abort a dial, and those are different levers on
// different objects — but each is armed for the distance that REMAINS to the
// same deadline. `max-elapsed` is one window since the commit, never a
// per-hop budget.
//
// The clock is the caller's, deliberately: this type never reads time itself.
// Every caller passes a reading of the REQUEST's one clock — the engine's, the
// same source the walk, the budget and the frozen policy were timed by — so a
// recovery session cannot drift between two notions of "now", and so a test
// can drive the whole session through the same seam it already drives the
// engine through. In production that seam is time.Now, so deadline carries
// Go's monotonic reading and every comparison below survives a wall-clock
// step; a fake clock only ever replaces it in tests.
type recoveryWindow struct {
	// deadline is the one instant. Read once, at construction, and never
	// recomputed — that is what makes it a bound rather than a per-hop clock.
	deadline time.Time
	// closed records that the window has been REACHED. It is set by whichever
	// watchdog gets there first, or by a caller that finds the instant already
	// past, and it is the single authority on "this proxy's own bound ended
	// that pass" as opposed to "the upstream cut it" or "the caller left". It
	// is stored BEFORE the body is closed or the context canceled, so a caller
	// that reads it after an unblocked read or an aborted dial can never miss
	// it and never misattribute the stop to a peer.
	closed atomic.Bool
}

// newRecoveryWindow opens the window: start is the instant the committed
// stream began relaying, maxElapsed the frozen policy's bound.
func newRecoveryWindow(start time.Time, maxElapsed time.Duration) *recoveryWindow {
	return &recoveryWindow{deadline: start.Add(maxElapsed)}
}

// expired reports whether the window is over at a reading of the request's
// clock. It compares against the deadline itself rather than recomputing an
// elapsed duration, so there is exactly one place the bound is expressed.
func (w *recoveryWindow) expired(now time.Time) bool { return now.After(w.deadline) }

// shut reports whether a watchdog has already reached the window.
func (w *recoveryWindow) shut() bool { return w.closed.Load() }

// armBody arms this window's hard bound against one relay pass: at the
// deadline the returned watchdog closes the upstream body the pass is reading
// from, which is what unblocks a relay sitting inside a read.
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
//     "the operator's window elapsed" from "the reader left".
//   - It creates no goroutine of its own. time.AfterFunc runs its function on
//     the runtime's timer goroutine, and the caller's stop() — deferred by
//     the relay closure — releases the timer on every path, including the one
//     where the pass ended long before the deadline.
//   - The distance it arms for is measured from the caller's own reading of
//     the request's clock to the window's one deadline, so arming it per pass
//     shares one window rather than restarting it.
func (w *recoveryWindow) armBody(now time.Time, body io.Closer) (stop func()) {
	remaining := w.deadline.Sub(now)
	if remaining <= 0 {
		// The window is already shut — the only way a pass can start here is
		// a commit that itself outlived the window. Close the body now: the
		// pass returns immediately and the loop reports the bound instead of
		// relaying bytes past a deadline the operator set.
		w.closed.Store(true)
		_ = body.Close()
		return func() {}
	}
	timer := time.AfterFunc(remaining, func() {
		w.closed.Store(true)
		_ = body.Close()
	})
	return func() { timer.Stop() }
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
// deadline. It is called only after dialable has agreed, so the window is open
// here by construction and the watchdog always has a positive distance to arm
// for.
func (w *recoveryWindow) bind(parent context.Context, now time.Time) *hopBounds {
	ctx, cancel := context.WithCancel(parent)
	timer := time.AfterFunc(w.deadline.Sub(now), func() {
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
