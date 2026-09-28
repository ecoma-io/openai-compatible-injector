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
	// bound: the window's watchdog closes the upstream body the relay is
	// blocked on, so a stream that stops producing bytes without closing its
	// connection cannot outlive it (armRecoveryWindow).
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
func (h *injectorHandler) dialContinuation(hop continuationHop) continuationDial {
	var dial continuationDial
	// The endpoint normalized exactly as the walk normalizes it: trailing
	// slashes trimmed, RawPath cleared so EscapedPath cannot percent-decode
	// the endpoint path behind the caller's back.
	upstream := *hop.cand.Endpoint
	upstream.Path = strings.TrimRight(upstream.Path, "/") + hop.suffix
	upstream.RawPath = ""
	dial.upstream = &upstream
	dial.credKey = hop.credKey

	out, terr := hop.transform(hop.body, hop.model)
	if terr != nil {
		dial.phase, dial.err = "build", terr
		return dial
	}
	req, rerr := http.NewRequestWithContext(hop.ctx, http.MethodPost, upstream.String(), bytes.NewReader(out))
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
			Ctx:       hop.ctx,
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
		dial.phase = "dial"
	} else if dial.resp == nil {
		// The pool's zero-dial budget refusal is the ONE Execute outcome that
		// is neither an answer nor an error: a nil response with a nil error,
		// raised when the envelope would not fund this attempt's first dial
		// (internal/transport/pool.go). It is a refusal by this proxy, so it
		// names the budget phase and blames no endpoint — the same reading the
		// walk gives it. Naming the phase here is what keeps the caller off
		// dial.resp: an answer that never arrived must never be dereferenced.
		dial.phase = "budget"
	}
	return dial
}

// armRecoveryWindow arms the recovery window's HARD bound against one relay
// pass: at end — a wall-clock instant, computed once when the committed
// stream began relaying — the returned watchdog closes the upstream body the
// pass is reading from, which is what unblocks a relay sitting inside a read.
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
//   - end is read once for the whole recovery effort, so every hop shares one
//     window rather than each pass restarting its own clock. The bound is
//     `max-elapsed` since the commit, exactly as the policy states it.
//
// closed is set only by the watchdog itself, so a caller can read it after a
// pass to tell a relay this proxy cut from one the upstream cut.
func armRecoveryWindow(end time.Time, body io.Closer, closed *atomic.Bool) (stop func()) {
	remaining := time.Until(end)
	if remaining <= 0 {
		// The window is already shut — the only way a pass can start here is
		// a commit that itself outlived the window. Close the body now: the
		// pass returns immediately and the loop reports the bound instead of
		// relaying bytes past a deadline the operator set.
		closed.Store(true)
		_ = body.Close()
		return func() {}
	}
	timer := time.AfterFunc(remaining, func() {
		closed.Store(true)
		_ = body.Close()
	})
	return func() { timer.Stop() }
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
