package proxy

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/url"
	"strings"
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
	// over. It bounds the whole recovery effort, not one hop.
	recoveryMaxElapsed = "max_elapsed"
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
	}
	return dial
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
