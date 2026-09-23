package proxy

import (
	"context"
	"net/http"
	"strconv"
	"strings"
	"time"

	"math/rand/v2"

	"openai-compatible-injector/internal/config"
)

// Provider-level retry policy. The injector owns provider retry/fallback;
// the transport layer owns egress recovery — this file never touches pools
// or proxies, and the pool never sees a status-driven retry. Everything
// here is a pure function of its inputs: no clocks, no sleeping, no I/O.
// The handler supplies the facts (observed status, budget state, parsed
// Retry-After, clock readings) and executes the returned decision; tests
// supply their own clock, jitter draw, and wait function through the
// package vars at the bottom.

// statusDisposition is the closed classification of one upstream result
// under the provider retry matrix. It is code-owned, not configurable: the
// matrix is the reviewable policy, and no status outside these rules is
// ever retried.
type statusDisposition int

const (
	// dispAnswer is a result the walk commits as-is: 2xx (buffered or
	// SSE), 3xx, 204, 304, and anything above 599. Never retried.
	dispAnswer statusDisposition = iota
	// dispTerminal is an upstream error the walk relays (normalized)
	// exactly as a single-provider deployment would: 400, 406, 410, 413,
	// 415, 416, 421, 424, 428, 431, 451, 501, 505, and every unlisted
	// status. A body that fails one candidate's transform fails every
	// candidate's — 400 asks again nowhere. Never retried, never fallen
	// back over.
	dispTerminal
	// dispFallbackOnly moves to the next candidate immediately — no
	// same-candidate retry: 401, 403, 404, 405, 409, 422. A credential or
	// authorization rejection justifies trying the NEXT configured
	// provider, never hammering the same one; a 404/409/422 is a request
	// the provider has already answered definitively.
	dispFallbackOnly
	// dispRetryable re-asks the same candidate while its retry budget
	// lasts, then moves to the next candidate: 408, 425, 429, every 5xx
	// (known and unknown), and malformed/incomplete upstream responses
	// received before commitment (an unparseable 200 body, an error body
	// whose bounded capture stalled or failed).
	dispRetryable
)

// classifyStatus maps an upstream HTTP status to its disposition. The
// handler branches 2xx and verbatim statuses before calling, but the
// function is total: anything outside 400..599 is an answer, and every
// status inside the range outside the explicitly listed sets lands on
// dispTerminal — an unrecognized status is never accidentally retryable.
func classifyStatus(status int) statusDisposition {
	if status < http.StatusBadRequest || status > 599 {
		return dispAnswer
	}
	switch status {
	case http.StatusRequestTimeout, // 408
		http.StatusTooEarly,        // 425
		http.StatusTooManyRequests: // 429
		return dispRetryable
	case http.StatusUnauthorized, // 401
		http.StatusForbidden,           // 403
		http.StatusNotFound,            // 404
		http.StatusMethodNotAllowed,    // 405
		http.StatusConflict,            // 409
		http.StatusUnprocessableEntity: // 422
		return dispFallbackOnly
	case http.StatusNotImplemented, // 501
		http.StatusHTTPVersionNotSupported: // 505
		// The one terminal carve-outs inside 5xx: the provider cannot speak
		// this protocol at all, and no amount of re-asking changes that.
		return dispTerminal
	}
	if status >= http.StatusInternalServerError {
		// Every other 5xx — the closed list above plus unknown codes (520,
		// 529, whatever a broken peer emits) — is a provider-side failure
		// worth one bounded re-ask.
		return dispRetryable
	}
	return dispTerminal
}

// Retry reason tokens. Closed set, code-owned, never derived from error
// text: the exact retryable statuses name themselves, other statuses carry
// their number, and the malformed/incomplete-response cases carry typed
// tokens. Transport-layer failures keep the transport package's existing
// closed causes and never enter this list.
const (
	reasonHTTP408        = "http_408"
	reasonHTTP425        = "http_425"
	reasonHTTP429        = "http_429"
	reasonHTTP5xx        = "http_5xx"
	reasonInvalidBody    = "upstream_invalid_response"
	reasonBodyTimeout    = "upstream_body_timeout"
	reasonBodyReadFailed = "upstream_body_read_failed"
)

// statusReason is the closed reason token for an upstream HTTP status.
func statusReason(status int) string {
	switch status {
	case http.StatusRequestTimeout:
		return reasonHTTP408
	case http.StatusTooEarly:
		return reasonHTTP425
	case http.StatusTooManyRequests:
		return reasonHTTP429
	}
	if status >= http.StatusInternalServerError && status <= 599 {
		return reasonHTTP5xx
	}
	return "http_" + strconv.Itoa(status)
}

// retryAction is what the handler does with one failed attempt.
type retryAction int

const (
	// retrySameCandidate re-asks the current candidate after Delay.
	retrySameCandidate retryAction = iota
	// retryNextCandidate moves to the next candidate immediately — no
	// wait between candidates; a different provider has not earned a
	// sleep off this request's clock.
	retryNextCandidate
	// retryFinalize ends the walk: the last received answer (or the
	// unreachable/invalid synthesis) becomes the client response.
	retryFinalize
)

// retryDecision is the outcome of evaluating one failed attempt against
// the retry policy. Action and Delay only; the handler owns execution.
type retryDecision struct {
	Action retryAction
	// Delay applies only to retrySameCandidate: the bounded wait before
	// the next attempt, already capped by the backoff ceiling, the
	// remaining retry window, and the caller's remaining deadline.
	Delay time.Duration
}

// retryInput carries the facts one decision needs. Everything is a value;
// nothing here can observe a clock or the outside world on its own.
type retryInput struct {
	// Disp is the failed attempt's disposition (never dispAnswer — that
	// result would have been committed, not decided).
	Disp statusDisposition
	// Attempts is the number of attempts already made against the current
	// candidate, >= 1 (1 = the initial attempt just failed).
	Attempts int
	// HasNext reports whether a next candidate is reachable — another
	// chain entry exists AND the provider-fallback budget still covers it.
	HasNext bool
	// Start is when the current candidate's first attempt began.
	Start time.Time
	// Now is the current reading of the injected clock.
	Now time.Time
	// Policy is the model's snapshot-bound retry policy.
	Policy config.RetryPolicy
	// CallerDeadline is the request context's deadline, zero when none.
	CallerDeadline time.Time
	// RetryAfter is the upstream's parsed Retry-After directive, 0 when
	// absent or unparseable. Honored as a floor over the jittered backoff,
	// always capped by the policy's backoff ceiling and remaining budgets.
	RetryAfter time.Duration
}

// evaluateRetry decides what one failed attempt means for the walk. The
// budgets compose: a candidate's own attempt budget (MaxRetries AFTER its
// initial attempt) and its elapsed window are both enforced here, and
// whichever runs out first stops the same-candidate retries — the walk
// then either moves to the next candidate (when the fallback budget still
// reaches one) or finalizes.
func evaluateRetry(in retryInput) retryDecision {
	switch in.Disp {
	case dispAnswer, dispTerminal:
		return retryDecision{Action: retryFinalize}
	case dispFallbackOnly:
		return moveOrFinalize(in.HasNext)
	case dispRetryable:
	}
	// Same-candidate budget: Attempts counts exchanges against this
	// candidate, so Attempts > MaxRetries means the initial attempt plus
	// MaxRetries retries have all failed.
	if in.Attempts > in.Policy.MaxRetries {
		return moveOrFinalize(in.HasNext)
	}
	windowLeft := in.Policy.MaxElapsed - in.Now.Sub(in.Start)
	if windowLeft <= 0 {
		return moveOrFinalize(in.HasNext)
	}
	delay := jitteredBackoff(in.Policy.Backoff, in.Attempts)
	if in.RetryAfter > delay {
		delay = in.RetryAfter
	}
	// The caps, in order: the configured ceiling (an upstream can never
	// stretch a sleep past backoff.max — Retry-After: 3600 stays a 2s
	// nap by default), then the remaining window, then the caller's
	// remaining deadline. A capped-to-zero delay means no room to wait:
	// the candidate's retries are over.
	if delay > in.Policy.Backoff.Max {
		delay = in.Policy.Backoff.Max
	}
	if delay > windowLeft {
		delay = windowLeft
	}
	if !in.CallerDeadline.IsZero() {
		until := in.CallerDeadline.Sub(in.Now)
		if until <= 0 {
			return moveOrFinalize(in.HasNext)
		}
		if delay > until {
			delay = until
		}
	}
	if delay < 0 {
		delay = 0
	}
	return retryDecision{Action: retrySameCandidate, Delay: delay}
}

// moveOrFinalize is the shared tail of every budget exhaustion: forward
// when the fallback budget still reaches a candidate, done when it does
// not.
func moveOrFinalize(hasNext bool) retryDecision {
	if hasNext {
		return retryDecision{Action: retryNextCandidate}
	}
	return retryDecision{Action: retryFinalize}
}

// backoffDelay is the unjittered delay before retry number `attempts`
// (attempts 1 = the initial attempt failed, so its retry waits Initial):
// Initial doubled per retry, capped at Max. Iterative, so no shift can
// overflow on a large attempt count.
func backoffDelay(b config.BackoffPolicy, attempts int) time.Duration {
	d := b.Initial
	for i := 1; i < attempts; i++ {
		d *= 2
		if d >= b.Max {
			return b.Max
		}
	}
	if d > b.Max {
		return b.Max
	}
	return d
}

// jitteredBackoff spreads the unjittered delay by a uniform ±Jitter
// fraction (Jitter 0.1 on a 1s delay → 900ms..1100ms), so a recovering
// provider is not re-hit by a synchronized herd of retries. The draw comes
// from the injected source; the result never goes negative.
func jitteredBackoff(b config.BackoffPolicy, attempts int) time.Duration {
	base := backoffDelay(b, attempts)
	if b.Jitter <= 0 {
		return base
	}
	d := base + time.Duration(float64(base)*b.Jitter*retryJitterDraw())
	if d < 0 {
		d = 0
	}
	return d
}

// parseRetryAfter reads an upstream Retry-After value — delta-seconds or
// an HTTP-date — into a delay. Invalid, negative, zero, unparseable, or
// already-past values return 0 (no directive): an upstream cannot make the
// proxy misbehave with a hostile header, and everything it does return is
// re-capped by the policy before any sleep. Overflowing integers fail the
// Atoi and fall through to the date parse, which fails too — still 0.
func parseRetryAfter(v string, now time.Time) time.Duration {
	v = strings.TrimSpace(v)
	if v == "" {
		return 0
	}
	if secs, err := strconv.Atoi(v); err == nil {
		if secs <= 0 {
			return 0
		}
		return time.Duration(secs) * time.Second
	}
	if t, err := http.ParseTime(v); err == nil {
		d := t.Sub(now)
		if d <= 0 {
			return 0
		}
		return d
	}
	return 0
}

// Test seams. Production uses the wall clock, a crypto-quality global
// random source, and a context-aware timer wait; tests pin all three for
// determinism (the same pattern as the thinking-usage draw and the capture
// timeout).

var (
	// retryNow is the clock the walk reads.
	retryNow = time.Now

	// retryJitterDraw returns the uniform jitter factor in [-1, 1].
	retryJitterDraw = func() float64 { return 2*rand.Float64() - 1 }

	// retryWait sleeps for d, reporting whether the full delay elapsed
	// (true) or the context ended first (false). A d <= 0 returns
	// immediately with the context's liveness.
	retryWait = func(ctx context.Context, d time.Duration) bool {
		if d <= 0 {
			return ctx.Err() == nil
		}
		t := time.NewTimer(d)
		defer t.Stop()
		select {
		case <-t.C:
			return true
		case <-ctx.Done():
			return false
		}
	}
)
