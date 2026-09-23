package proxy

import (
	"context"
	"net/http"
	"testing"
	"time"

	"openai-compatible-injector/internal/config"
)

// retryTestPolicy is the policy the decision tests walk under: two retries
// per candidate after its initial attempt, a 10s window, and jitter off so
// delays are exact. Individual tests override fields they pin.
var retryTestPolicy = config.RetryPolicy{
	MaxRetries: 2,
	MaxElapsed: 10 * time.Second,
	Backoff:    config.BackoffPolicy{Initial: 100 * time.Millisecond, Max: 400 * time.Millisecond, Jitter: 0},
}

// startFor returns a candidate Start the given elapsed before now.
func startFor(now time.Time, elapsed time.Duration) time.Time { return now.Add(-elapsed) }

// TestClassifyStatusMatrix pins the closed status matrix, row by row:
// retryable, fallback-only, terminal, and answer — including unknown codes
// on both sides of the 500 line and the out-of-range verbatim shapes.
func TestClassifyStatusMatrix(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
		want   statusDisposition
	}{
		// Retry-then-fallback: the transient rows.
		{"408", http.StatusRequestTimeout, dispRetryable},
		{"425", http.StatusTooEarly, dispRetryable},
		{"429", http.StatusTooManyRequests, dispRetryable},
		{"500", http.StatusInternalServerError, dispRetryable},
		{"502", http.StatusBadGateway, dispRetryable},
		{"503", http.StatusServiceUnavailable, dispRetryable},
		{"504", http.StatusGatewayTimeout, dispRetryable},
		{"unknown 529", 529, dispRetryable},
		{"edge 599", 599, dispRetryable},
		{"unknown 510", 510, dispRetryable},
		// Fallback-only: definitive answers worth trying the NEXT provider
		// for, never the same one again.
		{"401", http.StatusUnauthorized, dispFallbackOnly},
		{"403", http.StatusForbidden, dispFallbackOnly},
		{"404", http.StatusNotFound, dispFallbackOnly},
		{"405", http.StatusMethodNotAllowed, dispFallbackOnly},
		{"409", http.StatusConflict, dispFallbackOnly},
		{"422", http.StatusUnprocessableEntity, dispFallbackOnly},
		// Terminal: relaid exactly as a single-provider deployment would.
		{"400", http.StatusBadRequest, dispTerminal},
		{"406", http.StatusNotAcceptable, dispTerminal},
		{"410", http.StatusGone, dispTerminal},
		{"413", http.StatusRequestEntityTooLarge, dispTerminal},
		{"415", http.StatusUnsupportedMediaType, dispTerminal},
		{"416", http.StatusRequestedRangeNotSatisfiable, dispTerminal},
		{"421", http.StatusMisdirectedRequest, dispTerminal},
		{"424", http.StatusFailedDependency, dispTerminal},
		{"428", http.StatusPreconditionRequired, dispTerminal},
		{"431", http.StatusRequestHeaderFieldsTooLarge, dispTerminal},
		{"451", http.StatusUnavailableForLegalReasons, dispTerminal},
		{"unlisted 418", http.StatusTeapot, dispTerminal},
		{"unlisted 426", http.StatusUpgradeRequired, dispTerminal},
		{"edge 499", 499, dispTerminal},
		{"501", http.StatusNotImplemented, dispTerminal},
		{"505", http.StatusHTTPVersionNotSupported, dispTerminal},
		// Answers: committed as-is, never classified for retry.
		{"200", http.StatusOK, dispAnswer},
		{"204", http.StatusNoContent, dispAnswer},
		{"301", http.StatusMovedPermanently, dispAnswer},
		{"304", http.StatusNotModified, dispAnswer},
		{"verbatim 600", 600, dispAnswer},
	} {
		if got := classifyStatus(tc.status); got != tc.want {
			t.Errorf("%s: classifyStatus(%d) = %v, want %v", tc.name, tc.status, got, tc.want)
		}
	}
}

// TestStatusReasonTokens pins the closed reason tokens: the named
// retryable statuses, the 5xx bucket, and the numbered remainder.
func TestStatusReasonTokens(t *testing.T) {
	for _, tc := range []struct {
		status int
		want   string
	}{
		{http.StatusRequestTimeout, "http_408"},
		{http.StatusTooEarly, "http_425"},
		{http.StatusTooManyRequests, "http_429"},
		{http.StatusInternalServerError, "http_5xx"},
		{http.StatusBadGateway, "http_5xx"},
		{529, "http_5xx"},
		{http.StatusUnauthorized, "http_401"},
		{http.StatusNotFound, "http_404"},
		{451, "http_451"},
	} {
		if got := statusReason(tc.status); got != tc.want {
			t.Errorf("statusReason(%d) = %q, want %q", tc.status, got, tc.want)
		}
	}
}

// TestEvaluateRetryTerminalAlwaysFinalizes: an answer or a terminal status
// ends the walk regardless of budget, window, or directive — nothing is
// ever decided on their behalf.
func TestEvaluateRetryTerminalAlwaysFinalizes(t *testing.T) {
	now := time.Now()
	for _, disp := range []statusDisposition{dispAnswer, dispTerminal} {
		for _, hasNext := range []bool{true, false} {
			got := evaluateRetry(retryInput{
				Disp: disp, Attempts: 1, HasNext: hasNext,
				Start: startFor(now, 0), Now: now, Policy: retryTestPolicy,
				RetryAfter: 30 * time.Second,
			})
			if got.Action != retryFinalize || got.Delay != 0 {
				t.Errorf("disp %d hasNext %v: action %v delay %v, want finalize/0", disp, hasNext, got.Action, got.Delay)
			}
		}
	}
}

// TestEvaluateRetryFallbackOnlyMovesWithoutWaiting: a fallback-only status
// moves straight to the next candidate when one is reachable and finalizes
// when one is not — never a same-candidate retry, never a delay.
func TestEvaluateRetryFallbackOnlyMovesWithoutWaiting(t *testing.T) {
	now := time.Now()
	in := retryInput{
		Disp: dispFallbackOnly, Attempts: 1, HasNext: true,
		Start: startFor(now, 0), Now: now, Policy: retryTestPolicy,
		RetryAfter: 30 * time.Second,
	}
	got := evaluateRetry(in)
	if got.Action != retryNextCandidate || got.Delay != 0 {
		t.Errorf("has next: action %v delay %v, want nextCandidate/0", got.Action, got.Delay)
	}
	in.HasNext = false
	got = evaluateRetry(in)
	if got.Action != retryFinalize || got.Delay != 0 {
		t.Errorf("no next: action %v delay %v, want finalize/0", got.Action, got.Delay)
	}
}

// TestEvaluateRetrySameCandidateBudget: MaxRetries bounds the retries AFTER
// the initial attempt — a candidate gets exactly MaxRetries+1 exchanges,
// then the walk moves on.
func TestEvaluateRetrySameCandidateBudget(t *testing.T) {
	now := time.Now()
	// max-retries 2: attempts 1 and 2 retry the same candidate, attempt 3
	// moves forward when it can.
	for attempts, want := range map[int]retryAction{1: retrySameCandidate, 2: retrySameCandidate, 3: retryNextCandidate} {
		got := evaluateRetry(retryInput{
			Disp: dispRetryable, Attempts: attempts, HasNext: true,
			Start: startFor(now, 0), Now: now, Policy: retryTestPolicy,
		})
		if got.Action != want {
			t.Errorf("maxRetries 2 attempts %d: action %v, want %v", attempts, got.Action, want)
		}
	}
	// Budget exhausted with no reachable next candidate finalizes.
	got := evaluateRetry(retryInput{
		Disp: dispRetryable, Attempts: 3, HasNext: false,
		Start: startFor(now, 0), Now: now, Policy: retryTestPolicy,
	})
	if got.Action != retryFinalize {
		t.Errorf("exhausted, no next: action %v, want finalize", got.Action)
	}
	// max-retries 0: the initial failure itself moves on — no re-ask.
	zero := retryTestPolicy
	zero.MaxRetries = 0
	got = evaluateRetry(retryInput{
		Disp: dispRetryable, Attempts: 1, HasNext: true,
		Start: startFor(now, 0), Now: now, Policy: zero,
	})
	if got.Action != retryNextCandidate {
		t.Errorf("maxRetries 0: action %v, want nextCandidate", got.Action)
	}
}

// TestEvaluateRetryWindowClosesRetries: once MaxElapsed has passed since
// the candidate's first attempt, same-candidate retries stop even with
// attempts left; inside the window the delay is capped to what remains.
func TestEvaluateRetryWindowClosesRetries(t *testing.T) {
	now := time.Now()
	// Window fully spent → move on.
	got := evaluateRetry(retryInput{
		Disp: dispRetryable, Attempts: 1, HasNext: true,
		Start: startFor(now, retryTestPolicy.MaxElapsed), Now: now, Policy: retryTestPolicy,
	})
	if got.Action != retryNextCandidate {
		t.Errorf("window spent: action %v, want nextCandidate", got.Action)
	}
	// One millisecond left: retry, but the delay is clipped to it.
	got = evaluateRetry(retryInput{
		Disp: dispRetryable, Attempts: 1, HasNext: true,
		Start: startFor(now, retryTestPolicy.MaxElapsed-time.Millisecond), Now: now, Policy: retryTestPolicy,
	})
	if got.Action != retrySameCandidate || got.Delay != time.Millisecond {
		t.Errorf("window nearly spent: action %v delay %v, want retry/1ms", got.Action, got.Delay)
	}
}

// TestEvaluateRetryDelayProgression pins the pure delay arithmetic with
// jitter off: exponential growth capped at backoff.max, Retry-After as a
// floor over the backoff, and the ceiling/window/deadline caps in order.
func TestEvaluateRetryDelayProgression(t *testing.T) {
	now := time.Now()
	// A wider attempt budget so the cap-at-max row is reachable: four
	// retries means the fourth (initial + 3) still retries.
	policy := retryTestPolicy
	policy.MaxRetries = 4
	build := func(attempts int, retryAfter time.Duration, elapsed time.Duration, deadline time.Time) retryInput {
		return retryInput{
			Disp: dispRetryable, Attempts: attempts, HasNext: true,
			Start: startFor(now, elapsed), Now: now, Policy: policy,
			CallerDeadline: deadline, RetryAfter: retryAfter,
		}
	}
	for _, tc := range []struct {
		name string
		in   retryInput
		want time.Duration
	}{
		{"first retry waits initial", build(1, 0, 0, time.Time{}), 100 * time.Millisecond},
		{"second retry doubles", build(2, 0, 0, time.Time{}), 200 * time.Millisecond},
		{"third retry doubles again", build(3, 0, 0, time.Time{}), 400 * time.Millisecond},
		{"growth caps at max", build(4, 0, 0, time.Time{}), 400 * time.Millisecond},
		{"retry-after floor above backoff", build(1, 300*time.Millisecond, 0, time.Time{}), 300 * time.Millisecond},
		{"retry-after capped at backoff max", build(1, 30*time.Second, 0, time.Time{}), 400 * time.Millisecond},
		{"window caps the delay", build(2, 0, policy.MaxElapsed-150*time.Millisecond, time.Time{}), 150 * time.Millisecond},
		{"deadline caps the delay", build(1, 0, 0, now.Add(50*time.Millisecond)), 50 * time.Millisecond},
	} {
		got := evaluateRetry(tc.in)
		if got.Action != retrySameCandidate || got.Delay != tc.want {
			t.Errorf("%s: action %v delay %v, want retry/%v", tc.name, got.Action, got.Delay, tc.want)
		}
	}
	// A deadline that already fired ends the candidate's retries.
	got := evaluateRetry(build(1, 0, 0, now.Add(-time.Second)))
	if got.Action != retryNextCandidate {
		t.Errorf("passed deadline: action %v, want nextCandidate", got.Action)
	}
}

// TestEvaluateRetryBudgetCheckedBeforeDirective: a spent budget moves on
// even when the upstream sent a Retry-After — the directive sizes a wait,
// it never re-opens a closed budget.
func TestEvaluateRetryBudgetCheckedBeforeDirective(t *testing.T) {
	now := time.Now()
	got := evaluateRetry(retryInput{
		Disp: dispRetryable, Attempts: 3, HasNext: true,
		Start: startFor(now, 0), Now: now, Policy: retryTestPolicy,
		RetryAfter: 5 * time.Second,
	})
	if got.Action != retryNextCandidate || got.Delay != 0 {
		t.Errorf("spent budget with directive: action %v delay %v, want nextCandidate/0", got.Action, got.Delay)
	}
}

// TestBackoffDelayTable pins the unjittered progression directly: initial,
// doubling per retry, capped at max, iterative so no attempt count can
// overflow.
func TestBackoffDelayTable(t *testing.T) {
	b := config.BackoffPolicy{Initial: 100 * time.Millisecond, Max: 700 * time.Millisecond}
	for _, tc := range []struct {
		attempts int
		want     time.Duration
	}{
		{1, 100 * time.Millisecond},
		{2, 200 * time.Millisecond},
		{3, 400 * time.Millisecond},
		{4, 700 * time.Millisecond}, // 800 capped to max
		{9, 700 * time.Millisecond},
	} {
		if got := backoffDelay(b, tc.attempts); got != tc.want {
			t.Errorf("backoffDelay(attempts=%d) = %v, want %v", tc.attempts, got, tc.want)
		}
	}
	// A max at or below the initial pins every delay to the max.
	same := config.BackoffPolicy{Initial: 500 * time.Millisecond, Max: 500 * time.Millisecond}
	if got := backoffDelay(same, 5); got != 500*time.Millisecond {
		t.Errorf("initial==max: got %v, want %v", got, 500*time.Millisecond)
	}
}

// TestJitteredBackoffBounds pins the jitter spread against injected draws:
// -1 takes the delay to its lower bound, +1 to its upper, 0 leaves it
// exact, and jitter 0 disables the draw entirely.
func TestJitteredBackoffBounds(t *testing.T) {
	orig := retryJitterDraw
	defer func() { retryJitterDraw = orig }()
	b := config.BackoffPolicy{Initial: time.Second, Max: time.Second, Jitter: 0.1}

	for _, tc := range []struct {
		draw float64
		want time.Duration
	}{
		{-1, 900 * time.Millisecond},
		{0, time.Second},
		{1, 1100 * time.Millisecond},
	} {
		retryJitterDraw = func() float64 { return tc.draw }
		if got := jitteredBackoff(b, 1); got != tc.want {
			t.Errorf("draw %v: jitteredBackoff = %v, want %v", tc.draw, got, tc.want)
		}
	}
	// Jitter zero: base returned, draw never consulted.
	retryJitterDraw = func() float64 { panic("draw must not be called when jitter is 0") }
	none := b
	none.Jitter = 0
	if got := jitteredBackoff(none, 1); got != time.Second {
		t.Errorf("jitter 0: got %v, want the base", got)
	}
}

// TestParseRetryAfter pins the bounded directive parser: both wire forms,
// and every hostile shape collapsing to zero.
func TestParseRetryAfter(t *testing.T) {
	now := time.Date(2026, time.September, 23, 12, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name  string
		value string
		want  time.Duration
	}{
		{"delta seconds", "30", 30 * time.Second},
		{"padded delta", "  7  ", 7 * time.Second},
		{"future http-date", now.Add(90 * time.Second).UTC().Format(http.TimeFormat), 90 * time.Second},
		{"past http-date", now.Add(-time.Hour).UTC().Format(http.TimeFormat), 0},
		{"zero", "0", 0},
		{"negative", "-5", 0},
		{"empty", "", 0},
		{"garbage", "soon", 0},
		{"overflow", "99999999999999999999", 0},
	} {
		if got := parseRetryAfter(tc.value, now); got != tc.want {
			t.Errorf("%s: parseRetryAfter(%q) = %v, want %v", tc.name, tc.value, got, tc.want)
		}
	}
}

// TestRetryWaitDefaultRespectsContext pins the production wait seam: a
// live context waits out the delay, a canceled one returns immediately.
func TestRetryWaitDefaultRespectsContext(t *testing.T) {
	ctx := context.Background()
	if !retryWait(ctx, time.Millisecond) {
		t.Error("live context: retryWait reported early exit, want full delay")
	}
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	if retryWait(cctx, time.Hour) {
		t.Error("canceled context: retryWait reported full delay, want early exit")
	}
	if retryWait(cctx, 0) {
		t.Error("canceled context, zero delay: retryWait reported live, want dead")
	}
	if !retryWait(ctx, 0) {
		t.Error("live context, zero delay: retryWait reported dead, want live")
	}
}
