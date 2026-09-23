package proxy

import (
	"context"
	"net/http"
	"strconv"
	"strings"
	"time"

	"math/rand/v2"
)

// The request-scoped environment the walk runs in: a clock to read, a jitter
// draw to spread a backoff with, and one sleep site. All three are package
// vars so tests can pin them without reaching into a running handler; the
// decision itself lives in internal/recovery, which consumes the parsed
// Retry-After as a resolved duration and never parses a header itself.

// parseRetryAfter reads an upstream Retry-After value — delta-seconds or an
// HTTP-date — into a delay. Invalid, negative, zero, unparseable, or
// already-past values return 0 (no directive): an upstream cannot make the
// proxy misbehave with a hostile header, and everything it does return is
// re-capped by the policy before any sleep. Integers Atoi rejects fall
// through to the date parse, which fails too — still 0.
func parseRetryAfter(v string, now time.Time) time.Duration {
	v = strings.TrimSpace(v)
	if v == "" {
		return 0
	}
	if secs, err := strconv.Atoi(v); err == nil {
		if secs <= 0 {
			return 0
		}
		d := time.Duration(secs) * time.Second
		if d <= 0 {
			// A delta large enough to wrap the multiply is discarded here
			// rather than carried as a negative duration for a later
			// comparison to reject by accident.
			return 0
		}
		return d
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

// Test seams. Production uses the wall clock, a randomly-seeded global
// random source, and a context-aware timer wait; tests pin all three for
// determinism (the same pattern as the thinking-usage draw and the capture
// timeout). The jitter draw is the SAME non-crypto math/rand/v2 seam the
// thinking-usage share uses (thinkingDraw): de-synchronizing retries is not
// a security or key-derivation decision, no credential or identifier is
// derived from it, and predicting it buys an adversary nothing beyond
// guessing one backoff a little early. It stays math/rand/v2 deliberately —
// crypto/rand would drag an error path into a pure policy function for no
// gain.
//
// retryNow and retryJitterDraw are handed to the recovery engine as its clock
// and jitter source, so the one request is measured and spread by one clock
// and one draw, whichever layer asks.

var (
	// retryNow is the clock the walk reads.
	retryNow = time.Now

	// retryJitterDraw returns the uniform jitter factor in [-1, 1].
	retryJitterDraw = func() float64 { return 2*rand.Float64() - 1 }

	// retryWait sleeps for d, reporting whether the full delay elapsed
	// (true) or the context ended first (false). A d <= 0 returns
	// immediately with the context's liveness. It is the walk's only sleep
	// site, which is what lets a test assert "no sleep happened" on it
	// rather than on a stopwatch.
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
