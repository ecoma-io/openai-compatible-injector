package proxy

import (
	"context"
	"net/http"
	"testing"
	"time"
)

// TestParseRetryAfter pins the bounded directive parser: both wire forms,
// and every hostile shape collapsing to zero. The parser lives here rather
// than in internal/recovery on purpose — the engine consumes an
// Observation's RetryAfter as an already-resolved duration and never reads
// a header, so turning an upstream's header into that duration is the
// handler's business.
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
		// Parseable as an int but far past what a duration holds: the
		// multiply would wrap negative, so it is discarded explicitly.
		{"wrapping delta", "10000000000", 0},
	} {
		if got := parseRetryAfter(tc.value, now); got != tc.want {
			t.Errorf("%s: parseRetryAfter(%q) = %v, want %v", tc.name, tc.value, got, tc.want)
		}
	}
}

// stubRetryTiming pins the walk's environmental seams for a handler test:
// a clock frozen at a fixed instant (every elapsed_ms reads 0 and no window
// ever closes), jitter zeroed so a backoff is its exact scheduled value,
// and a wait that never sleeps — it reports only the context's liveness.
// The originals are restored on cleanup. A test that needs the wait to
// misbehave (simulated cancellation, a blocked sleep, a captured delay)
// overrides retryWait again after calling this.
func stubRetryTiming(t *testing.T) {
	t.Helper()
	origNow, origDraw, origWait := retryNow, retryJitterDraw, retryWait
	t.Cleanup(func() { retryNow, retryJitterDraw, retryWait = origNow, origDraw, origWait })
	frozen := time.Date(2026, time.September, 23, 12, 0, 0, 0, time.UTC)
	retryNow = func() time.Time { return frozen }
	retryJitterDraw = func() float64 { return 0 }
	retryWait = func(ctx context.Context, _ time.Duration) bool { return ctx.Err() == nil }
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
