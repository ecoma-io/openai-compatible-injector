package recovery

import (
	"math"
	"time"
)

// backoffDelay is the unjittered wait before a retry: the initial delay for
// the first retry, doubled once per further retry and saturating at the
// ceiling. attempts is the number of attempts already made against the
// candidate (1 = the initial attempt just failed, so its retry waits
// Initial).
//
// It is iterative rather than a shift so no attempt count can overflow the
// duration before the ceiling clamps it — the ceiling is validated to at
// most two minutes, well inside an int64 nanosecond count.
func backoffDelay(b BackoffPolicy, attempts int) time.Duration {
	if attempts < 1 {
		attempts = 1
	}
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

// jitteredBackoff spreads the wait by a uniform ±Jitter fraction — a 1s
// delay at jitter 0.1 lands in 900ms..1100ms — so a provider coming back
// from an outage is not re-hit by a synchronized herd of retries. The draw
// is injected rather than read from a package global: the caller owns the
// randomness source, which is what lets a test pin the schedule exactly.
//
// A negative draw cannot produce a negative delay: a full negative swing is
// floored at zero, and the policy's ceiling is applied by the caller before
// the wait is scheduled.
//
// NaN is treated as no jitter rather than as a fraction: Validate rejects it,
// and this second gate keeps the arithmetic honest for a policy that reached
// here without passing validation — every comparison against NaN is false, so
// the range test above would let it through and the spread would convert to
// the minimum int64 duration, clamping the wait to zero.
func jitteredBackoff(b BackoffPolicy, attempts int, draw func() float64) time.Duration {
	base := backoffDelay(b, attempts)
	if math.IsNaN(b.Jitter) || b.Jitter <= 0 {
		return base
	}
	d := base + time.Duration(float64(base)*b.Jitter*draw())
	if d < 0 {
		return 0
	}
	return d
}
