package recovery

import (
	"net/http"
	"strconv"
	"strings"
	"time"
)

// ParseRetryAfter reads an upstream Retry-After value — delta-seconds or an
// HTTP-date — into a delay.
//
// Invalid, negative, zero, unparseable, and already-past values all return 0,
// which the engine reads as "no directive": an upstream cannot make the
// proxy misbehave with a hostile header. What it does return is still only a
// floor, and the engine re-caps it against the policy's own ceiling, the
// backoff ceiling, the remaining windows, and the caller's deadline before
// any sleep is scheduled.
//
// Integers Atoi rejects fall through to the date parse, which fails too —
// still 0.
func ParseRetryAfter(v string, now time.Time) time.Duration {
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
