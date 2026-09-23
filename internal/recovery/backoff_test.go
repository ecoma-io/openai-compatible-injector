package recovery

import (
	"math"
	"testing"
	"time"
)

// TestBackoffDelayLadder pins the unjittered schedule: the initial wait for
// the first retry, a doubling per further retry, and saturation at the
// ceiling rather than an overflow past it.
func TestBackoffDelayLadder(t *testing.T) {
	b := BackoffPolicy{Initial: 250 * time.Millisecond, Max: 2 * time.Second}
	cases := []struct {
		attempts int
		want     time.Duration
	}{
		{0, 250 * time.Millisecond}, // defensive: below the first attempt reads as the first
		{1, 250 * time.Millisecond},
		{2, 500 * time.Millisecond},
		{3, time.Second},
		{4, 2 * time.Second},
		{5, 2 * time.Second},
		{64, 2 * time.Second},
	}
	for _, c := range cases {
		if got := backoffDelay(b, c.attempts); got != c.want {
			t.Errorf("attempt %d: %v, want %v", c.attempts, got, c.want)
		}
	}
}

// TestJitteredBackoffSpread pins the spread's shape: a full positive draw
// raises the wait by the jitter fraction, a full negative draw lowers it, a
// zero draw is the base, and a negative swing deeper than the base is floored
// at zero rather than handed on as a negative duration.
func TestJitteredBackoffSpread(t *testing.T) {
	b := BackoffPolicy{Initial: time.Second, Max: time.Second, Jitter: 0.1}
	cases := []struct {
		name string
		draw float64
		want time.Duration
	}{
		{"no draw", 0, time.Second},
		{"full positive draw", 1, 1100 * time.Millisecond},
		{"full negative draw", -1, 900 * time.Millisecond},
		{"negative swing deeper than the base", -20, 0},
	}
	for _, c := range cases {
		draw := func() float64 { return c.draw }
		if got := jitteredBackoff(b, 1, draw); got != c.want {
			t.Errorf("%s: %v, want %v", c.name, got, c.want)
		}
	}
}

// TestJitteredBackoffNaNIsNotASpread is the guard behind Validate: NaN is
// false against every comparison, so the `Jitter <= 0` test alone would let
// it through, and `base + base*NaN` converts to the minimum int64 duration
// and clamps to zero — a configured wait silently becoming an immediate
// re-ask. Here it degrades to the unjittered base instead.
func TestJitteredBackoffNaNIsNotASpread(t *testing.T) {
	b := BackoffPolicy{Initial: time.Second, Max: time.Second, Jitter: math.NaN()}
	draw := func() float64 { return 1 }
	if got := jitteredBackoff(b, 1, draw); got != time.Second {
		t.Fatalf("NaN jitter produced %v, want the unjittered base", got)
	}
}
