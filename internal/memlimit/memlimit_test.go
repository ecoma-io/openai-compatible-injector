package memlimit

import (
	"sync"
	"sync/atomic"
	"testing"
)

func TestAcquireRefusesBeyondLimit(t *testing.T) {
	b := New(100)
	if !b.Acquire(60) {
		t.Fatal("a reservation inside the limit was refused")
	}
	if !b.Acquire(40) {
		t.Fatal("a reservation that exactly fills the budget was refused")
	}
	if b.Acquire(1) {
		t.Fatal("a reservation past the limit was admitted")
	}
	// A refusal leaves the budget exactly as full as it was: the bytes it
	// declined are neither reserved nor lost.
	if got := b.Used(); got != 100 {
		t.Fatalf("Used after a refusal = %d, want 100", got)
	}
	b.Release(100)
	if !b.Acquire(100) {
		t.Fatal("the budget did not come back after release")
	}
}

func TestAcquireNonPositiveIsANoOp(t *testing.T) {
	b := New(1)
	if !b.Acquire(0) || !b.Acquire(-5) {
		t.Fatal("a non-positive reservation must succeed: there is nothing to reserve")
	}
	if got := b.Used(); got != 0 {
		t.Fatalf("Used = %d, want 0", got)
	}
}

// TestReleaseClampsAtZero pins the one direction a mis-count must never take:
// a double release can never make the budget larger than the limit.
func TestReleaseClampsAtZero(t *testing.T) {
	b := New(10)
	if !b.Acquire(10) {
		t.Fatal("Acquire(10) refused on an empty 10-byte budget")
	}
	b.Release(10)
	b.Release(10) // a second release of the same reservation
	if got := b.Used(); got != 0 {
		t.Fatalf("Used after an over-release = %d, want 0 (never negative)", got)
	}
	if !b.Acquire(10) {
		t.Fatal("the budget is still whole after an over-release")
	}
}

func TestReleaseNonPositiveIsANoOp(t *testing.T) {
	b := New(10)
	if !b.Acquire(10) {
		t.Fatal("Acquire(10) refused on an empty 10-byte budget")
	}
	b.Release(0)
	b.Release(-1)
	if got := b.Used(); got != 10 {
		t.Fatalf("Used = %d, want 10 — a non-positive release must not add", got)
	}
}

func TestPeakTracksTheHighWaterMark(t *testing.T) {
	b := New(1000)
	b.Acquire(300)
	b.Acquire(400)
	if got := b.Peak(); got != 700 {
		t.Fatalf("Peak = %d, want 700", got)
	}
	b.Release(700)
	b.Acquire(100)
	if got := b.Peak(); got != 700 {
		t.Fatalf("Peak after a smaller reservation = %d, want the mark to hold at 700", got)
	}
	b.Acquire(100)
	b.Release(200)
	if got := b.Peak(); got != 700 {
		t.Fatalf("Peak = %d, want 700", got)
	}
}

func TestNonPositiveLimitAdmitsNothing(t *testing.T) {
	for _, limit := range []int64{0, -1} {
		b := New(limit)
		if b.Acquire(1) {
			t.Fatalf("limit %d admitted a byte", limit)
		}
		if got := b.Used(); got != 0 {
			t.Fatalf("limit %d: Used = %d, want 0", limit, got)
		}
	}
}

// TestBudgetNeverExceedsItsLimitUnderConcurrency is the property the budget
// exists for, checked as an atomic high-water mark while many goroutines
// reserve at once. It is run under -race in CI, and the reservation it hands
// each goroutine is deliberately larger than a fair share so the refusals are
// frequent rather than incidental.
//
// Each goroutine keeps what it reserves until the end of the test rather than
// releasing inside the loop. That makes the refusals certain instead of
// merely likely: the combined appetite (goroutines × iterations × unit) is
// orders of magnitude past the limit, so once the ceiling is full every later
// reservation must be refused, whatever the scheduler does. A test that only
// usually contends is a test that usually proves nothing.
func TestBudgetNeverExceedsItsLimitUnderConcurrency(t *testing.T) {
	const (
		limit  = 32 << 20
		unit   = 5 << 20 // six units fit; eight goroutines compete for them
		peers  = 8
		rounds = 500
	)
	b := New(limit)
	var (
		wg       sync.WaitGroup
		refusals atomic.Int64
		grants   atomic.Int64
	)
	start := make(chan struct{})
	for range peers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			var held int64
			for range rounds {
				if !b.Acquire(unit) {
					refusals.Add(1)
					continue
				}
				grants.Add(1)
				held += unit
				// The invariant, read from the goroutine that just reserved:
				// whatever its neighbours did, the counter it is now part of
				// can never be over the ceiling.
				if used := b.Used(); used > limit {
					t.Errorf("Used = %d, over the limit of %d", used, limit)
				}
			}
			for ; held > 0; held -= unit {
				b.Release(unit)
			}
		}()
	}
	close(start)
	wg.Wait()

	if grants.Load() == 0 {
		t.Fatal("no reservation was ever granted")
	}
	if refusals.Load() == 0 {
		t.Fatal("no reservation was ever refused: the budget was never contended")
	}
	if peak, l := b.Peak(), b.Limit(); peak > l {
		t.Fatalf("peak admission = %d, want at most the limit of %d", peak, l)
	}
	if got := b.Used(); got != 0 {
		t.Fatalf("Used after every release = %d, want 0", got)
	}
	// The whole budget is available again: nothing was lost to a race.
	if !b.Acquire(limit) {
		t.Fatalf("the full limit of %d was not available after the storm", limit)
	}
	b.Release(limit)
}
