// Package memlimit bounds the memory this process commits to buffering at
// one moment.
//
// The request path has two expensive buffers — the client's request body and
// a buffered (non-streaming) upstream response — and each is already capped
// by internal/proxy. Those caps bound a REQUEST. The failure they prevent is
// a PROCESS failure: N concurrent requests may each hold a cap's worth at
// the same time with nothing refusing the N+1-th, and the walk's retries and
// the recovery loop's re-asks multiply N by whatever the policy does rather
// than by anything the operator sets. Budget is the process-wide ceiling
// that closes the gap: a finite number of bytes, reserved before a buffer is
// filled and released when it is done with.
//
// Three decisions shape the type, and each is a policy rather than an
// implementation detail:
//
//   - ACQUIRE IS IMMEDIATE. A caller that cannot reserve its bytes is
//     refused on the spot; it is never queued and never waited for. A bounded
//     wait would park the request — its goroutine, its connection, and the
//     bytes it has already sent — which is precisely the resource this
//     budget exists to protect, and it would make the moment of refusal
//     depend on unrelated traffic. An immediate refusal is deterministic,
//     testable, and costs the process nothing to produce.
//   - A BUDGET IS NOT AN ALLOCATOR. Budget counts bytes and nothing else: it
//     never allocates, never sees the bytes it is accounting for, and never
//     decides what they are for. The caller chooses the size and the moment
//     of every reservation and every release.
//   - NO LOCK IS EVER HELD ACROSS I/O. A reservation is a compare-and-swap
//     on one counter, so a caller may reserve, perform network I/O, and
//     release without a lock existing at all — there is nothing to hold.
package memlimit

import "sync/atomic"

// Budget is a finite, process-wide byte budget. The zero Budget admits
// nothing; build one with New.
type Budget struct {
	// limit is immutable after New, so every read of it — including the
	// one inside Acquire's loop — is safe without synchronization.
	limit int64
	used  atomic.Int64
	peak  atomic.Int64
}

// New returns a Budget of limit bytes. A non-positive limit is accepted as
// stated and admits nothing: "buffer nothing at all" is a meaningful
// configuration, and a constructor that silently substituted a default
// would be the one place a caller's intent could be inverted.
func New(limit int64) *Budget {
	return &Budget{limit: limit}
}

// Limit is the budget's ceiling in bytes.
func (b *Budget) Limit() int64 { return b.limit }

// Used is how many bytes are outstanding right now.
func (b *Budget) Used() int64 { return b.used.Load() }

// Peak is the largest number of bytes that were ever outstanding at once
// since the Budget was created. It is a diagnostic — the observed high-water
// mark of the ceiling — and never an input to a decision.
func (b *Budget) Peak() int64 { return b.peak.Load() }

// Acquire reserves n bytes, reporting whether the budget could meet the
// request. It never blocks and never queues: false means the caller must
// refuse its work now, and the process is exactly as full as it was before
// the call. n <= 0 has nothing to reserve and succeeds.
//
// The counter it publishes never exceeds Limit, which is the whole
// guarantee: a caller reading Limit knows at most that many bytes are
// outstanding, whatever the callers around it did. The comparison is
// arranged as a subtraction so a reservation can never overflow the sum.
func (b *Budget) Acquire(n int64) bool {
	if n <= 0 {
		return true
	}
	for {
		used := b.used.Load()
		if n > b.limit-used {
			return false
		}
		if b.used.CompareAndSwap(used, used+n) {
			b.observePeak(used + n)
			return true
		}
	}
}

// Release returns n bytes to the budget. n <= 0 is a no-op.
//
// The counter is clamped at zero rather than wrapping negative: a release
// that does not correspond to a reservation is a bookkeeping error
// somewhere, and the one outcome it must never produce is a budget that
// hands out bytes nobody reserved. Clamping keeps "used" at or above the
// truth in every over-release case, so the ceiling still holds — the failure
// surfaces as a budget that is slightly too full, never as an unbounded one.
func (b *Budget) Release(n int64) {
	if n <= 0 {
		return
	}
	if b.used.Add(-n) < 0 {
		b.used.Store(0)
	}
}

// observePeak raises the high-water mark to now. Concurrent callers race
// harmlessly: the mark is the maximum a caller has to beat, so a lost update
// is one that lost to a larger value.
func (b *Budget) observePeak(now int64) {
	for {
		peak := b.peak.Load()
		if now <= peak || b.peak.CompareAndSwap(peak, now) {
			return
		}
	}
}
