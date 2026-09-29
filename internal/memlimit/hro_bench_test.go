package memlimit

import (
	"strconv"
	"testing"
)

// ---------------------------------------------------------------------------
// memlimit: the process-wide byte admission. One Acquire/Release pair per
// growing block of every buffer on the request path.
// ---------------------------------------------------------------------------

// BenchmarkHroBudgetAcquire measures the uncontended CAS reservation + peak
// observation: the per-block cost of admitting one buffer. Serial, so this is
// the pure atomic cost with no cache-line bouncing.
func BenchmarkHroBudgetAcquire(b *testing.B) {
	bud := New(256 << 20)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if !bud.Acquire(64 << 10) {
			b.Fatal("refused")
		}
		bud.Release(64 << 10)
	}
}

// BenchmarkHroBudgetAcquireLarge measures the same path with a 1 MiB
// reservation — the admissionBlockMax step a growing 64 MiB buffer pays.
func BenchmarkHroBudgetAcquireLarge(b *testing.B) {
	bud := New(256 << 20)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if !bud.Acquire(1 << 20) {
			b.Fatal("refused")
		}
		bud.Release(1 << 20)
	}
}

// BenchmarkHroBudgetRefused measures the refusal path: what a request pays
// when the process is full (the 503 capacity_exceeded decision point).
func BenchmarkHroBudgetRefused(b *testing.B) {
	bud := New(1 << 20)
	if !bud.Acquire(1 << 20) {
		b.Fatal("setup")
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if bud.Acquire(1) {
			b.Fatal("should refuse")
		}
	}
}

// BenchmarkHroBudgetParallel measures contention on the single used counter
// at low and high concurrency. This is the ONLY shared counter on the
// buffered request path, and every concurrent request's body read and every
// buffered upstream answer both hit it.
func BenchmarkHroBudgetParallel(b *testing.B) {
	for _, tc := range []struct {
		name string
		par  int
	}{
		{"par1", 1},
		{"par2", 2},
		{"par4", 4},
		{"par8", 8},
		{"par16", 16},
		{"par32", 32},
	} {
		b.Run(tc.name, func(b *testing.B) {
			bud := New(1 << 40)
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				var sink int64
				for pb.Next() {
					if !bud.Acquire(4096) {
						b.Fatal("refused")
					}
					sink++
					bud.Release(4096)
				}
				_ = sink
			})
			b.SetBytes(4096)
		})
	}
}

// BenchmarkHroBudgetContendedWriters drives the counter from par goroutines
// that each hold their reservation until the round ends, so the counter
// climbs and the CAS must fail and retry under sustained write contention —
// the worst case a saturated process creates.
func BenchmarkHroBudgetContendedWriters(b *testing.B) {
	for _, par := range []int{2, 4, 8, 16} {
		b.Run("par"+strconv.Itoa(par), func(b *testing.B) {
			bud := New(1 << 40)
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					bud.Acquire(64)
				}
			})
		})
	}
}
