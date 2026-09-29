package recovery

import (
	"context"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// Recovery engine: the decision surface every attempt crosses. Pure — no I/O,
// no clock beyond the seam it is handed.
// ---------------------------------------------------------------------------

// hroEngine returns an engine over the shipped default policy with a fixed
// clock and a fixed jitter draw, so a benchmark measures the engine's own
// arithmetic rather than entropy or scheduling.
func hroEngine(tb testing.TB) *Engine {
	tb.Helper()
	p := Default()
	return NewEngine(context.Background(), p,
		WithClock(func() time.Time { return time.Unix(1_700_000_000, 0) }),
		WithJitterSource(func() float64 { return 0.5 }),
	)
}

// BenchmarkHroEngineNew measures the per-request engine construction: the
// handler builds one per request, and Default() builds a fresh 20+-row
// matrix each call only in the config layer — here the policy is reused, so
// this isolates NewEngine plus NewBudget.
func BenchmarkHroEngineNew(b *testing.B) {
	p := Default()
	ctx := context.Background()
	now := func() time.Time { return time.Unix(1_700_000_000, 0) }
	draw := func() float64 { return 0.5 }
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = NewEngine(ctx, p, WithClock(now), WithJitterSource(draw))
	}
}

// BenchmarkHroPolicyDefault measures building the shipped default policy from
// scratch — what a config load does, and what any operator editing the file
// pays. COLD path (startup / reload), reported for completeness.
func BenchmarkHroPolicyDefault(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = Default()
	}
}

// BenchmarkHroPolicyHash measures the per-candidate policy hash, which
// handler.go reads as cand.RecoveryHash — resolved at config load in the
// shipped code, so this bounds what a load-time change would cost.
func BenchmarkHroPolicyHash(b *testing.B) {
	p := Default()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = p.Hash()
	}
}

// BenchmarkHroPolicyValidate measures policy validation, a load-time cost.
func BenchmarkHroPolicyValidate(b *testing.B) {
	p := Default()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := p.Validate(); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkHroEngineObserve measures one decision: the matrix match plus the
// mechanical sizing. This runs once per finished attempt, i.e. at most a
// handful of times per client request.
func BenchmarkHroEngineObserve(b *testing.B) {
	cases := []struct {
		name string
		obs  Observation
	}{
		{"http_503_retry", Observation{Class: FailureHTTP, HTTPStatus: 503, CandidateIndex: 1, CandidateAttempt: 1}},
		{"http_400_terminal", Observation{Class: FailureHTTP, HTTPStatus: 400, CandidateIndex: 1, CandidateAttempt: 1}},
		{"http_429_retry", Observation{Class: FailureHTTP, HTTPStatus: 429, CandidateIndex: 1, CandidateAttempt: 1}},
		{"transport_connection", Observation{Class: FailureTransport, TransportClass: TransportClassConnection, TransportCause: CauseConnectionRefused, CandidateIndex: 1, CandidateAttempt: 1}},
		{"protocol_invalid", Observation{Class: FailureProtocol, ProtocolCause: ProtocolInvalidResponse, CandidateIndex: 1, CandidateAttempt: 1}},
	}
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			eng := hroEngine(b)
			if !eng.EnterCandidate(eng.policy) {
				b.Fatal("EnterCandidate")
			}
			obs := tc.obs
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// CandidateAttempt is bumped so the retry branch keeps
				// returning the same disposition instead of falling into
				// the exhausted path; the work per call is the same.
				obs.CandidateAttempt = i%2 + 1
				_ = eng.Observe(obs)
			}
		})
	}
}

// BenchmarkHroEngineWalk measures a whole walk's decision cost under three
// shapes: no retry, a same-candidate retry, and a multi-candidate fallback.
// The wall-clock per walk is dominated by the policy's backoff (250ms
// default), so this measures the engine's ARITHMETIC per walk — which is the
// part this proxy owns — and reports the decision count per op.
func BenchmarkHroEngineWalk(b *testing.B) {
	cases := []struct {
		name     string
		attempts int // attempts on the primary before the walk moves
		cands    int
	}{
		{"no_retry_1_attempt", 0, 1},
		{"same_candidate_retry_1", 1, 1},
		{"same_candidate_retry_2", 2, 1},
		{"fallback_2_candidates", 1, 2},
		{"fallback_3_candidates", 1, 3},
	}
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				p := Default()
				eng := NewEngine(context.Background(), p,
					WithClock(func() time.Time { return time.Unix(1_700_000_000, 0) }),
					WithJitterSource(func() float64 { return 0.5 }),
				)
				decisions := 0
				for c := 0; c < tc.cands; c++ {
					if !eng.EnterCandidate(p) {
						break
					}
					for a := 0; a <= tc.attempts; a++ {
						d := eng.Observe(Observation{Class: FailureHTTP, HTTPStatus: 503, CandidateIndex: c + 1, CandidateAttempt: a + 1})
						decisions++
						if d.Action != ActionRetry {
							break
						}
					}
				}
				if decisions == 0 {
					b.Fatal("no decisions")
				}
			}
		})
	}
}

// BenchmarkHroBudgetAcquire measures one exchange-envelope claim, the mutex
// the transport takes immediately before every dial. Both scopes must be
// opened — an exchange is funded by the request AND the candidate envelope,
// so a budget with no candidate opened refuses the first claim.
func BenchmarkHroBudgetAcquire(b *testing.B) {
	now := func() time.Time { return time.Unix(1_700_000_000, 0) }
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bd := NewBudget(Envelope{MaxExchanges: 32, MaxElapsed: time.Hour}, now)
		bd.BeginCandidate(Envelope{MaxExchanges: 16, MaxElapsed: time.Hour})
		for j := 0; j < 8; j++ {
			if !bd.AcquireExchange().Granted {
				b.Fatal("refused")
			}
		}
	}
}
