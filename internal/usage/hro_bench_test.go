package usage

import (
	"context"
	"io"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// discardLogger is the pipeline's logger: writes nowhere, at a level that
// emits nothing, so no benchmark measures the logging path instead of the
// work under test.
func discardLogger() zerolog.Logger {
	return zerolog.New(io.Discard).Level(zerolog.Disabled)
}

// hroNoopRepo is an InsertRepository that accepts every batch immediately —
// the shape of a healthy database for the request path's purposes, so the
// benchmark measures the pipeline's own bookkeeping, not Postgres.
type hroNoopRepo struct{ inserts atomic.Int64 }

func (r *hroNoopRepo) InsertEvents(_ context.Context, events []Event) error {
	r.inserts.Add(int64(len(events)))
	return nil
}

func hroEvent(i int) Event {
	return Event{
		EventID:          "00000000-0000-4000-8000-" + strconv.Itoa(i),
		OccurredAt:       time.Unix(1_700_000_000, 0),
		RequestID:        "0123456789abcdef",
		ConfigGeneration: 1,
		PublicModel:      "public",
		Provider:         "openai",
		UpstreamModel:    "upstream-model",
		API:              "chat",
		HTTPStatus:       200,
		Outcome:          "relayed",
		BytesIn:          4096,
		BytesOut:         8192,
		ProviderAttempts: 1,
		EgressAttempts:   1,
		EgressKind:       "direct",
		LatencyMS:        42,
	}
}

// BenchmarkHroPipelineRecord measures the request path's ONLY cost on the
// usage path: a mutex-guarded non-blocking channel send plus a stat bump.
// One call per metered request.
func BenchmarkHroPipelineRecord(b *testing.B) {
	p := newPipeline(&hroNoopRepo{}, discardLogger(), 8192, 256, time.Hour, time.Hour, 3)
	defer p.Close(context.Background())
	ev := hroEvent(1)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p.Record(ev)
	}
}

// BenchmarkHroPipelineRecordParallel measures the Record mutex under
// concurrency — one shared lock on the request path when metering is on.
func BenchmarkHroPipelineRecordParallel(b *testing.B) {
	for _, par := range []int{1, 2, 4, 8, 16, 32} {
		b.Run("par"+strconv.Itoa(par), func(b *testing.B) {
			p := newPipeline(&hroNoopRepo{}, discardLogger(), 1<<16, 256, time.Hour, time.Hour, 3)
			defer p.Close(context.Background())
			ev := hroEvent(1)
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					p.Record(ev)
				}
			})
		})
	}
}

// BenchmarkHroNewEventID measures the UUIDv4 mint — one crypto/rand read per
// metered request.
func BenchmarkHroNewEventID(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = NewEventID()
	}
}

// BenchmarkHroUsageExtract measures the pre-rewrite usage capture the
// response paths run on EVERY relayed body (buffered: once; streamed: once
// per data line the gate admits).
func BenchmarkHroUsageExtract(b *testing.B) {
	small := []byte(`{"id":"x","model":"u","choices":[{"message":{"role":"assistant","content":"hi"}}],"usage":{"prompt_tokens":10,"completion_tokens":20,"total_tokens":30}}`)
	chunk := []byte(`{"id":"x","object":"chat.completion.chunk","model":"u","choices":[{"index":0,"delta":{"content":"hi"}}],"usage":{"prompt_tokens":10,"completion_tokens":20,"total_tokens":30}}`)
	resp := []byte(`{"type":"response.completed","response":{"id":"r","model":"u","status":"completed","output":[],"usage":{"input_tokens":5,"output_tokens":9,"total_tokens":14}},"sequence_number":41}`)
	big := make([]byte, 0, 64<<10)
	big = append(big, `{"id":"x","model":"u","choices":[{"message":{"role":"assistant","content":"`...)
	for len(big) < 64<<10 {
		big = append(big, 'm')
	}
	big = append(big, `"}}],"usage":{"prompt_tokens":10,"completion_tokens":20,"total_tokens":30}}`...)

	b.Run("chat_small", func(b *testing.B) {
		b.SetBytes(int64(len(small)))
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, ok := ExtractChatUsage(small); !ok {
				b.Fatal("no usage")
			}
		}
	})
	b.Run("chat_chunk", func(b *testing.B) {
		b.SetBytes(int64(len(chunk)))
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, ok := ExtractChatUsage(chunk); !ok {
				b.Fatal("no usage")
			}
		}
	})
	b.Run("chat_64KB", func(b *testing.B) {
		b.SetBytes(int64(len(big)))
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, ok := ExtractChatUsage(big); !ok {
				b.Fatal("no usage")
			}
		}
	})
	b.Run("responses_envelope", func(b *testing.B) {
		b.SetBytes(int64(len(resp)))
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, ok := ExtractResponsesUsage(resp); !ok {
				b.Fatal("no usage")
			}
		}
	})
}

// BenchmarkHroCaptureObserve measures the streaming capture's per-line cost:
// the observe seam runs on every data line the relay admits.
func BenchmarkHroCaptureObserve(b *testing.B) {
	line := []byte(`{"id":"x","object":"chat.completion.chunk","model":"u","choices":[{"index":0,"delta":{"content":"hi"}}],"usage":{"prompt_tokens":10,"completion_tokens":20,"total_tokens":30}}`)
	c := NewCapture("chat")
	b.SetBytes(int64(len(line)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Observe(line)
	}
}
