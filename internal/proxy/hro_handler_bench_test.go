package proxy

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/rs/zerolog"

	"openai-compatible-injector/internal/config"
	"openai-compatible-injector/internal/credential"
	"openai-compatible-injector/internal/transport"
	"openai-compatible-injector/internal/usage"
)

// ---------------------------------------------------------------------------
// End-to-end buffered request lifecycle through the real handler: auth, body
// read under admission, probe, snapshot model lookup, engine build, per-attempt
// transform, outbound build, upstream exchange, buffered read under admission,
// composed response rewrite, and the client write.
//
// This is the number a production optimization claim has to move, and the
// only benchmark here that includes the real wire (loopback).
// ---------------------------------------------------------------------------

// hroSilentLogger is the handler's logger at a level that emits nothing. The
// handler's log is a zerolog.Logger; the request path builds a derived logger
// per request (With().Str().Str().Logger()), so a benchmark that leaves the
// level at info would measure event CONSTRUCTION on top of the request.
func hroSilentLogger() zerolog.Logger {
	return zerolog.New(io.Discard).Level(zerolog.Disabled)
}

// hroFastRetryRecovery is the recovery block used by the retry/fallback
// benchmarks: 3 retries, a 1ms backoff so a benchmark does not spend 250ms
// per re-ask, and a 3-candidate fallback reach.
const hroFastRetryRecovery = `
recovery:
  retries:
    max-retries: 3
    backoff:
      initial: 1ms
      max: 2ms
  fallback:
    enabled: true
    max-candidates: 3
`

// hroUpstream is a fake upstream whose answer shape and failure pattern the
// benchmark controls. hits counts the exchanges it actually received — the
// ground truth for "how many times was a body transformed".
type hroUpstream struct {
	srv      *httptest.Server
	hits     atomic.Int64
	bodies   atomic.Int64
	respBody string
	// failFirst answers the first n requests with failStatus (the 503 that
	// the shipped matrix turns into a same-candidate retry), then succeeds.
	failFirst  int64
	failStatus int
	// models records the upstream "model" every exchange carried, so a walk's
	// per-candidate transform is directly observable: one entry per real
	// transform, one per real exchange.
	models   []string
	modelsMu sync.Mutex
}

func newHroUpstream(b testing.TB, respBody string, failFirst int64, failStatus int, cands int) *hroUpstream {
	b.Helper()
	u := &hroUpstream{respBody: respBody, failFirst: failFirst, failStatus: failStatus}
	u.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		u.hits.Add(1)
		u.bodies.Add(int64(len(raw)))
		var probe struct {
			Model string `json:"model"`
		}
		_ = json.Unmarshal(raw, &probe)
		u.modelsMu.Lock()
		u.models = append(u.models, probe.Model)
		u.modelsMu.Unlock()
		if u.hits.Load() <= u.failFirst {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(u.failStatus)
			_, _ = io.WriteString(w, `{"error":{"message":"try again","type":"server_error","code":"overloaded"}}`)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, u.respBody)
	}))
	b.Cleanup(u.srv.Close)
	return u
}

func (u *hroUpstream) seenModels() []string {
	u.modelsMu.Lock()
	defer u.modelsMu.Unlock()
	return append([]string(nil), u.models...)
}

// hroDefaultPrompt is the per-model injection prompt every handler benchmark
// configures unless an arm deliberately turns injection off.
const hroDefaultPrompt = "You are a helpful assistant."

// hroAuthBlock is a two-key rotation pool for one provider.
const hroAuthBlock = `    auth:
        type: api_key
        header: Authorization
        prefix: "Bearer "
        strategy: round_robin
        rate-limit:
            cooldown: 2s
            max-cooldown: 60s
        keys:
            - id: primary
              value: sk-example-primary
            - id: secondary
              value: sk-example-secondary
`

// hroStore builds a store whose `m` model is served by one candidate per
// upstream in order, with the given extra model YAML. withAuth attaches a
// credential pool to every candidate's provider.
func hroStore(tb testing.TB, upstreams []*hroUpstream, extraModel string) *config.Store {
	return hroStorePrompt(tb, upstreams, extraModel, hroDefaultPrompt)
}

func hroStorePrompt(tb testing.TB, upstreams []*hroUpstream, extraModel, prompt string) *config.Store {
	return hroStoreAuthPrompt(tb, upstreams, extraModel, false, prompt)
}

func hroStoreAuth(tb testing.TB, upstreams []*hroUpstream, extraModel string, withAuth bool) *config.Store {
	return hroStoreAuthPrompt(tb, upstreams, extraModel, withAuth, hroDefaultPrompt)
}

func hroStoreAuthPrompt(tb testing.TB, upstreams []*hroUpstream, extraModel string, withAuth bool, prompt string) *config.Store {
	tb.Helper()
	var providers, chain strings.Builder
	for i, u := range upstreams {
		providers.WriteString("  p" + strconv.Itoa(i) + ":\n    base-url: " + u.srv.URL + "\n")
		if withAuth {
			providers.WriteString(hroAuthBlock)
		}
		chain.WriteString("      - provider: p" + strconv.Itoa(i) + "\n        upstream-model: up-" + strconv.Itoa(i) + "\n")
	}
	yaml := "api-key: " + testAPIKey + "\nproviders:\n" + providers.String() + "models:\n  m:\n    injection-prompt: " + strconv.Quote(prompt) + "\n    providers:\n" + chain.String() + extraModel
	snap, err := config.LoadRuntime([]byte(yaml))
	if err != nil {
		tb.Fatalf("LoadRuntime: %v", err)
	}
	return config.NewStore(snap)
}

func hroHandler(tb testing.TB, store *config.Store, creds *credential.Registry, meter usage.Ingest) http.Handler {
	tb.Helper()
	return NewHandler(store, directResolver(), creds, nil, meter, hroSilentLogger())
}

// hroChatRequest builds a client chat body of ~want bytes.
func hroChatRequest(tb testing.TB, want int, stream bool) string {
	tb.Helper()
	var sb strings.Builder
	sb.WriteString(`{"model":"m","messages":[{"role":"system","content":"be brief."},{"role":"user","content":"`)
	sb.WriteString(strings.Repeat("m", want))
	sb.WriteString(`"}]`)
	if stream {
		sb.WriteString(`,"stream":true`)
	}
	sb.WriteString(`}`)
	return sb.String()
}

func hroChatResponse(want int) string {
	return `{"id":"chatcmpl-1","object":"chat.completion","created":1,"model":"up-name","choices":[{"index":0,"message":{"role":"assistant","content":"` +
		strings.Repeat("r", want) + `"},"finish_reason":"stop"}],"usage":{"prompt_tokens":10,"completion_tokens":20,"total_tokens":30}}`
}

// hroDoBuffered runs one buffered chat request through the handler and
// returns the recorder, so a caller can assert the status.
func hroDoBuffered(tb testing.TB, h http.Handler, body string) *httptest.ResponseRecorder {
	tb.Helper()
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+testAPIKey)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

// BenchmarkHroServeBuffered is the end-to-end buffered request: the whole
// handler, a real loopback exchange, and the full response transform. Sizes
// are the mandated 1 KB / 64 KB / 1 MB, plus the injection-on and
// injection-off arms because the prompt merge is the transform's dominant
// cost when configured.
func BenchmarkHroServeBuffered(b *testing.B) {
	for _, size := range []int{1 << 10, 64 << 10, 1 << 20} {
		for _, inject := range []bool{true, false} {
			name := "size" + strconv.Itoa(size) + "/inject=" + strconv.FormatBool(inject)
			b.Run(name, func(b *testing.B) {
				up := newHroUpstream(b, hroChatResponse(1<<10), 0, 0, 1)
				prompt := hroDefaultPrompt
				if !inject {
					prompt = ""
				}
				h := hroHandler(b, hroStorePrompt(b, []*hroUpstream{up}, "", prompt), nil, nil)
				body := hroChatRequest(b, size, false)
				b.SetBytes(int64(len(body)))
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					rec := hroDoBuffered(b, h, body)
					if rec.Code != http.StatusOK {
						b.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
					}
				}
			})
		}
	}
}

// BenchmarkHroServeBufferedResponseSize varies the UPSTREAM RESPONSE size at a
// fixed small request, isolating the response-side cost: the admitted
// buffered read, the validity gate, and the composed rewrite.
func BenchmarkHroServeBufferedResponseSize(b *testing.B) {
	for _, size := range []int{1 << 10, 64 << 10, 1 << 20, 8 << 20} {
		b.Run("resp"+strconv.Itoa(size), func(b *testing.B) {
			up := newHroUpstream(b, hroChatResponse(size), 0, 0, 1)
			h := hroHandler(b, hroStore(b, []*hroUpstream{up}, ""), nil, nil)
			body := hroChatRequest(b, 1<<10, false)
			b.SetBytes(int64(size))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				rec := hroDoBuffered(b, h, body)
				if rec.Code != http.StatusOK {
					b.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
				}
			}
		})
	}
}

// BenchmarkHroServeStripFields measures the response path with a provider
// strip list configured, so the strip rewriter runs on every relayed body
// (and the SSE gate is widened) — the config-driven worst case for the
// composed rewrite.
func BenchmarkHroServeStripFields(b *testing.B) {
	for _, size := range []int{1 << 10, 64 << 10} {
		b.Run("resp"+strconv.Itoa(size), func(b *testing.B) {
			up := newHroUpstream(b, `{"provider":"kilo","service_tier":"std","model":"up-name","id":"x","choices":[{"message":{"role":"assistant","content":"`+strings.Repeat("r", size)+`"},"finish_reason":"stop"}],"usage":{"prompt_tokens":1,"completion_tokens":2,"total_tokens":3}}`, 0, 0, 1)
			extra := "    strip-fields:\n      - provider\n      - service_tier\n"
			h := hroHandler(b, hroStore(b, []*hroUpstream{up}, extra), nil, nil)
			body := hroChatRequest(b, 1<<10, false)
			b.SetBytes(int64(size))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				rec := hroDoBuffered(b, h, body)
				if rec.Code != http.StatusOK {
					b.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
				}
			}
		})
	}
}

// BenchmarkHroServeWithMeter measures the metered request path: the capture
// created per request, the pre-rewrite observation, and the pipeline Record
// at Close.
func BenchmarkHroServeWithMeter(b *testing.B) {
	up := newHroUpstream(b, hroChatResponse(1<<10), 0, 0, 1)
	store := hroStore(b, []*hroUpstream{up}, "")
	p := newNoopPipeline(b)
	h := hroHandler(b, store, nil, p)
	body := hroChatRequest(b, 1<<10, false)
	b.SetBytes(int64(len(body)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rec := hroDoBuffered(b, h, body)
		if rec.Code != http.StatusOK {
			b.Fatalf("status = %d", rec.Code)
		}
	}
}

// ---------------------------------------------------------------------------
// Recovery: how many times is a body transformed across retries and
// fallback? The upstream's own hit counter is the ground truth.
// ---------------------------------------------------------------------------

// TestHroTransformRepetitionE2E measures the ACTUAL repetition count of the
// request transform by counting the upstream's real exchanges and the distinct
// upstream model names each answer carried. It is a test rather than a
// benchmark because the count is the finding, not a timing — and a count
// cannot be made less trustworthy by a noisy machine.
func TestHroTransformRepetitionE2E(t *testing.T) {
	// hroFastRetryRecovery widens the policy to 3 retries / 3 candidates /
	// 1ms backoff; "" keeps the shipped default (1 retry, 2 candidates).
	cases := []struct {
		name      string
		cands     int
		failFirst int64
		recovery  string
	}{
		{"default_no_failure", 1, 0, ""},
		{"default_primary_503_once", 1, 1, ""},
		{"default_two_candidates", 2, 0, ""},
		{"wide_same_candidate_retry_3", 1, 3, hroFastRetryRecovery},
		{"wide_fallback_2_candidates", 2, 0, hroFastRetryRecovery},
		{"wide_fallback_3_candidates", 3, 0, hroFastRetryRecovery},
		{"wide_fallback_3_last_retries_1", 3, 1, hroFastRetryRecovery},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ups := make([]*hroUpstream, tc.cands)
			// Every candidate but the last fails PERMANENTLY (503 forever),
			// so the walk must enter all tc.cands of them; the last candidate
			// answers 503 for its first failFirst exchanges then succeeds —
			// so the whole configured walk AND its retries run in one request.
			ups[tc.cands-1] = newHroUpstream(t, hroChatResponse(256), tc.failFirst, http.StatusServiceUnavailable, tc.cands)
			for i := 0; i < tc.cands-1; i++ {
				ups[i] = newHroUpstream(t, "", 1<<30, http.StatusServiceUnavailable, tc.cands)
			}
			store := hroStore(t, ups, tc.recovery)
			h := NewHandler(store, directResolver(), nil, nil, nil, hroSilentLogger())
			rec := hroDoBuffered(t, h, hroChatRequest(t, 512, false))
			if rec.Code != http.StatusOK {
				t.Fatalf("status = %d body = %s", rec.Code, rec.Body.String())
			}
			total := int64(0)
			for _, u := range ups {
				total += u.hits.Load()
			}
			models := ups[tc.cands-1].seenModels()
			t.Logf("HRO transform repetition e2e: %-32s candidates=%d -> upstream exchanges=%d, distinct transform calls=%d, models seen on answering candidate=%v",
				tc.name, tc.cands, total, total, models)
		})
	}
}

// ---------------------------------------------------------------------------
// Concurrency: credential contention, egress contention, shared-pool
// contention, at low and high concurrency through the real handler.
// ---------------------------------------------------------------------------

// BenchmarkHroServeParallelBuffered measures the whole handler under
// concurrency: every request sharing the snapshot store, the credential
// registry's pool mutex, the transport registry's Doer map, and the
// memlimit CAS counter.
func BenchmarkHroServeParallelBuffered(b *testing.B) {
	base := newHroUpstream(b, hroChatResponse(1<<10), 0, 0, 1)
	for _, conc := range []int{1, 4, 16, 64} {
		b.Run("conc"+strconv.Itoa(conc), func(b *testing.B) {
			store := hroStoreAuth(b, []*hroUpstream{base}, "", true)
			h := hroHandler(b, store, credential.NewRegistry(), nil)
			body := hroChatRequest(b, 1<<10, false)
			b.SetBytes(int64(len(body)))
			b.ReportAllocs()
			b.ResetTimer()
			var ok, fail atomic.Int64
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					rec := hroDoBuffered(b, h, body)
					if rec.Code == http.StatusOK {
						ok.Add(1)
					} else {
						fail.Add(1)
					}
				}
			})
			if n := fail.Load(); n > 0 {
				b.Fatalf("concurrent failures: %d", n)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Streaming, end to end: a real SSE relay through the real handler, so the
// per-event relay cost is measured together with the handler's own
// per-stream work (keep-alive wiring, the recovery loop's idle window, the
// usage observer).
// ---------------------------------------------------------------------------

// hroSSEUpstream answers every stream request with n SSE events, flushed one
// at a time, and records the exchanges it served.
type hroSSEUpstream struct {
	srv    *httptest.Server
	events int
	sse    string
	hits   atomic.Int64
}

func newHroSSEUpstream(b testing.TB, events int, chunkBytes int, responses bool) *hroSSEUpstream {
	b.Helper()
	var sb strings.Builder
	for i := 0; i < events; i++ {
		if responses {
			sb.WriteString("event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"" +
				strings.Repeat("t", chunkBytes) + "\"}\n\n")
		} else {
			sb.WriteString(`data: {"id":"c","object":"chat.completion.chunk","model":"up-name","choices":[{"index":0,"delta":{"content":"` +
				strings.Repeat("m", chunkBytes) + `"}}],"usage":{"prompt_tokens":1,"completion_tokens":2,"total_tokens":3}}` + "\n\n")
		}
	}
	if !responses {
		sb.WriteString("data: [DONE]\n\n")
	} else {
		sb.WriteString("event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"r\",\"model\":\"up-name\",\"status\":\"completed\"}}\n\n")
	}
	u := &hroSSEUpstream{events: events, sse: sb.String()}
	u.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		u.hits.Add(1)
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, u.sse)
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
	}))
	b.Cleanup(u.srv.Close)
	return u
}

// BenchmarkHroServeSSE is the streaming end-to-end relay: the handler's
// per-stream setup plus CopySSE over a real body. events x chunkBytes is the
// stream size, so the arms cover short, long, and high-frequency streams.
func BenchmarkHroServeSSE(b *testing.B) {
	for _, tc := range []struct {
		name       string
		events     int
		chunkBytes int
		responses  bool
	}{
		{"chat_10x16B", 10, 16, false},
		{"chat_100x64B", 100, 64, false},
		{"chat_1000x64B", 1000, 64, false},
		{"chat_200x2KB", 200, 2048, false},
		{"chat_50x16KB", 50, 16 << 10, false},
		{"responses_500x64B", 500, 64, true},
	} {
		b.Run(tc.name, func(b *testing.B) {
			up := newHroSSEUpstream(b, tc.events, tc.chunkBytes, tc.responses)
			yaml := "api-key: " + testAPIKey + "\nproviders:\n  p0:\n    base-url: " + up.srv.URL + "\nmodels:\n  m:\n    injection-prompt: \"be brief.\"\n    providers:\n      - provider: p0\n        upstream-model: up-0\n"
			snap, err := config.LoadRuntime([]byte(yaml))
			if err != nil {
				b.Fatalf("LoadRuntime: %v", err)
			}
			h := hroHandler(b, config.NewStore(snap), nil, nil)
			path := "/v1/chat/completions"
			if tc.responses {
				path = "/v1/responses"
			}
			body := hroChatRequest(b, 1<<10, true)
			if tc.responses {
				body = `{"model":"m","instructions":"be brief.","input":"hello","stream":true}`
			}
			b.SetBytes(int64(len(up.sse)))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
				req.Header.Set("Authorization", "Bearer "+testAPIKey)
				req.Header.Set("Content-Type", "application/json")
				rec := httptest.NewRecorder()
				h.ServeHTTP(rec, req)
				if rec.Code != http.StatusOK {
					b.Fatalf("status = %d body = %s", rec.Code, rec.Body.String())
				}
			}
		})
	}
}

// BenchmarkHroServeSSEParallel measures N concurrent SSE relays through the
// handler — the shape that decides how many streams a process can hold. Each
// stream pins a keep-alive-free relay goroutine, a memlimit reservation, and
// (when configured) a credential permit.
func BenchmarkHroServeSSEParallel(b *testing.B) {
	for _, conc := range []int{1, 8, 32} {
		b.Run("conc"+strconv.Itoa(conc), func(b *testing.B) {
			up := newHroSSEUpstream(b, 200, 64, false)
			yaml := "api-key: " + testAPIKey + "\nproviders:\n  p0:\n    base-url: " + up.srv.URL + "\nmodels:\n  m:\n    injection-prompt: \"be brief.\"\n    providers:\n      - provider: p0\n        upstream-model: up-0\n"
			snap, err := config.LoadRuntime([]byte(yaml))
			if err != nil {
				b.Fatalf("LoadRuntime: %v", err)
			}
			h := hroHandler(b, config.NewStore(snap), nil, nil)
			body := hroChatRequest(b, 1<<10, true)
			b.SetBytes(int64(len(up.sse)))
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
					req.Header.Set("Authorization", "Bearer "+testAPIKey)
					req.Header.Set("Content-Type", "application/json")
					rec := httptest.NewRecorder()
					h.ServeHTTP(rec, req)
					if rec.Code != http.StatusOK {
						b.Errorf("status = %d", rec.Code)
						return
					}
				}
			})
		})
	}
}

// hroNoopRepo accepts every batch immediately: the shape of a healthy
// database from the request path's point of view.
type hroNoopRepo struct{}

func (hroNoopRepo) InsertEvents(context.Context, []usage.Event) error { return nil }

// newNoopPipeline is a live metering pipeline over the no-op repository, so
// the metered request path is measured without Postgres.
func newNoopPipeline(tb testing.TB) *usage.Pipeline {
	tb.Helper()
	p := usage.NewPipeline(hroNoopRepo{}, zerolog.New(io.Discard).Level(zerolog.Disabled))
	tb.Cleanup(func() { p.Close(context.Background()) })
	return p
}

var _ = fmt.Sprint
var _ = transport.Direct
