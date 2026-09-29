package proxy

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"openai-compatible-injector/internal/memlimit"
)

// withBufferBudget points the process-wide admission at a test-sized budget
// for the duration of one test. The handler reads the package variable per
// request, exactly as it reads the two per-request caps, so the swap is
// visible to every request the test makes. It is restored when the test ends
// — after everything a test defers, since cleanups run last — and must not
// move while a request is in flight.
func withBufferBudget(t *testing.T, limit int64) *memlimit.Budget {
	t.Helper()
	b := memlimit.New(limit)
	old := bufferBudget
	bufferBudget = b
	t.Cleanup(func() { bufferBudget = old })
	return b
}

// TestBufferBudgetRefusesARequestBody503 pins the request-body refusal: a
// body that outgrows the process-wide budget, mid-read, is answered with the
// canonical 503 envelope rather than being allowed to grow. The budget here
// is deliberately larger than the first block, so the refusal lands on a
// request that has already been read in part — the case where a leaked
// reservation would be invisible to the caller but permanent to the process.
func TestBufferBudgetRefusesARequestBody503(t *testing.T) {
	budget := withBufferBudget(t, 200<<10) // 200 KiB: one 64 KiB and one 128 KiB block, then no more

	_, log := captureLog(zerolog.InfoLevel)
	h := NewHandler(newTestStore(t, "http://127.0.0.1:9/v1"), directResolver(), nil, nil, nil, log)

	body := `{"model":"test-model","padding":"` + strings.Repeat("x", 400<<10) + `"}`
	rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions", body, nil)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rec.Code)
	}
	if got := rec.Body.String(); got != envelopeAtCapacity {
		t.Errorf("body mismatch:\n got %s\nwant %s", got, envelopeAtCapacity)
	}
	// The partial read is given back: the request holds nothing once it is
	// over, so the next request sees the whole budget again.
	if got := budget.Used(); got != 0 {
		t.Fatalf("Used after a refused body = %d, want 0", got)
	}
	if peak := budget.Peak(); peak <= admissionBlockStart {
		t.Fatalf("peak admission = %d, want the read to have been admitted before it was refused", peak)
	}
}

// TestBufferBudgetRefusesABufferedResponse503 pins the other expensive
// buffer. The budget funds the request body and nothing else, so the answer
// — which is small, and would fit any sane budget — cannot be admitted. The
// refusal is a local capacity answer, not a provider failure: a 502 with a
// provider class here would send an operator after an upstream that behaved
// perfectly.
func TestBufferBudgetRefusesABufferedResponse503(t *testing.T) {
	budget := withBufferBudget(t, admissionBlockStart) // exactly one 64 KiB block

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"model":"upstream-name","choices":[]}`)
	}))
	defer upstream.Close()

	_, log := captureLog(zerolog.InfoLevel)
	h := NewHandler(newTestStore(t, upstream.URL+"/v1"), directResolver(), nil, nil, nil, log)
	rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions", `{"model":"test-model"}`, nil)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rec.Code)
	}
	if got := rec.Body.String(); got != envelopeAtCapacity {
		t.Errorf("body mismatch:\n got %s\nwant %s", got, envelopeAtCapacity)
	}
	if got := budget.Used(); got != 0 {
		t.Fatalf("Used after a refused answer = %d, want 0", got)
	}
}

// TestAdmissionReleasedOnEveryReturnPath walks the request paths that take
// an admission and then leave by different doors — the buffered success, the
// streaming success, the rejections that happen before any upstream call,
// and the size cap — and asserts each one hands its bytes back. A path that
// forgot to would not fail loudly: it would simply make the process a little
// fuller on every request until the budget refused traffic that used to
// work.
func TestAdmissionReleasedOnEveryReturnPath(t *testing.T) {
	upstreamBody := `{"model":"upstream-name","choices":[{"index":0,"message":{"role":"assistant","content":"hi"}}]}`
	newUpstream := func(t *testing.T) *httptest.Server {
		t.Helper()
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if strings.Contains(r.URL.Path, "responses") {
				w.Header().Set("Content-Type", "text/event-stream")
				_, _ = io.WriteString(w, "event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"model\":\"upstream-name\"}\n\n")
				_, _ = io.WriteString(w, "event: response.completed\ndata: {\"type\":\"response.completed\",\"model\":\"upstream-name\"}\n\n")
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, upstreamBody)
		}))
		t.Cleanup(srv.Close)
		return srv
	}

	pad := func(n int) string { return strings.Repeat("x", n) }
	cases := []struct {
		name     string
		path     string
		body     string
		budget   int64
		bodyCap  int64
		wantCode int
	}{
		{name: "buffered_success", path: "/v1/chat/completions", body: `{"model":"test-model"}`, wantCode: http.StatusOK},
		{name: "stream_success", path: "/v1/responses", body: `{"model":"test-model","stream":true}`, wantCode: http.StatusOK},
		{name: "invalid_json", path: "/v1/chat/completions", body: `{nope`, wantCode: http.StatusBadRequest},
		{name: "missing_model", path: "/v1/chat/completions", body: `{"messages":[]}`, wantCode: http.StatusBadRequest},
		{name: "model_not_found", path: "/v1/chat/completions", body: `{"model":"nope"}`, wantCode: http.StatusNotFound},
		{
			name: "body_too_large", path: "/v1/chat/completions",
			body:     `{"model":"test-model","pad":"` + pad(300<<10) + `"}`,
			bodyCap:  64 << 10,
			wantCode: http.StatusRequestEntityTooLarge,
		},
		{
			name: "capacity_refused", path: "/v1/chat/completions",
			body:     `{"model":"test-model","pad":"` + pad(300<<10) + `"}`,
			budget:   200 << 10,
			wantCode: http.StatusServiceUnavailable,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			limit := tc.budget
			if limit == 0 {
				limit = 64 << 20
			}
			budget := withBufferBudget(t, limit)
			if tc.bodyCap != 0 {
				old := maxRequestBodyBytes
				maxRequestBodyBytes = tc.bodyCap
				defer func() { maxRequestBodyBytes = old }()
			}

			upstream := newUpstream(t)
			h := newTestHandler(t, newTestStore(t, upstream.URL+"/v1"))

			rec := doRequest(t, h, http.MethodPost, tc.path, tc.body, nil)
			if rec.Code != tc.wantCode {
				t.Fatalf("status = %d, want %d (body %s)", rec.Code, tc.wantCode, rec.Body.String())
			}
			if got := budget.Used(); got != 0 {
				t.Fatalf("Used = %d after the request returned, want 0 — the path leaked its admission", got)
			}
		})
	}
}

// TestAdmissionHeldWhileTheRequestBodyIsLive is the other half of the
// release contract: the reservation must not be handed back early. The
// upstream reads the budget while the proxy is inside the call, where the
// request body is still live for the transforms and the continuation
// builder — a proxy that released it before the walk would be advertising
// memory it is still holding, and the budget would stop bounding anything.
func TestAdmissionHeldWhileTheRequestBodyIsLive(t *testing.T) {
	budget := withBufferBudget(t, 64<<20)

	seen := make(chan int64, 1)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		seen <- budget.Used()
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"model":"upstream-name","choices":[]}`)
	}))
	defer upstream.Close()

	h := newTestHandler(t, newTestStore(t, upstream.URL+"/v1"))
	body := `{"model":"test-model","pad":"` + strings.Repeat("x", 200<<10) + `"}`
	rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions", body, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (%s)", rec.Code, rec.Body.String())
	}
	select {
	case held := <-seen:
		if held <= admissionBlockStart {
			t.Fatalf("budget held %d bytes during the upstream call, want the request body's reservation to still be live", held)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the upstream was never called")
	}
}

// TestAdmissionReleasedWhenTheRequestPanics pins the third exit from a
// request: the panic unwinding. The streaming path is the one that can
// panic in the wild — a relay fault after the client's 2xx headers — and it
// is the path where a leaked reservation would be worst, because the bytes
// are held for the whole life of a long-lived stream. A panicking transport
// stands in for that fault: the defer that releases runs whichever way the
// stack unwinds.
func TestAdmissionReleasedWhenTheRequestPanics(t *testing.T) {
	budget := withBufferBudget(t, 64<<20)

	h := NewHandler(newTestStore(t, "http://panic.test/v1"),
		fixedDoer{d: panicDoer{}}, nil, nil, nil, zerolog.Nop())

	panicked := false
	func() {
		defer func() {
			if recover() != nil {
				panicked = true
			}
		}()
		_ = doRequest(t, h, http.MethodPost, "/v1/chat/completions", `{"model":"test-model"}`, nil)
	}()

	if !panicked {
		t.Fatal("the transport did not panic, so the test proved nothing")
	}
	if got := budget.Used(); got != 0 {
		t.Fatalf("Used after a panicking request = %d, want 0 — the reservation must be released while the panic unwinds", got)
	}
}

// panicDoer is a transport whose one exchange panics — the shape of a relay
// fault that no error return can express.
type panicDoer struct{}

func (panicDoer) Do(*http.Request) (*http.Response, error) { panic("transport fault") }

// TestAdmissionBudgetHoldsUnderConcurrentRequests runs the whole request
// path — body read, walk, buffered answer — from many goroutines at once
// against a budget far smaller than their combined appetite, and asserts the
// process-wide invariant the budget exists for: the high-water mark never
// exceeds the ceiling, and every request that was refused gives back
// everything it took.
func TestAdmissionBudgetHoldsUnderConcurrentRequests(t *testing.T) {
	const limit = 1 << 20 // 1 MiB
	budget := withBufferBudget(t, limit)

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"model":"upstream-name","choices":[]}`)
	}))
	defer upstream.Close()

	h := newTestHandler(t, newTestStore(t, upstream.URL+"/v1"))
	body := `{"model":"test-model","pad":"` + strings.Repeat("x", 120<<10) + `"}`

	var (
		wg                sync.WaitGroup
		refused, admitted atomic.Int64
	)
	start := make(chan struct{})
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for range 8 {
				rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions", body, nil)
				switch rec.Code {
				case http.StatusOK:
					admitted.Add(1)
				case http.StatusServiceUnavailable:
					refused.Add(1)
				default:
					t.Errorf("unexpected status %d (%s)", rec.Code, rec.Body.String())
				}
				if used := budget.Used(); used > limit {
					t.Errorf("Used = %d, over the %d-byte limit", used, limit)
				}
			}
		}()
	}
	close(start)
	wg.Wait()

	if admitted.Load() == 0 {
		t.Fatal("no request was admitted at all")
	}
	if peak := budget.Peak(); peak > limit {
		t.Fatalf("peak admission = %d, want at most the %d-byte limit", peak, limit)
	}
	if got := budget.Used(); got != 0 {
		t.Fatalf("Used after every request = %d, want 0", got)
	}
	// Every refusal is a clean 503 and every admission a clean 200 — but the
	// point of the assertion above is the counter, not the split: the same
	// run on a machine with different scheduling may refuse a different
	// number of requests, and that is the budget working, not flaking.
	t.Logf("admitted=%d refused=%d", admitted.Load(), refused.Load())
}
