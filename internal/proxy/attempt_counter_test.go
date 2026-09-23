package proxy

import (
	"net/http"
	"slices"
	"testing"

	"github.com/rs/zerolog"

	"openai-compatible-injector/internal/transport"
)

// The walk carries two counters that are deliberately independent, and this
// file pins the boundary between them — the place a single counter used to
// blur them.
//
// provider_attempts counts LOGICAL provider-level attempts: each candidate
// attempt the walk began, whatever the transport then managed to do with it.
// upstream_exchanges counts REAL outbound dials, claimed by the budget where
// the dials happen. So provider_attempts is the number the recovery policy
// decides on, and upstream_exchanges is the number the wire carried; one can
// exceed the other in either direction.
//
// The regression these tests exist for: on the pooled path provider_attempts
// was incremented only AFTER the transport returned and only when it had
// dialed something (the direct path counted before its dial), so an attempt
// that dialed nothing reported the contradictory pair
// provider_attempt_started = N with provider_attempts = N-1. A logical
// attempt is an attempt whether or not it reached the wire.

// startedIndexes returns the provider_attempt index of every
// provider_attempt_started marker, in order.
func startedIndexes(t *testing.T, buf *logBuffer) []int {
	t.Helper()
	var got []int
	for _, ev := range buf.events(t, "provider_attempt_started") {
		n, ok := ev["provider_attempt"].(float64)
		if !ok {
			t.Fatalf("provider_attempt_started without a provider_attempt index: %v", ev)
		}
		got = append(got, int(n))
	}
	return got
}

// assertCounterAxes pins the two axes together with the markers, so a change
// that moves one without the other fails here.
func assertCounterAxes(t *testing.T, buf *logBuffer, wantStarted []int, wantExchanges int) {
	t.Helper()
	if got := startedIndexes(t, buf); !slices.Equal(got, wantStarted) {
		t.Errorf("provider_attempt_started indexes = %v, want %v", got, wantStarted)
	}
	done := buf.events(t, "request_completed")
	if len(done) != 1 {
		t.Fatalf("request_completed events = %d, want 1", len(done))
	}
	// The counter must never disagree with the last marker it emitted: that
	// is the invariant, and wantStarted[len-1] is the logical attempt count.
	wantAttempts := float64(len(wantStarted))
	if done[0]["provider_attempts"] != wantAttempts || done[0]["candidate_attempts"] != wantAttempts {
		t.Errorf("provider_attempts/candidate_attempts = %v/%v, want %v (one per logical attempt)",
			done[0]["provider_attempts"], done[0]["candidate_attempts"], wantAttempts)
	}
	if done[0]["upstream_exchanges"] != float64(wantExchanges) {
		t.Errorf("upstream_exchanges = %v, want %d (real dials, counted apart from attempts)",
			done[0]["upstream_exchanges"], wantExchanges)
	}
}

// A direct attempt is one logical attempt and one exchange.
func TestProviderAttemptCounterDirectAttempt(t *testing.T) {
	store := newPoolStore(t)
	d := &stubDoer{code: http.StatusOK, body: `{"id":"x","choices":[]}`}
	buf, log := captureLog(zerolog.DebugLevel)
	h := NewHandler(store, &singleDoerResolver{d: d}, nil, nil, log)

	rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions",
		`{"model":"direct-model","messages":[{"role":"user","content":"hi"}]}`, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body %s", rec.Code, rec.Body.String())
	}
	assertCounterAxes(t, buf, []int{1}, 1)
}

// A same-candidate retry is a second LOGICAL attempt, not a second exchange
// folded into the first: the marker, the attempt counter and the retry
// counter advance together, and each attempt claims its own exchange.
func TestProviderAttemptCounterSameCandidateRetry(t *testing.T) {
	stubRetryTiming(t)
	store := newChainStoreRetries(t, "", "    retries:\n      max-retries: 2\n")
	pa := newScript(
		scriptStep{status: http.StatusServiceUnavailable, body: `{}`},
		scriptStep{status: http.StatusServiceUnavailable, body: `{}`},
		scriptStep{status: http.StatusOK, body: `{"model":"up-a","choices":[]}`},
	)
	pb := newScript(scriptStep{status: http.StatusOK, body: `{"model":"up-b","choices":[]}`})
	buf, log := captureLog(zerolog.DebugLevel)
	h := NewHandler(store, kindResolver{direct: pa, proxied: pb}, nil, nil, log)

	rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions", chainChatBody, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body %s", rec.Code, rec.Body.String())
	}
	assertCounterAxes(t, buf, []int{1, 2, 3}, 3)
	done := buf.events(t, "request_completed")
	if done[0]["retries_total"] != float64(2) || done[0]["retry_attempts"] != float64(2) {
		t.Errorf("retry counters = %v/%v, want 2/2", done[0]["retries_total"], done[0]["retry_attempts"])
	}
}

// A pooled attempt that dials once counts one attempt and one exchange — the
// transport's dial count and the walk's attempt count agree here, which is
// why they must still be counted separately.
func TestProviderAttemptCounterPooledAttempt(t *testing.T) {
	store := newPoolStore(t)
	ex := &stubExecutor{info: transport.AttemptInfo{Attempts: 1, Kind: "direct", Target: "direct"}}
	buf, log := captureLog(zerolog.DebugLevel)
	h := NewHandler(store, &singleDoerResolver{d: ex}, nil, nil, log)

	rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions", poolChatBody, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body %s", rec.Code, rec.Body.String())
	}
	assertCounterAxes(t, buf, []int{1}, 1)
}

// The zero-dial case is the regression. A pool that reaches no member at all
// — every one gated out — still performed a provider attempt, and the walk
// decided on it. provider_attempts
// must therefore read 1 while upstream_exchanges reads 0, and the started
// marker must carry the same index the completion record counts.
func TestProviderAttemptCounterPooledZeroDial(t *testing.T) {
	store := newPoolStore(t)
	// No member eligible: the executor dials nothing and claims no exchange,
	// reporting the exhaustion sentinel exactly as the real pool does.
	ex := &allIneligibleExecutor{}
	buf, log := captureLog(zerolog.DebugLevel)
	h := NewHandler(store, &singleDoerResolver{d: ex}, nil, nil, log)

	rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions", poolChatBody, nil)
	if rec.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502 (body %s)", rec.Code, rec.Body.String())
	}
	// A transport failure is never re-asked on the same candidate: one
	// attempt means exactly one execution.
	if ex.calls != 1 {
		t.Errorf("pool calls = %d, want 1: a transport failure is not a same-candidate retry", ex.calls)
	}
	assertCounterAxes(t, buf, []int{1}, 0)
	done := buf.events(t, "request_completed")
	if done[0]["candidates_entered"] != float64(1) || done[0]["provider_exhausted"] != true {
		t.Errorf("completion = %v, want one candidate entered and exhausted", done[0])
	}
	// The attempt is a logical fact, so the per-attempt evidence that exists
	// for it must name it.
	if failed := buf.events(t, "upstream_request_failed"); len(failed) != 1 {
		t.Errorf("upstream_request_failed events = %d, want 1", len(failed))
	} else if failed[0]["provider_attempt"] != float64(1) {
		t.Errorf("exhaustion provider_attempt = %v, want 1", failed[0]["provider_attempt"])
	}
	// The egress report stays dial-shaped in both directions: the counters
	// name the zero dials truthfully rather than being omitted or faked, and
	// the exhaustion is reported as the pool's own condition, not as an
	// endpoint verdict.
	if done[0]["egress_attempts"] != float64(0) || done[0]["egress_exhausted"] != true {
		t.Errorf("egress report = %v/%v, want 0 dials and exhaustion",
			done[0]["egress_attempts"], done[0]["egress_exhausted"])
	}
	// No endpoint was dialed, so the report names none: an egress kind or
	// target here would be an identity invented for a dial that never
	// happened.
	if done[0]["egress_kind"] != "" || done[0]["egress_target"] != "" {
		t.Errorf("egress identity = %v/%v, want empty where nothing was dialed",
			done[0]["egress_kind"], done[0]["egress_target"])
	}
	if done[0]["outcome"] != "upstream_unreachable" {
		t.Errorf("outcome = %v, want the zero-dial 502", done[0]["outcome"])
	}
	if n := len(buf.events(t, "egress_attempt_failed")); n != 0 {
		t.Errorf("egress_attempt_failed events = %d, want 0: no endpoint was dialed", n)
	}
}

// allIneligibleExecutor models the real pool's all-members-ineligible
// answer: nothing dialed, no exchange claimed, the exhaustion sentinel
// raised, and no endpoint blamed for it. Kind and Target stay EMPTY because
// no endpoint was dialed — transporting.AttemptInfo documents them as the
// last dialed endpoint's, empty when none was (pool.go's zero-dial return
// sets only Exhausted), so a fixture that filled them would let a handler
// regression that fabricated an egress identity for a zero-dial attempt pass
// this test.
type allIneligibleExecutor struct{ calls int }

func (e *allIneligibleExecutor) Execute(*transport.AttemptRequest) (*http.Response, transport.AttemptInfo, error) {
	e.calls++
	return nil, transport.AttemptInfo{Exhausted: true}, transport.ErrExhausted
}

func (e *allIneligibleExecutor) Do(*http.Request) (*http.Response, error) {
	panic("handler called Do on an Executor-capable doer")
}

// The other half of the same boundary, from the transport's side: the
// envelope refuses an attempt's FIRST dial, so the attempt is begun,
// announced and counted with no exchange behind it.
//
// What this pins is the handler's contract with the transport, and the
// comment says so rather than pretending otherwise: the engine examines both
// envelopes when it DECIDES, so it never authorizes a dial it cannot fund —
// a retryable failure whose candidate envelope is spent goes straight to the
// retry policy's on-exhausted action. This shape is the backstop beneath
// that (the elapsed half of an envelope expiring between the decision and
// the dial, or a transport that refuses for its own reasons), and the
// transport reports it exactly as the real pool does.
func TestProviderAttemptCounterEnvelopeRefusalAtDial(t *testing.T) {
	store := newPoolStore(t)
	ex := &refusingExecutor{}
	buf, log := captureLog(zerolog.DebugLevel)
	h := NewHandler(store, &singleDoerResolver{d: ex}, nil, nil, log)

	rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions", poolChatBody, nil)
	if rec.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502 (body %s)", rec.Code, rec.Body.String())
	}
	if ex.calls != 1 {
		t.Fatalf("pool calls = %d, want 1", ex.calls)
	}
	// The attempt exists as a logical fact; no exchange exists at all.
	assertCounterAxes(t, buf, []int{1}, 0)
	// The refusal is reported once, under the attempt it refused, and it is
	// an envelope event rather than an endpoint verdict: no member was
	// blamed and no egress attempt was recorded.
	spent := buf.events(t, "candidate_exchange_budget_spent")
	if len(spent) != 1 || spent[0]["provider_attempt"] != float64(1) ||
		spent[0]["failure_origin"] != "envelope" || spent[0]["policy_rule_id"] != "budget-candidate" {
		t.Errorf("refusal = %v, want one envelope record naming provider_attempt 1", spent)
	}
	if n := len(buf.events(t, "egress_attempt_failed")); n != 0 {
		t.Errorf("egress_attempt_failed events = %d, want 0: no endpoint was dialed", n)
	}
	done := buf.events(t, "request_completed")
	if done[0]["egress_attempts"] != float64(0) || done[0]["provider_exhausted"] != true {
		t.Errorf("completion = %v, want zero dials and an exhausted walk", done[0])
	}
}

// refusingExecutor reports exactly what the real pool reports when an
// envelope refuses an attempt's first dial: BudgetExhausted with no
// attempts, no failures, no blame and no error.
type refusingExecutor struct{ calls int }

func (e *refusingExecutor) Execute(*transport.AttemptRequest) (*http.Response, transport.AttemptInfo, error) {
	e.calls++
	return nil, transport.AttemptInfo{BudgetExhausted: true}, nil
}

func (e *refusingExecutor) Do(*http.Request) (*http.Response, error) {
	panic("handler called Do on an Executor-capable doer")
}
