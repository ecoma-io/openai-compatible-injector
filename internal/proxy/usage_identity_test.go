package proxy

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

// Two joins that the request-identity and metering invariants each assert
// half of, and that nothing asserted together.
//
// Both read the same local `requestID` today, which is exactly why neither
// needs a test — until something routes one of them through a different
// source, at which point they diverge silently. The response header is what
// the operator correlates a bill with, and the usage row is what they are
// charged from; a disagreement between the two is a revenue bug that reports
// itself as nothing.

// TestUsageRequestIDIsTheOneTheClientWasGiven pins the join. Every other
// surface's id is covered somewhere; this asserts the metered row is the SAME
// id the response carried, not merely a non-empty one.
func TestUsageRequestIDIsTheOneTheClientWasGiven(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"1","model":"upstream-name","usage":{"prompt_tokens":1,"completion_tokens":2,"total_tokens":3}}`))
	}))
	defer up.Close()

	meter := &recordingMeter{}
	h := usageHandler(t, newTestStore(t, up.URL+"/v1"), meter)
	rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions",
		`{"model":"test-model","messages":[{"role":"user","content":"hi"}]}`, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}

	stamped := rec.Header().Get("X-Request-Id")
	if stamped == "" {
		t.Fatal("the response carried no request id")
	}
	// The client's own id is consumed, never echoed: a client that sends one
	// must still get the proxy's, and the metered row must match THAT.
	ev := meter.single(t)
	if ev.RequestID != stamped {
		t.Fatalf("usage row request id = %q, response header = %q; these are what an operator joins a bill on", ev.RequestID, stamped)
	}
}

// A client-supplied id must not become the metered identity. Attacker-
// controlled cardinality in the id column is both a log-injection surface and
// a cardinality attack on the table's index, and the proxy owns the id.
func TestUsageRequestIDIgnoresTheClientSuppliedOne(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"1","model":"upstream-name","usage":{"prompt_tokens":1,"completion_tokens":2,"total_tokens":3}}`))
	}))
	defer up.Close()

	meter := &recordingMeter{}
	h := usageHandler(t, newTestStore(t, up.URL+"/v1"), meter)
	rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions",
		`{"model":"test-model","messages":[{"role":"user","content":"hi"}]}`,
		map[string]string{"X-Request-Id": "client-chosen-id"})
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}

	stamped := rec.Header().Get("X-Request-Id")
	if stamped == "client-chosen-id" {
		t.Fatal("the proxy echoed the client's request id")
	}
	if ev := meter.single(t); ev.RequestID != stamped {
		t.Fatalf("usage row request id = %q, want the proxy's own %q", ev.RequestID, stamped)
	}
}

// A client that disconnects is still a request this proxy spent upstream
// money on, so it is still metered — exactly once, and with no fabricated
// tokens. The disconnect is real usage; dropping the row would be a silent
// revenue loss, and the tokens must stay absent rather than become zeros,
// because absent usage is "unstated" and zero usage is a claim.
//
// Note what the recorder does and does not show. A ResponseRecorder accepts
// the header write whether or not the context is done, so rec.Code reads 200
// even though the caller is gone; the real client received nothing, which the
// empty body is the observable proof of. The metering is what this pins: the
// row exists, the status is 0 rather than 200 because no answer was delivered,
// and the upstream's reported usage is NOT read off a body this proxy never
// managed to forward.
func TestUsageDisconnectIsMeteredExactlyOnceWithNoTokens(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"1","model":"upstream-name","usage":{"prompt_tokens":7,"completion_tokens":8,"total_tokens":15}}`))
	}))
	defer up.Close()

	meter := &recordingMeter{}
	h := usageHandler(t, newTestStore(t, up.URL+"/v1"), meter)
	rec := doDisconnectedRequest(t, h, http.MethodPost, "/v1/chat/completions",
		`{"model":"test-model","messages":[{"role":"user","content":"hi"}]}`, nil)
	if rec.Body.Len() != 0 {
		t.Fatalf("body = %q, want nothing delivered to a caller that is gone", rec.Body.String())
	}

	// The whole point: the row exists. A one-line refactor that moved
	// complete() past a return would make this fail, which is the event
	// this test exists to make loud.
	ev := meter.single(t)
	if ev.Outcome != "client_disconnected" {
		t.Fatalf("outcome = %q, want client_disconnected", ev.Outcome)
	}
	if ev.PublicModel != "test-model" {
		t.Fatalf("public model = %q, want test-model (a metered row must name what was asked for)", ev.PublicModel)
	}
	// No answer reached the client, so the row records no status. A 200 here
	// would claim this proxy delivered something it did not.
	if ev.HTTPStatus != 0 {
		t.Fatalf("metered status = %d, want 0 (no answer reached the caller)", ev.HTTPStatus)
	}
	// The usage the upstream reported belongs to a body this proxy never
	// managed to forward. The columns stay NULL: absent usage is "unstated",
	// and a fabricated 7/8/15 would be a claim about a read that never
	// completed on the path the client was served.
	if ev.PromptTokens != nil || ev.CompletionTokens != nil || ev.TotalTokens != nil {
		t.Fatalf("a disconnect fabricated usage tokens: %v/%v/%v", ev.PromptTokens, ev.CompletionTokens, ev.TotalTokens)
	}
}

// A disconnect after the walk already spent a real exchange must still be one
// row, and it must report the attempt honestly. provider_attempts is a fact
// about what this proxy did, and it does not become zero because the client
// hung up — a dropped row or a zeroed counter is a revenue bug, since the
// upstream billed for the dial either way.
//
// The upstream is a real httptest server, not a scripted Doer. A fake that
// answers without ever touching the request context cannot produce a
// mid-flight cancellation at all: the whole exchange completes and the
// request reports `completed`, which would make this test assert the fake's
// behaviour while claiming to assert the system's.
func TestUsageDisconnectAfterAnExchangeReportsTheAttemptHonestly(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"1","model":"upstream-name","choices":[]}`))
	}))
	defer up.Close()

	meter := &recordingMeter{}
	h := usageHandler(t, newTestStore(t, up.URL+"/v1"), meter)
	rec := doDisconnectedRequest(t, h, http.MethodPost, "/v1/chat/completions",
		`{"model":"test-model","messages":[{"role":"user","content":"hi"}]}`, nil)
	if rec.Body.Len() != 0 {
		t.Fatalf("body = %q, want nothing delivered to a caller that is gone", rec.Body.String())
	}
	ev := meter.single(t)
	if ev.Outcome != "client_disconnected" {
		t.Fatalf("outcome = %q, want client_disconnected", ev.Outcome)
	}
	// The candidate was dialed, and a dial this proxy made is a fact that
	// outlives the caller.
	if ev.ProviderAttempts < 1 {
		t.Fatalf("metered provider attempts = %d, want at least 1 (a dial happened)", ev.ProviderAttempts)
	}
	if ev.HTTPStatus == http.StatusOK {
		t.Fatal("a disconnect metered as a 200 completion")
	}
}
