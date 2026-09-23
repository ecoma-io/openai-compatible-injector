package e2e_test

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"
)

// The provider fallback contract, black-box: a chain model's candidate list
// is walked primary-first under the provider-fallback policy, moving to the
// next candidate only when one fails BEFORE answering (transport-level
// failure); any HTTP status is the answer, relayed as a single-provider
// deployment would relay it. These scenarios drive the real binary with two
// providers — one unreachable (a closed port: connection refused is a
// transport failure, not a status) and one live — over real HTTP.

// chainYAML renders a runtime file whose chain-model lists the given
// provider candidates in order (base URLs), plus an optional
// provider-fallback block.
func chainYAML(t *testing.T, candidates []string, fallbackBlock string) string {
	t.Helper()
	var sb strings.Builder
	sb.WriteString("api-key: " + e2eAPIKey + "\nproviders:\n")
	for i, base := range candidates {
		fmt.Fprintf(&sb, "  chain-p%d:\n    base-url: %s/v1\n", i+1, base)
	}
	sb.WriteString("models:\n  chain-model:\n    providers:\n")
	for i := range candidates {
		fmt.Fprintf(&sb, "      - provider: chain-p%d\n        upstream-model: chain-up-%d\n", i+1, i+1)
	}
	sb.WriteString(fallbackBlock)
	return sb.String()
}

// TestE2EProviderChainFallsBackToSecondProvider pins the happy fallback:
// the primary's dial fails (closed port), the second candidate serves, the
// client sees the second candidate's answer under the public model name,
// and the completion event reports the walk.
func TestE2EProviderChainFallsBackToSecondProvider(t *testing.T) {
	up := newFakeUpstream(t)
	up.setHandler(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"id":"c2","object":"chat.completion","model":"chain-up-2","choices":[{"index":0,"message":{"role":"assistant","content":"from-second"}}]}`)
	})

	body := chainYAML(t, []string{"http://127.0.0.1:1", up.url()}, "")
	p := startSubprocess(t, startOpts{yaml: body, logLevel: "info"})

	code, _, respBody := postJSON(t, p.addr, "/v1/chat/completions",
		poolChatBody("chain-model", "hi"), nil)
	if code != http.StatusOK {
		t.Fatalf("status = %d, body %s — the dead primary must fall back to the second candidate", code, respBody)
	}
	if !strings.Contains(string(respBody), `"model":"chain-model"`) {
		t.Errorf("response model not rewritten to the public name: %s", respBody)
	}
	req, ok := up.last()
	if !ok {
		t.Fatal("the second candidate was never reached")
	}
	if !strings.Contains(string(req.Body), `"model":"chain-up-2"`) {
		t.Errorf("second candidate got upstream model %q in body %s", "chain-up-2", req.Body)
	}
	if up.count() != 1 {
		t.Errorf("upstream hits = %d, want exactly 1", up.count())
	}

	ev := waitForEventCount(t, p, "request_completed", 1)[0]
	if ev["outcome"] != "completed" {
		t.Errorf("outcome = %v, want completed", ev["outcome"])
	}
	if ev["provider_attempts"] != float64(2) {
		t.Errorf("provider_attempts = %v, want 2", ev["provider_attempts"])
	}
	if ev["final_provider"] != "chain-p2" {
		t.Errorf("final_provider = %v, want chain-p2", ev["final_provider"])
	}
	if _, exhausted := ev["provider_exhausted"]; exhausted {
		t.Errorf("provider_exhausted set on a successful fallback")
	}
	failed := waitForEventCount(t, p, "provider_attempt_failed", 1)[0]
	if failed["provider"] != "chain-p1" || failed["provider_attempt"] != float64(1) {
		t.Errorf("provider_attempt_failed = %v/%v, want chain-p1/1", failed["provider"], failed["provider_attempt"])
	}
}

// TestE2EProviderChainStatusIsTerminal pins the boundary over the real
// wire: a 429 from the primary is THE answer — status preserved, canonical
// envelope, and the second candidate is never dialed.
func TestE2EProviderChainStatusIsTerminal(t *testing.T) {
	rateLimited := newFakeUpstream(t)
	rateLimited.setHandler(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Retry-After", "7")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = io.WriteString(w, `{"error":{"message":"slow down"}}`)
	})
	second := newFakeUpstream(t)
	second.setHandler(func(w http.ResponseWriter, _ *http.Request) {
		t.Error("the second candidate was dialed after the primary answered 429")
	})

	body := chainYAML(t, []string{rateLimited.url(), second.url()}, "")
	p := startSubprocess(t, startOpts{yaml: body, logLevel: "info"})

	code, hdrs, respBody := postJSON(t, p.addr, "/v1/chat/completions",
		poolChatBody("chain-model", "hi"), nil)
	if code != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429", code)
	}
	if got := hdrs.Get("Retry-After"); got != "7" {
		t.Errorf("Retry-After = %q, want the relayed 7", got)
	}
	if !strings.Contains(string(respBody), `"code":"upstream_http_429"`) {
		t.Errorf("body = %s, want the canonical upstream_http_429 envelope", respBody)
	}
	if strings.Contains(string(respBody), "slow down") {
		t.Errorf("provider error body relayed raw: %s", respBody)
	}

	ev := waitForEventCount(t, p, "request_completed", 1)[0]
	if ev["provider_attempts"] != float64(1) || ev["final_provider"] != "chain-p1" {
		t.Errorf("provider fields = %v/%v, want one attempt on chain-p1",
			ev["provider_attempts"], ev["final_provider"])
	}
}

// TestE2EProviderChainExhaustionIsLoud pins the walk's dead end over the
// real wire: both candidates unreachable, canonical 502, and the completion
// event carries provider_exhausted.
func TestE2EProviderChainExhaustionIsLoud(t *testing.T) {
	body := chainYAML(t, []string{"http://127.0.0.1:1", "http://127.0.0.1:2"},
		"provider-fallback:\n  enabled: true\n  max-attempts: 2\n")
	p := startSubprocess(t, startOpts{yaml: body, logLevel: "info"})

	code, _, respBody := postJSON(t, p.addr, "/v1/chat/completions",
		poolChatBody("chain-model", "hi"), nil)
	if code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502", code)
	}
	if got := strings.TrimSpace(string(respBody)); got != envelopeUpstreamUnreachable {
		t.Errorf("body = %q, want the canonical upstream_unreachable envelope", got)
	}

	ev := waitForEventCount(t, p, "request_completed", 1)[0]
	if ev["outcome"] != "upstream_unreachable" {
		t.Errorf("outcome = %v, want upstream_unreachable", ev["outcome"])
	}
	if ev["provider_attempts"] != float64(2) || ev["final_provider"] != "chain-p2" {
		t.Errorf("provider fields = %v/%v, want 2/chain-p2", ev["provider_attempts"], ev["final_provider"])
	}
	if ev["provider_exhausted"] != true {
		t.Errorf("provider_exhausted = %v, want true", ev["provider_exhausted"])
	}
}

// holdingEgress accepts every dial and never answers — a request routed
// through it hangs in-flight, so a caller-side deadline fires while the
// attempt is live (a closed port would refuse before any deadline could).
type holdingEgress struct {
	ln    net.Listener
	mu    sync.Mutex
	dials int
}

func newHoldingEgress(t *testing.T) *holdingEgress {
	t.Helper()
	h := &holdingEgress{}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	h.ln = ln
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			_, err := ln.Accept()
			if err != nil {
				return
			}
			h.mu.Lock()
			h.dials++
			h.mu.Unlock()
			// Held, never answered — closed only when the listener dies.
		}
	}()
	return h
}

func (h *holdingEgress) addr() string { return h.ln.Addr().String() }

func (h *holdingEgress) count() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.dials
}

// chainPoolYAML renders the real-binary shape behind the attempt matrix: a
// two-candidate chain whose primary rides a pool of two dead proxy members
// (distinct refusing endpoints — the config rejects duplicates) and whose
// second candidate is the healthy provider on a direct transport.
func chainPoolYAML(up string) string {
	return fmt.Sprintf(`api-key: %s
transports:
  chain-pool:
    type: pool
    members: [dead-a, dead-b]
  dead-a:
    type: proxy
    proxy: http://127.0.0.1:1
  dead-b:
    type: proxy
    proxy: http://127.0.0.1:2
  pb-transport:
    type: direct
providers:
  chain-p1:
    base-url: %s/v1
    transport: chain-pool
  chain-p2:
    base-url: %s/v1
    transport: pb-transport
models:
  chain-model:
    providers:
      - provider: chain-p1
        upstream-model: chain-up-1
      - provider: chain-p2
        upstream-model: chain-up-2
`, e2eAPIKey, up, up)
}

// chainDirectPoolYAML renders two single-endpoint models over the same dead
// upstream: one on a direct transport, one on a pool whose only member is a
// direct ref to it — the same dial failure through both egress shapes.
func chainDirectPoolYAML() string {
	return fmt.Sprintf(`api-key: %s
transports:
  t-direct:
    type: direct
  t-pool:
    type: pool
    members: [m-dead]
  m-dead:
    type: direct
providers:
  direct-provider:
    base-url: http://127.0.0.1:1/v1
    transport: t-direct
  pooled-provider:
    base-url: http://127.0.0.1:1/v1
    transport: t-pool
models:
  direct-model:
    provider: direct-provider
    upstream-model: up
  pooled-model:
    provider: pooled-provider
    upstream-model: up
`, e2eAPIKey)
}

// TestE2ECallerDeadlinePreventsProviderFallback pins the deadline half of
// the ownership rule black-box: a client whose deadline fires while the
// primary is mid-attempt is done — no envelope, the completion records the
// client's own canceled class with the caller_deadline_exceeded cause, one
// provider attempt, and the second candidate is never dialed. A caller
// deadline must never read as a provider-local timeout, which would burn
// the fallback budget for a client that is gone.
func TestE2ECallerDeadlinePreventsProviderFallback(t *testing.T) {
	held := newHoldingEgress(t)
	up := newFakeUpstream(t)
	up.setHandler(egressChatOK)

	body := chainYAML(t, []string{"http://" + held.addr(), up.url()}, "")
	p := startSubprocess(t, startOpts{yaml: body, logLevel: "info"})

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost,
			"http://"+p.addr+"/v1/chat/completions",
			strings.NewReader(poolChatBody("chain-model", "hi")))
		if err != nil {
			done <- err
			return
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+e2eAPIKey)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			done <- err
			return
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		done <- nil
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("the expired-deadline request returned a response")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the deadline request never returned to the client")
	}

	ev := waitForEventCount(t, p, "request_completed", 1)[0]
	if ev["outcome"] != "client_disconnected" {
		t.Errorf("outcome = %v, want client_disconnected", ev["outcome"])
	}
	if ev["provider_attempts"] != float64(1) {
		t.Errorf("provider_attempts = %v, want 1 (no second candidate)", ev["provider_attempts"])
	}
	if _, exhausted := ev["provider_exhausted"]; exhausted {
		t.Errorf("provider_exhausted set on a caller deadline")
	}
	if held.count() != 1 {
		t.Errorf("primary dials = %d, want exactly 1", held.count())
	}
	failed := eventsWithMessage(parseLogEvents(t, p.stderr.String()), "upstream_request_failed")
	if len(failed) != 1 {
		t.Fatalf("upstream_request_failed events = %d, want 1", len(failed))
	}
	if failed[0]["error_class"] != "canceled" {
		t.Errorf("error_class = %v, want canceled", failed[0]["error_class"])
	}
	// Over the wire a client deadline has no separate signal: the client
	// abandoning the connection IS the event, so the cause is
	// caller_canceled. Distinguishing a deadline from a cancel needs the
	// server-side request context and is pinned in the unit suite.
	if failed[0]["error_cause"] != "caller_canceled" {
		t.Errorf("error_cause = %v, want caller_canceled", failed[0]["error_cause"])
	}
	// The quiet direction, observed after the walk provably ended.
	if up.count() != 0 {
		t.Errorf("fallback candidate dialed %d times after the caller's deadline", up.count())
	}
}

// TestE2EDirectAndPoolDialFailureEvidenceMatch pins evidence parity: the
// same dial failure reaching the handler through a direct transport and
// through a one-member pool produces the identical canonical record — same
// class, same cause, same egress kind and target — so log consumers need
// no per-egress vocabulary. A refusal is endpoint-owned and fallback-eligible
// in both shapes.
func TestE2EDirectAndPoolDialFailureEvidenceMatch(t *testing.T) {
	p := startSubprocess(t, startOpts{yaml: chainDirectPoolYAML(), logLevel: "info"})

	for _, model := range []string{"direct-model", "pooled-model"} {
		code, _, respBody := postJSON(t, p.addr, "/v1/chat/completions",
			poolChatBody(model, "hi"), nil)
		if code != http.StatusBadGateway {
			t.Fatalf("%s: status = %d, body %s — want 502", model, code, respBody)
		}
		if got := strings.TrimSpace(string(respBody)); got != envelopeUpstreamUnreachable {
			t.Errorf("%s: body = %q, want the canonical upstream_unreachable envelope", model, got)
		}
	}
	waitForEventCount(t, p, "request_completed", 2)

	// The exhaustion record reads the same for both shapes: the layer-owned
	// provider_exhausted class over the final refusal cause. (Both chains
	// are single-candidate, so the walk exhausts.)
	failed := eventsWithMessage(parseLogEvents(t, p.stderr.String()), "upstream_request_failed")
	if len(failed) != 2 {
		t.Fatalf("upstream_request_failed events = %d, want 2", len(failed))
	}
	for i, ev := range failed {
		if ev["error_class"] != "provider_exhausted" || ev["error_cause"] != "connection_refused" {
			t.Errorf("request %d class/cause = %v/%v, want provider_exhausted/connection_refused",
				i, ev["error_class"], ev["error_cause"])
		}
	}
	// Parity lives on the per-dial record: one egress_attempt_failed per
	// shape, byte-equal — same class, cause, kind, target, attempt index —
	// so log consumers need no per-egress vocabulary.
	eg := eventsWithMessage(parseLogEvents(t, p.stderr.String()), "egress_attempt_failed")
	if len(eg) != 2 {
		t.Fatalf("egress_attempt_failed events = %d, want 2 (one per shape)", len(eg))
	}
	for k, want := range map[string]any{
		"error_class":    "connection",
		"error_cause":    "connection_refused",
		"egress_kind":    "direct",
		"egress_target":  "direct",
		"egress_attempt": float64(1),
	} {
		if eg[0][k] != want {
			t.Errorf("direct dial: %s = %v, want %v", k, eg[0][k], want)
		}
		if eg[1][k] != want {
			t.Errorf("pooled dial: %s = %v, want the direct record's %v", k, eg[1][k], want)
		}
	}
}

// TestE2EChainPoolAttemptIdentities pins the nested attempt identity
// black-box: one provider candidate whose egress is a two-member pool
// produces egress attempts 1 and 2 under provider attempt 1 (the `attempt`
// field riding along as the compatibility alias), the candidate's failure
// record carries both indexes, and the walk still moves to the healthy
// second candidate.
func TestE2EChainPoolAttemptIdentities(t *testing.T) {
	up := newFakeUpstream(t)
	up.setHandler(egressChatOK)
	p := startSubprocess(t, startOpts{yaml: chainPoolYAML(up.url()), logLevel: "info"})

	code, _, respBody := postJSON(t, p.addr, "/v1/chat/completions",
		poolChatBody("chain-model", "hi"), nil)
	if code != http.StatusOK {
		t.Fatalf("status = %d, body %s — the dead pool must fall back to candidate 2", code, respBody)
	}
	if !strings.Contains(string(respBody), `"model":"chain-model"`) {
		t.Errorf("response model not rewritten to the public name: %s", respBody)
	}

	ev := waitForEventCount(t, p, "request_completed", 1)[0]
	if ev["outcome"] != "completed" {
		t.Errorf("outcome = %v, want completed", ev["outcome"])
	}
	if ev["provider_attempts"] != float64(2) || ev["final_provider"] != "chain-p2" {
		t.Errorf("provider fields = %v/%v, want 2/chain-p2", ev["provider_attempts"], ev["final_provider"])
	}

	events := parseLogEvents(t, p.stderr.String())
	eg := eventsWithMessage(events, "egress_attempt_failed")
	if len(eg) != 2 {
		t.Fatalf("egress_attempt_failed events = %d, want 2", len(eg))
	}
	wantTargets := []string{"http://127.0.0.1:1", "http://127.0.0.1:2"}
	for i, e := range eg {
		if e["provider_attempt"] != float64(1) || e["egress_attempt"] != float64(i+1) {
			t.Errorf("event %d attempts = %v/%v, want 1/%d", i, e["provider_attempt"], e["egress_attempt"], i+1)
		}
		if e["attempt"] != e["egress_attempt"] {
			t.Errorf("event %d: attempt alias %v != egress_attempt %v", i, e["attempt"], e["egress_attempt"])
		}
		if e["error_class"] != "connection" || e["error_cause"] != "connection_refused" {
			t.Errorf("event %d class/cause = %v/%v, want connection/connection_refused", i, e["error_class"], e["error_cause"])
		}
		if e["egress_kind"] != "http" || e["egress_target"] != wantTargets[i] {
			t.Errorf("event %d kind/target = %v/%v, want http/%s", i, e["egress_kind"], e["egress_target"], wantTargets[i])
		}
	}
	failed := eventsWithMessage(events, "provider_attempt_failed")
	if len(failed) != 1 {
		t.Fatalf("provider_attempt_failed events = %d, want 1", len(failed))
	}
	if failed[0]["provider"] != "chain-p1" || failed[0]["provider_attempt"] != float64(1) || failed[0]["egress_attempt"] != float64(2) {
		t.Errorf("provider_attempt_failed = %v/%v/%v, want chain-p1/1/2",
			failed[0]["provider"], failed[0]["provider_attempt"], failed[0]["egress_attempt"])
	}
	if failed[0]["error_class"] != "connection" || failed[0]["error_cause"] != "connection_refused" {
		t.Errorf("provider_attempt_failed class/cause = %v/%v, want connection/connection_refused",
			failed[0]["error_class"], failed[0]["error_cause"])
	}
}
