package e2e_test

import (
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
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
