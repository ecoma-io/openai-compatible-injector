package e2e_test

import (
	"bufio"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"
)

// The provider recovery policy, black-box: what a failure MEANS is a matrix
// resolved from the runtime file at load, frozen onto the request's snapshot,
// and executed by the candidate walk. These scenarios drive the real binary
// over real HTTP and assert only externally observable behaviour — the
// relayed status, which upstreams were dialed, and the evidence the process
// logs about its own decisions. The legacy retries/provider-fallback
// scenarios in provider_fallback_test.go stay as they are: they pin the
// behaviour the default policy must reproduce, not the policy engine itself.

// recoveryFile renders a runtime config from ordered pre-rendered sections —
// each a top-level YAML key plus its indented body — and the client api-key.
// Tests keep their YAML literals intact inside the section strings so the
// policy under test is readable off the page.
func recoveryFile(sections ...string) string {
	var sb strings.Builder
	sb.WriteString("api-key: " + e2eAPIKey + "\n")
	for _, s := range sections {
		sb.WriteString(s)
	}
	return sb.String()
}

// upstreamErrorWriter answers every request with one status and an
// OpenAI-shaped error object carrying a token type/code, which is what the
// matrix's provider-error predicate matches on.
func upstreamErrorWriter(status int, errType, errCode string) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = fmt.Fprintf(w, `{"error":{"message":"rejected","type":%q,"code":%q}}`, errType, errCode)
	}
}

// markerJSONHandler answers 200 with a fixed id so a test can tell which
// upstream produced a response.
func markerJSONHandler(id, upstreamModel string) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = fmt.Fprintf(w, `{"id":%q,"model":%q,"choices":[]}`, id, upstreamModel)
	}
}

// completionsFor returns every request_completed event bound to one public
// model, in emission order.
func completionsFor(t *testing.T, p *proc, model string) []logEvent {
	t.Helper()
	var out []logEvent
	for _, ev := range eventsWithMessage(parseLogEvents(t, p.stderr.String()), "request_completed") {
		if ev["public_model"] == model {
			out = append(out, ev)
		}
	}
	return out
}

// dialFailuresFor counts the per-dial failure records one model produced —
// one per endpoint actually dialed and failed.
func dialFailuresFor(t *testing.T, p *proc, model string) int {
	t.Helper()
	n := 0
	for _, ev := range eventsWithMessage(parseLogEvents(t, p.stderr.String()), "egress_attempt_failed") {
		if ev["public_model"] == model {
			n++
		}
	}
	return n
}

// httpErrorEvidenceFor returns the upstream_http_error evidence records bound
// to one model.
func httpErrorEvidenceFor(t *testing.T, p *proc, model string) []logEvent {
	t.Helper()
	var out []logEvent
	for _, ev := range eventsWithMessage(parseLogEvents(t, p.stderr.String()), "upstream_http_error") {
		if ev["public_model"] == model {
			out = append(out, ev)
		}
	}
	return out
}

// TestE2ERecoveryProviderErrorPredicateDecides429 pins the provider-error
// predicate on the real wire. Two models receive the same 429; only the one
// whose upstream error object carries the configured `code` token is
// re-routed. The rule states the canonical `http-429` identity, so it
// REPLACES the built-in exact-status row — the only way a provider-error
// predicate can outrank an exact status, since precedence is exact status
// first by design. The other model's 429 matches no exact row and falls to
// the 4xx bucket, which is terminal.
func TestE2ERecoveryProviderErrorPredicateDecides429(t *testing.T) {
	quota := newFakeUpstream(t)
	quota.setHandler(upstreamErrorWriter(http.StatusTooManyRequests, "insufficient_quota", "insufficient_quota"))
	throttle := newFakeUpstream(t)
	throttle.setHandler(upstreamErrorWriter(http.StatusTooManyRequests, "rate_limit_error", "rate_limit_exceeded"))
	okQuota := newFakeUpstream(t)
	okQuota.setHandler(jsonChatHandler("pe-up-2"))
	okThrottle := newFakeUpstream(t)
	okThrottle.setHandler(jsonChatHandler("pe-up-2"))

	top := `recovery:
  matrix:
    rules:
      - id: http-429
        when:
          status: 429
          provider-error:
            code: insufficient_quota
        action: fallback
`
	providers := fmt.Sprintf(`providers:
  pe-quota-p1:
    base-url: %s/v1
  pe-quota-p2:
    base-url: %s/v1
  pe-throttle-p1:
    base-url: %s/v1
  pe-throttle-p2:
    base-url: %s/v1
`, quota.url(), okQuota.url(), throttle.url(), okThrottle.url())
	models := `models:
  pe-quota:
    providers:
      - provider: pe-quota-p1
        upstream-model: pe-up-1
      - provider: pe-quota-p2
        upstream-model: pe-up-2
  pe-throttle:
    providers:
      - provider: pe-throttle-p1
        upstream-model: pe-up-1
      - provider: pe-throttle-p2
        upstream-model: pe-up-2
`
	p := startSubprocess(t, startOpts{yaml: recoveryFile(top, providers, models), logLevel: "info"})

	// The quota 429 matches the provider-error predicate: fall back at once.
	code, _, respBody := postJSON(t, p.addr, "/v1/chat/completions",
		poolChatBody("pe-quota", "hi"), nil)
	if code != http.StatusOK {
		t.Fatalf("pe-quota: status = %d, body %s — a 429 carrying the configured code must fall back", code, respBody)
	}
	if !strings.Contains(string(respBody), `"model":"pe-quota"`) {
		t.Errorf("pe-quota: response model not rewritten to the public name: %s", respBody)
	}
	if quota.count() != 1 {
		t.Errorf("pe-quota: primary hits = %d, want 1 (fallback, never a retry)", quota.count())
	}
	if okQuota.count() != 1 {
		t.Errorf("pe-quota: second candidate hits = %d, want 1", okQuota.count())
	}

	// The throttled 429 carries a different code, so the replacing rule does
	// not match and the walk ends on the status itself.
	code, _, respBody = postJSON(t, p.addr, "/v1/chat/completions",
		poolChatBody("pe-throttle", "hi"), nil)
	if code != http.StatusTooManyRequests {
		t.Fatalf("pe-throttle: status = %d, body %s — a 429 without the configured code stays terminal", code, respBody)
	}
	if !strings.Contains(string(respBody), `"code":"upstream_http_429"`) {
		t.Errorf("pe-throttle: body = %s, want the canonical upstream_http_429 envelope", respBody)
	}
	if throttle.count() != 1 || okThrottle.count() != 0 {
		t.Errorf("pe-throttle: primary/second = %d/%d, want 1/0 (no retry, no fallback)",
			throttle.count(), okThrottle.count())
	}

	waitForEventCount(t, p, "request_completed", 2)

	quotaEv := httpErrorEvidenceFor(t, p, "pe-quota")
	if len(quotaEv) != 1 {
		t.Fatalf("pe-quota: upstream_http_error events = %d, want 1", len(quotaEv))
	}
	for k, want := range map[string]any{
		"upstream_status":     float64(http.StatusTooManyRequests),
		"disposition":         "fallback",
		"reason":              "http_429",
		"policy_rule_id":      "http-429",
		"provider_error_code": "insufficient_quota",
	} {
		if quotaEv[0][k] != want {
			t.Errorf("pe-quota evidence: %s = %v, want %v", k, quotaEv[0][k], want)
		}
	}
	throttleEv := httpErrorEvidenceFor(t, p, "pe-throttle")
	if len(throttleEv) != 1 {
		t.Fatalf("pe-throttle: upstream_http_error events = %d, want 1", len(throttleEv))
	}
	for k, want := range map[string]any{
		"disposition":         "terminal",
		"policy_rule_id":      "http-class-4xx",
		"provider_error_code": "rate_limit_exceeded",
	} {
		if throttleEv[0][k] != want {
			t.Errorf("pe-throttle evidence: %s = %v, want %v", k, throttleEv[0][k], want)
		}
	}

	evs := completionsFor(t, p, "pe-quota")
	if len(evs) != 1 {
		t.Fatalf("pe-quota completions = %d, want 1", len(evs))
	}
	if evs[0]["final_provider"] != "pe-quota-p2" || evs[0]["provider_attempts"] != float64(2) ||
		evs[0]["retries_total"] != float64(0) {
		t.Errorf("pe-quota completion = %v/%v/%v, want final pe-quota-p2, 2 attempts, 0 retries",
			evs[0]["final_provider"], evs[0]["provider_attempts"], evs[0]["retries_total"])
	}
	evs = completionsFor(t, p, "pe-throttle")
	if len(evs) != 1 {
		t.Fatalf("pe-throttle completions = %d, want 1", len(evs))
	}
	if evs[0]["final_provider"] != "pe-throttle-p1" || evs[0]["provider_attempts"] != float64(1) {
		t.Errorf("pe-throttle completion = %v/%v, want final pe-throttle-p1, 1 attempt",
			evs[0]["final_provider"], evs[0]["provider_attempts"])
	}
}

// TestE2ERecoveryLayersChooseEffectiveRetries pins the layer merge black-box.
// Three models walk the same disposition table over the same retryable 429,
// and differ only in how many layers state a retry budget: the global layer
// says 3, a provider override says 2, and a model override says 1. The
// effective budget is observable as the number of times the primary is
// dialed before the walk falls back — narrowest layer last, so the model
// override wins; a model that states nothing inherits the global value.
func TestE2ERecoveryLayersChooseEffectiveRetries(t *testing.T) {
	// One retryable upstream and one healthy fallback per model, so every
	// dial count is unambiguous.
	global429 := newFakeUpstream(t)
	global429.setHandler(upstreamErrorWriter(http.StatusTooManyRequests, "rate_limit_error", "rate_limit_exceeded"))
	globalOK := newFakeUpstream(t)
	globalOK.setHandler(jsonChatHandler("lay-up-2"))

	provider429 := newFakeUpstream(t)
	provider429.setHandler(upstreamErrorWriter(http.StatusTooManyRequests, "rate_limit_error", "rate_limit_exceeded"))
	providerOK := newFakeUpstream(t)
	providerOK.setHandler(jsonChatHandler("lay-up-2"))

	model429 := newFakeUpstream(t)
	model429.setHandler(upstreamErrorWriter(http.StatusTooManyRequests, "rate_limit_error", "rate_limit_exceeded"))
	modelOK := newFakeUpstream(t)
	modelOK.setHandler(jsonChatHandler("lay-up-2"))

	top := `recovery:
  retries:
    max-retries: 3
    backoff:
      initial: 1ms
      max: 5ms
`
	providers := fmt.Sprintf(`providers:
  lay-g-p1:
    base-url: %s/v1
  lay-g-p2:
    base-url: %s/v1
  lay-p-p1:
    base-url: %s/v1
    recovery:
      retries:
        max-retries: 2
  lay-p-p2:
    base-url: %s/v1
  lay-m-p1:
    base-url: %s/v1
    recovery:
      retries:
        max-retries: 2
  lay-m-p2:
    base-url: %s/v1
`, global429.url(), globalOK.url(), provider429.url(), providerOK.url(), model429.url(), modelOK.url())
	models := `models:
  lay-global:
    providers:
      - provider: lay-g-p1
        upstream-model: lay-up-1
      - provider: lay-g-p2
        upstream-model: lay-up-2
  lay-provider:
    providers:
      - provider: lay-p-p1
        upstream-model: lay-up-1
      - provider: lay-p-p2
        upstream-model: lay-up-2
  lay-model:
    recovery:
      retries:
        max-retries: 1
    providers:
      - provider: lay-m-p1
        upstream-model: lay-up-1
      - provider: lay-m-p2
        upstream-model: lay-up-2
`
	p := startSubprocess(t, startOpts{yaml: recoveryFile(top, providers, models), logLevel: "info"})

	for _, tc := range []struct {
		model    string
		primary  *fakeUpstream
		fallback *fakeUpstream
		dials    int
		retries  float64
	}{
		{"lay-global", global429, globalOK, 4, 3},
		{"lay-provider", provider429, providerOK, 3, 2},
		{"lay-model", model429, modelOK, 2, 1},
	} {
		code, _, body := postJSON(t, p.addr, "/v1/chat/completions", poolChatBody(tc.model, "hi"), nil)
		if code != http.StatusOK {
			t.Fatalf("%s: status = %d, body %s — the retried 429 must fall back to the healthy candidate",
				tc.model, code, body)
		}
		if got := tc.primary.count(); got != tc.dials {
			t.Errorf("%s: primary dials = %d, want %d (effective max-retries %v)",
				tc.model, got, tc.dials, tc.retries)
		}
		if got := tc.fallback.count(); got != 1 {
			t.Errorf("%s: fallback candidate hits = %d, want 1", tc.model, got)
		}
	}

	waitForEventCount(t, p, "request_completed", 3)
	for model, wantRetries := range map[string]float64{
		"lay-global":   3,
		"lay-provider": 2,
		"lay-model":    1,
	} {
		evs := completionsFor(t, p, model)
		if len(evs) != 1 {
			t.Fatalf("%s: completions = %d, want 1", model, len(evs))
		}
		if evs[0]["retries_total"] != wantRetries {
			t.Errorf("%s: retries_total = %v, want %v", model, evs[0]["retries_total"], wantRetries)
		}
		if evs[0]["final_candidate"] != float64(2) {
			t.Errorf("%s: final_candidate = %v, want 2", model, evs[0]["final_candidate"])
		}
	}
}

// TestE2ERecoveryFrozenAcrossReload pins the immutability invariant black-box:
// a request that is mid-walk when the config file is rewritten keeps the
// snapshot it bound to. The primary of snapshot A holds the first attempt on a
// channel; the file is reloaded to snapshot B, whose primary is a different
// upstream and whose retry budget is three instead of zero; only then is the
// held attempt released. The request must finish under A — one dial of A's
// primary, fallback to A's healthy second candidate, generation 0 — while a
// request issued after the reload runs under B and spends B's budget.
func TestE2ERecoveryFrozenAcrossReload(t *testing.T) {
	heldA := newFakeUpstream(t)
	pending := make(chan struct{})
	release := make(chan struct{})
	var pendingOnce sync.Once
	heldA.setHandler(func(w http.ResponseWriter, r *http.Request) {
		pendingOnce.Do(func() { close(pending) })
		<-release
		upstreamErrorWriter(http.StatusTooManyRequests, "rate_limit_error", "rate_limit_exceeded")(w, r)
	})
	okA := newFakeUpstream(t)
	okA.setHandler(markerJSONHandler("A-MARKER", "fr-a-up-2"))

	immediateB := newFakeUpstream(t)
	immediateB.setHandler(upstreamErrorWriter(http.StatusTooManyRequests, "rate_limit_error", "rate_limit_exceeded"))
	okB := newFakeUpstream(t)
	okB.setHandler(markerJSONHandler("B-MARKER", "fr-b-up-2"))

	// The held handler blocks until released; the upstream's own cleanup runs
	// after this one (LIFO), so a failed assertion can never wedge Close.
	var releaseOnce sync.Once
	releaseFn := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(releaseFn)

	cfgA := recoveryFile(fmt.Sprintf(`providers:
  fr-a-p1:
    base-url: %s/v1
  fr-a-p2:
    base-url: %s/v1
`, heldA.url(), okA.url()), `models:
  fr-model:
    recovery:
      retries:
        max-retries: 0
    providers:
      - provider: fr-a-p1
        upstream-model: fr-a-up-1
      - provider: fr-a-p2
        upstream-model: fr-a-up-2
`)
	cfgB := recoveryFile(fmt.Sprintf(`providers:
  fr-b-p1:
    base-url: %s/v1
  fr-b-p2:
    base-url: %s/v1
`, immediateB.url(), okB.url()), `models:
  fr-model:
    recovery:
      retries:
        max-retries: 3
        backoff:
          initial: 1ms
          max: 5ms
    providers:
      - provider: fr-b-p1
        upstream-model: fr-b-up-1
      - provider: fr-b-p2
        upstream-model: fr-b-up-2
`)
	p := startSubprocess(t, startOpts{yaml: cfgA, logLevel: "info"})

	resCh := make(chan result, 1)
	go func() {
		status, _, body, err := postJSONRaw(p.addr, "/v1/chat/completions", poolChatBody("fr-model", "hi"), nil)
		resCh <- result{status: status, body: body, err: err}
	}()
	<-pending // snapshot A's primary is holding the first attempt

	rewriteConfig(t, p.cfgPath, cfgB)
	waitForEventCount(t, p, "log_level_applied", 1) // the reload ack: B is published

	if n := immediateB.count(); n != 0 {
		t.Fatalf("B's primary saw %d requests before release, want 0 (the request predates the reload)", n)
	}
	if n := okB.count(); n != 0 {
		t.Fatalf("B's fallback saw %d requests before release, want 0", n)
	}

	releaseFn()
	var res result
	select {
	case res = <-resCh:
	case <-time.After(5 * time.Second):
		t.Fatal("the in-flight request never completed after release")
	}
	if res.err != nil {
		t.Fatalf("in-flight request failed: %v", res.err)
	}
	if res.status != http.StatusOK {
		t.Fatalf("in-flight request status = %d, want 200 (body %s)", res.status, res.body)
	}
	if !strings.Contains(string(res.body), `"id":"A-MARKER"`) {
		t.Errorf("in-flight response = %s, want A's answer (the request keeps its snapshot)", res.body)
	}
	if got := heldA.count(); got != 1 {
		t.Errorf("A's primary dials = %d, want 1 — snapshot A states max-retries 0", got)
	}
	if got := okA.count(); got != 1 {
		t.Errorf("A's fallback hits = %d, want 1", got)
	}
	if n := immediateB.count(); n != 0 {
		t.Errorf("B's primary saw %d requests during the in-flight walk, want 0", n)
	}

	// A request issued after the reload runs under B and spends B's budget,
	// which is what makes the frozen behaviour above a contrast rather than a
	// coincidence of a reload that never landed.
	code, _, body := postJSON(t, p.addr, "/v1/chat/completions", poolChatBody("fr-model", "hi"), nil)
	if code != http.StatusOK || !strings.Contains(string(body), `"id":"B-MARKER"`) {
		t.Fatalf("post-reload request = %d %s, want B's 200 answer", code, body)
	}
	if got := immediateB.count(); got != 4 {
		t.Errorf("B's primary dials = %d, want 4 (snapshot B states max-retries 3)", got)
	}

	waitForEventCount(t, p, "request_completed", 2)
	evs := completionsFor(t, p, "fr-model")
	if len(evs) != 2 {
		t.Fatalf("completions for fr-model = %d, want 2", len(evs))
	}
	if evs[0]["config_generation"] != float64(0) {
		t.Errorf("in-flight config_generation = %v, want 0 (the boot snapshot)", evs[0]["config_generation"])
	}
	if evs[0]["final_provider"] != "fr-a-p2" || evs[0]["retries_total"] != float64(0) {
		t.Errorf("in-flight completion = %v/%v, want final fr-a-p2 and no retries",
			evs[0]["final_provider"], evs[0]["retries_total"])
	}
	if evs[1]["config_generation"] != float64(1) {
		t.Errorf("post-reload config_generation = %v, want 1", evs[1]["config_generation"])
	}
}

// TestE2ERecoveryCommittedSSENeverRetried pins the commitment boundary
// black-box: the candidate whose SSE headers were selected HAS produced the
// response. The upstream flushes one event and then dies mid-stream; the
// client keeps the event and sees a truncated stream, and neither a
// same-candidate retry (three are configured) nor the healthy fallback
// candidate is ever dialed.
func TestE2ERecoveryCommittedSSENeverRetried(t *testing.T) {
	aborting := newFakeUpstream(t)
	aborting.setHandler(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		fl := w.(http.Flusher)
		_, _ = fmt.Fprintf(w, "data: {\"id\":\"s1\",\"model\":\"cs-up-1\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"one\"},\"finish_reason\":null}]}\n\n")
		fl.Flush()
		// The committed stream now dies mid-flight.
		panic(http.ErrAbortHandler)
	})
	fallback := newFakeUpstream(t)
	fallback.setHandler(func(w http.ResponseWriter, _ *http.Request) {
		t.Error("a fallback candidate was dialed after the stream committed")
		w.WriteHeader(http.StatusInternalServerError)
	})

	top := `recovery:
  retries:
    max-retries: 3
    backoff:
      initial: 1ms
      max: 5ms
  fallback:
    max-candidates: 2
`
	providers := fmt.Sprintf(`providers:
  cs-p1:
    base-url: %s/v1
  cs-p2:
    base-url: %s/v1
`, aborting.url(), fallback.url())
	models := `models:
  cs-model:
    providers:
      - provider: cs-p1
        upstream-model: cs-up-1
      - provider: cs-p2
        upstream-model: cs-up-2
`
	p := startSubprocess(t, startOpts{yaml: recoveryFile(top, providers, models), logLevel: "info"})

	resp := openJSON(t, p.addr, "/v1/chat/completions", chatStreamRequest("cs-model"),
		map[string]string{"Accept": "text/event-stream"})
	defer func() { _ = resp.Body.Close() }()
	br := bufio.NewReader(resp.Body)

	// The committed event reaches the client: the response was produced before
	// the upstream died, which is exactly why nothing may be retried after it.
	lines, eof := nextSSEEvent(t, br, 3*time.Second)
	if eof {
		t.Fatal("stream ended before the committed event arrived")
	}
	assertChatSSE(t, lines, "cs-model", "one")

	// Whatever follows is a truncation, never a second event or a [DONE].
	tail, _ := io.ReadAll(br)
	if strings.Contains(string(tail), "data:") || strings.Contains(string(tail), "[DONE]") {
		t.Errorf("bytes followed the committed event after the upstream died: %q", tail)
	}

	waitForEventCount(t, p, "request_completed", 1)
	evs := completionsFor(t, p, "cs-model")
	if len(evs) != 1 {
		t.Fatalf("cs-model completions = %d, want 1", len(evs))
	}
	if evs[0]["outcome"] != "stream_truncated" {
		t.Errorf("outcome = %v, want stream_truncated", evs[0]["outcome"])
	}
	if got := aborting.count(); got != 1 {
		t.Errorf("committed candidate dials = %d, want 1 (no same-candidate retry)", got)
	}
	if got := fallback.count(); got != 0 {
		t.Errorf("fallback candidate dials = %d, want 0 (no fallback after commitment)", got)
	}
	trunc := eventsWithMessage(parseLogEvents(t, p.stderr.String()), "stream_truncated")
	if len(trunc) != 1 {
		t.Fatalf("stream_truncated events = %d, want 1", len(trunc))
	}
	if trunc[0]["phase"] != "upstream_read" {
		t.Errorf("stream_truncated phase = %v, want upstream_read", trunc[0]["phase"])
	}
}

// TestE2ERecoveryExchangeBudgetCapsPoolDials pins the exchange envelope
// black-box. Two models walk the same three-member dead pool; one states a
// per-candidate ceiling of two exchanges and one states none. The envelope
// counts REAL outbound exchanges, so the capped walk stops after two dials —
// the third member is never reached, no endpoint is blamed, and the walk ends
// on the envelope — while the uncapped walk dials all three.
func TestE2ERecoveryExchangeBudgetCapsPoolDials(t *testing.T) {
	sections := []string{
		`transports:
  eb-pool-capped:
    type: pool
    members: [eb-dead-a, eb-dead-b, eb-dead-c]
  eb-dead-a:
    type: proxy
    proxy: http://127.0.0.1:1
  eb-dead-b:
    type: proxy
    proxy: http://127.0.0.1:2
  eb-dead-c:
    type: proxy
    proxy: http://127.0.0.1:3
  eb-pool-open:
    type: pool
    members: [eb-dead-x, eb-dead-y, eb-dead-z]
  eb-dead-x:
    type: proxy
    proxy: http://127.0.0.1:4
  eb-dead-y:
    type: proxy
    proxy: http://127.0.0.1:5
  eb-dead-z:
    type: proxy
    proxy: http://127.0.0.1:6
`,
		`providers:
  eb-provider-capped:
    base-url: http://127.0.0.1:1/v1
    transport: eb-pool-capped
  eb-provider-open:
    base-url: http://127.0.0.1:1/v1
    transport: eb-pool-open
`,
		`models:
  eb-capped:
    provider: eb-provider-capped
    upstream-model: eb-up
    recovery:
      retries:
        max-retries: 0
      budget:
        candidate:
          max-exchanges: 2
  eb-open:
    provider: eb-provider-open
    upstream-model: eb-up
`,
	}
	p := startSubprocess(t, startOpts{yaml: recoveryFile(sections...), logLevel: "info"})

	for _, model := range []string{"eb-capped", "eb-open"} {
		code, _, body := postJSON(t, p.addr, "/v1/chat/completions", poolChatBody(model, "hi"), nil)
		if code != http.StatusBadGateway {
			t.Fatalf("%s: status = %d, body %s — a walk that never received an answer is 502", model, code, body)
		}
		if got := strings.TrimSpace(string(body)); got != envelopeUpstreamUnreachable {
			t.Errorf("%s: body = %q, want the canonical upstream_unreachable envelope", model, got)
		}
	}

	waitForEventCount(t, p, "request_completed", 2)

	if got := dialFailuresFor(t, p, "eb-capped"); got != 2 {
		t.Errorf("eb-capped dials = %d, want 2 (the candidate envelope refuses the third)", got)
	}
	if got := dialFailuresFor(t, p, "eb-open"); got != 3 {
		t.Errorf("eb-open dials = %d, want 3 (no envelope: every member is dialed)", got)
	}
	capped := completionsFor(t, p, "eb-capped")
	if len(capped) != 1 {
		t.Fatalf("eb-capped completions = %d, want 1", len(capped))
	}
	if capped[0]["upstream_exchanges"] != float64(2) {
		t.Errorf("eb-capped upstream_exchanges = %v, want 2", capped[0]["upstream_exchanges"])
	}
	if capped[0]["provider_exhausted"] != true {
		t.Errorf("eb-capped provider_exhausted = %v, want true", capped[0]["provider_exhausted"])
	}
	open := completionsFor(t, p, "eb-open")
	if len(open) != 1 {
		t.Fatalf("eb-open completions = %d, want 1", len(open))
	}
	if open[0]["upstream_exchanges"] != float64(3) {
		t.Errorf("eb-open upstream_exchanges = %v, want 3", open[0]["upstream_exchanges"])
	}

	// The refusal names the envelope, never an endpoint: the floor-priced
	// walk reports the candidate-scope identity and the budget cause.
	var exhausted logEvent
	for _, ev := range eventsWithMessage(parseLogEvents(t, p.stderr.String()), "upstream_request_failed") {
		if ev["public_model"] == "eb-capped" {
			exhausted = ev
		}
	}
	if exhausted == nil {
		t.Fatalf("no exhaustion record for eb-capped; stderr:\n%s", p.stderr.String())
	}
	for k, want := range map[string]any{
		"error_class":        "provider_exhausted",
		"error_cause":        "exchange_budget",
		"policy_rule_id":     "budget-candidate",
		"provider_exhausted": true,
	} {
		if exhausted[k] != want {
			t.Errorf("eb-capped exhaustion: %s = %v, want %v", k, exhausted[k], want)
		}
	}
}

// TestE2ERecoveryRetryRotatesEgressWithoutProviderFallback is the flagship
// recovery story on the wire: a candidate is throttled, the retry is routed
// out a DIFFERENT egress member, and that second dial answers 200 — so the
// walk never leaves the first candidate. The chain carries a healthy second
// provider on purpose: the only proof that provider fallback was not used is
// a second candidate that was never dialed.
func TestE2ERecoveryRetryRotatesEgressWithoutProviderFallback(t *testing.T) {
	up := newFakeUpstream(t)
	var mu sync.Mutex
	calls := 0
	up.setHandler(func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		calls++
		n := calls
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		if n == 1 {
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = io.WriteString(w, `{"error":{"message":"slow down","type":"rate_limit_error","code":"rate_limit_exceeded"}}`)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, `{"id":"s1-ok","model":"s1-up","choices":[]}`)
	})
	second := newFakeUpstream(t)
	second.setHandler(markerJSONHandler("s1-second", "s1-up"))
	relay := newForwardingProxy(t)
	socks := newSocks5Egress(t)
	socksURL := "socks5://" + socks.addr()

	sections := []string{
		fmt.Sprintf(`transports:
  s1-relay:
    type: proxy
    proxy: %s
  s1-socks:
    type: proxy
    proxy: %s
  s1-pool:
    type: pool
    members: [s1-relay, s1-socks]
`, relay.srv.URL, socksURL),
		fmt.Sprintf(`providers:
  s1-primary:
    base-url: %s/v1
    transport: s1-pool
  s1-second:
    base-url: %s/v1
`, up.url(), second.url()),
		`models:
  s1-model:
    providers:
      - provider: s1-primary
        upstream-model: s1-up
      - provider: s1-second
        upstream-model: s1-up
`,
	}
	p := startSubprocess(t, startOpts{yaml: recoveryFile(sections...), logLevel: "info"})

	code, _, body := postJSON(t, p.addr, "/v1/chat/completions", poolChatBody("s1-model", "hi"), nil)
	if code != http.StatusOK {
		t.Fatalf("status = %d, body %s — the retry should have answered", code, body)
	}
	if id := decodeMap(t, body)["id"]; id != "s1-ok" {
		t.Fatalf("response id = %v, want the primary's s1-ok", id)
	}
	if got := up.count(); got != 2 {
		t.Fatalf("primary dials = %d, want 2 (the throttled dial plus its retry)", got)
	}
	if got := relay.count(); got != 1 {
		t.Errorf("http proxy hits = %d, want 1 (the first dial)", got)
	}
	if got := socks.count(); got != 1 {
		t.Errorf("socks dials = %d, want 1 (the retry rotated to the second egress)", got)
	}
	if got := second.count(); got != 0 {
		t.Errorf("fallback provider dials = %d, want 0 (the retry answered on the first candidate)", got)
	}

	waitForEventCount(t, p, "request_completed", 1)
	evs := completionsFor(t, p, "s1-model")
	if len(evs) != 1 {
		t.Fatalf("completions = %d, want 1", len(evs))
	}
	for k, want := range map[string]any{
		"outcome":            "completed",
		"candidates_entered": float64(1),
		"retries_total":      float64(1),
		"candidate_attempts": float64(2),
		"upstream_exchanges": float64(2),
		"final_provider":     "s1-primary",
		"final_candidate":    float64(1),
		"egress_kind":        "socks5",
		"egress_target":      socksURL,
	} {
		if evs[0][k] != want {
			t.Errorf("completion %s = %v, want %v", k, evs[0][k], want)
		}
	}

	// The throttled attempt left evidence of its own: one received 429,
	// disposed as a retry, with the status carried.
	http4xx := httpErrorEvidenceFor(t, p, "s1-model")
	if len(http4xx) != 1 {
		t.Fatalf("upstream_http_error records = %d, want 1", len(http4xx))
	}
	if http4xx[0]["upstream_status"] != float64(429) || http4xx[0]["disposition"] != "retry" {
		t.Errorf("429 evidence = status %v / disposition %v, want 429 / retry",
			http4xx[0]["upstream_status"], http4xx[0]["disposition"])
	}
}
