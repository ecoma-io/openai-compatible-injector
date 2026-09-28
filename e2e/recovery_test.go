package e2e_test

import (
	"bufio"
	"encoding/json"
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

// TestE2EStreamRecoveryContinuesACutStream drives the real binary with
// post-commitment stream recovery switched on. The upstream commits a
// fragment and dies mid-generation; the proxy re-asks the SAME upstream with
// the committed text as an assistant turn and relays the second answer into
// the client's still-open stream. The client holds one connection and sees
// exactly one terminal marker across both hops.
//
// Its sibling above — TestE2ERecoveryCommittedSSENeverRetried — is the same
// wire with the block absent. Read together they are the whole compatibility
// claim: the identical truncation that is nothing but a truncation by default
// is the one this block exists to continue, and nothing about the default
// path changes to make that possible.
func TestE2EStreamRecoveryContinuesACutStream(t *testing.T) {
	up := newFakeUpstream(t)
	up.setHandler(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		fl := w.(http.Flusher)
		// The first dial commits a fragment and dies; the second — the
		// continuation — finishes the answer. The request being served is
		// already recorded, so the first dial observes 1.
		if up.count() == 1 {
			_, _ = fmt.Fprintf(w, "data: {\"id\":\"sr1\",\"model\":\"sr-up-1\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"one \"},\"finish_reason\":null}]}\n\n")
			fl.Flush()
			panic(http.ErrAbortHandler)
		}
		_, _ = fmt.Fprintf(w, "data: {\"id\":\"sr2\",\"model\":\"sr-up-1\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"two\"},\"finish_reason\":null}]}\n\n")
		fl.Flush()
		_, _ = fmt.Fprintf(w, "data: [DONE]\n\n")
		fl.Flush()
	})

	top := `recovery:
  stream:
    enabled: true
    max-recoveries: 1
    max-elapsed: 30s
    max-partial-bytes: 4096
`
	providers := fmt.Sprintf(`providers:
  sr-p1:
    base-url: %s/v1
`, up.url())
	models := `models:
  sr-model:
    providers:
      - provider: sr-p1
        upstream-model: sr-up-1
`
	p := startSubprocess(t, startOpts{yaml: recoveryFile(top, providers, models), logLevel: "info"})

	resp := openJSON(t, p.addr, "/v1/chat/completions", chatStreamRequest("sr-model"),
		map[string]string{"Accept": "text/event-stream"})
	defer func() { _ = resp.Body.Close() }()
	br := bufio.NewReader(resp.Body)

	// Hop 1's fragment, hop 2's continuation, then the ONE terminal marker —
	// three events on one connection, in order, the public model rewritten in
	// both payloads.
	lines, eof := nextSSEEvent(t, br, 5*time.Second)
	if eof {
		t.Fatal("stream ended before the committed fragment arrived")
	}
	assertChatSSE(t, lines, "sr-model", "one ")

	lines, eof = nextSSEEvent(t, br, 5*time.Second)
	if eof {
		t.Fatal("no continuation event: the stream was truncated instead of recovered")
	}
	assertChatSSE(t, lines, "sr-model", "two")

	lines, eof = nextSSEEvent(t, br, 5*time.Second)
	if eof {
		t.Fatal("stream ended without a terminal marker: the continuation's [DONE] never reached the client")
	}
	if len(lines) != 1 || !strings.Contains(lines[0], "[DONE]") {
		t.Fatalf("terminal event = %q, want exactly one data: [DONE]", lines)
	}
	// Nothing follows the marker: a stream is terminated once, not twice.
	if tail, _ := io.ReadAll(br); strings.Contains(string(tail), "data:") {
		t.Errorf("bytes followed the terminal marker: %q", tail)
	}

	// Two dials and no third: the walk stopped at commitment, and the
	// continuation was the one extra request the block's reach allows.
	if got := up.count(); got != 2 {
		t.Fatalf("upstream dials = %d, want 2 (one walk attempt plus one continuation)", got)
	}

	// The continuation is the same candidate RE-ASKED, not a blind replay: the
	// second request carries the committed text as an assistant turn, and it
	// is still a stream.
	reqs := up.requests()
	if len(reqs) != 2 {
		t.Fatalf("recorded requests = %d, want 2", len(reqs))
	}
	var sent struct {
		Stream   bool `json:"stream"`
		Messages []struct {
			Role    string `json:"role"`
			Content string `json:"content"`
		} `json:"messages"`
	}
	if err := json.Unmarshal(reqs[1].Body, &sent); err != nil {
		t.Fatalf("decode the continuation body: %v", err)
	}
	if len(sent.Messages) != 2 {
		t.Fatalf("continuation messages = %d, want the client's one plus the assistant turn: %s", len(sent.Messages), reqs[1].Body)
	}
	if sent.Messages[1].Role != "assistant" || sent.Messages[1].Content != "one " {
		t.Fatalf("the continuation did not carry the committed prefix as an assistant turn: %s", reqs[1].Body)
	}
	if !sent.Stream {
		t.Fatalf("the continuation request is not a stream: %s", reqs[1].Body)
	}

	waitForEventCount(t, p, "request_completed", 1)
	evs := completionsFor(t, p, "sr-model")
	if len(evs) != 1 {
		t.Fatalf("sr-model completions = %d, want 1", len(evs))
	}
	// The request is completed, not truncated: the continuation carried the
	// stream all the way to its marker, so the outcome a client would act on
	// is the honest one.
	if evs[0]["outcome"] != "completed" {
		t.Errorf("outcome = %v, want completed", evs[0]["outcome"])
	}
	// A continuation is a real outbound exchange on the SAME provider, so the
	// walk's own counters see it: two provider-level attempts, two dials, one
	// candidate, no retry.
	if got := evs[0]["provider_attempts"]; got != float64(2) {
		t.Errorf("provider_attempts = %v, want 2 (the walk attempt plus the continuation)", got)
	}
	if got := evs[0]["upstream_exchanges"]; got != float64(2) {
		t.Errorf("upstream_exchanges = %v, want 2", got)
	}
	if got := evs[0]["candidates_entered"]; got != float64(1) {
		t.Errorf("candidates_entered = %v, want 1 (a continuation never moves candidate)", got)
	}
	if got := evs[0]["final_provider"]; got != "sr-p1" {
		t.Errorf("final_provider = %v, want sr-p1", got)
	}

	// The recovery evidence itself. started and succeeded are exactly paired,
	// and nothing reported a truncation of a stream that finished.
	logs := parseLogEvents(t, p.stderr.String())
	if got := len(eventsWithMessage(logs, "stream_recovery_started")); got != 1 {
		t.Errorf("stream_recovery_started events = %d, want 1", got)
	}
	succeeded := eventsWithMessage(logs, "stream_recovery_succeeded")
	if len(succeeded) != 1 {
		t.Fatalf("stream_recovery_succeeded events = %d, want 1", len(succeeded))
	}
	if got := succeeded[0]["public_model"]; got != "sr-model" {
		t.Errorf("stream_recovery_succeeded public_model = %v, want sr-model", got)
	}
	if got := succeeded[0]["provider"]; got != "sr-p1" {
		t.Errorf("stream_recovery_succeeded provider = %v, want sr-p1", got)
	}
	if got := len(eventsWithMessage(logs, "stream_recovery_failed")); got != 0 {
		t.Errorf("stream_recovery_failed events = %d, want 0", got)
	}
	if got := len(eventsWithMessage(logs, "stream_recovery_exhausted")); got != 0 {
		t.Errorf("stream_recovery_exhausted events = %d, want 0", got)
	}
	if got := len(eventsWithMessage(logs, "stream_truncated")); got != 0 {
		t.Errorf("stream_truncated events = %d, want 0", got)
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
	// The completion record reports the REQUEST-wide headroom, not the tighter
	// number: eb-capped spent its candidate envelope (that is why the walk
	// stopped) and still has 30 of the default 32 request exchanges left. A
	// completion that reported the tighter envelope would read 0 here and
	// could not tell the two walks apart.
	if capped[0]["request_exchange_budget_remaining"] != float64(30) {
		t.Errorf("eb-capped request_exchange_budget_remaining = %v, want 30", capped[0]["request_exchange_budget_remaining"])
	}
	open := completionsFor(t, p, "eb-open")
	if len(open) != 1 {
		t.Fatalf("eb-open completions = %d, want 1", len(open))
	}
	if open[0]["upstream_exchanges"] != float64(3) {
		t.Errorf("eb-open upstream_exchanges = %v, want 3", open[0]["upstream_exchanges"])
	}
	if open[0]["request_exchange_budget_remaining"] != float64(29) {
		t.Errorf("eb-open request_exchange_budget_remaining = %v, want 29", open[0]["request_exchange_budget_remaining"])
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

// TestE2ERecoverySpentCandidateEnvelopeStillFallsBack pins the candidate
// envelope's boundary on the wire: it bounds ONE candidate's exchanges and
// nothing else. The primary is a pool whose members are all dead, so the
// candidate's two exchange units are spent on two failed dials; the walk
// must then reach the healthy backup, whose own envelope is fresh, rather
// than reporting the request exhausted. A per-candidate number that pinned
// the chain would silently overrule the fallback policy the file states.
func TestE2ERecoverySpentCandidateEnvelopeStillFallsBack(t *testing.T) {
	backup := newFakeUpstream(t)
	backup.setHandler(markerJSONHandler("sc-backup", "sc-up"))

	sections := []string{
		`transports:
  sc-pool:
    type: pool
    members: [sc-dead-a, sc-dead-b, sc-dead-c]
  sc-dead-a:
    type: proxy
    proxy: http://127.0.0.1:1
  sc-dead-b:
    type: proxy
    proxy: http://127.0.0.1:2
  sc-dead-c:
    type: proxy
    proxy: http://127.0.0.1:3
`,
		fmt.Sprintf(`providers:
  sc-primary:
    base-url: http://127.0.0.1:1/v1
    transport: sc-pool
  sc-backup:
    base-url: %s/v1
`, backup.url()),
		`models:
  sc-model:
    recovery:
      retries:
        max-retries: 0
      budget:
        candidate:
          max-exchanges: 2
    providers:
      - provider: sc-primary
        upstream-model: sc-up
      - provider: sc-backup
        upstream-model: sc-up
`,
	}
	p := startSubprocess(t, startOpts{yaml: recoveryFile(sections...), logLevel: "info"})

	code, _, body := postJSON(t, p.addr, "/v1/chat/completions", poolChatBody("sc-model", "hi"), nil)
	if code != http.StatusOK {
		t.Fatalf("status = %d, body %s — the backup candidate should have answered", code, body)
	}
	if id := decodeMap(t, body)["id"]; id != "sc-backup" {
		t.Fatalf("response id = %v, want the backup's sc-backup", id)
	}
	if got := backup.count(); got != 1 {
		t.Errorf("backup dials = %d, want 1 (entered after the primary's envelope was spent)", got)
	}

	waitForEventCount(t, p, "request_completed", 1)
	evs := completionsFor(t, p, "sc-model")
	if len(evs) != 1 {
		t.Fatalf("completions = %d, want 1", len(evs))
	}
	for k, want := range map[string]any{
		"outcome":            "completed",
		"candidates_entered": float64(2),
		"candidate_attempts": float64(2),
		"retries_total":      float64(0),
		"upstream_exchanges": float64(3),
		"final_provider":     "sc-backup",
		"final_candidate":    float64(2),
	} {
		if evs[0][k] != want {
			t.Errorf("completion %s = %v, want %v", k, evs[0][k], want)
		}
	}
	// A provider answered, so the walk is not exhausted.
	if _, set := evs[0]["provider_exhausted"]; set {
		t.Errorf("provider_exhausted set on a walk a provider answered")
	}
	// The spent envelope is visible as two dialed-and-failed endpoints, and
	// the refusal that ended that candidate blamed no endpoint.
	if got := dialFailuresFor(t, p, "sc-model"); got != 2 {
		t.Errorf("dial failures = %d, want 2 (the candidate envelope refuses the third dial)", got)
	}
}

// ---- The adversarial stream-recovery matrix -------------------------------

// drainSSE reads every remaining SSE line until EOF and reports the whole
// text plus whether a terminal marker was among it. A cut stream ends here
// exactly as a client experiences it: bytes, then the connection.
func drainSSE(t *testing.T, br *bufio.Reader, timeout time.Duration) (text string, terminal bool) {
	t.Helper()
	var sb strings.Builder
	for {
		line, err := readSSELine(t, br, timeout)
		if err == io.EOF {
			sb.WriteString(line)
			break
		}
		if err != nil {
			t.Fatalf("read SSE line: %v", err)
		}
		sb.WriteString(line)
		sb.WriteString("\n")
	}
	s := sb.String()
	return s, strings.Contains(s, "[DONE]") || strings.Contains(s, "event: response.completed")
}

// streamRecoveryYAML renders the standard recovery-on file for one upstream.
func streamRecoveryYAML(up *fakeUpstream, stream string) string {
	return recoveryFile(fmt.Sprintf(`recovery:
  stream:
%s`, stream), fmt.Sprintf(`providers:
  srm-p1:
    base-url: %s/v1
`, up.url()), `models:
  srm-model:
    providers:
      - provider: srm-p1
        upstream-model: srm-up
`)
}

// TestE2EStreamRecoveryRefusalMatrix is the fail-closed half of the feature on
// the real wire: every shape below is one this proxy must NOT continue. What
// each row proves is the SAME two facts a client can observe — the upstream
// was dialed exactly once (no continuation request was ever made) and the
// client's stream ended with the bytes it already had, with no marker this
// proxy invented. The per-row reason distinguishes WHY the pool of refusals
// fired, and `logical_terminal` is deliberately not `unsafe_content`: a
// generation the upstream declared finished is not a stream this proxy failed
// to read.
func TestE2EStreamRecoveryRefusalMatrix(t *testing.T) {
	cases := []struct {
		name string
		// extra is appended after the committed fragment.
		extra string
		// wantExhaust is the closed-set reason on stream_recovery_exhausted;
		// empty means the stream was refused with no recovery event at all.
		wantExhaust string
		// wantUnsafe is the gate's own token, when there is one.
		wantUnsafe string
	}{
		// The cut stream itself — a clean fragment and then an abort — is the
		// one shape that IS recoverable, so it is asserted where it belongs:
		// TestE2EStreamRecoveryContinuesACutStream.
		{
			name:        "the upstream declared the finish",
			extra:       `data: {"id":"sr2","model":"srm-up","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}` + "\n\n",
			wantExhaust: "logical_terminal",
		},
		{
			name:        "the stream carries a tool call",
			extra:       `data: {"id":"sr2","model":"srm-up","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"call_1","function":{"name":"ls"}}]},"finish_reason":null}]}` + "\n\n",
			wantExhaust: "unsafe_content",
			wantUnsafe:  "tool_calls",
		},
		{
			name:        "an unrecognised data line",
			extra:       `data: {"choices":{"delta":{"content":"x"}}}` + "\n\n",
			wantExhaust: "unsafe_content",
			wantUnsafe:  "unknown_shape",
		},
		{
			name:        "a data line that is not JSON",
			extra:       "data: }{ \n\n",
			wantExhaust: "unsafe_content",
			wantUnsafe:  "not_object",
		},
		{
			name: "a prefix past the configured bound",
			// max-partial-bytes is 4096 in the file below.
			extra:       "data: {\"id\":\"sr2\",\"model\":\"srm-up\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"" + strings.Repeat("x", 8192) + "\"},\"finish_reason\":null}]}\n\n",
			wantExhaust: "unsafe_content",
			wantUnsafe:  "oversize",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			up := newFakeUpstream(t)
			up.setHandler(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "text/event-stream")
				w.WriteHeader(http.StatusOK)
				fl := w.(http.Flusher)
				_, _ = fmt.Fprintf(w, "data: {\"id\":\"sr1\",\"model\":\"srm-up\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"half \"},\"finish_reason\":null}]}\n\n")
				fl.Flush()
				_, _ = fmt.Fprint(w, tc.extra)
				fl.Flush()
				panic(http.ErrAbortHandler)
			})

			p := startSubprocess(t, startOpts{
				yaml:     streamRecoveryYAML(up, "    enabled: true\n    max-recoveries: 2\n    max-elapsed: 30s\n    max-partial-bytes: 4096\n"),
				logLevel: "info",
			})
			resp := openJSON(t, p.addr, "/v1/chat/completions", chatStreamRequest("srm-model"),
				map[string]string{"Accept": "text/event-stream"})
			defer func() { _ = resp.Body.Close() }()
			br := bufio.NewReader(resp.Body)

			lines, eof := nextSSEEvent(t, br, 5*time.Second)
			if eof {
				t.Fatal("the committed fragment never arrived")
			}
			assertChatSSE(t, lines, "srm-model", "half ")

			text, terminal := drainSSE(t, br, 10*time.Second)
			if terminal {
				t.Errorf("a refused stream carried a terminal marker: %q", text)
			}

			// ONE dial: no continuation request was made for this stream.
			if got := up.count(); got != 1 {
				t.Fatalf("upstream dials = %d, want 1 — a refused stream must not be re-asked", got)
			}
			// The client's EOF and the process's report of it are two
			// different clocks — the relay ends the moment the upstream
			// aborts, and the refusal is written after. Wait for the
			// truncation report before reading the refusal that precedes it.
			waitForEventCount(t, p, "stream_truncated", 1)
			logs := parseLogEvents(t, p.stderr.String())
			if got := len(eventsWithMessage(logs, "stream_recovery_started")); got != 0 {
				t.Errorf("stream_recovery_started events = %d, want 0", got)
			}
			exh := eventsWithMessage(logs, "stream_recovery_exhausted")
			if len(exh) != 1 {
				t.Fatalf("stream_recovery_exhausted events = %v, want 1", exh)
			}
			if exh[0]["reason"] != tc.wantExhaust {
				t.Errorf("reason = %v, want %v", exh[0]["reason"], tc.wantExhaust)
			}
			if tc.wantUnsafe != "" && exh[0]["unsafe_reason"] != tc.wantUnsafe {
				t.Errorf("unsafe_reason = %v, want %v", exh[0]["unsafe_reason"], tc.wantUnsafe)
			}
			// A declared finish is NOT unsafe content: the field that carries
			// the gate's refusal must be absent, because there was no gate
			// refusal — the generation simply ended.
			if tc.wantUnsafe == "" {
				if _, has := exh[0]["unsafe_reason"]; has {
					t.Errorf("a logical terminal was reported as unsafe content: %v", exh[0])
				}
			}
			trunc := eventsWithMessage(parseLogEvents(t, p.stderr.String()), "stream_truncated")
			if len(trunc) != 1 {
				t.Fatalf("stream_truncated events = %d, want 1", len(trunc))
			}
			if trunc[0]["recovery_reason"] != tc.wantExhaust {
				t.Errorf("stream_truncated recovery_reason = %v, want %v", trunc[0]["recovery_reason"], tc.wantExhaust)
			}
		})
	}
}

// TestE2EStreamRecoveryMaxElapsedCutsAStalledUpstream is the hard bound on
// real sockets, and the reason the bound is not merely a check the loop makes
// between reads: the upstream sends a partial event and then HOLDS THE TCP
// CONNECTION OPEN, so the relay is parked inside a read of a real
// net/http response body. Nothing the loop can inspect afterwards can reach
// it — the recovery window's watchdog closing that body is what ends the
// stream, and the client sees exactly the truncation a deployment without the
// feature always had.
func TestE2EStreamRecoveryMaxElapsedCutsAStalledUpstream(t *testing.T) {
	up := newFakeUpstream(t)
	hold := make(chan struct{})
	// A failed assertion must not leave the handler blocked forever:
	// httptest.Server.Close waits for it, and with it the whole test binary.
	t.Cleanup(func() { close(hold) })
	up.setHandler(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		fl := w.(http.Flusher)
		_, _ = fmt.Fprintf(w, "data: {\"id\":\"st1\",\"model\":\"srm-up\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"half \"},\"finish_reason\":null}]}\n\n")
		fl.Flush()
		// ... and then not another byte, with the connection still open.
		<-hold
	})

	p := startSubprocess(t, startOpts{
		yaml:     streamRecoveryYAML(up, "    enabled: true\n    max-recoveries: 2\n    max-elapsed: 2s\n    max-partial-bytes: 4096\n"),
		logLevel: "info",
	})
	resp := openJSON(t, p.addr, "/v1/chat/completions", chatStreamRequest("srm-model"),
		map[string]string{"Accept": "text/event-stream"})
	defer func() { _ = resp.Body.Close() }()
	br := bufio.NewReader(resp.Body)

	lines, eof := nextSSEEvent(t, br, 5*time.Second)
	if eof {
		t.Fatal("the committed fragment never arrived")
	}
	assertChatSSE(t, lines, "srm-model", "half ")

	// The bound fires and the stream ends. Generous read window: the assertion
	// is that it ends at all, not how fast the timer is.
	text, terminal := drainSSE(t, br, 20*time.Second)
	if terminal {
		t.Errorf("an abandoned stream carried a terminal marker: %q", text)
	}
	if got := up.count(); got != 1 {
		t.Fatalf("upstream dials = %d, want 1 — no continuation past the window", got)
	}
	waitForEventCount(t, p, "stream_truncated", 1)

	logs := parseLogEvents(t, p.stderr.String())
	exh := eventsWithMessage(logs, "stream_recovery_exhausted")
	if len(exh) != 1 || exh[0]["reason"] != "max_elapsed" {
		t.Fatalf("stream_recovery_exhausted = %v, want one max_elapsed refusal", exh)
	}
	trunc := eventsWithMessage(logs, "stream_truncated")
	if len(trunc) != 1 {
		t.Fatalf("stream_truncated = %v, want one", trunc)
	}
	if trunc[0]["recovery_reason"] != "max_elapsed" {
		t.Errorf("recovery_reason = %v, want max_elapsed", trunc[0]["recovery_reason"])
	}
	// The order the events are read in is the ordering that keeps the bound
	// legible: this is the operator's own window, NOT a peer that broke and
	// NOT a client that left. Both of those would have produced a different
	// outcome, and neither can produce this pair.
	if _, has := trunc[0]["error"]; has {
		t.Errorf("the window reported an upstream error it invented: %v", trunc[0])
	}
	if trunc[0]["outcome"] == "client_disconnected" {
		t.Errorf("the window was reported as a client disconnect: %v", trunc[0])
	}
	evs := completionsFor(t, p, "srm-model")
	if len(evs) != 1 || evs[0]["outcome"] != "stream_truncated" {
		t.Fatalf("completions = %v, want one stream_truncated", evs)
	}
}

// TestE2EStreamRecoverySpendsNothingForADepartedClient pins the cancellation
// invariant on real sockets: once the client is gone, the proxy makes no
// further upstream request. The upstream here is stalled — the proxy's read is
// parked on a real response body — and the client closing its own connection
// is what has to end it: the request context cancels, the transport aborts the
// parked read, and the loop refuses to dial for a stream nobody is reading.
func TestE2EStreamRecoverySpendsNothingForADepartedClient(t *testing.T) {
	up := newFakeUpstream(t)
	hold := make(chan struct{})
	t.Cleanup(func() { close(hold) })
	up.setHandler(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		fl := w.(http.Flusher)
		_, _ = fmt.Fprintf(w, "data: {\"id\":\"dc1\",\"model\":\"srm-up\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"half \"},\"finish_reason\":null}]}\n\n")
		fl.Flush()
		<-hold
	})

	// A wide window: the disconnect has to be what ends this, not the bound.
	p := startSubprocess(t, startOpts{
		yaml:     streamRecoveryYAML(up, "    enabled: true\n    max-recoveries: 2\n    max-elapsed: 60s\n    max-partial-bytes: 4096\n"),
		logLevel: "info",
	})
	resp := openJSON(t, p.addr, "/v1/chat/completions", chatStreamRequest("srm-model"),
		map[string]string{"Accept": "text/event-stream"})
	br := bufio.NewReader(resp.Body)
	lines, eof := nextSSEEvent(t, br, 5*time.Second)
	if eof {
		t.Fatal("the committed fragment never arrived")
	}
	assertChatSSE(t, lines, "srm-model", "half ")

	// The client walks away mid-generation.
	_ = resp.Body.Close()

	waitForEventCount(t, p, "request_completed", 1)
	evs := completionsFor(t, p, "srm-model")
	if len(evs) != 1 {
		t.Fatalf("completions = %d, want 1", len(evs))
	}
	if got := up.count(); got != 1 {
		t.Fatalf("upstream dials = %d, want 1 — no exchange may be spent for a client that is gone", got)
	}
	logs := parseLogEvents(t, p.stderr.String())
	for _, slug := range []string{"stream_recovery_started", "stream_recovery_failed", "stream_recovery_succeeded"} {
		if got := len(eventsWithMessage(logs, slug)); got != 0 {
			t.Errorf("%s events = %d, want 0 after the client left", slug, got)
		}
	}
	if evs[0]["outcome"] != "client_disconnected" {
		t.Errorf("outcome = %v, want client_disconnected", evs[0]["outcome"])
	}
}

// TestE2EStreamRecoveryContinuesACleanEOF pins the OTHER way a committed
// stream ends without its marker: the upstream closes the response body
// normally. That is a clean EOF on the wire — no read error at all — and it is
// the most common truncation a real provider produces, so the feature has to
// continue it exactly like an aborted connection. The client ends on the hop's
// marker, and nothing follows it: the proxy appends no terminal of its own to
// a hop that ended, and adds none after one.
//
// (A provider that sends the marker twice has its bytes relayed twice, exactly
// as it would with the feature off: the relay is byte-faithful and never edits
// a stream. What is pinned here is the proxy's own contribution — nothing.)
func TestE2EStreamRecoveryContinuesACleanEOF(t *testing.T) {
	// Two sentinels travel through this request: the client's own turn, and
	// the answer text the proxy has to hold in order to continue it. Both are
	// asserted absent from the process's output at the end — the committed
	// prefix is the one piece of client-visible text this feature takes
	// custody of, and custody never means writing it to a log.
	const (
		userSentinel   = "USERTURNSEKRIT"
		prefixSentinel = "PREFIXSEKRIT"
	)
	up := newFakeUpstream(t)
	up.setHandler(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		if up.count() == 1 {
			_, _ = fmt.Fprintf(w, "data: {\"id\":\"ce1\",\"model\":\"srm-up\",\"choices\":[{\"index\":0,\"delta\":{\"content\":%q},\"finish_reason\":null}]}\n\n", prefixSentinel+" ")
			w.(http.Flusher).Flush()
			// A clean return: the response ends, the connection stays healthy.
			return
		}
		_, _ = fmt.Fprintf(w, "data: {\"id\":\"ce2\",\"model\":\"srm-up\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"rest\"},\"finish_reason\":null}]}\n\n")
		_, _ = fmt.Fprintf(w, "data: [DONE]\n\n")
		w.(http.Flusher).Flush()
	})

	p := startSubprocess(t, startOpts{
		yaml:     streamRecoveryYAML(up, "    enabled: true\n    max-recoveries: 1\n    max-elapsed: 30s\n    max-partial-bytes: 4096\n"),
		logLevel: "info",
	})
	resp := openJSON(t, p.addr, "/v1/chat/completions",
		fmt.Sprintf(`{"model":"srm-model","messages":[{"role":"user","content":%q}],"stream":true}`, userSentinel),
		map[string]string{"Accept": "text/event-stream"})
	defer func() { _ = resp.Body.Close() }()
	br := bufio.NewReader(resp.Body)

	for _, want := range []string{prefixSentinel + " ", "rest"} {
		lines, eof := nextSSEEvent(t, br, 5*time.Second)
		if eof {
			t.Fatalf("stream ended before %q arrived", want)
		}
		assertChatSSE(t, lines, "srm-model", want)
	}
	lines, eof := nextSSEEvent(t, br, 5*time.Second)
	if eof || len(lines) != 1 || !strings.Contains(lines[0], "[DONE]") {
		t.Fatalf("terminal event = %q (eof=%v), want exactly one data: [DONE]", lines, eof)
	}
	// The hop's marker is the stream's last byte: the proxy adds no second
	// terminal of its own, here or after.
	if tail, _ := io.ReadAll(br); strings.Contains(string(tail), "data:") {
		t.Errorf("bytes followed the terminal marker: %q", tail)
	}
	if got := up.count(); got != 2 {
		t.Fatalf("upstream dials = %d, want 2", got)
	}
	waitForEventCount(t, p, "request_completed", 1)
	evs := completionsFor(t, p, "srm-model")
	if len(evs) != 1 || evs[0]["outcome"] != "completed" {
		t.Fatalf("completions = %v, want one completed", evs)
	}

	// The continuation body this proxy assembled carried the prefix as an
	// assistant turn — and that is a REQUEST the upstream received, never a
	// line the process wrote about itself.
	if reqs := up.requests(); len(reqs) != 2 || !strings.Contains(string(reqs[1].Body), prefixSentinel) {
		t.Fatalf("the hop did not carry the committed prefix to the upstream: %v", reqs)
	}
	if out := p.stderr.String(); strings.Contains(out, prefixSentinel) || strings.Contains(out, userSentinel) {
		t.Errorf("the process logged client-visible text: %s", out)
	}
}

// TestE2EStreamRecoveryRefusesAnEmptyPrefix pins the row no heuristic may
// guess its way past: a committed stream that carries no assistant TEXT is not
// a truncated answer, it is a stream this proxy has nothing to continue from.
// Re-asking the upstream here would replay the whole request wearing a
// continuation's shape — the model would answer from nothing and the client
// would receive a second answer spliced onto a live stream. One dial, no hop,
// and the refusal names the reason.
func TestE2EStreamRecoveryRefusesAnEmptyPrefix(t *testing.T) {
	up := newFakeUpstream(t)
	up.setHandler(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		// A role announcement and a finish_reason-free usage chunk: the shape
		// of a stream that committed and produced no text yet.
		_, _ = fmt.Fprint(w, "data: {\"id\":\"ep1\",\"model\":\"srm-up\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\"},\"finish_reason\":null}]}\n\n")
		w.(http.Flusher).Flush()
	})

	p := startSubprocess(t, startOpts{
		yaml:     streamRecoveryYAML(up, "    enabled: true\n    max-recoveries: 2\n    max-elapsed: 30s\n    max-partial-bytes: 4096\n"),
		logLevel: "info",
	})
	resp := openJSON(t, p.addr, "/v1/chat/completions", chatStreamRequest("srm-model"),
		map[string]string{"Accept": "text/event-stream"})
	defer func() { _ = resp.Body.Close() }()
	br := bufio.NewReader(resp.Body)

	// The role delta reaches the client unchanged; the gate decides whether to
	// CONTINUE, it never edits a stream in flight.
	lines, eof := nextSSEEvent(t, br, 5*time.Second)
	if eof || len(lines) != 1 || !strings.Contains(lines[0], `"role":"assistant"`) {
		t.Fatalf("role event = %q (eof=%v), want one relayed unchanged", lines, eof)
	}
	text, terminal := drainSSE(t, br, 10*time.Second)
	if terminal {
		t.Errorf("an empty prefix produced a terminal marker: %q", text)
	}
	if got := up.count(); got != 1 {
		t.Fatalf("upstream dials = %d, want 1 — a stream with no text is not continued", got)
	}
	waitForEventCount(t, p, "stream_truncated", 1)
	logs := parseLogEvents(t, p.stderr.String())
	exh := eventsWithMessage(logs, "stream_recovery_exhausted")
	if len(exh) != 1 || exh[0]["reason"] != "unsafe_content" || exh[0]["unsafe_reason"] != "no_prefix" {
		t.Fatalf("refusal = %v, want unsafe_content/no_prefix", exh)
	}
	if got := len(eventsWithMessage(logs, "stream_recovery_started")); got != 0 {
		t.Errorf("stream_recovery_started events = %d, want 0", got)
	}
}

// TestE2EStreamRecoveryHopFailureIsBounded pins what a FAILED continuation
// costs and what it reports. A hop that dies, is answered with a status, or is
// answered with something that is not an event stream must not become a retry
// loop: each is recorded once, under the phase that failed, and the client's
// stream ends exactly where it stood. `max-recoveries: 1` is deliberate — the
// reach is one hop, so a failed hop is also the end of the reach, and the
// assertion that no third dial ever happens is the assertion that a failure
// never buys itself another attempt.
func TestE2EStreamRecoveryHopFailureIsBounded(t *testing.T) {
	cases := []struct {
		name string
		// hop answers the SECOND dial.
		hop       func(w http.ResponseWriter)
		wantPhase string
		// wantStatus is the hop's own status when the phase reports one.
		wantStatus float64
		// wantBounds is the bound the loop stopped on, when a failed hop
		// leaves it with one. A hop that never produced a stream is a
		// refusal the loop stops AT — the failure is the whole report, and
		// the stream truncates with no `recovery_reason` because no bound was
		// reached. A hop that DIED MID-STREAM is the one shape where the
		// loop re-evaluates — the failure is recorded either way, and the
		// loop's own bounds (here: the reach) are what decide whether the
		// stream is worth continuing again. Both readings are pinned below.
		wantBounds string
	}{
		{
			name: "the hop dies mid-stream",
			hop: func(w http.ResponseWriter) {
				w.Header().Set("Content-Type", "text/event-stream")
				w.WriteHeader(http.StatusOK)
				_, _ = fmt.Fprintf(w, "data: {\"id\":\"hf2\",\"model\":\"srm-up\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"two\"},\"finish_reason\":null}]}\n\n")
				w.(http.Flusher).Flush()
				panic(http.ErrAbortHandler)
			},
			wantPhase:  "upstream_read",
			wantBounds: "max_recoveries",
		},
		{
			name: "the hop is answered with a status",
			hop: func(w http.ResponseWriter) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusTooManyRequests)
				_, _ = fmt.Fprint(w, `{"error":{"message":"slow down","type":"rate_limit_error"}}`)
			},
			wantPhase:  "upstream_status",
			wantStatus: 429,
		},
		{
			name: "the hop is answered with a non-stream 2xx",
			hop: func(w http.ResponseWriter) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)
				_, _ = fmt.Fprint(w, `{"id":"hf2","object":"chat.completion"}`)
			},
			wantPhase:  "upstream_status",
			wantStatus: 200,
		},
		{
			name: "the hop's connection dies before it answers",
			hop: func(w http.ResponseWriter) {
				conn, _, err := w.(http.Hijacker).Hijack()
				if err != nil {
					t.Errorf("hijack: %v", err)
					return
				}
				_ = conn.Close()
			},
			wantPhase: "dial",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			up := newFakeUpstream(t)
			up.setHandler(func(w http.ResponseWriter, _ *http.Request) {
				if up.count() > 1 {
					tc.hop(w)
					return
				}
				w.Header().Set("Content-Type", "text/event-stream")
				w.WriteHeader(http.StatusOK)
				_, _ = fmt.Fprintf(w, "data: {\"id\":\"hf1\",\"model\":\"srm-up\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"half \"},\"finish_reason\":null}]}\n\n")
				w.(http.Flusher).Flush()
				panic(http.ErrAbortHandler)
			})

			p := startSubprocess(t, startOpts{
				yaml:     streamRecoveryYAML(up, "    enabled: true\n    max-recoveries: 1\n    max-elapsed: 30s\n    max-partial-bytes: 4096\n"),
				logLevel: "info",
			})
			resp := openJSON(t, p.addr, "/v1/chat/completions", chatStreamRequest("srm-model"),
				map[string]string{"Accept": "text/event-stream"})
			defer func() { _ = resp.Body.Close() }()
			br := bufio.NewReader(resp.Body)

			lines, eof := nextSSEEvent(t, br, 5*time.Second)
			if eof {
				t.Fatal("the committed fragment never arrived")
			}
			assertChatSSE(t, lines, "srm-model", "half ")

			// The stream ends. Whatever the hop did or did not deliver, the
			// proxy never invents the marker that would claim it finished.
			text, terminal := drainSSE(t, br, 10*time.Second)
			if terminal {
				t.Errorf("a failed hop produced a terminal marker at the client: %q", text)
			}

			waitForEventCount(t, p, "stream_truncated", 1)
			logs := parseLogEvents(t, p.stderr.String())
			if got := len(eventsWithMessage(logs, "stream_recovery_started")); got != 1 {
				t.Errorf("stream_recovery_started events = %d, want 1", got)
			}
			failed := eventsWithMessage(logs, "stream_recovery_failed")
			if len(failed) != 1 {
				t.Fatalf("stream_recovery_failed events = %v, want exactly one", failed)
			}
			if failed[0]["phase"] != tc.wantPhase {
				t.Errorf("phase = %v, want %v", failed[0]["phase"], tc.wantPhase)
			}
			if tc.wantStatus != 0 && failed[0]["upstream_status"] != tc.wantStatus {
				t.Errorf("upstream_status = %v, want %v", failed[0]["upstream_status"], tc.wantStatus)
			}
			if got := len(eventsWithMessage(logs, "stream_recovery_succeeded")); got != 0 {
				t.Errorf("stream_recovery_succeeded events = %d, want 0", got)
			}
			// The reach was ONE hop and it was spent by the failure, so the
			// hop is never asked again — that is the whole point of the row.
			if got := up.count(); got != 2 {
				t.Errorf("upstream dials = %d, want 2 (one walk attempt, one hop, no retry of the hop)", got)
			}
			exh := eventsWithMessage(logs, "stream_recovery_exhausted")
			switch tc.wantBounds {
			case "":
				if len(exh) != 0 {
					t.Errorf("stream_recovery_exhausted = %v, want none: the hop's refusal is the whole report", exh)
				}
			default:
				if len(exh) != 1 || exh[0]["reason"] != tc.wantBounds {
					t.Errorf("stream_recovery_exhausted = %v, want one %s", exh, tc.wantBounds)
				}
			}
			trunc := eventsWithMessage(parseLogEvents(t, p.stderr.String()), "stream_truncated")
			if len(trunc) != 1 || trunc[0]["outcome"] == "client_disconnected" {
				t.Fatalf("stream_truncated = %v, want one non-disconnect truncation", trunc)
			}
			if got, has := trunc[0]["recovery_reason"]; has != (tc.wantBounds != "") {
				t.Errorf("stream_truncated recovery_reason = %v (present=%v), want %q", got, has, tc.wantBounds)
			} else if has && got != tc.wantBounds {
				t.Errorf("stream_truncated recovery_reason = %v, want %v", got, tc.wantBounds)
			}
			evs := completionsFor(t, p, "srm-model")
			if len(evs) != 1 || evs[0]["outcome"] != "stream_truncated" {
				t.Fatalf("completions = %v, want one stream_truncated", evs)
			}
			// One candidate, two attempts: the failed hop is a real outbound
			// exchange on the SAME provider, and it never moves the walk.
			if got := evs[0]["candidates_entered"]; got != float64(1) {
				t.Errorf("candidates_entered = %v, want 1", got)
			}
			if got := evs[0]["provider_attempts"]; got != float64(2) {
				t.Errorf("provider_attempts = %v, want 2", got)
			}
		})
	}
}

// TestE2EStreamRecoveryResponsesHopIdentity is the hop boundary black-box, on
// the surface the defect was reported against. A continuation hop is a NEW
// upstream response, so it announces its own output item and its own item_id
// — which is what every real upstream emits for a new response. The proxy
// must join the hop's text to the committed prefix anyway, and it must be
// able to do it TWICE: the identity was scoped to the logical stream, so the
// second hop was refused as multiple_outputs and `max-recoveries: 2` could
// never be spent on this surface.
//
// The second dial is cut too, on purpose. A hop that reaches its own terminal
// marker ends the loop on the marker, before the accumulator's verdict is
// consulted, so a single-hop test passes whether or not the boundary is
// right; a cut hop is what forces the verdict to be read before the next dial
// is made.
func TestE2EStreamRecoveryResponsesHopIdentity(t *testing.T) {
	hops := []string{
		`{"type":"response.output_text.delta","item_id":"m1","output_index":0,"content_index":0,"delta":"Once"}`,
		`{"type":"response.output_text.delta","item_id":"m2","output_index":0,"content_index":0,"delta":" upon a"}`,
	}
	up := newFakeUpstream(t)
	up.setHandler(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		fl := w.(http.Flusher)
		// The request being served is already recorded, so the first dial
		// observes 1. Dials 1 and 2 commit a fragment each and die; the third
		// finishes the answer.
		if n := up.count(); n <= len(hops) {
			_, _ = fmt.Fprintf(w, "event: response.output_text.delta\ndata: %s\n\n", hops[n-1])
			fl.Flush()
			panic(http.ErrAbortHandler)
		}
		_, _ = fmt.Fprintf(w, "event: response.output_text.delta\ndata: %s\n\n",
			`{"type":"response.output_text.delta","item_id":"m3","output_index":0,"content_index":0,"delta":" time"}`)
		fl.Flush()
		_, _ = fmt.Fprintf(w, "event: response.completed\ndata: {}\n\n")
		fl.Flush()
	})

	p := startSubprocess(t, startOpts{
		yaml:     streamRecoveryYAML(up, "    enabled: true\n    max-recoveries: 2\n    max-elapsed: 30s\n    max-partial-bytes: 4096\n"),
		logLevel: "info",
	})
	resp := openJSON(t, p.addr, "/v1/responses",
		`{"model":"srm-model","stream":true,"input":"tell me a story"}`,
		map[string]string{"Accept": "text/event-stream"})
	defer func() { _ = resp.Body.Close() }()
	br := bufio.NewReader(resp.Body)

	// Every hop's text reached the client, in order, on one connection.
	line, eof := nextSSEEvent(t, br, 10*time.Second)
	if eof {
		t.Fatal("the committed fragment never arrived")
	}
	if !strings.Contains(strings.Join(line, "\n"), "Once") {
		t.Fatalf("committed event = %q, want the m1 delta", line)
	}
	line, eof = nextSSEEvent(t, br, 10*time.Second)
	if eof {
		t.Fatal("no first continuation event: the hop's own item id was refused as a second output")
	}
	if !strings.Contains(strings.Join(line, "\n"), " upon a") {
		t.Fatalf("first hop event = %q, want the m2 delta", line)
	}
	line, eof = nextSSEEvent(t, br, 10*time.Second)
	if eof {
		t.Fatal("no second continuation event: max-recoveries was never spendable past one hop")
	}
	if !strings.Contains(strings.Join(line, "\n"), " time") {
		t.Fatalf("second hop event = %q, want the m3 delta", line)
	}
	line, eof = nextSSEEvent(t, br, 10*time.Second)
	if eof {
		t.Fatal("stream ended without a terminal marker")
	}
	if !strings.Contains(strings.Join(line, "\n"), "event: response.completed") {
		t.Fatalf("terminal event = %q, want the one response.completed", line)
	}
	// A stream is terminated once: the marker is the last thing on the wire,
	// and nothing this proxy invented follows it.
	if tail, _ := io.ReadAll(br); strings.Contains(string(tail), "event:") || strings.Contains(string(tail), "data:") {
		t.Errorf("bytes followed the terminal marker: %q", tail)
	}
	if got := up.count(); got != 3 {
		t.Fatalf("upstream dials = %d, want 3 (the committed pass and two hops)", got)
	}

	waitForEventCount(t, p, "request_completed", 1)
	evs := completionsFor(t, p, "srm-model")
	if len(evs) != 1 || evs[0]["outcome"] != "completed" {
		t.Fatalf("completions = %v, want one completed", evs)
	}
	if got := evs[0]["provider_attempts"]; got != float64(3) {
		t.Errorf("provider_attempts = %v, want 3", got)
	}
	logs := parseLogEvents(t, p.stderr.String())
	if got := len(eventsWithMessage(logs, "stream_recovery_started")); got != 2 {
		t.Errorf("stream_recovery_started events = %d, want 2", got)
	}
	if got := len(eventsWithMessage(logs, "stream_recovery_succeeded")); got != 1 {
		t.Errorf("stream_recovery_succeeded events = %d, want 1", got)
	}
	if got := eventsWithMessage(logs, "stream_recovery_exhausted"); len(got) != 0 {
		t.Errorf("stream_recovery_exhausted = %v, want none", got)
	}
	if got := len(eventsWithMessage(logs, "stream_truncated")); got != 0 {
		t.Errorf("stream_truncated events = %d, want 0", got)
	}
}

// TestE2EStreamRecoveryResponsesIdentityFailsClosed is the Responses
// continuation contract on the real wire: the MVP continues plain text from
// exactly one message output's exactly one content stream. A stream whose
// deltas disagree about which output they belong to is refused rather than
// concatenated — splicing two answers into one assistant turn would hand the
// model a conversation that never happened, which is worse than the
// truncation it replaces.
func TestE2EStreamRecoveryResponsesIdentityFailsClosed(t *testing.T) {
	const (
		first  = `{"type":"response.output_text.delta","item_id":"m1","output_index":0,"content_index":0,"delta":"Once"}`
		second = `{"type":"response.output_text.delta","item_id":"m2","output_index":1,"content_index":0,"delta":" upon a time"}`
	)
	up := newFakeUpstream(t)
	up.setHandler(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		fl := w.(http.Flusher)
		for _, payload := range []string{first, second} {
			_, _ = fmt.Fprintf(w, "event: %s\ndata: %s\n\n", "response.output_text.delta", payload)
			fl.Flush()
		}
		panic(http.ErrAbortHandler)
	})

	p := startSubprocess(t, startOpts{
		yaml:     streamRecoveryYAML(up, "    enabled: true\n    max-recoveries: 2\n    max-elapsed: 30s\n    max-partial-bytes: 4096\n"),
		logLevel: "info",
	})
	resp := openJSON(t, p.addr, "/v1/responses",
		`{"model":"srm-model","stream":true,"input":"tell me a story"}`,
		map[string]string{"Accept": "text/event-stream"})
	defer func() { _ = resp.Body.Close() }()
	br := bufio.NewReader(resp.Body)

	// Both deltas reach the client — the gate decides whether to CONTINUE, it
	// never edits the stream in flight.
	text, terminal := drainSSE(t, br, 10*time.Second)
	for _, must := range []string{"Once", " upon a time"} {
		if !strings.Contains(text, must) {
			t.Fatalf("the relayed stream is missing %q: %s", must, text)
		}
	}
	if terminal {
		t.Errorf("a refused stream carried a terminal marker: %q", text)
	}
	if got := up.count(); got != 1 {
		t.Fatalf("upstream dials = %d, want 1 — a multi-output stream is not continued", got)
	}
	waitForEventCount(t, p, "stream_truncated", 1)
	logs := parseLogEvents(t, p.stderr.String())
	exh := eventsWithMessage(logs, "stream_recovery_exhausted")
	if len(exh) != 1 {
		t.Fatalf("stream_recovery_exhausted = %v, want one", exh)
	}
	if exh[0]["reason"] != "unsafe_content" || exh[0]["unsafe_reason"] != "multiple_outputs" {
		t.Errorf("refusal = %v/%v, want unsafe_content/multiple_outputs", exh[0]["reason"], exh[0]["unsafe_reason"])
	}
	if got := len(eventsWithMessage(logs, "stream_recovery_started")); got != 0 {
		t.Errorf("stream_recovery_started events = %d, want 0", got)
	}
}

// TestE2EStreamRecoveryEnabledWindowBoundsTheCommittedRelay is the other half
// of the hard-window test, and the row that keeps the window from being read
// as "a budget for recovery hops". When the block is ENABLED, `max-elapsed`
// is one wall-clock window over the whole committed stream: it is measured
// from the commit, and the watchdog closes the upstream body at the deadline
// whether or not anything is ever cut. So on the feature's own configuration a
// slow generation is cut with `max_elapsed` — not `max_recoveries`, not an
// upstream failure, and never a synthesized terminal marker. An operator who
// needs longer generations sets a longer window; this pins that the window is
// genuinely hard rather than a number consulted only between recovery attempts.
func TestE2EStreamRecoveryEnabledWindowBoundsTheCommittedRelay(t *testing.T) {
	up := newFakeUpstream(t)
	hold := make(chan struct{})
	t.Cleanup(func() { close(hold) })
	up.setHandler(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		fl := w.(http.Flusher)
		_, _ = fmt.Fprintf(w, "data: {\"id\":\"st1\",\"model\":\"srm-up\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"thinking\"},\"finish_reason\":null}]}\n\n")
		fl.Flush()
		<-hold
	})

	p := startSubprocess(t, startOpts{
		yaml:     streamRecoveryYAML(up, "    enabled: true\n    max-recoveries: 2\n    max-elapsed: 2s\n    max-partial-bytes: 4096\n"),
		logLevel: "info",
	})
	resp := openJSON(t, p.addr, "/v1/chat/completions", chatStreamRequest("srm-model"),
		map[string]string{"Accept": "text/event-stream"})
	defer func() { _ = resp.Body.Close() }()
	br := bufio.NewReader(resp.Body)

	lines, eof := nextSSEEvent(t, br, 5*time.Second)
	if eof {
		t.Fatal("the committed fragment never arrived")
	}
	assertChatSSE(t, lines, "srm-model", "thinking")

	text, terminal := drainSSE(t, br, 20*time.Second)
	if terminal {
		t.Errorf("a window-cut stream carried a terminal marker: %q", text)
	}
	if got := up.count(); got != 1 {
		t.Fatalf("upstream dials = %d, want 1 — the window closed the body before any hop", got)
	}
	waitForEventCount(t, p, "stream_truncated", 1)
	logs := parseLogEvents(t, p.stderr.String())
	exh := eventsWithMessage(logs, "stream_recovery_exhausted")
	if len(exh) != 1 || exh[0]["reason"] != "max_elapsed" {
		t.Fatalf("stream_recovery_exhausted = %v, want one max_elapsed refusal", exh)
	}
	trunc := eventsWithMessage(logs, "stream_truncated")
	if len(trunc) != 1 || trunc[0]["recovery_reason"] != "max_elapsed" {
		t.Fatalf("stream_truncated = %v, want one with recovery_reason max_elapsed", trunc)
	}
	// The window is the operator's bound, not a diagnosis about the peer: no
	// invented error, and not the outcome a broken upstream or a departed
	// client would have produced.
	if _, has := trunc[0]["error"]; has {
		t.Errorf("the window reported an upstream error it invented: %v", trunc[0])
	}
	if trunc[0]["outcome"] == "client_disconnected" {
		t.Errorf("the window was reported as a client disconnect: %v", trunc[0])
	}
	evs := completionsFor(t, p, "srm-model")
	if len(evs) != 1 || evs[0]["outcome"] != "stream_truncated" {
		t.Fatalf("completions = %v, want one stream_truncated", evs)
	}
}
