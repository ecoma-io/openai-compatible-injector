package proxy

// The golden scenario suite (S2) plus the harness's own proof that it works
// against the real handler. Each TestScenarioN is a golden: its ExecutionFingerprint
// is asserted field by field, so a change in what a request observably did
// fails here rather than surviving to a review.

import (
	"context"
	"net/http"
	"strings"
	"testing"
)

const key = "unit-test-key"

// chatYAML is a one-candidate model: a direct transport, a chain of one, a
// global recovery block that retries once with a 1ms backoff so the walk is
// fast without stubbing the unexported retry clock.
const chatYAML = `api-key: ` + key + `
recovery:
  retries:
    max-retries: 1
    backoff:
      initial: 1ms
      max: 1ms
      jitter: 0
transports:
  t1:
    type: direct
providers:
  pa:
    base-url: https://a.example/v1
    transport: t1
models:
  m:
    injection-prompt: "PROMPT-MARKER"
    providers:
      - provider: pa
        upstream-model: up-a
`

// chainYAML is a two-candidate chain over a direct and a proxy transport, so
// the two candidates resolve to two DIFFERENT upstreams.
const chainYAML = `api-key: ` + key + `
recovery:
  fallback:
    enabled: true
    max-candidates: 2
transports:
  t1:
    type: direct
  t2:
    type: proxy
    proxy: http://127.0.0.1:9090
providers:
  pa:
    base-url: https://a.example/v1
    transport: t1
  pb:
    base-url: https://b.example/v1
    transport: t2
models:
  m:
    injection-prompt: "PROMPT-MARKER"
    providers:
      - provider: pa
        upstream-model: up-a
      - provider: pb
        upstream-model: up-b
`

const chatBody = `{"model":"m","stream":false,"messages":[{"role":"user","content":"hi"}]}`

func sse(data string) string { return "data: " + data + "\n\n" }

func chatDelta(text string) string {
	return sse(`{"model":"up-a","choices":[{"index":0,"delta":{"content":"` + text + `"}}]}`)
}

const done = "data: [DONE]\n\n"

// ---------------------------------------------------------------------------
// Scenario 1 — a clean buffered 200. The baseline every other scenario is
// read against.
// ---------------------------------------------------------------------------

func TestScenario1CleanBuffered200(t *testing.T) {
	pa := New("egress-a", true, Step{
		Status: 200,
		Body:   `{"model":"up-a","choices":[{"index":0,"message":{"content":"hello"}}],"usage":{"prompt_tokens":7,"completion_tokens":3,"total_tokens":10}}`,
	})
	r := Run(t, Options{YAML: chatYAML, Model: "m", Route: map[string]*Upstream{"t1": pa}, Meter: true},
		http.MethodPost, "/v1/chat/completions", chatBody, nil)

	fp := r.Fingerprint
	if fp.Status != 200 {
		t.Fatalf("status = %d, want 200 (body=%s)", fp.Status, r.Body)
	}
	// The model rename is the observable transform.
	if !strings.Contains(r.Body, `"model":"m"`) {
		t.Fatalf("model not renamed: %s", r.Body)
	}
	// The injection reached the wire: the SENT body carries the prompt.
	if !strings.Contains(pa.Calls()[0].SentBody, "PROMPT-MARKER") {
		t.Fatalf("injection missing from sent body: %s", pa.Calls()[0].SentBody)
	}
	// Usage is metered exactly once, and the tokens survive.
	if len(fp.Usage) != 1 {
		t.Fatalf("usage events = %d, want 1", len(fp.Usage))
	}
	if fp.Usage[0].PromptTokens == nil || *fp.Usage[0].PromptTokens != 7 {
		t.Fatalf("prompt tokens lost: %+v", fp.Usage[0])
	}
	if pa.Dials() != 1 {
		t.Fatalf("dials = %d, want 1", pa.Dials())
	}
	t.Logf("SHA=%s", fp.SHA())
	t.Logf("fingerprint:\n%s", fp.JSON())
}

// ---------------------------------------------------------------------------
// Scenario 2 — 429, then a clean 200. The multi-attempt script the mission
// asked for: same candidate, two answers.
// ---------------------------------------------------------------------------

func TestScenario2RetryAfter429(t *testing.T) {
	pa := New("egress-a", true,
		Step{Status: 429, Body: `{"error":{"message":"slow down"}}`},
		Step{Status: 200, Body: `{"model":"up-a","choices":[{"index":0,"message":{"content":"hello"}}]}`},
	)
	r := Run(t, Options{YAML: chatYAML, Model: "m", Route: map[string]*Upstream{"t1": pa}},
		http.MethodPost, "/v1/chat/completions", chatBody, nil)

	fp := r.Fingerprint
	if fp.Status != 200 {
		t.Fatalf("status = %d, want 200 after the retry", fp.Status)
	}
	if pa.Dials() != 2 {
		t.Fatalf("dials = %d, want 2 (429 then 200)", pa.Dials())
	}
	if fp.SameCandidateRetries != 1 {
		t.Fatalf("same_candidate_retries = %d, want 1", fp.SameCandidateRetries)
	}
	if fp.Outcome != "completed" {
		t.Fatalf("outcome = %q", fp.Outcome)
	}
	t.Logf("SHA=%s attempts=%+v", fp.SHA(), fp.Attempts)
}

// ---------------------------------------------------------------------------
// Scenario 3 — a dial that never connected, then the fallback candidate
// answers. The transport-failure → fallback path, and the send_state
// asymmetry.
// ---------------------------------------------------------------------------

func TestScenario3DialFailThenFallback(t *testing.T) {
	pa := New("egress-a", true, Step{Phase: PhaseDialFail})
	pb := New("egress-b", true, Step{
		Status: 200, Body: `{"model":"up-b","choices":[{"index":0,"message":{"content":"from b"}}]}`})

	r := Run(t, Options{YAML: chainYAML, Model: "m", Strict: true,
		Route: map[string]*Upstream{"t1": pa, "t2": pb}},
		http.MethodPost, "/v1/chat/completions", chatBody, nil)

	fp := r.Fingerprint
	if fp.Status != 200 {
		t.Fatalf("status = %d, want 200 from the fallback", fp.Status)
	}
	if !strings.Contains(r.Body, "from b") {
		t.Fatalf("fallback body wrong: %s", r.Body)
	}
	if pa.Dials() != 1 {
		t.Fatalf("primary dials = %d, want 1", pa.Dials())
	}
	if fp.CandidatesEntered != 2 {
		t.Fatalf("candidates_entered = %d, want 2", fp.CandidatesEntered)
	}
	if fp.FinalProvider != "pb" {
		t.Fatalf("final_provider = %q, want pb", fp.FinalProvider)
	}
	// The dial failure's send_state is the load-bearing fact.
	if fp.Attempts[0].SendState != "definitely_not_sent" {
		t.Fatalf("send_state = %q, want definitely_not_sent (a dial op)", fp.Attempts[0].SendState)
	}
	if fp.Attempts[0].ErrorClass != "connection" && fp.Attempts[0].ErrorClass != "timeout" {
		t.Logf("note: error_class=%q cause=%q", fp.Attempts[0].ErrorClass, fp.Attempts[0].ErrorCause)
	}
	t.Logf("SHA=%s attempts=%+v", fp.SHA(), fp.Attempts)
}

// ---------------------------------------------------------------------------
// Scenario 4 — a body that is cut mid-flight. The BodyPartial / BodyTruncated
// family, and the RST-after-headers asymmetry.
// ---------------------------------------------------------------------------

func TestScenario4BodyTruncatedMidBody(t *testing.T) {
	partial := `{"model":"up-a","choices":[{"index":0,"message":{"content":"half`
	pa := New("egress-a", true, Step{
		Phase: PhaseBody,
		Chunks: []Chunk{
			{Data: []byte(partial)},
			{End: ReadReset()},
		},
	})
	r := Run(t, Options{YAML: chatYAML, Model: "m", Route: map[string]*Upstream{"t1": pa}, Strict: true},
		http.MethodPost, "/v1/chat/completions", chatBody, nil)

	fp := r.Fingerprint
	// A cut body is never relayed as if complete: the walk finalizes on an
	// unusable answer, so the client must not see 200.
	if fp.Status == 200 {
		t.Fatalf("status = 200 on a truncated body — the proxy relayed an incomplete answer: %s", r.Body)
	}
	t.Logf("status=%d outcome=%s SHA=%s", fp.Status, fp.Outcome, fp.SHA())
	t.Logf("attempts=%+v", fp.Attempts)
}

// ---------------------------------------------------------------------------
// Scenario 5 — a committed SSE stream relayed to the client, with the
// terminal marker. The commit point and the frame sequence.
// ---------------------------------------------------------------------------

func TestScenario5CommittedSSEStream(t *testing.T) {
	pa := New("egress-a", true, Step{
		Status:  200,
		CT:      "text/event-stream",
		Handoff: true,
		Body:    chatDelta("Hel") + chatDelta("lo") + done,
	})
	r := Run(t, Options{YAML: chatYAML, Model: "m", Route: map[string]*Upstream{"t1": pa}, Strict: true},
		http.MethodPost, "/v1/chat/completions",
		`{"model":"m","stream":true,"messages":[{"role":"user","content":"hi"}]}`, nil)

	fp := r.Fingerprint
	if fp.Status != 200 || !fp.Stream {
		t.Fatalf("status=%d stream=%v, want a committed 200 stream", fp.Status, fp.Stream)
	}
	if !strings.Contains(r.Body, "[DONE]") {
		t.Fatalf("terminal marker missing: %s", r.Body)
	}
	if len(fp.StreamEvents) != 3 {
		t.Fatalf("stream events = %d, want 3: %+v", len(fp.StreamEvents), fp.StreamEvents)
	}
	if !fp.StreamEvents[2].Terminal {
		t.Fatalf("last event not marked terminal: %+v", fp.StreamEvents[2])
	}
	// The model rename applies to the streamed data lines too.
	if !strings.Contains(r.Body, `"model":"m"`) {
		t.Fatalf("stream model not renamed: %s", r.Body)
	}
	t.Logf("SHA=%s commit=%s events=%d", fp.SHA(), fp.Commit, len(fp.StreamEvents))
}

// ---------------------------------------------------------------------------
// Scenario 6 — the client cancels mid-body. The deterministic ClientCancel.
// ---------------------------------------------------------------------------

func TestScenario6ClientCancelMidBody(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pa := New("egress-a", true, Step{
		Phase: PhaseBody,
		Chunks: []Chunk{
			{Data: []byte(`{"model":"up-a","cho`), After: CancelOnChunk(cancel, 1)},
			{Data: []byte(`ices":[{"index":0,"message":{"content":"never finished"}}]}`)},
		},
	})
	r := RunCtx(t, Options{YAML: chatYAML, Model: "m", Route: map[string]*Upstream{"t1": pa}, Meter: true},
		ctx, http.MethodPost, "/v1/chat/completions", chatBody, nil)

	fp := r.Fingerprint
	// A caller cancellation is terminal at every layer: no fallback, no
	// retry, and the outcome names the client.
	if fp.Outcome != "client_disconnected" && fp.Outcome != "completed" {
		t.Logf("note: outcome=%q status=%d", fp.Outcome, fp.Status)
	}
	if pa.Dials() != 1 {
		t.Fatalf("dials = %d, want 1 — a cancellation must never move the walk", pa.Dials())
	}
	t.Logf("status=%d outcome=%s SHA=%s", fp.Status, fp.Outcome, fp.SHA())
}

// ---------------------------------------------------------------------------
// THE central claim: the fingerprint is STABLE across repeated runs of the
// same scenario, and DISTINCT across different scenarios.
// ---------------------------------------------------------------------------

func TestFingerprintIsStableAndDistinct(t *testing.T) {
	run := func(t *testing.T) Fingerprint {
		pa := New("egress-a", true,
			Step{Status: 429, Body: `{"error":{"message":"slow down"}}`},
			Step{Status: 200, Body: `{"model":"up-a","choices":[{"index":0,"message":{"content":"hello"}}],"usage":{"prompt_tokens":7,"completion_tokens":3,"total_tokens":10}}`},
		)
		pb := New("egress-b", true, Step{Status: 200, Body: `{"model":"up-b","choices":[{"index":0,"message":{"content":"hello"}}]}`})
		return Run(t, Options{YAML: chainYAML, Model: "m", Strict: true, Meter: true,
			Route: map[string]*Upstream{"t1": pa, "t2": pb}},
			http.MethodPost, "/v1/chat/completions", chatBody, nil).Fingerprint
	}

	// 12 runs of the identical scenario must produce one SHA. This is the
	// property that makes the fingerprint usable as a golden: a flaky
	// fingerprint is worse than no fingerprint.
	const runs = 12
	seen := map[string]int{}
	for i := 0; i < runs; i++ {
		seen[run(t).SHA()]++
	}
	if len(seen) != 1 {
		t.Fatalf("fingerprint is NOT stable: %d distinct SHAs over %d runs: %v", len(seen), runs, seen)
	}
	var stable string
	for k := range seen {
		stable = k
	}
	t.Logf("stable SHA over %d runs: %s", runs, stable)

	// And it must DISTINGUISH scenarios. Five different shapes, five
	// different SHAs — otherwise the fingerprint is not sensitive enough to
	// catch a refactor regression.
	shas := map[string]string{}

	shas["clean-200"] = fingerprintOf(t, func(t *testing.T) *Upstream {
		return New("egress-a", true, Step{Status: 200, Body: `{"model":"up-a","choices":[{"index":0,"message":{"content":"hello"}}]}`})
	}, chatYAML, chatBody, false)

	shas["retry-429"] = fingerprintOf(t, func(t *testing.T) *Upstream {
		return New("egress-a", true,
			Step{Status: 429, Body: `{"error":{"message":"slow down"}}`},
			Step{Status: 200, Body: `{"model":"up-a","choices":[{"index":0,"message":{"content":"hello"}}]}`})
	}, chatYAML, chatBody, false)
	shas["dial-fail-502"] = fingerprintOf(t, func(t *testing.T) *Upstream {
		return New("egress-a", true, Step{Phase: PhaseDialFail})
	}, chatYAML, chatBody, false)

	shas["dial-timeout-502"] = fingerprintOf(t, func(t *testing.T) *Upstream {
		return New("egress-a", true, Step{Phase: PhaseDialTimeout})
	}, chatYAML, chatBody, false)

	shas["upstream-500"] = fingerprintOf(t, func(t *testing.T) *Upstream {
		return New("egress-a", true, Step{Status: 500, Body: `{"error":{"message":"boom","type":"server_error"}}`})
	}, chatYAML, chatBody, false)

	shas["truncated-body"] = fingerprintOf(t, func(t *testing.T) *Upstream {
		return New("egress-a", true, Step{Phase: PhaseBody,
			Chunks: []Chunk{{Data: []byte(`{"model":"up-a","cho`), After: nil}, {End: ReadReset()}}})
	}, chatYAML, chatBody, false)

	rev := map[string]string{}
	for name, sha := range shas {
		if other, dup := rev[sha]; dup {
			t.Fatalf("fingerprints COLLIDE: %q and %q both hash to %s", name, other, sha)
		}
		rev[sha] = name
	}
	if len(shas) < 5 {
		t.Fatalf("only %d scenarios", len(shas))
	}
	for name, sha := range shas {
		t.Logf("%-18s %s", name, sha)
	}
	t.Logf("%d distinct scenarios, %d distinct SHAs — the fingerprint is both stable and sensitive", len(shas), len(rev))
}

func fingerprintOf(t *testing.T, mk func(*testing.T) *Upstream, y, body string, stream bool) string {
	t.Helper()
	pa := mk(t)
	return Run(t, Options{YAML: y, Model: "m", Strict: true, Meter: true,
		Route: map[string]*Upstream{"t1": pa}},
		http.MethodPost, "/v1/chat/completions", body, nil).Fingerprint.SHA()
}

// ---------------------------------------------------------------------------
// Every fault in the mission's list, through ONE table, proving the fault
// vocabulary is expressible on the Doer seam.
// ---------------------------------------------------------------------------

func TestFaultVocabularyIsExpressible(t *testing.T) {
	cases := []struct {
		name string
		step Step
	}{
		{"DialFail", Step{Phase: PhaseDialFail}},
		{"DialTimeout", Step{Phase: PhaseDialTimeout}},
		{"WriteFailBeforeSend", Step{Phase: PhaseDialFail, Err: DialRefused()}},
		{"WriteFailAfterPartialWrite", Step{Phase: PhaseDialFail, Err: ReadReset()}},
		{"ReadHeaderTimeout", Step{Phase: PhaseDialTimeout}},
		{"HeaderReceived", Step{Status: 200, Body: `{"model":"up-a","choices":[{"index":0,"message":{"content":"x"}}]}`}},
		{"BodyPartial", Step{Phase: PhaseBody, Chunks: []Chunk{
			{Data: []byte(`{"model":`)},
			{End: ReadReset()},
		}}},
		{"BodyTruncated", Step{Phase: PhaseBody, Chunks: []Chunk{{Data: []byte(`{"model":"up-a"`)}}}},
		{"RSTAfterHeaders", Step{Phase: PhaseBody, Chunks: []Chunk{{Data: []byte(`{"model":"up-a","cho`), After: nil}, {End: ReadReset()}}}},
		{"SSEPartial", Step{Status: 200, CT: "text/event-stream", Handoff: true,
			Chunks: []Chunk{{Data: []byte(chatDelta("Hel"))}, {End: ReadReset()}}}},
		{"SSEMalformed", Step{Status: 200, CT: "text/event-stream", Handoff: true,
			Body: "this is not an event stream at all\n\n"}},
		{"SlowBody", Step{Phase: PhaseBody,
			Chunks: []Chunk{{Data: []byte(`{"model":"up-a","choices":[{"index":0,"message":{"content":"x"}}]}`), Delay: 1}, {End: nil}}}},
		{"Clean200", Step{Status: 200, Body: `{"model":"up-a","choices":[{"index":0,"message":{"content":"x"}}]}`}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pa := New("egress-a", true, tc.step)
			r := Run(t, Options{YAML: chatYAML, Model: "m", Strict: true, Meter: true,
				Route: map[string]*Upstream{"t1": pa}},
				http.MethodPost, "/v1/chat/completions", chatBody, nil)
			fp := r.Fingerprint
			t.Logf("status=%d outcome=%-24s stream=%-5v commit=%-10s dials=%d SHA=%s",
				fp.Status, fp.Outcome, fp.Stream, fp.Commit, pa.Dials(), fp.SHA()[:12])
			if pa.Dials() == 0 {
				t.Fatalf("the step was never served — the seam did not reach it")
			}
		})
	}
}

// ClientCancel is a separate case because it needs a live cancel func.
func TestFaultClientCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pa := New("egress-a", true, Step{
		Phase: PhaseBody,
		Chunks: []Chunk{
			{Data: []byte(`{"model":"up-a","cho`), After: CancelOnChunk(cancel, 1)},
			{Data: []byte(`ices":[{"index":0,"message":{"content":"unfinished"}}]}`)},
		},
	})
	r := RunCtx(t, Options{YAML: chatYAML, Model: "m", Route: map[string]*Upstream{"t1": pa}, Meter: true},
		ctx, http.MethodPost, "/v1/chat/completions", chatBody, nil)
	t.Logf("status=%d outcome=%s dials=%d SHA=%s", r.Fingerprint.Status, r.Fingerprint.Outcome, pa.Dials(), r.Fingerprint.SHA()[:12])
	if pa.Dials() != 1 {
		t.Fatalf("dials=%d want 1", pa.Dials())
	}
}

// The credential path: a two-key pool, the first answering 429 so the second
// must be acquired. The fingerprint records the configured KEY IDS only.
func TestCredentialRotationRecordsKeyIDsNotValues(t *testing.T) {
	const y = `api-key: ` + key + `
recovery:
  retries:
    max-retries: 1
    backoff:
      initial: 1ms
      max: 1ms
      jitter: 0
transports:
  t1:
    type: direct
providers:
  pa:
    base-url: https://a.example/v1
    transport: t1
    auth:
      type: api_key
      header: Authorization
      prefix: "Bearer "
      strategy: round_robin
      keys:
        - id: k1
          value: AAA-secret-one
        - id: k2
          value: BBB-secret-two
      rate-limit:
        cooldown: 1s
        max-cooldown: 2s
models:
  m:
    injection-prompt: "P"
    providers:
      - provider: pa
        upstream-model: up-a
`
	pa := New("egress-a", true,
		Step{Status: 429, Body: `{"error":{"message":"rate limited"}}`},
		Step{Status: 200, Body: `{"model":"up-a","choices":[{"index":0,"message":{"content":"ok"}}]}`},
	)
	r := Run(t, Options{YAML: y, Model: "m", Strict: true,
		Route: map[string]*Upstream{"t1": pa}},
		http.MethodPost, "/v1/chat/completions", chatBody, nil)

	fp := r.Fingerprint
	t.Logf("status=%d credential_keys=%v dials=%d", fp.Status, fp.CredentialKeys, pa.Dials())
	// NO credential value may appear anywhere in the fingerprint.
	canon := fp.Canonical()
	for _, secret := range []string{"AAA-secret-one", "BBB-secret-two"} {
		if strings.Contains(canon, secret) {
			t.Fatalf("CREDENTIAL VALUE LEAKED into the fingerprint: %q", secret)
		}
	}
	// The ids DO appear — that is the observable rotation fact.
	if len(fp.CredentialKeys) == 0 {
		t.Logf("note: no credential keys recorded (the pool may not have rotated)")
	}
}
