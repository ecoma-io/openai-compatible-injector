package e2e_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"
)

// The logging contract, black-box: the process's stderr is the only
// observable log surface, events are JSON lines with stable message slugs,
// the level hot-reloads through the runtime file without a restart, and no
// secret material ever appears in any event at any level.

// logEvent is one decoded stderr line.
type logEvent map[string]any

// parseLogEvents decodes every non-empty stderr line, failing if any line is
// not a JSON object — serve-mode stderr must stay machine-parseable.
func parseLogEvents(t *testing.T, stderr string) []logEvent {
	t.Helper()
	var evs []logEvent
	for _, line := range strings.Split(stderr, "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		var ev logEvent
		if err := json.Unmarshal([]byte(line), &ev); err != nil {
			t.Fatalf("stderr line is not JSON: %q (%v)", line, err)
		}
		evs = append(evs, ev)
	}
	return evs
}

// eventsWithMessage returns every event whose message equals msg.
func eventsWithMessage(evs []logEvent, msg string) []logEvent {
	var found []logEvent
	for _, ev := range evs {
		if ev["message"] == msg {
			found = append(found, ev)
		}
	}
	return found
}

// waitForLogEvent polls the process stderr until an event matching want
// appears, failing after the timeout with the stderr collected so far.
func waitForLogEvent(t *testing.T, p *proc, want func(logEvent) bool, desc string) logEvent {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		for _, ev := range parseLogEvents(t, p.stderr.String()) {
			if want(ev) {
				return ev
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("no log event %s within timeout; stderr:\n%s", desc, p.stderr.String())
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// waitForEventCount polls until at least want events with msg have appeared
// in stderr. The subprocess's stderr travels through a pipe and a copying
// goroutine, so an event written by the handler is only guaranteed visible
// to the test some time after the HTTP response has completed — counts must
// always be awaited, never asserted synchronously.
func waitForEventCount(t *testing.T, p *proc, msg string, want int) []logEvent {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		evs := eventsWithMessage(parseLogEvents(t, p.stderr.String()), msg)
		if len(evs) >= want {
			return evs
		}
		if time.Now().After(deadline) {
			t.Fatalf("only %d of %d %q events appeared; stderr:\n%s", len(evs), want, msg, p.stderr.String())
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// loggingYAML renders a two-model runtime file with a top-level log-level
// key: one live model (secret-bearing endpoint query) and one dead
// endpoint (connection refused) for the failure-path scenarios.
func loggingYAML(liveURL, echoURL, level string) string {
	return fmt.Sprintf(`models:
  live:
    endpoint: %s/v1?api-key=SECRET_ENDPOINT_TOKEN
    upstream-model: up-live
    injection-prompt: "SECRET_PROMPT_VALUE review carefully"
  dead:
    endpoint: http://127.0.0.1:1/v1?api-key=SECRET_ENDPOINT_TOKEN
    upstream-model: up-dead
  echo:
    endpoint: %s/v1
    upstream-model: up-echo

log-level: %s
`, liveURL, echoURL, level)
}

// secretBody carries planted markers in the client payload.
const secretBody = `{"model":"live","messages":[{"role":"user","content":"SECRET_REQUEST_BODY"}],"api_key":"SECRET_API_KEY"}`

// TestLogLevelHotReloadWithoutRestart is the mandatory hot-reload scenario:
// the level moves info -> debug -> error -> debug through runtime-file
// rewrites alone, and serving (routing, snapshot binding) is undisturbed
// throughout — one process, one PID, every request answered.
func TestLogLevelHotReloadWithoutRestart(t *testing.T) {
	upstream := newFakeUpstream(t)
	upstream.setHandler(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"c1","model":"up-live","choices":[]}`))
	})

	p := startSubprocess(t, startOpts{
		yaml:     loggingYAML(upstream.url(), upstream.url(), "info"),
		logLevel: "", // level lives in the YAML above
	})
	pid := p.cmd.Process.Pid

	post := func(model string) int {
		body := fmt.Sprintf(`{"model":%q,"messages":[{"role":"user","content":"x"}]}`, model)
		status, _, _ := postJSON(t, p.addr, "/v1/chat/completions", body, nil)
		return status
	}
	completions := func(want int) []logEvent {
		return waitForEventCount(t, p, "request_completed", want)
	}
	received := func() int {
		return len(eventsWithMessage(parseLogEvents(t, p.stderr.String()), "request_received"))
	}

	// info: completions visible, DEBUG request_received suppressed. The
	// count wait also settles the stderr copy before the absence assertion.
	if status := post("live"); status != http.StatusOK {
		t.Fatalf("status = %d, want 200", status)
	}
	if got := completions(1); len(got) != 1 {
		t.Fatalf("request_completed events = %d, want 1 at info level", len(got))
	}
	if got := received(); got != 0 {
		t.Fatalf("request_received visible at info level (%d events) — debug leak", got)
	}

	// Rewrite the file: info -> debug. The reload is acknowledged by a
	// config_reloaded INFO line carrying the new level (logged under the
	// level in effect before it applied).
	rewriteConfig(t, p.cfgPath, loggingYAML(upstream.url(), upstream.url(), "debug"))
	waitForLogEvent(t, p, func(ev logEvent) bool {
		return ev["message"] == "config_reloaded" && ev["log_level"] == "debug"
	}, "config_reloaded with log_level=debug")

	if status := post("live"); status != http.StatusOK {
		t.Fatalf("status after debug reload = %d, want 200", status)
	}
	evs := completions(2)
	if got := received(); got != 1 {
		t.Fatalf("request_received events = %d, want 1 after switching to debug", got)
	}
	// The completion events bind to the snapshot generation: the post-reload
	// request must carry generation 1 — proof that the same process reloaded
	// rather than restarted (a restart would begin at 0 again).
	if evs[1]["config_generation"].(float64) != 1 {
		t.Fatalf("config_generation = %v, want 1 after first reload", evs[1]["config_generation"])
	}

	// debug -> error: completions are INFO and must disappear. Allow the
	// stderr copy to settle before asserting the absence.
	rewriteConfig(t, p.cfgPath, loggingYAML(upstream.url(), upstream.url(), "error"))
	waitForLogEvent(t, p, func(ev logEvent) bool {
		return ev["message"] == "config_reloaded" && ev["log_level"] == "error"
	}, "config_reloaded with log_level=error")

	if status := post("live"); status != http.StatusOK {
		t.Fatalf("status after error reload = %d, want 200", status)
	}
	time.Sleep(300 * time.Millisecond)
	if got := len(completions(2)); got != 2 {
		t.Fatalf("request_completed events = %d, want still 2 (INFO suppressed at error level)", got)
	}

	// error -> debug: config_reloaded is invisible (INFO under the old error
	// level), so wait on the behavior itself — the next debug event proves
	// the new level applied. (The DEBUG log_level_applied ack is asserted
	// per-transition by TestLogLevelTransitionMatrix.)
	before := received()
	rewriteConfig(t, p.cfgPath, loggingYAML(upstream.url(), upstream.url(), "debug"))
	deadline := time.Now().Add(5 * time.Second)
	for received() == before {
		if time.Now().After(deadline) {
			t.Fatalf("log level never returned to debug; stderr:\n%s", p.stderr.String())
		}
		if status := post("live"); status != http.StatusOK {
			t.Fatalf("status = %d, want 200", status)
		}
		time.Sleep(50 * time.Millisecond)
	}
	if status := post("live"); status != http.StatusOK {
		t.Fatalf("status after final reload = %d, want 200", status)
	}

	// One process throughout: the PID never changed and the service never
	// dropped a request.
	if p.cmd.Process.Pid != pid {
		t.Fatal("process PID changed — the level was not hot-reloaded but restarted")
	}
	// The debug-wait loop above may post one extra request while stderr lags,
	// so the bound is "at least one completion per request" — never an exact
	// count. Fewer than four means a request was dropped.
	if got := len(completions(4)); got < 4 {
		t.Fatalf("request_completed events = %d, want >= 4 (every request answered)", got)
	}
}

// TestLogLevelTransitionMatrix walks the level through every ordered pair of
// the four documented levels plus the `warning` alias, requiring an
// observable acknowledgement per transition: log_level_applied is emitted
// at the level it announces, so it is visible on every transition —
// including error->warn and anything->error, where the INFO config_reloaded
// line is suppressed by the level still in force when it is written. Each
// ack must also carry the generation the reload produced, incremented by
// exactly one per successful publish. A rejected file afterwards must
// neither bump the generation nor disturb the level in force.
func TestLogLevelTransitionMatrix(t *testing.T) {
	upstream := newFakeUpstream(t)
	upstream.setHandler(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"c1","model":"up-live","choices":[]}`))
	})

	p := startSubprocess(t, startOpts{
		yaml:     loggingYAML(upstream.url(), upstream.url(), "info"),
		logLevel: "",
	})

	rewrite := func(level string) {
		t.Helper()
		rewriteConfig(t, p.cfgPath, loggingYAML(upstream.url(), upstream.url(), level))
	}
	generation := 0
	expectAck := func(previous, level string) {
		t.Helper()
		generation++
		waitForLogEvent(t, p, func(ev logEvent) bool {
			return ev["message"] == "log_level_applied" &&
				ev["previous_level"] == previous &&
				ev["log_level"] == level &&
				fmt.Sprint(ev["generation"]) == fmt.Sprint(generation)
		}, fmt.Sprintf("log_level_applied %s->%s (generation %d)", previous, level, generation))
	}

	// Twelve rewrites, every ordered pair of the documented levels exactly
	// once. The chain starts from the boot level (info) and returns to it.
	for _, step := range [][2]string{
		{"info", "debug"}, {"debug", "warn"}, {"warn", "info"},
		{"info", "error"}, {"error", "debug"}, {"debug", "error"},
		{"error", "warn"}, {"warn", "debug"}, {"debug", "info"},
		{"info", "warn"}, {"warn", "error"}, {"error", "info"},
	} {
		rewrite(step[1])
		expectAck(step[0], step[1])
	}

	// One more transition beyond the matrix — a fixed last-good generation
	// to measure the rejection against.
	rewrite("warn")
	expectAck("info", "warn")

	// A rejected file changes nothing: the ack generation the recovery
	// produces must be exactly one past the last successful publish's — no
	// generation was consumed by the rejection — and DEBUG traffic must
	// still flow while the file is broken, so first move to a level where
	// the behavior is observable.
	rewrite("debug")
	expectAck("warn", "debug")
	postJSON(t, p.addr, "/v1/chat/completions", secretBody, nil)
	before := len(eventsWithMessage(parseLogEvents(t, p.stderr.String()), "request_received"))
	if before == 0 {
		t.Fatal("no request_received at debug level — precondition broken")
	}

	rewriteConfig(t, p.cfgPath, "models: [unclosed")
	waitForLogEvent(t, p, func(ev logEvent) bool {
		return ev["message"] == "config_reload_rejected"
	}, "config_reload_rejected")
	postJSON(t, p.addr, "/v1/chat/completions", secretBody, nil)
	deadline := time.Now().Add(5 * time.Second)
	for {
		if got := len(eventsWithMessage(parseLogEvents(t, p.stderr.String()), "request_received")); got > before {
			break // debug-level events still flowing: the level survived
		}
		if time.Now().After(deadline) {
			t.Fatalf("level did not survive the rejected reload — no new request_received; stderr:\n%s", p.stderr.String())
		}
		time.Sleep(20 * time.Millisecond)
	}

	// Recovery: generation resumes one past the last successful publish.
	rewrite("warn")
	expectAck("debug", "warn")
}

// TestDebugLifecycleCheckpoints pins the lifecycle chain black-box: at debug
// verbosity one buffered request produces every checkpoint exactly once, in
// flow order, correlated by request_id — and the response body's own bytes
// appear nowhere.
func TestDebugLifecycleCheckpoints(t *testing.T) {
	upstream := newFakeUpstream(t)
	upstream.setHandler(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"c1","model":"up-live","choices":[{"marker":"SECRET_RESPONSE_BODY"}]}`))
	})

	p := startSubprocess(t, startOpts{
		yaml:     loggingYAML(upstream.url(), upstream.url(), "debug"),
		logLevel: "",
	})

	status, _, _ := postJSON(t, p.addr, "/v1/chat/completions", secretBody, nil)
	if status != http.StatusOK {
		t.Fatalf("status = %d, want 200", status)
	}
	completed := waitForEventCount(t, p, "request_completed", 1)
	requestID, _ := completed[0]["request_id"].(string)
	if requestID == "" {
		t.Fatalf("request_completed missing request_id:\n%v", completed[0])
	}

	chain := []string{
		"request_received", "probe_completed", "model_resolved",
		"request_transform_started", "request_transform_completed",
		"upstream_request_started", "upstream_response_received",
		"response_transform_started", "response_transform_completed",
		"client_write_completed", "request_completed",
	}
	byID := func(evs []logEvent) map[string]int {
		m := make(map[string]int, len(evs))
		for i, ev := range evs {
			if ev["request_id"] == requestID {
				if _, dup := m[ev["message"].(string)]; !dup {
					m[ev["message"].(string)] = i
				}
			}
		}
		return m
	}
	all := parseLogEvents(t, p.stderr.String())
	m := byID(all)
	prev := -1
	for _, msg := range chain {
		idx, ok := m[msg]
		if !ok {
			t.Fatalf("checkpoint %q missing for request %s:\n%s", msg, requestID, p.stderr.String())
		}
		if idx <= prev {
			t.Errorf("checkpoint %q out of order", msg)
		}
		prev = idx
	}
	// One process-wide repetition guard: each slug appears exactly once per
	// buffered request.
	for _, msg := range chain {
		if got := len(eventsWithMessage(all, msg)); got != 1 {
			t.Errorf("%s appeared %d times, want 1", msg, got)
		}
	}

	if strings.Contains(p.stderr.String(), "SECRET_RESPONSE_BODY") {
		t.Error("upstream response body bytes reached the logs — leak")
	}
}

// TestLoggingNeverLeaksSecrets drives every log-producing path at maximum
// verbosity with planted markers in the credentials, the payload, the
// injection prompt, and the upstream endpoint query — none may surface in
// stderr (or stdout) at any level.
func TestLoggingNeverLeaksSecrets(t *testing.T) {
	upstream := newFakeUpstream(t)
	upstream.setHandler(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"c1","model":"up-live","choices":[]}`))
	})

	p := startSubprocess(t, startOpts{
		yaml:     loggingYAML(upstream.url(), upstream.url(), "debug"),
		logLevel: "",
	})

	// Success paths carry the Authorization header and planted markers in
	// the body; the prompt rides in every forwarded request.
	headers := map[string]string{"Authorization": "Bearer SECRET_AUTH_VALUE"}
	secretStream := `{"model":"live","stream":true,"messages":[{"role":"user","content":"SECRET_REQUEST_BODY"}],"api_key":"SECRET_API_KEY"}`
	if status, _, _ := postJSON(t, p.addr, "/v1/chat/completions", secretBody, headers); status != http.StatusOK {
		t.Fatalf("non-stream status = %d, want 200", status)
	}
	if status, _, _ := postJSON(t, p.addr, "/v1/chat/completions", secretStream, headers); status != http.StatusOK {
		t.Fatalf("stream status = %d, want 200", status)
	}

	// Unmapped model: the 404 envelope echoes the requested name by
	// contract, so the name must stay free of markers.
	if status, _, _ := postJSON(t, p.addr, "/v1/chat/completions",
		`{"model":"ghost-model"}`, nil); status != http.StatusNotFound {
		t.Fatalf("unmapped status = %d, want 404", status)
	}

	// Dead endpoint: the dial failure logs the sanitized error and origin —
	// the secret in the endpoint query must not survive sanitization.
	if status, _, _ := postJSON(t, p.addr, "/v1/chat/completions",
		`{"model":"dead"}`, nil); status != http.StatusBadGateway {
		t.Fatalf("dead endpoint status = %d, want 502", status)
	}

	// Upstream 500 echoing the secrets back: the response is relayed
	// verbatim to the client by contract, but the echo must not reach logs.
	upstream.setHandler(func(w http.ResponseWriter, r *http.Request) {
		body := make([]byte, r.ContentLength)
		_, _ = readFullBody(r, body)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write(body) // echoes Authorization-adjacent payload verbatim
	})
	if status, _, respBody := postJSON(t, p.addr, "/v1/chat/completions", secretBody, headers); status != http.StatusInternalServerError {
		t.Fatalf("echo status = %d, want 500 (got body %s)", status, respBody)
	}

	// Invalid reload: the rejected file carries the endpoint secret; the
	// WARN must stay clean. Restore with DIFFERENT valid content — a
	// byte-identical restore is "unchanged file" by the hash design (the
	// last-known-good snapshot is already serving) and never re-fires
	// config_reloaded.
	rewriteConfig(t, p.cfgPath, "models:\n  x:\n    endpoint: nope?api-key=SECRET_ENDPOINT_TOKEN\n    upstream-model: m\n")
	waitForLogEvent(t, p, func(ev logEvent) bool {
		return ev["message"] == "config_reload_rejected"
	}, "config_reload_rejected")
	rewriteConfig(t, p.cfgPath, loggingYAML(upstream.url(), upstream.url(), "info"))
	waitForLogEvent(t, p, func(ev logEvent) bool {
		return ev["message"] == "config_reloaded"
	}, "config_reloaded after restore")

	// The scenarios above must actually have produced the log events under
	// test — otherwise the leak assertion would pass vacuously.
	stderr := p.stderr.String()
	for _, msg := range []string{"upstream_request_failed", "config_reload_rejected"} {
		if len(eventsWithMessage(parseLogEvents(t, stderr), msg)) == 0 {
			t.Fatalf("expected %q events missing — scenario did not exercise logs", msg)
		}
	}

	for _, secret := range []string{
		"SECRET_AUTH_VALUE", "SECRET_REQUEST_BODY", "SECRET_PROMPT_VALUE",
		"SECRET_API_KEY", "SECRET_ENDPOINT_TOKEN",
	} {
		if strings.Contains(stderr, secret) {
			t.Errorf("stderr contains %q — logging leak", secret)
		}
	}
	if stdout := p.stdout.String(); stdout != "" {
		t.Errorf("stdout not empty: %q", stdout)
	}
}

// readFullBody reads exactly len(buf) bytes (test-local helper kept tiny).
func readFullBody(r *http.Request, buf []byte) (int, error) {
	n := 0
	for n < len(buf) {
		m, err := r.Body.Read(buf[n:])
		n += m
		if err != nil {
			return n, err
		}
	}
	return n, nil
}

// TestServeStderrIsJSONLines pins the machine-readable contract: at maximum
// verbosity, across success, streaming, rejection, and failure traffic,
// every stderr line is a JSON object with the three structural keys.
func TestServeStderrIsJSONLines(t *testing.T) {
	upstream := newFakeUpstream(t)
	upstream.setHandler(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/v1/chat/completions") {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"id":"c1","model":"up-live","choices":[]}`))
			return
		}
		w.WriteHeader(http.StatusNotFound)
	})

	p := startSubprocess(t, startOpts{
		yaml:     loggingYAML(upstream.url(), upstream.url(), "debug"),
		logLevel: "",
	})

	postJSON(t, p.addr, "/v1/chat/completions", secretBody, nil)
	postJSON(t, p.addr, "/v1/chat/completions", `{"model":"live","stream":true}`, nil)
	postJSON(t, p.addr, "/v1/chat/completions", `{"model":"ghost"}`, nil)
	postJSON(t, p.addr, "/v1/chat/completions", `{broken`, nil)
	postJSON(t, p.addr, "/v1/chat/completions", `{"model":"dead"}`, nil)
	postJSON(t, p.addr, "/healthz", "", nil) // wrong method on a proxied route

	// Settle the stderr copy before scanning: the five POSTs above each end
	// with a completion line (the 405 route logs at debug only).
	waitForEventCount(t, p, "request_completed", 5)
	for _, line := range strings.Split(p.stderr.String(), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		var ev map[string]any
		if err := json.Unmarshal([]byte(line), &ev); err != nil {
			t.Fatalf("stderr line is not JSON: %q (%v)", line, err)
		}
		for _, key := range []string{"level", "time", "message"} {
			if _, ok := ev[key]; !ok {
				t.Errorf("event missing %q key: %s", key, line)
			}
		}
	}
}

// TestLifecycleLogMatrix pins the failure-path matrix end to end: each
// externally-triggered outcome appears in stderr with its level, slug, and
// discriminating fields — including a reload round-trip with generation
// accounting.
func TestLifecycleLogMatrix(t *testing.T) {
	upstream := newFakeUpstream(t)
	upstream.setHandler(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"c1","model":"up-live","choices":[]}`))
	})

	p := startSubprocess(t, startOpts{
		yaml:     loggingYAML(upstream.url(), upstream.url(), "info"),
		logLevel: "",
	})

	type want struct {
		msg    string
		level  string
		fields map[string]any
	}
	expect := func(w want) {
		t.Helper()
		ev := waitForLogEvent(t, p, func(ev logEvent) bool {
			if ev["message"] != w.msg || ev["level"] != w.level {
				return false
			}
			for k, v := range w.fields {
				got, ok := ev[k]
				if !ok || fmt.Sprint(got) != fmt.Sprint(v) {
					return false
				}
			}
			return true
		}, fmt.Sprintf("%s/%s %v", w.level, w.msg, w.fields))
		_ = ev
	}

	// Completed request.
	status, _, _ := postJSON(t, p.addr, "/v1/chat/completions", secretBody, nil)
	if status != http.StatusOK {
		t.Fatalf("status = %d, want 200", status)
	}
	expect(want{"request_completed", "info", map[string]any{
		"outcome": "completed", "status": 200, "stream": false, "public_model": "live",
	}})

	// Streaming completion.
	status, _, _ = postJSON(t, p.addr, "/v1/chat/completions",
		`{"model":"live","stream":true}`, nil)
	if status != http.StatusOK {
		t.Fatalf("stream status = %d, want 200", status)
	}
	expect(want{"request_completed", "info", map[string]any{
		"outcome": "completed", "stream": true,
	}})

	// Rejections.
	postJSON(t, p.addr, "/v1/chat/completions", `{"model":"ghost"}`, nil)
	expect(want{"request_completed", "info", map[string]any{
		"outcome": "model_not_found", "status": 404,
	}})
	postJSON(t, p.addr, "/v1/chat/completions", `{"messages":[]}`, nil)
	expect(want{"request_completed", "info", map[string]any{"outcome": "missing_model"}})
	postJSON(t, p.addr, "/v1/chat/completions", `{broken`, nil)
	expect(want{"request_completed", "info", map[string]any{"outcome": "invalid_json"}})

	// Upstream unreachable: ERROR with the classified cause, plus the
	// completion line with the 502 outcome.
	postJSON(t, p.addr, "/v1/chat/completions", `{"model":"dead"}`, nil)
	expect(want{"upstream_request_failed", "error", map[string]any{
		"error_class": "connection_refused", "public_model": "dead",
	}})
	expect(want{"request_completed", "info", map[string]any{
		"outcome": "upstream_unreachable", "status": 502,
	}})

	// Reload accounting: an invalid file warns and is rejected; a valid
	// change reloads with a bumped generation and the level echoed.
	rewriteConfig(t, p.cfgPath, "models: [unclosed")
	expect(want{"config_reload_rejected", "warn", nil})
	rewriteConfig(t, p.cfgPath, loggingYAML(upstream.url(), upstream.url(), "warn"))
	ev := waitForLogEvent(t, p, func(ev logEvent) bool {
		return ev["message"] == "config_reloaded"
	}, "config_reloaded")
	if ev["generation"].(float64) != 1 {
		t.Errorf("config_reloaded generation = %v, want 1", ev["generation"])
	}
	if ev["model_count"].(float64) != 3 {
		t.Errorf("config_reloaded model_count = %v, want 3", ev["model_count"])
	}
	if ev["log_level"] != "warn" {
		t.Errorf("config_reloaded log_level = %v, want warn", ev["log_level"])
	}
}
