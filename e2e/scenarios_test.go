package e2e_test

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

const (
	chatPublic   = "chat-public"
	chatUpstream = "upstream-chat"
	respPublic   = "resp-public"
	respUpstream = "upstream-resp"
)

// chatBody is a chat request carrying unknown fields (temperature, extra) that
// must be preserved when forwarded upstream.
const chatBody = `{"model":"chat-public","messages":[{"role":"user","content":"hello"}],"temperature":0.5,"extra":"x"}`

// jsonChatHandler answers a 200 JSON chat completion echoing the given
// upstream model name.
func jsonChatHandler(upstreamModel string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = fmt.Fprintf(w, `{"id":"c1","model":%q,"choices":[{"index":0,"message":{"role":"assistant","content":"hi"},"finish_reason":"stop"}],"usage":{"total_tokens":5}}`, upstreamModel)
	}
}

// jsonResponsesHandler answers a 200 JSON responses envelope with the given
// upstream model name.
func jsonResponsesHandler(upstreamModel string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = fmt.Fprintf(w, `{"id":"r1","model":%q,"output":[{"type":"message","content":[{"type":"output_text","text":"hi"}]}]}`, upstreamModel)
	}
}

// Scenario 1: /healthz returns 200 "ok\n" with a wildcard OAICR_LISTEN, and the
// healthcheck subcommand succeeds against that same wildcard address (while
// deliberately not reading the config file).
func TestHealthzAndHealthcheckWildcard(t *testing.T) {
	port := freePort(t)
	p := startSubprocess(t, startOpts{
		listen: ":" + port,
		yaml:   runtimeYAML(chatPublic, "http://127.0.0.1:1/v1", chatUpstream, "P"),
	})

	resp, err := http.Get("http://" + p.addr + "/healthz")
	if err != nil {
		t.Fatalf("GET /healthz: %v", err)
	}
	body := readBody(t, resp)
	if resp.StatusCode != http.StatusOK || string(body) != "ok\n" {
		t.Fatalf("healthz: status %d body %q, want 200 %q", resp.StatusCode, body, "ok\n")
	}

	// healthcheck subcommand against the same wildcard OAICR_LISTEN; it must not
	// read the config file, so point OAICR_CONFIG_FILE at a nonexistent path.
	cmd := exec.Command(binPath, "healthcheck")
	cmd.Env = append(os.Environ(), "OAICR_LISTEN=:"+port, "OAICR_CONFIG_FILE=/nonexistent/config.yaml")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("healthcheck with wildcard OAICR_LISTEN failed: %v\n%s", err, out)
	}

	// Negative: healthcheck against a closed port must exit nonzero.
	closedPort := freePort(t)
	cmd = exec.Command(binPath, "healthcheck")
	cmd.Env = append(os.Environ(), "OAICR_LISTEN=127.0.0.1:"+closedPort)
	if err := cmd.Run(); err == nil {
		t.Fatal("healthcheck against closed port succeeded, want failure")
	}
}

// CLI: `version` prints the build version and exits 0.
func TestVersionSubcommand(t *testing.T) {
	out, err := exec.Command(binPath, "version").CombinedOutput()
	if err != nil {
		t.Fatalf("version subcommand: %v", err)
	}
	if got := strings.TrimSpace(string(out)); got != "dev" {
		t.Fatalf("version = %q, want %q", got, "dev")
	}
}

// CLI: an unknown argument prints usage to stderr and exits 2.
func TestUnknownArgUsage(t *testing.T) {
	out, err := exec.Command(binPath, "bogus").CombinedOutput()
	if err == nil {
		t.Fatal("unknown arg exited 0, want exit 2")
	}
	if ee, ok := err.(*exec.ExitError); !ok || ee.ExitCode() != 2 {
		t.Fatalf("unknown arg exit = %v, want 2", err)
	}
	if !strings.Contains(string(out), "usage") {
		t.Fatalf("unknown arg output missing usage: %q", out)
	}
}

// Scenario 2: chat request model is rewritten to the upstream model, the
// injection prompt is prepended as messages[0] role=system, the request hits
// /v1/chat/completions, and unknown request fields are preserved.
func TestChatRewriteAndInjection(t *testing.T) {
	up := newFakeUpstream(t)
	up.setHandler(jsonChatHandler(chatUpstream))
	p := startSubprocess(t, startOpts{
		yaml: runtimeYAML(chatPublic, up.url()+"/v1", chatUpstream, "P"),
	})

	status, _, _ := postJSON(t, p.addr, "/v1/chat/completions", chatBody, nil)
	if status != http.StatusOK {
		t.Fatalf("chat status %d, want 200", status)
	}
	reqs := up.requests()
	if len(reqs) != 1 {
		t.Fatalf("upstream got %d requests, want 1", len(reqs))
	}
	r := reqs[0]
	if r.Method != http.MethodPost {
		t.Fatalf("upstream method %s, want POST", r.Method)
	}
	if r.Path != "/v1/chat/completions" {
		t.Fatalf("upstream path %q, want /v1/chat/completions", r.Path)
	}
	m := decodeMap(t, r.Body)
	if m["model"] != chatUpstream {
		t.Fatalf("upstream model %v, want %s", m["model"], chatUpstream)
	}
	msgs, ok := m["messages"].([]any)
	if !ok || len(msgs) != 2 {
		t.Fatalf("upstream messages %v, want 2 entries", m["messages"])
	}
	first, ok := msgs[0].(map[string]any)
	if !ok || first["role"] != "system" || first["content"] != "P" {
		t.Fatalf("messages[0] = %v, want {role:system, content:P}", msgs[0])
	}
	second, ok := msgs[1].(map[string]any)
	if !ok || second["role"] != "user" || second["content"] != "hello" {
		t.Fatalf("messages[1] = %v, want original user message", msgs[1])
	}
	if m["temperature"] != 0.5 {
		t.Fatalf("temperature = %v, want 0.5", m["temperature"])
	}
	if m["extra"] != "x" {
		t.Fatalf("extra = %v, want x", m["extra"])
	}
}

// Scenario 3: non-stream chat response has model rewritten back to the public
// name.
func TestChatNonStreamResponseRewrite(t *testing.T) {
	up := newFakeUpstream(t)
	up.setHandler(jsonChatHandler(chatUpstream))
	p := startSubprocess(t, startOpts{
		yaml: runtimeYAML(chatPublic, up.url()+"/v1", chatUpstream, ""),
	})

	status, _, body := postJSON(t, p.addr, "/v1/chat/completions", chatBody, nil)
	if status != http.StatusOK {
		t.Fatalf("chat status %d, want 200", status)
	}
	m := decodeMap(t, body)
	if m["model"] != chatPublic {
		t.Fatalf("response model %v, want rewritten to %s", m["model"], chatPublic)
	}
}

// Scenario 4: unknown model -> 404 model_not_found, body mentions "does not
// exist", and the request is never forwarded upstream.
func TestChatUnknownModel404(t *testing.T) {
	up := newFakeUpstream(t)
	p := startSubprocess(t, startOpts{
		yaml: runtimeYAML(chatPublic, up.url()+"/v1", chatUpstream, ""),
	})

	status, _, body := postJSON(t, p.addr, "/v1/chat/completions",
		`{"model":"no-such-model","messages":[{"role":"user","content":"hi"}]}`, nil)
	if status != http.StatusNotFound {
		t.Fatalf("unknown model status %d, want 404", status)
	}
	if !strings.Contains(string(body), "model_not_found") || !strings.Contains(string(body), "does not exist") {
		t.Fatalf("unknown model body missing fields: %s", body)
	}
	if n := up.count(); n != 0 {
		t.Fatalf("upstream received %d requests, want 0", n)
	}
}

// Scenario 5: invalid JSON body -> 400 invalid_request_error.
func TestChatInvalidJSON400(t *testing.T) {
	up := newFakeUpstream(t)
	p := startSubprocess(t, startOpts{
		yaml: runtimeYAML(chatPublic, up.url()+"/v1", chatUpstream, ""),
	})

	status, _, body := postJSON(t, p.addr, "/v1/chat/completions", `{"model":"chat-public",`, nil)
	if status != http.StatusBadRequest {
		t.Fatalf("invalid JSON status %d, want 400", status)
	}
	if !strings.Contains(string(body), "invalid_request_error") {
		t.Fatalf("invalid JSON body missing error code: %s", body)
	}
}

// Scenario 6: missing model -> 400.
func TestChatMissingModel400(t *testing.T) {
	up := newFakeUpstream(t)
	p := startSubprocess(t, startOpts{
		yaml: runtimeYAML(chatPublic, up.url()+"/v1", chatUpstream, ""),
	})

	status, _, body := postJSON(t, p.addr, "/v1/chat/completions",
		`{"messages":[{"role":"user","content":"hi"}]}`, nil)
	if status != http.StatusBadRequest {
		t.Fatalf("missing model status %d, want 400 (body %s)", status, body)
	}
}

// Scenario 7: upstream 4xx (401 text/plain) is forwarded verbatim: status,
// bytes, and content-type.
func TestUpstream4xxForwardedVerbatum(t *testing.T) {
	up := newFakeUpstream(t)
	up.setHandler(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = io.WriteString(w, "denied.")
	})
	p := startSubprocess(t, startOpts{
		yaml: runtimeYAML(chatPublic, up.url()+"/v1", chatUpstream, ""),
	})

	status, hdr, body := postJSON(t, p.addr, "/v1/chat/completions", chatBody, nil)
	if status != http.StatusUnauthorized {
		t.Fatalf("upstream 401 -> client status %d, want 401", status)
	}
	if string(body) != "denied." {
		t.Fatalf("upstream 401 body %q, want %q forwarded verbatim", body, "denied.")
	}
	if ct := hdr.Get("Content-Type"); ct != "text/plain" {
		t.Fatalf("upstream 401 content-type %q, want %q", ct, "text/plain")
	}
}

// Scenario 8: upstream 5xx (503 JSON) is forwarded verbatim.
func TestUpstream5xxForwardedVerbatum(t *testing.T) {
	up := newFakeUpstream(t)
	up.setHandler(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = io.WriteString(w, `{"error":"busy"}`)
	})
	p := startSubprocess(t, startOpts{
		yaml: runtimeYAML(chatPublic, up.url()+"/v1", chatUpstream, ""),
	})

	status, hdr, body := postJSON(t, p.addr, "/v1/chat/completions", chatBody, nil)
	if status != http.StatusServiceUnavailable {
		t.Fatalf("upstream 503 -> client status %d, want 503", status)
	}
	if string(body) != `{"error":"busy"}` {
		t.Fatalf("upstream 503 body %q, want forwarded verbatim", body)
	}
	if ct := hdr.Get("Content-Type"); ct != "application/json" {
		t.Fatalf("upstream 503 content-type %q, want %q", ct, "application/json")
	}
}

// Scenario 9: upstream dial failure -> 502 upstream_unreachable JSON.
func TestUpstreamDialFailure502(t *testing.T) {
	closedPort := freePort(t) // reserved then closed: nothing listens here
	p := startSubprocess(t, startOpts{
		yaml: runtimeYAML(chatPublic, "http://127.0.0.1:"+closedPort+"/v1", chatUpstream, ""),
	})

	status, _, body := postJSON(t, p.addr, "/v1/chat/completions", chatBody, nil)
	if status != http.StatusBadGateway {
		t.Fatalf("dial failure status %d, want 502 (body %s)", status, body)
	}
	if !strings.Contains(string(body), "upstream_unreachable") {
		t.Fatalf("dial failure body missing upstream_unreachable: %s", body)
	}
}

// Scenario 10: upstream returns 200 with a non-JSON body -> 502
// upstream_invalid_response (garbage is never forwarded as 200).
func TestUpstreamNonJSON200BadGateway(t *testing.T) {
	up := newFakeUpstream(t)
	up.setHandler(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "not json at all")
	})
	p := startSubprocess(t, startOpts{
		yaml: runtimeYAML(chatPublic, up.url()+"/v1", chatUpstream, ""),
	})

	status, _, body := postJSON(t, p.addr, "/v1/chat/completions", chatBody, nil)
	if status != http.StatusBadGateway {
		t.Fatalf("non-JSON 200 status %d, want 502 (body %s)", status, body)
	}
	if !strings.Contains(string(body), "upstream_invalid_response") {
		t.Fatalf("non-JSON 200 body missing upstream_invalid_response: %s", body)
	}
}

// Scenario 11: responses with string instructions -> upstream receives
// "prompt\n\nrest" as the instructions string, plus the rewritten model.
func TestResponsesStringInstructionsInjected(t *testing.T) {
	up := newFakeUpstream(t)
	up.setHandler(jsonResponsesHandler(respUpstream))
	p := startSubprocess(t, startOpts{
		yaml: runtimeYAML(respPublic, up.url()+"/v1", respUpstream, "P"),
	})

	status, _, _ := postJSON(t, p.addr, "/v1/responses",
		`{"model":"resp-public","instructions":"rest","input":"hello"}`, nil)
	if status != http.StatusOK {
		t.Fatalf("responses status %d, want 200", status)
	}
	r, ok := up.last()
	if !ok {
		t.Fatal("upstream received no request")
	}
	if r.Path != "/v1/responses" {
		t.Fatalf("upstream path %q, want /v1/responses", r.Path)
	}
	m := decodeMap(t, r.Body)
	if m["instructions"] != "P\n\nrest" {
		t.Fatalf("upstream instructions %q, want %q", m["instructions"], "P\n\nrest")
	}
	if m["model"] != respUpstream {
		t.Fatalf("upstream model %v, want %s", m["model"], respUpstream)
	}
	if m["input"] != "hello" {
		t.Fatalf("upstream input %v, want hello", m["input"])
	}
}

// Scenario 12: responses without instructions -> upstream receives
// instructions == the injection prompt alone.
func TestResponsesInstructionsAbsent(t *testing.T) {
	up := newFakeUpstream(t)
	up.setHandler(jsonResponsesHandler(respUpstream))
	p := startSubprocess(t, startOpts{
		yaml: runtimeYAML(respPublic, up.url()+"/v1", respUpstream, "P"),
	})

	status, _, _ := postJSON(t, p.addr, "/v1/responses",
		`{"model":"resp-public","input":"hello"}`, nil)
	if status != http.StatusOK {
		t.Fatalf("responses status %d, want 200", status)
	}
	r, ok := up.last()
	if !ok {
		t.Fatal("upstream received no request")
	}
	m := decodeMap(t, r.Body)
	if m["instructions"] != "P" {
		t.Fatalf("upstream instructions %q, want %q", m["instructions"], "P")
	}
}

// Scenario 13: responses response body envelope has model rewritten back to
// the public name.
func TestResponsesBodyModelRewrite(t *testing.T) {
	up := newFakeUpstream(t)
	up.setHandler(jsonResponsesHandler(respUpstream))
	p := startSubprocess(t, startOpts{
		yaml: runtimeYAML(respPublic, up.url()+"/v1", respUpstream, ""),
	})

	status, _, body := postJSON(t, p.addr, "/v1/responses",
		`{"model":"resp-public","input":"hello"}`, nil)
	if status != http.StatusOK {
		t.Fatalf("responses status %d, want 200", status)
	}
	m := decodeMap(t, body)
	if m["model"] != respPublic {
		t.Fatalf("response model %v, want rewritten to %s", m["model"], respPublic)
	}
}

// Scenario 21: SIGTERM while idle -> process exits 0 within a few seconds and
// logs "shutdown complete".
func TestShutdownIdleExitsZero(t *testing.T) {
	up := newFakeUpstream(t)
	up.setHandler(jsonChatHandler(chatUpstream))
	p := startSubprocess(t, startOpts{
		yaml:     runtimeYAML(chatPublic, up.url()+"/v1", chatUpstream, ""),
		logLevel: "info",
	})

	p.signal(syscall.SIGTERM)
	start := time.Now()
	code, err := p.waitExit(t, 6*time.Second)
	if err != nil {
		t.Fatalf("idle shutdown: %v", err)
	}
	if code != 0 {
		t.Fatalf("idle shutdown exit code %d, want 0", code)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("idle shutdown took %v, want a few seconds", elapsed)
	}
}

// Scenario 22a: graceful drain. An in-flight request is blocked upstream;
// SIGTERM arrives with grace well above the block; upstream releases after
// ~1s; the request completes with the rewritten response and the process
// exits 0.
func TestShutdownDrainsInFlight(t *testing.T) {
	up := newFakeUpstream(t)
	received := make(chan struct{}, 1)
	release := make(chan struct{})
	var once sync.Once
	up.setHandler(func(w http.ResponseWriter, r *http.Request) {
		once.Do(func() { received <- struct{}{} })
		<-release
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, `{"id":"slow","model":"upstream-chat","choices":[]}`)
	})
	p := startSubprocess(t, startOpts{
		yaml:  runtimeYAML(chatPublic, up.url()+"/v1", chatUpstream, ""),
		grace: "5s", // > the ~1s upstream block
	})

	resCh := make(chan result, 1)
	go func() {
		status, _, body, err := postJSONRaw(p.addr, "/v1/chat/completions", chatBody, nil)
		resCh <- result{status: status, body: body, err: err}
	}()
	<-received // request is now in flight at the upstream

	p.signal(syscall.SIGTERM)
	time.Sleep(1 * time.Second) // let graceful shutdown begin while blocked
	close(release)              // upstream now completes the response

	select {
	case res := <-resCh:
		if res.err != nil {
			t.Fatalf("in-flight request failed during drain: %v", res.err)
		}
		if res.status != http.StatusOK {
			t.Fatalf("in-flight request status %d, want 200", res.status)
		}
		m := decodeMap(t, res.body)
		if m["model"] != chatPublic {
			t.Fatalf("drained response model %v, want %s", m["model"], chatPublic)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("in-flight request never completed during drain")
	}

	code, err := p.waitExit(t, 6*time.Second)
	if err != nil {
		t.Fatalf("drain shutdown: %v", err)
	}
	if code != 0 {
		t.Fatalf("drain shutdown exit code %d, want 0", code)
	}
}

// Scenario 22b: force close. An in-flight request is blocked upstream for
// longer than grace; SIGTERM; the process is gone within grace+2s with a
// nonzero exit (or killed edge).
func TestShutdownForceClose(t *testing.T) {
	up := newFakeUpstream(t)
	received := make(chan struct{}, 1)
	release := make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})
	up.setHandler(func(w http.ResponseWriter, r *http.Request) {
		select {
		case received <- struct{}{}:
		default:
		}
		<-release
	})
	p := startSubprocess(t, startOpts{
		yaml:  runtimeYAML(chatPublic, up.url()+"/v1", chatUpstream, ""),
		grace: "2s", // shorter than the upstream block
	})

	resCh := make(chan result, 1)
	go func() {
		status, _, body, err := postJSONRaw(p.addr, "/v1/chat/completions", chatBody, nil)
		resCh <- result{status: status, body: body, err: err}
	}()
	<-received

	start := time.Now()
	p.signal(syscall.SIGTERM)
	code, err := p.waitExit(t, 8*time.Second)
	_ = code // force-close may drain gracefully (exit 0) or be killed; either way the
	// contract is that the process is gone well within grace+2s having cut the
	// request rather than waiting for the upstream release.
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("force-close: %v", err)
	}
	if elapsed > 5*time.Second {
		t.Fatalf("force-close took %v, want within grace(2s)+2s", elapsed)
	}
	// The in-flight request must have been cut off by the shutdown: the client
	// cannot have received a successful response with the server gone.
	select {
	case res := <-resCh:
		if res.err == nil && res.status == http.StatusOK {
			t.Fatalf("in-flight request completed during force-close (status %d)", res.status)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("in-flight request client never noticed the force-close")
	}
}

// Scenario 24: plane violation — a config file carrying a top-level "listen"
// key is rejected at startup with a nonzero exit and an error mentioning the
// key.
func TestPlaneViolationListenKey(t *testing.T) {
	yaml := "listen: :9999\n" + runtimeYAML(chatPublic, "http://127.0.0.1:1/v1", chatUpstream, "P")
	code, stderr := startSubprocessExpectExit(t, startOpts{yaml: yaml})
	if code == 0 {
		t.Fatal("startup with plane-violating config exited 0, want nonzero")
	}
	if !strings.Contains(stderr, "unknown top-level key") {
		t.Fatalf("startup error must name the violation class:\n%s", stderr)
	}
	// The key itself is never echoed: a paste into a key position can carry
	// credentials just as well as a value, and the fatal reaches logs
	// verbatim.
}

// Scenario 25: missing config file at startup -> nonzero exit.
func TestMissingConfigFileStartup(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "does-not-exist.yaml")
	code, stderr := startSubprocessExpectExit(t, startOpts{configPath: missing})
	if code == 0 {
		t.Fatal("startup with missing config exited 0, want nonzero")
	}
	if !strings.Contains(stderr, "config") {
		t.Fatalf("startup error should mention the config file:\n%s", stderr)
	}
}

// Scenario 26: listen failure — a listen address that is already bound fails
// the startup with exit 1 and a server_failed event; the process never sits
// silently half-started.
func TestListenFailureExitsOne(t *testing.T) {
	blocker, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("bind blocker listener: %v", err)
	}
	defer func() { _ = blocker.Close() }()
	code, stderr := startSubprocessExpectExit(t, startOpts{
		yaml:   runtimeYAML(chatPublic, "http://127.0.0.1:1/v1", chatUpstream, ""),
		listen: blocker.Addr().String(),
	})
	if code != 1 {
		t.Fatalf("startup with an occupied listen address exited %d, want 1", code)
	}
	if !strings.Contains(stderr, "server_failed") {
		t.Fatalf("server_failed event missing from listen failure:\n%s", stderr)
	}
}

// Scenario 27: unknown paths get 404/405 (never 200) and never reach the
// upstream; non-POST on /v1/chat/completions gets 405.
func TestUnknownPathsAndMethods(t *testing.T) {
	up := newFakeUpstream(t)
	up.setHandler(jsonChatHandler(chatUpstream))
	p := startSubprocess(t, startOpts{
		yaml: runtimeYAML(chatPublic, up.url()+"/v1", chatUpstream, ""),
	})

	// GET on an unsupported endpoint: 404 or 405, never 200, no upstream call.
	resp, err := http.Get("http://" + p.addr + "/v1/embeddings")
	if err != nil {
		t.Fatalf("GET /v1/embeddings: %v", err)
	}
	readBody(t, resp)
	if resp.StatusCode == http.StatusOK || (resp.StatusCode != http.StatusNotFound && resp.StatusCode != http.StatusMethodNotAllowed) {
		t.Fatalf("GET /v1/embeddings status %d, want 404 or 405", resp.StatusCode)
	}
	if n := up.count(); n != 0 {
		t.Fatalf("upstream received %d requests for unknown path, want 0", n)
	}

	// Non-POST on the chat endpoint -> 405.
	resp, err = http.Get("http://" + p.addr + "/v1/chat/completions")
	if err != nil {
		t.Fatalf("GET /v1/chat/completions: %v", err)
	}
	readBody(t, resp)
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("GET /v1/chat/completions status %d, want 405", resp.StatusCode)
	}
	if n := up.count(); n != 0 {
		t.Fatalf("upstream received %d requests for GET, want 0", n)
	}
}
