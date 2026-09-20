// Package e2e_test contains black-box end-to-end tests for the
// openai-compatible-injector binary. Every scenario talks to a real built
// binary over real HTTP and asserts only externally observable behaviour.
package e2e_test

import (
	"bufio"
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// binPath is the freshly built injector binary, produced once in TestMain.
var binPath string

func TestMain(m *testing.M) {
	flag.Parse()
	if testing.Short() {
		fmt.Fprintln(os.Stderr, "e2e: skipping suite under -short")
		os.Exit(0)
	}
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		fmt.Fprintln(os.Stderr, "e2e: cannot locate test source")
		os.Exit(1)
	}
	root := filepath.Dir(filepath.Dir(file)) // e2e/ -> module root
	tmp, err := os.MkdirTemp("", "injector-e2e-*")
	if err != nil {
		fmt.Fprintln(os.Stderr, "e2e:", err)
		os.Exit(1)
	}
	binPath = filepath.Join(tmp, "injector")
	cmd := exec.Command("go", "build", "-o", binPath, "./cmd/openai-compatible-injector")
	cmd.Dir = root
	out, err := cmd.CombinedOutput()
	if err != nil {
		fmt.Fprintf(os.Stderr, "e2e: building injector failed: %v\n%s", err, out)
		os.Exit(1)
	}
	code := m.Run()
	_ = os.RemoveAll(tmp)
	os.Exit(code)
}

// ---- harness ----

type lockedBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *lockedBuf) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *lockedBuf) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

// startOpts configures a subprocess launch.
type startOpts struct {
	yaml         string   // runtime config file content (used unless configPath set)
	configPath   string   // if set, use this exact path (e.g. missing file)
	listen       string   // LISTEN value; empty -> ephemeral loopback port
	grace        string   // SHUTDOWN_GRACE; default "5s"
	logLevel     string   // runtime YAML logging.level; default "error"
	pollInterval string   // CONFIG_POLL_INTERVAL; default "50ms"
	extraEnv     []string // extra "K=V" entries appended to the process env
}

// proc is a running injector subprocess with captured output.
type proc struct {
	t       *testing.T
	cmd     *exec.Cmd
	cfgPath string
	addr    string // loopback "IP:port" used by the harness to reach the service
	stderr  *lockedBuf
	stdout  *lockedBuf
	done    chan struct{}
	exit    int32
}

func newProc(t *testing.T, o startOpts) *proc {
	t.Helper()
	if o.grace == "" {
		o.grace = "5s"
	}
	if o.logLevel == "" {
		o.logLevel = "error"
	}
	if o.pollInterval == "" {
		o.pollInterval = "50ms"
	}
	cfgPath := o.configPath
	if cfgPath == "" {
		cfgPath = filepath.Join(t.TempDir(), "config.yaml")
		if err := os.WriteFile(cfgPath, []byte(withLoggingLevel(o.yaml, o.logLevel)), 0o644); err != nil {
			t.Fatalf("write config file: %v", err)
		}
	}
	listen := o.listen
	port := freePort(t)
	if listen == "" {
		listen = "127.0.0.1:" + port
	} else {
		port = portOf(listen)
	}
	p := &proc{
		t:       t,
		cmd:     exec.Command(binPath),
		cfgPath: cfgPath,
		addr:    "127.0.0.1:" + port,
		stderr:  &lockedBuf{},
		stdout:  &lockedBuf{},
		done:    make(chan struct{}),
		exit:    -1,
	}
	p.cmd.Env = append(os.Environ(),
		"LISTEN="+listen,
		"CONFIG_FILE="+cfgPath,
		"CONFIG_POLL_INTERVAL="+o.pollInterval,
		"SHUTDOWN_GRACE="+o.grace,
	)
	p.cmd.Env = append(p.cmd.Env, o.extraEnv...)
	p.cmd.Stdout = p.stdout
	p.cmd.Stderr = p.stderr
	return p
}

// start launches the process and begins collecting its exit status.
func (p *proc) start() {
	if err := p.cmd.Start(); err != nil {
		p.t.Fatalf("start subprocess: %v", err)
	}
	go func() {
		err := p.cmd.Wait()
		if err != nil {
			if ee, ok := err.(*exec.ExitError); ok {
				atomic.StoreInt32(&p.exit, int32(ee.ExitCode()))
			} else {
				atomic.StoreInt32(&p.exit, -1)
			}
		} else {
			atomic.StoreInt32(&p.exit, 0)
		}
		close(p.done)
	}()
}

// signal sends a signal to the process, ignoring "already gone" errors.
func (p *proc) signal(sig os.Signal) {
	if err := p.cmd.Process.Signal(sig); err != nil {
		p.t.Logf("signal %v: %v", sig, err)
	}
}

// waitExit waits for process exit up to timeout, killing it if necessary.
// Returns the exit code and a non-nil error if the process had to be killed.
func (p *proc) waitExit(t *testing.T, timeout time.Duration) (int, error) {
	t.Helper()
	select {
	case <-p.done:
	case <-time.After(timeout):
		_ = p.cmd.Process.Kill()
		<-p.done
		return int(atomic.LoadInt32(&p.exit)), fmt.Errorf("process did not exit within %v; killed", timeout)
	}
	return int(atomic.LoadInt32(&p.exit)), nil
}

// terminate SIGTERMs and waits; used by t.Cleanup, so it never calls Fatal.
func (p *proc) terminate(timeout time.Duration) {
	select {
	case <-p.done:
		return
	default:
	}
	p.signal(syscall.SIGTERM)
	select {
	case <-p.done:
	case <-time.After(timeout):
		_ = p.cmd.Process.Kill()
		<-p.done
	}
}

// startSubprocess boots the binary, registers termination cleanup and waits
// for /healthz to report 200 "ok\n" (up to timeout).
func startSubprocess(t *testing.T, o startOpts) *proc {
	t.Helper()
	p := newProc(t, o)
	p.start()
	t.Cleanup(func() { p.terminate(10 * time.Second) })
	p.waitHealth(t, 5*time.Second)
	return p
}

// startSubprocessExpectExit boots the binary and waits for it to exit on its
// own (used for startup-failure scenarios). Returns the exit code and stderr.
func startSubprocessExpectExit(t *testing.T, o startOpts) (int, string) {
	t.Helper()
	p := newProc(t, o)
	p.start()
	code, err := p.waitExit(t, 10*time.Second)
	if err != nil {
		t.Fatalf("startup-failure process still running: %v", err)
	}
	if p.cmd.ProcessState != nil && !p.cmd.ProcessState.Exited() {
		t.Fatalf("process did not exit cleanly: %v", p.cmd.ProcessState)
	}
	return code, p.stderr.String()
}

// waitHealth polls /healthz until it returns 200 "ok\n" or the timeout passes.
func (p *proc) waitHealth(t *testing.T, timeout time.Duration) {
	t.Helper()
	url := "http://" + p.addr + "/healthz"
	client := &http.Client{Timeout: 500 * time.Millisecond}
	deadline := time.Now().Add(timeout)
	for {
		resp, err := client.Get(url)
		if err == nil {
			body, _ := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK && string(body) == "ok\n" {
				return
			}
		}
		select {
		case <-p.done:
			t.Fatalf("subprocess exited before healthz ready; stderr:\n%s", p.stderr.String())
		case <-time.After(50 * time.Millisecond):
		}
		if time.Now().After(deadline) {
			t.Fatalf("healthz not ready within %v (stderr:\n%s)", timeout, p.stderr.String())
		}
	}
}

// freePort reserves an ephemeral loopback TCP port and releases it for reuse.
func freePort(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve port: %v", err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	_ = ln.Close()
	return strconv.Itoa(port)
}

// portOf extracts the port from a LISTEN-style address (":8080", "[::]:8080",
// "127.0.0.1:8080").
func portOf(listen string) string {
	_, port, err := net.SplitHostPort(listen)
	if err != nil {
		return strings.TrimPrefix(listen, ":")
	}
	return port
}

// rewriteConfig overwrites the runtime config file in place (same path); the
// running poller picks up the new content by hash.
func rewriteConfig(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("rewrite config %s: %v", path, err)
	}
}

// runtimeYAML renders the runtime config file body for a single model entry.
func runtimeYAML(publicName, endpoint, upstreamModel, injectionPrompt string) string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "models:\n  %s:\n    endpoint: %s\n    upstream-model: %s\n",
		publicName, endpoint, upstreamModel)
	if injectionPrompt != "" {
		fmt.Fprintf(&sb, "    injection-prompt: %s\n", injectionPrompt)
	}
	return sb.String()
}

// withLoggingLevel appends a logging.level section to a runtime YAML body.
// The log level travels in the runtime file — the same hot-reloadable plane
// as model mappings — because LOG_LEVEL no longer exists: the runtime file
// is mandatory at boot, so an env override had no legitimate window. A body
// that already carries a logging section is returned unchanged.
func withLoggingLevel(yamlBody, level string) string {
	if level == "" || strings.Contains(yamlBody, "\nlogging:") {
		return yamlBody
	}
	return yamlBody + "\nlogging:\n  level: " + level + "\n"
}

// recordedRequest is an immutable snapshot of one upstream HTTP request.
type recordedRequest struct {
	Method  string
	Path    string
	Headers http.Header
	Body    []byte
}

// fakeUpstream is a switchable httptest server that records every request it
// receives. Endpoint configs point scripts at u.url()+"/v1" so upstream
// requests arrive at /v1/chat/completions and /v1/responses.
type fakeUpstream struct {
	t       *testing.T
	srv     *httptest.Server
	mu      sync.Mutex
	reqs    []recordedRequest
	handler http.HandlerFunc
}

func newFakeUpstream(t *testing.T) *fakeUpstream {
	t.Helper()
	u := &fakeUpstream{t: t}
	u.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			u.t.Errorf("upstream read body: %v", err)
		}
		u.mu.Lock()
		u.reqs = append(u.reqs, recordedRequest{
			Method:  r.Method,
			Path:    r.URL.Path,
			Headers: r.Header.Clone(),
			Body:    body,
		})
		h := u.handler
		u.mu.Unlock()
		if h != nil {
			h(w, r)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	t.Cleanup(u.srv.Close)
	return u
}

func (u *fakeUpstream) setHandler(h http.HandlerFunc) {
	u.mu.Lock()
	u.handler = h
	u.mu.Unlock()
}

// url returns the base URL (scheme://host:port).
func (u *fakeUpstream) url() string { return u.srv.URL }

func (u *fakeUpstream) count() int {
	u.mu.Lock()
	defer u.mu.Unlock()
	return len(u.reqs)
}

func (u *fakeUpstream) requests() []recordedRequest {
	u.mu.Lock()
	defer u.mu.Unlock()
	return append([]recordedRequest(nil), u.reqs...)
}

func (u *fakeUpstream) last() (recordedRequest, bool) {
	u.mu.Lock()
	defer u.mu.Unlock()
	if len(u.reqs) == 0 {
		return recordedRequest{}, false
	}
	return u.reqs[len(u.reqs)-1], true
}

// ---- HTTP helpers ----

// openJSON issues a request with a JSON content type and returns the raw
// response without consuming the body (needed for streaming scenarios).
func openJSON(t *testing.T, addr, path string, body any, hdr map[string]string) *http.Response {
	t.Helper()
	var rdr io.Reader
	switch b := body.(type) {
	case string:
		rdr = strings.NewReader(b)
	case []byte:
		rdr = bytes.NewReader(b)
	default:
		rdr = nil
	}
	req, err := http.NewRequest(http.MethodPost, "http://"+addr+path, rdr)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST %s: %v", path, err)
	}
	return resp
}

// postJSON issues a POST and returns status, headers, and body bytes.
func postJSON(t *testing.T, addr, path string, body string, hdr map[string]string) (int, http.Header, []byte) {
	t.Helper()
	resp := openJSON(t, addr, path, body, hdr)
	defer func() { _ = resp.Body.Close() }()
	got, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read response body: %v", err)
	}
	return resp.StatusCode, resp.Header.Clone(), got
}

func readBody(t *testing.T, resp *http.Response) []byte {
	t.Helper()
	defer func() { _ = resp.Body.Close() }()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return b
}

func decodeMap(t *testing.T, b []byte) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatalf("unmarshal %s: %v", b, err)
	}
	return m
}

// ---- SSE helpers ----

// readSSELine reads one line (including its newline) with a bounded timeout.
func readSSELine(t *testing.T, br *bufio.Reader, timeout time.Duration) (string, error) {
	t.Helper()
	type res struct {
		line string
		err  error
	}
	ch := make(chan res, 1)
	go func() {
		l, e := br.ReadString('\n')
		ch <- res{l, e}
	}()
	select {
	case r := <-ch:
		return r.line, r.err
	case <-time.After(timeout):
		t.Fatalf("timed out after %v waiting for SSE line", timeout)
		return "", io.EOF
	}
}

// nextSSEEvent reads one SSE event (lines until a blank line or EOF) and
// returns the trimmed non-empty lines plus whether EOF was reached.
func nextSSEEvent(t *testing.T, br *bufio.Reader, timeout time.Duration) (lines []string, eof bool) {
	t.Helper()
	for {
		line, err := readSSELine(t, br, timeout)
		if err == io.EOF {
			if line != "" {
				lines = append(lines, strings.TrimRight(line, "\r\n"))
			}
			return lines, true
		}
		if err != nil {
			t.Fatalf("read SSE line: %v", err)
		}
		trimmed := strings.TrimRight(line, "\r\n")
		if trimmed == "" {
			return lines, false
		}
		lines = append(lines, trimmed)
	}
}

// sseDataContent extracts the payload after a "data:" prefix, trimming only
// the framing space (interior whitespace is preserved).
func sseDataContent(t *testing.T, line string) string {
	t.Helper()
	if !strings.HasPrefix(line, "data:") {
		t.Fatalf("expected SSE data line, got %q", line)
	}
	return strings.TrimPrefix(line, "data:")
}

// result carries a completed request outcome, safe to pass across goroutines
// (used by tests that issue requests from goroutines, where t.Fatal is
// forbidden).
type result struct {
	status int
	body   []byte
	err    error
}

// postJSONRaw is the goroutine-safe variant of postJSON: never calls t.Fatal.
func postJSONRaw(addr, path, body string, hdr map[string]string) (int, http.Header, []byte, error) {
	rdr := strings.NewReader(body)
	req, err := http.NewRequest(http.MethodPost, "http://"+addr+path, rdr)
	if err != nil {
		return 0, nil, nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, nil, nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, nil, nil, err
	}
	return resp.StatusCode, resp.Header.Clone(), b, nil
}

// waitForUpstream polls for an upstream request by issuing warm-up requests to
// the model "common" until the target upstream records one, or the deadline
// passes.
func waitForUpstream(t *testing.T, p *proc, target *fakeUpstream, timeout time.Duration) error {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if target.count() > 0 {
			return nil
		}
		_, _, _, err := postJSONRaw(p.addr, "/v1/chat/completions",
			`{"model":"common","messages":[{"role":"user","content":"warm"}]}`, nil)
		if err != nil {
			return err
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("target upstream received no request within %v", timeout)
}
