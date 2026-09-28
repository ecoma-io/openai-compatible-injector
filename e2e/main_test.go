// Package e2e_test contains black-box end-to-end tests for the
// openai-compatible-injector binary. Every scenario talks to a real built
// binary over real HTTP and asserts only externally observable behaviour.
package e2e_test

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
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

// e2eAPIKey is the client bearer credential every standard config carries;
// request helpers present it automatically unless a test sets its own
// Authorization header.
const e2eAPIKey = "e2e-client-key"

// startOpts configures a subprocess launch.
type startOpts struct {
	yaml         string   // runtime config file content (used unless configPath set)
	configPath   string   // if set, use this exact path (e.g. missing file)
	listen       string   // OAICR_LISTEN value; empty -> ephemeral loopback port
	grace        string   // OAICR_SHUTDOWN_GRACE; default "5s"
	logLevel     string   // runtime YAML log-level; default "error"
	pollInterval string   // OAICR_CONFIG_POLL_INTERVAL; default "50ms"
	extraEnv     []string // extra "K=V" entries appended to the process env
	apiKey       string   // api-key upserted into the boot YAML; default e2eAPIKey
	noAPIKey     bool     // write the boot YAML without an api-key (boot-rejection scenarios)
}

// proc is a running injector subprocess with captured output.
type proc struct {
	t       testing.TB
	cmd     *exec.Cmd
	cfgPath string
	addr    string // loopback "IP:port" used by the harness to reach the service
	stderr  *lockedBuf
	stdout  *lockedBuf
	done    chan struct{}
	exit    int32
}

func newProc(tb testing.TB, o startOpts) *proc {
	tb.Helper()
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
		body := withLoggingLevel(o.yaml, o.logLevel)
		if !o.noAPIKey {
			key := o.apiKey
			if key == "" {
				key = e2eAPIKey
			}
			body = withAPIKey(body, key)
		}
		cfgPath = filepath.Join(tb.TempDir(), "config.yaml")
		if err := os.WriteFile(cfgPath, []byte(body), 0o644); err != nil {
			tb.Fatalf("write config file: %v", err)
		}
	}
	listen := o.listen
	port := freePort(tb)
	if listen == "" {
		listen = "127.0.0.1:" + port
	} else {
		port = portOf(listen)
	}
	p := &proc{
		t:       tb,
		cmd:     exec.Command(binPath),
		cfgPath: cfgPath,
		addr:    "127.0.0.1:" + port,
		stderr:  &lockedBuf{},
		stdout:  &lockedBuf{},
		done:    make(chan struct{}),
		exit:    -1,
	}
	p.cmd.Env = append(os.Environ(),
		"OAICR_LISTEN="+listen,
		"OAICR_CONFIG_FILE="+cfgPath,
		"OAICR_CONFIG_POLL_INTERVAL="+o.pollInterval,
		"OAICR_SHUTDOWN_GRACE="+o.grace,
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
func (p *proc) waitHealth(tb testing.TB, timeout time.Duration) {
	tb.Helper()
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
			tb.Fatalf("subprocess exited before healthz ready; stderr:\n%s", p.stderr.String())
		case <-time.After(50 * time.Millisecond):
		}
		if time.Now().After(deadline) {
			tb.Fatalf("healthz not ready within %v (stderr:\n%s)", timeout, p.stderr.String())
		}
	}
}

// freePort reserves an ephemeral loopback TCP port and releases it for reuse.
func freePort(tb testing.TB) string {
	tb.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatalf("reserve port: %v", err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	_ = ln.Close()
	return strconv.Itoa(port)
}

// portOf extracts the port from a listen-style address (":8080", "[::]:8080",
// "127.0.0.1:8080").
func portOf(listen string) string {
	_, port, err := net.SplitHostPort(listen)
	if err != nil {
		return strings.TrimPrefix(listen, ":")
	}
	return port
}

// rewriteConfig replaces the runtime config file ATOMICALLY (same path); the
// running poller picks up the new content by hash.
//
// It writes a sibling temp file in the same directory, fsyncs it, then
// renames it over the target, so a reader never observes a partial document.
// The old os.WriteFile call truncated first, which exposed an empty or
// half-written file to the poller for the duration of the write: with a 50ms
// poll interval the proxy would legitimately reject the transient state and
// log `config_reload_rejected` for a document no operator ever wrote. That
// was a harness defect, not a proxy defect — the poller's rejection was
// correct. Tests that mean "this file is invalid" must write invalid
// content, not depend on catching a rewrite in flight; see
// writeConfigPartial for the deliberate partial-write scenario.
func rewriteConfig(t *testing.T, path, content string) {
	t.Helper()
	dir, base := filepath.Split(path)
	if dir == "" {
		dir = "."
	}
	tmp, err := os.CreateTemp(dir, "."+base+".e2e-*")
	if err != nil {
		t.Fatalf("rewrite config %s: %v", path, err)
	}
	tmpName := tmp.Name()
	// Any failure past this point must not leave the temp file behind, and
	// must not have touched the live config.
	committed := false
	defer func() {
		_ = tmp.Close()
		if !committed {
			_ = os.Remove(tmpName)
		}
	}()
	if _, err := tmp.WriteString(content); err != nil {
		t.Fatalf("rewrite config %s: %v", path, err)
	}
	if err := tmp.Sync(); err != nil {
		t.Fatalf("rewrite config %s: sync: %v", path, err)
	}
	if err := tmp.Close(); err != nil {
		t.Fatalf("rewrite config %s: close: %v", path, err)
	}
	if err := os.Chmod(tmpName, 0o644); err != nil {
		t.Fatalf("rewrite config %s: chmod: %v", path, err)
	}
	if err := os.Rename(tmpName, path); err != nil {
		t.Fatalf("rewrite config %s: rename: %v", path, err)
	}
	committed = true
}

// writeConfigTruncated is the NON-atomic write the harness used to perform
// everywhere. It is retained deliberately as `writeConfigPartial`'s
// primitive, and as the ability to reproduce the truncate/write window that
// made a valid reload look like a rejection.
func writeConfigPartial(t *testing.T, path, content string) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		t.Fatalf("write config %s: %v", path, err)
	}
	if _, err := f.WriteString(content); err != nil {
		_ = f.Close()
		t.Fatalf("write config %s: %v", path, err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("write config %s: %v", path, err)
	}
}

// runtimeYAML renders the runtime config file body for a single model entry.
// Each extraFields line is appended verbatim under the entry (indented as a
// member), for optional blocks such as thinking-usage. The api-key rides the
// body itself: reload scenarios rewrite the file with a fresh runtimeYAML
// render, so the key must survive every rewrite, not just the boot one.
func runtimeYAML(publicName, endpoint, upstreamModel, injectionPrompt string, extraFields ...string) string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "api-key: %s\nmodels:\n  %s:\n    endpoint: %s\n    upstream-model: %s\n",
		e2eAPIKey, publicName, endpoint, upstreamModel)
	if injectionPrompt != "" {
		fmt.Fprintf(&sb, "    injection-prompt: %s\n", injectionPrompt)
	}
	for _, f := range extraFields {
		sb.WriteString("    " + f + "\n")
	}
	return sb.String()
}

// withAPIKey prepends the api-key to a runtime YAML body that does not carry
// one, mirroring withLoggingLevel. A body that already defines the key (at
// the top or on its own line) is returned unchanged — upserting over it
// would duplicate the key and turn a valid file into a rejection.
func withAPIKey(yamlBody, key string) string {
	if yamlBody == "" || strings.HasPrefix(yamlBody, "api-key:") || strings.Contains(yamlBody, "\napi-key:") {
		return yamlBody
	}
	return "api-key: " + key + "\n" + yamlBody
}

// withLoggingLevel appends a top-level log-level key to a runtime YAML
// body. The log level travels in the runtime file — the same
// hot-reloadable plane as model mappings — because LOG_LEVEL no longer
// exists: the runtime file is mandatory at boot, so an env override had no
// legitimate window. A body that already carries a log-level key is
// returned unchanged.
func withLoggingLevel(yamlBody, level string) string {
	if level == "" || strings.Contains(yamlBody, "\nlog-level:") {
		return yamlBody
	}
	return yamlBody + "\nlog-level: " + level + "\n"
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
//
// The response body is ALWAYS closed, at the latest when the test ends: this
// helper hands out a live body so streaming callers can read it, but a caller
// that neither drains it to EOF nor closes it leaks the connection, its
// transport read-loop goroutine and the file descriptor — the confirmed
// defect in the streamed-response scenarios. Registering the close here makes
// the release unconditional on every path, an early t.Fatalf included. It is
// not a substitute for prompt ownership: a cleanup that runs only at test end
// holds the connection for the whole test, so callers still close what they
// own as soon as they are done (a deferred close right after the call, or
// newSSEStream below). Body.Close is idempotent, so an explicit close beside
// this one is safe.
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
	// Default to the configured bearer so the suite's traffic passes the
	// auth gate; a test that sets its own Authorization opts out.
	if _, ok := hdr["Authorization"]; !ok {
		req.Header.Set("Authorization", "Bearer "+e2eAPIKey)
	}
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST %s: %v", path, err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
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

// sseT is the slice of *testing.T an sseStream uses: the failure sink, the
// cleanup registrar and Helper for stack attribution. It is an interface
// rather than a *testing.T field for one reason only: a timeout must fail the
// test, and the regression that proves it needs to observe that failure
// without failing itself. testing.TB is sealed by its unexported private
// method, so a stand-in is possible only against a seam this package owns.
type sseT interface {
	Helper()
	Cleanup(func())
	Fatalf(format string, args ...any)
}

// errSSETimeout is what line returns on the timeout branch after it has
// failed the test. A real *testing.T never sees it — Fatalf ends the test
// goroutine first — but it exists so the branch says what happened instead of
// returning the clean-looking io.EOF the old helper carried, which let a
// caller read a timeout as an orderly end of stream.
var errSSETimeout = errors.New("sse stream line read timed out")

// sseStream owns one streamed response: the live body, the reader over it,
// and the per-line timeout that bounds every read. It exists so the reader
// and the body can never be separated — the old readSSELine took a bare
// *bufio.Reader and, on timeout, left its reader goroutine blocked in
// ReadString until something closed the connection, while the unreachable
// `return "", io.EOF` after the fatal hid that the caller would have seen a
// clean EOF rather than an error.
type sseStream struct {
	t    sseT
	resp *http.Response
	br   *bufio.Reader
}

// newSSEStream wraps a streamed response, registers the close that runs at
// the latest when the test ends, and returns the owner of both halves. The
// body is closed on every exit path — an early t.Fatalf return included —
// because the cleanup, and not the caller's control flow, guarantees it.
func newSSEStream(t sseT, resp *http.Response) *sseStream {
	t.Helper()
	s := &sseStream{t: t, resp: resp, br: bufio.NewReader(resp.Body)}
	t.Cleanup(s.close)
	return s
}

// close releases the connection. It is idempotent, so a site that closes
// promptly (a mid-stream client disconnect, say) and the end-of-test cleanup
// can both call it. Closing a response body that was not read to EOF aborts
// its connection — net/http's early-close path closes the wire and does not
// wait on the body's own read lock — so this is also what unparks a read
// blocked on a silent upstream, and it cannot deadlock behind that read.
func (s *sseStream) close() {
	if s.resp != nil && s.resp.Body != nil {
		_ = s.resp.Body.Close()
	}
}

// line reads one line (including its newline) with a bounded timeout. On
// timeout it CLOSES the source and then drains the reader: the close aborts
// the connection, which unparks the goroutine sitting in ReadString, and
// waiting on the channel proves that goroutine has finished before the test
// fails. No reader outlives the failure, and the caller gets an error rather
// than the clean-looking EOF the old helper returned. The fatal is raised
// HERE, on the caller's own goroutine: FailNow from the reader goroutine
// would end a goroutine that has no test to end.
func (s *sseStream) line(timeout time.Duration) (string, error) {
	s.t.Helper()
	type res struct {
		line string
		err  error
	}
	ch := make(chan res, 1)
	go func() {
		l, e := s.br.ReadString('\n')
		ch <- res{l, e}
	}()
	select {
	case r := <-ch:
		return r.line, r.err
	case <-time.After(timeout):
		s.close()
		<-ch
		s.t.Fatalf("timed out after %v waiting for SSE line", timeout)
		return "", errSSETimeout
	}
}

// readLine reads one line with no deadline. It exists for the
// timing-sensitive perf helper, which must not pay the per-line goroutine and
// timer that bounding a read costs — its numbers are compared against a
// threshold — yet still must not hold the reader and the body apart.
func (s *sseStream) readLine() (string, error) { return s.br.ReadString('\n') }

// event reads one SSE event (lines until a blank line or EOF) and returns the
// trimmed non-empty lines plus whether EOF was reached. It is the exact
// behaviour of the helper it replaces; only the ownership moved.
func (s *sseStream) event(timeout time.Duration) (lines []string, eof bool) {
	s.t.Helper()
	for {
		line, err := s.line(timeout)
		if err == io.EOF {
			if line != "" {
				lines = append(lines, strings.TrimRight(line, "\r\n"))
			}
			return lines, true
		}
		if err != nil {
			s.t.Fatalf("read SSE line: %v", err)
		}
		trimmed := strings.TrimRight(line, "\r\n")
		if trimmed == "" {
			return lines, false
		}
		lines = append(lines, trimmed)
	}
}

// recordingT is the timeout regression's stand-in for *testing.T: it records
// the fatal where the real one would end the test, so the regression can
// assert that the timeout failed the test without failing itself. It is
// reached through the sseT seam and satisfies every method that seam names.
type recordingT struct {
	mu     sync.Mutex
	fatals int
	text   string
}

func (r *recordingT) Helper()        {}
func (r *recordingT) Cleanup(func()) {}
func (r *recordingT) Fatalf(format string, args ...any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.fatals++
	r.text = fmt.Sprintf(format, args...)
}

func (r *recordingT) failed() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.fatals
}

func (r *recordingT) message() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.text
}

// TestSSEStreamTimeoutUnblocksAndFails is the regression for the parked
// reader: a stream that never produces a line must fail the test promptly,
// must unblock the goroutine waiting on the connection, and must leave the
// source closed so nothing is left reading it. The failure is observed
// through the recording sink — asserting a real fatal would fail the very
// test doing the asserting. Both cases are bounded by the same wall clock,
// so a regression turns into a fast, named failure rather than a suite
// timeout.
func TestSSEStreamTimeoutUnblocksAndFails(t *testing.T) {
	t.Run("pipe", func(t *testing.T) {
		// An io.Pipe read blocks until the write end is closed, which is the
		// "stream produced no line" shape the timeout exists for, with no
		// network involved.
		pr, pw := io.Pipe()
		defer func() { _ = pw.Close() }()

		sink := &recordingT{}
		s := newSSEStream(sink, &http.Response{Body: pr})
		waitForSSETimeout(t, sink, s)

		// The source was closed, which is what unparks the reader: a closed
		// pipe reports ErrClosedPipe on any further read.
		if _, err := pr.Read(make([]byte, 1)); !errors.Is(err, io.ErrClosedPipe) {
			t.Fatalf("source not closed after timeout (read err = %v): the reader had nothing to unblock it", err)
		}
	})

	t.Run("http response", func(t *testing.T) {
		// The load-bearing case: a REAL net/http response whose read is
		// parked on a silent upstream. Closing such a body is what aborts its
		// connection — net/http's early-close path does not wait on the
		// body's own read lock — and that is the property this helper's
		// timeout depends on. If it did not hold, line would block here
		// instead of failing, and the suite would hang until its -timeout.
		stall := make(chan struct{})
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/event-stream")
			w.WriteHeader(http.StatusOK)
			w.(http.Flusher).Flush()
			<-stall
		}))
		defer srv.Close()
		defer close(stall) // unblock the handler before srv.Close waits on it

		resp, err := http.Get(srv.URL)
		if err != nil {
			t.Fatalf("get: %v", err)
		}
		sink := &recordingT{}
		s := newSSEStream(sink, resp)
		// line returns only after the parked read goroutine has finished, so
		// reaching this point proves the reader was unparked, not merely that
		// the timer fired.
		waitForSSETimeout(t, sink, s)
	})
}

// waitForSSETimeout drives one timeout through s.line and asserts the three
// things the timeout branch owes: it returns promptly (no parked reader), it
// fails the test exactly once, and the failure names the timeout rather than
// a clean EOF.
func waitForSSETimeout(t *testing.T, sink *recordingT, s *sseStream) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = s.line(50 * time.Millisecond)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("line did not return after its own timeout: the reader is still parked")
	}
	if got := sink.failed(); got != 1 {
		t.Fatalf("timeout raised %d failures, want exactly 1", got)
	}
	if msg := sink.message(); !strings.Contains(msg, "timed out") {
		t.Fatalf("failure message = %q, want the timeout wording", msg)
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
	// Same default-bearer rule as openJSON; goroutine-safe tests that need
	// to vary it pass an explicit Authorization header.
	if _, ok := hdr["Authorization"]; !ok {
		req.Header.Set("Authorization", "Bearer "+e2eAPIKey)
	}
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
