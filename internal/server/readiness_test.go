package server

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"openai-compatible-injector/internal/transport"
)

// The suite in this file is about one ordering promise and the state machine
// that keeps it: readiness goes false BEFORE the listener stops accepting.
//
// The two failure modes it exists to prevent are symmetric. A readiness
// endpoint that only flips when the listener closes tells a load balancer
// nothing it could not learn from a failed TCP connect, so every scheduler
// sweep that had already chosen this instance lands on a closed port — a
// connection error where a served request was available. A readiness endpoint
// that flips but never closes would be worse: a permanently draining instance
// that never stops. Both halves are pinned below, and so is the state
// machine's own monotonicity: once this process has said "not ready", nothing
// may make it say "ready" again.

// ---- helpers ----

// captureLogger returns a logger writing JSON lines into a buffer the test can
// read back as ordered events, at a level that shows debug.
func captureLogger() (zerolog.Logger, *logCapture) {
	c := &logCapture{}
	return zerolog.New(c).Level(zerolog.TraceLevel), c
}

type logCapture struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (c *logCapture) Write(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.buf.Write(p)
}

// messages returns the log slugs in the order they were emitted.
func (c *logCapture) messages(t *testing.T) []string {
	t.Helper()
	var out []string
	for _, line := range strings.Split(strings.TrimSpace(c.String()), "\n") {
		if line == "" {
			continue
		}
		var ev map[string]any
		if err := json.Unmarshal([]byte(line), &ev); err != nil {
			t.Fatalf("log line is not JSON: %q (%v)", line, err)
		}
		msg, _ := ev["message"].(string)
		out = append(out, msg)
	}
	return out
}

func (c *logCapture) String() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.buf.String()
}

// event returns the first event with the given slug, or nil.
func (c *logCapture) event(t *testing.T, msg string) map[string]any {
	t.Helper()
	for _, line := range strings.Split(strings.TrimSpace(c.String()), "\n") {
		if line == "" {
			continue
		}
		var ev map[string]any
		if err := json.Unmarshal([]byte(line), &ev); err != nil {
			t.Fatalf("log line is not JSON: %q (%v)", line, err)
		}
		if ev["message"] == msg {
			return ev
		}
	}
	return nil
}

// requireOrder asserts the given slugs all appear and appear in this order.
func (c *logCapture) requireOrder(t *testing.T, msgs ...string) {
	t.Helper()
	all := c.messages(t)
	at := 0
	for _, want := range msgs {
		idx := -1
		for i := at; i < len(all); i++ {
			if all[i] == want {
				idx = i
				break
			}
		}
		if idx < 0 {
			t.Fatalf("%q missing from the log trail after %v: %v", want, msgs[:at], all)
		}
		at = idx + 1
	}
}

// probeClient issues probes and API requests on their own connections. Keep
// alives are disabled deliberately: a pooled connection would hide exactly the
// transition under test — a request that rides an already-open socket proves
// nothing about whether the listener still accepts new ones.
func probeClient() *http.Client {
	return &http.Client{
		Timeout:   2 * time.Second,
		Transport: &http.Transport{DisableKeepAlives: true},
	}
}

// readyProbe reports the readiness of the server at addr: true only for 200
// with the "ok\n" body; false for a served 503; and an error when nothing
// answered at all.
func readyProbe(client *http.Client, addr string) (bool, error) {
	resp, err := client.Get("http://" + addr + readyPath)
	if err != nil {
		return false, err
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<16))
	if err != nil {
		return false, err
	}
	if resp.StatusCode == http.StatusOK && string(body) == readyBody {
		return true, nil
	}
	return false, nil
}

// freeAddrs reserves n ephemeral loopback ports AT ONCE and releases them
// together: releasing one before reserving the next lets the kernel hand the
// same port back, which would put two servers of the same scenario on one
// address.
func freeAddrs(t *testing.T, n int) []string {
	t.Helper()
	addrs := make([]string, 0, n)
	listeners := make([]net.Listener, 0, n)
	defer func() {
		for _, ln := range listeners {
			_ = ln.Close()
		}
	}()
	for range n {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("reserve port: %v", err)
		}
		listeners = append(listeners, ln)
		addrs = append(addrs, ln.Addr().String())
	}
	return addrs
}

// waitForReady blocks until the server at addr advertises readiness.
func waitForReady(t *testing.T, addr string) {
	t.Helper()
	client := probeClient()
	deadline := time.Now().Add(10 * time.Second)
	for {
		ready, err := readyProbe(client, addr)
		if err == nil && ready {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("server at %s never became ready (last err: %v)", addr, err)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// chatOnce issues one chat completion through the proxy, returning the status
// and the upstream model the body was rewritten to. A non-nil error means
// nothing answered — the client-visible shape of "the listener is gone".
func chatOnce(client *http.Client, addr string) (int, string, error) {
	req, err := http.NewRequest(http.MethodPost, "http://"+addr+"/v1/chat/completions",
		strings.NewReader(`{"model":"test-model","messages":[{"role":"user","content":"hi"}]}`))
	if err != nil {
		return 0, "", err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+serverTestAPIKey)
	resp, err := client.Do(req)
	if err != nil {
		return 0, "", err
	}
	defer func() { _ = resp.Body.Close() }()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return resp.StatusCode, "", err
	}
	model := ""
	var got map[string]any
	if json.Unmarshal(b, &got) == nil {
		model, _ = got["model"].(string)
	}
	return resp.StatusCode, model, nil
}

// jsonUpstream answers every chat completion with a 200 JSON body naming the
// given upstream model.
func jsonUpstream(upstreamModel string) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"id":"c1","model":"`+upstreamModel+`","choices":[{"index":0,"message":{"role":"assistant","content":"hi"},"finish_reason":"stop"}]}`)
	}))
}

// ---- the state machine ----

// TestLifecycleTransitionsAreMonotone pins the machine itself: forward
// transitions are taken, backward ones are refused, and a refused transition
// leaves the state where it was. The CAS that makes this hold against a
// concurrent writer is why the refusal cannot be a silent no-op.
func TestLifecycleTransitionsAreMonotone(t *testing.T) {
	lc := newLifecycle()
	if got := lc.load(); got != stateStarting {
		t.Fatalf("new lifecycle state = %v, want starting (ready must never be a zero value)", got)
	}
	if lc.ready() {
		t.Error("a lifecycle that has not started reports ready")
	}

	if !lc.markReady() || !lc.ready() {
		t.Fatal("markReady did not move a starting process to ready")
	}
	// A repeated markReady is harmless: it changes nothing and reports true.
	if !lc.markReady() || lc.load() != stateReady {
		t.Fatal("a repeated markReady did not leave the process ready")
	}

	if !lc.beginDraining() || lc.ready() {
		t.Fatal("beginDraining did not stop advertising readiness")
	}
	// The one that matters: a late caller cannot re-advertise an instance
	// that has already told its load balancer to stop.
	if lc.markReady() {
		t.Error("markReady succeeded after the process began draining")
	}
	if lc.load() != stateDraining {
		t.Errorf("a refused markReady moved the state to %v", lc.load())
	}
	if !lc.beginDraining() || lc.load() != stateDraining {
		t.Error("a repeated beginDraining moved the state")
	}

	if !lc.markStopped() || lc.load() != stateStopped || lc.ready() {
		t.Fatal("markStopped did not reach the terminal state")
	}

	// Startup to draining to stopped, without ever advertising readiness: a
	// process signalled before it served must still answer "not ready".
	early := newLifecycle()
	if !early.beginDraining() || early.load() != stateDraining {
		t.Error("a starting process could not go straight to draining")
	}
	if !early.markStopped() || early.load() != stateStopped {
		t.Error("a draining process could not reach stopped")
	}
}

// TestReadyzAnswersPerState pins the endpoint's answer for every state and
// every method. A probe reads the status code; a human reads the body; and a
// POST is refused without inventing an OpenAI-shaped envelope for an
// endpoint that is not part of that surface.
func TestReadyzAnswersPerState(t *testing.T) {
	cases := []struct {
		state state
		code  int
		body  string
	}{
		{stateStarting, http.StatusServiceUnavailable, "starting\n"},
		{stateReady, http.StatusOK, "ok\n"},
		{stateDraining, http.StatusServiceUnavailable, "draining\n"},
		{stateStopped, http.StatusServiceUnavailable, "stopped\n"},
	}
	for _, tc := range cases {
		lc := newLifecycle()
		lc.state.Store(int32(tc.state))
		rec := httptest.NewRecorder()
		lc.serveReadyz(rec, httptest.NewRequest(http.MethodGet, readyPath, nil))
		if rec.Code != tc.code || rec.Body.String() != tc.body {
			t.Errorf("state %v: got %d %q, want %d %q", tc.state, rec.Code, rec.Body.String(), tc.code, tc.body)
		}
		if got := rec.Header().Get("Cache-Control"); got != "no-store" {
			t.Errorf("state %v: Cache-Control = %q, want no-store (a replayed 200 routes traffic into a closing socket)", tc.state, got)
		}
	}

	for _, method := range []string{http.MethodPost, http.MethodPut, http.MethodDelete, http.MethodHead} {
		lc := newLifecycle()
		lc.state.Store(int32(stateReady))
		rec := httptest.NewRecorder()
		lc.serveReadyz(rec, httptest.NewRequest(method, readyPath, nil))
		if rec.Code != http.StatusMethodNotAllowed {
			t.Errorf("%s %s: status %d, want 405", method, readyPath, rec.Code)
		}
		if got := rec.Header().Get("Allow"); got != http.MethodGet {
			t.Errorf("%s: Allow = %q, want GET", method, got)
		}
	}
}

// TestLifecycleRouterMatchesReadyPathExactly pins the routing rule: the
// readiness path is exact-match only, and everything else — "/readyz/"
// included — reaches the API handler, which owns the JSON 404 envelope for
// unknown paths. A readiness endpoint that answered under a trailing slash
// would be a second undocumented spelling of the same surface.
func TestLifecycleRouterMatchesReadyPathExactly(t *testing.T) {
	lc := newLifecycle()
	lc.state.Store(int32(stateReady))
	reached := 0
	rt := lifecycleRouter{lc: lc, api: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reached++
		w.WriteHeader(http.StatusTeapot)
	})}

	for _, path := range []string{"/readyz/", "/readyz/x", "/readyzz", "/v1/readyz"} {
		reached = 0
		rec := httptest.NewRecorder()
		rt.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		if reached != 1 {
			t.Errorf("%s was handled by the lifecycle router, want the API handler", path)
		}
		if rec.Code != http.StatusTeapot {
			t.Errorf("%s status = %d, want the API handler's answer", path, rec.Code)
		}
	}

	reached = 0
	rec := httptest.NewRecorder()
	rt.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, readyPath, nil))
	if reached != 0 {
		t.Error("/readyz reached the API handler")
	}
	if rec.Code != http.StatusOK || rec.Body.String() != readyBody {
		t.Errorf("/readyz = %d %q, want 200 %q", rec.Code, rec.Body.String(), readyBody)
	}
}

// ---- the ordering promise ----

// TestReadinessFlipsBeforeTheListenerStopsAccepting is the core invariant, in
// the only form an outside observer can check it: the first observation of
// "not ready" strictly precedes the first observation of "nothing answered",
// and at least one request was SERVED after readiness dropped — which is what
// proves the listener was still accepting while probes were already reading
// the refusal.
//
// The probe and the request share one loop on purpose: two independent loops
// could both be right while the order between them is wrong.
func TestReadinessFlipsBeforeTheListenerStopsAccepting(t *testing.T) {
	up := jsonUpstream("upstream-name")
	defer up.Close()

	const grace = 2 * time.Second
	addr := freeAddr(t)
	srv := New(testStore(t, up.URL+"/v1"), transport.NewRegistry(), nil, nil, nil, addr, grace, testLogger(t))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runErr := make(chan error, 1)
	go func() { runErr <- srv.Run(ctx) }()
	waitForReady(t, addr)

	client := probeClient()
	cancel()

	var (
		firstUnready time.Time
		firstRefused time.Time
		servedAfter  int
	)
	deadline := time.Now().Add(grace + 5*time.Second)
	for time.Now().Before(deadline) {
		ready, err := readyProbe(client, addr)
		switch {
		case err != nil:
			if firstRefused.IsZero() {
				firstRefused = time.Now()
			}
		case !ready:
			if firstUnready.IsZero() {
				firstUnready = time.Now()
			}
			// A request issued the moment readiness dropped: this is the
			// scheduler sweep that had already chosen this instance.
			status, model, err := chatOnce(client, addr)
			if err == nil && status == http.StatusOK {
				servedAfter++
				if model != "test-model" {
					t.Errorf("served request model = %q, want the rewritten public name", model)
				}
			}
		}
		if !firstUnready.IsZero() && !firstRefused.IsZero() {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}

	if firstUnready.IsZero() {
		t.Fatal("readiness never went false")
	}
	if firstRefused.IsZero() {
		t.Fatal("the listener never stopped accepting within the shutdown budget")
	}
	if !firstUnready.Before(firstRefused) {
		t.Errorf("readiness dropped %v AFTER the listener stopped accepting: a load balancer can only learn about the drain from a refused connection",
			firstRefused.Sub(firstUnready))
	}
	if servedAfter == 0 {
		t.Error("no request was served after readiness dropped: the readiness transition was not a head start, it was the shutdown itself")
	}

	select {
	case err := <-runErr:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("Run did not return")
	}
	if got := srv.lc.load(); got != stateStopped {
		t.Errorf("final lifecycle state = %v, want stopped", got)
	}
}

// TestReadinessNeverComesBack is the same promise under concurrency: many
// probe goroutines hammer /readyz across the whole shutdown, and every one of
// them must observe a monotone sequence. A single ready-after-unready in any
// goroutine is a routing flap — traffic sent to an instance that is going
// away after it said so.
//
// Run under -race this also exercises the transition itself: the state is
// read by probe goroutines and written by the shutdown path with no lock
// between them.
func TestReadinessNeverComesBack(t *testing.T) {
	up := jsonUpstream("upstream-name")
	defer up.Close()

	const grace = 1500 * time.Millisecond
	addr := freeAddr(t)
	srv := New(testStore(t, up.URL+"/v1"), transport.NewRegistry(), nil, nil, nil, addr, grace, testLogger(t))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runErr := make(chan error, 1)
	go func() { runErr <- srv.Run(ctx) }()
	waitForReady(t, addr)

	const probes = 8
	type seq struct {
		saw []bool
	}
	results := make([]seq, probes)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := range probes {
		wg.Add(1)
		go func() {
			defer wg.Done()
			client := probeClient()
			<-start
			for {
				ready, err := readyProbe(client, addr)
				if err != nil {
					return // the listener is gone; the sequence is complete
				}
				results[i].saw = append(results[i].saw, ready)
			}
		}()
	}
	close(start)
	time.Sleep(100 * time.Millisecond) // let the probes observe readiness first
	cancel()

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(15 * time.Second):
		t.Fatal("probe goroutines did not finish: a probe is parked on a listener that is neither serving nor gone")
	}

	var sawReady, sawUnready, both int
	for i, s := range results {
		dropped := false
		for _, ready := range s.saw {
			if ready {
				if dropped {
					t.Fatalf("probe %d observed ready AFTER observing not-ready: readiness flapped\nsequence: %v", i, s.saw)
				}
				sawReady++
			} else {
				dropped = true
				sawUnready++
			}
		}
		if dropped {
			both++
		}
	}
	if sawReady == 0 {
		t.Error("no probe ever observed readiness")
	}
	if sawUnready == 0 {
		t.Error("no probe observed the readiness drop: the probes all raced ahead of the shutdown")
	}
	if both == 0 {
		t.Error("no probe observed the whole transition")
	}

	if err := <-runErr; err != nil {
		t.Fatalf("Run: %v", err)
	}
}

// ---- the head start ----

// staleRouter models the load balancer the head start exists for: one whose
// view of readiness is at most one probe interval old. It hands back its
// cached choice until the interval expires, then re-probes.
type staleRouter struct {
	interval time.Duration
	addrs    []string
	client   *http.Client

	mu     sync.Mutex
	lastAt time.Time
	ready  map[string]bool
}

func (r *staleRouter) pick() (string, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if time.Since(r.lastAt) >= r.interval {
		for _, a := range r.addrs {
			ok, err := readyProbe(r.client, a)
			r.ready[a] = err == nil && ok
		}
		r.lastAt = time.Now()
	}
	for _, a := range r.addrs {
		if r.ready[a] {
			return a, true
		}
	}
	return "", false
}

// TestRolloutDrainsOneInstanceWhileTheOtherServes is the two-backend rollout:
// A is stopped while B keeps serving, and the load balancer's view is one
// probe interval stale throughout — the state every real rollout is in.
//
// The property is that the stale view never costs a request. Every request
// issued in the window between A's readiness flipping and A's listener
// closing is SERVED, because the listener is still there; the balancer's next
// sweep moves to B well before A's socket goes away. Without the head start
// the two events coincide, and 100% of that window's traffic is a refused
// connection mid-rollout.
func TestRolloutDrainsOneInstanceWhileTheOtherServes(t *testing.T) {
	up := jsonUpstream("upstream-name")
	defer up.Close()
	store := testStore(t, up.URL+"/v1")

	// The head start is grace/2, capped at the internal constant: 1s here.
	const grace = 2 * time.Second
	head := grace / 2
	// The balancer's view is this stale — comfortably inside the head start,
	// which is exactly the relation a deployment must satisfy.
	const staleInterval = 200 * time.Millisecond

	addrs := freeAddrs(t, 2)
	addrA, addrB := addrs[0], addrs[1]
	srvA := New(store, transport.NewRegistry(), nil, nil, nil, addrA, grace, testLogger(t))
	srvB := New(store, transport.NewRegistry(), nil, nil, nil, addrB, grace, testLogger(t))

	ctxA, cancelA := context.WithCancel(context.Background())
	defer cancelA()
	ctxB, cancelB := context.WithCancel(context.Background())
	defer cancelB()
	runA := make(chan error, 1)
	runB := make(chan error, 1)
	go func() { runA <- srvA.Run(ctxA) }()
	go func() { runB <- srvB.Run(ctxB) }()
	waitForReady(t, addrA)
	waitForReady(t, addrB)

	client := probeClient()
	router := &staleRouter{
		interval: staleInterval,
		addrs:    []string{addrA, addrB},
		client:   client,
		ready:    map[string]bool{},
	}

	// Warm the balancer: it now believes A is the healthy choice.
	chosen, ok := router.pick()
	if !ok || chosen != addrA {
		t.Fatalf("warm pick = %q ok=%v, want %s (the first ready backend)", chosen, ok, addrA)
	}

	// Stop A, and watch its readiness directly so the test can measure the
	// window against the instant it actually flipped.
	cancelA()
	flip := time.Time{}
	pollDeadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(pollDeadline) {
		ready, err := readyProbe(client, addrA)
		if err == nil && !ready {
			flip = time.Now()
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	if flip.IsZero() {
		t.Fatal("instance A never stopped advertising readiness")
	}

	// Everything the stale balancer routes from here on must be served, for
	// the whole staleness window — and it must still be routing to A for part
	// of it, because its view has not expired yet.
	window := staleInterval + 100*time.Millisecond
	var servedA, servedB, failed int
	for time.Since(flip) < window {
		addr, ok := router.pick()
		if !ok {
			t.Fatalf("the balancer found no ready backend %v into the rollout", time.Since(flip))
		}
		status, _, err := chatOnce(client, addr)
		switch {
		case err != nil:
			failed++
			t.Errorf("request to %s failed %v into the rollout: %v", addr, time.Since(flip), err)
		case status != http.StatusOK:
			failed++
			t.Errorf("request to %s returned %d, want 200", addr, status)
		default:
			if addr == addrA {
				servedA++
			} else {
				servedB++
			}
		}
	}
	if failed > 0 {
		t.Fatalf("%d requests failed during the rollout window: the readiness transition did not precede the listener closing", failed)
	}
	if servedA == 0 {
		t.Error("no request was routed to the draining instance after its readiness dropped: the test did not exercise a stale view")
	}
	if servedB == 0 {
		t.Error("no request was routed to the healthy instance: the balancer never moved")
	}

	// The other half of the promise: A does stop. Its listener must be gone
	// within the head start, and not before it — a "head start" that is
	// actually an immediate close is the defect this test exists for.
	goneAt := time.Time{}
	deadline := time.Now().Add(head + 5*time.Second)
	for time.Now().Before(deadline) {
		if _, err := readyProbe(client, addrA); err != nil {
			goneAt = time.Now()
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if goneAt.IsZero() {
		t.Fatal("instance A kept answering long after its head start and drain budget")
	}
	if elapsed := goneAt.Sub(flip); elapsed < head/2 {
		t.Errorf("A stopped accepting %v after it dropped readiness, want about %v: the head start was not taken", elapsed.Round(time.Millisecond), head)
	}

	// B is untouched by any of this and must still be serving.
	if status, _, err := chatOnce(client, addrB); err != nil || status != http.StatusOK {
		t.Fatalf("the surviving instance stopped serving: status=%d err=%v", status, err)
	}

	if err := <-runA; err != nil {
		t.Fatalf("A's Run: %v", err)
	}
	cancelB()
	if err := <-runB; err != nil {
		t.Fatalf("B's Run: %v", err)
	}
}

// ---- readiness is about this process, not its dependencies ----

// TestReadinessIgnoresUpstreamHealth pins the boundary the endpoint must not
// cross: readiness says "send me traffic", not "every provider I front is
// healthy". A dead upstream is a per-request 502 — the models that still work
// keep working, and emptying a load balancer's pool because one upstream is
// down would take the whole service down with it.
func TestReadinessIgnoresUpstreamHealth(t *testing.T) {
	// A port nothing listens on: every dial fails immediately.
	dead := freeAddr(t)
	addr := freeAddr(t)
	srv := New(testStore(t, "http://"+dead+"/v1"), transport.NewRegistry(), nil, nil, nil, addr, 2*time.Second, testLogger(t))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runErr := make(chan error, 1)
	go func() { runErr <- srv.Run(ctx) }()
	waitForReady(t, addr)

	client := probeClient()
	status, _, err := chatOnce(client, addr)
	if err != nil {
		t.Fatalf("request against a dead upstream did not answer at all: %v", err)
	}
	if status == http.StatusOK {
		t.Fatalf("a request to a dead upstream returned %d", status)
	}
	// The provider is down and the proxy is still perfectly ready to be sent
	// more traffic.
	if ready, err := readyProbe(client, addr); err != nil || !ready {
		t.Fatalf("readiness = %v (err %v) while an upstream was down, want ready", ready, err)
	}

	cancel()
	if err := <-runErr; err != nil {
		t.Fatalf("Run: %v", err)
	}
}

// ---- the event trail ----

// TestShutdownEventOrder pins the log order an operator reads a rollout by:
// readiness_ready when the process starts advertising, readiness_unready at
// the instant it stops (before anything is closed), drain_started when the
// in-flight budget begins, and the readiness state ending stopped.
//
// readiness_unready carrying the propagation head start is what makes the
// shutdown explainable afterwards: "traffic stopped here, the socket stayed
// open this much longer".
func TestShutdownEventOrder(t *testing.T) {
	up := jsonUpstream("upstream-name")
	defer up.Close()

	log, cap := captureLogger()
	const grace = 2 * time.Second
	addr := freeAddr(t)
	srv := New(testStore(t, up.URL+"/v1"), transport.NewRegistry(), nil, nil, nil, addr, grace, log)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runErr := make(chan error, 1)
	go func() { runErr <- srv.Run(ctx) }()
	waitForReady(t, addr)
	cancel()
	if err := <-runErr; err != nil {
		t.Fatalf("Run: %v", err)
	}

	cap.requireOrder(t, "listener_ready", "readiness_ready", "readiness_unready", "drain_started")

	unready := cap.event(t, "readiness_unready")
	if unready == nil {
		t.Fatalf("readiness_unready missing:\n%s", cap.String())
	}
	if unready["level"] != "info" {
		t.Errorf("readiness_unready level = %v, want info (a state change an operator must see)", unready["level"])
	}
	// The whole budget and the head start drawn from it, both named.
	if unready["grace"] != float64(grace.Milliseconds()) {
		t.Errorf("readiness_unready grace = %v, want %d", unready["grace"], grace.Milliseconds())
	}
	if unready["propagation"] != float64((grace / 2).Milliseconds()) {
		t.Errorf("readiness_unready propagation = %v, want %d", unready["propagation"], (grace / 2).Milliseconds())
	}
	drain := cap.event(t, "drain_started")
	if drain["grace"] != float64(grace.Milliseconds()) {
		t.Errorf("drain_started grace = %v, want the whole post-signal budget %d", drain["grace"], grace.Milliseconds())
	}
	if drain["drain"] != float64((grace / 2).Milliseconds()) {
		t.Errorf("drain_started drain = %v, want the budget left after the head start %d", drain["drain"], (grace / 2).Milliseconds())
	}
	if got := srv.lc.load(); got != stateStopped {
		t.Errorf("final state = %v, want stopped", got)
	}
}

// TestPropagationIsCappedByTheDrainBudget pins the arithmetic that keeps the
// head start safe on every configuration rather than only on the default one:
// it never exceeds half of grace, so the in-flight drain always keeps the
// majority of the post-signal budget, and a tiny delay (a test, or an
// operator asking for a fast stop) is never dominated by it.
func TestPropagationIsCappedByTheDrainBudget(t *testing.T) {
	cases := []struct {
		grace time.Duration
		want  time.Duration
	}{
		{60 * time.Second, readinessPropagation},
		{2 * readinessPropagation, readinessPropagation},
		{8 * time.Second, 4 * time.Second},
		{2 * time.Second, time.Second},
		{time.Second, 500 * time.Millisecond},
		{0, 0},
		{-time.Second, 0},
	}
	for _, tc := range cases {
		s := &Server{grace: tc.grace}
		if got := s.propagation(); got != tc.want {
			t.Errorf("grace %v: propagation = %v, want %v", tc.grace, got, tc.want)
		}
	}
	// The default deployment: the head start fits well inside the documented
	// stop_grace_period alongside the drain and the usage flush.
	if readinessPropagation >= 55*time.Second/2 {
		t.Errorf("the default head start %v is too much of the default budget", readinessPropagation)
	}
}

// TestListenFailureLeavesTheProcessUnready pins the non-signal exit: a
// listener that cannot be created must not leave a process advertising
// readiness it never had.
func TestListenFailureLeavesTheProcessUnready(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve port: %v", err)
	}
	occupied := ln.Addr().String()
	defer func() { _ = ln.Close() }()

	srv := New(testStore(t, "http://127.0.0.1:9/v1"), transport.NewRegistry(), nil, nil, nil, occupied, time.Second, testLogger(t))
	if err := srv.Run(context.Background()); err == nil {
		t.Fatalf("Run on occupied %s: want a listen error", occupied)
	}
	if srv.lc.ready() {
		t.Error("a server that never listened reports ready")
	}
	if got := srv.lc.load(); got != stateStarting {
		t.Errorf("state after a listen failure = %v, want starting (nothing was ever served)", got)
	}
}
