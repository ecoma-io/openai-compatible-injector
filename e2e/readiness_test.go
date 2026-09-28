package e2e_test

import (
	"io"
	"net/http"
	"os/exec"
	"syscall"
	"testing"
	"time"
)

// The readiness contract, black-box, over the real binary: GET /readyz reports
// whether this process should be sent traffic, it goes false BEFORE the
// listener goes away, GET /healthz keeps reporting a live process throughout,
// and the healthcheck subcommand — the same one the container image's
// HEALTHCHECK runs — follows readiness rather than liveness.
//
// The distinction is the whole point of the two endpoints. A draining instance
// is alive (restarting it would be wrong) and must not be sent new traffic
// (routing to it is also wrong). A probe that cannot tell those apart either
// restarts a process that was stopping correctly or keeps feeding a socket
// that is about to close.

// readyClient issues probes and API requests on their own connections: a
// pooled connection would hide the transition under test, because a request
// that rides an already-open socket proves nothing about whether the listener
// still accepts new ones.
func readyClient() *http.Client {
	return &http.Client{
		Timeout:   2 * time.Second,
		Transport: &http.Transport{DisableKeepAlives: true},
	}
}

// readyAt reports the process's readiness: true only for 200 "ok\n", false for
// a served 503, and an error when nothing answered.
func readyAt(client *http.Client, addr string) (bool, error) {
	resp, err := client.Get("http://" + addr + "/readyz")
	if err != nil {
		return false, err
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<16))
	if err != nil {
		return false, err
	}
	return resp.StatusCode == http.StatusOK && string(body) == "ok\n", nil
}

// healthzAt reports liveness, and fails the test if the endpoint answers with
// anything but the documented 200 "ok\n": liveness has no third state.
func healthzAt(t *testing.T, client *http.Client, addr string) bool {
	t.Helper()
	resp, err := client.Get("http://" + addr + "/healthz")
	if err != nil {
		return false
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<16))
	if err != nil {
		return false
	}
	if resp.StatusCode != http.StatusOK || string(body) != "ok\n" {
		t.Fatalf("GET /healthz = %d %q, want 200 %q", resp.StatusCode, body, "ok\n")
	}
	return true
}

// waitReady polls /readyz until the process advertises readiness.
func (p *proc) waitReady(t *testing.T, timeout time.Duration) {
	t.Helper()
	client := readyClient()
	deadline := time.Now().Add(timeout)
	for {
		ready, err := readyAt(client, p.addr)
		if err == nil && ready {
			return
		}
		select {
		case <-p.done:
			t.Fatalf("process exited before becoming ready; stderr:\n%s", p.stderr.String())
		case <-time.After(10 * time.Millisecond):
		}
		if time.Now().After(deadline) {
			t.Fatalf("never became ready within %v (last err %v); stderr:\n%s", timeout, err, p.stderr.String())
		}
	}
}

// chatAt issues one chat completion and returns the status. A non-nil error
// means nothing answered — the client-visible shape of a closed listener.
func chatAt(client *http.Client, addr string) (int, error) {
	status, _, _, err := postJSONRaw(addr, "/v1/chat/completions", chatBody, nil)
	return status, err
}

// TestReadyzDropsBeforeTheListenerCloses drives the ordering promise over the
// real binary and the real signal path: after SIGTERM the process must answer
// 503 on /readyz while it still answers 200 on /healthz and still SERVES a
// request, and only then close the listener.
func TestReadyzDropsBeforeTheListenerCloses(t *testing.T) {
	up := newFakeUpstream(t)
	up.setHandler(jsonChatHandler(chatUpstream))
	// 4s of grace is a 2s head start, which is far more than the loop below
	// needs; the assertions are on order, not on the size of the window.
	p := startSubprocess(t, startOpts{
		yaml:     runtimeYAML(chatPublic, up.url()+"/v1", chatUpstream, ""),
		logLevel: "debug", // the whole lifecycle trail, listener_ready included
		grace:    "4s",
	})
	p.waitReady(t, 5*time.Second)

	client := readyClient()
	if status, err := chatAt(client, p.addr); err != nil || status != http.StatusOK {
		t.Fatalf("warm-up request: status=%d err=%v", status, err)
	}

	p.signal(syscall.SIGTERM)

	var (
		firstUnready time.Time
		firstClosed  time.Time
		servedAfter  int
	)
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		ready, err := readyAt(client, p.addr)
		if err != nil {
			if firstClosed.IsZero() {
				firstClosed = time.Now()
			}
		} else if !ready {
			if firstUnready.IsZero() {
				firstUnready = time.Now()
			}
			// Liveness must still hold, and a request that arrives the moment
			// readiness drops must still be served: this is the sweep that had
			// already been scheduled against the old view.
			if !healthzAt(t, client, p.addr) {
				t.Error("/healthz stopped answering while /readyz reported the drain: liveness and readiness are not the same question")
			}
			if status, err := chatAt(client, p.addr); err == nil && status == http.StatusOK {
				servedAfter++
			}
		}
		if !firstUnready.IsZero() && !firstClosed.IsZero() {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if firstUnready.IsZero() {
		t.Fatalf("/readyz never reported the drain; stderr:\n%s", p.stderr.String())
	}
	if firstClosed.IsZero() {
		t.Fatal("the listener never closed after the readiness drop")
	}
	if !firstUnready.Before(firstClosed) {
		t.Errorf("readiness dropped %v after the listener closed: a load balancer could only learn about the drain from a refused connection",
			firstClosed.Sub(firstUnready))
	}
	if servedAfter == 0 {
		t.Error("no request was served after readiness dropped: the process closed as fast as it went unready")
	}

	if code, err := p.waitExit(t, 15*time.Second); err != nil || code != 0 {
		t.Fatalf("clean shutdown after SIGTERM: code=%d err=%v", code, err)
	}

	// The trail, in order, and shutdown_complete LAST: nothing may log after
	// the process announced its exit — a reload or a component still running
	// there would be a lifecycle that ended on paper only.
	evs := parseLogEvents(t, p.stderr.String())
	order := []string{"listener_ready", "readiness_ready", "readiness_unready", "drain_started", "shutdown_complete"}
	at := 0
	for _, want := range order {
		idx := -1
		for i := at; i < len(evs); i++ {
			if evs[i]["message"] == want {
				idx = i
				break
			}
		}
		if idx < 0 {
			t.Fatalf("%q missing or out of order in the shutdown trail after %v", want, order[:at])
		}
		at = idx + 1
	}
	if at != len(evs) {
		t.Errorf("%d event(s) logged after shutdown_complete: %v", len(evs)-at, evs[at:])
	}
	unready := shutdownEvent(t, p, "readiness_unready")
	if unready["level"] != "info" {
		t.Errorf("readiness_unready level = %v, want info", unready["level"])
	}
	if unready["propagation"] != float64(2000) {
		t.Errorf("readiness_unready propagation = %v, want 2000ms (half of a 4s budget)", unready["propagation"])
	}
}

// TestHealthcheckSubcommandFollowsReadiness pins what the container probe
// actually probes. The image's HEALTHCHECK runs this subcommand, so its answer
// decides when an orchestrator stops routing to (or restarts) this container;
// while the process is draining it must fail, and while the process is serving
// normally it must pass.
func TestHealthcheckSubcommandFollowsReadiness(t *testing.T) {
	up := newFakeUpstream(t)
	up.setHandler(jsonChatHandler(chatUpstream))
	port := freePort(t)
	p := startSubprocess(t, startOpts{
		listen: "127.0.0.1:" + port,
		yaml:   runtimeYAML(chatPublic, up.url()+"/v1", chatUpstream, ""),
		grace:  "6s",
	})
	p.waitReady(t, 5*time.Second)

	probe := func() error {
		cmd := exec.Command(binPath, "healthcheck")
		cmd.Env = append(cmd.Environ(), "OAICR_LISTEN=127.0.0.1:"+port)
		return cmd.Run()
	}
	if err := probe(); err != nil {
		t.Fatalf("healthcheck against a ready process failed: %v", err)
	}

	p.signal(syscall.SIGTERM)
	// Wait until the process has actually observed the signal, so the probe
	// below cannot race it and pass on a pre-drain answer.
	client := readyClient()
	flipDeadline := time.Now().Add(5 * time.Second)
	for {
		ready, err := readyAt(client, p.addr)
		if (err == nil && !ready) || err != nil {
			break
		}
		if time.Now().After(flipDeadline) {
			t.Fatalf("the process never reacted to SIGTERM; stderr:\n%s", p.stderr.String())
		}
		time.Sleep(5 * time.Millisecond)
	}

	// Still inside the head start (6s budget, 3s window), and the probe must
	// already refuse: this is the container-level view of "stop routing here".
	if err := probe(); err == nil {
		t.Error("healthcheck succeeded against a draining process: the container probe follows liveness, not readiness")
	}

	if code, err := p.waitExit(t, 20*time.Second); err != nil || code != 0 {
		t.Fatalf("clean shutdown: code=%d err=%v", code, err)
	}
	// Once the process is gone the probe fails too — it must not report a
	// stopped process as healthy.
	if err := probe(); err == nil {
		t.Error("healthcheck succeeded against a process that has exited")
	}
}

// staleBalancer models the load balancer a rollout is always performed behind:
// one whose view of readiness is at most one probe interval old.
type staleBalancer struct {
	interval time.Duration
	addrs    []string
	client   *http.Client

	lastAt time.Time
	ready  map[string]bool
}

func newStaleBalancer(interval time.Duration, addrs []string, client *http.Client) *staleBalancer {
	return &staleBalancer{interval: interval, addrs: addrs, client: client, ready: map[string]bool{}}
}

// pick returns the first backend its (possibly stale) view calls ready.
func (b *staleBalancer) pick() (string, bool) {
	if time.Since(b.lastAt) >= b.interval {
		for _, a := range b.addrs {
			ok, err := readyAt(b.client, a)
			b.ready[a] = err == nil && ok
		}
		b.lastAt = time.Now()
	}
	for _, a := range b.addrs {
		if b.ready[a] {
			return a, true
		}
	}
	return "", false
}

// TestTwoBackendRolloutKeepsServing is the rollout scenario end to end: two
// real processes behind a balancer whose view lags by one probe interval, one
// of them stopped mid-flight. Not a single request may fail — the head start
// is the whole reason the drain is not simply a close.
func TestTwoBackendRolloutKeepsServing(t *testing.T) {
	up := newFakeUpstream(t)
	up.setHandler(jsonChatHandler(chatUpstream))
	upstream := up.url() + "/v1"

	// 4s of grace is a 2s head start; the balancer lags 250ms, comfortably
	// inside it — the relation a deployment must satisfy.
	const grace = "4s"
	const head = 2 * time.Second
	const lag = 250 * time.Millisecond

	a := startSubprocess(t, startOpts{
		yaml:     runtimeYAML(chatPublic, upstream, chatUpstream, ""),
		logLevel: "info", // readiness_unready is asserted below
		grace:    grace,
	})
	b := startSubprocess(t, startOpts{
		yaml:  runtimeYAML(chatPublic, upstream, chatUpstream, ""),
		grace: grace,
	})
	a.waitReady(t, 5*time.Second)
	b.waitReady(t, 5*time.Second)

	client := readyClient()
	balancer := newStaleBalancer(lag, []string{a.addr, b.addr}, client)
	if chosen, ok := balancer.pick(); !ok || chosen != a.addr {
		t.Fatalf("warm pick = %q ok=%v, want the first ready backend %s", chosen, ok, a.addr)
	}

	a.signal(syscall.SIGTERM)
	flip := time.Time{}
	flipDeadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(flipDeadline) {
		if ready, err := readyAt(client, a.addr); err == nil && !ready {
			flip = time.Now()
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if flip.IsZero() {
		t.Fatal("instance A never stopped advertising readiness")
	}

	// For one full lag plus a margin, every request the stale balancer routes
	// must be served — and some of them must still be routed to A, which is
	// only possible because A is still accepting.
	window := lag + 100*time.Millisecond
	var servedA, servedB int
	for time.Since(flip) < window {
		addr, ok := balancer.pick()
		if !ok {
			t.Fatalf("the balancer found no ready backend %v into the rollout", time.Since(flip))
		}
		status, err := chatAt(client, addr)
		if err != nil {
			t.Fatalf("request to %s failed %v into the rollout: %v", addr, time.Since(flip), err)
		}
		if status != http.StatusOK {
			t.Fatalf("request to %s returned %d, want 200", addr, status)
		}
		if addr == a.addr {
			servedA++
		} else {
			servedB++
		}
	}
	if servedA == 0 {
		t.Error("nothing was routed to the draining instance: the balancer's view was not stale, so the head start was never exercised")
	}
	if servedB == 0 {
		t.Error("nothing was routed to the healthy instance: the balancer never moved")
	}

	// A finishes its stop, without a request ever noticing. The exit is still
	// clean and the head start was actually taken rather than skipped.
	if code, err := a.waitExit(t, 20*time.Second); err != nil || code != 0 {
		t.Fatalf("instance A: code=%d err=%v", code, err)
	}
	unready := shutdownEvent(t, a, "readiness_unready")
	if unready == nil {
		t.Fatalf("A's trail has no readiness_unready; stderr:\n%s", a.stderr.String())
	}
	if unready["propagation"] != float64(head.Milliseconds()) {
		t.Errorf("A's head start = %v, want %d", unready["propagation"], head.Milliseconds())
	}

	// B is untouched and still serving.
	if status, err := chatAt(client, b.addr); err != nil || status != http.StatusOK {
		t.Fatalf("the surviving instance stopped serving: status=%d err=%v", status, err)
	}
	if up.count() == 0 {
		t.Error("no request reached the upstream: the rollout drove nothing")
	}
}
