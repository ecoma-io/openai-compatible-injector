package e2e_test

import (
	"net/http"
	"os"
	"strings"
	"syscall"
	"testing"
	"time"
)

// The shutdown matrix beyond the drain scenarios in scenarios_test.go: the
// second-signal force exit, and the shutdown lifecycle events themselves —
// drain_started / drain_deadline_exceeded / shutdown_complete are part of the
// observable contract of a graceful stop.

// shutdownEvent returns the first event with the given message, or nil.
func shutdownEvent(t *testing.T, p *proc, msg string) map[string]any {
	t.Helper()
	for _, ev := range parseLogEvents(t, p.stderr.String()) {
		if ev["message"] == msg {
			return ev
		}
	}
	return nil
}

// secondSignalForcesExitOne is the shared body of the force-exit pins: a
// request is in flight (upstream blocked) so the drain cannot finish; a
// second delivery of sig must force an immediate exit 1 with the
// second_signal_forced_exit event — before the grace deadline, not after it.
func secondSignalForcesExitOne(t *testing.T, sig os.Signal) {
	t.Helper()
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
		yaml:     runtimeYAML(chatPublic, up.url()+"/v1", chatUpstream, ""),
		logLevel: "warn", // the forced-exit event is a WARN line
		grace:    "8s",   // long enough that the natural drain would outlast the test
	})

	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _, _, _ = postJSONRaw(p.addr, "/v1/chat/completions", chatBody, nil)
	}()
	<-received

	p.signal(sig)
	time.Sleep(200 * time.Millisecond)
	p.signal(sig) // second signal: force exit

	start := time.Now()
	code, err := p.waitExit(t, 4*time.Second)
	elapsed := time.Since(start)
	close(release)
	if err != nil {
		t.Fatalf("second signal: %v", err)
	}
	if code != 1 {
		t.Fatalf("exit code after second signal = %d, want 1", code)
	}
	if elapsed > 3*time.Second {
		t.Fatalf("forced exit took %v after the second signal — it waited for the drain", elapsed)
	}
	if shutdownEvent(t, p, "second_signal_forced_exit") == nil {
		t.Fatalf("second_signal_forced_exit event missing; stderr:\n%s", p.stderr.String())
	}
	<-done
}

// TestSecondSignalForcesExitOne pins the force exit on the SIGTERM half of
// the registration.
func TestSecondSignalForcesExitOne(t *testing.T) {
	secondSignalForcesExitOne(t, syscall.SIGTERM)
}

// TestSecondInterruptForcesExitOne pins the SIGINT half. Every other
// shutdown test drives SIGTERM; without this, dropping os.Interrupt from the
// Notify registration would ship green.
func TestSecondInterruptForcesExitOne(t *testing.T) {
	secondSignalForcesExitOne(t, syscall.SIGINT)
}

// TestInterruptWhileIdleExitsZero: SIGINT with nothing in flight is a plain
// clean stop — same exit code, same shutdown_complete trail as SIGTERM.
func TestInterruptWhileIdleExitsZero(t *testing.T) {
	up := newFakeUpstream(t)
	up.setHandler(jsonChatHandler(chatUpstream))
	p := startSubprocess(t, startOpts{
		yaml:     runtimeYAML(chatPublic, up.url()+"/v1", chatUpstream, ""),
		logLevel: "info",
		grace:    "3s",
	})

	p.signal(syscall.SIGINT)
	if code, err := p.waitExit(t, 8*time.Second); err != nil || code != 0 {
		t.Fatalf("SIGINT while idle: code=%d err=%v", code, err)
	}
	if shutdownEvent(t, p, "shutdown_complete") == nil {
		t.Errorf("shutdown_complete missing after SIGINT; stderr:\n%s", p.stderr.String())
	}
}

// TestShutdownLifecycleEvents pins the event trail of a clean stop at a level
// where INFO is visible: drain_started carries the grace, shutdown_complete
// follows it, and both come after the service actually served.
func TestShutdownLifecycleEvents(t *testing.T) {
	up := newFakeUpstream(t)
	up.setHandler(jsonChatHandler(chatUpstream))
	p := startSubprocess(t, startOpts{
		yaml:     runtimeYAML(chatPublic, up.url()+"/v1", chatUpstream, ""),
		logLevel: "debug",
		grace:    "3s",
	})

	if status, _, _ := postJSON(t, p.addr, "/v1/chat/completions", chatBody, nil); status != http.StatusOK {
		t.Fatalf("status = %d, want 200", status)
	}

	p.signal(syscall.SIGTERM)
	if code, err := p.waitExit(t, 8*time.Second); err != nil || code != 0 {
		t.Fatalf("clean shutdown: code=%d err=%v", code, err)
	}

	drain := shutdownEvent(t, p, "drain_started")
	if drain == nil {
		t.Fatalf("drain_started event missing; stderr:\n%s", p.stderr.String())
	}
	if drain["level"] != "info" {
		t.Errorf("drain_started level %v, want info", drain["level"])
	}
	if drain["grace"] != float64(3000) { // zerolog renders Dur in milliseconds
		t.Errorf("drain_started grace %v, want 3000ms", drain["grace"])
	}
	complete := shutdownEvent(t, p, "shutdown_complete")
	if complete == nil {
		t.Fatalf("shutdown_complete event missing; stderr:\n%s", p.stderr.String())
	}
	if shutdownEvent(t, p, "drain_deadline_exceeded") != nil {
		t.Error("drain_deadline_exceeded fired on a drain that finished within grace")
	}
	if shutdownEvent(t, p, "listener_ready") == nil {
		t.Error("listener_ready debug event missing")
	}
}

// TestDrainDeadlineExceededEvent: grace is shorter than the upstream block;
// the drain overflows, the WARN names the grace that elapsed, the in-flight
// client is cut, and the process still exits 0 — exit 1 is reserved for the
// second signal.
func TestDrainDeadlineExceededEvent(t *testing.T) {
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
		yaml:     runtimeYAML(chatPublic, up.url()+"/v1", chatUpstream, ""),
		logLevel: "debug",
		grace:    "1s",
	})

	resCh := make(chan result, 1)
	go func() {
		status, _, _, err := postJSONRaw(p.addr, "/v1/chat/completions", chatBody, nil)
		resCh <- result{status: status, err: err}
	}()
	<-received

	p.signal(syscall.SIGTERM)
	code, err := p.waitExit(t, 8*time.Second)
	close(release)
	if err != nil {
		t.Fatalf("overflow shutdown: %v", err)
	}
	if code != 0 {
		t.Fatalf("drain overflow exit code = %d, want 0 (only a second signal exits 1)", code)
	}

	deadline := shutdownEvent(t, p, "drain_deadline_exceeded")
	if deadline == nil {
		t.Fatalf("drain_deadline_exceeded event missing; stderr:\n%s", p.stderr.String())
	}
	if deadline["level"] != "warn" {
		t.Errorf("drain_deadline_exceeded level %v, want warn", deadline["level"])
	}
	if deadline["grace"] != float64(1000) {
		t.Errorf("drain_deadline_exceeded grace %v, want 1000ms", deadline["grace"])
	}
	if !strings.Contains(p.stderr.String(), "shutdown_complete") {
		t.Error("shutdown_complete missing after a forced drain")
	}

	// The client must have been cut: no 200 through a force-closed connection.
	select {
	case res := <-resCh:
		if res.err == nil && res.status == http.StatusOK {
			t.Fatalf("in-flight request completed during force-close (status %d)", res.status)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("client never noticed the force-close")
	}
}
