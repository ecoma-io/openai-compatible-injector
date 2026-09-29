package proxy

import (
	"context"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// The suite in this file is the ADVERSARIAL half of `max-elapsed`: it exists to
// prove the bound holds on every way a peer can leave this proxy waiting, and
// that the answer to "who ended this?" is never ambiguous when it is this
// proxy's own window.
//
// The waits a continuation hop can be parked in are two, and they need
// different levers:
//
//   - the response HEADERS never arrive. The request is on the wire, the peer
//     accepted it, and Do blocks. No body exists yet, so nothing can be closed
//     to unblock it — only the request's context can be canceled
//     (recoveryWindow.bind).
//   - the body stalls after some events. The headers arrived, so the response
//     exists; Body.Read takes no context, so only closing the body returns the
//     read (recoveryWindow.armBody).
//
// Both levers are armed against the SAME instant, computed once when the
// committed stream began, and the tests below assert on that instant rather
// than on any individual timer.

// headerParkedDial answers a dial by refusing to answer it: it never writes a
// status line and never writes a header, and returns only when the request's
// own context is done — which is what net/http does for a peer that accepts a
// connection and then says nothing. entered is closed when the dial is
// received, so a test can prove the hop reached the wire rather than being
// refused before it.
func headerParkedDial(entered chan<- struct{}) dialFunc {
	var once sync.Once
	return func(req *http.Request) (*http.Response, error) {
		once.Do(func() { close(entered) })
		<-req.Context().Done()
		return nil, req.Context().Err()
	}
}

// closeRecorder is a body a test can watch: closed records that something
// closed it, which is the only observable effect the body watchdog has.
type closeRecorder struct {
	closed atomic.Bool
}

func (c *closeRecorder) Read([]byte) (int, error) { return 0, nil }
func (c *closeRecorder) Close() error {
	c.closed.Store(true)
	return nil
}

// TestRecoveryWindowBindsEveryLeverToOneDeadline is the unit-level proof that
// the four mechanisms a recovery session has share one instant, and that
// re-arming a lever mid-window extends nothing.
//
// It is deliberately about the window type rather than about a request: the
// defects this replaces were structural — a deadline read from one clock and a
// gate from another, a dial bounded by neither — and a request-level test can
// only observe the ones that happen to fire first.
func TestRecoveryWindowBindsEveryLeverToOneDeadline(t *testing.T) {
	const window = 150 * time.Millisecond
	start := time.Now()
	w := newRecoveryWindow(start, window)

	if w.expired(start) {
		t.Error("the window was already expired at the instant it opened")
	}
	if w.expired(w.deadline) {
		t.Error("the deadline itself counts as expired; the bound moved one tick inward")
	}
	if !w.expired(w.deadline.Add(time.Nanosecond)) {
		t.Error("the window does not expire one tick past its deadline")
	}
	if w.shut() {
		t.Error("the window reported itself shut before anything reached it")
	}

	// A gate that finds the instant already past marks the window shut as well
	// as refusing: the refusal and the record are one fact.
	if w.dialable(w.deadline.Add(time.Nanosecond)) {
		t.Error("a hop was told it may dial past the window's deadline")
	}
	if !w.shut() {
		t.Error("a refusal past the deadline did not record the window as shut")
	}

	// bind: the lever for a dial waiting on response headers. It cancels at
	// the deadline, and its release cancels without claiming the window was
	// reached — the release fires on a hop that SUCCEEDED.
	fresh := newRecoveryWindow(time.Now(), window)
	b := fresh.bind(context.Background(), time.Now())
	if err := b.ctx.Err(); err != nil {
		t.Fatalf("a hop was refused a context while the window was open: %v", err)
	}
	select {
	case <-b.ctx.Done():
		t.Fatal("the dial's context was canceled before the window was reached")
	case <-time.After(window / 3):
	}
	select {
	case <-b.ctx.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("the dial's context outlived the window: a header wait is unbounded")
	}
	if !fresh.shut() {
		t.Error("the dial watchdog canceled without recording the window as shut")
	}
	b.release()

	// The two levers end different waits, and that separation is load-bearing:
	// net/http aborts an unread response body when the request's context is
	// canceled, so a hop releases its header watchdog when the dial returns
	// while the CONTEXT lives until the body is closed. stop() is the first
	// half — and it must leave the context alone.
	open := newRecoveryWindow(time.Now(), 10*time.Second)
	live := open.bind(context.Background(), time.Now())
	live.stop()
	if err := live.ctx.Err(); err != nil {
		t.Errorf("releasing a hop's header watchdog canceled the context its body is read under: %v", err)
	}
	if open.shut() {
		t.Error("stopping a hop's header watchdog recorded the window as reached")
	}
	live.release()
	if err := live.ctx.Err(); err == nil {
		t.Error("releasing a hop's context left it live")
	}
	if open.shut() {
		t.Error("releasing a hop's context recorded the window as reached")
	}

	// boundBody is the second half: the release rides the hop's own Close, so
	// the context cannot be released while a relay is still reading.
	null := io.NopCloser(strings.NewReader(""))
	bounds := newRecoveryWindow(time.Now(), 10*time.Second).bind(context.Background(), time.Now())
	wrapped := &boundBody{ReadCloser: null, release: bounds.release}
	if err := bounds.ctx.Err(); err != nil {
		t.Fatalf("a fresh hop context was already done: %v", err)
	}
	if err := wrapped.Close(); err != nil {
		t.Fatalf("closing a hop body: %v", err)
	}
	if err := bounds.ctx.Err(); err == nil {
		t.Error("closing a hop's body left its context live: a hop would leak one context per attempt")
	}

	// armBody: the lever for a stalled body. It closes at the deadline and not
	// before.
	body := &closeRecorder{}
	armed := newRecoveryWindow(time.Now(), window)
	stop := armed.armBody(time.Now(), body)
	defer stop()
	time.Sleep(window / 3)
	if body.closed.Load() {
		t.Error("a healthy body was closed before the window was reached")
	}
	deadline := time.Now().Add(2 * time.Second)
	for !body.closed.Load() {
		if time.Now().After(deadline) {
			t.Fatal("the body watchdog never closed a body past the window")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !armed.shut() {
		t.Error("the body watchdog closed a body without recording the window as shut")
	}

	// One window, several levers, still one instant: a lever armed after part
	// of the window has already run fires on what is LEFT of it. A per-hop
	// reset would let this second lever outlive the deadline by most of a
	// window, which is exactly the property `max-elapsed` claims not to have.
	shared := newRecoveryWindow(time.Now(), window)
	first := &closeRecorder{}
	stopFirst := shared.armBody(time.Now(), first)
	defer stopFirst()
	time.Sleep(window * 2 / 3)
	second := &closeRecorder{}
	stopSecond := shared.armBody(time.Now(), second)
	defer stopSecond()
	time.Sleep(window * 2 / 3)
	if !second.closed.Load() {
		t.Error("a lever re-armed mid-window restarted the bound instead of sharing it")
	}
	if !first.closed.Load() {
		t.Error("the lever armed when the window opened never fired")
	}

	// The already-expired arm is the commit that itself outlived the window:
	// nothing is relayed past it.
	late := newRecoveryWindow(time.Now(), time.Millisecond)
	time.Sleep(5 * time.Millisecond)
	expiredBody := &closeRecorder{}
	late.armBody(time.Now(), expiredBody)()
	if !expiredBody.closed.Load() || !late.shut() {
		t.Error("a pass armed against an already-shut window was allowed to read it")
	}
}

// TestStreamRecoveryHopHeaderWaitIsBounded is the response-header blind spot:
// a hop whose peer accepts the request and then answers nothing.
//
// Nothing about the committed stream was unhealthy — it was cut once, which is
// the whole reason a hop was attempted — so the only thing that can end this
// request is the window's own cancel reaching inside Do. Without it the hop
// waits on the client's context, which for a client that is still reading is
// forever, and `max-elapsed` is a number in a file that bounds nothing.
func TestStreamRecoveryHopHeaderWaitIsBounded(t *testing.T) {
	const window = 250 * time.Millisecond
	h, logBuf, pa, pb := recoveryHandler(t,
		recoveryBlock(t, "    enabled: true\n    max-elapsed: 250ms\n"))
	entered := make(chan struct{})
	pa.script = []dialFunc{
		sseCut(sseChat("Hello")),
		headerParkedDial(entered),
	}

	start := time.Now()
	rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions", chatRequest, nil)
	elapsed := time.Since(start)

	select {
	case <-entered:
	default:
		t.Fatal("the hop was never dialed: the test did not exercise a header wait")
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want the already-committed 200", rec.Code)
	}
	// What already reached the client stays: the bound cuts the recovery, it
	// does not retract the committed answer.
	if !strings.Contains(rec.Body.String(), "Hello") {
		t.Fatalf("the committed text never reached the client: %s", rec.Body.String())
	}
	if pa.dials() != 2 {
		t.Fatalf("dials = %d, want the walk attempt and the one hop", pa.dials())
	}
	if pb.dials() != 0 {
		t.Fatalf("dials on the fallback candidate = %d, want 0: a continuation never moves candidate", pb.dials())
	}
	// The bound, measured on the request: it must have been REACHED (the window
	// is not a look-away) and it must not have been exceeded by anything like
	// another window.
	if elapsed < window {
		t.Errorf("request returned in %v, before its %v window could be reached", elapsed.Round(time.Millisecond), window)
	}
	if elapsed > window+1500*time.Millisecond {
		t.Errorf("request took %v against a %v window: a header wait is unbounded", elapsed.Round(time.Millisecond), window)
	}

	started := logBuf.events(t, "stream_recovery_started")
	if len(started) != 1 {
		t.Fatalf("stream_recovery_started = %v, want the one hop", started)
	}
	failed := logBuf.events(t, "stream_recovery_failed")
	if len(failed) != 1 {
		t.Fatalf("stream_recovery_failed = %v, want one", failed)
	}
	// The phase names the OWNER. A hop cut by this proxy's window must never be
	// recorded as a peer that misbehaved, and the error the cancel produced
	// belongs to nothing outside this process, so it is not attached.
	if failed[0]["phase"] != "max_elapsed" {
		t.Errorf("hop failure phase = %v, want max_elapsed rather than an upstream fault", failed[0]["phase"])
	}
	if _, has := failed[0]["error"]; has {
		t.Errorf("the window reported an upstream error it invented: %v", failed[0])
	}
	exh := logBuf.events(t, "stream_recovery_exhausted")
	if len(exh) != 1 || exh[0]["reason"] != "max_elapsed" {
		t.Fatalf("stream_recovery_exhausted = %v, want one max_elapsed refusal", exh)
	}
	if ev := logBuf.events(t, "stream_recovery_succeeded"); len(ev) != 0 {
		t.Errorf("a cut hop was reported as a success: %v", ev)
	}
	trunc := logBuf.events(t, "stream_truncated")
	if len(trunc) != 1 {
		t.Fatalf("stream_truncated = %v, want one", trunc)
	}
	if trunc[0]["recovery_reason"] != "max_elapsed" {
		t.Errorf("truncation reason = %v, want max_elapsed", trunc[0]["recovery_reason"])
	}
	if _, has := trunc[0]["error"]; has {
		t.Errorf("our own bound was reported with an upstream error: %v", trunc[0])
	}
	done := logBuf.events(t, "request_completed")
	if len(done) != 1 || done[0]["outcome"] != "stream_truncated" {
		t.Fatalf("request_completed = %v, want stream_truncated", done)
	}
	if done[0]["outcome"] == "client_disconnected" {
		t.Errorf("a live client's request was reported as a disconnect: %v", done[0])
	}
}

// TestStreamRecoveryHopStallAfterEventsIsCutAtTheSameDeadline is the other
// lever on the same instant: the hop answered, relayed several events, and
// then held the connection open without finishing.
//
// The window has to reach a read this time, not a dial, and the events it
// already relayed are the client's. What must NOT happen is the stream sitting
// there until the client gives up — with the bound in place the read returns
// and the loop stops on the same token the header case stops on.
func TestStreamRecoveryHopStallAfterEventsIsCutAtTheSameDeadline(t *testing.T) {
	const window = 250 * time.Millisecond
	h, logBuf, pa, _ := recoveryHandler(t,
		recoveryBlock(t, "    enabled: true\n    max-elapsed: 250ms\n"))
	// More than one event in the first read, then silence: the shape a
	// per-read deadline would mis-handle and a per-event one would never
	// bound.
	body, dial := sseParked(context.Background(), sseChat(" world")+sseChat(" again"))
	pa.script = []dialFunc{sseCut(sseChat("Hello")), dial}

	start := time.Now()
	rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions", chatRequest, nil)
	elapsed := time.Since(start)

	if !body.released.Load() {
		t.Fatal("the hop's parked read never returned: the window did not reach the body")
	}
	if elapsed > window+1500*time.Millisecond {
		t.Errorf("request took %v against a %v window", elapsed.Round(time.Millisecond), window)
	}
	relayed := rec.Body.String()
	if !strings.Contains(relayed, "Hello") || !strings.Contains(relayed, "world") {
		t.Fatalf("the committed and recovered text did not both reach the client: %s", relayed)
	}
	exh := logBuf.events(t, "stream_recovery_exhausted")
	if len(exh) != 1 || exh[0]["reason"] != "max_elapsed" {
		t.Fatalf("stream_recovery_exhausted = %v, want one max_elapsed refusal", exh)
	}
	if exh[0]["recoveries"] != float64(1) {
		t.Errorf("recoveries = %v, want the cut hop counted", exh[0]["recoveries"])
	}
	failed := logBuf.events(t, "stream_recovery_failed")
	if len(failed) != 1 || failed[0]["phase"] != "max_elapsed" {
		t.Fatalf("stream_recovery_failed = %v, want one max_elapsed phase", failed)
	}
}

// TestStreamRecoverySecondHopHeaderWaitIsBounded puts the blind spot on the
// LAST hop rather than the first: the committed pass and the first hop both
// end cleanly without a marker, and the second hop is the one that parks in Do.
//
// A bound that were armed once per session is the property under test, so the
// assertion is the whole request's elapsed time against the ONE window: a
// per-hop budget would let this request run for the walk's time plus a fresh
// window per hop.
func TestStreamRecoverySecondHopHeaderWaitIsBounded(t *testing.T) {
	const window = 300 * time.Millisecond
	h, logBuf, pa, _ := recoveryHandler(t,
		recoveryBlock(t, "    enabled: true\n    max-elapsed: 300ms\n    max-recoveries: 2\n"))
	entered := make(chan struct{})
	pa.script = []dialFunc{
		sseCut(sseChat("Hello")),
		sseCut(sseChat(" world")),
		headerParkedDial(entered),
	}

	start := time.Now()
	rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions", chatRequest, nil)
	elapsed := time.Since(start)

	select {
	case <-entered:
	default:
		t.Fatal("the second hop was never dialed")
	}
	if pa.dials() != 3 {
		t.Fatalf("dials = %d, want the walk attempt and two hops", pa.dials())
	}
	if elapsed > window+1500*time.Millisecond {
		t.Errorf("request took %v against a %v window: a later hop outlived the same instant",
			elapsed.Round(time.Millisecond), window)
	}
	if !strings.Contains(rec.Body.String(), " world") {
		t.Fatalf("the first hop's text never reached the client: %s", rec.Body.String())
	}
	started := logBuf.events(t, "stream_recovery_started")
	if len(started) != 2 {
		t.Fatalf("stream_recovery_started = %v, want both hops", started)
	}
	failed := logBuf.events(t, "stream_recovery_failed")
	if len(failed) != 2 {
		t.Fatalf("stream_recovery_failed = %v, want one per hop", failed)
	}
	if failed[1]["phase"] != "max_elapsed" {
		t.Errorf("the second hop's phase = %v, want max_elapsed", failed[1]["phase"])
	}
	if failed[0]["phase"] == "max_elapsed" {
		t.Errorf("the first hop was reported as a bound it did not hit: %v", failed[0])
	}
	exh := logBuf.events(t, "stream_recovery_exhausted")
	if len(exh) != 1 || exh[0]["reason"] != "max_elapsed" {
		t.Fatalf("stream_recovery_exhausted = %v, want one max_elapsed refusal", exh)
	}
}
