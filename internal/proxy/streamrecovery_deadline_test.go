package proxy

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// The suite in this file is the ADVERSARIAL half of `max-elapsed`: it exists to
// prove the bound holds on every way a peer can leave this proxy waiting, and
// that the answer to "who ended this?" is never ambiguous when it is this
// proxy's own window.
//
// The bound is IDLE time — the silence this proxy tolerates from an upstream —
// not total time since the commit. An absolute deadline could not tell a
// healthy long generation from a stalled peer and cut both with the same event;
// production measured healthy streams of 73s, 83s and 106s losing their marker
// to a 20s default. The window is still ONE measure for the whole session and
// is never re-opened by a hop, so every lever below shares it — but every
// accepted upstream byte moves it forward by a full interval, and only genuine
// silence spends it.
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
// Both levers are armed against the SAME window, opened once when the
// committed stream began, and the tests below assert on that window rather
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

// TestRecoveryWindowBindsEveryLeverToOneDeadline proves the exact idle-time
// boundary, including the clock/timer pairing that production uses through the
// recoveryClock seam. Every assertion advances logical time and watchdog time
// together; no test can accidentally accept a policy deadline whose real timer
// is still running on another clock.
func TestRecoveryWindowBindsEveryLeverToOneDeadline(t *testing.T) {
	start := time.Date(2026, time.September, 29, 12, 0, 0, 0, time.UTC)
	const idle = time.Second

	t.Run("exact deadline closes the body", func(t *testing.T) {
		clock := newManualRecoveryClock(start)
		w := newRecoveryWindow(clock, idle)
		body := &closeRecorder{}
		stop := w.armBody(body)
		defer stop()

		clock.Advance(idle - time.Nanosecond)
		if body.closed.Load() || w.expired() {
			t.Fatal("window expired before its half-open boundary")
		}
		clock.Advance(time.Nanosecond)
		if !body.closed.Load() || !w.shut() || !w.expired() {
			t.Fatal("window remained live at its exact deadline")
		}
		if w.progress() {
			t.Fatal("a byte at the deadline revived an expired window")
		}
	})

	t.Run("pre-deadline progress resets the active watchdog", func(t *testing.T) {
		clock := newManualRecoveryClock(start)
		w := newRecoveryWindow(clock, idle)
		body := &closeRecorder{}
		stop := w.armBody(body)
		defer stop()

		clock.Advance(idle / 2)
		if !w.progress() {
			t.Fatal("pre-deadline source progress was rejected")
		}
		// The original callback is due now, but its identity was replaced by
		// progress and must not close the body.
		clock.Advance(idle / 2)
		if body.closed.Load() || w.shut() {
			t.Fatal("stale watchdog cut a stream after source progress")
		}
		clock.Advance(idle / 2)
		if !body.closed.Load() || !w.shut() {
			t.Fatal("replacement watchdog did not enforce the extended idle window")
		}
	})

	t.Run("header handoff moves the window and guards the body", func(t *testing.T) {
		clock := newManualRecoveryClock(start)
		w := newRecoveryWindow(clock, idle)
		bounds, ok := w.bind(context.Background())
		if !ok {
			t.Fatal("open window refused continuation header context")
		}
		defer bounds.release()

		clock.Advance(idle / 2)
		body := &closeRecorder{}
		if !bounds.promote(body) {
			t.Fatal("timely response headers were not accepted as upstream progress")
		}
		clock.Advance(idle / 2)
		if body.closed.Load() || w.shut() {
			t.Fatal("stale header timer canceled a successful response body")
		}
		clock.Advance(idle / 2)
		if !body.closed.Load() || !w.shut() {
			t.Fatal("promoted body was not protected by the renewed idle deadline")
		}
	})

	t.Run("header wait cancels at the exact deadline", func(t *testing.T) {
		clock := newManualRecoveryClock(start)
		w := newRecoveryWindow(clock, idle)
		bounds, ok := w.bind(context.Background())
		if !ok {
			t.Fatal("open window refused continuation header context")
		}
		defer bounds.release()
		clock.Advance(idle)
		select {
		case <-bounds.ctx.Done():
		default:
			t.Fatal("header wait remained live at the exact idle deadline")
		}
		if !w.shut() {
			t.Fatal("header timeout did not latch the window")
		}
	})

	t.Run("completion disarms the current reset guard", func(t *testing.T) {
		clock := newManualRecoveryClock(start)
		w := newRecoveryWindow(clock, idle)
		body := &closeRecorder{}
		stop := w.armBody(body)
		clock.Advance(idle / 2)
		if !w.progress() {
			t.Fatal("pre-deadline source progress was rejected")
		}
		stop()
		clock.Advance(idle)
		if body.closed.Load() || w.shut() {
			t.Fatal("relay cleanup left its replacement watchdog armed")
		}
	})

	t.Run("late reader returns a terminating error", func(t *testing.T) {
		clock := newManualRecoveryClock(start)
		w := newRecoveryWindow(clock, idle)
		clock.Advance(idle)
		r := upstreamProgressReader{Reader: strings.NewReader("late"), progress: w.progress}
		buf := make([]byte, 8)
		n, err := r.Read(buf)
		if n != 0 || !errors.Is(err, errStreamWindowExpired) {
			t.Fatalf("late Read = (%d, %v), want (0, errStreamWindowExpired)", n, err)
		}
	})
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

// TestRecoveryWindowIdleTimeIsExtendedByUpstreamProgress is the QUIET
// direction of the bound, and the one the absolute deadline could not express.
//
// A generation that keeps talking is healthy, however long it talks. Under the
// old window, a stream whose total length exceeded `max-elapsed` was cut with
// `max_elapsed` while it was still producing tokens — the same event the
// proxy emits for a peer that has genuinely stopped, so an operator reading
// the log could not tell a runaway from a long answer. That is the failure
// this pins: the answer below runs for roughly three and a half windows end to
// end, so an absolute deadline would have fired in the middle of it, and every
// one of its events is the proof that keeps the window alive.
//
// The shape is a steady stream with gaps well INSIDE the bound, and that is
// deliberate rather than convenient: silence longer than the bound is not a
// long answer, it is the stall, and a fixture that outran the window with gaps
// longer than it would be testing the opposite property. Its inverse is pinned
// separately by TestStreamRecoveryIdleWindowStillCutsAStalledBody: the same
// bound, on an upstream that goes quiet, still ends the request.
func TestRecoveryWindowIdleTimeIsExtendedByUpstreamProgress(t *testing.T) {
	const window = 200 * time.Millisecond
	h, logBuf, pa, _ := recoveryHandler(t,
		recoveryBlock(t, "    enabled: true\n    max-elapsed: 200ms\n"))
	// Eight events spread over roughly three and a half windows: every gap is
	// HALF the bound, so the only thing carrying the stream past the total is
	// that the upstream keeps saying something. A per-read or per-event bound
	// would also survive this; only a bound that MOVES on upstream progress
	// does. Under the old absolute deadline this request would have been cut
	// around its second event, with the same `max_elapsed` record a genuinely
	// stalled peer produces.
	upstream := &pacedReader{
		segments: []string{
			sseChat("One"), sseChat(" two"), sseChat(" three"), sseChat(" four"),
			sseChat(" five"), sseChat(" six"), sseChat(" seven"), sseChat(" eight"),
		},
		pause:  window / 2,
		final:  "data: [DONE]\n\n",
		closed: make(chan struct{}),
	}
	pa.script = []dialFunc{func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
			Body:       upstream,
		}, nil
	}}

	start := time.Now()
	rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions", chatRequest, nil)
	elapsed := time.Since(start)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want the committed 200", rec.Code)
	}
	body := rec.Body.String()
	for _, want := range []string{"One", " two", " three", " four", " five", " six", " seven", " eight", "[DONE]"} {
		if !strings.Contains(body, want) {
			t.Fatalf("a healthy stream outliving the window lost %q: %s", want, body)
		}
	}
	// The proof that the window was genuinely outrun rather than merely not
	// armed: the whole answer took several windows, so an absolute deadline
	// would have fired in the middle of it. The floor is below the fixture's
	// own arithmetic (seven gaps of half a window) so a loaded machine cannot
	// fail it, while still being more than twice the absolute bound.
	if elapsed < 2*window {
		t.Errorf("the answer arrived in %v; the test did not outrun the %v window", elapsed, window)
	}
	if upstream.closedEarly.Load() {
		t.Error("the window closed a body whose upstream was still producing events")
	}
	if pa.dials() != 1 {
		t.Errorf("dials = %d, want the primary once and no continuation", pa.dials())
	}
	for _, slug := range []string{"stream_recovery_started", "stream_recovery_failed", "stream_recovery_succeeded", "stream_recovery_exhausted", "stream_truncated"} {
		if ev := logBuf.events(t, slug); len(ev) != 0 {
			t.Errorf("%s fired for a healthy stream that outran the window: %v", slug, ev)
		}
	}
	completed := logBuf.events(t, "stream_completed")
	if len(completed) != 1 || completed[0]["stream_recoveries"] != float64(0) {
		t.Fatalf("stream_completed = %v, want one completion with no recovery", completed)
	}
}

// TestRecoveryWindowCountsFragmentedUpstreamBytesBeforeEventDispatch pins the
// source-read boundary. A legitimate SSE event can be large enough, or arrive
// fragmented enough, that it takes longer than max-elapsed to reach its blank
// line. The peer is still talking throughout. Counting only event boundaries
// would cut it before the parser could dispatch its first event; counting
// client writes would be even later and would make a slow client part of the
// liveness decision. Every successful upstream read is the one fact that is
// both early enough and true.
func TestRecoveryWindowCountsFragmentedUpstreamBytesBeforeEventDispatch(t *testing.T) {
	const window = 200 * time.Millisecond
	h, logBuf, pa, _ := recoveryHandler(t,
		recoveryBlock(t, "    enabled: true\n    max-elapsed: 200ms\n"))
	// No complete SSE line exists until the last segment, more than two
	// windows after the first. The source yields a fragment every half-window,
	// so an idle bound has to see those reads while CopySSE is still waiting
	// for its first event boundary.
	upstream := &pacedReader{
		segments: []string{
			`data: {"model":"up-a","choices":[{"index":0,"delta":{"content":"`,
			`one `,
			`fragmented `,
			`SSE `,
			`event` + "\"}}]}\n\n",
		},
		pause:  window / 2,
		final:  "data: [DONE]\n\n",
		closed: make(chan struct{}),
	}
	pa.script = []dialFunc{func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
			Body:       upstream,
		}, nil
	}}

	start := time.Now()
	rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions", chatRequest, nil)
	elapsed := time.Since(start)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want the committed 200", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "one fragmented SSE event") ||
		!strings.Contains(rec.Body.String(), "[DONE]") {
		t.Fatalf("fragmented healthy event did not finish: %s", rec.Body.String())
	}
	if elapsed < 2*window {
		t.Errorf("stream arrived in %v; the test did not outrun its %v window before dispatch", elapsed, window)
	}
	if upstream.closedEarly.Load() {
		t.Error("the window closed a peer that was still feeding one fragmented event")
	}
	for _, slug := range []string{"stream_recovery_exhausted", "stream_truncated"} {
		if ev := logBuf.events(t, slug); len(ev) != 0 {
			t.Errorf("%s fired before the fragmented event could dispatch: %v", slug, ev)
		}
	}
}

// TestStreamRecoveryIdleWindowStillCutsAStalledBody is the OTHER half of the
// same property, and the one that keeps the change from being a relaxation.
//
// Moving the bound is only correct if the bound still bites. The upstream here
// is exactly the one from the quiet test's opposite: it says one thing and
// then stops. The stream was healthy up to that point and the bound is still
// reached, still reported as this proxy's own and never as a peer's fault,
// and still spends no hop — the request ends where it stopped rather than
// parking the client on a silent connection.
func TestStreamRecoveryIdleWindowStillCutsAStalledBody(t *testing.T) {
	const window = 250 * time.Millisecond
	h, logBuf, pa, _ := recoveryHandler(t,
		recoveryBlock(t, "    enabled: true\n    max-elapsed: 250ms\n"))
	// A generation that is well past the point of proving it is alive, then
	// stops for good. The window may be extended by what came before, but the
	// silence after it is what spends it.
	body, dial := sseParked(context.Background(), sseChat("Hello ")+sseChat("world"))
	pa.script = []dialFunc{dial}

	start := time.Now()
	rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions", chatRequest, nil)
	elapsed := time.Since(start)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want the already-committed 200", rec.Code)
	}
	// What already reached the client stays: the bound cuts the recovery, it
	// does not retract the committed answer.
	if !strings.Contains(rec.Body.String(), "Hello ") {
		t.Fatalf("the committed text never reached the client: %s", rec.Body.String())
	}
	if !body.released.Load() {
		t.Fatal("the parked read never returned: the window did not reach the body")
	}
	if elapsed > window+1500*time.Millisecond {
		t.Errorf("request took %v against a %v idle window: silence is not bounded", elapsed.Round(time.Millisecond), window)
	}
	if pa.dials() != 1 {
		t.Errorf("dials = %d, want no hop past the window", pa.dials())
	}
	if ev := logBuf.events(t, "stream_recovery_started"); len(ev) != 0 {
		t.Errorf("a hop was dialed past the window: %v", ev)
	}
	exh := logBuf.events(t, "stream_recovery_exhausted")
	if len(exh) != 1 || exh[0]["reason"] != "max_elapsed" {
		t.Fatalf("stream_recovery_exhausted = %v, want one max_elapsed refusal", exh)
	}
	trunc := logBuf.events(t, "stream_truncated")
	if len(trunc) != 1 || trunc[0]["recovery_reason"] != "max_elapsed" {
		t.Fatalf("stream_truncated = %v, want one with recovery_reason max_elapsed", trunc)
	}
	// The window is this proxy's own bound, not a diagnosis about the peer, so
	// it carries no error and is never reported as a departed client.
	if _, has := trunc[0]["error"]; has {
		t.Errorf("the window reported an upstream error it invented: %v", trunc[0])
	}
	if trunc[0]["outcome"] == "client_disconnected" {
		t.Errorf("the window was reported as a client disconnect: %v", trunc[0])
	}
	done := logBuf.events(t, "request_completed")
	if len(done) != 1 || done[0]["outcome"] != "stream_truncated" {
		t.Fatalf("request_completed = %v, want stream_truncated", done)
	}
}

// pacedReader streams one segment at a time with a pause between each, then a
// terminal marker, then EOF: a slow but perfectly healthy upstream whose total
// length is many times any window. It is pacedReader rather than
// pausedReader because the property under test is the WHOLE session's length,
// not one gap — a single long gap is indistinguishable from a stall, and a
// bound that treated it as one would be a bound nobody could safely raise.
type pacedReader struct {
	segments []string
	pause    time.Duration
	final    string
	closed   chan struct{}
	i        int
	offset   int
	// closedEarly records a Close that arrived while segments remained — the
	// watchdog firing on a body whose upstream was still producing. It is the
	// quiet direction's own evidence, because a test that only checked the
	// relayed bytes could not tell a close that lost a race from one that
	// never happened.
	closedEarly atomic.Bool
}

func (r *pacedReader) Close() error {
	select {
	case <-r.closed:
	default:
		close(r.closed)
	}
	return nil
}

func (r *pacedReader) Read(p []byte) (int, error) {
	if r.i < len(r.segments) {
		if r.i > 0 && r.offset == 0 {
			select {
			case <-time.After(r.pause):
			case <-r.closed:
				// Close after the terminal marker is ordinary handler cleanup,
				// not a watchdog firing early. While segments remain, though,
				// it proves a bound cut a still-speaking upstream.
				r.closedEarly.Store(true)
				return 0, errors.New("http: read on closed response body")
			}
		}
		seg := r.segments[r.i]
		n := copy(p, seg[r.offset:])
		r.offset += n
		if r.offset == len(seg) {
			r.i++
			r.offset = 0
		}
		return n, nil
	}
	if r.i == len(r.segments) {
		r.i++
		return copy(p, r.final), nil
	}
	return 0, io.EOF
}

// TestRecoveryWindowIsNotRevivedByThisProxysOwnKeepAlive is the ping.
//
// The bound measures whether the UPSTREAM is alive, and the SSE keep-alive is
// this proxy talking to its own client: it is written on a timer, it is never
// read from the peer, and it says nothing whatever about whether the model is
// still generating. Wiring it to the window's progress would let a dead
// upstream stay alive indefinitely — every ping would restart the silence, and
// a client that kept reading would sit on a stream that would never end. It is
// the one way an idle bound is strictly worse than the absolute deadline it
// replaced, so the guard is a test rather than a comment.
//
// The floor on the ping interval is 1s, so the bound here is set just UNDER
// two ping intervals: a ping-driven window would be restarted before it ever
// fell, and the request would never end. The real window falls well inside the
// first gap between pings, so the request is expected to be CUT at its bound.
// The test fails if it survives — which is the only shape that failure can
// take, since a bound that stopped biting would hang rather than truncate.
func TestRecoveryWindowIsNotRevivedByThisProxysOwnKeepAlive(t *testing.T) {
	// Just under two ping intervals, so the stall below spans more than one
	// ping and a window fed by pings could not expire anywhere in it.
	const window = 1900 * time.Millisecond
	store := newChainStore(t, recoveryBlock(t,
		"    enabled: true\n    max-elapsed: 1900ms\n")+"\n"+keepAliveYAML)
	pa := &scriptedDoer{}
	pb := &scriptedDoer{}
	logBuf, log := captureLog(zerolog.DebugLevel)
	h := NewHandler(store, kindResolver{direct: pa, proxied: pb}, nil, nil, nil, log)

	// One healthy event, then silence for good. The heartbeat fires inside the
	// stall; the window must not notice it.
	//
	// The timeout makes the sabotage failure finite: if a future change lets
	// our own pings revive the window, this request returns as a caller stop
	// rather than hanging the test forever. It is deliberately well past the
	// real window (and its scheduling headroom), so a correct run never reaches
	// it and the event assertions below still distinguish the two owners.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	body, dial := sseParked(ctx, sseChat("Hello"))
	pa.script = []dialFunc{dial}

	start := time.Now()
	rec := doRequestWithContext(t, h, ctx, http.MethodPost, "/v1/chat/completions", chatRequest, nil)
	elapsed := time.Since(start)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want the already-committed 200", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "Hello") {
		t.Fatalf("the committed event never reached the client: %s", rec.Body.String())
	}
	if !body.released.Load() {
		t.Fatal("the parked read never returned: the keep-alive kept a dead upstream alive")
	}
	if elapsed > window+1500*time.Millisecond {
		t.Errorf("request took %v against a %v window: a dead upstream outlived it anyway",
			elapsed.Round(time.Millisecond), window)
	}
	// The ping may be visible in the relayed bytes — it is an ignorable
	// comment and its presence is not the defect. The defect is the window
	// surviving it, so the outcome is the assertion.
	exh := logBuf.events(t, "stream_recovery_exhausted")
	if len(exh) != 1 || exh[0]["reason"] != "max_elapsed" {
		t.Fatalf("stream_recovery_exhausted = %v, want one max_elapsed refusal: the "+
			"window was kept alive by this proxy's own client traffic", exh)
	}
	trunc := logBuf.events(t, "stream_truncated")
	if len(trunc) != 1 || trunc[0]["recovery_reason"] != "max_elapsed" {
		t.Fatalf("stream_truncated = %v, want one with recovery_reason max_elapsed", trunc)
	}
}

// keepAliveYAML turns the SSE keep-alive on at its 1s floor — the shortest
// interval the configuration accepts, and therefore the one that gives a
// ping-driven window the best chance of outliving a bound. It is a constant
// rather than a parameter because exactly one test needs it, and every other
// keep-alive test asserts on the ping's own behaviour rather than on a window.
const keepAliveYAML = "sse-keep-alive:\n  enabled: true\n  interval: 1s\n"
