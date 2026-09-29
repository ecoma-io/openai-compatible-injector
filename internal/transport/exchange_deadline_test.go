package transport

// Regression tests for the exchange-deadline handoff (the P1 finding).
//
// The defect these pin is one sentence long: net/http binds a response BODY to
// the context of the request that produced it, so a request cloned onto a
// context carrying a `max-elapsed` timeout produced a body that DIED with that
// timeout. An SSE stream that legitimately runs for minutes was truncated at
// whatever the operator's envelope said, and the proxy reported upstream
// truncation for a stream that was answering perfectly.
//
// The quiet direction is the whole point of this file. A test that only checks
// "the slow dial still times out" would pass on the broken implementation too
// — the broken one timed out the body as well. Every test here is phrased as
// what must NOT happen: a body outliving its window, a bound that does not
// touch a response, a classification that keeps two owners apart.
//
// The phase split is decided by the CALLER, after it knows what the response
// is. A streaming REQUEST says nothing about the RESPONSE, and guessing from it
// is the defect these tests exist to prevent: a `stream: true` request answered
// with buffered JSON would have its envelope dropped at headers, while the body
// is still a pre-commitment answer read inside the walk.

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// mutedStream answers with headers and a body that never produces a byte. A
// silent stream keeps an earlier event buffered in the client's socket, so a
// read still succeeds after the body has been cut — which is precisely how a
// truncated stream can pass unnoticed. This one cannot: the read has to wait
// for upstream bytes that will never come, so the body's real state is the
// only thing the read can report.
type mutedStream struct{ srv *httptest.Server }

func newMutedStream(t *testing.T) *mutedStream {
	t.Helper()
	m := &mutedStream{}
	m.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		// Hold the connection open and say nothing, ever.
		<-r.Context().Done()
	}))
	t.Cleanup(m.srv.Close)
	return m
}

func newRequest(t *testing.T, url string) *http.Request {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, url, strings.NewReader(`{"model":"m"}`))
	if err != nil {
		t.Fatal(err)
	}
	return req
}

// acceptUndecided is a body with no window on it: the shape every caller that
// never went through DialWithin — and every exchange the envelope refused to
// fund — hands back. It is what the no-op case of HandoffStream is proven
// against.
type acceptUndecided struct{ io.ReadCloser }

// TestDialWithinCommittedBodyOutlivesTheWindow is THE regression for the
// finding, and the handoff is explicit in it: the caller saw the event stream
// and said so. Only then is the body free of the pre-commitment bound.
//
// The window here is short and the stream stays silent for far longer than it.
// On the broken implementation the body IS the request's context, so the first
// read after the window elapses fails with the window's cancellation. The
// direction that fails is the quiet one, which is the direction nothing else in
// the suite would notice.
func TestDialWithinCommittedBodyOutlivesTheWindow(t *testing.T) {
	up := newMutedStream(t)
	const window = 150 * time.Millisecond

	resp, err := DialWithin(window, context.Background(), NewDirectClient(), newRequest(t, up.srv.URL))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	// Headers are in, the answer is a confirmed event stream, so the envelope
	// is no longer this body's to enforce: a committed body may not be cut by a
	// pre-commitment bound.
	HandoffStream(resp)

	// Wait past the window. A body still bound to it is already dead here, and
	// the read below can only succeed if the exchange really handed it off.
	time.Sleep(4 * window)

	readDone := make(chan error, 1)
	go func() {
		_, err := resp.Body.Read(make([]byte, 1))
		readDone <- err
	}()
	select {
	case err := <-readDone:
		t.Fatalf("body read failed %v after the window elapsed — the committed body is still bound to the pre-response bound", err)
	case <-time.After(500 * time.Millisecond):
		// Still waiting on upstream bytes that will never arrive: the body is
		// live and unbounded, which is what a long-lived stream needs.
	}
}

// TestDialWithinWindowBoundsThePreResponsePhase is the other half, and it is
// the half that was already true before the fix: a peer that accepts the
// connection and sends no status line must still be cut off. Without this the
// fix would have bought a live body by giving up the bound entirely, which is
// not a trade this service makes.
func TestDialWithinWindowBoundsThePreResponsePhase(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		// Hold the connection open without answering it.
		defer func() { _ = conn.Close() }()
		time.Sleep(30 * time.Second)
	}()

	const window = 200 * time.Millisecond
	start := time.Now()
	_, err = DialWithin(window, context.Background(), NewDirectClient(), newRequest(t, "http://"+ln.Addr().String()))
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("a peer that never sent a status line was not bounded by the window")
	}
	if elapsed > 5*time.Second {
		t.Fatalf("the window took %v to cut a silent peer — the bound is not on the pre-response phase", elapsed)
	}
	var ete *ExchangeTimeoutError
	if !errors.As(err, &ete) {
		t.Fatalf("failure = %T (%v), want *ExchangeTimeoutError: the window is this proxy's own bound and must be typed as such", err, err)
	}
	if ete.Elapsed != window {
		t.Errorf("reported window = %v, want the granted %v", ete.Elapsed, window)
	}
}

// TestDialWithinCallerCancellationOutranksTheWindow pins the ownership question
// the two bounds race for. A client that hangs up mid-dial is the caller's
// event; reporting it as this proxy's window would hand the walk a
// fallback-eligible timeout for a client that simply left, which is how a
// disconnect becomes an upstream failure in the evidence.
func TestDialWithinCallerCancellationOutranksTheWindow(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer func() { _ = conn.Close() }()
		time.Sleep(30 * time.Second)
	}()

	// A window far longer than the test: the caller must be the only thing
	// that can end this dial.
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := DialWithin(30*time.Second, ctx, NewDirectClient(), newRequest(t, "http://"+ln.Addr().String()))
		done <- err
	}()
	// Cancel well before the window could ever fire.
	time.Sleep(100 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		var ede *ExchangeDeadlineError
		if !errors.As(err, &ede) {
			t.Fatalf("failure = %T (%v), want *ExchangeDeadlineError: a caller's own cancellation must never read as this proxy's window", err, err)
		}
		// The classification a pool reads must agree with the type, and must
		// not claim the CALLER ended: the context it was handed is live.
		f := ClassifyAttempt(context.Background(), err)
		if f.CallerTerminated {
			t.Error("classified as caller-terminated against a live caller context — the dial failed against a context that had not ended")
		}
		if Classify(err) != ClassCanceled {
			t.Errorf("Classify = %v, want %v", Classify(err), ClassCanceled)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("a canceled caller did not end the dial — the pre-response context is not reachable by the caller")
	}
}

// TestDialWithinEndpointFailureTravelsUnchanged guards the other direction: an
// endpoint's own failure must not be reshaped by the window's machinery. The
// classification a pool makes — the class, the cause and above all the send
// state — is what decides whether a request may be replayed on another egress,
// and a proxy-imposed bound that rewrote it would either duplicate a request
// upstream or refuse a safe one.
func TestDialWithinEndpointFailureTravelsUnchanged(t *testing.T) {
	// A reserved-then-closed port: refused, provably before any request byte.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()

	_, err = DialWithin(30*time.Second, context.Background(), NewDirectClient(), newRequest(t, "http://"+addr))
	if err == nil {
		t.Fatal("a refused connection returned no error")
	}
	// A refused syscall is provably never-sent, so replaying it is safe. The
	// window's context was live throughout, so the failure must still be the
	// endpoint's.
	f := ClassifyAttempt(context.Background(), err)
	if f.CallerTerminated {
		t.Errorf("a refused dial was read as the caller's event: %+v", f)
	}
	if f.Class != ClassConnection {
		t.Errorf("class = %v, want %v", f.Class, ClassConnection)
	}
	if f.Cause != CauseConnectionRefused {
		t.Errorf("cause = %q, want %q", f.Cause, CauseConnectionRefused)
	}
	if f.SendState != SendStateNotSent {
		t.Errorf("send state = %v, want %v — the window must never make a provably-unsent failure look replay-unsafe", f.SendState, SendStateNotSent)
	}
}

// TestDialWithinClassifiesTheTwoBoundsApart is the classification contract,
// stated once in both directions. The envelope's window is this proxy's own
// bound and reads as the endpoint's timeout; the caller's is a client
// disconnect. The two must never produce the same class, because the pool acts
// on the difference: one is fallback-eligible, the other ends the request.
func TestDialWithinClassifiesTheTwoBoundsApart(t *testing.T) {
	windowErr := ClassifyAttempt(context.Background(), &ExchangeTimeoutError{Elapsed: time.Second})
	if windowErr.Class != ClassTimeout {
		t.Errorf("the window classified as %v, want %v", windowErr.Class, ClassTimeout)
	}
	if windowErr.SendState != SendStateUnknown {
		t.Errorf("the window's send state = %v, want %v: a peer that answered nothing may have the request", windowErr.SendState, SendStateUnknown)
	}

	callerErr := ClassifyAttempt(context.Background(), &ExchangeDeadlineError{cause: context.Canceled})
	if callerErr.Class != ClassCanceled {
		t.Errorf("the caller's own end classified as %v, want %v", callerErr.Class, ClassCanceled)
	}
	if windowErr == callerErr {
		t.Error("the two bounds classify identically; ownership has been collapsed")
	}

	// And the caller's own context still wins the owner check when it has
	// ended, whichever typed error arrived: a client that left takes the
	// request with it, whatever the dial was doing.
	dead, cancel := context.WithCancel(context.Background())
	cancel()
	if f := ClassifyAttempt(dead, &ExchangeTimeoutError{Elapsed: time.Second}); !f.CallerTerminated || f.Cause != CauseCallerCanceled {
		t.Errorf("a dead caller did not own the failure: %+v", f)
	}
}

// TestDialWithinNoWindowLeavesTheDialUntouched pins the unmetered path. A
// caller that does not claim an exchange gets the historical Do(req) with no
// proxy-imposed bound whatsoever — no derived context, no timer, and above all
// a request this function did not clone.
func TestDialWithinNoWindowLeavesTheDialUntouched(t *testing.T) {
	up := newMutedStream(t)
	req := newRequest(t, up.srv.URL)
	resp, err := DialWithin(0, context.Background(), NewDirectClient(), req)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.Body == nil {
		t.Fatal("no body")
	}
	// The request itself is untouched: no clone, no rewritten context.
	if req.Context() != context.Background() {
		t.Error("the unmetered path altered the caller's own request")
	}
}

// TestHandoffStreamStopsTheWindow is the direction the header-only tests above
// cannot see: the commit WINNING the race. Before the handoff the response is
// already in the caller's hands, and nothing about the response has released the
// window yet — a caller that only ever commits an event stream would keep it
// armed for the whole stream. The timer is a real goroutine holding the body
// closure, so a commit that fails to remove it truncates a committed stream at
// the candidate's max-elapsed, which is the exact defect this change exists to
// remove.
func TestHandoffStreamStopsTheWindow(t *testing.T) {
	up := newMutedStream(t)
	const window = 50 * time.Millisecond

	resp, err := DialWithin(window, context.Background(), NewDirectClient(), newRequest(t, up.srv.URL))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	HandoffStream(resp)

	// Well past the window. If the timer survived the commit, it fired by now,
	// claimed the exchange and closed the body this stream is about to read.
	time.Sleep(8 * window)

	read := make(chan error, 1)
	go func() {
		_, err := resp.Body.Read(make([]byte, 1))
		read <- err
	}()
	select {
	case err := <-read:
		t.Fatalf("committed body read failed %v — the window timer was never stopped by the handoff", err)
	case <-time.After(300 * time.Millisecond):
		// The body is still live: the handoff removed the timer.
	}
}

// TestHandoffStreamIsIdempotentAndForeignSafe covers the two ways a caller can
// reach the release twice, and the body that has no release at all. Both must
// be inert: a second stop of an already-stopped timer, or a release of an
// exchange that another response owns, would corrupt whichever exchange is
// actually running — the same failure the sync.Once guards exist to prevent.
func TestHandoffStreamIsIdempotentAndForeignSafe(t *testing.T) {
	up := newMutedStream(t)

	t.Run("a second commit on one response is inert", func(t *testing.T) {
		resp, err := DialWithin(50*time.Millisecond, context.Background(), NewDirectClient(), newRequest(t, up.srv.URL))
		if err != nil {
			t.Fatalf("dial: %v", err)
		}
		defer func() { _ = resp.Body.Close() }()
		HandoffStream(resp)
		HandoffStream(resp)
		// The body is still the live, committed one: a second release did not
		// cancel the context underneath it.
		time.Sleep(100 * time.Millisecond)
		read := make(chan error, 1)
		go func() {
			_, err := resp.Body.Read(make([]byte, 1))
			read <- err
		}()
		select {
		case err := <-read:
			t.Fatalf("read after a repeated commit: %v — the release ran twice", err)
		case <-time.After(200 * time.Millisecond):
		}
	})

	t.Run("a commit racing the body's own Close is inert", func(t *testing.T) {
		resp, err := DialWithin(time.Hour, context.Background(), NewDirectClient(), newRequest(t, up.srv.URL))
		if err != nil {
			t.Fatalf("dial: %v", err)
		}
		// Both orders occur in production — a client that hangs up at the same
		// moment the content type is read — and neither may double-release.
		var wg sync.WaitGroup
		for _, commit := range []bool{false, true} {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if commit {
					HandoffStream(resp)
					return
				}
				_ = resp.Body.Close()
			}()
		}
		wg.Wait()
	})

	t.Run("a body with no window of its own is untouched", func(t *testing.T) {
		// The nil response, a body the envelope never funded, and a body that
		// never came through DialWithin at all: none of them has a window, and
		// all of them must come out the other side unchanged.
		HandoffStream(nil)
		resp := &http.Response{Body: acceptUndecided{io.NopCloser(strings.NewReader("x"))}}
		HandoffStream(resp)
		// A pool stacks its own wrapper on top; the window is one layer down and
		// must still be found, or a pooled event stream is cut at the
		// candidate's max-elapsed while a direct one is not.
		inner := acceptUndecided{io.NopCloser(strings.NewReader("x"))}
		stack := &http.Response{Body: &releaseBody{ReadCloser: inner}}
		HandoffStream(stack)
	})
}

// TestHandoffStreamReachesTheWindowUnderTheProductionStack pins the
// composition a committed answer actually carries. A pooled continuation hop
// stacks THREE decorators over the window DialWithin installed: the pool's
// permit holder, then the hop's own context binder. A lookup that stopped at a
// fixed depth would find the hop's wrapper, see no window, and return — and
// the window would then fire at the candidate's max-elapsed and close a body
// spliced into a stream the client is already reading. That is the exact
// regression this handoff exists to prevent, reintroduced one layer up.
//
// The assertion is that the body SURVIVES the window, not merely that the call
// returns: a handoff that quietly found nothing and a handoff that worked both
// "do not panic", and only the first leaves a committed stream killable.
func TestHandoffStreamReachesTheWindowUnderTheProductionStack(t *testing.T) {
	up := newMutedStream(t)
	const window = 150 * time.Millisecond

	resp, err := DialWithin(window, context.Background(), NewDirectClient(), newRequest(t, up.srv.URL))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	// The production stack, innermost first: the transport's window, the pool
	// member's permit holder, the hop's context binder.
	stack := &http.Response{Body: &hopBody{ReadCloser: &releaseBody{ReadCloser: resp.Body, fn: func() {}}}}
	HandoffStream(stack)
	defer func() { _ = stack.Body.Close() }()

	// Past the window by a wide margin, and still readable: the window was
	// reached and disarmed, not merely missed.
	time.Sleep(3 * window)
	read := make(chan error, 1)
	go func() {
		_, err := stack.Body.Read(make([]byte, 1))
		read <- err
	}()
	select {
	case err := <-read:
		t.Fatalf("read after the window elapsed through the production stack: %v "+
			"\u2014 the handoff did not reach the window, so a committed stream is cut "+
			"at the candidate's max-elapsed", err)
	case <-time.After(300 * time.Millisecond):
	}
}

// hopBody stands in for the continuation hop's own wrapper: a decorator that
// holds its inner body, closes it on Close, and exposes Unwrap so the handoff
// can reach the layer beneath. It exists here so the transport test pins the
// SHAPE of a caller's own layer without importing the proxy package.
type hopBody struct{ io.ReadCloser }

func (b *hopBody) Unwrap() io.ReadCloser { return b.ReadCloser }

// TestDialWithinClosesTheBodyWhenTheWindowWinsTheCommit covers the interval
// that is otherwise unobservable: the window firing at the very instant a
// response is committed. The interval is half-open, and this pins which side of
// it wins — the window does, and the response is dropped rather than returned,
// because a body whose connection the timer has already torn down would fail
// on its next read with no upstream event and no evidence this proxy caused it.
//
// The collision is driven through the arming seam rather than by timing, so the
// test decides the race instead of hoping to win it: the action the production
// timer runs is fired synchronously in the instant between the response
// existing and the body being published, which is the earliest instant a caller
// could have committed it.
func TestDialWithinClosesTheBodyWhenTheWindowWinsTheCommit(t *testing.T) {
	up := newMutedStream(t)
	served := atomic.Bool{}
	closed := atomic.Bool{}

	// The window the seam reports is an hour, and never elapses on its own —
	// the only thing that ends it is the action fired below.
	collided := false
	// The action is armed by the seam and fired by the doer, on the doer's own
	// goroutine. A real timer's function runs on the timer's goroutine, and that
	// is the only place this can be staged faithfully: firing it from inside
	// stop re-enters the exchange's once-guarded release while it is executing,
	// which a real timer can never do and which would deadlock the test rather
	// than test anything. So the doer fires it on return, in the exact instant
	// the response exists and before the caller can see it — the earliest a
	// commit could be attempted.
	var fire func()
	var fireOnce sync.Once
	arm := func(action func()) func() bool {
		fire = func() { fireOnce.Do(func() { collided = true; action() }) }
		return func() bool {
			if !served.Load() {
				t.Error("the response was committed before the peer answered; the collision this test pins did not happen")
			}
			// A timer that has already run is removed by no stop. Nothing reads
			// that verdict any more — the publish check decides on the race
			// instead — so the report is a plain true and the collision is
			// observable only through the refusal below.
			return true
		}
	}
	doer := doerFunc(func(req *http.Request) (*http.Response, error) {
		resp, err := NewDirectClient().Do(req)
		if err != nil {
			return nil, err
		}
		served.Store(true)
		// Track the body so the refusal's Close is observable. The tracking
		// wraps nothing this proxy relies on: it forwards Read and Close
		// untouched.
		resp.Body = closeRecorder{ReadCloser: resp.Body, closed: &closed}
		fire()
		return resp, nil
	})

	resp, err := dialWithin(time.Hour, arm, context.Background(), doer, newRequest(t, up.srv.URL))
	if err == nil {
		_ = resp.Body.Close()
		t.Fatal("a response that lost the commit race was returned; its body is a stream the window already tore down")
	}
	if !collided {
		t.Fatal("the window was never fired against a live response")
	}
	var ete *ExchangeTimeoutError
	if !errors.As(err, &ete) {
		t.Fatalf("failure = %T (%v), want *ExchangeTimeoutError", err, err)
	}
	if !closed.Load() {
		t.Error("the response that lost the commit race was not closed — its connection is leaked for a stream this proxy refused to relay")
	}
	if resp != nil {
		t.Error("a response accompanied the refusal")
	}
}

// closeRecorder reports whether the body it wraps was closed.
type closeRecorder struct {
	io.ReadCloser
	closed *atomic.Bool
}

func (c closeRecorder) Close() error {
	c.closed.Store(true)
	return c.ReadCloser.Close()
}

// doerFunc adapts a function to the Doer seam.
type doerFunc func(*http.Request) (*http.Response, error)

func (f doerFunc) Do(req *http.Request) (*http.Response, error) { return f(req) }

// TestDialWithinReleasesOnceUnderPressure is the stress case the handoff has to
// survive: many concurrent exchanges, each with a window, each with a body
// closed at an unpredictable moment relative to the window's firing. The
// invariant under test is that nothing panics, nothing deadlocks, and no body
// is left bound — i.e. that the release is genuinely exactly-once and not
// merely once on the path this test happens to take.
//
// It is run under -race in CI, so a release that races itself is a failure
// here rather than a suspicion.
func TestDialWithinReleasesOnceUnderPressure(t *testing.T) {
	up := newMutedStream(t)
	const workers = 24
	const window = 20 * time.Millisecond

	var wg sync.WaitGroup
	unexpected := make(chan error, workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			// Stagger the commit and the Close against the window so both
			// orders occur, and neither of them is on the path this test
			// would take by accident.
			delay := time.Duration(i%4) * 5 * time.Millisecond
			resp, err := DialWithin(window, context.Background(), NewDirectClient(), newRequest(t, up.srv.URL))
			if err != nil {
				// A window that beat the response is a legal outcome here.
				var ete *ExchangeTimeoutError
				if !errors.As(err, &ete) {
					unexpected <- err
				}
				return
			}
			if i%2 == 0 {
				time.Sleep(delay)
				HandoffStream(resp)
			}
			// Close twice on purpose: a double Close must not double-release.
			_ = resp.Body.Close()
			_ = resp.Body.Close()
		}(i)
	}
	wg.Wait()
	close(unexpected)
	for err := range unexpected {
		t.Errorf("unexpected failure: %v", err)
	}
}

// TestDialWithinDisarmsTheWindowOnEveryPathThatEnds pins the third defect: a
// timer that outlives its exchange. The window is an hour here and never fires,
// so a surviving timer cannot be observed by waiting for it — it is observed
// structurally, through the arming seam, by the fact that every path which ends
// an exchange must have stopped it.
//
// The three paths are the three an un-disarmed timer hides behind: a
// synchronous failure before any response, a buffered answer closed inside its
// window, and an abandoned dial. Each left a live time.AfterFunc holding the
// response closure until the whole envelope elapsed — one goroutine and one
// retained closure per exchange, on the busiest path in the service.
func TestDialWithinDisarmsTheWindowOnEveryPathThatEnds(t *testing.T) {
	up := newMutedStream(t)

	// stopped reports whether the arming seam's stop was ever called.
	type arming struct {
		armed   bool
		stopped bool
		fire    func()
	}
	newArming := func() *arming {
		a := &arming{}
		a.fire = func() {}
		return a
	}

	t.Run("a synchronous failure before any response", func(t *testing.T) {
		a := newArming()
		fail := errors.New("refused before the wire")
		doer := doerFunc(func(*http.Request) (*http.Response, error) { return nil, fail })

		var stopCalls atomic.Int64
		arm := func(action func()) func() bool {
			a.armed = true
			return func() bool {
				stopCalls.Add(1)
				a.stopped = true
				return true
			}
		}
		if _, err := dialWithin(time.Hour, arm, context.Background(), doer, newRequest(t, up.srv.URL)); !errors.Is(err, fail) {
			t.Fatalf("failure = %v, want the endpoint's own %v", err, fail)
		}
		if !a.armed {
			t.Fatal("the window was never armed; this test proved nothing")
		}
		if !a.stopped {
			t.Error("a failed dial left the window armed: a live timer holding the response closure for the whole envelope")
		}
		if stopCalls.Load() != 1 {
			t.Errorf("the window's stop was called %d times, want exactly 1", stopCalls.Load())
		}
	})

	t.Run("a buffered answer closed inside its window", func(t *testing.T) {
		a := newArming()
		var stopCalls atomic.Int64
		arm := func(func()) func() bool {
			a.armed = true
			return func() bool {
				stopCalls.Add(1)
				a.stopped = true
				return true
			}
		}
		resp, err := dialWithin(time.Hour, arm, context.Background(), NewDirectClient(), newRequest(t, up.srv.URL))
		if err != nil {
			t.Fatalf("dial: %v", err)
		}
		if a.stopped {
			t.Fatal("the window was already disarmed at headers; this test is about the close, not the commit")
		}
		_ = resp.Body.Close()
		if !a.stopped {
			t.Error("closing a buffered answer left the window armed")
		}
		if stopCalls.Load() != 1 {
			t.Errorf("the window's stop was called %d times, want exactly 1", stopCalls.Load())
		}
		// A second Close is what a retrying handler does, and it must not stop
		// the same timer again.
		_ = resp.Body.Close()
		if stopCalls.Load() != 1 {
			t.Errorf("the window's stop was called %d times after a second Close, want 1", stopCalls.Load())
		}
	})
}

// TestDialWithinBoundsBufferedBodyThroughEOF pins the deliberate semantic
// split: a buffered 2xx stays under its candidate's absolute `max-elapsed`
// through EOF, because it has not become a response the proxy may commit yet.
// A committed SSE body transfers to the stream recovery idle window instead
// (TestDialWithinCommittedBodyOutlivesTheWindow).
//
// The peer sends headers now but holds its JSON body longer than the window.
// The body must fail with the exchange envelope's typed timeout. If it instead
// completes, the transport has silently turned a candidate's absolute attempt
// bound into a header-only deadline — reintroducing issue #96 for slow buffered
// answers while all header-only tests still pass.
func TestDialWithinBoundsBufferedBodyThroughEOF(t *testing.T) {
	payload := strings.Repeat("abcdefghij", 2000) // 20 KB
	const window = 60 * time.Millisecond
	const delay = 6 * window

	headers := make(chan struct{})
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		close(headers)
		time.Sleep(delay)
		_, _ = io.WriteString(w, payload)
	}))
	defer up.Close()

	// A streaming REQUEST answered with buffered JSON. The answer is not
	// committed, so nothing is handed off: the envelope must still own the
	// body, and the request's own stream flag has no say in it.
	resp, err := DialWithin(window, context.Background(), NewDirectClient(), newRequest(t, up.URL))
	if err != nil {
		t.Fatalf("headers should be returned before the buffered body stalls: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	select {
	case <-headers:
	case <-time.After(5 * time.Second):
		t.Fatal("the peer never sent its headers")
	}

	_, err = io.ReadAll(resp.Body)
	var ete *ExchangeTimeoutError
	if !errors.As(err, &ete) {
		t.Fatalf("buffered body read = %T (%v), want *ExchangeTimeoutError", err, err)
	}
	if ete.Elapsed != window {
		t.Errorf("reported elapsed window = %v, want %v", ete.Elapsed, window)
	}
}

// TestDialWithinCompletesBufferedBodyInsideTheWindow is the opposite quiet
// direction: the window stays through EOF, but does not preempt a valid
// buffered answer that finishes within it.
func TestDialWithinCompletesBufferedBodyInsideTheWindow(t *testing.T) {
	payload := strings.Repeat("xy", 8192)
	const window = 200 * time.Millisecond

	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		time.Sleep(window / 8)
		_, _ = io.WriteString(w, payload)
	}))
	defer up.Close()

	resp, err := DialWithin(window, context.Background(), NewDirectClient(), newRequest(t, up.URL))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	got, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("reading a buffered answer inside its window: %v", err)
	}
	if string(got) != payload {
		t.Fatalf("body = %d bytes, want %d", len(got), len(payload))
	}
}

// TestDialWithinCallerCancellationStillOwnsBufferedBody proves that retaining
// the window through a buffered read did not replace the client's authority.
// The window is intentionally huge; only the caller can end this blocked read,
// and its original context error must travel unchanged rather than be reported
// as an envelope timeout suitable for retry/fallback.
func TestDialWithinCallerCancellationStillOwnsBufferedBody(t *testing.T) {
	up := newMutedStream(t)
	ctx, cancel := context.WithCancel(context.Background())
	resp, err := DialWithin(time.Hour, ctx, NewDirectClient(), newRequest(t, up.srv.URL))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	read := make(chan error, 1)
	go func() {
		_, err := resp.Body.Read(make([]byte, 1))
		read <- err
	}()
	cancel()
	select {
	case err := <-read:
		var ete *ExchangeTimeoutError
		if errors.As(err, &ete) {
			t.Fatalf("caller cancellation became %v, want the caller-owned context error", err)
		}
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("body read = %T (%v), want context.Canceled", err, err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("caller cancellation did not unblock the buffered body")
	}
}
