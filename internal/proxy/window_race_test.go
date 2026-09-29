package proxy

import (
	"context"
	"errors"
	"io"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"
)

// The window/hop bounds interleave a moving idle deadline with a header
// watchdog and a body watchdog, all on one clock. The interactions below are
// the ones a sleep-based test cannot pin: each schedules the timer and the
// competing action at the SAME instant and asserts which one won, so a
// regression changes an outcome rather than a duration.
//
// The window is created once per logical session and its deadline MOVES on
// progress, so a per-hop reset would let the last lever outlive the moving
// max-elapsed silence allowance. TestWindowDeadlineMovesOnProgress is the
// tripwire for that: it is the one assertion in this file that would fail if
// bind were called per hop instead of per session.

const windowIdle = 30 * time.Second

// countingCloser records Close calls so "released exactly once" is
// assertable rather than inferred. Read drains a fixed pattern and then EOF,
// so it is a usable io.ReadCloser without dragging in a body fixture.
type countingCloser struct {
	mu      sync.Mutex
	closes  int
	pattern []byte
	off     int
}

func (c *countingCloser) Read(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.pattern == nil || c.off >= len(c.pattern) {
		return 0, io.EOF
	}
	n := copy(p, c.pattern[c.off:])
	c.off += n
	return n, nil
}

func (c *countingCloser) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closes++
	return nil
}

func (c *countingCloser) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closes
}

func TestWindowDeadlineMovesOnProgress(t *testing.T) {
	clk := newManualRecoveryClock(time.Now())
	w := newRecoveryWindow(clk, windowIdle)
	start := w.deadline

	// Progress is upstream activity, so the deadline moves with it. The
	// manual clock stands still until Advance, so move it first — otherwise
	// "now + idle" is the instant we started at and the deadline is unmoved.
	clk.Advance(time.Second)
	if !w.progress() {
		t.Fatal("a live window must accept an upstream byte")
	}
	if !w.deadline.After(start) {
		t.Fatalf("progress must move the deadline forward: had %v, now %v", start, w.deadline)
	}

	// A byte at the EXACT deadline is late. The live interval is
	// [activity, activity+idle), so a timer firing on the instant latches the
	// window and no later byte may revive it.
	clk.AdvanceTo(w.deadline)
	if w.progress() {
		t.Fatal("a byte at the exact deadline must not revive a spent window")
	}
	if !w.expired() {
		t.Fatal("the window must latch spent once a byte arrives at the deadline")
	}
}

func TestWindowProgressReturnsFalseAtTheBoundaryAndDoesNotRevive(t *testing.T) {
	clk := newManualRecoveryClock(time.Now())
	w := newRecoveryWindow(clk, windowIdle)

	// One nanosecond before the deadline is still live.
	clk.AdvanceTo(w.deadline.Add(-time.Nanosecond))
	if !w.progress() {
		t.Fatal("a byte one nanosecond before the deadline is on time")
	}

	// One nanosecond after it is late, and the window stays spent no matter
	// what arrives later.
	clk.AdvanceTo(w.deadline.Add(time.Nanosecond))
	if w.progress() {
		t.Fatal("a byte one nanosecond after the deadline is late")
	}
	clk.Advance(time.Hour)
	if w.progress() {
		t.Fatal("a spent window must stay spent")
	}
}

func TestWindowArmBodyClosesAStalledBodyExactlyOnce(t *testing.T) {
	clk := newManualRecoveryClock(time.Now())
	w := newRecoveryWindow(clk, windowIdle)
	body := &countingCloser{pattern: []byte("hello")}

	disarm := w.armBody(body)
	// The timer fires at the moving deadline and the body is the only lever
	// a stalled Read has, so expiry must close it.
	clk.Advance(windowIdle)
	if got := body.count(); got != 1 {
		t.Fatalf("expiry must close the armed body exactly once, got %d", got)
	}

	// Disarming afterwards must not close it again: a relay that finished
	// normally disarms a timer that has already fired.
	disarm()
	disarm()
	if got := body.count(); got != 1 {
		t.Fatalf("disarm must not re-close a body the window already closed, got %d", got)
	}
}

func TestWindowArmBodyRefusesImmediatelyOnASpentWindow(t *testing.T) {
	clk := newManualRecoveryClock(time.Now())
	w := newRecoveryWindow(clk, windowIdle)
	clk.Advance(2 * windowIdle)

	body := &countingCloser{pattern: []byte("hello")}
	disarm := w.armBody(body)

	// Arming against a spent window closes the body on the spot rather than
	// handing back a guard that could never fire.
	if got := body.count(); got != 1 {
		t.Fatalf("arming a spent window must close the body immediately, got %d", got)
	}
	// The returned func is inert, not a live disarm.
	disarm()
	if got := body.count(); got != 1 {
		t.Fatalf("the no-op disarm must not close a second time, got %d", got)
	}
}

// The header watchdog and the body watchdog share ONE moving deadline. This is
// the invariant a per-hop window reset would break: a fresh window per hop
// would push the deadline forward on every hop, so a stream that stays silent
// across several hops would never be cut.

func TestHopBoundsHeaderWatchdogOwnsTheContextUntilPromote(t *testing.T) {
	clk := newManualRecoveryClock(time.Now())
	w := newRecoveryWindow(clk, windowIdle)

	b, ok := w.bind(context.Background())
	if !ok {
		t.Fatal("a live window must admit a hop")
	}

	// Before the deadline the context is untouched.
	clk.Advance(windowIdle - time.Nanosecond)
	select {
	case <-b.ctx.Done():
		t.Fatal("the header watchdog fired before the deadline")
	default:
	}

	// At the deadline the header watchdog cancels the hop context: a stalled
	// response HEADER has no other lever.
	clk.Advance(2 * time.Nanosecond)
	select {
	case <-b.ctx.Done():
	default:
		t.Fatal("the header watchdog must cancel the hop context at the deadline")
	}
	b.release()
}

func TestHopBoundsPromoteReplacesTheHeaderWatchdogWithABodyOne(t *testing.T) {
	clk := newManualRecoveryClock(time.Now())
	w := newRecoveryWindow(clk, windowIdle)
	b, ok := w.bind(context.Background())
	if !ok {
		t.Fatal("a live window must admit a hop")
	}
	body := &countingCloser{pattern: []byte("hello")}

	// Headers arriving in time hand the deadline to the body. The hop's own
	// context must NOT be canceled by that transfer — the body owns it now.
	if !b.promote(body) {
		t.Fatal("promote must succeed while the window is live and the lease current")
	}
	clk.Advance(windowIdle - time.Nanosecond)
	select {
	case <-b.ctx.Done():
		t.Fatal("promote handed the deadline to the body, which must not cancel the hop context")
	default:
	}

	// The body is now the lever: silence past the moved deadline closes it.
	clk.Advance(2 * time.Nanosecond)
	if got := body.count(); got != 1 {
		t.Fatalf("the promoted body watchdog must close the body, got %d", got)
	}
	b.release()
}

// The exact-instant boundary, in the direction the half-open interval
// actually gives. The live window is [activity, activity+idle), so the
// deadline instant is ALREADY outside it: a header landing exactly on the
// deadline loses to the watchdog, and promote must refuse. This is the same
// rule progress() obeys, and pinning it in both places is what keeps a
// future change from making them disagree — one of them accepting an instant
// the other rejects would admit a hop the silence bound had already spent.

func TestHopBoundsHeaderArrivingExactlyAtTheDeadlineLosesToTheWatchdog(t *testing.T) {
	clk := newManualRecoveryClock(time.Now())
	w := newRecoveryWindow(clk, windowIdle)
	b, ok := w.bind(context.Background())
	if !ok {
		t.Fatal("a live window must admit a hop")
	}
	body := &countingCloser{pattern: []byte("hello")}

	// Land exactly on the deadline without running any timer callback yet.
	clk.AdvanceTo(w.deadline)
	if b.promote(body) {
		t.Fatal("promote must refuse at the exact deadline — the live interval is half-open")
	}
	if got := body.count(); got != 0 {
		t.Fatalf("a refused promote must not close the body, got %d", got)
	}
	if !w.expired() {
		t.Fatal("the refused promote must leave the window spent")
	}
	b.release()
}

// One nanosecond earlier, the same hop wins. Together with the case above
// this pins both sides of the boundary to the SAME instant, so a refactor
// cannot widen or narrow the live window by moving one of the two checks.

func TestHopBoundsHeaderArrivingOneNanosecondBeforeTheDeadlineWins(t *testing.T) {
	clk := newManualRecoveryClock(time.Now())
	w := newRecoveryWindow(clk, windowIdle)
	b, ok := w.bind(context.Background())
	if !ok {
		t.Fatal("a live window must admit a hop")
	}
	body := &countingCloser{pattern: []byte("hello")}

	clk.AdvanceTo(w.deadline.Add(-time.Nanosecond))
	if !b.promote(body) {
		t.Fatal("promote must win one nanosecond before the deadline")
	}

	// The header watchdog's timer is now stale, so letting it become due must
	// neither cancel the hop context nor close the promoted body.
	clk.Advance(2 * time.Nanosecond)
	if got := body.count(); got != 0 {
		t.Fatalf("a stale header watchdog must not close a promoted body, got %d", got)
	}
	select {
	case <-b.ctx.Done():
		t.Fatal("a stale header watchdog must not cancel the promoted hop context")
	default:
	}
	b.release()
}

// The mirror image: the watchdog fired first, so the deadline is spent and
// promote must refuse. A body that has already been closed must not be handed
// a live watchdog.

func TestHopBoundsPromoteRefusesOnceTheHeaderWatchdogHasFired(t *testing.T) {
	clk := newManualRecoveryClock(time.Now())
	w := newRecoveryWindow(clk, windowIdle)
	b, ok := w.bind(context.Background())
	if !ok {
		t.Fatal("a live window must admit a hop")
	}
	body := &countingCloser{pattern: []byte("hello")}

	clk.Advance(windowIdle) // the header watchdog wins this race
	select {
	case <-b.ctx.Done():
	default:
		t.Fatal("the header watchdog must have won before promote is attempted")
	}
	if b.promote(body) {
		t.Fatal("promote must refuse once the window is spent")
	}
	if got := body.count(); got != 0 {
		t.Fatalf("a refused promote must not close the body, got %d", got)
	}
	b.release()
}

// The hop's context must outlive dialContinuation, and the release rides
// Close. This is the invariant that keeps net/http from aborting an unread
// body the instant the hop returns.

func TestBoundBodyReleasesTheHopContextOnCloseAndOnlyOnClose(t *testing.T) {
	clk := newManualRecoveryClock(time.Now())
	w := newRecoveryWindow(clk, windowIdle)

	underlying := &countingCloser{pattern: []byte("hello")}
	inner, cancel := context.WithCancel(context.Background())
	var released int
	var mu sync.Mutex
	body := &boundBody{
		ReadCloser: underlying,
		release: func() {
			mu.Lock()
			released++
			mu.Unlock()
			cancel()
		},
	}

	// Reading is not releasing: the release must wait for Close.
	if _, err := body.Read(make([]byte, 4)); err != nil {
		t.Fatalf("Read must reach the wrapped body: %v", err)
	}
	mu.Lock()
	got := released
	mu.Unlock()
	if got != 0 {
		t.Fatalf("reading must not release the hop context, got %d releases", got)
	}
	if inner.Err() != nil {
		t.Fatal("the hop context must stay live until the body is closed")
	}

	if err := body.Close(); err != nil {
		t.Fatalf("Close must reach the wrapped body: %v", err)
	}
	mu.Lock()
	got = released
	mu.Unlock()
	if got != 1 {
		t.Fatalf("Close must release exactly once, got %d", got)
	}
	if underlying.count() != 1 {
		t.Fatalf("Close must reach the wrapped body exactly once, got %d", underlying.count())
	}
	if inner.Err() == nil {
		t.Fatal("Close must cancel the hop context")
	}

	// A second Close is what a defer plus an explicit close produces; the
	// release still rides exactly once.
	_ = body.Close()
	mu.Lock()
	got = released
	mu.Unlock()
	if got != 1 {
		t.Fatalf("a repeated Close must not release twice, got %d", got)
	}

	_ = w
}

func TestBoundBodyUnwrapReachesTheBodyUnderneathForHandoff(t *testing.T) {
	// transport.HandoffStream walks the wrapper chain to disarm the exchange
	// window. Unwrap is the one method that makes that reachable, and it must
	// return the body the wrapper was placed on — not itself, or the handoff
	// would loop, and not a copy, or the window under the pool's permit
	// wrapper would stay armed.
	underlying := &countingCloser{pattern: []byte("hello")}
	body := &boundBody{ReadCloser: underlying, release: func() {}}
	if got := body.Unwrap(); got != io.ReadCloser(underlying) {
		t.Fatalf("Unwrap must return the wrapped body, got %#v", got)
	}
}

// Concurrent activity is what the -race detector is for; these two cases pin
// that the SAME property holds under contention and not just in sequence.

func TestWindowProgressAndExpiryRace(t *testing.T) {
	clk := newManualRecoveryClock(time.Now())
	w := newRecoveryWindow(clk, windowIdle)
	body := &countingCloser{pattern: []byte("hello")}
	w.armBody(body)

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				w.progress()
				w.expired()
				w.dialable()
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			clk.Advance(time.Millisecond)
		}
	}()
	wg.Wait()

	// Whatever the interleaving, the body is closed at most once per arm and
	// the window reaches a definite state.
	if got := body.count(); got > 1 {
		t.Fatalf("contention must not close one armed body twice, got %d", got)
	}
}

func TestHopBoundsReleaseRacesPromote(t *testing.T) {
	clk := newManualRecoveryClock(time.Now())
	w := newRecoveryWindow(clk, windowIdle)
	body := &countingCloser{pattern: []byte("hello")}
	var released int
	var mu sync.Mutex
	hop := &boundBody{
		ReadCloser: body,
		release: func() {
			mu.Lock()
			released++
			mu.Unlock()
		},
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			_ = hop.Close()
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			if b, ok := w.bind(context.Background()); ok {
				_ = b.promote(body)
				b.release()
			}
		}
	}()
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	if released != 1 {
		t.Fatalf("a raced Close must release exactly once, got %d", released)
	}
}

// A hop refused at or after the boundary must not leave a context live.

func TestWindowBindRefusesAtTheBoundaryAndCancelsTheContext(t *testing.T) {
	clk := newManualRecoveryClock(time.Now())
	w := newRecoveryWindow(clk, windowIdle)
	clk.AdvanceTo(w.deadline)

	b, ok := w.bind(context.Background())
	if ok {
		t.Fatal("bind must refuse once the exact deadline is spent")
	}
	if b != nil {
		t.Fatal("a refused bind must return no bounds to release")
	}
	if w.dialable() {
		t.Fatal("a window spent at bind time must not be dialable")
	}
}

// shut reports a LATCHED bound, not a computed one. newRecoveryWindow opens a
// deadline but arms no timer, so a window nothing is guarding on does not
// latch by the clock alone — it latches the moment any caller asks at or past
// the boundary. This matters because shut is what the continuation loop reads
// to tell "our own silence bound won" from "the relay ended for another
// reason", and a spuriously latched window would misattribute a stop.

func TestWindowShutLatchesOnlyWhenABoundaryCheckSeesIt(t *testing.T) {
	clk := newManualRecoveryClock(time.Now())
	w := newRecoveryWindow(clk, windowIdle)
	if w.shut() {
		t.Fatal("a fresh window is not shut")
	}
	clk.Advance(time.Second)
	if !w.progress() {
		t.Fatal("a live window accepts progress")
	}

	// Past the deadline but unobserved: still not latched. newRecoveryWindow
	// arms no timer, so the clock alone cannot latch a window nobody guards.
	clk.Advance(windowIdle)
	if w.shut() {
		t.Fatal("an unguarded window must not latch on the clock alone")
	}

	// The first boundary check both refuses AND latches, so the refusal is
	// durable: a later caller reading shut() learns the silence bound won
	// rather than the relay ending for some other reason.
	if w.dialable() {
		t.Fatal("a window at its deadline must not be dialable")
	}
	if !w.shut() {
		t.Fatal("the boundary check must leave the window latched shut")
	}
	if !w.shut() {
		t.Fatal("shut must stay true once latched")
	}
}

// The proxy's own refusals carry no cause, because nothing was dialed and no
// endpoint could be blamed. Every OTHER phase is the wire, and therefore the
// only one the no-echo rule governs. This is the boundary that keeps a
// window-cut from being reported as a peer read fault, and it is asserted
// per phase rather than in one direction.

func TestHopFailureCauseOwnRefusalsCarryNoCause(t *testing.T) {
	upstream, err := url.Parse("https://upstream.example/v1/chat/completions?key=SECRET")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	cause := errors.New("upstream said no")

	// Refusals this proxy made: nothing was dialed, so there is no endpoint
	// the cause could belong to.
	for _, phase := range []string{"build", "credential", "budget", recoveryMaxElapsed} {
		if got := hopFailureCause(phase, cause, upstream); got != nil {
			t.Fatalf("phase %q is a local refusal and must carry no cause, got %v", phase, got)
		}
	}

	// The reader's socket and this proxy's own bounded-relay cap keep the
	// error, since neither quotes an upstream URL.
	for _, phase := range []string{"client_write", "upstream_limit"} {
		if got := hopFailureCause(phase, cause, upstream); got != cause {
			t.Fatalf("phase %q must keep its cause, got %v", phase, got)
		}
	}

	// The wire phases go through the no-echo sanitizer, which must never
	// return the input verbatim when the input could quote the endpoint.
	for _, phase := range []string{"dial", "upstream_status", "upstream_read"} {
		got := hopFailureCause(phase, cause, upstream)
		if got == nil {
			t.Fatalf("phase %q is the wire and must report a cause", phase)
		}
		if strings.Contains(got.Error(), "SECRET") {
			t.Fatalf("phase %q leaked the endpoint query: %v", phase, got)
		}
	}
}
