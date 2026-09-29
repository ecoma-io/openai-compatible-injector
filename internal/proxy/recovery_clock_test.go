package proxy

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fixedRecoveryClock is sufficient for walk tests that only read time. It
// deliberately never runs timers: those tests use an idle interval far beyond
// their synchronous request, while recovery-window tests use manualRecoveryClock
// below to drive reads and watchdog callbacks together.
type fixedRecoveryClock struct{ now time.Time }

func (c fixedRecoveryClock) Now() time.Time { return c.now }

func (fixedRecoveryClock) AfterFunc(time.Duration, func()) recoveryTimer {
	return stoppedRecoveryTimer{}
}

type stoppedRecoveryTimer struct{}

func (stoppedRecoveryTimer) Stop() bool { return true }

// manualRecoveryClock owns both halves of the recovery-time seam. Advance runs
// every due callback outside its mutex, in deadline order, and repeats until no
// callback remains due at the selected instant. A callback may therefore arm a
// replacement timer at the same instant without being missed.
type manualRecoveryClock struct {
	mu     sync.Mutex
	now    time.Time
	timers []*manualRecoveryTimer
}

type manualRecoveryTimer struct {
	clock   *manualRecoveryClock
	at      time.Time
	fn      func()
	stopped bool
	fired   bool
}

func newManualRecoveryClock(now time.Time) *manualRecoveryClock {
	return &manualRecoveryClock{now: now}
}

func (c *manualRecoveryClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *manualRecoveryClock) AfterFunc(d time.Duration, fn func()) recoveryTimer {
	c.mu.Lock()
	defer c.mu.Unlock()
	t := &manualRecoveryTimer{clock: c, at: c.now.Add(d), fn: fn}
	c.timers = append(c.timers, t)
	return t
}

func (t *manualRecoveryTimer) Stop() bool {
	t.clock.mu.Lock()
	defer t.clock.mu.Unlock()
	if t.stopped || t.fired {
		return false
	}
	t.stopped = true
	return true
}

func (c *manualRecoveryClock) Advance(d time.Duration) {
	c.AdvanceTo(c.Now().Add(d))
}

func (c *manualRecoveryClock) AdvanceTo(now time.Time) {
	c.mu.Lock()
	if now.Before(c.now) {
		c.mu.Unlock()
		panic("manual recovery clock moved backwards")
	}
	c.now = now
	c.mu.Unlock()

	for {
		var due *manualRecoveryTimer
		c.mu.Lock()
		for _, t := range c.timers {
			if !t.stopped && !t.fired && !t.at.After(c.now) &&
				(due == nil || t.at.Before(due.at)) {
				due = t
			}
		}
		if due != nil {
			due.fired = true
		}
		c.mu.Unlock()
		if due == nil {
			return
		}
		due.fn()
	}
}

// steppingRecoveryClock is the seam a request-level test uses when it needs the
// window to be REACHED at a chosen point in a real handler run: time stands
// still until a source body marks `dialed`, and `afterReads` further readings
// then return `at` before every reading after those steps `step` past it.
//
// Advancing itself is left to the watchdog's real timer, so an expiry is
// observable through the same close or cancel a production expiry uses and a
// deadline alone can never satisfy a test.
type steppingRecoveryClock struct {
	at         time.Time
	step       time.Duration
	afterReads int
	dialed     atomic.Bool
	reads      atomic.Int64
}

func (c *steppingRecoveryClock) Now() time.Time {
	if !c.dialed.Load() {
		return c.at
	}
	if c.reads.Add(1) <= int64(c.afterReads) {
		return c.at
	}
	return c.at.Add(c.step)
}

// AfterFunc leaves the deadline to Go's scheduler: a window that genuinely
// expires closes the body or cancels the hop, and that is the effect under test.
func (*steppingRecoveryClock) AfterFunc(d time.Duration, f func()) recoveryTimer {
	return time.AfterFunc(d, f)
}

// stubRecoveryClock installs c as the request's recovery clock for one test and
// restores the previous one afterwards.
func stubRecoveryClock(t *testing.T, c recoveryClock) {
	t.Helper()
	orig := retryClock
	t.Cleanup(func() { retryClock = orig })
	retryClock = c
}
