package auth

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"sync"
	"testing"
	"time"
)

// The tests in this file are deliberately database-free: they pin the
// SHUTDOWN contract of PGStore, which is pure local state — a queue, a
// flusher goroutine, and one pool close. Running them against PostgreSQL
// would buy nothing (no SQL path is exercised) and would lose them from CI,
// where OAICR_TEST_DATABASE_URL is unset. The store is therefore built by
// hand, in-package, around a stub connector.
//
// stubConnector satisfies database/sql's connector seam without a server:
// Connect refuses every dial, so the flusher's batch write fails fast and is
// dropped exactly as the production contract allows, and Close returns the
// error the test chooses. That second half is what makes the idempotence
// test meaningful — with a real unreachable pool db.Close() returns nil, and
// "every caller sees the same error" would pass vacuously. With a failing
// close it fails the moment a later caller is handed a fresh nil.

// stubConnector never dials and always fails the pool's own close.
type stubConnector struct {
	closeErr error
}

func (c stubConnector) Connect(context.Context) (driver.Conn, error) {
	return nil, errors.New("stub connector never dials")
}

func (c stubConnector) Driver() driver.Driver { return stubDriver{} }

func (c stubConnector) Close() error { return c.closeErr }

// stubDriver exists only to satisfy driver.Connector.
type stubDriver struct{}

func (stubDriver) Open(string) (driver.Conn, error) {
	return nil, errors.New("stub driver never dials")
}

// newShutdownTestStore builds a PGStore the way NewPGStore does, minus the
// database: same bounded queue, same flusher, same done channel, and a pool
// whose Close returns closeErr.
func newShutdownTestStore(t *testing.T, closeErr error) *PGStore {
	t.Helper()
	s := &PGStore{
		db:      sql.OpenDB(stubConnector{closeErr: closeErr}),
		touches: make(chan touch, touchQueueCap),
		done:    make(chan struct{}),
	}
	go s.flushLoop()
	t.Cleanup(func() { _ = s.Close(context.Background()) })
	return s
}

// A touch that arrives after Close has returned must be the documented drop
// — the same no-op as a full queue — and never a send on a closed channel.
// This is the deterministic half of the race: the queue is provably sealed
// before the touches are issued, so the old code panics here every run.
func TestPGStoreTouchAfterCloseIsANoOp(t *testing.T) {
	s := newShutdownTestStore(t, nil)
	if err := s.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}

	for i := 0; i < 100; i++ {
		s.TouchLastUsed("pak_after_close", time.Now()) // must not panic
	}
	if got := len(s.touches); got != 0 {
		t.Fatalf("touches queued after Close = %d, want 0 (the queue is sealed)", got)
	}
}

// Close is idempotent in outcome: the pool close happens once, and every
// caller — concurrent or late — is handed that one result. With the old
// Once-and-local-err shape the second and later callers read a fresh nil, so
// a failed pool close was reported only to whoever arrived first.
func TestPGStoreCloseReportsTheSameErrorToEveryCaller(t *testing.T) {
	poolErr := errors.New("pool close failed")
	s := newShutdownTestStore(t, poolErr)

	const callers = 8
	errs := make([]error, callers)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			errs[i] = s.Close(context.Background())
		}(i)
	}
	close(start)
	wg.Wait()

	for i, err := range errs {
		if !errors.Is(err, poolErr) {
			t.Fatalf("caller %d saw err=%v, want the pool close error every caller must observe", i, err)
		}
	}
	// A caller arriving after the Once has fired gets the stored result too.
	if err := s.Close(context.Background()); !errors.Is(err, poolErr) {
		t.Fatalf("post-close caller saw err=%v, want the same pool close error", err)
	}
}

// Close still drains what the queue already holds before it returns: the
// flusher's final batch is the documented shutdown behavior, and the seal
// must not turn it into a discard. The pool close is sequenced after the
// flusher exits, which is why s.done being closed is a valid post-condition
// for "the queue was drained".
func TestPGStoreCloseDrainsTheQueue(t *testing.T) {
	s := newShutdownTestStore(t, nil)
	for i := 0; i < flushBatch/2; i++ {
		s.TouchLastUsed("pak_queued", time.Now())
	}
	if err := s.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := len(s.touches); got != 0 {
		t.Fatalf("queued touches after Close = %d, want 0 (the flusher drains before the pool closes)", got)
	}
	select {
	case <-s.done:
	default:
		t.Fatal("Close returned before the flusher exited")
	}
}

// The headline race: touches in flight from many goroutines while Close runs
// concurrently. No panic, no blocked caller, and — under -race — no
// reported data race on the queue or the seal.
func TestPGStoreTouchRacingCloseDoesNotPanic(t *testing.T) {
	s := newShutdownTestStore(t, nil)

	const touchers = 32
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < touchers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			for j := 0; j < 50; j++ {
				s.TouchLastUsed("pak_racing", time.Now())
			}
		}(i)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		_ = s.Close(context.Background())
	}()
	close(start)
	wg.Wait()

	// Everything after the race is a no-op, including from this goroutine.
	for i := 0; i < 50; i++ {
		s.TouchLastUsed("pak_racing", time.Now())
	}
}

// The stress variant: thousands of concurrent touches racing Close, repeated
// with several closers so the Once is contended as well. Run under -race
// with -count=20; a single send on the closed channel is a process-killing
// panic, so any regression here is loud.
func TestPGStoreTouchRacingCloseStress(t *testing.T) {
	s := newShutdownTestStore(t, nil)

	const (
		touchers   = 64
		perToucher = 500
		closers    = 8
	)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < touchers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			for j := 0; j < perToucher; j++ {
				s.TouchLastUsed("pak_stress", time.Now())
			}
		}(i)
	}
	for i := 0; i < closers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_ = s.Close(context.Background())
		}()
	}
	close(start)
	wg.Wait()

	if got := len(s.touches); got != 0 {
		t.Fatalf("touches left queued after the stress race = %d, want 0", got)
	}
}
