package usage

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// fakeRepo records every inserted batch and can fail a number of inserts
// before succeeding.
type fakeRepo struct {
	mu      sync.Mutex
	batches [][]Event
	fail    int // remaining failures
	err     error
}

func (f *fakeRepo) InsertEvents(ctx context.Context, events []Event) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.fail > 0 {
		f.fail--
		return f.err
	}
	clone := make([]Event, len(events))
	copy(clone, events)
	f.batches = append(f.batches, clone)
	return nil
}

func (f *fakeRepo) inserted() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, b := range f.batches {
		n += len(b)
	}
	return n
}

func testEvent(n int) Event {
	return Event{EventID: NewEventID(), RequestID: "req", PublicModel: "m", API: "chat", HTTPStatus: 200, Outcome: "completed", LatencyMS: int64(n)}
}

// Events fill the batch and flush without waiting for the window.

func TestPipelineBatchesBySize(t *testing.T) {
	repo := &fakeRepo{}
	p := NewPipeline(repo, zerolog.Nop())
	defer closePipeline(t, p)
	for n := 0; n < defaultBatchSize; n++ {
		p.Record(testEvent(n))
	}
	waitFor(t, func() bool { return repo.inserted() == defaultBatchSize })
	if got := p.Stats().Inserted; got != defaultBatchSize {
		t.Fatalf("Inserted = %d, want %d", got, defaultBatchSize)
	}
}

// Close drains the backlog: every recorded event lands.

func TestPipelineCloseDrains(t *testing.T) {
	repo := &fakeRepo{}
	p := NewPipeline(repo, zerolog.Nop())
	for n := 0; n < 50; n++ {
		p.Record(testEvent(n))
	}
	closePipeline(t, p)
	if repo.inserted() != 50 {
		t.Fatalf("inserted = %d, want 50", repo.inserted())
	}
	if got := p.Stats().Queued; got != 0 {
		t.Fatalf("Queued after close = %d, want 0", got)
	}
}

// Transient failures are retried; a persistent failure drops the batch,
// counts it, and keeps the pipeline alive for later events.

func TestPipelineRetriesTransientFailure(t *testing.T) {
	repo := &fakeRepo{fail: 2, err: errors.New("db momentarily away")}
	p := NewPipeline(repo, zerolog.Nop())
	defer closePipeline(t, p)
	p.Record(testEvent(1))
	waitFor(t, func() bool { return repo.inserted() == 1 })
	st := p.Stats()
	if st.Retries != 2 || st.InsertFailures != 2 {
		t.Fatalf("Retries/InsertFailures = %d/%d, want 2/2", st.Retries, st.InsertFailures)
	}
	if st.Dropped != 0 {
		t.Fatalf("Dropped = %d, want 0 (the event landed after retries)", st.Dropped)
	}
}

func TestPipelineDropsAfterPersistentFailure(t *testing.T) {
	repo := &fakeRepo{fail: 1 << 30, err: errors.New("db down")}
	p := NewPipeline(repo, zerolog.Nop())
	defer closePipeline(t, p)
	p.Record(testEvent(1))
	deadline := time.Now().Add(2 * time.Second)
	for p.Stats().Dropped == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	st := p.Stats()
	if st.Dropped != 1 {
		t.Fatalf("Dropped = %d, want 1", st.Dropped)
	}
	if st.InsertFailures != defaultMaxRetries+1 {
		t.Fatalf("InsertFailures = %d, want %d", st.InsertFailures, defaultMaxRetries+1)
	}

	// Recovery: the next event flows through normally.
	repo.mu.Lock()
	repo.fail = 0
	repo.mu.Unlock()
	p.Record(testEvent(2))
	waitFor(t, func() bool { return repo.inserted() == 1 })
}

// A full queue drops instead of blocking, and every drop is accounted.

func TestPipelineQueueFullDropsAndAccounts(t *testing.T) {
	repo := &fakeRepo{fail: 1 << 30, err: errors.New("db down")}
	p := NewPipeline(repo, zerolog.Nop())
	defer closePipeline(t, p)
	// With the repository failing, the writer keeps retrying; fill the
	// queue past capacity so Record must drop.
	for n := 0; n < defaultQueueCap+100; n++ {
		p.Record(testEvent(n))
	}
	deadline := time.Now().Add(2 * time.Second)
	for p.Stats().Dropped == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := p.Stats().Dropped; got == 0 {
		t.Fatal("full queue never dropped: Record must not block past the cap")
	}
}

// Concurrent Record calls are race-clean and none are lost.

func TestPipelineConcurrentRecord(t *testing.T) {
	repo := &fakeRepo{}
	p := NewPipeline(repo, zerolog.Nop())
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for n := 0; n < 100; n++ {
				p.Record(testEvent(g*100 + n))
			}
		}(g)
	}
	wg.Wait()
	closePipeline(t, p)
	if got := repo.inserted(); got != 800 {
		t.Fatalf("inserted = %d, want 800", got)
	}
}

// Record and Close intentionally race in production: a handler can finish
// as graceful shutdown starts. The only acceptable result is an inserted or
// explicitly counted shutdown-drop event — never a send-on-closed panic.
func TestPipelineConcurrentRecordAndClose(t *testing.T) {
	repo := &fakeRepo{}
	p := NewPipeline(repo, zerolog.Nop())
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for n := 0; n < 200; n++ {
				p.Record(testEvent(g*200 + n))
			}
		}(g)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	p.Close(ctx)
	wg.Wait()
	st := p.Stats()
	if got := int(st.Inserted + st.Dropped); got != 1600 {
		t.Fatalf("inserted+dropped = %d, want every 1600 record accounted", got)
	}
}

// slowRepo is the shape of a store that is reachable but wedged: every
// insert blocks until its caller's context is done, then reports that
// context — the flush deadline is what resolves the batch.
type slowRepo struct {
	delay time.Duration
}

func (s *slowRepo) InsertEvents(ctx context.Context, events []Event) error {
	select {
	case <-time.After(s.delay):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// A flush that outlives its deadline is a counted drop, not a silent
// remainder: the timeout class, the failure, and the drop all land in the
// stats the final accounting reports.

func TestPipelineFlushTimeoutDropsAndAccounts(t *testing.T) {
	repo := &slowRepo{delay: time.Second}
	p := newPipeline(repo, zerolog.Nop(), defaultQueueCap, defaultBatchSize, 20*time.Millisecond, 30*time.Millisecond, defaultMaxRetries)
	defer closePipeline(t, p)
	p.Record(testEvent(1))
	deadline := time.Now().Add(2 * time.Second)
	for p.Stats().Dropped == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	st := p.Stats()
	if st.Dropped != 1 || st.InsertFailures == 0 {
		t.Fatalf("Dropped/InsertFailures = %d/%d, want the timed-out batch counted", st.Dropped, st.InsertFailures)
	}
	if st.Queued != 0 {
		t.Fatalf("Queued = %d, want 0 (the batch left the queue when flushed)", st.Queued)
	}
}

// Close with an expired drain window must not hand the caller back into
// repository teardown while the writer is mid-insert: a bounded grace
// waits for the writer, whose own flush deadline guarantees it exits.

func TestPipelineCloseJoinsWriterAfterExpiredDrain(t *testing.T) {
	repo := &slowRepo{delay: 150 * time.Millisecond}
	p := newPipeline(repo, zerolog.Nop(), defaultQueueCap, defaultBatchSize, defaultFlushEvery, 5*time.Second, defaultMaxRetries)
	for n := 0; n < 10; n++ {
		p.Record(testEvent(n))
	}
	// An already-expired drain window sends Close down the join path.
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	started := time.Now()
	p.Close(ctx)
	if elapsed := time.Since(started); elapsed >= 5*time.Second {
		t.Fatalf("Close took %s, want bounded by the join grace", elapsed)
	}
	st := p.Stats()
	if st.Inserted != 10 {
		t.Fatalf("Inserted = %d, want 10 (the join let the final insert land)", st.Inserted)
	}
	if st.Dropped != 0 || st.Queued != 0 {
		t.Fatalf("Dropped/Queued = %d/%d, want 0/0", st.Dropped, st.Queued)
	}
}

func closePipeline(t *testing.T, p *Pipeline) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	p.Close(ctx)
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition never became true")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestNewEventIDShape(t *testing.T) {
	seen := make(map[string]bool)
	for n := 0; n < 1000; n++ {
		id := NewEventID()
		if len(id) != 36 || id[8] != '-' || id[13] != '-' || id[18] != '-' || id[23] != '-' {
			t.Fatalf("NewEventID = %q, want canonical 8-4-4-4-12 layout", id)
		}
		if id[14] != '4' {
			t.Fatalf("NewEventID = %q, want version 4", id)
		}
		if seen[id] {
			t.Fatalf("NewEventID repeated %q", id)
		}
		seen[id] = true
	}
}
