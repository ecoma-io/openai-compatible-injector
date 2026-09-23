package usage

import (
	"context"
	"sync"
	"time"

	"github.com/rs/zerolog"
)

// Pipeline bounds and decouples: the request path hands an event to Record
// and returns immediately — never blocks on the database, never sees a
// metering error, never lets a metering failure touch the response. One
// writer goroutine drains the bounded queue in batches through the
// repository, retrying transient failures a bounded number of times and
// dropping — loudly, with running totals — when a batch still will not land.
// Every counter that answers "did I lose data, and how much" is kept and
// exported.
//
// The queue is bounded on purpose: under a sustained database outage the
// alternative is unbounded memory on the request path. Dropping is the
// designed behavior; silence about it would be the defect.
type Pipeline struct {
	repo InsertRepository
	log  zerolog.Logger
	done chan struct{}

	batchSize   int
	flushEvery  time.Duration
	flushExpiry time.Duration
	maxRetries  int

	// mu serializes Record with Close. Holding it only around a channel send
	// (which is non-blocking by contract) prevents a handler completing
	// during shutdown from sending to a closed channel — and returns it as a
	// counted drop rather than a process panic. The writer never holds mu
	// while it calls the repository.
	mu        sync.Mutex
	events    chan Event
	accepting bool
	stats     Stats
	// droppedSinceReport bounds queue-full/drop reporting to one WARN per
	// writer flush rather than one per dropped event under pressure.
	droppedSinceReport int64
	closeOnce          sync.Once
}

// Ingest is the seam the request path depends on. A nil Ingest means
// metering is off.
type Ingest interface {
	Record(ev Event)
}

// InsertRepository is the ingestion half of the repository surface: write
// only. Queries live on PGRepository and are consumed separately; the
// pipeline cannot grow a read path by accident.
type InsertRepository interface {
	InsertEvents(ctx context.Context, events []Event) error
}

// Stats is the observability snapshot: everything needed to answer "is the
// meter keeping up, and what did it cost". The snapshot is safe to read
// concurrently with Record, flushing, and shutdown.
type Stats struct {
	Queued         int64 // events currently waiting in the queue
	Inserted       int64 // events that reached the repository
	Dropped        int64 // queue full, shutdown, or a batch that never landed
	InsertFailures int64
	Retries        int64
	Flushes        int64
	// LastFlush is when the last successful flush completed; zero means none
	// has. LastFlushDurationMS is that flush's insert duration.
	LastFlush           time.Time
	LastFlushDurationMS int64
}

// Pipeline tuning. The queue holds several seconds of burst traffic; the
// batch is small enough to flush promptly and large enough to keep insert
// overhead amortized. One flush gets a fixed deadline and a bounded retry
// count, so a batch that outlives both resolves into a counted drop — and
// Close's drain therefore lands every accepted event in inserted or
// dropped, never a silent remainder.
const (
	defaultQueueCap    = 8192
	defaultBatchSize   = 256
	defaultFlushEvery  = time.Second
	defaultFlushExpiry = 10 * time.Second
	defaultMaxRetries  = 3
	retryBackoff       = 100 * time.Millisecond
	// closeJoinGrace bounds the extra wait when the drain window expired
	// with the writer still mid-flush. Each flush carries its own
	// flushExpiry deadline, so the writer always exits on its own; this
	// grace lets a nearly-finished final insert land before the caller
	// tears the repository down underneath it.
	closeJoinGrace = 5 * time.Second
)

// NewPipeline starts the writer goroutine. Events recorded before Close are
// flushed on Close.
func NewPipeline(repo InsertRepository, log zerolog.Logger) *Pipeline {
	return newPipeline(repo, log, defaultQueueCap, defaultBatchSize, defaultFlushEvery, defaultFlushExpiry, defaultMaxRetries)
}

// newPipeline is the tuning surface: the windows must be fixed before the
// writer starts — it reads them without synchronization — so tests shrink
// them at construction, never after.
func newPipeline(repo InsertRepository, log zerolog.Logger, queueCap, batchSize int, flushEvery, flushExpiry time.Duration, maxRetries int) *Pipeline {
	p := &Pipeline{
		events:      make(chan Event, queueCap),
		repo:        repo,
		log:         log,
		done:        make(chan struct{}),
		batchSize:   batchSize,
		flushEvery:  flushEvery,
		flushExpiry: flushExpiry,
		maxRetries:  maxRetries,
		accepting:   true,
	}
	go p.writeLoop()
	return p
}

// Record hands one event to the pipeline. Non-blocking by contract: a full
// queue — or a pipeline that has begun shutdown — drops the event, counts it,
// and returns immediately. Metering pressure must never become proxy latency,
// and an event racing graceful shutdown must never panic the proxy.
func (p *Pipeline) Record(ev Event) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.accepting {
		p.stats.Dropped++
		p.droppedSinceReport++
		return
	}
	select {
	case p.events <- ev:
		p.stats.Queued++
	default:
		p.stats.Dropped++
		p.droppedSinceReport++
	}
}

// Close stops intake, drains what was already queued (bounded by ctx), and
// waits for the writer. Called after the server's drain: events from requests
// that finished within the shutdown grace still land. A timeout cannot kill a
// repository call, but it returns control to the process after a bounded
// grace that lets a nearly-finished final insert land; every accepted event
// ends in the stats as inserted or dropped either way, and records arriving
// after Close starts are explicitly counted as shutdown drops.
func (p *Pipeline) Close(ctx context.Context) {
	p.closeOnce.Do(func() {
		p.mu.Lock()
		p.accepting = false
		close(p.events)
		p.mu.Unlock()
		select {
		case <-p.done:
		case <-ctx.Done():
			// The drain window expired with the writer still working. Each
			// flush carries its own deadline, so it exits on its own; wait a
			// bounded extra grace rather than let the caller's repository
			// teardown race the final insert into a self-inflicted failure.
			select {
			case <-p.done:
			case <-time.After(closeJoinGrace):
			}
		}
	})
}

// Stats returns the observability snapshot.
func (p *Pipeline) Stats() Stats {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.stats
}

// writeLoop batches events and flushes them: when the batch fills, when the
// flush window elapses, and one final time at shutdown.
func (p *Pipeline) writeLoop() {
	defer close(p.done)
	ticker := time.NewTicker(p.flushEvery)
	defer ticker.Stop()
	batch := make([]Event, 0, p.batchSize)
	flush := func() {
		if len(batch) == 0 {
			p.reportPendingDrops()
			return
		}
		p.flush(batch)
		clear(batch)
		batch = batch[:0]
	}
	for {
		select {
		case ev, ok := <-p.events:
			if !ok {
				flush()
				return
			}
			batch = append(batch, ev)
			if len(batch) >= p.batchSize {
				flush()
			}
		case <-ticker.C:
			flush()
		}
	}
}

// flush inserts one batch with bounded retries. Failure is not an error to
// anyone — there is no caller to fail — it is accounting: retries counted,
// insert failures counted, and a batch that never lands is dropped with its
// size folded into the running total and one ERROR event.
func (p *Pipeline) flush(batch []Event) {
	p.mu.Lock()
	p.stats.Queued -= int64(len(batch))
	p.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), p.flushExpiry)
	defer cancel()
	start := time.Now()
	var err error
	for attempt := 0; attempt <= p.maxRetries; attempt++ {
		if ctx.Err() != nil {
			err = ctx.Err()
			break
		}
		if err = p.repo.InsertEvents(ctx, batch); err == nil {
			break
		}
		p.mu.Lock()
		p.stats.InsertFailures++
		p.mu.Unlock()
		if attempt < p.maxRetries {
			p.mu.Lock()
			p.stats.Retries++
			p.mu.Unlock()
			// The insert context carries the fixed flush deadline. Do not
			// sleep past it: an unavailable store must resolve this batch
			// into a counted drop promptly, especially during shutdown.
			select {
			case <-time.After(retryBackoff):
			case <-ctx.Done():
			}
		}
	}
	elapsed := time.Since(start).Milliseconds()
	if err != nil {
		// Persistent failure: the batch is dropped, loudly. The class only —
		// the driver's error text can embed infrastructure detail.
		p.mu.Lock()
		p.stats.Dropped += int64(len(batch))
		queued := p.stats.Queued
		failures := p.stats.InsertFailures
		retries := p.stats.Retries
		p.mu.Unlock()
		p.log.Error().
			Str("error_class", StoreErrorClass(err)).
			Int("dropped", len(batch)).
			Int64("queue_depth", queued).
			Int64("insert_failures", failures).
			Int64("retries", retries).
			Int64("duration_ms", elapsed).
			Msg("usage_flush_failed")
	} else {
		now := time.Now()
		p.mu.Lock()
		p.stats.Inserted += int64(len(batch))
		p.stats.Flushes++
		p.stats.LastFlush = now
		p.stats.LastFlushDurationMS = elapsed
		queued := p.stats.Queued
		p.mu.Unlock()
		p.log.Debug().
			Int("events", len(batch)).
			Int64("duration_ms", elapsed).
			Int64("queue_depth", queued).
			Msg("usage_flush_completed")
	}
	p.reportPendingDrops()
}

// reportPendingDrops emits the bounded overflow/shutdown loss signal after a
// flush window. The running Stats counter is the authoritative total; this
// event gives operators prompt, payload-free visibility that it changed.
func (p *Pipeline) reportPendingDrops() {
	p.mu.Lock()
	dropped := p.droppedSinceReport
	p.droppedSinceReport = 0
	total := p.stats.Dropped
	queued := p.stats.Queued
	p.mu.Unlock()
	if dropped > 0 {
		p.log.Warn().
			Int64("dropped", dropped).
			Int64("dropped_total", total).
			Int64("queue_depth", queued).
			Msg("usage_events_dropped")
	}
}
