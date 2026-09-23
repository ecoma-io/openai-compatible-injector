package transport

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"sync"
	"time"
)

// errExhausted is returned when a pool dialed nothing: every member was
// statically ineligible for the request, in a health cooldown, or at its
// concurrency cap. It is a pool-level condition, not an endpoint failure —
// no member can be blamed, so it carries no cause. Static text only.
var errExhausted = errors.New("egress pool: no eligible endpoint available")

// poolDoer is the Doer/Executor for one EgressPool transport. It is
// stateless glue: the scheduler position, health state, concurrency
// permits, and the in-flight lease all live in the shared poolState, which
// the registry keys by pool identity so it survives unchanged reloads.
type poolDoer struct {
	pool *Pool
	st   *poolState
}

// memberState is one member's runtime slice: the memoized endpoint client
// (content-keyed, shared with any plain transport of the same config),
// its passive health, and its concurrency limiter.
type memberState struct {
	client Doer
	health *endpointHealth
	lim    *limiter
}

// poolState is the mutable runtime of one pool identity. Fields split into
// three lock domains: mu owns leases/retirement (registry-facing), schedMu
// owns the scheduler cursor/weights AND the permit acquisition that turns a
// candidate into a selection, and each member owns its health and limiter
// mutexes. members is immutable after construction.
//
// Lock order (one direction only, so no path can deadlock): schedMu is
// always the outer lock when a member's health or limiter mutex is taken —
// selection reads health and acquires permits under it — and nothing ever
// takes schedMu while holding either of those. mu (leases) and r.mu
// (registry) never nest with schedMu at all.
type poolState struct {
	pool    *Pool
	key     string // the registry's doer-map key for this identity
	members []memberState
	clock   func() time.Time

	mu       sync.Mutex
	leases   int
	retired  bool
	onRetire func()

	schedMu sync.Mutex
	cursor  int
	cw      []int64
}

// endpointHealth tracks one member's consecutive fallback-eligible
// failures. Any response — 4xx/5xx included — counts as success: the path
// delivered an answer, which is all passive health can ask for.
type endpointHealth struct {
	mu        sync.Mutex
	fails     int
	until     time.Time // zero: healthy
	threshold int       // 0: strikes disabled
	cooldown  time.Duration
	clock     func() time.Time
}

func (h *endpointHealth) usable() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return !h.clock().Before(h.until)
}

func (h *endpointHealth) success() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.fails = 0
	h.until = time.Time{}
}

func (h *endpointHealth) strike() {
	if h.threshold <= 0 {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.fails++
	if h.fails >= h.threshold {
		h.until = h.clock().Add(h.cooldown)
		h.fails = 0
	}
}

// limiter is one member's concurrency cap. tryAcquire is non-blocking: a
// saturated member is skipped (no attempt, no health strike), because a
// full member is not a failing one.
type limiter struct {
	mu  sync.Mutex
	cur int
	max int // 0: unlimited
}

func (l *limiter) tryAcquire() bool {
	if l.max <= 0 {
		return true
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.cur >= l.max {
		return false
	}
	l.cur++
	return true
}

func (l *limiter) release() {
	if l.max <= 0 {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.cur > 0 {
		l.cur--
	}
}

// selectInitial chooses the request's first endpoint and returns its index
// with the member's concurrency permit already held, or -1 when no member
// is currently usable. The scheduling contract is
//
//	static eligibility → dynamic eligibility → scheduler → permit → dial
//
// so a member in a health cooldown or at its concurrency cap is never
// selected: it consumes no scheduler turn (the cursor and the weighted
// counters move only when a member is actually taken), receives no strike,
// and costs no attempt. Because the permit is acquired inside the same
// schedMu critical section that decides the pick, two concurrent requests
// can never both observe the same last available permit and both dial —
// the loser sees a full member and moves on. No network I/O happens under
// the lock: health checks and permit attempts are in-memory operations.
func (st *poolState) selectInitial(eligible []bool) int {
	st.schedMu.Lock()
	defer st.schedMu.Unlock()
	if st.pool.Strategy == WeightedRoundRobin {
		return st.selectWeightedLocked(eligible)
	}
	n := len(eligible)
	for i := 0; i < n; i++ {
		j := (st.cursor + i) % n
		if !eligible[j] {
			continue
		}
		ms := &st.members[j]
		if !ms.health.usable() {
			continue
		}
		if !ms.lim.tryAcquire() {
			continue
		}
		// The turn is consumed by the member that was taken, not by the
		// ones passed over: an unhealthy or saturated head did not have
		// "its turn" — it was not in the running.
		st.cursor = (j + 1) % n
		return j
	}
	return -1
}

// selectWeightedLocked is the WeightedRoundRobin half of selectInitial:
// smooth weighted round-robin (nginx-style) restricted to the CURRENTLY
// eligible set. r.schedMu must be held.
//
// Per selection, candidates gain their weight, the leader pays the sum of
// the CANDIDATES' weights (never the whole pool's — an unavailable member
// must not inflate the price the winner pays), and the member taken keeps
// its paid-down credit while the others carry theirs forward. A member
// excluded from the candidate set has its credit reset to zero: unavailability
// cannot accumulate credit, so a recovered member re-enters with none and
// cannot burst ahead on stale balance.
//
// A candidate that turns out saturated (permit refused) has its round's
// step undone and is excluded from the retry — its turn is not consumed,
// and the smooth step reruns over the shrunken candidate set. At most one
// member is excluded per round, so the loop ends within n rounds.
func (st *poolState) selectWeightedLocked(eligible []bool) int {
	n := len(eligible)
	cand := make([]bool, n)
	remaining := 0
	for i := range eligible {
		// Statically eligible plus healthy decides the candidate set; the
		// permit gate runs per round below. Everyone else's credit zeroes:
		// no accumulation while unavailable, no burst on recovery.
		cand[i] = eligible[i] && st.members[i].health.usable()
		if cand[i] {
			remaining++
		} else {
			st.cw[i] = 0
		}
	}
	for remaining > 0 {
		total := int64(0)
		best := -1
		for i := 0; i < n; i++ {
			if !cand[i] {
				continue
			}
			st.cw[i] += int64(st.pool.Members[i].Weight)
			total += int64(st.pool.Members[i].Weight)
			if best == -1 || st.cw[i] > st.cw[best] {
				best = i
			}
		}
		st.cw[best] -= total
		if st.members[best].lim.tryAcquire() {
			return best
		}
		// Saturated, not taken: undo this round's step exactly, drop the
		// member, and rerun. The undo keeps the counters byte-identical to
		// a world where the round never ran, so saturation cannot tilt
		// future balance in anyone's favor.
		for i := 0; i < n; i++ {
			if cand[i] {
				st.cw[i] -= int64(st.pool.Members[i].Weight)
			}
		}
		st.cw[best] += total
		cand[best] = false
		remaining--
	}
	return -1
}

// begin/end bracket one in-flight Execute. The lease pins the pool state
// against a reload's Retain: a retired pool whose last lease ends is
// retired for real through onRetire (set by the registry at defer time).
func (st *poolState) begin() {
	st.mu.Lock()
	st.leases++
	st.mu.Unlock()
}

func (st *poolState) end() {
	st.mu.Lock()
	st.leases--
	retire := st.retired && st.leases <= 0
	fn := st.onRetire
	st.mu.Unlock()
	if retire && fn != nil {
		fn()
	}
}

// Execute runs the eligibility → schedule → bounded-attempt loop. The
// contract: any response ends the loop (statuses are answers, never
// triggers); only pre-response failures classified as proxy/connection/
// timeout fall back; the caller's own cancellation aborts everything
// without penalizing any member; zero dials is exhaustion, not an endpoint
// error.
func (p *poolDoer) Execute(ar *AttemptRequest) (*http.Response, AttemptInfo, error) {
	st := p.st
	st.begin()
	info := AttemptInfo{}

	// Static eligibility first — scheduling never sees a member the request
	// cannot legally use. These members consume no attempt, no strike, no
	// fallback slot: a 6 MB body was never a 413 on the small relay.
	eligible := make([]bool, len(p.pool.Members))
	any := false
	for i, m := range p.pool.Members {
		ok := true
		if ar.Streaming && !m.Streaming {
			ok = false
		}
		if m.MaxBodyBytes > 0 && int64(len(ar.Body)) > m.MaxBodyBytes {
			ok = false
		}
		eligible[i] = ok
		if ok {
			any = true
		}
	}

	// The dial order: the scheduler's initial pick, then the remaining
	// members in declaration order (cyclic) — fallback order is
	// deterministic and weight-free. Dynamic gates (health, concurrency)
	// gate every dial: the initial selection holds its member's permit by
	// construction; each fallback step re-checks health and acquires its
	// own permit. A member failing a gate consumes no attempt, no strike,
	// and no scheduler turn.
	maxAttempts := 1
	if p.pool.Fallback.Enabled {
		maxAttempts = p.pool.Fallback.MaxAttempts
	}
	first := -1
	if any {
		first = st.selectInitial(eligible)
	}
	attempts := 0
	var lastErr error
	for off := 0; off < len(p.pool.Members) && first >= 0; off++ {
		if attempts >= maxAttempts {
			break
		}
		idx := (first + off) % len(p.pool.Members)
		if !eligible[idx] {
			continue
		}
		ms := &st.members[idx]
		if off > 0 {
			if !ms.health.usable() {
				continue
			}
			if !ms.lim.tryAcquire() {
				continue
			}
		}
		resp, err := p.dial(ms, ar)
		attempts++
		info.Attempts = attempts
		info.Kind = p.pool.Members[idx].Endpoint.kindName()
		info.Target = p.pool.Members[idx].Endpoint.target()
		if err == nil {
			// An answer of any status. Health recovers; the permit and the
			// lease hold until the caller closes the body (the stream's
			// whole lifetime for SSE).
			ms.health.success()
			resp.Body = newReleaseBody(resp.Body, func() {
				ms.lim.release()
				st.end()
			})
			return resp, info, nil
		}
		ms.lim.release()
		// The context the attempt ran under decides ownership: a caller
		// cancellation or deadline that fired during the dial is terminal —
		// no fallback, no strike, no evidence — while a failure against a
		// still-live context is the endpoint's and keeps its bounded
		// fallback eligibility (a provider-local timeout included).
		f := ClassifyAttempt(ar.Ctx, err)
		if f.CallerTerminated {
			st.end()
			return nil, info, err
		}
		ms.health.strike()
		// Bounded per-attempt evidence for the access log: one record per
		// actually dialed-and-failed endpoint, canonical class and closed-set
		// cause only — never the error text, never more than maxAttempts
		// records per request.
		info.Failures = append(info.Failures, AttemptFailure{
			Kind:   info.Kind,
			Target: info.Target,
			Class:  f.Class.String(),
			Cause:  f.Cause,
		})
		lastErr = err
	}
	st.end()
	if attempts == 0 {
		info.Exhausted = true
		return nil, info, errExhausted
	}
	// At least one real attempt: the last error is an endpoint failure and
	// reaches the handler's existing transport-error path (sanitized there
	// like any other).
	return nil, info, lastErr
}

// dial builds this attempt's request from the AttemptRequest — a fresh
// request object per attempt (the caller's request, and any previous
// attempt's consumed body, are never reused) — and executes it on the
// member's own client.
func (p *poolDoer) dial(ms *memberState, ar *AttemptRequest) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ar.Ctx, ar.Method, ar.URL.String(), bytes.NewReader(ar.Body))
	if err != nil {
		return nil, err
	}
	req.Header = ar.Header.Clone()
	return ms.client.Do(req)
}

// Do satisfies Doer for contexts that resolve a pool but do not use the
// Executor seam (and for tests standing in for the handler). Request
// handling uses Execute; Do buffers the body — every body on this proxy's
// paths is buffered already.
func (p *poolDoer) Do(req *http.Request) (*http.Response, error) {
	var body []byte
	if req.Body != nil {
		b, err := io.ReadAll(req.Body)
		if err != nil {
			return nil, err
		}
		body = b
		_ = req.Body.Close()
	}
	resp, _, err := p.Execute(&AttemptRequest{
		Ctx:    req.Context(),
		Method: req.Method,
		URL:    req.URL,
		Header: req.Header,
		Body:   body,
	})
	return resp, err
}

// CloseIdleConnections drains the members' idle pooled connections — the
// registry's shutdown-time call reaches into pools through this.
func (p *poolDoer) CloseIdleConnections() {
	for i := range p.st.members {
		closeIdle(p.st.members[i].client)
	}
}

// releaseBody runs one cleanup exactly when the caller closes the response
// body — the member's concurrency permit and the pool's in-flight lease.
// SSE streams hold both for the stream's whole lifetime; a reload that
// retires the pool mid-stream sees the lease, not the stream.
type releaseBody struct {
	io.ReadCloser
	once sync.Once
	fn   func()
}

func newReleaseBody(rc io.ReadCloser, fn func()) *releaseBody {
	return &releaseBody{ReadCloser: rc, fn: fn}
}

func (b *releaseBody) Close() error {
	err := b.ReadCloser.Close()
	b.once.Do(b.fn)
	return err
}
