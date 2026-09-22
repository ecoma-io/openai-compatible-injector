package transport

import (
	"context"
	"errors"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"
)

// The scheduler-math suite: deterministic drives of selectInitial over
// scripted health/concurrency states. The weighted half pins the smooth
// algorithm's exact sequences and its eligibility invariants — credit
// accumulates only while a member is actually eligible, an unavailable
// member neither consumes turns nor banks credit, and a recovered member
// re-enters with none — instead of spot-checking one arithmetic line.

// weightedMembers builds an n-member weighted pool state with health
// enabled at the given threshold and unlimited concurrency, plus the
// all-statically-eligible vector selectInitial expects from Execute.
func weightedMembers(t *testing.T, weights []int, threshold int, cooldown time.Duration, clock func() time.Time) (*poolState, []bool) {
	t.Helper()
	members := make([]Member, len(weights))
	clients := make([]Doer, len(weights))
	for i, w := range weights {
		members[i] = Member{Endpoint: Config{}, Streaming: true, Weight: w}
		clients[i] = &stubEndpoint{script: []stubResult{okResult("{}")}}
	}
	p := NewPool(members, WeightedRoundRobin,
		FallbackPolicy{Enabled: true, MaxAttempts: len(members)},
		HealthPolicy{Enabled: threshold > 0, FailureThreshold: threshold, Cooldown: cooldown})
	if clock == nil {
		clock = time.Now
	}
	st := &poolState{
		pool:    p,
		key:     "pool " + p.Identity(),
		members: make([]memberState, len(members)),
		clock:   clock,
		cw:      make([]int64, len(members)),
	}
	for i := range members {
		st.members[i] = memberState{
			client: clients[i],
			health: &endpointHealth{threshold: threshold, cooldown: cooldown, clock: clock},
			lim:    &limiter{max: members[i].MaxConcurrency},
		}
	}
	eligible := make([]bool, len(members))
	for i := range eligible {
		eligible[i] = true
	}
	return st, eligible
}

// letter renders a pick index as A/B/C for the sequence assertions.
func letter(i int) byte {
	if i < 0 {
		return '-'
	}
	return byte('A' + i)
}

// driveSelections runs selectInitial n times and returns the pick sequence.
func driveSelections(st *poolState, eligible []bool, n int) string {
	out := make([]byte, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, letter(st.selectInitial(eligible)))
	}
	return string(out)
}

// pickCounts maps a sequence onto per-member counts.
func pickCounts(seq string) map[byte]int {
	counts := map[byte]int{}
	for i := 0; i < len(seq); i++ {
		counts[seq[i]]++
	}
	return counts
}

// TestSchedulerWeightedFiveOne pins the exact smooth sequence for weights
// 5:1, both healthy: three A, one B, two A per six-pick cycle — the
// deterministic interleaving smooth WRR is chosen for.
func TestSchedulerWeightedFiveOne(t *testing.T) {
	st, eligible := weightedMembers(t, []int{5, 1}, 0, time.Second, nil)
	seq := driveSelections(st, eligible, 12)
	const cycle = "AAABAA"
	want := strings.Repeat(cycle, 2)
	if string(seq) != want {
		t.Fatalf("sequence = %s, want %s", seq, want)
	}
	// State returns to zero at each cycle boundary: no drift.
	for i, cw := range st.cw {
		if cw != 0 {
			t.Errorf("cw[%d] = %d after two full cycles, want 0", i, cw)
		}
	}
}

// TestSchedulerWeightedFiveOneThirdUnavailable pins that an unhealthy
// member is excluded from the weighted state entirely: the 5:1 pair keeps
// its exact sequence over the remaining candidates, the sick member is
// never picked, and its credit never accumulates while it is out.
func TestSchedulerWeightedFiveOneThirdUnavailable(t *testing.T) {
	clk := newFakeClock()
	st, eligible := weightedMembers(t, []int{5, 1, 1}, 1, 30*time.Second, clk.Now)
	// Strike C into its cooldown (threshold 1).
	st.members[2].health.strike()

	seq := driveSelections(st, eligible, 12)
	if want := strings.Repeat("AAABAA", 2); string(seq) != want {
		t.Fatalf("sequence with C down = %s, want %s", seq, want)
	}
	if counts := pickCounts(seq); counts['C'] != 0 {
		t.Fatalf("unhealthy member picked %d times", counts['C'])
	}
	if st.cw[2] != 0 {
		t.Fatalf("unavailable member banked credit: cw[2] = %d, want 0", st.cw[2])
	}
}

// TestSchedulerWeightedHundredOneThirdUnavailable pins proportionality and
// smoothness at an extreme ratio over a changing candidate set: with
// weights 100:1:1 and C in cooldown, 202 picks split 200/2/0, B's two picks
// sit far apart (the smooth property — no end-of-cycle burst), and C banks
// nothing.
func TestSchedulerWeightedHundredOneThirdUnavailable(t *testing.T) {
	clk := newFakeClock()
	st, eligible := weightedMembers(t, []int{100, 1, 1}, 1, 30*time.Second, clk.Now)
	st.members[2].health.strike()

	seq := driveSelections(st, eligible, 202)
	counts := pickCounts(seq)
	if counts['A'] != 200 || counts['B'] != 2 || counts['C'] != 0 {
		t.Fatalf("counts = %v, want A=200 B=2 C=0", counts)
	}
	lastB, bPicks := -1, 0
	for i, c := range seq {
		if c == 'B' {
			if lastB >= 0 && i-lastB < 40 {
				t.Errorf("B picks %d and %d only %d apart: burst, not smooth", lastB, i, i-lastB)
			}
			lastB, bPicks = i, bPicks+1
		}
	}
	if bPicks != 2 {
		t.Fatalf("B picked %d times, want 2", bPicks)
	}
	if st.cw[2] != 0 {
		t.Fatalf("unavailable member banked credit: cw[2] = %d", st.cw[2])
	}
}

// TestSchedulerWeightedCooldownEnterExit pins the lifecycle across a health
// transition: an equal 1:1:1 pool runs proportionally, C trips into
// cooldown mid-run and is excluded immediately (its credit reset), and on
// recovery it rejoins with no burst — the next 21 picks split 7/7/7.
func TestSchedulerWeightedCooldownEnterExit(t *testing.T) {
	clk := newFakeClock()
	st, eligible := weightedMembers(t, []int{1, 1, 1}, 1, 30*time.Second, clk.Now)

	first := driveSelections(st, eligible, 9)
	if counts := pickCounts(first); counts['A'] != 3 || counts['B'] != 3 || counts['C'] != 3 {
		t.Fatalf("baseline counts = %v, want 3/3/3", counts)
	}

	// C trips (threshold 1) right before the next selection.
	st.members[2].health.strike()
	during := driveSelections(st, eligible, 12)
	if counts := pickCounts(during); counts['C'] != 0 {
		t.Fatalf("cooling member picked %d times in %s", counts['C'], during)
	}
	if counts := pickCounts(during); counts['A'] != 6 || counts['B'] != 6 {
		t.Fatalf("remaining pair = %v, want 6/6", counts)
	}
	if st.cw[2] != 0 {
		t.Fatalf("cooling member banked credit: cw[2] = %d", st.cw[2])
	}

	// Cooldown expires: C rejoins with zero credit and the pool is
	// proportionally smooth again — no catch-up burst. C's first pick is
	// third, not first: a burst would start at index 0.
	clk.advance(31 * time.Second)
	after := driveSelections(st, eligible, 21)
	if counts := pickCounts(after); counts['A'] != 7 || counts['B'] != 7 || counts['C'] != 7 {
		t.Fatalf("post-recovery counts = %v, want 7/7/7", counts)
	}
	if idx := indexByte(after, 'C'); idx != 2 {
		t.Fatalf("recovered member picked at index %d, want 2 (smooth re-entry, no burst): %s", idx, after)
	}
}

func indexByte(s string, c byte) int {
	for i := range s {
		if s[i] == c {
			return i
		}
	}
	return -1
}

// TestSchedulerWeightedSaturationFreezesCredit pins the concurrency half:
// while a healthy member's permit is exhausted it is never picked, and its
// weighted credit is frozen (every failed round is undone exactly), so
// recovery resumes the smooth state instead of bursting on stale balance.
func TestSchedulerWeightedSaturationFreezesCredit(t *testing.T) {
	st, eligible := weightedMembers(t, []int{5, 1}, 0, time.Second, nil)
	st.members[0].lim.max = 1
	// select-and-release: each pick's permit goes back immediately, so a
	// member is free again for the next selection (selection HOLDS the
	// permit it grants).
	pick := func() byte {
		idx := st.selectInitial(eligible)
		if idx >= 0 {
			st.members[idx].lim.release()
		}
		return letter(idx)
	}

	// Baseline: two picks to settle the smooth state, then freeze.
	pick()
	pick()
	cwA, cwB := st.cw[0], st.cw[1]

	// Hold A's permit: A is saturated.
	if !st.members[0].lim.tryAcquire() {
		t.Fatal("failed to saturate the fixture")
	}
	for i := 0; i < 20; i++ {
		if got := pick(); got != 'B' {
			t.Fatalf("pick %d while A saturated = %c, want B", i, got)
		}
	}
	if st.cw[0] != cwA || st.cw[1] != cwB {
		t.Fatalf("saturation moved credit: cw = %v, want frozen %d/%d", st.cw, cwA, cwB)
	}

	// Release the permit: the 5:1 ratio holds over the next window, from
	// the resumed state (no burst).
	st.members[0].lim.release()
	var sb strings.Builder
	for i := 0; i < 60; i++ {
		sb.WriteByte(pick())
	}
	if counts := pickCounts(sb.String()); counts['A'] != 50 || counts['B'] != 10 {
		t.Fatalf("post-release counts = %v, want A=50 B=10 over 60", counts)
	}
}

// TestSchedulerRoundRobinUnhealthySparesTurn pins the round-robin cursor
// contract behind the new ordering: a cooling member is passed over without
// consuming its turn, so it is picked first again once recovered.
func TestSchedulerRoundRobinUnhealthySparesTurn(t *testing.T) {
	clk := newFakeClock()
	members := []Member{{Endpoint: Config{}, Streaming: true, Weight: 1}, {Endpoint: Config{}, Streaming: true, Weight: 1}}
	p := NewPool(members, RoundRobin,
		FallbackPolicy{Enabled: true, MaxAttempts: 2},
		HealthPolicy{Enabled: true, FailureThreshold: 1, Cooldown: 30 * time.Second})
	st := &poolState{
		pool:    p,
		key:     "pool " + p.Identity(),
		members: make([]memberState, 2),
		clock:   clk.Now,
		cw:      make([]int64, 2),
	}
	for i := range members {
		st.members[i] = memberState{
			client: &stubEndpoint{script: []stubResult{okResult("{}")}},
			health: &endpointHealth{threshold: 1, cooldown: 30 * time.Second, clock: clk.Now},
			lim:    &limiter{},
		}
	}
	eligible := []bool{true, true}

	if got := st.selectInitial(eligible); got != 0 {
		t.Fatalf("first pick = %d, want 0", got)
	}
	// A cools down before its next turn; B takes the request instead.
	st.members[0].health.strike()
	if got := st.selectInitial(eligible); got != 1 {
		t.Fatalf("pick while A cooling = %d, want 1", got)
	}
	// Recovery: A is picked first — its turn was never consumed.
	clk.advance(31 * time.Second)
	if got := st.selectInitial(eligible); got != 0 {
		t.Fatalf("pick after recovery = %d, want 0 (turn was spared)", got)
	}
}

// TestSchedulerAllBusyIsNoSelection pins the exhaustion shape at the
// selection layer: every member saturated (or cooling) yields -1 with zero
// permits taken and no counter movement.
func TestSchedulerAllBusyIsNoSelection(t *testing.T) {
	members := []Member{{Endpoint: Config{}, Streaming: true, Weight: 1}, {Endpoint: Config{}, Streaming: true, Weight: 1}}
	p := NewPool(members, WeightedRoundRobin,
		FallbackPolicy{Enabled: true, MaxAttempts: 2},
		HealthPolicy{Enabled: true, FailureThreshold: 1, Cooldown: 30 * time.Second})
	st := &poolState{
		pool:    p,
		key:     "pool " + p.Identity(),
		members: make([]memberState, 2),
		clock:   time.Now,
		cw:      make([]int64, 2),
	}
	for i := range members {
		st.members[i] = memberState{
			client: &stubEndpoint{script: []stubResult{okResult("{}")}},
			health: &endpointHealth{threshold: 1, cooldown: 30 * time.Second, clock: time.Now},
			lim:    &limiter{max: 1},
		}
	}
	eligible := []bool{true, true}
	// Saturate both members.
	for i := range st.members {
		if !st.members[i].lim.tryAcquire() {
			t.Fatal("failed to saturate the fixture")
		}
	}
	if got := st.selectInitial(eligible); got != -1 {
		t.Fatalf("selection over saturated pool = %d, want -1", got)
	}
	for i := range st.members {
		st.members[i].lim.release()
	}
	if st.cw[0] != 0 || st.cw[1] != 0 {
		t.Fatalf("failed selection moved credit: %v", st.cw)
	}
}

// TestSchedulerChurnUnderConcurrency hammers selection from many goroutines
// while health and permits flip underneath — the race detector is the
// assertion. Invariants checked at the end: every acquire was paired (the
// limiters never went negative or over cap), and the weighted counters
// stayed within the smooth algorithm's bounded band.
func TestSchedulerChurnUnderConcurrency(t *testing.T) {
	st, eligible := weightedMembers(t, []int{3, 2, 1}, 1, time.Second, nil)
	for i := range st.members {
		st.members[i].lim.max = 2
	}
	stop := make(chan struct{})
	var wg sync.WaitGroup
	// The flapper: open and close cooldowns while selections run.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			i := int(time.Now().UnixNano() % 3)
			st.members[i].health.strike()
			st.members[i].health.success()
			// success resets; a cooldown is only observable if a strike
			// lands between, which is the point: whatever interleaving
			// occurs, selection must stay consistent.
		}
	}()
	var selWG sync.WaitGroup
	for g := 0; g < 16; g++ {
		selWG.Add(1)
		go func() {
			defer selWG.Done()
			for i := 0; i < 200; i++ {
				idx := st.selectInitial(eligible)
				if idx >= 0 {
					st.members[idx].lim.release()
				}
			}
		}()
	}
	selWG.Wait()
	close(stop)
	wg.Wait()

	for i := range st.members {
		if st.members[i].lim.cur != 0 {
			t.Errorf("member %d leaked %d permits", i, st.members[i].lim.cur)
		}
	}
	const totalWeight = 6
	for i, cw := range st.cw {
		if cw < -totalWeight*3 || cw > totalWeight*3 {
			t.Errorf("cw[%d] = %d outside the bounded band", i, cw)
		}
	}
}

// TestPoolAttemptFailureEvidence pins the per-attempt evidence the access
// log consumes: one record per dialed-and-failed endpoint, in dial order,
// carrying the endpoint's kind/target and the typed class — never the error
// text — and nothing for skipped members.
func TestPoolAttemptFailureEvidence(t *testing.T) {
	proxyURL, err := url.Parse("socks5h://user:secret@proxy.example:1080")
	if err != nil {
		t.Fatal(err)
	}
	proxyDown := &stubEndpoint{script: []stubResult{{err: &ProxyConnectError{msg: "socks5: dial proxy", cause: errors.New("refused")}}}}
	authDown := &stubEndpoint{script: []stubResult{{err: &ProxyAuthError{msg: "socks5: proxy authentication failed"}}}}
	live := &stubEndpoint{script: []stubResult{okResult("{}")}}
	members := poolMembers(Config{Kind: Proxy, ProxyURL: proxyURL}, Config{Kind: Proxy, ProxyURL: proxyURL}, Config{})
	pd, _ := newTestPool(members, RoundRobin,
		FallbackPolicy{Enabled: true, MaxAttempts: 3}, HealthPolicy{Enabled: false}, nil, proxyDown, authDown, live)

	resp, info, err := pd.Execute(execReq(false, "{}"))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	_ = resp.Body.Close()
	if len(info.Failures) != 2 {
		t.Fatalf("failures = %+v, want two records", info.Failures)
	}
	if info.Failures[0].Kind != "socks5h" || info.Failures[0].Class != "proxy_connect" {
		t.Errorf("failure[0] = %+v, want socks5h/proxy_connect", info.Failures[0])
	}
	if info.Failures[1].Class != "proxy_auth" {
		t.Errorf("failure[1] = %+v, want proxy_auth", info.Failures[1])
	}
	for i, f := range info.Failures {
		if strings.Contains(f.Target, "secret") || strings.Contains(f.Target, "user") {
			t.Errorf("failure[%d] target carries userinfo: %q", i, f.Target)
		}
	}
	// The final serving endpoint is not a failure.
	if info.Attempts != 3 || info.Target != "direct" {
		t.Errorf("info = %+v, want 3 attempts ending direct", info)
	}
}

// TestPoolFallbackAttemptContextCanceledSparesEvidence pins that a canceled
// dial produces no failure record: the caller going away is nobody's
// endpoint failure.
func TestPoolFallbackAttemptContextCanceledSparesEvidence(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	slow := &stubEndpoint{script: []stubResult{{err: context.Canceled}}}
	members := poolMembers(Config{}, Config{})
	pd, _ := newTestPool(members, RoundRobin,
		FallbackPolicy{Enabled: true, MaxAttempts: 2}, HealthPolicy{Enabled: false}, nil, slow, slow)
	ar := execReq(false, "{}")
	ar.Ctx = ctx
	_, info, err := pd.Execute(ar)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if len(info.Failures) != 0 {
		t.Fatalf("failures = %+v, want none for a canceled caller", info.Failures)
	}
}
