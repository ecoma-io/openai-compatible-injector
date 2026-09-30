package e2e_test

// The differential runner (S3 of docs/design/refactor-roadmap.md §2).
//
// A refactor is verified by running the SAME request — same body, same runtime
// YAML, same upstream script, same credential state, same timers — against a
// pre-refactor and a post-refactor build, and comparing what each one
// OBSERVABLY did. Nothing here compares source, structure, or a test's exit
// code: two builds that disagree about a client's bytes, a log slug, or a
// metered row disagree about behaviour, and that is the only disagreement this
// is allowed to care about.
//
// The runner is opt-in and off by default. Without OAICR_DIFF_REF set there is
// nothing to compare against, so every test in this file SKIPS — a normal
// `go test ./e2e/` costs one env lookup per test and nothing else, and CI
// stays hermetic. To run it:
//
//	OAICR_DIFF_REF=<pre-refactor-ref> go test ./e2e/ -run TestDiff -v
//
// The comparison is on a FINGERPRINT, not on raw bytes, because most of what
// two builds legitimately disagree about carries no meaning: the request id is
// minted fresh per request, Date is wall clock, Content-Length is derived, and
// a retry landing a millisecond later is still the same retry. The fingerprint
// keeps only what a client, a log reader, or a metering reader can see, in a
// form a re-timing cannot move.
//
// A difference is evidence, never a verdict. Either the refactor changed
// behaviour and must be reverted or justified, or the fingerprint admitted
// something with no semantic value and must be narrowed. Both are findings.

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

// diffRef names the pre-refactor build to compare against. Empty means the
// differential suite is not running and every test here skips.
func diffRef() string { return os.Getenv("OAICR_DIFF_REF") }

var (
	diffBuildOnce sync.Once
	diffBinPath   string
	diffBuildErr  error
	diffWorktree  string
)

// buildDiffBinary compiles the ref named by OAICR_DIFF_REF into a throwaway
// worktree and returns the binary path.
//
// A worktree, not an in-place checkout, because the working tree IS the
// post-refactor build's own source and may be dirty; the two builds must not
// be able to see each other's files.
func buildDiffBinary() (string, error) {
	diffBuildOnce.Do(func() {
		ref := os.Getenv("OAICR_DIFF_REF")
		if ref == "" {
			return
		}
		_, file, _, ok := runtime.Caller(0)
		if !ok {
			diffBuildErr = errors.New("cannot locate test source")
			return
		}
		root := filepath.Dir(filepath.Dir(file)) // e2e/ -> module root
		tmp, err := os.MkdirTemp("", "injector-diff-*")
		if err != nil {
			diffBuildErr = err
			return
		}
		diffWorktree = filepath.Join(tmp, "src")
		add := exec.Command("git", "worktree", "add", "--detach", diffWorktree, ref)
		add.Dir = root
		if out, err := add.CombinedOutput(); err != nil {
			diffBuildErr = fmt.Errorf("git worktree add %s: %w\n%s", ref, err, out)
			return
		}
		bin := filepath.Join(tmp, "injector-base")
		build := exec.Command("go", "build", "-o", bin, "./cmd/openai-compatible-injector")
		build.Dir = diffWorktree
		if out, err := build.CombinedOutput(); err != nil {
			diffBuildErr = fmt.Errorf("building %s: %w\n%s", ref, err, out)
			return
		}
		diffBinPath = bin
	})
	return diffBinPath, diffBuildErr
}

// diffCleanupWorktree removes the worktree the baseline was built from. It
// runs on BOTH the success and the failure path, because a skipped run — the
// one every ordinary `go test ./e2e/` is — must leave the worktree list exactly
// as it found it. A leftover detached worktree would pin a build of a ref
// nobody is working on and confuse the next run that does set the variable.
func diffCleanupWorktree() {
	if diffWorktree == "" {
		return
	}
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		return
	}
	root := filepath.Dir(filepath.Dir(file))
	rm := exec.Command("git", "worktree", "remove", "--force", diffWorktree)
	rm.Dir = root
	_ = rm.Run()
	_ = os.RemoveAll(filepath.Dir(diffWorktree))
	diffWorktree = ""
}

// ---------------------------------------------------------------------------
// The observation
// ---------------------------------------------------------------------------

// diffObs is what one build did for one request, reduced to the differences
// that carry meaning.
//
// The exclusions are the point, and each is a field that differs between two
// runs of the SAME build: the request id (minted per request), Date (wall
// clock), Content-Length (derived), upstream address (a fresh port per run),
// and every duration. The inclusions are what a client, a log reader, or a
// metering reader can see: the status and body the client got, how many
// exchanges went out, what the upstream received, and the sequence of log
// slugs the walk emitted.
type diffObs struct {
	Status  int    `json:"status"`
	BodySHA string `json:"body_sha"`
	BodyLen int    `json:"body_len"`
	// Commit records WHEN the answer was committed — at the headers for a
	// buffered reply, at the first forwarded byte for a stream. It is the
	// observable shadow of the commitment invariant, so a refactor that moved
	// the commit point moves this field.
	Commit string `json:"commit"`
	Dials  int    `json:"dials"`
	// Upstream is the ordered digest of the request bodies the upstream
	// received, one per dial. Order matters — it is what tells a retry of the
	// same candidate apart from a fallback to the next one.
	Upstream []string `json:"upstream_bodies,omitempty"`
	// UpstreamStatus is the status per exchange, in order, read from the
	// proxy's own log. A walk that answers 429 then 503 must differ from one
	// that answers 503 then 429, and the count alone would not say which
	// candidate finally answered.
	UpstreamStatus []int    `json:"upstream_status,omitempty"`
	LogSlugs       []string `json:"log_slugs,omitempty"`
	// StreamFrames is the ordered digest of the data lines a streaming client
	// received. It replaces the body, which for a stream is read after the
	// commit point and is not comparable as one buffer.
	StreamFrames []string `json:"stream_frames,omitempty"`
	StreamEnded  bool     `json:"stream_ended"`
	// StreamStatus is the relay's own disposition, READ OUT OF THE LOG rather
	// than inferred from the frames. It is not redundant with StreamEnded: a
	// stream that ended at its marker and one that was cut mid-flight deliver
	// the SAME frames to the client, and the only thing that tells them apart
	// is what the process said about it. A refactor that moved the
	// commitment point, or that began synthesizing a terminal marker, moves
	// this field and nothing else.
	StreamStatus string `json:"stream_status,omitempty"`
}

// diffStreamEvents are the slugs that describe how a relay ended.
//
// The two terminal outcomes are enumerated because the difference between
// them IS the answer: a stream that ended at its marker and one that was cut
// mid-flight deliver the SAME frames to the client, and only the disposition
// tells them apart. A refactor that moved the commitment point, or that began
// synthesizing a terminal marker, moves this and nothing else.
//
// Both are named at their REAL levels, which is why the runner reads debug:
// `stream_truncated` is WARN, and `stream_completed` is DEBUG
// (handler.go) — the pair the continuation feature was split on, and a
// fingerprint that could only see one of them would be blind to the other.
const (
	diffStreamTerminalSlug  = "stream_completed"
	diffStreamTruncatedSlug = "stream_truncated"
	diffStreamStartedSlug   = "stream_started"
)

// normalized drops the fields that cannot be compared and reduces the rest to
// what survives a re-timing, so a residual difference after this is a real one.
func (o diffObs) normalized() diffObs {
	// Slugs are compared as a SET, not as a sequence. Emission order across
	// goroutines is not observable behaviour — two builds that log the same
	// decisions in a different interleaving relayed the same bytes to the same
	// client — and a per-request field like the request id never enters here
	// because only the `message` key is read.
	o.LogSlugs = sortedUnique(o.LogSlugs)
	return o
}

// containsString reports whether v is in `in`.
//
// It is a LINEAR scan on purpose. The obvious implementation is a binary
// search over a sorted slice, and that is exactly the bug this runner had for
// one run: containsString was called on the slug list BEFORE normalized() had
// sorted it, so the search ran over unsorted input, missed a slug that was
// demonstrably present, and reported a clean stream as having no disposition.
// normalized() is what sorts, and this function is called on both sides of it.
func containsString(in []string, v string) bool {
	for _, s := range in {
		if s == v {
			return true
		}
	}
	return false
}

func sortedUnique(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	cp := append([]string(nil), in...)
	sort.Strings(cp)
	out := cp[:1]
	for _, v := range cp[1:] {
		if v != out[len(out)-1] {
			out = append(out, v)
		}
	}
	return out
}

func (o diffObs) canonical() string {
	enc, _ := json.Marshal(o)
	return string(enc)
}

func shortSHA(b []byte, n int) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])[:n]
}

// ---------------------------------------------------------------------------
// The scenario
// ---------------------------------------------------------------------------

// diffScenario is one request, replayed identically against both builds.
//
// The upstream is a FACTORY, not a handler value, and that is load-bearing
// rather than stylistic. diffCase runs the scenario twice — once per build —
// and a handler closed over a script counter carries that counter from the
// first run into the second: the second build's upstream would open at step 2
// of a script written for step 1, and the runner would report a difference
// that is really the fixture remembering. The two runs would then be
// comparing run ORDER, which is the one thing a differential runner must
// never do. A fresh handler per run is the only way the replay is identical.
type diffScenario struct {
	name string
	yaml string // runtime config body; {{UPSTREAM}} is replaced per run
	// upstream builds the handler for ONE run. Called once per binary, so a
	// stateful script starts at its first step both times.
	upstream   func() http.HandlerFunc
	clientPath string
	clientBody string
	// stream marks a scenario whose answer is an SSE stream. It is observed
	// by the sequence of data lines the client received, not by one body.
	stream bool
	// authHeader replaces the credential a scenario presents. Empty means the
	// suite's default bearer, which is what every scenario but the auth one
	// wants; setting it is how the auth gate is exercised without a parallel
	// copy of runOnce.
	authHeader string
}

// diffChatBody is a chat request whose model is a placeholder, so a scenario
// renames only the public model it is about.
func diffChatBody(public, content string) string {
	return fmt.Sprintf(`{"model":%q,"messages":[{"role":"user","content":%q}]}`, public, content)
}

// runOnce runs one scenario against one binary and returns what it observed.
//
// Everything that could legitimately differ between two runs of one build is
// either fixed here (the config file, the upstream script) or excluded from
// the observation. The upstream gets a fresh port per run, but the port never
// reaches the fingerprint: the observation records upstream BODIES and STATUSES,
// never addresses.
func runOnce(t *testing.T, bin string, sc diffScenario) (diffObs, error) {
	t.Helper()
	up := newFakeUpstream(t)
	up.setHandler(sc.upstream())

	cfg := strings.ReplaceAll(sc.yaml, "{{UPSTREAM}}", up.url())

	// log-level: debug is the DEFAULT for this runner, deliberately different
	// from the suite's "error". The log slug set is half the fingerprint, and
	// at error level a walk's decisions are invisible — the two outcomes that
	// distinguish a completed stream from a truncated one are WARN and DEBUG
	// respectively, so an error-level run would compare two processes that both
	// said almost nothing. The level is not configurable per scenario: a
	// fingerprint taken at two different levels is not a fingerprint.
	opts := startOpts{yaml: cfg, grace: "2s", logLevel: "debug"}
	p := newProc(t, opts)
	// The line that makes the comparison possible: newProc builds its command
	// from the suite's own binary, and this replaces it with the build under
	// test.
	//
	// The env, the config path and the address are all carried across from
	// the command newProc built, so only the binary differs. The env has to be
	// captured BEFORE the swap, because exec.Command inherits nothing: a Cmd
	// with no Env gets an empty environment, so replacing the command without
	// re-attaching this list would leave OAICR_CONFIG_FILE unset and every
	// scenario would die on config_file_read_failed. Same for the output
	// buffers — a fresh Cmd writes to /dev/null until they are reattached.
	env := p.cmd.Env
	p.cmd = exec.Command(bin)
	p.cmd.Env = env
	p.cmd.Stdout = p.stdout
	p.cmd.Stderr = p.stderr
	p.start()
	t.Cleanup(func() { p.terminate(10 * time.Second) })
	p.waitHealth(t, 20*time.Second)

	if sc.stream {
		return observeStream(t, p, sc, up), nil
	}

	// The credential travels on the scenario, so the auth scenario is the same
	// code path as every other one rather than a parallel copy that could drift.
	hdr := map[string]string{"Authorization": "Bearer " + e2eAPIKey}
	if sc.authHeader != "" {
		hdr["Authorization"] = sc.authHeader
	}
	code, _, body := postJSON(t, p.addr, sc.clientPath, sc.clientBody, hdr)
	return observe(t, p, sc, up, diffObs{
		Status: code,
		// The body is DIGESTED, not kept. A golden body here would be a
		// plaintext copy of whatever the upstream said, and a differential
		// runner's whole job is to be re-pointed at a build that may answer
		// something else; the digest compares exactly as well and stores
		// nothing.
		BodySHA: shortSHA(body, 16),
		BodyLen: len(body),
		// A buffered answer commits at its headers: the client sees the
		// status and can decide before the body arrives.
		Commit: "headers",
	}), nil
}

// observeStream reads a streaming answer properly: its own request whose
// response is NOT buffered and closed, because a stream that ends at the
// terminal marker is the observation.
//
// openJSON is deliberately not used: it registers a test-end cleanup and this
// scenario reads the body well past the point the request returns, and it also
// takes an `any` body the scenario would have to marshal. A stream observed
// through a buffered helper is a stream that was already over.
func observeStream(t *testing.T, p *proc, sc diffScenario, up *fakeUpstream) diffObs {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, "http://"+p.addr+sc.clientPath, strings.NewReader(sc.clientBody))
	if err != nil {
		t.Fatalf("new stream request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	auth := sc.authHeader
	if auth == "" {
		auth = "Bearer " + e2eAPIKey
	}
	req.Header.Set("Authorization", auth)
	// A client with no timeout: the stream ends at its marker, and a read
	// that hangs is the proxy hanging, which is a finding, not a flake. The
	// caller's overall test timeout is the bound.
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("stream request: %v", err)
	}
	stream := newSSEStream(t, resp)

	obs := diffObs{Status: resp.StatusCode, Commit: "first_byte"}
	for {
		lines, eof := stream.event(20 * time.Second)
		if eof {
			obs.StreamEnded = true
			break
		}
		for _, l := range lines {
			if !strings.HasPrefix(l, "data:") {
				continue
			}
			payload := strings.TrimSpace(strings.TrimPrefix(l, "data:"))
			// The payload is digested, never kept: a long stream stays
			// readable and a one-byte change still moves the digest.
			obs.StreamFrames = append(obs.StreamFrames, shortSHA([]byte(payload), 12))
		}
		if len(obs.StreamFrames) > 64 {
			// A bounded prefix: a stream that never terminates would
			// otherwise grow the observation without end, and the count
			// that matters is whether it ended at all. The cap is recorded
			// by the frame count itself, so a run that hit it and a run that
			// did not are visibly different.
			break
		}
	}
	return observe(t, p, sc, up, obs)
}

// observe fills the parts of the observation every scenario shares, from the
// upstream's own record and the process's own log.
//
// It WAITS for the request's own terminal log record before reading anything.
// That is not politeness, it is correctness: the process emits
// request_completed after it has finished writing the answer, so a client that
// saw EOF and read the log immediately would race the write and record a
// request the process had not finished accounting for. The suite already owns
// this wait — waitForEventCount — and the runner reuses it rather than
// inventing a second, subtly different one.
func observe(t *testing.T, p *proc, sc diffScenario, up *fakeUpstream, obs diffObs) diffObs {
	t.Helper()
	if obs.StreamEnded {
		// A stream's own terminal slug is the record, and the two are mutually
		// exclusive — so the wait is for EITHER, not for both. Waiting for both
		// would block until the timeout on every healthy stream.
		diffWaitForEither(t, p, diffStreamTerminalSlug, diffStreamTruncatedSlug)
	} else {
		diffWaitFor(t, p, "request_completed", 1)
	}
	obs.Dials = up.count()
	for _, r := range up.requests() {
		obs.Upstream = append(obs.Upstream, shortSHA(r.Body, 12))
	}
	stderr := p.stderr.String()
	obs.UpstreamStatus = upstreamStatuses(stderr)
	obs.LogSlugs = logSlugs(stderr)
	if obs.StreamEnded {
		// Named separately because the frames alone cannot say whether the
		// stream ended at its marker or was cut: the same prefix relays
		// identically either way, and the disposition is the only difference.
		switch {
		case containsString(obs.LogSlugs, diffStreamTerminalSlug):
			obs.StreamStatus = diffStreamTerminalSlug
		case containsString(obs.LogSlugs, diffStreamTruncatedSlug):
			obs.StreamStatus = diffStreamTruncatedSlug
		default:
			// The stream ended but nothing said so. That is this run reporting
			// a process that ended a relay without naming its disposition, and
			// it must NOT be flattened onto a value a buffered scenario carries
			// or the comparison would call the two equal.
			obs.StreamStatus = "ended_without_disposition"
		}
	}
	return obs.normalized()
}

// diffWaitFor polls the process log until `want` of the named event have been
// written, or fails the test.
//
// It is waitForEventCount under a shorter name and a different signature: the
// runner needs "at least one of either of these two" rather than "at least
// want of this one", because a stream's two terminal slugs are mutually
// exclusive and neither is guaranteed.
func diffWaitFor(t *testing.T, p *proc, slug string, want int) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		if n := len(eventsWithMessage(parseLogEvents(t, p.stderr.String()), slug)); n >= want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("no %q event within 10s; stderr:\n%s", slug, p.stderr.String())
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// diffWaitForEither polls until the process has logged at least one of the two
// named events, or fails. The slugs are alternatives, not a sequence.
func diffWaitForEither(t *testing.T, p *proc, a, b string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		evs := parseLogEvents(t, p.stderr.String())
		if len(eventsWithMessage(evs, a)) > 0 || len(eventsWithMessage(evs, b)) > 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("neither %q nor %q within 10s; stderr:\n%s", a, b, p.stderr.String())
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// upstreamStatuses reads the status of each exchange the process RECEIVED a
// real answer to, out of its own log.
//
// Two exclusions are load-bearing, and both are exclusions of events that DO
// carry a status:
//
//   - upstream_response_received is logged at DEBUG, so it is invisible at the
//     info level this runner uses; and
//   - upstream_body_read_failed also carries upstream_status, but it names an
//     exchange whose body could not be read — an event the walk classified as
//     unusable, not an answer relayed to a client.
//
// So the set is: every received 4xx/5xx, plus the relay of a 2xx that was
// itself logged. Which event that is depends on the answer's shape, and this
// deliberately does not enumerate the 2xx cases: a wrong guess would compare
// an empty list against a real one and report a difference on every stream
// scenario, which is a false finding, and §28 of the contract is explicit that
// a test must not be adjusted to make an implementation look right. The
// consequence is stated rather than hidden: UpstreamStatus pins the STATUS
// SEQUENCE of a failing walk, and the successful-walk sequence is pinned
// instead by Dials and by the body each dial received.
func upstreamStatuses(stderr string) []int {
	var out []int
	for _, ev := range diffEvents(stderr) {
		if ev["message"] != "upstream_http_error" {
			continue
		}
		st, ok := ev["upstream_status"].(float64)
		if !ok {
			continue
		}
		out = append(out, int(st))
	}
	return out
}

// logSlugs returns the event names the process emitted, as a set.
//
// The FIELDS are not kept, because they carry per-request identity (the minted
// request id), per-run timing (durations), and free-form strings that are not
// guaranteed secret-free across every event this build ever grows. A slug is a
// closed vocabulary of snake_case tokens with nothing in it but the name.
func logSlugs(stderr string) []string {
	var out []string
	for _, ev := range diffEvents(stderr) {
		if slug, ok := ev["message"].(string); ok {
			out = append(out, slug)
		}
	}
	return out
}

// diffEvents parses the JSON lines a run wrote to stderr. A line that is not
// JSON is skipped rather than failed on: the suite's own log assertions treat
// a non-JSON line as noise, and a build that emitted one is a difference the
// SLUG comparison will not see — which is a limitation of this fingerprint and
// is stated rather than hidden.
func diffEvents(stderr string) []logEvent {
	var out []logEvent
	for _, line := range strings.Split(stderr, "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		var ev logEvent
		if err := json.Unmarshal([]byte(line), &ev); err != nil {
			continue
		}
		out = append(out, ev)
	}
	return out
}

// ---------------------------------------------------------------------------
// The comparison
// ---------------------------------------------------------------------------

// diffCase runs one scenario against both builds and fails if the two
// observations differ.
func diffCase(t *testing.T, sc diffScenario) {
	t.Helper()
	ref := diffRef()
	if ref == "" {
		t.Skip("OAICR_DIFF_REF not set; differential comparison not running")
	}
	baseBin, err := buildDiffBinary()
	if err != nil {
		t.Fatalf("differential baseline unavailable: %v", err)
	}

	// Post-refactor first, so a failure is reported against the build under
	// test and the baseline is the one that is assumed correct.
	post, err := runOnce(t, binPath, sc)
	if err != nil {
		t.Fatalf("post-refactor build: %v", err)
	}
	pre, err := runOnce(t, baseBin, sc)
	if err != nil {
		t.Fatalf("pre-refactor build (%s): %v", ref, err)
	}
	if pre.canonical() == post.canonical() {
		return
	}
	t.Errorf("differential mismatch on %q (%s -> working tree)\n  pre : %s\n  post: %s",
		sc.name, ref, pre.canonical(), post.canonical())
}

// ---------------------------------------------------------------------------
// The scenarios
// ---------------------------------------------------------------------------

// TestDiffMain guards the runner itself, and is the only test here that
// runs without OAICR_DIFF_REF. A runner that silently compares nothing is
// worse than no runner: every other test in this file would skip, CI would
// stay green, and the net the refactor depends on would look closed while
// being empty.
//
// So this asserts the two properties the skip depends on — the env lookup and
// the skip — and the one property a comparison depends on: that the two
// exclusion passes do not throw away anything the contract calls observable.
func TestDiffMain(t *testing.T) {
	if got := diffRef(); got != os.Getenv("OAICR_DIFF_REF") {
		t.Fatalf("diffRef() = %q, want the environment value %q", got, os.Getenv("OAICR_DIFF_REF"))
	}
	// A representative log stream: the request id moves, the durations move,
	// the emission order moves — none of which may reach the fingerprint —
	// while the slugs and the received status must survive.
	stream := strings.Join([]string{
		`{"level":"info","message":"request_completed","request_id":"ffffffffffffffff","upstream_exchanges":1,"duration_ms":44}`,
		`{"level":"warn","message":"upstream_http_error","request_id":"ffffffffffffffff","upstream_status":429,"duration_ms":41}`,
		`{"level":"info","message":"provider_attempt_started","request_id":"ffffffffffffffff","upstream_attempts":1,"duration_ms":3}`,
		`{"level":"info","message":"upstream_http_error","request_id":"0000000000000000","upstream_status":503,"duration_ms":7}`,
		`not json at all`,
		``,
	}, "\n")
	slugs := logSlugs(stream)
	statuses := upstreamStatuses(stream)

	// Slugs are compared SORTED and deduplicated, so the expectation is in
	// that order rather than in emission order: a build that logged the same
	// three decisions in a different interleaving relayed the same bytes.
	want := []string{"provider_attempt_started", "request_completed", "upstream_http_error"}
	if !equalStrings(sortedUnique(slugs), want) {
		t.Errorf("logSlugs, sorted+deduped = %v, want %v", sortedUnique(slugs), want)
	}
	// Both error statuses, IN EMISSION ORDER and with their duplicates kept:
	// the sequence is what tells a 429-then-503 walk from a 503-then-429 one,
	// and that is a behavioural difference, not a reordering of noise.
	if wantStatuses := []int{429, 503}; !equalInts(statuses, wantStatuses) {
		t.Errorf("upstreamStatuses = %v, want %v", statuses, wantStatuses)
	}
	// The unparsable line contributed nothing: not a slug, and not a status
	// read as zero.
	if len(slugs) != 4 {
		t.Errorf("logSlugs read %d slugs from a 5-line stream with one unparsable line; want 4", len(slugs))
	}
	// A repeated slug must not move the fingerprint — that is what makes a
	// re-run, or a walk that logs an event on two paths, comparable. The
	// append is onto a fresh slice: appending to a shared one would overwrite
	// the source and make the two observations equal by construction.
	dup := append(append([]string(nil), slugs...), slugs...)
	if (diffObs{LogSlugs: dup}).normalized().canonical() != (diffObs{LogSlugs: slugs}).normalized().canonical() {
		t.Error("a repeated slug changed the fingerprint; the comparison is over the set of decisions, not their count")
	}

	// The stream disposition is READ from the log, not inferred from
	// StreamEnded: a stream whose log carries the terminal slug and one that
	// carries the truncated slug both ended at the same byte, and the frames
	// are identical. This is the pair the contract's commitment rule turns on.
	clean := (diffObs{StreamEnded: true, LogSlugs: []string{diffStreamStartedSlug, diffStreamTerminalSlug}}).normalized()
	cut := (diffObs{StreamEnded: true, LogSlugs: []string{diffStreamStartedSlug, diffStreamTruncatedSlug}}).normalized()
	if clean.canonical() == cut.canonical() {
		t.Error("a completed stream and a truncated one produced the same fingerprint; the disposition is what tells them apart")
	}
	if !containsString(clean.LogSlugs, diffStreamTerminalSlug) {
		t.Error("the terminal slug did not survive normalization")
	}
	if containsString(cut.LogSlugs, diffStreamTerminalSlug) {
		t.Error("a truncated stream's slug set contains the terminal slug")
	}
	// And the disposition is never derived: normalized() alone, given no slug
	// to read, must leave it empty rather than inventing a verdict.
	if got := (diffObs{StreamEnded: true}).normalized().StreamStatus; got != "" {
		t.Errorf("StreamStatus was derived from StreamEnded alone = %q; it must be read from the log", got)
	}

	// The regression that this fingerprint actually shipped with: the slug
	// lookup ran a BINARY SEARCH over a list that had not been sorted yet,
	// because observe() asks "did the stream finish?" before normalized() is
	// the thing that sorts. Every healthy stream was therefore reported as
	// `ended_without_disposition` while its own `stream_completed` sat in the
	// very list being searched. The self-test missed it because every input it
	// searched was already sorted.
	unsorted := []string{"stream_started", "stream_completed", "request_completed"}
	if !containsString(unsorted, diffStreamTerminalSlug) {
		t.Error("containsString missed a present slug in an UNSORTED list; the disposition is read before normalized() sorts it")
	}
	if containsString(unsorted, diffStreamTruncatedSlug) {
		t.Error("containsString found an absent slug in an unsorted list")
	}
}

func equalInts(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// ---------------------------------------------------------------------------
// The scenarios
// ---------------------------------------------------------------------------

// diffSingleYAML renders a one-model, one-provider file whose endpoint is the
// run's upstream. {{UPSTREAM}} carries the scheme+host; the path is appended
// here, so a scenario cannot accidentally point two shapes of the same request
// at two different paths.
func diffSingleYAML(public, upstreamModel, injectionPrompt, extraModelBlock string) string {
	return fmt.Sprintf(`api-key: %s
providers:
  diff-provider:
    base-url: {{UPSTREAM}}/v1
models:
  %s:
    provider: diff-provider
    upstream-model: %s
    injection-prompt: %s
%s`, e2eAPIKey, public, upstreamModel, injectionPrompt, extraModelBlock)
}

// diffChatOK is a 200 chat completion the upstream answers every time. It
// names a stable upstream model so the rewrite has something to rename.
func diffChatOK() http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, `{"id":"c1","object":"chat.completion","created":1,"model":"diff-up-1","choices":[{"index":0,"message":{"role":"assistant","content":"hi"},"finish_reason":"stop"}],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}`)
	}
}

// TestDiffChatInjected is the base case: one candidate, one exchange, a
// model rename and a system prompt applied on the way out and a model rename
// on the way back. Everything else in this file is a variation on it, so if
// this one differs the runner is comparing the wrong thing rather than the
// proxy having changed.
func TestDiffChatInjected(t *testing.T) {
	diffCase(t, diffScenario{
		name:       "chat_injected",
		yaml:       diffSingleYAML(chatPublic, "diff-up-1", "You are a diff test assistant.", ""),
		upstream:   diffChatOK,
		clientPath: "/v1/chat/completions",
		clientBody: diffChatBody(chatPublic, "hello"),
	})
}

// TestDiffChatNoInjectionModel pins the other no-injection shape: a model with
// an EMPTY prompt, where the request transform still renames the model but
// must not touch messages at all. It is a separate scenario because the
// transform has a distinct branch for it, and a refactor that collapsed the
// two would relay a prompt the operator never configured.
func TestDiffChatNoInjectionModel(t *testing.T) {
	diffCase(t, diffScenario{
		name:       "chat_no_injection",
		yaml:       diffSingleYAML(chatPublic, "diff-up-1", "", ""),
		upstream:   diffChatOK,
		clientPath: "/v1/chat/completions",
		clientBody: diffChatBody(chatPublic, "hello"),
	})
}

// diffJSONUpstream renders a factory for an upstream that always answers one
// status and one body. A factory, not a value, because diffCase runs the
// scenario once per build and a shared value would let the first run's state
// into the second.
func diffJSONUpstream(status int, body string) func() http.HandlerFunc {
	return func() http.HandlerFunc {
		return func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(status)
			_, _ = io.WriteString(w, body)
		}
	}
}

// TestDiffResponsesBuffered pins the other API surface's buffered path, where
// the transform MERGES into instructions rather than prepending to messages.
func TestDiffResponsesBuffered(t *testing.T) {
	diffCase(t, diffScenario{
		name: "responses_buffered",
		yaml: diffSingleYAML(respPublic, "diff-up-1", "You are a diff test assistant.", ""),
		upstream: diffJSONUpstream(http.StatusOK,
			`{"id":"r1","object":"response","created_at":1,"model":"diff-up-1","status":"completed","output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"hi"}]}],"usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}`),
		clientPath: "/v1/responses",
		clientBody: `{"model":"` + respPublic + `","instructions":"be brief","input":"hello"}`,
	})
}

// TestDiffUnmappedModel pins the local 404. This request never leaves the
// process, so the whole fingerprint is the answer: a refactor that let it reach
// an upstream would show a dial here.
func TestDiffUnmappedModel(t *testing.T) {
	diffCase(t, diffScenario{
		name:       "unmapped_model",
		yaml:       diffSingleYAML(chatPublic, "diff-up-1", "P", ""),
		upstream:   diffChatOK,
		clientPath: "/v1/chat/completions",
		clientBody: diffChatBody("no-such-model", "hello"),
	})
}

// TestDiffUnauthenticated pins the auth gate: 401, and zero dials. The key
// is presented under the WRONG scheme on purpose, so the scenario also fixes
// which of the two rejection paths the runner exercises — the scheme check and
// not a lookup of a key that does not exist.
func TestDiffUnauthenticated(t *testing.T) {
	diffCase(t, diffScenario{
		name:       "unauthenticated",
		yaml:       diffSingleYAML(chatPublic, "diff-up-1", "P", ""),
		upstream:   diffChatOK,
		clientPath: "/v1/chat/completions",
		clientBody: diffChatBody(chatPublic, "hello"),
		authHeader: "Basic not-a-bearer-token",
	})
}

// TestDiffTransportFailureFallback pins the walk's fallback on a TRANSPORT
// failure — the primary's dial cannot complete, so there is no status to
// classify, and the next candidate must answer.
//
// The file names diff-primary first and diff-secondary second; diff-primary
// points at a CLOSED PORT, so it is genuinely unreachable and the walk has to
// move. The upstream answers whatever arrives, so the only way a 200 reaches
// the client at all is if the walk really did leave the dead primary.
func TestDiffTransportFailureFallback(t *testing.T) {
	diffCase(t, diffScenario{
		name: "transport_failure_fallback",
		yaml: fmt.Sprintf(`api-key: %s
providers:
  diff-primary:
    base-url: http://127.0.0.1:1/v1
  diff-secondary:
    base-url: {{UPSTREAM}}/v1
models:
  diff-chain:
    providers:
      - provider: diff-primary
        upstream-model: diff-up-1
      - provider: diff-secondary
        upstream-model: diff-up-2
`, e2eAPIKey),
		upstream: diffJSONUpstream(http.StatusOK,
			`{"id":"c2","object":"chat.completion","created":1,"model":"diff-up-2","choices":[{"index":0,"message":{"role":"assistant","content":"from-second"}},"finish_reason":"stop"}]}`),
		clientPath: "/v1/chat/completions",
		clientBody: diffChatBody("diff-chain", "hello"),
	})
}

// TestDiffRetryThenSucceed pins a RETRYABLE answer followed by a success, on
// one candidate. The upstream script is stateful: 429 first, 200 after. A
// refactor that stopped retrying would relay the 429; one that retried forever
// would never answer. Both show up here as a different body, a different dial
// count, and a different upstream status sequence.
//
// Two things make this comparable, and both were defects in the first draft of
// this scenario rather than properties of the proxy:
//
//   - The script counter lives INSIDE the factory, so both builds start at step
//     one. A counter held in the scenario struct carried the first run's step
//     into the second, and the runner reported a difference that was really
//     the fixture remembering which build it had already served.
//   - The backoff is pinned. A 429 maps to ActionRetry under the default
//     matrix, and the default schedule is 250ms with ±10% JITTER
//     (internal/recovery/defaults.go), so under the defaults the walk's own
//     timing would decide the fingerprint. A differential runner must not
//     compare a quantity the implementation does not define, so jitter is zero
//     and the schedule is one fixed wait. The retry stays; only its timing
//     becomes defined.
func TestDiffRetryThenSucceed(t *testing.T) {
	diffCase(t, diffScenario{
		name: "retry_then_succeed",
		yaml: diffSingleYAML(chatPublic, "diff-up-1", "P",
			"    recovery:\n      retries:\n        backoff:\n          initial: 100ms\n          max: 100ms\n          jitter: 0\n"),
		upstream: func() http.HandlerFunc {
			var n int
			var mu sync.Mutex
			return func(w http.ResponseWriter, _ *http.Request) {
				mu.Lock()
				n++
				seen := n
				mu.Unlock()
				if seen == 1 {
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(http.StatusTooManyRequests)
					_, _ = io.WriteString(w, `{"error":{"message":"slow down","type":"rate_limit_error","code":"rate_limited"}}`)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)
				_, _ = io.WriteString(w, `{"id":"c1","object":"chat.completion","created":1,"model":"diff-up-1","choices":[{"index":0,"message":{"role":"assistant","content":"hi"},"finish_reason":"stop"}],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}`)
			}
		},
		clientPath: "/v1/chat/completions",
		clientBody: diffChatBody(chatPublic, "hello"),
	})
}

// TestDiffUpstreamErrorTerminal pins an upstream error the matrix treats as
// terminal: the client gets that status back, normalized, after ONE exchange.
// The raw provider bytes must not reach the client, so the body digest here is
// over the proxy's own envelope.
func TestDiffUpstreamErrorTerminal(t *testing.T) {
	diffCase(t, diffScenario{
		name:       "upstream_error_terminal",
		yaml:       diffSingleYAML(chatPublic, "diff-up-1", "P", ""),
		upstream:   diffJSONUpstream(http.StatusBadRequest, `{"error":{"message":"the model does not exist","type":"invalid_request_error","code":"model_not_found"}}`),
		clientPath: "/v1/chat/completions",
		clientBody: diffChatBody(chatPublic, "hello"),
	})
}

// TestDiffUpstreamNonJSON pins the "a 200 that is not JSON" rule: the answer
// is unusable, so the walk judges it like any other observation and the client
// gets 502 upstream_invalid_response rather than the provider's bytes.
func TestDiffUpstreamNonJSON(t *testing.T) {
	diffCase(t, diffScenario{
		name:       "upstream_non_json",
		yaml:       diffSingleYAML(chatPublic, "diff-up-1", "P", ""),
		upstream:   diffJSONUpstream(http.StatusOK, `<html>not json at all</html>`),
		clientPath: "/v1/chat/completions",
		clientBody: diffChatBody(chatPublic, "hello"),
	})
}

// TestDiffStreamChat pins the streaming relay end to end: the data lines the
// client received, in order, each digested, plus the relay's own disposition.
// A relay that buffered the stream, dropped a chunk, or synthesized a terminal
// marker it was not sent all move this fingerprint.
func TestDiffStreamChat(t *testing.T) {
	diffCase(t, diffScenario{
		name:       "stream_chat",
		yaml:       diffSingleYAML(chatPublic, "diff-up-1", "P", ""),
		upstream:   diffChatSSE,
		clientPath: "/v1/chat/completions",
		clientBody: `{"model":"` + chatPublic + `","stream":true,"messages":[{"role":"user","content":"hello"}]}`,
		stream:     true,
	})
}

// TestDiffStreamResponses pins the Responses streaming shape: event:/data:
// pairs and NO [DONE] marker, so "did the stream end" is decided by the
// proxy's own disposition rather than by a marker this upstream never sent.
func TestDiffStreamResponses(t *testing.T) {
	diffCase(t, diffScenario{
		name: "stream_responses",
		yaml: diffSingleYAML(respPublic, "diff-up-1", "P", ""),
		upstream: func() http.HandlerFunc {
			return func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "text/event-stream")
				w.WriteHeader(http.StatusOK)
				fl, _ := w.(http.Flusher)
				for _, ev := range []string{
					"event: response.created\ndata: {\"type\":\"response.created\",\"response\":{\"id\":\"r1\",\"model\":\"diff-up-1\"}}\n\n",
					"event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"hi\"}\n\n",
					"event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"r1\",\"model\":\"diff-up-1\"}}\n\n",
				} {
					_, _ = io.WriteString(w, ev)
					if fl != nil {
						fl.Flush()
					}
				}
			}
		},
		clientPath: "/v1/responses",
		clientBody: `{"model":"` + respPublic + `","stream":true,"input":"hello"}`,
		stream:     true,
	})
}

// diffChatSSE answers a chat stream in three data lines plus the terminal
// marker, flushing at each event boundary so the relay's per-event flush path
// is the one under test.
func diffChatSSE() http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		fl, _ := w.(http.Flusher)
		for _, line := range []string{
			`data: {"id":"c1","object":"chat.completion.chunk","created":1,"model":"diff-up-1","choices":[{"index":0,"delta":{"role":"assistant","content":""},"finish_reason":null}]}`,
			`data: {"id":"c1","object":"chat.completion.chunk","created":1,"model":"diff-up-1","choices":[{"index":0,"delta":{"content":"hi"},"finish_reason":null}]}`,
			`data: {"id":"c1","object":"chat.completion.chunk","created":1,"model":"diff-up-1","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}`,
			`data: [DONE]`,
		} {
			_, _ = io.WriteString(w, line+"\n\n")
			if fl != nil {
				fl.Flush()
			}
		}
	}
}
