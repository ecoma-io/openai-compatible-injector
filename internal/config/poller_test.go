package config

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

func testLog(t *testing.T) zerolog.Logger {
	t.Helper()
	return zerolog.New(zerolog.TestWriter{T: t}).Level(zerolog.Disabled)
}

// syncBuffer is a mutex-guarded bytes.Buffer: the poller logs from its own
// goroutine while the test reads, and -race runs in CI.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// countEvents counts log lines whose message equals msg.
func (b *syncBuffer) countEvents(msg string) int {
	return strings.Count(b.String(), `"message":"`+msg+`"`)
}

// waitUntil polls cond until it holds, failing the test after a timeout.
func waitUntil(t *testing.T, timeout time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal(msg)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// writeFile is a helper that fails the test on write errors.
func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

// waitGen blocks until store.Gen() reaches want, or fails after a timeout.
func waitGen(t *testing.T, store *Store, want uint64) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for store.Gen() != want {
		if time.Now().After(deadline) {
			t.Fatalf("store.Gen() = %d, want %d", store.Gen(), want)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// waitGenStable asserts that store.Gen() stays at want for a hold period —
// used to prove an unchanged file does not republish.
func waitGenStable(t *testing.T, store *Store, want uint64, hold time.Duration) {
	t.Helper()
	deadline := time.Now().Add(hold)
	for time.Now().Before(deadline) {
		if store.Gen() != want {
			t.Fatalf("generation changed to %d while holding, want %d", store.Gen(), want)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestPollerReloadAndLastKnownGood(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	writeFile(t, path, validRuntime())

	store := NewStore(mustSnapshot(t, validRuntime()))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	p := NewPoller(store, path, []byte(validRuntime()), 20*time.Millisecond, testLog(t), nil)
	go p.Run(ctx)

	// Let the poller record its initial hash before changing the file. A
	// change written before that first read would become the "initial"
	// content, the poller would never see a difference, and generation would
	// stay 0 forever. Three intervals is enough margin that the entry read
	// (and a tick) have certainly happened.
	time.Sleep(3 * 20 * time.Millisecond)

	// Valid content change -> republished with a new generation.
	modified := `
models:
  gpt-reviewer:
    endpoint: https://api.provider.example/v2
    upstream-model: gpt-6
    injection-prompt: changed
`
	writeFile(t, path, modified)
	waitGen(t, store, 1)
	if m, _ := store.Load().Model("gpt-reviewer"); m.UpstreamModel != "gpt-6" {
		t.Errorf("reload not visible: %+v", m)
	}

	// Invalid content -> logged, last-known-good kept, generation unchanged.
	writeFile(t, path, "models:\n  x:\n    endpoint: nope\n    upstream-model: m\n")
	waitGenStable(t, store, 1, 150*time.Millisecond)
	if m, _ := store.Load().Model("gpt-reviewer"); m.UpstreamModel != "gpt-6" {
		t.Errorf("invalid reload clobbered last-known-good: %+v", m)
	}

	// Recovery after invalid content. This must differ from the last-good
	// `modified` bytes: back-to-identical content is "unchanged file" by the
	// hash design and must not republish.
	recovered := strings.Replace(modified, "upstream-model: gpt-6", "upstream-model: gpt-7", 1)
	writeFile(t, path, recovered)
	waitGen(t, store, 2)
}

func TestPollerUnchangedFileDoesNotRepublish(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	writeFile(t, path, validRuntime())

	store := NewStore(mustSnapshot(t, validRuntime()))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	p := NewPoller(store, path, []byte(validRuntime()), 15*time.Millisecond, testLog(t), nil)
	go p.Run(ctx)

	// Initial Run() must not republish: generation stays at its snapshot seed.
	waitGenStable(t, store, store.Gen(), 150*time.Millisecond)
}

func TestPollerMissingFileKeepsLastKnownGood(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	writeFile(t, path, validRuntime())

	store := NewStore(mustSnapshot(t, validRuntime()))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	p := NewPoller(store, path, []byte(validRuntime()), 15*time.Millisecond, testLog(t), nil)
	go p.Run(ctx)

	_ = os.Remove(path)
	waitGenStable(t, store, store.Gen(), 150*time.Millisecond)
	if _, ok := store.Load().Model("gpt-reviewer"); !ok {
		t.Fatal("last-known-good lost after file removal")
	}
}

func TestPollerStopsOnCancel(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	writeFile(t, path, validRuntime())

	store := NewStore(mustSnapshot(t, validRuntime()))
	ctx, cancel := context.WithCancel(context.Background())
	p := NewPoller(store, path, []byte(validRuntime()), 10*time.Millisecond, testLog(t), nil)
	done := make(chan struct{})
	go func() {
		p.Run(ctx)
		close(done)
	}()

	cancel()
	select {
	case <-done:
		// clean stop
	case <-time.After(2 * time.Second):
		t.Fatal("poller did not stop after context cancellation")
	}
}

func TestStoreConcurrentPublish(t *testing.T) {
	// Many concurrent publishes: every published generation must be unique
	// and monotonically increasing (the store's own counter, not a caller
	// value), and the active snapshot must always be a published one.
	store := NewStore(mustSnapshot(t, validRuntime()))
	const n = 64
	done := make(chan struct{}, n)
	for i := 0; i < n; i++ {
		go func() {
			defer func() { done <- struct{}{} }()
			s := mustSnapshot(t, validRuntime())
			_ = store.Publish(s)
		}()
	}
	for i := 0; i < n; i++ {
		<-done
	}
	if got := store.Gen(); got != n {
		t.Errorf("final Gen = %d, want %d", got, n)
	}
	// Retained snapshot still reports its own generation.
	active := store.Load()
	if active.Gen() != n {
		t.Errorf("active Gen = %d, want %d", active.Gen(), n)
	}
}

func TestPollerOnPublishCallback(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	writeFile(t, path, validRuntime())

	store := NewStore(mustSnapshot(t, validRuntime()))
	published := make(chan *Snapshot, 4)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	p := NewPoller(store, path, []byte(validRuntime()), 15*time.Millisecond, testLog(t), func(s *Snapshot) { published <- s })
	go p.Run(ctx)

	time.Sleep(3 * 15 * time.Millisecond)

	changed := strings.Replace(validRuntime(), "gpt-5-pro", "gpt-6", 1)
	writeFile(t, path, changed)
	waitGen(t, store, 1)

	select {
	case s := <-published:
		if s.Gen() != 1 {
			t.Errorf("callback snapshot Gen = %d, want 1", s.Gen())
		}
		if m, _ := s.Model("gpt-reviewer"); m.UpstreamModel != "gpt-6" {
			t.Errorf("callback snapshot stale: %+v", m)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("onPublish not called after reload")
	}

	// Unchanged ticks must not re-invoke the hook: exactly one callback for
	// exactly one reload.
	waitGenStable(t, store, 1, 120*time.Millisecond)
	select {
	case s := <-published:
		t.Fatalf("onPublish fired without a reload: gen %d", s.Gen())
	case <-time.After(100 * time.Millisecond):
	}
}

// TestPollerFailureLoggingTransitions pins the transition-based failure
// logging: a broken file warns once, not once per tick, and recovery after
// successful reload is announced with the new generation's facts. Without
// this pin, reverting to per-tick error logging would pass every other test
// while emitting one identical line per interval in production.
func TestPollerFailureLoggingTransitions(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	writeFile(t, path, validRuntime())

	store := NewStore(mustSnapshot(t, validRuntime()))
	var buf syncBuffer
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	p := NewPoller(store, path, []byte(validRuntime()), 15*time.Millisecond, zerolog.New(&buf).Level(zerolog.InfoLevel), nil)
	go p.Run(ctx)

	time.Sleep(3 * 15 * time.Millisecond)

	// Two distinct invalid files in a row: one WARN for the transition into
	// failure, the second rejection downgraded to debug (suppressed here).
	writeFile(t, path, "models:\n  x:\n    endpoint: nope\n    upstream-model: m\n")
	waitUntil(t, 2*time.Second, func() bool { return buf.countEvents("config_reload_rejected") >= 1 },
		"no config_reload_rejected WARN after invalid file")
	writeFile(t, path, "models: [unclosed\n")
	time.Sleep(6 * 15 * time.Millisecond)
	if got := buf.countEvents("config_reload_rejected"); got != 1 {
		t.Errorf("config_reload_rejected logged %d times, want exactly 1 (transition only)", got)
	}

	// Recovery: new valid content reloads and logs the reload facts.
	recovered := strings.Replace(validRuntime(), "gpt-5-pro", "gpt-7", 1)
	writeFile(t, path, recovered)
	waitGen(t, store, 1)
	waitUntil(t, 2*time.Second, func() bool { return buf.countEvents("config_reloaded") >= 1 },
		"no config_reloaded INFO after recovery")
	line := buf.String()
	for _, want := range []string{`"generation":1`, `"model_count":1`, `"log_level":"info"`} {
		if !strings.Contains(line, want) {
			t.Errorf("config_reloaded line missing %s: %s", want, line)
		}
	}
}

// TestPollerUnreadableTransitionLogging pins the same transition rule for
// the read-failure branch, plus recovery when the file returns byte-identical.
func TestPollerUnreadableTransitionLogging(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	writeFile(t, path, validRuntime())

	store := NewStore(mustSnapshot(t, validRuntime()))
	var buf syncBuffer
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	p := NewPoller(store, path, []byte(validRuntime()), 15*time.Millisecond, zerolog.New(&buf).Level(zerolog.InfoLevel), nil)
	go p.Run(ctx)

	time.Sleep(3 * 15 * time.Millisecond)

	if err := os.Remove(path); err != nil {
		t.Fatalf("remove: %v", err)
	}
	waitUntil(t, 2*time.Second, func() bool { return buf.countEvents("config_file_unreadable") >= 1 },
		"no config_file_unreadable WARN after file removal")
	time.Sleep(6 * 15 * time.Millisecond)
	if got := buf.countEvents("config_file_unreadable"); got != 1 {
		t.Errorf("config_file_unreadable logged %d times, want exactly 1 (transition only)", got)
	}

	// Restore the byte-identical file: recovery is announced, nothing is
	// republished (the hash matches the last-known-good content).
	writeFile(t, path, validRuntime())
	waitUntil(t, 2*time.Second, func() bool { return buf.countEvents("config_file_recovered") >= 1 },
		"no config_file_recovered INFO after restoring the file")
	waitGenStable(t, store, store.Gen(), 120*time.Millisecond)
}

// TestPollerDebugHeartbeat pins the debug-level persistence contract: with
// the level raised to debug, a failure that persists across ticks emits its
// documented debug event, so an operator debugging a stuck reload can see
// every failing poll outcome without recompiling.
func TestPollerDebugHeartbeat(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	writeFile(t, path, validRuntime())

	store := NewStore(mustSnapshot(t, validRuntime()))
	var buf syncBuffer
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	p := NewPoller(store, path, []byte(validRuntime()), 15*time.Millisecond, zerolog.New(&buf).Level(zerolog.DebugLevel), nil)
	go p.Run(ctx)

	time.Sleep(3 * 15 * time.Millisecond)
	writeFile(t, path, "models:\n  x:\n    endpoint: nope\n    upstream-model: m\n")
	waitUntil(t, 2*time.Second, func() bool { return buf.countEvents("config_reload_rejected") >= 1 },
		"no config_reload_rejected after invalid file")
	writeFile(t, path, "models: [unclosed\n")
	waitUntil(t, 2*time.Second, func() bool { return buf.countEvents("config_reload_still_rejected") >= 1 },
		"no config_reload_still_rejected DEBUG while the failure persists")
}

// TestPollerHealthyTicksAreSilent pins the healthy-state logging contract:
// an unchanged file produces no event per tick — not even at debug level —
// so a process serving one static config emits no perpetual heartbeat
// (86,400 lines a day at the default interval) on top of an unchanged
// generation. The reload path keeps its own events; only the quiet state is
// quiet.
func TestPollerHealthyTicksAreSilent(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	writeFile(t, path, validRuntime())

	store := NewStore(mustSnapshot(t, validRuntime()))
	var buf syncBuffer
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	p := NewPoller(store, path, []byte(validRuntime()), 15*time.Millisecond, zerolog.New(&buf).Level(zerolog.DebugLevel), nil)
	go p.Run(ctx)

	// Enough unchanged ticks that the old per-tick DEBUG event would have
	// fired many times over.
	waitGenStable(t, store, store.Gen(), 300*time.Millisecond)
	if got := buf.countEvents("config_unchanged"); got != 0 {
		t.Errorf("config_unchanged logged %d times on healthy ticks, want 0 (no perpetual heartbeat)", got)
	}
	if lines := buf.lines(); lines != 0 {
		t.Errorf("healthy ticks emitted %d log lines, want 0:\n%s", lines, buf.String())
	}

	// The silence is scoped to the unchanged state: a change still logs.
	changed := strings.Replace(validRuntime(), "gpt-5-pro", "gpt-9", 1)
	writeFile(t, path, changed)
	waitGen(t, store, 1)
	waitUntil(t, 2*time.Second, func() bool { return buf.countEvents("config_reloaded") >= 1 },
		"no config_reloaded INFO for the changed file")
}

// lines counts captured log lines (every line is one JSON event).
func (b *syncBuffer) lines() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	n := 0
	for _, line := range strings.Split(b.buf.String(), "\n") {
		if strings.TrimSpace(line) != "" {
			n++
		}
	}
	return n
}

// TestPollerSeedsHashFromBootContent pins the boot-content seed: Run must
// compare against the exact bytes that were loaded at startup, not re-read
// the file when it starts. A file rewritten in the window between the boot
// LoadRuntime and Run is still a change the poller must observe — seeded by a
// fresh read, that window's write would be mistaken for the initial content
// and swallowed forever.
func TestPollerSeedsHashFromBootContent(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	writeFile(t, path, validRuntime())

	store := NewStore(mustSnapshot(t, validRuntime()))

	// The file changes after boot, before Run.
	changed := strings.Replace(validRuntime(), "gpt-5-pro", "gpt-8", 1)
	writeFile(t, path, changed)

	var buf syncBuffer
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p := NewPoller(store, path, []byte(validRuntime()), 15*time.Millisecond, zerolog.New(&buf).Level(zerolog.InfoLevel), nil)
	go p.Run(ctx)

	waitGen(t, store, 1)
	if m, _ := store.Load().Model("gpt-reviewer"); m.UpstreamModel != "gpt-8" {
		t.Errorf("post-boot change swallowed: %+v", m)
	}
	if got := buf.countEvents("config_initial_read_failed"); got != 0 {
		t.Errorf("config_initial_read_failed logged %d times — the seed must come from boot content, not a fresh read", got)
	}
}

// TestPollerBootReadFailureDoesNotSpuriouslyRepublish: with the seed taken
// from boot content, a file that is unreadable when Run starts and returns
// byte-identical afterward is unchanged — no reload, no generation bump, no
// config_initial_read_failed WARN (there is no initial read to fail).
func TestPollerBootReadFailureDoesNotSpuriouslyRepublish(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	writeFile(t, path, validRuntime())

	store := NewStore(mustSnapshot(t, validRuntime()))

	if err := os.Remove(path); err != nil {
		t.Fatalf("remove: %v", err)
	}
	var buf syncBuffer
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p := NewPoller(store, path, []byte(validRuntime()), 15*time.Millisecond, zerolog.New(&buf).Level(zerolog.InfoLevel), nil)
	go p.Run(ctx)

	time.Sleep(3 * 15 * time.Millisecond)
	writeFile(t, path, validRuntime())
	waitGenStable(t, store, 0, 200*time.Millisecond)
	if got := buf.countEvents("config_initial_read_failed"); got != 0 {
		t.Errorf("config_initial_read_failed logged %d times — there must be no initial read", got)
	}
}

// TestPollerRapidEditsConvergeToFinalContent pins the rapid-edit contract: a
// content-hash poller observes file STATE at each tick, not write events, so
// edits landing between two reads are collapsed. Whatever the interleaving,
// once the writes have settled the poller must converge to the final content
// and never wedge on an intermediate invalid file it may never see.
func TestPollerRapidEditsConvergeToFinalContent(t *testing.T) {
	valid := func(model string) string {
		return `
models:
  gpt-reviewer:
    endpoint: https://api.provider.example/v2
    upstream-model: ` + model + `
    injection-prompt: p
`
	}
	t.Run("two valid edits collapse to the last", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "config.yaml")
		writeFile(t, path, valid("first"))

		store := NewStore(mustSnapshot(t, valid("first")))
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		p := NewPoller(store, path, []byte(valid("first")), 50*time.Millisecond, testLog(t), nil)
		go p.Run(ctx)

		time.Sleep(3 * 50 * time.Millisecond)
		// Both writes land well inside one interval: the middle state
		// ("second") may or may not ever be read.
		writeFile(t, path, valid("second"))
		writeFile(t, path, valid("third"))

		waitUntil(t, 2*time.Second, func() bool {
			m, _ := store.Load().Model("gpt-reviewer")
			return m.UpstreamModel == "third"
		}, "poller did not converge to the final content")
		// Convergence is monotone: the generation only ever moved forward,
		// and no further reload happens after the content has settled.
		gen := store.Gen()
		waitGenStable(t, store, gen, 200*time.Millisecond)
	})

	t.Run("invalid intermediate never wedges the poller", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "config.yaml")
		writeFile(t, path, valid("first"))

		store := NewStore(mustSnapshot(t, valid("first")))
		var buf syncBuffer
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		p := NewPoller(store, path, []byte(valid("first")), 50*time.Millisecond, zerolog.New(&buf).Level(zerolog.InfoLevel), nil)
		go p.Run(ctx)

		time.Sleep(3 * 50 * time.Millisecond)
		// valid -> invalid -> valid: the broken state exists only inside one
		// interval, so the poller may legitimately never observe it. Whether
		// it does or not, the final valid content must win.
		writeFile(t, path, "models: [unclosed\n")
		writeFile(t, path, valid("final"))

		waitUntil(t, 2*time.Second, func() bool {
			m, _ := store.Load().Model("gpt-reviewer")
			return m.UpstreamModel == "final"
		}, "poller did not converge past a transient invalid file")
	})
}

// TestPollerFailureKindSwitchWarns pins the failure-kind tracking: a file
// that fails as invalid content and then fails as unreadable has produced a
// NEW failure condition, which must warn again — a single failing bit would
// downgrade the second kind to a debug heartbeat and leave the operator
// looking at a file that no longer exists.
func TestPollerFailureKindSwitchWarns(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	writeFile(t, path, validRuntime())

	store := NewStore(mustSnapshot(t, validRuntime()))
	var buf syncBuffer
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p := NewPoller(store, path, []byte(validRuntime()), 15*time.Millisecond, zerolog.New(&buf).Level(zerolog.InfoLevel), nil)
	go p.Run(ctx)

	// Fail as invalid content: one WARN on the transition into failure.
	writeFile(t, path, "models:\n  x:\n    endpoint: nope\n    upstream-model: m\n")
	waitUntil(t, 2*time.Second, func() bool { return buf.countEvents("config_reload_rejected") >= 1 },
		"no config_reload_rejected WARN for the invalid file")
	time.Sleep(3 * 15 * time.Millisecond)
	if got := buf.countEvents("config_reload_rejected"); got != 1 {
		t.Fatalf("config_reload_rejected logged %d times, want 1 before the kind switch", got)
	}

	// The failure switches kind (invalid content -> unreadable): warn again.
	if err := os.Remove(path); err != nil {
		t.Fatalf("remove: %v", err)
	}
	waitUntil(t, 2*time.Second, func() bool { return buf.countEvents("config_file_unreadable") >= 1 },
		"no config_file_unreadable WARN when the failure switched kind")
}
