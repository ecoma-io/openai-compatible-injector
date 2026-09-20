package config

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

func testLog(t *testing.T) zerolog.Logger {
	t.Helper()
	return zerolog.New(zerolog.TestWriter{T: t}).Level(zerolog.Disabled)
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

	p := NewPoller(store, path, 20*time.Millisecond, testLog(t))
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

	p := NewPoller(store, path, 15*time.Millisecond, testLog(t))
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

	p := NewPoller(store, path, 15*time.Millisecond, testLog(t))
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
	p := NewPoller(store, path, 10*time.Millisecond, testLog(t))
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
