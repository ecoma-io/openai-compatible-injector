package config

import (
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/rs/zerolog"
)

// runtimeVariants returns valid runtime files whose model tables differ: one
// keeps gpt-reviewer as configured, one renames its upstream model, one
// drops the public name entirely, and one carries two models. Cycling
// through them makes a model legitimately appear and disappear across
// reloads, so a reader that fails to accept both states is wrong — and a
// reader that ever observes a half-built table is a real defect.
func runtimeVariants() []string {
	base := validRuntime()
	return []string{
		base,
		strings.Replace(base, "gpt-5-pro", "gpt-6-pro", 1),
		strings.Replace(base, "gpt-reviewer", "gpt-auditor", 1),
		`
api-key: unit-test-key
models:
  gpt-reviewer:
    endpoint: https://api.provider.example/v1
    upstream-model: gpt-5-pro
    injection-prompt: review carefully
  gpt-coder:
    endpoint: http://localhost:9000/v1
    upstream-model: gpt-5-coder
`,
	}
}

// TestStoreConcurrentReadersUnderPublish drives the snapshot handoff under a
// publish storm: many readers each bind thousands of snapshots in sequence
// while several writers republish fresh ones. The one-snapshot-per-request
// contract says every pointer a reader ever loads must be a complete,
// immutable snapshot — generation monotonically non-decreasing inside the
// reader, every table entry whole and self-consistent, gpt-reviewer either
// present and correct or legitimately absent, Models() safe to copy and
// range — and the store's generation must equal the number of publishes
// when the dust settles.
func TestStoreConcurrentReadersUnderPublish(t *testing.T) {
	variants := runtimeVariants()
	for i, v := range variants {
		if _, err := LoadRuntime([]byte(v)); err != nil {
			t.Fatalf("variant %d is not valid runtime YAML: %v", i, err)
		}
	}

	store := NewStore(mustSnapshot(t, validRuntime()))

	const (
		readers     = 128
		readerIters = 3000
		writers     = 4
		writerIters = 1000
	)

	var violations, reads atomic.Int64
	var publishes atomic.Uint64
	fail := func(format string, args ...any) {
		violations.Add(1)
		t.Errorf(format, args...)
	}

	var readersWG, writersWG sync.WaitGroup
	for r := 0; r < readers; r++ {
		readersWG.Add(1)
		go func() {
			defer readersWG.Done()
			lastGen := uint64(0)
			for i := 0; i < readerIters; i++ {
				s := store.Load()
				if s == nil {
					fail("Load() returned a nil snapshot")
					return
				}
				if gen := s.Gen(); gen < lastGen {
					fail("generation went backwards inside one reader: %d after %d", gen, lastGen)
					return
				} else {
					lastGen = gen
				}
				if m, ok := s.Model("gpt-reviewer"); ok {
					if m.Public != "gpt-reviewer" || m.Endpoint == nil || m.UpstreamModel == "" {
						fail("gpt-reviewer resolved to a broken entry: %+v", m)
						return
					}
				} // absent is legal: a reload may have dropped the model
				if i%8 == 0 {
					mods := s.Models()
					if len(mods) != s.Len() {
						fail("Len() = %d but Models() returned %d entries", s.Len(), len(mods))
						return
					}
					for name, m := range mods {
						if name == "" || m.Public != name || m.Endpoint == nil || m.Endpoint.Host == "" || m.UpstreamModel == "" {
							fail("table entry %q is not whole: %+v", name, m)
							return
						}
					}
					switch s.LogLevel() {
					case zerolog.DebugLevel, zerolog.InfoLevel, zerolog.WarnLevel, zerolog.ErrorLevel:
					default:
						fail("snapshot carried log level %v", s.LogLevel())
						return
					}
				}
				reads.Add(1)
			}
		}()
	}
	for w := 0; w < writers; w++ {
		writersWG.Add(1)
		go func(w int) {
			defer writersWG.Done()
			for i := 0; i < writerIters; i++ {
				// A fresh snapshot per publish: Publish writes the generation
				// into its argument, so a shared pointer would race.
				s, err := LoadRuntime([]byte(variants[(w+i)%len(variants)]))
				if err != nil {
					fail("writer %d: variant %d failed to load: %v", w, (w+i)%len(variants), err)
					return
				}
				if err := store.Publish(s); err != nil {
					fail("writer %d: Publish: %v", w, err)
					return
				}
				publishes.Add(1)
				if i%64 == 0 {
					runtime.Gosched()
				}
			}
		}(w)
	}

	readersWG.Wait()
	writersWG.Wait()

	if got := store.Gen(); got != publishes.Load() {
		t.Errorf("store.Gen() = %d, want %d (one generation per publish)", got, publishes.Load())
	}
	if got := reads.Load(); got != readers*readerIters {
		t.Errorf("completed %d reads, want %d", got, readers*readerIters)
	}
	active := store.Load()
	if active == nil || active.Len() < 1 {
		t.Errorf("final active snapshot is not usable: %v", active)
	}
	if v := violations.Load(); v > 0 {
		t.Errorf("%d invariant violations observed across %d readers and %d writers", v, readers, writers)
	}
}
