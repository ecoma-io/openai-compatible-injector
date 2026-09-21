package config

import (
	"errors"
	"net/url"
	"sync"
	"sync/atomic"

	"github.com/rs/zerolog"
)

// ThinkingMode selects when simulated thinking-usage synthesis applies to a
// model's responses.
type ThinkingMode int

const (
	// ThinkingOff never synthesizes. It is the zero value and the state of
	// every model whose runtime entry carries no thinking-usage block.
	ThinkingOff ThinkingMode = iota
	// ThinkingAuto synthesizes only when the request body signals thinking.
	ThinkingAuto
	// ThinkingAlways synthesizes for every request, overriding whatever
	// thinking signal the request does or does not carry.
	ThinkingAlways
)

// ThinkingUsage is the validated per-model simulated thinking-usage config:
// some upstream models reason without ever reporting reasoning tokens, and
// this synthesis attributes a share of the upstream-reported output tokens to
// thinking in the client-facing usage object. Lo and Hi are that share's
// bounds in [0,1]; Lo == Hi is a fixed share with no per-request draw. The
// zero value is off.
type ThinkingUsage struct {
	Mode ThinkingMode
	Lo   float64
	Hi   float64
}

// Model is one validated public-model mapping. It is immutable after the
// Snapshot is built.
type Model struct {
	// Public is the model name clients use, i.e. the map key.
	Public string
	// Endpoint is the validated upstream base URL (e.g. https://h/v1).
	Endpoint *url.URL
	// UpstreamModel is the model name sent to the upstream provider.
	UpstreamModel string
	// InjectionPrompt is the system-level instruction injected into every
	// request. Empty means no injection.
	InjectionPrompt string
	// ThinkingUsage is the validated simulated thinking-usage synthesis
	// config. The zero value means the feature is off for this model.
	ThinkingUsage ThinkingUsage
}

// Snapshot is an immutable view of a validated runtime configuration. It is
// never mutated after construction; each HTTP request binds exactly one
// Snapshot for its whole lifetime, so a reload mid-request cannot change the
// endpoint, model, or prompt that request is using.
type Snapshot struct {
	gen      uint64
	models   map[string]Model
	logLevel zerolog.Level
}

// Gen returns the snapshot's generation number (0 for the initial snapshot,
// incremented on every successful reload).
func (s *Snapshot) Gen() uint64 { return s.gen }

// LogLevel returns the log level this snapshot carries. It is applied to the
// process-wide logger when the snapshot is published, making the level
// hot-reloadable through the same content-hash poll as everything else.
func (s *Snapshot) LogLevel() zerolog.Level { return s.logLevel }

// Model resolves a public model name. The second return value reports
// whether the model exists.
func (s *Snapshot) Model(public string) (Model, bool) {
	m, ok := s.models[public]
	return m, ok
}

// Models returns a copy of the model table.
func (s *Snapshot) Models() map[string]Model {
	out := make(map[string]Model, len(s.models))
	for k, v := range s.models {
		out[k] = v
	}
	return out
}

// Len returns the number of models in the table without copying it —
// the cheap form used by reload logging.
func (s *Snapshot) Len() int { return len(s.models) }

// Store holds the currently active Snapshot behind an atomic pointer. The
// reload loop is the only publisher; every request is a reader.
type Store struct {
	p     atomic.Pointer[Snapshot]
	gen   atomic.Uint64
	pubMu sync.Mutex // orders Publish's gen assignment with its pointer store
}

// NewStore returns a store seeded with the initial snapshot.
func NewStore(initial *Snapshot) *Store {
	s := &Store{}
	s.p.Store(initial)
	return s
}

// Load returns the active snapshot. The returned pointer is immutable and
// remains valid after any later publish; callers must not retain it past the
// request they are serving.
func (s *Store) Load() *Snapshot { return s.p.Load() }

// Publish atomically replaces the active snapshot with next and assigns it a
// monotonically increasing generation. Only the reload loop (or tests) may
// call it.
//
// The mutex is not about the pointer store (that is atomic on its own) but
// about pairing gen assignment with the store: without it, two concurrent
// publishers can assign generations 4 then 5 yet store in the opposite
// order — the active snapshot's generation visibly going backwards, and a
// final Gen() below the publish count.
func (s *Store) Publish(next *Snapshot) error {
	if next == nil {
		return errors.New("cannot publish a nil snapshot")
	}
	s.pubMu.Lock()
	defer s.pubMu.Unlock()
	next.gen = s.gen.Add(1)
	s.p.Store(next)
	return nil
}

// Gen returns the generation of the currently active snapshot.
func (s *Store) Gen() uint64 { return s.p.Load().Gen() }
