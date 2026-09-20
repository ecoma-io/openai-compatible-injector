package config

import (
	"errors"
	"net/url"
	"sync/atomic"
)

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
}

// Snapshot is an immutable view of a validated runtime configuration. It is
// never mutated after construction; each HTTP request binds exactly one
// Snapshot for its whole lifetime, so a reload mid-request cannot change the
// endpoint, model, or prompt that request is using.
type Snapshot struct {
	gen    uint64
	models map[string]Model
}

// Gen returns the snapshot's generation number (0 for the initial snapshot,
// incremented on every successful reload).
func (s *Snapshot) Gen() uint64 { return s.gen }

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

// Store holds the currently active Snapshot behind an atomic pointer. The
// reload loop is the only publisher; every request is a reader.
type Store struct {
	p   atomic.Pointer[Snapshot]
	gen atomic.Uint64
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
func (s *Store) Publish(next *Snapshot) error {
	if next == nil {
		return errors.New("cannot publish a nil snapshot")
	}
	next.gen = s.gen.Add(1)
	s.p.Store(next)
	return nil
}

// Gen returns the generation of the currently active snapshot.
func (s *Store) Gen() uint64 { return s.p.Load().Gen() }
