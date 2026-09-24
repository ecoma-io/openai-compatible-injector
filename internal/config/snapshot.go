package config

import (
	"errors"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"

	"openai-compatible-injector/internal/credential"
	"openai-compatible-injector/internal/recovery"
	"openai-compatible-injector/internal/transport"
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

// SSEKeepAlive is the validated SSE keep-alive heartbeat config: while a
// client-facing SSE stream sits silent (a long model "thinking" phase), one
// ignorable comment is written per interval so intermediary proxies do not
// cut an idle stream — a Cloudflare-proxied hostname kills a silent HTTP/2
// stream after ~125s. The zero value is not a usable default; snapshots are
// built by LoadRuntime, which always fills Enabled and Interval.
type SSEKeepAlive struct {
	// Enabled turns the heartbeat on. The default is on: this proxy is
	// deployed behind Cloudflare, where a silent stream dies at ~125s.
	Enabled bool
	// Interval is the silence threshold: after this long without a byte
	// written to the client, one ": ping" comment is written and flushed.
	// At least minSSEKeepAliveInterval.
	Interval time.Duration
}

// StripPath is one parsed response-strip path: the ordered object-key
// segments a path descends, e.g. `usage.cache_read_input_tokens` → two
// segments. Dotted source segments were written through single-quote JSON
// quoting at config load, so segments arrive here already decoded (a `.`
// inside a quoted segment is literal). The traversal is object-only: a
// segment never descends through an array.
type StripPath struct {
	Segments []string
}

// Candidate is one provider hop in a model's routing chain: where the
// request goes, under what upstream model name, over which outbound path,
// and under which recovery policy. Chains are built immutable at config
// load; the first candidate is the primary route and the rest are fallbacks
// a request walks only when an earlier one fails before answering (never on
// an HTTP status — statuses are answers).
type Candidate struct {
	// Provider names the providers-table entry; empty for the legacy inline
	// endpoint form, which builds a one-candidate chain.
	Provider string
	// Endpoint is the candidate's validated upstream base URL
	// (e.g. https://h/v1), from the referenced provider's base-url.
	Endpoint *url.URL
	// UpstreamModel is the model name sent to this candidate's provider.
	UpstreamModel string
	// Transport is the outbound path this candidate executes through —
	// the provider's referenced transport, or the zero value (direct) for
	// a provider without a transport reference.
	Transport transport.Config
	// Recovery is the effective recovery policy this candidate executes
	// under. It is resolved at load time — global block, then this
	// candidate's provider override, then the model override, then the
	// candidate's own override — and frozen onto the snapshot, so a reload
	// mid-walk (mid-wait included) cannot reshape these budgets: a request
	// is bound to the policy it started under.
	Recovery recovery.Policy
	// RecoveryHash is Recovery's data identity, computed once at load time
	// and carried alongside it so the request path never re-hashes a policy
	// per attempt. It names the policy DATA, not a behavioural claim: two
	// candidates sharing a hash ran under the same rules and budgets, which
	// is exactly what the evidence needs to say.
	RecoveryHash string
	// Strip is the response-strip list this candidate executes under. It is
	// the candidate's provider's list, unless the model overrides it (a
	// model-level list replaces the provider list — see Model.Strip). Nil or
	// empty means no stripping for answers from this candidate.
	Strip []StripPath
	// Cred is the candidate's upstream credential pool configuration — the
	// provider's auth block, spec plus cooldown policy — or nil when the
	// provider states no auth block and requests carry no injected
	// credential. It is configuration, not state: which key the next
	// attempt carries is decided per attempt through the credential
	// registry keyed by Spec content. Nil keeps every request byte exactly
	// as it was before credentials existed.
	Cred *credential.Provider
}

// Label is the candidate's observability identity: the providers-table
// name, or the endpoint origin for the legacy inline form. Log-safe by
// construction — scheme+host only, the same surface every upstream URL is
// allowed to show.
func (c Candidate) Label() string {
	if c.Provider != "" {
		return c.Provider
	}
	return c.Endpoint.Scheme + "://" + c.Endpoint.Host
}

// Model is one validated public-model mapping. It is immutable after the
// Snapshot is built.
type Model struct {
	// Public is the model name clients use, i.e. the map key.
	Public string
	// Provider names the providers-table entry this model routes through;
	// empty for the legacy inline endpoint form. It is resolution metadata,
	// not a second lookup: the entry's base URL and transport are already
	// flattened into Endpoint and Transport here. It always mirrors
	// Chain[0].
	Provider string
	// Endpoint is the validated upstream base URL (e.g. https://h/v1),
	// from the referenced provider's base-url or the model's own endpoint.
	// It always mirrors Chain[0].
	Endpoint *url.URL
	// UpstreamModel is the model name sent to the upstream provider. It
	// always mirrors Chain[0].
	UpstreamModel string
	// InjectionPrompt is the system-level instruction injected into every
	// request. Empty means no injection. Injection is a property of the
	// PUBLIC model: every candidate in the chain receives the same prompt.
	InjectionPrompt string
	// ThinkingUsage is the validated simulated thinking-usage synthesis
	// config. The zero value means the feature is off for this model.
	ThinkingUsage ThinkingUsage
	// Strip is the model's response-strip list. When the model states its
	// own strip-fields, this is that list, applied uniformly to every
	// candidate's answer (model overrides provider). When it does not, this
	// is nil and each candidate carries its provider's list (Candidate.Strip)
	// — the handler reads per-hop. Nil or empty means no stripping.
	Strip []StripPath
	// Recovery is the model's primary-candidate recovery policy: the
	// effective policy the handler builds its engine from without reaching
	// into the chain. It always mirrors Chain[0].Recovery.
	Recovery recovery.Policy
	// RecoveryHash is Recovery's data identity, and always mirrors
	// Chain[0].RecoveryHash. It is what the completion record names, so an
	// operator can tell two request populations apart by the policy they ran
	// under without reading the policy itself.
	RecoveryHash string
	// Transport is the outbound path requests for this model execute
	// through — the primary candidate's path. It always mirrors Chain[0].
	Transport transport.Config
	// Chain is the model's provider candidate list, primary first. It
	// always has at least one entry: the legacy provider/endpoint forms
	// build a one-candidate chain, so the handler walks one uniform
	// structure — there is no separate legacy code path to drift.
	Chain []Candidate
}

// Snapshot is an immutable view of a validated runtime configuration. It is
// never mutated after construction; each HTTP request binds exactly one
// Snapshot for its whole lifetime, so a reload mid-request cannot change the
// endpoint, model, or prompt that request is using.
type Snapshot struct {
	gen uint64
	// apiKey is the client bearer credential this snapshot requires. It is
	// credential material: compared per request, never logged, never echoed
	// in error text.
	apiKey string
	models map[string]Model
	// transports is the distinct set of outbound transport configs the
	// models reference — the retain set the transport registry reconciles
	// to on publish. Every chain candidate's transport is part of the set,
	// not just the primaries': a request that falls back must find its
	// candidate's clients warm.
	transports []transport.Config
	// credentials is the distinct set of upstream credential providers the
	// models reference — the retain set the credential registry reconciles
	// to on publish, deduplicated by spec content so two candidates sharing
	// one provider's accounts share one pool and one rotation cursor.
	credentials []*credential.Provider
	logLevel    zerolog.Level
	keepAlive   SSEKeepAlive
}

// Gen returns the snapshot's generation number (0 for the initial snapshot,
// incremented on every successful reload).
func (s *Snapshot) Gen() uint64 { return s.gen }

// LogLevel returns the log level this snapshot carries. It is applied to the
// process-wide logger when the snapshot is published, making the level
// hot-reloadable through the same content-hash poll as everything else.
func (s *Snapshot) LogLevel() zerolog.Level { return s.logLevel }

// APIKey returns the client bearer key this snapshot requires. It is read
// per request, so a reload rotates the key for subsequent requests only —
// an in-flight request stays bound to the snapshot it authenticated
// against. It is never logged.
func (s *Snapshot) APIKey() string { return s.apiKey }

// SSEKeepAlive returns the SSE keep-alive settings this snapshot carries.
// They bind to the request like everything else on the snapshot, so an
// in-flight stream keeps the interval it started with across a reload.
func (s *Snapshot) SSEKeepAlive() SSEKeepAlive { return s.keepAlive }

// Transports returns the distinct outbound transport configs this
// snapshot's models reference. The registry retains exactly these on
// publish; a transport absent from the new snapshot has its idle pool
// closed while in-flight requests on it finish untouched. The slice is a
// copy — the snapshot stays immutable.
func (s *Snapshot) Transports() []transport.Config {
	return append([]transport.Config(nil), s.transports...)
}

// Credentials returns the distinct upstream credential providers this
// snapshot's models reference. The credential registry retains exactly
// these on publish; a provider absent from the new snapshot has its pool
// dropped while in-flight requests holding the old pool finish untouched —
// a pool is pure state with nothing to close, so eviction is a map delete
// and no deferred teardown. The slice is a copy — the snapshot stays
// immutable.
func (s *Snapshot) Credentials() []*credential.Provider {
	return append([]*credential.Provider(nil), s.credentials...)
}

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
