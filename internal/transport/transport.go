// Package transport owns how an outbound request reaches its provider.
// The proxy layer decides which provider serves a request (the model →
// provider mapping); this package decides the path: direct, through one
// configured proxy endpoint, or across a configured pool of such endpoints.
// The seam is deliberately one method —
//
//	Do(req) → (*http.Response, error)
//
// with a contract the provider layer depends on: an HTTP response — any
// status, 429 and 5xx included — is an answer, never an error; only
// transport-level failures (dial, TLS, handshake, context cancellation
// before headers) return an error, and the response body is never read or
// buffered here, so a 200 text/event-stream reaches the caller as a live
// io.ReadCloser. Pool doers extend the seam with Execute, which carries the
// request facts eligibility needs (body size, stream flag) and returns what
// happened per attempt — but the response commitment is identical: any
// response ends the attempt loop, so a 429 is never retried on another
// egress. Upstream error interpretation, model rewriting, and prompt
// injection stay outside this package.
package transport

import (
	"context"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

// Doer executes one outbound HTTP exchange. *http.Client satisfies it; the
// interface exists so the request handler depends on the seam, not on a
// single global client — and so tests can stand in for it.
type Doer interface {
	Do(req *http.Request) (*http.Response, error)
}

// Resolver maps a validated transport config onto its Doer. The request
// handler resolves the model's provider transport through this, once per
// request; *Registry is the production implementation.
type Resolver interface {
	Doer(Config) Doer
}

// Kind selects the outbound path.
type Kind int

const (
	// Direct is the zero value: no configured proxy hop. It preserves the
	// historical shared client's behavior exactly, ambient
	// HTTP_PROXY/HTTPS_PROXY/NO_PROXY environment included.
	Direct Kind = iota
	// Proxy routes every request through the one configured proxy endpoint
	// (Config.ProxyURL). Exactly one endpoint — no scheduling, no health
	// tracking, no fallback: a pool of endpoints is the EgressPool kind.
	Proxy
	// EgressPool schedules each request onto one member endpoint (direct or
	// proxy), skipping members the request is ineligible for (streaming,
	// body size), a saturated member, or one in a health cooldown, and on a
	// pre-response transport failure falls back to a bounded number of
	// further members. Any response — 429 and 5xx included — ends the
	// attempt loop: HTTP statuses are answers, never fallback triggers.
	EgressPool
)

// Strategy selects how a pool schedules its initial endpoint among the
// currently eligible members. Weight never affects eligibility, and never
// affects the fallback order — only which member is tried first.
type Strategy int

const (
	// RoundRobin rotates one request at a time across the eligible members.
	RoundRobin Strategy = iota
	// WeightedRoundRobin schedules by smooth weighted round-robin: a member
	// with weight 2 is picked twice as often as one with weight 1, in a
	// deterministic interleaving.
	WeightedRoundRobin
)

// String renders the strategy as the config-facing token.
func (s Strategy) String() string {
	if s == WeightedRoundRobin {
		return "weighted_round_robin"
	}
	return "round_robin"
}

// Member is one pool endpoint: the direct/proxy transport it resolves to
// plus the eligibility and scheduling attributes the pool applies on top.
// Built only by the config layer, which normalizes defaults (streaming on,
// weight 1, no body or concurrency cap).
type Member struct {
	// Endpoint is the member's outbound path — a Direct or Proxy config,
	// never a pool (pools do not nest).
	Endpoint Config
	// MaxBodyBytes caps the OUTGOING (post-injection) request body this
	// member may carry; 0 means unlimited. A larger request is ineligible
	// for the member before any dial — the 6 MB request never produces a
	// 413 on a 4.5 MB relay.
	MaxBodyBytes int64
	// MaxConcurrency caps this member's live requests; 0 means unlimited.
	// A permit is held until the returned response body closes, so an SSE
	// stream occupies its member for the stream's whole lifetime.
	MaxConcurrency int
	// Streaming admits requests the client declared as streams; false
	// makes the member ineligible for them.
	Streaming bool
	// Weight scales this member's share of initial scheduling under
	// WeightedRoundRobin; at least 1.
	Weight int
}

// FallbackPolicy bounds the egress fallback within one request.
type FallbackPolicy struct {
	// Enabled turns the attempt loop past the first endpoint on. Disabled,
	// exactly one endpoint is dialed.
	Enabled bool
	// MaxAttempts is the maximum number of DISTINCT endpoints dialed for
	// one request, the initial attempt included. Skipped (ineligible,
	// unhealthy, saturated) members consume no attempt.
	MaxAttempts int
}

// HealthPolicy configures passive health tracking: consecutive
// fallback-eligible failures open a cooldown during which the member is
// skipped. Recovery needs no probe — any response, 4xx/5xx included,
// proves the path delivered and resets the state.
type HealthPolicy struct {
	// Enabled turns strike tracking on. Disabled, members are always
	// considered healthy.
	Enabled bool
	// FailureThreshold is the consecutive-failure count that opens a
	// cooldown; 0 (or Enabled=false) disables strikes.
	FailureThreshold int
	// Cooldown is how long a tripped member stays out of scheduling.
	Cooldown time.Duration
}

// Pool is the validated policy of one EgressPool transport: the ordered
// member list plus scheduling, fallback, and health policy. It is immutable
// after NewPool; its identity is a stable content hash string that two
// byte-equal configurations agree on, so the registry keeps scheduler,
// health, and concurrency state alive across reloads that do not change the
// policy.
type Pool struct {
	Members  []Member
	Strategy Strategy
	Fallback FallbackPolicy
	Health   HealthPolicy

	identity string
}

// NewPool assembles the immutable pool policy and computes its identity.
// The member slice is copied; later mutation of the caller's slice cannot
// change the pool.
func NewPool(members []Member, s Strategy, f FallbackPolicy, h HealthPolicy) *Pool {
	p := &Pool{
		Members:  append([]Member(nil), members...),
		Strategy: s,
		Fallback: f,
		Health:   h,
	}
	p.identity = p.computeIdentity()
	return p
}

// Identity returns the pool's stable content identity — the registry keys
// shared pool state (scheduler position, health, concurrency, leases) on
// it, so an unchanged pool across a reload keeps its warm state and a
// changed one starts fresh.
func (p *Pool) Identity() string { return p.identity }

// computeIdentity joins every policy input into one deterministic string.
// Fields are separated with bytes no config value can contain so that no
// concatenation is ambiguous; the result lives in memory (map keys, log
// fields on debug events) and carries member endpoint keys, whose proxy
// form embeds userinfo — the same memory-only treatment as Config.Key().
func (p *Pool) computeIdentity() string {
	var b []byte
	b = append(b, "strategy="...)
	b = append(b, p.Strategy.String()...)
	b = append(b, "\x1ffallback="...)
	b = strconv.AppendBool(b, p.Fallback.Enabled)
	b = append(b, ',')
	b = strconv.AppendInt(b, int64(p.Fallback.MaxAttempts), 10)
	b = append(b, "\x1fhealth="...)
	b = strconv.AppendBool(b, p.Health.Enabled)
	b = append(b, ',')
	b = strconv.AppendInt(b, int64(p.Health.FailureThreshold), 10)
	b = append(b, ",cooldown="...)
	b = append(b, p.Health.Cooldown.String()...)
	for i, m := range p.Members {
		b = append(b, "\x1emember="...)
		b = strconv.AppendInt(b, int64(i), 10)
		b = append(b, '|')
		b = append(b, m.Endpoint.Key()...)
		b = append(b, "|streaming="...)
		b = strconv.AppendBool(b, m.Streaming)
		b = append(b, "|body="...)
		b = strconv.AppendInt(b, m.MaxBodyBytes, 10)
		b = append(b, "|conc="...)
		b = strconv.AppendInt(b, int64(m.MaxConcurrency), 10)
		b = append(b, "|weight="...)
		b = strconv.AppendInt(b, int64(m.Weight), 10)
	}
	return string(b)
}

// Config is the validated shape of one named transport as it rides a config
// snapshot: the outbound kind plus, for Proxy, the parsed proxy endpoint,
// and for EgressPool the pool policy. Userinfo in a proxy URL is allowed —
// it is the proxy's authentication — and is credential material: it never
// reaches logs or error text. The zero value is the direct transport, which
// is also what a provider with no transport reference resolves to;
// validation guarantees ProxyURL is non-nil whenever Kind is Proxy and Pool
// non-nil whenever Kind is EgressPool.
type Config struct {
	Kind     Kind
	ProxyURL *url.URL
	// Pool carries the validated pool policy for Kind == EgressPool. It is
	// a pointer so Config stays comparable (it rides in sets); two
	// configurations that share a transport name share the pointer, and
	// content identity always goes through key(), which reads the pool's
	// identity string — never the pointer.
	Pool *Pool
}

// key identifies a Config by content — the registry's pool-sharing
// identity: two Configs with the same key are the same transport and share
// one connection pool, so a config reload that does not change a
// transport's settings keeps its warm pool. The proxy URL string carries
// userinfo; it lives in memory as a map key only and is never logged.
func (c Config) Key() string {
	switch c.Kind {
	case Proxy:
		if c.ProxyURL == nil {
			// Unreachable through config validation (a proxy transport
			// without a URL rejects the file); failing loud beats silently
			// routing a proxy request direct.
			panic("transport: proxy config without a proxy URL")
		}
		return "proxy " + c.ProxyURL.String()
	case EgressPool:
		if c.Pool == nil {
			// Unreachable through config validation (a pool transport
			// without members rejects the file); failing loud beats routing
			// a pooled request to an accidental default.
			panic("transport: pool config without pool policy")
		}
		return "pool " + c.Pool.Identity()
	default:
		return "direct"
	}
}

// kindName renders the endpoint kind for log metadata: "direct", the proxy
// scheme, or "pool".
func (c Config) kindName() string {
	switch c.Kind {
	case Proxy:
		return c.ProxyURL.Scheme
	case EgressPool:
		return "pool"
	default:
		return "direct"
	}
}

// target renders the endpoint's scheme+host — the same log-safe detail the
// upstream endpoint is allowed — or "direct" for the direct transport.
// Userinfo never appears.
func (c Config) target() string {
	if c.Kind == Proxy {
		return c.ProxyURL.Scheme + "://" + c.ProxyURL.Host
	}
	return "direct"
}

// AttemptRequest carries everything one outbound attempt needs, without
// binding the caller's *http.Request: the pool reconstructs a fresh request
// per attempt and never mutates the caller's. Body is the full outgoing
// (post-injection) request body — request bodies are buffered on this
// proxy's paths, so its length is the routing input for the body-size
// eligibility gate.
type AttemptRequest struct {
	Ctx       context.Context
	Method    string
	URL       *url.URL
	Header    http.Header
	Body      []byte
	Streaming bool
}

// AttemptFailure is the sanitized evidence of one dialed-and-failed
// endpoint inside one Execute: the endpoint's kind and scheme+host target
// (the same log-safe surface AttemptInfo.Target uses — userinfo never
// enters), the typed failure class ("connection", "proxy_connect",
// "proxy_auth", "timeout") and its bounded cause token ("connection_refused",
// "tls", "dial", ...). The error text never rides along: classes and causes
// are derived from typed errors and stdlib sentinels, not message parsing,
// and the raw error stays inside the pool. AttemptInfo.Failures carries at
// most the fallback budget's worth of these — one per dialed-and-failed
// endpoint, nothing for skipped members.
type AttemptFailure struct {
	Kind   string
	Target string
	Class  string
	Cause  string
}

// AttemptInfo reports what one Execute did: how many distinct endpoints
// were actually dialed (skipped members consume no attempt), the kind and
// scheme+host of the last endpoint dialed (empty when none was),
// whether the loop ended without dialing anything (every member was
// ineligible, unhealthy, or saturated), and the per-attempt failure
// evidence (one AttemptFailure per dialed endpoint that failed, in dial
// order — bounded by the fallback budget).
type AttemptInfo struct {
	Attempts  int
	Kind      string
	Target    string
	Exhausted bool
	Failures  []AttemptFailure
}

// Executor is the capability of a Doer that owns multi-egress policy. The
// request handler checks for it after resolving the model's Doer: a plain
// client gets Do (one endpoint, one exchange), a pool gets Execute and owns
// selection, attempts, and fallback. The response commitment matches Do's:
// any returned response — any status — ends the attempt loop; only
// pre-response transport failures fall back, and a returned response body
// remains a live io.ReadCloser the caller closes as usual (closing it
// releases the member's concurrency permit and the pool's in-flight lease).
type Executor interface {
	Execute(*AttemptRequest) (*http.Response, AttemptInfo, error)
}
