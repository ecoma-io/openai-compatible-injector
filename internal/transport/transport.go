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
	"io"
	"net/http"
	"net/url"
	"strconv"
	"sync"
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

	// Budget, when non-nil, is claimed once immediately before each real
	// dial. A nil budget means the caller is not metering exchanges.
	//
	// The claim sits AFTER every eligibility, health, and concurrency gate
	// and immediately before the dial, so it counts exchanges that actually
	// reached the wire: a member skipped before dialing spends nothing. A
	// claim that returns false stops the attempt loop — nothing is dialed
	// on that turn and no member is blamed for it.
	Budget ExchangeBudget
}

// AttemptFailure is the sanitized evidence of one dialed-and-failed
// endpoint inside one Execute: the endpoint's kind and scheme+host target
// (the same log-safe surface AttemptInfo.Target uses — userinfo never
// enters), the typed failure class ("connection", "proxy_connect",
// "proxy_auth", "timeout"), its bounded cause token ("connection_refused",
// "tls", "dial", ...), and the send state ("definitely_not_sent" /
// "send_unknown") that says whether the request bytes could have reached
// the endpoint. The error text never rides along: classes, causes and
// send states are derived from typed errors and stdlib sentinels, not
// message parsing, and the raw error stays inside the pool.
// AttemptInfo.Failures carries at most the fallback budget's worth of
// these — one per dialed-and-failed endpoint, nothing for skipped members.
type AttemptFailure struct {
	Kind      string
	Target    string
	Class     string
	Cause     string
	SendState string
}

// AttemptInfo reports what one Execute did: how many distinct endpoints
// were actually dialed (skipped members consume no attempt), the kind and
// scheme+host of the last endpoint dialed (empty when none was),
// whether the loop ended without dialing anything (every member was
// ineligible, unhealthy, or saturated, or the request's exchange budget
// refused the first dial), and the per-attempt failure evidence (one
// AttemptFailure per dialed endpoint that failed, in dial order — bounded
// by the fallback budget).
type AttemptInfo struct {
	Attempts  int
	Kind      string
	Target    string
	Exhausted bool
	Failures  []AttemptFailure

	// BudgetExhausted reports that the attempt loop stopped because the
	// request's exchange budget refused another dial. It is set whether the
	// loop stopped before any dial (Attempts == 0) or after some.
	BudgetExhausted bool
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

// unboundedWindow is the window a caller that does not meter its exchanges
// passes to DialWithin: no proxy-imposed bound, so the dial runs under the
// caller's own context exactly as the historical Do(req) did. It is
// non-positive on purpose — DialWithin's own guard treats any such window as
// "nothing to enforce" — and it is never a claim about a budget, because no
// budget is consulted on that path.
const unboundedWindow time.Duration = 0

// DialWithin executes one outbound exchange through the Doer, bounded by
// window. The window covers the WHOLE attempt: the pre-response phase, and —
// unless the caller hands the answer off with HandoffStream — every byte of
// the response body through EOF. It is the enforcement of `max-elapsed` on an
// exchange that has not yet committed, the gap the envelope's elapsed half was
// never able to cover on its own, because a peer that accepts the connection
// and sends no status line parks the caller in Do for as long as the client
// gives it.
//
// The window is a value, not a budget handle, and that is the point: the
// caller claimed it atomically against the envelope (ExchangeBudget) and hands
// over a decision already made, so no second reading of a moving budget can
// contradict the authority under which this dial starts.
//
// The bound is applied to a COPY of the request, never to the caller's own: a
// request's context is the caller's cancellation and deadline channel, and
// folding the proxy's exchange bound into it would make the walk read a
// proxy-owned timeout as the caller's own (caller_deadline_exceeded, terminal)
// — the exact mis-classification an exchange timeout must not produce. The
// caller's context stays the one ClassifyAttempt sees, so a proxy deadline
// surfaces as the endpoint's timeout: fallback-eligible, reviewable by the
// matrix.
//
// # Two phases, and the caller decides between them
//
// The obvious implementation — derive a context with a timeout, clone the
// request onto it, release the derived context on Close — does NOT do what it
// looks like it does. net/http ties a response BODY to the context of the
// request that produced it: when that context is canceled, the transport
// aborts the connection, so an already-returned body dies with its own dial's
// deadline. An SSE stream that legitimately runs for minutes is truncated at
// whatever `max-elapsed` happened to be — the relay stops mid-answer and the
// proxy reports upstream truncation for a stream that is answering perfectly.
//
// So the deadline is handed over rather than inherited, and the decision to
// hand it over is the CALLER's, taken after it knows what the response is:
//
//  1. By default the window is still armed when the response is returned. The
//     body is a pre-commitment answer, and the envelope covers reading it: a
//     candidate's `max-elapsed` is an absolute attempt window, not merely a
//     header deadline, and a buffered 2xx is read to EOF inside the walk —
//     before commitment — so a peer that sends headers and then stalls its
//     JSON is still inside its own envelope. A window that ends such a read
//     surfaces as ExchangeTimeoutError, this proxy's bound, typed precisely so
//     the evidence does not blame the peer for it.
//  2. HandoffStream(resp) is the release. A caller that has decided the answer
//     is committed — a confirmed event stream, or a body it will relay
//     verbatim — disarms the window, and the body then runs under the caller's
//     own context as if the proxy had never interfered. A committed body may
//     outlive the window without limit, because a stream's silence is bounded
//     by the recovery window, which knows about upstream activity and this
//     window does not.
//
// The handoff is decided AFTER the response, and it is atomic in both
// directions, so the outcome never depends on which side wins a race:
//
//   - window first: the body is never handed to the caller, the dial is
//     reported as a timeout, and the connection the timer tore down is closed
//     rather than returned for reading — a caller must never read a truncated
//     body and believe it was complete;
//   - handoff first: the timer is stopped before the body is published as
//     committed, so no later instant can fire. The interval is half-open: a
//     timer that fires at the very instant the caller commits is a refusal,
//     read as one, never as a body the timer has already chosen to kill.
//
// The caller's cancellation remains the ultimate authority in BOTH phases and
// stays the context the caller's own code reads: a client that hangs up aborts
// the dial AND the body, through the explicit stop rather than through the
// replaced Done channel. The caller's Values survive into the copy — the
// requirement of context.WithoutCancel — so transport-level context values
// keep working; only cancellation is replaced, and replaced by something the
// proxy controls.
//
// The exchange's resources are released EXACTLY ONCE, by whichever of the two
// events happens first — the body closing, or the dial being abandoned —
// through a single guarded release, never by a timer racing a Close. That
// release also DISARMS the window, so an exchange that ends in a fast failure
// or a fast buffered answer leaves no live timer holding its closure until the
// envelope elapses. A non-positive window bounds nothing: the dial runs as the
// caller's own request, with no proxy-imposed bound at all, which is what a
// caller that granted no window means. Such a response is unaffected by
// HandoffStream, which has nothing to disarm.
//
// armWindow is how the window's timer is armed, and it is replaceable only in
// tests. It exists for one reason: the handoff below is a race, and a race a
// test can only enter by hoping to win it is a race that passes on a fast
// machine and is missed on a loaded one. Given the action that ends the
// window, it returns the handoff's authority — the stop whose result says
// whether that action was removed before it ran, which is the one fact the
// handoff is decided on. A test supplies its own, ends the window at the exact
// instant a response exists, and pins which side of that instant the code
// treats as decisive. No production caller sets it.
type armWindow func(action func()) (stop func() bool)

// DialWithin executes one request under an already-granted window. The
// returned response keeps the window armed through its body; call
// HandoffStream on it once the answer is committed to release it.
func DialWithin(window time.Duration, ctx context.Context, doer Doer, req *http.Request) (*http.Response, error) {
	return dialWithin(window, nil, ctx, doer, req)
}

func dialWithin(window time.Duration, arm armWindow, ctx context.Context, doer Doer, req *http.Request) (*http.Response, error) {
	if window <= 0 {
		// No window to enforce. The caller's context is the only bound, and
		// the request is used as given — the historical Do(req), unchanged.
		// (A granted exchange never arrives here: the envelope refuses an
		// already-spent elapsed half, so a non-positive window is a caller
		// that never metered at all.)
		return doer.Do(req)
	}
	// The caller's Values, without its cancellation. Until the window is
	// disarmed the dial is bounded by the window and by the explicit stop wired
	// to the caller's Done channel below, which is the one path by which a
	// client that hangs up can still end it.
	//
	// ONE derived context, not a chain of them. A second layer would have to
	// be released separately, and releasing it on the success path — the only
	// way to stop watching the caller — cancels everything beneath it,
	// including the body this function has just handed back. One cancel, one
	// owner, one release point.
	pre, stop := context.WithCancel(context.WithoutCancel(ctx))

	// Two candidates for ending the window's authority — the window and the
	// caller's context — and exactly one winner. Which one it was IS the
	// ownership question, so it is decided in one critical section at the
	// instant the outcome is settled, never re-derived afterwards from a
	// context's error: a dial that loses a race to both at once must have one
	// owner, and the one that can say which.
	win := &dialRace{window: window}
	// The caller's own end, racing the window from the start. Without this
	// the derived context would be cancellation-free, and a client that
	// hangs up mid-dial would watch its request park until the window elapsed
	// — reporting this proxy's bound for the client's event.
	stopCallerWatch := context.AfterFunc(ctx, func() {
		win.claim(causeCaller)
		stop()
	})
	// The window's one action, and the stop that decides the handoff. Arming is
	// a seam so a test can fire the very same action at the very instant an
	// answer is committed; the action itself is the one production runs, so
	// what a test proves about the collision is a fact about this code and not
	// about a stand-in.
	if arm == nil {
		arm = func(action func()) func() bool {
			t := time.AfterFunc(window, action)
			return t.Stop
		}
	}
	// The timer callback runs on its own goroutine and may fire at any instant,
	// so the body it has to close is published under a mutex rather than read
	// from a variable the handoff writes.
	var bodyMu sync.Mutex
	var body io.Closer
	stopWindow := arm(func() {
		// The window elapsed. This runs on the timer's own goroutine and its
		// whole job is to abort the exchange: it must not block, must not touch
		// anything the caller reads concurrently, and must return. The typed
		// cause carries everything the matrix and the logs read; the raw
		// context error stays here. A body, once there is one, is closed too:
		// canceling net/http's request context usually achieves that, but
		// making the close explicit is what gives its blocked Read a prompt,
		// deterministic escape independent of transport internals.
		win.claim(causeExchangeWindow)
		stop()
		bodyMu.Lock()
		b := body
		bodyMu.Unlock()
		if b != nil {
			_ = b.Close()
		}
	})
	// release is the exchange's single release point, and it does everything
	// this function owes the exchange — the timer, the caller's watch and the
	// derived context — together, and exactly once. A guard makes "exactly
	// once" structural rather than a question about which path was taken:
	// handing the watch off without stopping it would leave a goroutine armed
	// against a context that is being torn down, and stopping it without the
	// guard would let a Close race the deferred call into a double release.
	//
	// stopWindow() belongs here and nowhere else, which is also why it is
	// captured by the closure rather than read from the outer variable: the
	// callback above may already be running, and every path that ends the
	// exchange — a synchronous failure, a body closed inside the window, a
	// deferred abandonment — reaches this one call, so no exchange leaves a
	// live timer holding its closure until the envelope elapses.
	//
	// So: one function, no parameter, and the handoff and the abandonment are
	// the same call with a different winner. That is what makes the two paths
	// provably identical instead of merely intended to be.
	//
	// The claim on the way out is what DECIDES the race for a path that has no
	// response, so nothing can win it afterwards. It does mean a body closed
	// after a response records the caller as the cause, which is inert while
	// the only reader of the decision is Read — that relabels a read solely
	// for the window's cause, so a caller-claim changes nothing. Code that
	// reads the decision AFTER a Close must therefore not read it as a
	// statement about who ended the exchange; the release already did.
	var releaseOnce sync.Once
	release := func() {
		releaseOnce.Do(func() {
			stopWindow()
			stopCallerWatch()
			win.claim(causeCaller)
			stop()
		})
	}

	resp, err := doer.Do(req.Clone(pre))
	if err != nil {
		// The outcome is read BEFORE the release, and that order is the whole
		// point: release claims the race on its way out, so a release that ran
		// first would convert every ordinary dial failure into the caller's own
		// cancellation and throw away the endpoint's error. Reading first keeps
		// the real winner — which is only ever the window or the caller, because
		// this path has no response for a body to be racing.
		outcome, decided := win.decided()
		// No response, so nothing survives the exchange: the timer, the
		// caller's watch and the derived context are all released here.
		release()
		if decided {
			// The context-done diagnostic is dropped on purpose: the error a
			// canceled request context produces names the stack it unwound
			// through and the address it was talking to, neither of which
			// belongs in this service's evidence. The endpoint's own failure
			// below is returned intact, so send-state classification is
			// untouched by anything this function does with the deadline.
			return nil, outcome.err()
		}
		return nil, err
	}

	// A RESPONSE EXISTS, and the window is STILL ARMED. The body is wrapped
	// so that a window-owned read surfaces as the typed timeout, and the
	// release rides the body's Close — the caller's, whenever it gets there.
	//
	// Publishing the wrapper is a race the caller resolves, not this function:
	// the timer may have fired in the interval between the response and this
	// line. `decided` preserves the real winner; if it was the window, the
	// body is closed and the bound is reported rather than handing the caller
	// an already-doomed reader.
	wrapped := &windowedBody{ReadCloser: resp.Body, win: win, stop: stopWindow, release: release}
	bodyMu.Lock()
	body = wrapped
	bodyMu.Unlock()
	if outcome, ok := win.decided(); ok && outcome.cause == causeExchangeWindow {
		_ = wrapped.Close()
		return nil, outcome.err()
	}
	resp.Body = wrapped
	return resp, nil
}

// HandoffStream disarms the exchange window on a response the caller has
// decided to commit: a confirmed text/event-stream answer, or a body it will
// relay verbatim. After it returns, the body runs under the caller's own
// context alone and may outlive the envelope without limit — a long generation
// is bounded by the stream recovery window, which measures upstream silence,
// and never by the candidate's absolute attempt window.
//
// It is a no-op on a response that never went through DialWithin (nothing is
// armed to release) and on a response already handed off, so a caller may
// reach the same branch twice without consequence. That is what lets the
// decision live where the response's nature is actually known — after the
// headers — instead of being guessed from the request's own stream flag, which
// is not the same fact: a streaming request is answered with buffered JSON
// often enough that treating the two as interchangeable silently dropped the
// envelope from a body that was still pre-commitment.
//
// It REPORTS whether a window was found, and a caller that discards the answer
// is discarding the only signal this package can give that a committed stream
// is still carrying a pre-commitment bound. That is not hypothetical: a body
// wrapped deeper than maxBodyUnwrapDepth returns false for the same reason an
// unwrapped body does, and the two are indistinguishable here by design. False
// is therefore the expected answer on a foreign body and must not be treated as
// a failure — but at a commitment point it is worth one line of evidence, since
// the alternative is a stream that truncates at the candidate's max-elapsed with
// nothing in the log to explain it.
func HandoffStream(resp *http.Response) bool {
	if resp == nil {
		return false
	}
	return handoffBody(resp.Body)
}

// maxBodyUnwrapDepth bounds the search for the windowed body a committed
// answer is wrapped in. It is a backstop against a wrapper cycle, not a
// statement about how many wrappers there are: the count is owned by
// composition, and a new one stacked on a committed answer must not need this
// constant raised. Three is the depth observed in production today — the
// continuation hop's boundBody over the pool's releaseBody over the windowed
// body DialWithin installed.
//
// Exhausting the bound is NOT a safety property and must never be read as one.
// It reports "no window found", which is indistinguishable from a genuinely
// unwrapped body, and the consequence of the miss is silent: a committed
// stream keeps a pre-commitment window and gets cut at the candidate's
// max-elapsed. The bound only converts a guaranteed-correct walk into a
// bounded one; it cannot make a miss loud. That is why the depth arm of the
// invariant is pinned by a test rather than by this constant, and why
// HandoffStream's caller is expected to treat a false answer as a thing to
// notice.
const maxBodyUnwrapDepth = 8

// handoffBody finds the exchange window under whatever wrappers the body
// carries and disarms it, reporting whether one was there.
//
// The search UNWRAPS rather than checking a fixed depth. Every layer here is
// a decorator that preserves the body it wraps, and each exposes Unwrap for
// exactly that reason; a caller that stacked its own layer on a committed
// answer — the continuation hop binds the dial's context to its body, so the
// body it relays is one wrapper above whatever the transport installed — would
// otherwise have its committed stream cut at the candidate's max-elapsed, the
// precise regression a one-layer lookup invites. A missed lookup is silent,
// so the invariant it protects is pinned by a test that asserts the window
// really was disarmed through the full stack.
func handoffBody(body io.ReadCloser) bool {
	for depth := 0; depth < maxBodyUnwrapDepth; depth++ {
		if w, ok := body.(*windowedBody); ok {
			w.handoff()
			return true
		}
		u, ok := body.(interface{ Unwrap() io.ReadCloser })
		if !ok {
			return false
		}
		inner := u.Unwrap()
		// A wrapper that returns itself would otherwise spin to the depth
		// bound; treating it as the end of the chain costs nothing and keeps
		// the walk total.
		if inner == nil || inner == body {
			return false
		}
		body = inner
	}
	return false
}

// windowedBody is one exchange's body and everything release owes it: the
// window's stop, the derived context, and the caller's cancellation watch. It
// is the whole of the two-phase handoff in one type — holding the stop is what
// tells HandoffStream which responses have a window to disarm at all.
//
// The Read translation is the one piece of ownership it adds. A window that
// ends while the caller is blocked in Body.Read must surface as the exchange
// timeout, while an endpoint error and a caller cancellation must keep their
// original owners — and `net/http` usually returns a bare context.Canceled
// after its request context ends, so that distinction is impossible from
// Read's error alone. The dialRace already owns the decisive fact, and this is
// the one place it can be read at the moment it matters.
type windowedBody struct {
	io.ReadCloser
	win     *dialRace
	stop    func() bool
	release func()
	// handoffOnce guards the disarm so a second handoff — or a handoff racing
	// the body's own Close — cannot stop the same timer twice or release the
	// exchange early.
	handoffOnce sync.Once
}

// handoff disarms the window and lets the wrapper step aside. The stop's
// RESULT is deliberately not read: the caller can only reach here after the
// dial returned, and a timer that has not already been decided against the
// response by the publish check in DialWithin can only fire later — at which
// point it would close a body the caller has committed to and nothing else.
// Its one job is to guarantee no later instant fires.
func (b *windowedBody) handoff() {
	b.handoffOnce.Do(func() {
		b.stop()
	})
}

// Unwrap lets a wrapper that was stacked on top — the pool's permit holder —
// reach the exchange window underneath without either layer knowing what the
// other is. It is the one concession to composition here, and it is what lets
// HandoffStream work identically on a direct and a pooled answer.
func (b *windowedBody) Unwrap() io.ReadCloser { return b.ReadCloser }

func (b *windowedBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	if err == nil {
		return n, nil
	}
	if outcome, ok := b.win.decided(); ok && outcome.cause == causeExchangeWindow {
		// The window owns this failed read: its callback closed the body
		// specifically to wake it, and the error net/http then reports is a
		// bare context.Canceled that says nothing about who closed it. The
		// bytes the peer DID send are still returned with n intact — a
		// partial read is never discarded, only the error is reattributed —
		// so a caller that asked for the envelope's own answer gets it on the
		// same read that delivered the last bytes before it.
		//
		// This is the window's decision, not a classification of the error: a
		// genuine endpoint failure never wins the race, so it is never
		// relabelled here.
		return n, outcome.err()
	}
	return n, err
}

func (b *windowedBody) Close() error {
	err := b.ReadCloser.Close()
	b.release()
	return err
}

// The closed set of causes a pre-response dial can end for that are not the
// endpoint's own fault. Typed, never error text, because the recovery matrix
// and the evidence vocabulary both key on them.
const (
	// causeExchangeWindow — the request's exchange envelope elapsed before a
	// response arrived. This proxy's own bound.
	causeExchangeWindow = "exchange_window"
	// causeCaller — the caller's own context ended the dial. The client's
	// event, never a provider's.
	causeCaller = "caller"
)

// dialOutcome is one decided race between the two pre-response causes. It is a
// value rather than a bare string so the caller reads the finished error, and
// the window, from one place that owns both.
type dialOutcome struct {
	cause string
	// elapsed is the window the exchange was granted, for the envelope
	// outcome's evidence. It is never parsed by any decision.
	elapsed time.Duration
}

// err renders the outcome as the typed error the rest of the service reads:
// the envelope's window is a timeout-shaped, fallback-eligible, send-unknown
// failure against the endpoint, and the caller's is a client disconnect. The
// two are never the same value, and neither is ever derived from error text.
func (o dialOutcome) err() error {
	if o.cause == causeExchangeWindow {
		return &ExchangeTimeoutError{Elapsed: o.elapsed}
	}
	return &ExchangeDeadlineError{cause: context.Canceled}
}

// dialRace decides which of two independent causes ended a pre-response dial:
// the exchange's window, or the caller's own context. It is what makes "which
// one fired" answerable at the instant the outcome is decided, from one
// critical section, rather than re-derived from a context's error afterwards —
// where both causes look identical and the endpoint's own failure is
// indistinguishable from a proxy-imposed bound.
type dialRace struct {
	mu     sync.Mutex
	cause  string
	window time.Duration
	done   bool
}

// claim records the cause as the winner if none is recorded yet, and returns
// the outcome. A losing claim still returns the OUTCOME, not itself, so a
// caller that asks a decided race for the answer gets the answer that was
// decided: the window cannot be reported as the caller's, or the reverse,
// just because the losing side asked last.
func (d *dialRace) claim(cause string) dialOutcome {
	d.mu.Lock()
	defer d.mu.Unlock()
	if !d.done {
		d.done = true
		d.cause = cause
	}
	return dialOutcome{cause: d.cause, elapsed: d.window}
}

// decided reports the finished outcome once the exchange has ended it. ok is
// false while the dial is still live, which is the caller's signal that a
// failure is the endpoint's own and must travel up untouched.
func (d *dialRace) decided() (dialOutcome, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return dialOutcome{cause: d.cause, elapsed: d.window}, d.done
}

// ExchangeBudget is the per-request ceiling on real outbound exchanges,
// claimed immediately before a dial. It is declared HERE, on the consumer
// side: the transport layer performs the dials, so the transport layer
// spends the units, and the pool takes one claim per dialed endpoint —
// fallback attempts included, skipped members excluded. The producer
// satisfies this interface structurally; this package never imports it.
//
// Consuming at the dial — not at the handler — is what makes the count
// honest: a handler-side count would miss the exchanges a pool's fallback
// adds to one candidate attempt.
//
// A SEPARATE method to ask how much time the envelope has left would break
// the one thing this seam is for. "Am I authorized to dial" and "how long may
// the dial take" are two halves of a single decision about a budget that
// moves, and answering them in two calls lets a claim be granted under a
// reading of the budget that no longer vouches for it — the counter moved,
// and the exchange then ran on a deadline the envelope had already given up.
// One method, one decision, one answer, is the only shape that makes the
// invariant hold: every increment of the request's exchange count
// corresponds to an exchange this caller is authorized to start.
type ExchangeBudget interface {
	// AcquireExchange atomically claims one unit for an exchange that is
	// about to start AND reports the time that exchange is allowed to take.
	// A non-granted result means the request has spent everything it may
	// spend and the caller MUST NOT dial — the grant, the counter increment
	// and the window are all decided under one lock, so a granted exchange
	// is always one the envelope vouched for at the instant it was claimed.
	AcquireExchange() Exchange
}

// Exchange is one atomic acquisition from an ExchangeBudget: a claim that
// either funded an exchange or refused to. It is the transport's declaration
// of the shape the budget publishes, and the two packages name the SAME shape
// rather than each declaring a struct of its own.
//
// That sameness is load-bearing and the reason is not stylistic. A method that
// returned a producer-local type would satisfy this interface structurally
// while handing the consumer a value it cannot read — the seam would compile
// and fail at the call, and the whole point of declaring the interface on the
// consumer side is that the two packages stay independent, not that the shape
// is duplicated. So the shape is declared here, the producer's own type is an
// ALIAS of it, and the producer's package still imports nothing from this
// one. internal/credential/guards_test.go keeps both directions honest: it
// fails when a guard cannot parse its target rather than when a package is
// merely a member of it, so a new import cannot slip past the check that was
// written to catch exactly this.
type Exchange struct {
	// Granted reports whether the exchange may start. A false value means the
	// caller MUST NOT dial, and Window is meaningless.
	Granted bool
	// Window is how long an exchange starting now may take before the
	// envelopes make it over. It is a remaining DURATION, anchored by the
	// caller on the machine's wall clock.
	//
	// It is a remaining time, never an idle bound. `max-elapsed` is what
	// bounds an exchange that answered nothing; a stream that IS answering
	// is bounded by the stream recovery window instead, and a returned body
	// is never measured against this. A spent window is reported as
	// non-positive, never clamped: clamping it would hand the transport a
	// fresh window and turn a refused exchange into one more dial.
	Window time.Duration
}

// ExchangeTimeoutError reports that the request's own exchange envelope
// (`recovery` budget max-elapsed) ended a dial that had not yet produced a
// response. It is this proxy's bound, not the endpoint's and not the caller's
// — and it is typed precisely so that it can be told apart from both.
//
// The distinction is load-bearing, not cosmetic:
//
//   - it is NOT the caller's cancellation. The dial ran on a context derived
//     with context.WithoutCancel, so the caller's own cancellation can reach
//     it only through the explicit stop path DialWithin wires to the caller's
//     Done channel, never through the window's timer. Reading it as a caller
//     deadline would make the walk report caller_deadline_exceeded —
//     terminal, no fallback, no envelope — for something the operator's own
//     configuration cut off.
//   - it IS a timeout against the endpoint, and therefore fallback-eligible:
//     the peer accepted the connection and sent no status line, the one
//     provably-unsafe-to-replay shape the matrix is there to decide. So
//     Classify reports ClassTimeout with SendStateUnknown, and the recovery
//     matrix — not this package — decides whether the walk moves.
//
// One consequence of SendStateUnknown is worth stating plainly, because the
// configuration implies otherwise. Fallback-eligible means eligible for the
// matrix's PROVIDER fallback, which moves the request to the next candidate. It
// does NOT mean the egress pool will try another member: a pool refuses its own
// fallback on a send-unknown failure, precisely because the request may already
// have reached the member and re-sending it could duplicate it. So a candidate
// reached through a multi-member pool can report this failure after ONE member
// even though the pool has more, and an operator who configures egress members
// for redundancy does not get member-level redundancy against a bound this
// proxy imposed. That is the conservative direction — a duplicate upstream
// request is worse than a failed one — but it is a real limit on what a pool
// buys here, not an accident of scheduling.
//
// A budget refusal that happens BEFORE any dial is not this error: nothing was
// dialed, no endpoint is to blame, and the pool reports it as
// AttemptInfo.BudgetExhausted with a nil error.
type ExchangeTimeoutError struct {
	// Elapsed is the window the envelope granted this exchange, measured on
	// the budget's own clock. It is reported for evidence only and is never
	// parsed: no decision anywhere in this service reads a duration out of an
	// error.
	Elapsed time.Duration
}

func (e *ExchangeTimeoutError) Error() string {
	return "transport: exchange envelope elapsed before a response"
}

// ExchangeDeadlineError reports that the CALLER's own context ended a dial
// that had not yet produced a response — the client hung up, or the caller's
// deadline passed.
//
// It is a distinct type from ExchangeTimeoutError on purpose. The envelope's
// elapsed half is a bound this proxy imposes on its own outbound traffic; the
// caller's deadline belongs to the client and ends the whole request. The two
// are different facts with different owners, and collapsing them would let a
// proxy-owned bound be reported as a client event (or the reverse) — which is
// the one classification the recovery matrix must never be handed.
//
// It is not an endpoint failure: no member is struck and no fallback is
// attempted, and the handler reports the request as a client disconnect. The
// unwrapped cause keeps the caller's own sentinel reachable through
// errors.Is/As for the owner check that does read the chain.
type ExchangeDeadlineError struct {
	cause error
}

func (e *ExchangeDeadlineError) Error() string {
	// Static wording only: the cause's text can carry a target address, and
	// this value reaches log lines. The owner — the caller — is in the type.
	return "transport: the caller's context ended the exchange"
}

func (e *ExchangeDeadlineError) Unwrap() error { return e.cause }
