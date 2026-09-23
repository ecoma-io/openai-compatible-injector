# Design note — Injector recovery-ownership alignment (issue #58)

Phase 1 reconnaissance of `v0.9.0` (`bae6e6d`), the defects it found, and the
changes this branch made. Line numbers refer to the pre-change tree.

## Architecture being aligned

```text
Injector = provider/model recovery authority
OFP      = OpenCode relay, no application-level retry
RPGW     = shared egress infrastructure
```

The injector already satisfies the core of this: it is the ONLY provider-level
retry authority, HTTP statuses are answers that reach the recovery matrix
and are never turned into lower-layer retries, and the egress pool dials the
wire. The gaps below are scoping/observability defects, not missing retry
loops.

## Request path (facts, `internal/proxy/handler.go`)

1. `serve` loads one snapshot (`store.Load()`, handler.go:247) — every later
   step binds to it, reloads never reshape in-flight work.
2. Auth gate → body read → `inject.Probe` → `snap.Model(model)` (404 if
   absent).
3. `recovery.NewEngine(r.Context(), m.Recovery, ...)` (handler.go:630) — the
   walk's primary policy is `Model.Recovery`, which mirrors `Chain[0]`.
4. Walk: `for i := range m.Chain`; `eng.EnterCandidate(cand.Recovery)`
   (handler.go:767); transform; build outbound request; straight
   `Do` (direct, handler.go:1021) or `ex.Execute(...)` (pooled, handler.go:915).
5. Transport failure → `transport.ClassifyAttempt` →
   `eng.Observe(Observation{Class: FailureTransport,...})` (handler.go:1040-1077).
6. HTTP answer (any status) → capture (4xx/5xx) or body-validate (2xx) →
   `eng.Observe(...)` (handler.go:1199-1225, 1341-1350, 1390-1399).
7. Decision executes: `ActionRetry` waits bounded delay and `continue`s the
   inner attempt loop; `ActionFallback` breaks to the next candidate;
   `ActionTerminal` breaks the walk. `walkAction` folds a fallback with no
   remaining candidate into terminal (handler.go:1714).
8. Commit → relay (SSE/verbatim/buffered/normalized-error) → `complete()`.

## Ownership boundaries that already hold (keep)

- **HTTP statuses stay answers.** `transport` returns `err==nil` with
  `StatusCode` set for 429/4xx/5xx; only network-level failures are errors
  (transport.go:9-19, pool.go:260-265); the pool's attempt loop ends on the
  first response regardless of status (pool.go:360-370). Recovery's matrix
  decides the rest via `Observation{Class: FailureHTTP}`.
- **No lower-layer retry on statuses.** The pool never status-retries, never
  interprets `Retry-After` (that is `internal/proxy/retryafter.go`, consumed
  by recovery). No provider-policy knowledge in `internal/transport`.
- **Commitment.** The walk completes before the first client byte; SSE
  commitment holds by construction (handler.go:1314); committed = terminal in
  the engine (engine.go:175).
- **Caller cancellation/deadline is terminal**, judged from the request
  context (`ClassifyAttempt` + `FailureCaller`), never from error text.
- **Budgets are code-owned and claimed at the dial** (`ExchangeBudget` seam;
  budget.go:88; pool.go:333 claims immediately before the dial, after every
  gate). `budget.request` is request-scoped and only legal at the global
  layer.

## Defects fixed (smallest coherent change)

### 1. `fallback` accepted at layers that cannot mean what it says

- Reach gate: engine.go `canEnter` reads `e.primary.Fallback` only
  (engine.go:150-158); `EnterCandidate` sets `e.policy` but never steers
  reach from it (engine.go:136-144). `e.primary` is the policy the handler
  passes to `NewEngine`, i.e. `Model.Recovery` = `Chain[0].Recovery`.
- Config accepted `fallback.*` at provider and candidate positions, where
  `Merge`, `Validate` and `Hash` all passed it. That is misleading in **both**
  directions, which is why the fix is a rejection rather than a narrowing:
  - on a non-primary candidate (or a provider that is not the primary's) it
    merged, validated, hashed, and then did nothing at all;
  - on the primary's own provider — or on candidate 1 — it genuinely steered
    (`resolveCandidateRecovery` folds the provider layer into that candidate's
    policy, runtime.go:1146), but only while that provider stayed the primary:
    the same provider entry used as a fallback for another model would be
    inert there. One block, two behaviours, decided by a chain position the
    operator cannot see from the provider entry.
- **Fix:** reject `fallback.*` in `buildRecoveryOverride`, which is the
  call-in for provider and candidate positions. `matrix`, `retries`,
  `retry-after`, `budget.candidate` stay legal at every layer (they are
  genuinely candidate-scoped). `buildModelRecovery` deliberately calls
  `buildRecoveryPartial` instead, so the model layer keeps `fallback` — a
  model's walk is a model's property.
- **This is a breaking config change** for a file that stated `fallback` on a
  provider entry or candidate 1: `LoadRuntime` now fails, so boot exits 1
  (a reload keeps last-known-good). It is shipped as `feat(config)!` for that
  reason, and the README's four-position contract states the restriction.

### 2. Pooled-path `upstream_request_started` fires after `Execute`

- Direct: logged at handler.go:1013-1020 before `d.Do(req)` at :1021 — OK.
- Pooled: `ex.Execute` ran (all dials) at :915, event at :936-943 — AFTER the
  operation it names, violating the `*_started`-before-operation contract.
- **Fix:** emit one `provider_attempt_started` before the first dial on BOTH
  paths (replaces the pooled path's late event; the direct path's existing
  event moves to the same moment/name). The pool's own per-dial events land
  where they land (they are per-exchange evidence, named
  `egress_attempt_failed`). The marker is emitted before the dial, so an
  attempt an envelope refusal never realizes leaves a marker whose index no
  later record reaches; the README says so.

### 3. No `failure_origin` field (recovery-ownership observability)

- `error_class`/`error_cause` imply the layer today; the axis is never
  explicit. Added a closed-set `failure_origin` (`upstream_http`, `transport`,
  `protocol`, `caller`, `envelope` — budget refusal) carried by evidence
  events, so upstream-HTTP vs transport vs caller vs protocol is
  distinguishable at a glance. No new classification required.

### 4. No send state on transport failures (replay safety below the injector)

- A `Do` error means no response came back — it does not mean the request
  never left. The pool's fallback (pool.go:377-393) struck health and dialed
  the next member on ANY non-caller error, a **post-connect timeout
  included**: the upstream may already be processing a request whose answer
  never arrived, and the next member would silently duplicate it.
- **Fix:** classify every transport failure with a send state and make the
  pool fall back only on `definitely_not_sent`. A `send_unknown` failure
  stops the attempt loop without a strike and travels up to the injector's
  recovery policy, the only layer that owns replay.
- The state is derived from the failing **wire operation**, not from the
  class. The first cut of this change mapped `ClassConnection` wholesale to
  `definitely_not_sent`; adversarial review (see below) showed that bucket is
  the classifier's catch-all and reaches failures on an ESTABLISHED
  connection, which is exactly the wrong answer:
  - `http.Client.Do` on a server that reads the whole body and closes returns
    `*url.Error{Err: io.EOF}` — no op at all;
  - a reset after the body returns `*net.OpError{Op: "read"}`;
  - a partial answer returns `net/http: HTTP/1.1 transport connection broken:
unexpected EOF` — partial bytes are positive proof of delivery.
    The same review showed the mirror defect: a blackholed egress path times
    out in the **dial** op, so the class is `timeout` while nothing was ever
    sent — the old mapping refused to fall back there, turning a dead egress
    path into a client-visible 502 and freezing the member's health at zero
    strikes forever.
- The rule now reads the op: a `dial`/`proxyconnect` op, a typed proxy-tunnel
  failure, a TLS handshake failure, or a bare refused syscall prove no request
  byte left; a `read`/`write` op, an error with no op to read, and everything
  else are `send_unknown` (the zero value, and the only defensible answer
  without evidence). `internal/transport/sendstate_test.go` drives the real
  HTTP stack to pin all five shapes.
- The state rides `AttemptFailure` into `egress_attempt_failed`, is derived
  again per attempt for `provider_attempt_failed`, and is reported by the
  post-walk exhaustion `upstream_request_failed` — but only when a dial
  actually happened. A zero-dial pool exhaustion carries no `send_state`: the
  sentinel that reports it is a pool-level condition, not an endpoint's
  failure, and attaching a wire state to a dial that never happened is
  evidence about nothing.
- Residual boundary, stated rather than assumed: a TLS-handshake _timeout_ is
  a plain Go error with no typed marker and no wire op, so classification by
  type (never by text) cannot prove it pre-send and it reads
  `send_unknown`. It could not fall back; the conservative direction is the
  safe one. Likewise `internal/transport` cannot distinguish a server that
  read the request and died from one that never accepted it _if_ the error
  shape is a bare EOF — hence the conservative default.
- The e2e suite had the old behavior written into it, which is how the
  boundary got tested rather than assumed. Its `deadEgress` stub accepted
  every TCP connection and closed it at once, and three tests
  (`TestEgressPoolMaxAttemptsCapsDistinctDials`,
  `TestEgressPoolHealthCooldownSkipsAndRecovers`,
  `TestEgressLogsNeverCarryProxyCredentials`) configured that stub as an
  `http://` member and required the pool to fall back off it. Under the new
  rule that stub is an ESTABLISHED connection dying with no answer — the
  request had already been written into the member — so it is send-unknown
  and no longer fallback-eligible. The stub is unchanged; its member URL is
  now `socks5://`, which moves the same fault into the TUNNEL phase, where
  the failure is typed (`ProxyConnectError`) and provably precedes any
  request byte. That is not a workaround: it is the difference between "the
  hop never formed" (recoverable below the injector) and "the hop took the
  request and died" (the injector's call), and the suite now exercises the
  first. An operator with an `http://` proxy member that accepts and dies
  gets the injector's policy instead of a silent pool replay — the same
  behavior change, one layer up.

### 5. Lease leak on the budget-refusal return (pre-existing, found in review)

- `pool.go` returned `nil, info, nil` from the zero-dial budget refusal
  without calling `st.end()`, so the lease taken at `st.begin()` was never
  released. A state that never returns to zero can never satisfy the
  registry's retire condition, so an evicted generation's teardown never
  fired and its members' idle connections stayed open. Pre-existing on `main`;
  fixed here because this branch's review surfaced it and the fix is one call.

## Reviewed and deliberately not changed

- **The walk still replays a `send_unknown` failure — at the provider layer,
  by policy.** `defaults.go` maps every transport class and cause to
  `ActionFallback`, so a chain whose candidates share an upstream base-url
  re-sends the request to the same upstream over a different egress. That is
  the documented division of labour (`below the injector` is the pool, the
  injector IS the replay authority, and the operator's matrix is where the
  decision belongs), but it means the pool's guarantee is scoped to one
  provider path. The default policy's comment and the README say so; a
  deployment that wants otherwise narrows its own matrix.
- **`fallback.on-exhausted` is accepted and inert.** `Validate` permits only
  `terminal`, which is `Action`'s zero value, and nothing reads
  `Fallback.OnExhausted` (the engine reads `Retry.OnExhausted`). It cannot
  mislead in a harmful direction — the only legal value is the do-nothing
  one — so removing it would be an unrelated breaking config change.
- **`upstream_request_build_failed` carries no `failure_origin`.** A local
  request-build failure is not one of the five layers the closed set names,
  and inventing a token for it would widen a vocabulary to describe a bug in
  this process rather than a failure somewhere on the wire.

## NOT changed (contract preservation)

- `internal/recovery` gains no transport/proxy/IP/RPGW/provider knowledge —
  mapping happens in `internal/proxy` (`transportClassOf`, handler.go:1729).
  (It does import `net/http`, for `http.ParseTime` in `retryafter.go`, so an
  upstream `Retry-After` can be read: pre-existing, and HTTP-date parsing is a
  date format, not transport coupling. It knows nothing about sockets,
  proxies, pools or addresses.)
- `ActionRetry`/`ActionFallback`/`ActionTerminal` unchanged (action.go).
- No new retry loop anywhere; no HTTP retry logic moved into transport; the
  transport still owns only paths and bytes.
- Request snapshot remains immutable; frozen policies, budgets, and
  `policy_hash` semantics unchanged.

## Test surface

- `internal/config` — rejection of provider/candidate override `fallback`;
  acceptance of global/model `fallback`; mutual-exclusion messages unchanged.
- `internal/proxy` — `provider_attempt_started` precedes every dial (buffered
  and pooled); `failure_origin` present on evidence events with correct
  values; `send_state` agrees across `egress_attempt_failed`,
  `provider_attempt_failed` and the exhaustion `upstream_request_failed`, and
  is absent when the pool dialed nothing.
- `internal/transport` — `ClassifyAttempt` send-state pinned per real wire
  shape (`sendstate_test.go`, against the real HTTP stack) and per synthetic
  cause token; a `send_unknown` failure dials exactly one member and strikes
  no health, while a refused dial and a dial timeout both fall back; the
  zero-dial budget refusal releases its lease.
- `internal/recovery` — unchanged (no engine semantics changed).
- e2e — `logging_test.go` lifecycle checks cover the new slug; the pool
  lifecycle tests exercise the fallback boundary through a tunnel-phase
  failure rather than an HTTP member that dies after the request was written
  (see Defect 4); `recovery_test.go` counters unchanged.
