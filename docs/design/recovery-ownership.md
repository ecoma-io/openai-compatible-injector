# Design note — Injector recovery-ownership alignment (issue #58)

Phase 1 reconnaissance of `v0.9.0` (`bae6e6d`). Facts about the three-layer
contract and the exact code paths this change touches. No code changes here —
this file is the Phase 1 exit deliverable and the plan the PR follows.

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

## Defects to fix (smallest coherent change)

### 1. Silently inert `fallback` at override layers (recovers scope honesty)

- Reach gate: engine.go `canEnter` reads `e.primary.Fallback` only
  (engine.go:150-158); `EnterCandidate` sets `e.policy` but never steers
  reach from it (engine.go:136-144).
- Config: `buildRecoveryOverride` (config/recovery.go:219) accepts
  `fallback.{enabled,max-candidates,on-exhausted}` at provider and candidate
  positions; `Merge` folds it (merge.go:128-144); `Validate` and `Hash` both
  pass it (policy.go:207-218, hash.go:72-74). Result: the file parses, the
  snapshot freezes, the walk ignores it.
- Only the GLOBAL and MODEL layers can reach the primary policy, so `fallback`
  is genuine there.
- **Fix:** reject `fallback.*` in `buildRecoveryOverride` (all override
  positions: provider, model, candidate) — fail-loud at load/reload, never
  silent. `matrix`, `retries`, `retry-after`, `budget.candidate` stay legal at
  every layer (they are genuinely candidate-scoped).
- Keeps `fallback` legal at the global layer AND on a model whose primary is
  candidate 0 — both steer the walk. A model-level override cannot affect the
  chain reach of a DIFFERENT model, which is correct.

### 2. Pooled-path `upstream_request_started` fires after `Execute`

- Direct: logged at handler.go:1013-1020 before `d.Do(req)` at :1021 — OK.
- Pooled: `ex.Execute` runs (all dials) at :915, event at :936-943 — AFTER the
  operation it names, violating the `*_started`-before-operation contract.
- **Fix:** emit one `provider_attempt_started` before the first dial on BOTH
  paths (replaces the pooled path's late event; direct path's existing event
  moves to the same moment/name). The pool's own per-dial events land where
  they land (they are per-exchange evidence, named `egress_attempt_failed`).

### 3. No `failure_origin` field (recovery-ownership observability)

- `error_class`/`error_cause` imply the layer today; the axis is never
  explicit. Add a closed-set `failure_origin` (`upstream_http`, `transport`,
  `protocol`, `caller`, `envelope` — budget refusal) carried by evidence
  events, so upstream-HTTP vs transport vs caller vs protocol is
  distinguishable at a glance. No new classification required.

## NOT changed (contract preservation)

- `internal/recovery` stays free of transport/HTTP/IP/proxy/RPGW knowledge —
  mapping happens in `internal/proxy` (`transportClassOf`, handler.go:1729).
- `ActionRetry`/`ActionFallback`/`ActionTerminal` unchanged (action.go).
- No new retry loop anywhere; no HTTP retry logic moved into transport.
- Request snapshot remains immutable; frozen policies, budgets, and
  `policy_hash` semantics unchanged.

## Test surface

- `internal/config` — rejection of provider/candidate/model override
  `fallback`; acceptance of global/model `fallback`; mutual-exclusion messages
  unchanged.
- `internal/proxy` — `provider_attempt_started` precedes every dial (buffered
  and pooled); `failure_origin` present on evidence events with correct values.
- `internal/recovery` — unchanged (no engine semantics changed).
- e2e — extend `logging_test.go` lifecycle checks for the new slug/field;
  `recovery_test.go` counters unchanged.
