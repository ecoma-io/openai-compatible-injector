# AGENTS.md — openai-compatible-injector

Working guidance for AI-assisted development in this repository. This file
carries the decisions that make the codebase self-consistent; the behavior
contract lives in README.md, and where the two disagree this file is wrong —
fix it, or the code, decide which.

## The service

An OpenAI-compatible request/response injector proxy. Clients talk to this
service as if it were an OpenAI API endpoint; it forwards to configured
upstream providers, renaming the model and injecting a per-model system
prompt into every request. Two API surfaces: Chat Completions
(`/v1/chat/completions`) and Responses (`/v1/responses`), both with SSE
streaming passthrough.

Owned decomposition:

| Directory                        | Owns                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| -------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `internal/config`                | Bootstrap env parsing (`LoadBootstrap`), runtime YAML (`LoadRuntime`, strict decode via `yaml.v3` known fields, required client `api-key`, log level via `ParseLogLevel`), providers/transports tables incl. the pool schema (`buildModel`/`buildProviders`/`buildTransports` two-pass + pool policy builders, no-echo rejections, duplicate resolved-endpoint member rejection via `endpointKey`, RFC 1929 credential bound in `parseProxyURL`), egress-closure snapshot retention over model CHAINS (every candidate's transport), provider candidate chains (`runtimeModelCandidate`/`buildChain`), the runtime `recovery` block's config surface (`recovery.go`: one shape accepted in four positions, shorthand-to-canonical-rule expansion with typed predicate parsing, per-layer `buildGlobalRecovery`/`buildRecoveryOverride`/`buildModelRecovery` carrying the layer-scoped mutual-exclusion rejections, the legacy `retries`/`provider-fallback` spellings normalized into the same engine rather than run beside it, and `resolveCandidateRecovery` folding the layers in the fixed global→provider→model→candidate order and freezing one effective `recovery.Policy` onto every `Candidate` — the model's `Recovery` is `Chain[0]`'s), snapshot store (`Store`/`Snapshot`, atomic pointer), content-hash poller (`Poller`, `onPublish` hook) |
| `internal/inject`                | Pure request/response transforms: `Probe` (model + stream detection), `Chat`, `Responses`, `RewriteChatModel`/`RewriteResponsesModel` (byte-preserving, API-scoped), thinking plan + usage synthesizers (`ThinkingPlanFor`, `SynthesizeChat/ResponsesThinkingUsage`)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| `internal/auth`                  | Client identity: `Principal`/`Reason`/`Authenticator`/`Provider` seam, `StaticProvider` (snapshot-bound shared key, padded constant-time compare), `PartnerProvider` (store-backed, bounded positive/negative decision cache — errors never cached), crypto-random token/keyID minting + SHA-256-at-rest hashing, PostgreSQL key store (`PGStore`: lookup/create/list/revoke, off-path batched `last_used_at` flusher)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| `internal/migrate`               | Shared SQL-first module-scoped migration runner: embedded-set parsing, legacy auth-ledger adoption, advisory-lock serialization, `information_schema` column-contract validation                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| `internal/usage`                 | Factual upstream usage capture (pre-rewrite), durable PostgreSQL event repository and reporting query seam, bounded asynchronous batch pipeline                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| `internal/transport`             | Outbound paths: `Doer`/`Executor`/`Resolver` seams, direct client (cloned default transport tuning), proxy client (`http.ProxyURL` or hand-rolled SOCKS5 dialer preserving socks5-vs-socks5h DNS semantics), typed proxy errors, the typed local `RequestBuildError` (a request this process could not construct — static wording, the cause reachable but never printed), `Classify` and the context-aware `ClassifyAttempt` (`Failure`: canonical class, closed-set cause, caller-terminated flag, send state), egress pool runtime (`pool.go`: eligibility, scheduling, bounded provably-unsent fallback — a `send_unknown` failure stops the loop rather than replay, health, permits, leases), `Registry` (content-keyed clients + per-identity pool state, retained on publish), the exchange-budget seam (`ExchangeBudget` declared HERE, on the consumer side — the transport performs the dials so the transport spends the units; the pool claims one per dialed endpoint after every eligibility, health and concurrency gate and immediately before the dial, skipped members claiming none, and a refused claim stops the loop without blaming an endpoint — and the producer satisfies it structurally, neither package importing the other for it) — the package knows nothing about providers or policy, only about paths and bytes        |
| `internal/recovery`              | The provider recovery policy domain, deliberately free of I/O and of HTTP: typed `Failure` classes with closed-set cause tokens, typed allow-listed `Match` predicates and the three `Action`s (zero value `terminal`), the failure→action matrix with canonical rule IDs, specificity-derived precedence and equal-specificity overlap rejection (`matrix.go`), the shipped default policy (`defaults.go`), the layer merge (`merge.go`: pointer-typed `Partial`s — scalars replace, maps deep-merge, same-ID rule replaces, new ID appends, unstated inherits) and `Resolve` (validate after every layer, so no layer can smuggle a broken policy past the merge), the policy's stable data hash (`hash.go`, hand-written field order, never map iteration), and the `Engine` (`engine.go`/`budget.go`/`retryafter.go`/`backoff.go`) turning one `Observation` into one `Decision` under the four code-owned invariants — commitment, caller, envelope, immutability. It never sleeps, dials, writes a response, reads a pool, or logs; the proxy owns execution and the transport owns the wire                                                                                                                                                                                                                                                         |
| `internal/proxy`                 | HTTP handler wiring, client bearer authentication/Authorization stripping (auth delegated to the `auth.Provider` seam — static or partner), error envelopes, SSE copying (`CopySSE`), the provider candidate walk, orchestrated THROUGH the `recovery.Engine` — the handler contributes facts (which candidate, which attempt, what the attempt produced, whether the client is still there and whether a byte already reached it) and executes the returned decision (wait the decision's bounded delay, enter the next candidate, or relay what it holds), never classifying a status itself; the walk may not start at all until the snapshot's frozen policy says so, and no decision is taken outside the engine. Executes upstream calls through the model's resolved `transport.Doer` — the `Executor` (pool) branch handed the request's `recovery.Budget` and its `egress_kind`/`egress_target`/`egress_exhausted` report folded into the evidence — composed response rewriter (`rewriteOut`: model rename + thinking-usage synthesis)                                                                                                                                                                                                                                                                                                           |
| `internal/server`                | Listener lifecycle and graceful shutdown (`Server.Run`)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| `cmd/openai-compatible-injector` | Entrypoint: subcommands `version`, `healthcheck`, `keys create/list/revoke` (partner key lifecycle; list exposes no secrets), default serve                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| `e2e`                            | Black-box tests driving the real binary as a subprocess                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |

## Non-negotiables

- **Two config planes.** Bootstrap settings (`OAICR_LISTEN`,
  `OAICR_CONFIG_FILE`, `OAICR_CONFIG_POLL_INTERVAL`,
  `OAICR_SHUTDOWN_GRACE`, `OAICR_AUTH_DATABASE_URL`,
  `OAICR_USAGE_DATABASE_URL` — every environment variable the service reads
  is `OAICR_`-prefixed; no unprefixed fallback exists) come from the
  environment and
  are enforced by the runtime file's strict decoding: a runtime file
  defining them is rejected. The runtime file must be a single YAML
  document — a `---`-separated second document is a rejection (a decoder
  reading only the first would hide what follows). Runtime model mapping
  (inline `endpoint` or `provider`+`transports`+`providers` tables), the
  required `api-key`, and the hot-reloadable top-level
  `sse-keep-alive` block live in YAML only; the latter defaults to enabled
  at 15s and accepts a duration of at least 1s. The auth database URL is
  infrastructure, not policy: empty = static mode, non-empty = partner
  mode; it never hot-reloads and the DSN is never logged (not even its
  length — it embeds a password). The usage database URL is likewise
  bootstrap-only: empty = metering off (no connection); non-empty = migrated,
  validated PostgreSQL-backed metering, asynchronous after startup; its DSN
  is never logged.
- **The router decides WHICH provider; the transport decides HOW the
  request reaches it.** Each candidate's flattened `transport.Config` comes
  from the request's own snapshot — loaded once per request and immutable
  for its life — and the handler resolves it through the `transport.Resolver`
  seam to execute on the returned `Doer`: the lookup happens per attempt
  (each candidate has its own config), and it is content-keyed, so every
  retry and fallback of the same candidate gets the same long-lived `Doer`
  and its warm pool state for as long as the config is live. No
  provider-specific branches, no global `http.Client` coupling. `direct` is
  the tuned Go stack (ambient env proxies honored, zero value of `Config`);
  `proxy` is exactly ONE
  configured endpoint (`http/https/socks5/socks5h`, explicit port, userinfo
  = proxy auth). `socks5` resolves the upstream hostname locally and
  CONNECTs the IP; `socks5h` sends the hostname (remote DNS) — never
  collapse the two. Upstream HTTP statuses are answers (`StatusCode` set,
  `err == nil`); only network-level failures are errors — the normalized
  upstream-error handling in this package builds on that seam. Bodies are
  never buffered inside
  `Do`. One long-lived pool per distinct transport config (`Registry`,
  content-keyed, `Retain` on publish evicts only dropped configs' idle
  connections). An unknown transport/provider reference, a bad type, or a
  malformed proxy URL is a whole-file rejection — never a silent fallback
  to direct.
- **Egress pools schedule, gate, and fall back — they never retry an
  answer.** `pool` is the third transport kind: an ordered member list of
  direct/proxy refs (never pools) with eligibility gates, scheduling
  (`round_robin` / `weighted_round_robin` — weight steers only the first
  pick), bounded provably-unsent fallback (`fallback.enabled`, default
  true/3, cap 16 — skipped members consume no attempt; only a
  `definitely_not_sent` failure may move to another member, because a
  failure that might have reached the member is `send_unknown` and must not
  be replayed below the layer that owns retry policy, so the pool stops the
  loop without a strike and hands it up), and passive health
  (`health.enabled`/`failure-threshold`/`cooldown`, defaults true/3/30s,
  min 1s; any response resets, and only `definitely_not_sent` failures
  count toward the threshold). Eligibility precedes everything — static
  (streaming gate, `max-body-bytes` vs the outgoing post-injection body,
  both checked BEFORE any dial: a 6 MB request is never a 413 on a 4.5 MB
  relay), then dynamic (health cooldown, concurrency permit) — and only
  then does the scheduler move: a skipped member consumes no scheduler
  turn, no attempt, no strike, so recovery reschedules immediately.
  Weighted scheduling is smooth WRR over the CURRENTLY eligible members
  (weight sums and credit arithmetic count candidates only; unavailable
  members bank no credit, recovered ones re-enter with none, a
  saturated loser's round is undone exactly), and the chosen member's
  permit is acquired inside the selection step under the scheduler lock —
  two concurrent requests can never both observe the same last permit.
  Permitted locks: the scheduler mutex is the only outer lock over a
  member's health/limiter mutexes, and no network I/O happens under any of
  them. The pool owns selection via the `Executor` seam — the handler
  hands the request facts (context, URL, headers, body, probed stream
  flag) and gets one response or one error; a caller cancellation or
  deadline (the attempt context done — ownership is decided by the
  context, never by the error chain, so an expired deadline can never
  masquerade as a provider-local timeout) aborts with no strike, zero
  dials is the exhaustion sentinel answering the canonical
  502 `upstream_unreachable` with `error_class: egress_exhausted`, and any
  HTTP status — 429/5xx included — ends the loop and relays like a
  single-endpoint response. Every dialed-and-failed endpoint appends one
  sanitized `AttemptFailure` (kind, scheme+host, canonical class +
  closed-set cause — never error text, never credentials) that the handler
  relays as WARN `egress_attempt_failed`. Proxy/SOCKS failures are typed
  (`ProxyAuthError`/`ProxyConnectError` wrapping their historical texts;
  classified by type, never message text) for fallback decisions and
  `error_class` tokens; pool state (scheduler cursor, health, permits,
  in-flight leases) is keyed by pool identity in the `Registry` —
  unchanged policy across a reload stays warm, changed policy starts
  fresh, and a leased state outlives its eviction until the last request
  releases it. Retirement is instance-owned: a deferred teardown that
  fires after the same identity was re-added is a verified no-op (the
  registry checks the state INSTANCE under the map key, never just the
  key), and re-adding a retired identity builds a fresh generation, never
  hands back the retired state. The snapshot retains the egress closure
  (transports + pools - every member endpoint, content-deduped). Config
  rejects a pool whose members resolve to duplicate ENDPOINTS (identity =
  canonical resolved endpoint with host case folded, never the YAML name)
  and a SOCKS5 userinfo credential over 255 decoded bytes (RFC 1929 wire
  limit; the dialer re-checks defensively as a typed auth error). Provider
  fallback never happens inside a pool — egress fallback moves a request
  between network paths to the SAME provider; only the handler's
  provider walk (its own non-negotiable below) moves between providers.
  The pool never status-retries either: ANY status is one answer handed up
  to the handler, and only the request's resolved recovery policy (its own
  non-negotiable below) retries or falls back on it. Its one piece of
  provider-agnostic bookkeeping is the exchange budget: the transport
  package declares the `ExchangeBudget` seam on the consumer side and the
  pool claims one unit immediately before each real dial — after every
  eligibility, health and concurrency gate — so a member skipped before
  dialing spends nothing, and a claim that comes back spent ends the
  attempt loop with no dial, no strike and no endpoint blamed. Claiming at
  the dial rather than at the handler is what makes the count measure
  traffic instead of intent: a candidate that fans out across a pool's
  fallback is several exchanges, and each of them pays its own way.
- **Provider recovery is policy DATA resolved per request and frozen for
  that request's life; the bounded walk over the candidate chain is what
  executes it.** Every model carries `Chain` (≥1 `Candidate`:
  providers-table name, endpoint, per-candidate upstream-model, transport)
  and EVERY candidate carries the effective `recovery.Policy` its layer
  chain resolved — global → provider → model → candidate, deep-merged and
  re-validated at each step, so an override states only what changes and
  inherits everything else, and a layer that contradicts the one below it
  is rejected rather than quietly winning. Two members are position-scoped
  and rejected outside their layers, because a block that cannot be
  honoured reads like a setting and is not one: `fallback` (the walk's
  reach, which is a property of the request's CHAIN, not of a hop) is legal
  at the global and model layers and rejected on a provider entry or a
  candidate — on a non-primary hop it could never matter, and on the
  primary's own provider it would steer only until that provider is used as
  a fallback for another model, so neither reading is one an operator can
  hold — while `budget.request` (the whole request's envelope) is legal
  only in the top-level block. What a failure MEANS is that
  policy's matrix, never a table in the handler: typed observations (exact
  status, status class, provider error type/code, transport class and
  cause, protocol cause, caller cause) map onto `retry`/`fallback`/
  `terminal`, written as shorthand (`http.exact`, `http.classes`,
  `transport`, `protocol`, `caller`, `default`) or as explicit rules with
  stable IDs — typed allow-listed predicates only, no expressions,
  scripting or regex, because a policy an operator cannot read off the
  page is one they cannot reason about while paged. `internal/recovery`
  ships the default matrix that reproduces the behavior of earlier
  releases exactly, so absent/null/empty changes no traffic; the block
  SIZES and STEERS the walk, it never gates whether one happens. The
  effective policy is resolved from the request's own snapshot and lives
  exactly as long as the request — a reload mid-walk, mid-wait included,
  cannot reshape in-flight budgets, retries or rows. Precedence is
  deterministic and independent of Go map iteration and of YAML order:
  exact status > provider-error predicate > status class > failure cause >
  failure class, with the shorthand expanded into canonical IDs
  (`http-<status>`, `http-class-<class>`, `transport-cause-<cause>`, …);
  equally-specific rules that overlap are REJECTED at load, so no
  disposition is left to whichever rule the runtime happens to reach
  first.
  Four things stay code-owned, non-configurable, and are applied BEFORE
  the matrix, because a configuration able to weaken them would turn a
  client disconnect into upstream traffic or a committed answer into a
  retried one: a committed response is terminal; a caller cancellation or
  expired deadline is terminal, judged from the request context and never
  from the error chain or its text, so a dead caller never spends the
  budget; the request-wide exchange envelope is absolute; and a resolved
  policy is immutable. The decision's evidence names which of the four
  fired (`committed`, `caller`, `budget-request`, `budget-candidate`)
  beside the rule ID, so an operator can tell "the 429 row said fall back"
  from "the client hung up". The two envelope identities are not
  interchangeable: a spent REQUEST envelope ends the walk, because no
  candidate can start another exchange, while a spent CANDIDATE envelope
  forbids only another exchange on THAT candidate — the walk may still
  fall back, since `EnterCandidate` opens the next candidate's envelope
  fresh, and a per-candidate number must never silently pin a chain the
  operator configured to fall back.
  What the matrix decides, the mechanical policies only size: a retried
  failure waits a bounded backoff before the re-ask (initial, doubling to a
  ceiling, then spread ±jitter), a fallback action moves immediately with
  no inter-candidate wait, and a retryable failure whose candidate budget
  is spent takes the policy's `on-exhausted` action — which is also the
  answer to the one refusal no observation can describe, a transport that
  declined to dial because the candidate's envelope was already spent
  (`Engine.CandidateSpent`), so a per-candidate number can never pin a
  chain the operator configured to fall back while a spent REQUEST
  envelope stays terminal there regardless of the policy. An upstream
  `Retry-After` (delta-seconds or HTTP-date; invalid/negative/past ignored
  silently) can only ever RAISE a wait, and never past the backoff ceiling,
  the retry-after policy's own `max-delay`, the candidate's remaining
  `max-elapsed` window, or the caller's remaining deadline — whichever
  binds first. `max-delay` ceilings the DIRECTIVE, not the schedule: a
  capped directive is then compared with the jittered backoff, so a
  `max-delay` below `backoff.max` shortens how far an upstream can push the
  wait and never shortens a wait the operator's own schedule asked for —
  a policy that ignores the directive has nothing to say about the file's
  backoff. That window is measured from the candidate's FIRST attempt
  and checked BEFORE a wait is scheduled, so a sleep never runs past it and
  a cancel during one aborts with no further attempt. `fallback.enabled:
false` pins the primary candidate while same-candidate retries still
  apply; `fallback.max-candidates` counts candidates ENTERED (the primary
  included), never the candidates the chain lists, and the retry budget is
  enforced PER CANDIDATE, never shared.
  The exchange envelope is the absolute bound, and it counts REAL outbound
  exchanges rather than the intent to make them: `budget.request` wraps
  `budget.candidate` and the tighter of the two binds, one unit is claimed
  by the transport immediately before each dial (its own non-negotiable
  above), so a candidate attempt that fans a pool's fallback across three
  members spends three while a member skipped by an eligibility gate
  spends nothing and a refused claim ends the walk without blaming any
  endpoint. A value above a cap REJECTS the file rather than being clamped,
  because a silent clamp would leave the operator's file saying one thing
  and the process doing another. Transport failure (dial/TLS/proxy/pool
  exhaustion) falls to the next candidate with NO same-candidate retry under
  the shipped default matrix — an operator matrix CAN ask for one, since
  `transport`/`transport-cause-*` rows are matchable and a `retry` action
  there is honored (only a configured matrix can ask for it); a
  malformed/incomplete answer before commitment (unparseable or over-cap
  200 body, error body that fails or stalls its bounded capture) is judged
  by the matrix like any other observation. The legacy `provider`,
  `endpoint`, `retries` and `provider-fallback` spellings normalize into
  this one engine — the legacy `provider`/`endpoint` forms build a
  one-candidate chain, so the handler walks one uniform structure, and
  there is no mode in which a legacy block and its `recovery` counterpart
  run side by side: the config layer rejects the pair. Hard boundaries: the
  walk
  (retries and fallbacks) completes before the first client-visible byte —
  the candidate whose answer produces it has produced THE response, so
  streaming commitment holds by construction, and a committed stream that
  dies mid-flight is truncated, never retried or switched. The LAST
  received HTTP answer wins: its status is preserved over the canonical
  envelope (a later candidate's 429/503 is never collapsed into 502). That
  includes retention: a candidate whose budget ran out on a received answer,
  followed only by candidates failing BEFORE answering, relays that retained
  answer — the answering candidate becomes `final_provider` and
  `provider_exhausted` stays unset (a provider answered);
  `provider_exhausted`/502 `upstream_unreachable` happens only when NO
  candidate ever answered; a capture failure on the final retained answer
  answers `upstream_invalid_response`. Each
  attempt — retries included — replays the immutable client body through
  that candidate's own transform (fresh request, identical transformed
  body per candidate; nothing observed on an earlier attempt feeds the
  next), and a local transform error still answers 400 immediately (a body
  failing one candidate's transform fails all). Chains, the resolved
  policies, and per-candidate transports bind to the request's snapshot,
  and the egress closure covers every candidate's transport. Boundary:
  provider
  retry/fallback = injector (WHICH provider answers); egress recovery =
  transport layer (HOW it is reached) — a pool never status-retries.
  Observability: a decision records `policy_rule_id` — the matrix row that
  decided, or one of the reserved invariant identities — beside the frozen
  policy's `policy_hash` and `policy_generation`, so two requests can be
  told apart as "ran under the same policy" without putting the policy on a
  log line. `provider_attempt` counts one-based **logical** provider-level
  attempts (the candidate attempt; numerically the old candidate index when
  no retry fires) and NO LONGER doubles as the exchange count: a logical
  attempt and an outbound exchange are separate axes, and one logical
  attempt may be several exchanges whenever an egress pool falls back — or
  NONE when the attempt never dialed (every member gated out, an envelope
  refusal before the dial).
  `upstream_exchange` counts the real outbound exchanges, one-based and
  request-wide, and the two differ in BOTH directions — exchanges exceed
  attempts when an attempt fans out across a pool's members, attempts exceed
  exchanges when one dials nothing — which is why they are counted apart.
  `provider_attempts` counts provider-level attempts on both
  the completion record and the usage event, while the outbound-exchange
  total is `upstream_exchanges` on the records and the persisted
  `egress_attempts` column on the usage event — one quantity, two names,
  because the column predates the field. The LOG field `egress_attempts` is
  a third thing again and the narrowest: the relayed candidate's own pool
  report, not the request-wide dial total. `candidates_entered`,
  `candidate_attempts` and `retry_attempts` complete the walk's counters and
  `request_exchange_budget_remaining` reporting the REQUEST envelope's
  headroom — never the tighter of the two, which would read zero for every
  walk that ended because its candidate envelope was spent; on both the
  per-attempt events and the completion record;
  `request_completed` adds `retries_total` and `final_candidate`
  (1-based) and `final_provider` names the relayed candidate;
  `provider_exhausted` on exhaustion (terminal `error_class:
provider_exhausted` over the final cause; a zero-dial pool reports
  `egress_exhausted`/`no_eligible_endpoint`); one WARN
  `provider_attempt_failed` per failed attempt and one `upstream_http_error`
  for EVERY received 4xx/5xx (discarded retry/fallback attempts included)
  — both carrying `candidate_index`, `candidate_attempt`, `retry_index`,
  `disposition` (`retry|fallback|terminal`), `reason` (closed set:
  `http_408`/`http_425`/`http_429`/`http_5xx`, `http_<code>` for every
  other status below 500 — fallback-only and terminal rows alike — the
  transport cause tokens, and
  `upstream_invalid_response`/`upstream_body_timeout`/
  `upstream_body_read_failed` on the unusable-answer events, never on
  `provider_attempt_failed`, which is transport failures only), `elapsed_ms`
  — the evidence and
  body-read/invalid events carry the received status as `upstream_status`
  (a transport failure has no status to carry) — and one WARN
  `egress_attempt_failed` per dialed-and-failed endpoint — pooled or
  direct (`egress_kind`/`egress_target` `direct`, egress attempt 1), so
  both egress shapes emit the same record — and one WARN
  `candidate_exchange_budget_spent` for an exchange the envelope refused
  before a dial (a refusal, never a failed endpoint: no strike, no
  `egress_attempt_failed`, and no `upstream_exchange`, because no exchange
  happened for it to index; `policy_rule_id` is always `budget-candidate`,
  and the record carries the disposition the refusal produced —
  `retries.on-exhausted` still owns whether the walk moves). A refusal by
  the REQUEST envelope never reaches that WARN: it is terminal whatever
  `retries.on-exhausted` says — no policy may buy an exchange the request
  envelope has already refused — so the walk ends there and the post-walk
  `upstream_request_failed` names it under `budget-request`). Failure evidence carries the
  canonical `error_class` plus a closed-set `error_cause` token
  (`connection_refused`, `tls`, `dial`, `network_timeout`,
  `deadline_exceeded`, `proxy_connect`, `proxy_auth`, `proxy_timeout`,
  `caller_canceled`, `caller_deadline_exceeded`, …) mapped from typed
  error shapes only, never message text, a closed-set `failure_origin`
  (`upstream_http`/`transport`/`protocol`/`caller`/`envelope`) naming the
  layer the failure belongs to, and — on transport failures —
  `send_state` (`definitely_not_sent`/`send_unknown`), which is also the
  pool's fallback gate: only a `definitely_not_sent` failure may move the
  request to another egress member, because a `send_unknown` request may
  already have reached the upstream and replaying it would duplicate it.
  That state is derived from the failing WIRE OPERATION — the classifier's
  class is a catch-all and reads an established-connection reset the same
  way it reads a refused connect, so `ClassConnection` must never be read as
  "never connected"; a `dial`/`proxyconnect` op, a typed proxy-tunnel
  failure, a TLS CERTIFICATE-VERIFICATION failure and a bare refused
  syscall prove no request byte left, and everything else (a read/write op,
  a bare EOF, a timeout on an established connection) is `send_unknown`.
  That TLS clause is deliberately one failure, not the phase: a handshake
  that fails any other way — a fatal alert (`*net.OpError` op
  `remote error` over unexported `tls` types), a plaintext peer (an untyped
  string error), a peer closing at accept (a bare EOF) — is pre-send in
  fact but not provable from a type, since Go produces those same shapes on
  an established connection, so it stays `send_unknown` and the pool gives
  up a fallback rather than risk a duplicate request. It is evidence
  about a DIALED attempt, so a zero-dial pool exhaustion reports none. The
  pool's refusal is inside one provider path only: the walk above it is the
  replay authority, so a chain whose candidates share a base-url does re-send
  by the matrix's own transport rows.
  `provider_attempt_started` fires before the attempt's first dial on both
  paths — so it announces an intent, but its `provider_attempt` is the index
  the logical attempt counter ALREADY holds, because the attempt is counted
  when it begins rather than when a dial succeeds: an attempt the envelope
  refuses before any dial still reports `provider_attempt` equal to
  `provider_attempts`, with `upstream_exchanges` alone unmoved — and a dial's
  own egress evidence follows the marker it belongs to, and one-based nested indexes
  `provider_attempt`/`egress_attempt` (egress index omitted when nothing
  was dialed); pooled records also carry the legacy `attempt` field as an
  alias equal to `egress_attempt` — compatibility only,
  `egress_attempt` is authoritative.
- **Invalid initial config = startup failure; invalid reload = last-known-good.**
  `LoadRuntime` failure at boot exits 1. `Poller.Run` on any failure logs and
  keeps the previous snapshot; its hash baseline is the boot content passed
  to `NewPoller`, never a fresh read. Rejection error text never quotes
  operator input (position/length/line only) — error text reaches logs
  verbatim.
- **One snapshot per request.** A handler calls `store.Load()` exactly once
  and binds the whole request — including its `api-key` and any active stream
  — to that snapshot forever. Reloads never affect in-flight work.
- **Client authentication is mandatory and terminal at this proxy.** Every
  Chat/Responses request presents a bearer credential as
  `Authorization: Bearer <key>`; the scheme is case-insensitive, and keys
  must be RFC 6750 bearer tokens (no whitespace or other invalid characters).
  A missing/malformed key gets the static missing-key 401, while a wrong key
  gets the static `invalid_api_key` 401. Authenticate before reading the body
  or upstream I/O; retain 405-before-401 ordering. `/healthz` and the 404
  catch-all stay unauthenticated. Consume — never forward or replace — the
  client's Authorization header; upstreams are trusted/internal.
- **Authentication resolves an identity through one seam, and every
  failure mode fails closed.** The handler asks the request snapshot's
  `auth.Provider` (nil at `NewHandler` = `StaticProvider`) — static mode
  compares against the snapshot's own `api-key` (padded constant-time, no
  length/mismatch-position leak), so a key rotation lands on the next
  request after the reload. Partner mode (`OAICR_AUTH_DATABASE_URL` set)
  resolves hashed per-partner keys: the store is opened, migrated
  (embedded, versioned, forward-only, transactional) and schema-validated
  at startup — any failure is fatal; the request path issues queries only.
  The YAML `api-key` is not a wire credential in partner mode (it stays
  required only so the file contract is unchanged) and revocation cannot
  be bypassed through it. The bounded decision cache never becomes a
  bypass: positive decisions ≤ 60s, negative ≤ 5s, backend errors NEVER
  cached — a store outage denies (static 401 + WARN `auth_backend_failed`
  with `error_class` only, never driver text) and is retried on the next
  request. Unknown and revoked are definitive negatives with the same
  static `invalid_api_key` wire body — why a key was rejected is
  enumeration material. Plaintext tokens exist once (`keys create` stdout),
  are crypto-random (`oaicr_` + 32 bytes), stored only as SHA-256 digests;
  `keys list` structurally exposes no secret material; `last_used_at`
  flushes off the request path (drop-on-full, never blocks, never fatal).
- **Injection must never corrupt.** Chat prepends to `messages` only when it
  is a JSON array; Responses merges into `instructions` (string, array, or
  absent) and touches nothing else. Empty prompt = no injection.
- **Rewrite is byte-preserving and API-scoped.** `RewriteChatModel` replaces
  only the top-level `"model"` string value; `RewriteResponsesModel`
  additionally replaces the `"model"` directly inside a top-level
  `"response"` object (Responses envelope events) — a chat payload's nested
  `response.model` is client data and passes untouched. Both inside a
  string-state-aware scan; unparseable input returns the input unchanged.
  Never re-serialize. Every call site (chat/responses × buffered/SSE) uses
  its own API's function.
- **Simulated thinking usage is opt-in, response-side, and fail-open.**
  A model's `thinking-usage` block (absent/null = off, the zero value)
  enriches existing client-facing usage objects with
  `floor(share × completion tokens)` (responses: `output tokens`), written
  into the API-native details field (`completion_tokens_details`/
  `output_tokens_details.reasoning_tokens`) under the API's own scope
  (responses also descends into the top-level `response` envelope). The
  share comes from `min-ratio`/`max-ratio` (both unset → 0.75, one → fixed,
  both → per-request uniform draw) and is drawn ONCE per request, before any
  upstream I/O, bound to the request's snapshot — every usage object in the
  request, streamed chunks included, reports the same share. Upstream-reported
  reasoning always wins (direct `reasoning_tokens`, or details > 0);
  completion ≤ 10 → 0; `mode: auto` activates only on the request's own
  thinking signals (`reasoning_effort`/`reasoning.effort` ≠ `"none"`,
  `enable_thinking`, `thinking.type == "enabled"`). Never fabricates a usage
  object, never touches other members, byte-preserving outside the single
  inserted/replaced details member. Buffered and SSE paths share one composed
  rewriter (`rewriteOut`), so parity is by construction; the SSE data-line
  gate accepts `"usage"` as well as `"model"`. Default off = byte-identical
  traffic, structurally: the zero-value plan is inactive and the composed
  rewriter degenerates to the model rename.
- **Streaming branches on the URL path**, not the body: chat = `data:`
  lines + `data: [DONE]`; responses = `event:`+`data:` pairs, no `[DONE]`
  (Responses termination events pass through untouched). `CopySSE` flushes
  per event boundary (blank line), is bounded (1 MiB per line, 2 MiB per
  in-flight event — breach stops the relay with outcome
  `stream_limit_exceeded`, the offending line never forwarded), and
  rewrites only `data:` lines containing a model string, with the same
  acceptance rule as the buffered path. With `sse-keep-alive` enabled
  (default: 15s), one request-owned ticker writes and flushes the ignorable
  SSE comment `: ping\n\n` only after an event-boundary silence interval;
  any forwarded byte resets it. It starts after headers commit, serializes
  with relay writes, exits/joined on every stream end or client disconnect,
  and is disabled immediately when `[DONE]` or `response.completed` is
  forwarded — never pings inside/after a terminal event; pre-header silence
  remains outside its scope.
- **3xx/204/304 verbatim; 4xx/5xx normalized, loud local.** Non-2xx
  non-error statuses (3xx redirects — never followed — 204, 304) forward
  byte for byte. Upstream 4xx/5xx are normalized: status preserved, body
  replaced by the canonical envelope, raw provider bytes relayed nowhere —
  a bounded 64 KiB prefix is read once under a short fixed internal
  capture timeout (test-overridable package var; never runtime
  configuration, and a `context.AfterFunc` closes the stalled body on the
  caller's or the timer's firing) to classify (`error_shape`) and
  fingerprint (`error_fingerprint`, SHA-256 of the prefix) into one
  `upstream_http_error` evidence event (WARN 4xx / ERROR 5xx,
  `error_class: upstream_error` with `error_cause:
upstream_http_4xx`/`upstream_http_5xx`, token-shaped
  `provider_error_type`/`code` only, log-normalized media-type-only
  `content_type` with static `invalid`/`oversized` markers, allow-listed
  `retry_after`/`x_ratelimit_*`, scheme+host upstream); the
  bytes themselves reach neither client nor logs. A 200 that is not JSON
  and an error-body read failure or stalled capture are both UNUSABLE
  answers before commitment, so both are retryable: the same candidate is
  re-asked while its budget lasts, then the walk falls back. The client
  sees 502 `upstream_invalid_response` only when the walk finalizes on one
  (outcome `upstream_invalid_response` for an unparseable/over-cap body,
  `upstream_read_failed` for a failed read or stalled capture), WARN
  `upstream_invalid_response` / `upstream_body_read_failed` carrying the
  closing disposition; a caller cancel/deadline during either is
  `client_disconnected` with no envelope.
  Dial failure is 502 `upstream_unreachable`; unmapped model is 404
  `model_not_found` and is NEVER forwarded.
- **Usage metering is optional, factual, and off the critical path.** With
  `OAICR_USAGE_DATABASE_URL` empty there is no connection or event. When set,
  startup migrates/validates the PostgreSQL store; each request that reaches
  the provider path hands exactly one event to a bounded async pipeline.
  Capture raw upstream usage before response rewrite/thinking synthesis;
  absent usage stays SQL NULL; streamed usage is last-readable-object wins,
  never a sum; a per-member unreadable count is that member unstated, never
  a discarded object. The event carries the walk's cumulative counters —
  provider attempts plus egress dials summed across every candidate — with
  `EgressKind` naming the RELAYED candidate's egress mode ("direct", the
  last dialed pool member's kind, empty when a pool exhausted without
  dialing) — the retained-answer case included, so a walk that ended on an
  earlier answer reports where THAT answer came from, not where the last
  dial failed. (`request_completed`'s `egress_kind`/`egress_target`/
  `egress_attempts` keep their own, narrower meaning: the pool report of
  the last candidate the request EXECUTED through.)
  and `Stream` records the mode actually relayed, not the probe's
  prediction. Failed attempts appear only in those counters. The queue
  drops explicitly/accountably under pressure; each flush carries a bounded
  deadline so every accepted event resolves into inserted-or-dropped, an
  expired drain waits a bounded grace for the in-flight insert before the
  repository closes, and `usage_meter_final` reports the totals after the
  shutdown drain — database failure never delays or mutates a client
  response. The shared migration ledger has module-scoped versions;
  deployed legacy auth rows are retained as `auth`, and advisory locks
  serialize bootstrap/migration races.
- **Never log or leak credentials.** No `Authorization`, keys, request
  bodies, or injection prompts in logs or error text; the usage DSN, raw
  provider bodies, and driver errors also never appear. A quote of these is a
  security defect (SECURITY.md), not a typo.
- **Graceful shutdown.** One signal channel: first SIGINT/SIGTERM →
  `Shutdown(grace)` → force `Close()` on overflow → `CloseIdleConnections` →
  drain the accepted usage-event queue within its bounded close window → exit 0. Second signal forces exit 1 — defers never run, so the usage meter's
  final accounting is that path's deliberate casualty; signals after the
  drain are ignored so a late duplicate cannot overwrite the exit code.
  Compose `stop_grace_period` (60s) > default `OAICR_SHUTDOWN_GRACE` (55s).
- **Logging hot-reloads like config, and leaks nothing at any level.**
  A top-level `log-level` key lives in the runtime YAML
  (`debug|info|warn|error`, exact-match — the org's other Go services share
  the spelling; absent = `info`); there is no `LOG_LEVEL` env var.
  A valid reload applies the level process-wide via the poller's
  `onPublish` hook calling `zerolog.SetGlobalLevel` (atomic store, no locks,
  no signal, no restart) and acknowledges it with `log_level_applied`
  emitted at the new level — the only severity visible under the level it
  announces — so no transition, even `error→warn`, is ever silent. Events are JSON lines on stderr with stable
  snake_case message slugs (`request_completed`, `config_reloaded`,
  `stream_truncated`, ...); one INFO `request_completed` per request binds
  `request_id`, outcome, byte counts, duration and `config_generation`. The
  credential rule is level-independent: no bodies, no `data:` payloads, no
  prompts, no Authorization, scheme+host only for upstream URLs. E2E pins
  assert on message slugs and fields (never prose) for logging tests only.
- **Healthcheck never reads YAML.** It probes `GET /healthz` (200 +
  `"ok\n"`) so a poisoned reload cannot fail the container probe.

## Error envelope contract (public API)

- 401 `invalid_request_error` — missing/malformed bearer: exact shape
  `{"error":{"message":"you must provide an API key in the Authorization header (Bearer <key>)","type":"invalid_request_error","param":null,"code":null}}`; wrong key: exact shape
  `{"error":{"message":"invalid API key","type":"invalid_request_error","param":null,"code":"invalid_api_key"}}`. Never interpolate credentials.
- 400 `invalid_request_error` — body not JSON, or missing `model`.
- 404 `model_not_found` — exact shape
  `{"error":{"message":"The model '<X>' does not exist or you do not have access to it.","type":"invalid_request_error","param":null,"code":"model_not_found"}}`.
  Interpolated names land byte-exact (no HTML escaping).
- 404 `invalid_request_error` — unknown path (no route matched):
  `{"error":{"message":"Invalid URL (<METHOD> <PATH>)","type":"invalid_request_error","param":null,"code":null}}`.
  OpenAI SDK clients always get parseable JSON, never the mux's plain text.
- 502 `upstream_error` — `code: "upstream_unreachable"` when no candidate
  ever answered; `code: "upstream_invalid_response"` when the walk
  finalizes on an unusable answer (200 + unparseable/over-cap JSON, a read
  that failed, a stalled capture) — both only AFTER the retry/fallback
  matrix has had its say. A client cancel while the upstream request is in
  flight is the WARN
  `client_disconnected` outcome, never this 502.
- Upstream 4xx/5xx — status preserved exactly (never collapsed to 502),
  body always the canonical envelope
  `{"error":{"message":"upstream provider returned HTTP <S>","type":"upstream_error","param":null,"code":"upstream_http_<S>"}}`
  with `Content-Type: application/json`; `Retry-After`/`X-RateLimit-*`/
  request-id headers still relay through the allow-list. 3xx/204/304
  forward verbatim.
- No overall request timeout; upstream timeouts surface as 502.

## Testing

- Unit tests co-located under `internal/`, stdlib only, deterministic.
  Exception: `internal/auth`, `internal/migrate`, and `internal/usage` have
  PostgreSQL integration tests (store lifecycle, migration idempotence,
  legacy-ledger adoption, concurrent migration serialization, foreign-schema
  fail-closed, forward-only history) that run against a real server only when
  `OAICR_TEST_DATABASE_URL` names a dedicated disposable test database —
  unset, they skip (and CI stays hermetic).
- E2E (`e2e/`) is a black-box suite over the built binary with in-process
  httptest upstreams; skips under `-short`; CI runs
  `go test ./e2e/ -count=1 -timeout 25m`.
- CI: `gofmt`, `go vet`, `golangci-lint v2.12.2` (checksum-pinned), race
  tests excluding `e2e`, `go build` with `-X main.version`. Check names are
  the aggregate gates `ci-gate` and `analysis-gate`.
- A change that fails only loudly is not tested. This service rewrites
  traffic in flight — pin the quiet direction: a prompt that stops being
  applied, a stream that gets buffered, a request bound to the wrong
  snapshot after a reload.

## The Semgrep directory has two non-obvious constraints

- `workflows.test.yaml` carries a top-level `rules: []` because
  `semgrep --config .github/semgrep` loads every `.yml`/`.yaml` there as a
  candidate rule config; without it the run aborts with exit 7.
- Every rule needs both halves of a fixture — the `ruleid:` case that must
  match (the load-bearing one) and `ok:` near-misses that must not; a rule
  that has quietly stopped matching passes every scan by finding nothing.

## Harness-agnostic conventions

- Go ≥ 1.25 (toolchain owned by `go.mod`); deps zerolog v1.35.1 +
  gopkg.in/yaml.v3 + jackc/pgx v5 (database/sql driver for partner keys and
  usage events), stdlib tests only; no Makefile — commands live in
  CONTRIBUTING.md and ci.yml.
- Conventional Commits via lefthook + commitlint (scopes: inject, proxy,
  server, config, cmd, e2e, docs, deps, ci, workspace, release). Signed
  commits everywhere. Squash merges into main only.
- release-please (go type) owns CHANGELOG.md/tags. The manifest baseline is
  0.0.0 until the first release lands — 0.0.0 disables the last-release
  backfill, so the first proposal comes straight from `initial-version`
  (0.1.0); afterwards the manifest tracks the landed version. Docker
  publishes ghcr.io/ecoma-io/openai-compatible-injector on release.
- AI-assisted commits carry `Assisted-by:`/`Generated-by:` trailers on the
  last commit of the PR, per CONTRIBUTING.md.
