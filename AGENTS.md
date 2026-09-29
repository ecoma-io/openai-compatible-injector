# AGENTS.md — openai-compatible-injector

Working guidance for AI-assisted development in this repository.

Three documents, three jobs: **README.md** is the behavior contract,
**`docs/design/`** carries the design rationale, and **this file** carries the
rules an agent cannot re-derive cheaply from the code plus the traps where the
intuitive change is the wrong one. Where this file disagrees with the code, with
README, or with `docs/design/`, this file is wrong — fix it, or the code; decide
which, deliberately. Long form for anything below lives in README.md, and the
section is named where a rule needs it. Do not grow this file back into a second
copy of the contract: point at the section instead.

Everything here is a rule: a bullet that only explains why the code looks the way
it does belongs in `docs/design/`.

## The service

An OpenAI-compatible request/response injector proxy. Clients talk to it as if it
were an OpenAI endpoint; it forwards to configured upstream providers, renaming
the model and injecting a per-model system prompt. Two model-serving surfaces —
Chat Completions (`/v1/chat/completions`) and Responses (`/v1/responses`), both
with SSE passthrough — plus authenticated `GET /v1/models`, answered from the
configured public mapping and never from an upstream. Three per-model response
transforms run in a fixed order: model rename → thinking-usage synthesis → field
stripping.

## Layout and boundaries

- **`internal/config`** — bootstrap env, strict runtime YAML, the
  providers/transports/pools tables, the `recovery` config surface, candidate
  chains, snapshot `Store`, content-hash `Poller`.
- **`internal/credential`** — per-provider credential `Spec`, rotation `Pool`,
  `Registry`. Inert by construction: no I/O, no goroutines, no timers.
- **`internal/inject`** — pure transforms: `Probe`, `Chat`/`Responses`, model
  rewriters, thinking/usage synthesizers, field stripping, continuation builders.
- **`internal/auth`** — client identity: `Principal`, the `Provider` seam, static
  and partner providers, token minting and at-rest hashing, the PostgreSQL key
  store.
- **`internal/migrate`** — the shared SQL-first, module-scoped migration runner;
  advisory locks serialize concurrent bootstrap and migration races.
- **`internal/usage`** — pre-rewrite usage capture, the PostgreSQL event
  repository, the bounded asynchronous pipeline.
- **`internal/transport`** — outbound paths: `Doer`/`Executor`/`Resolver` seams,
  direct/proxy/pool clients, failure classification, the exchange-budget seam.
- **`internal/memlimit`** — the process-wide byte admission: immediate CAS
  reservation, clamped release and `Peak` diagnostic; no allocation, lock or I/O.
- **`internal/recovery`** — the recovery policy domain: `Failure`/`Match`/`Action`,
  the matrix, layer merge and `Resolve`, the policy hash, the `Engine`.
- **`internal/proxy`** — HTTP wiring, client auth, `/v1/models`, error envelopes,
  the candidate walk, `CopySSE`, `rewriteOut`, the stream-continuation loop, and
  the two buffer admissions that draw on `internal/memlimit`.
- **`internal/server`, `cmd/…`, `e2e`** — listener lifecycle and shutdown;
  entrypoint and subcommands; black-box tests over the built binary.

Boundaries a helpful-looking refactor will cross:

- **`internal/recovery` does no I/O at all** — it never sleeps, dials, writes a
  response, reads a pool, or logs. The proxy owns execution; the transport owns
  the wire.
- **`internal/transport` knows nothing about providers or policy**, only about
  paths and bytes, and must never learn that a credential exists —
  `guards_test.go` parses the transport package's source to enforce that one. The
  same file parses `internal/credential`'s own imports, keeping it free of network
  capability and of every waiting primitive.
- **`internal/config` answers "which provider"; `internal/transport` answers "how
  the request reaches it"** — the boundary the recovery rules below rest on.
- **`internal/proxy` never classifies a status itself.** It contributes facts to
  the `recovery.Engine` and executes the decision it returns.

## Non-negotiables

### Configuration and reload

- **Two config planes, and the split is enforced, not conventional.** Bootstrap
  settings (`OAICR_LISTEN`, `OAICR_CONFIG_FILE`, `OAICR_CONFIG_POLL_INTERVAL`,
  `OAICR_SHUTDOWN_GRACE`, `OAICR_AUTH_DATABASE_URL`, `OAICR_USAGE_DATABASE_URL`
  — every environment variable the service reads is `OAICR_`-prefixed, with no
  unprefixed fallback) come from the environment, and a runtime file defining one
  is rejected by strict decoding. Everything else — model mapping, the required
  `api-key`, `log-level`, `sse-keep-alive` — lives in YAML only, as a single
  document (`---`-separated seconds are a rejection).
- **The database URLs are infrastructure, not policy.** Empty
  `OAICR_AUTH_DATABASE_URL` = static mode, non-empty = partner mode; empty
  `OAICR_USAGE_DATABASE_URL` = metering off, non-empty = migrated and validated
  at startup. Neither hot-reloads, and neither DSN is ever logged — not even its
  length, since it embeds a password.
- **Invalid initial config = startup failure; invalid reload = last-known-good.**
  A `LoadRuntime` failure at boot exits 1. On any later failure `Poller.Run` logs
  and keeps the previous snapshot, and its hash baseline is the boot content passed
  to `NewPoller` — never a fresh read. Rejection error text quotes position, length
  or line, never operator input.
- **One snapshot per request.** A handler calls `store.Load()` exactly once and
  binds the whole request to that snapshot forever — including its `api-key` and
  any active stream. Reloads never affect in-flight work.

### The request path

- **Client authentication is mandatory, terminal, and never forwarded.** Every
  `/v1` request — Chat, Responses, and the local `GET /v1/models` — presents
  `Authorization: Bearer <key>`; the scheme is case-insensitive and the key must be
  a valid RFC 6750 token. Authenticate before reading the body or doing any
  upstream I/O, and keep 405-before-401 ordering. `/healthz` and the 404 catch-all
  stay unauthenticated. Consume the client's Authorization header — never forward
  or replace it.
- **Injection must never corrupt.** Chat prepends to `messages` only when it is a
  JSON array; Responses merges into `instructions` (string, array, or absent) and
  touches nothing else. An empty prompt means no injection.
- **The request path is bounded in time and aggregate.** `maxRequestBodyBytes`
  (64 MiB) limits one client body; a per-read `requestBodyReadTimeout` (via
  `http.NewResponseController(w).SetReadDeadline`) limits its read; the
  process-wide `memlimit.Budget` (256 MiB) limits all request buffering. Clear
  the deadline as soon as the read returns; never use server `ReadTimeout` or
  `WriteTimeout` (which would break keep-alive or SSE). Reserve as a buffer grows,
  refuse immediately as 503 `capacity_exceeded`, and never send that refusal to
  recovery. The budget bounds LIVE bytes; `mem_limit` must be sized against RSS,
  which `GOMEMLIMIT` is what holds to the budget's arithmetic — do not restate
  a bare "2×" without it; see README "Buffering".
- **Every transform is byte-preserving and API-scoped; nothing is ever
  re-serialized.** `RewriteChatModel` replaces only the top-level `"model"` string
  value; `RewriteResponsesModel` additionally replaces the `"model"` directly
  inside a top-level `"response"` object (the Responses envelope) — a chat
  payload's nested `response.model` is client data and passes untouched. Both work
  inside a string-state-aware scan, and unparseable input returns the input
  unchanged. Stripping follows the same discipline: an excised member's own bytes
  — key, colon, value, comma — go via a shared splice, and unparseable input comes
  back as the _same slice_, which the SSE pointer-identity fast path depends on.

### The provider walk

- **Provider recovery is policy DATA resolved per request and frozen for that
  request's life; the bounded walk over the candidate chain executes it.** Every
  model carries a `Chain` (≥ 1 `Candidate`) and EVERY candidate carries the
  effective `recovery.Policy` its layer chain resolved — global → provider →
  model → candidate, deep-merged and re-validated at each step, so an override
  states only what changes and a layer contradicting the one below it is
  rejected rather than quietly winning. Two members are position-scoped and
  rejected elsewhere, because a block that cannot be honoured reads like a
  setting and is not one: `fallback` (the walk's reach — a property of the
  request's chain, not of a hop) at the global and model layers only, `stream`
  likewise (it changes text a client already received), and `budget.request`
  only in the top-level block.
- **What a failure MEANS is that policy's matrix, never a table in the handler.**
  Typed observations map onto `retry`/`fallback`/`terminal` through typed,
  allow-listed predicates only — no expressions, scripting or regex. The default
  matrix ships in `internal/recovery`, so absent, null or empty changes no
  traffic: the block SIZES and STEERS the walk, it never gates whether one
  happens. The effective policy lives exactly as long as the request — a reload
  mid-walk, mid-wait included, cannot reshape in-flight work.
- **Precedence is deterministic and independent of Go map iteration and of YAML
  order:** exact status > provider-error predicate > status class > failure
  cause > failure class, with shorthands expanded into canonical IDs
  (`http-<status>`, `http-class-<class>`, `transport-cause-<cause>`, …). Equally-specific
  overlapping rules are REJECTED at load, so no disposition is left to whichever
  rule the runtime happens to reach first.
- **Four things stay code-owned and non-configurable, and apply BEFORE the
  matrix:** a committed response is terminal; a caller cancellation or expired
  deadline is terminal, judged from the request context and never from the error
  chain or its text; the request-wide exchange envelope is absolute; a resolved
  policy is immutable. The evidence names which fired (`committed`, `caller`,
  `budget-request`, `budget-candidate`) beside the rule ID.
- **The two envelope identities are not interchangeable.** A spent REQUEST
  envelope ends the walk — no candidate can start another exchange. A spent
  CANDIDATE envelope forbids only another exchange on THAT candidate, and the walk
  may still fall back because `EnterCandidate` opens the next envelope fresh: a
  per-candidate number must never pin a chain the operator configured to fall
  back. The request envelope counts REAL outbound exchanges, one unit claimed by
  the transport immediately before each dial. A value above a cap REJECTS the file
  rather than being clamped.
- **The exchange window reaches a buffered attempt through EOF, and stops at the
  commitment — decided by the RESPONSE, never by the request's `stream` flag.** A
  pre-commitment exchange is bounded end to end: a stalled header and a stalled
  buffered body are the same window, and only `transport.HandoffStream`, called at a
  confirmed `text/event-stream` and at the verbatim 3xx/204/304 relays, hands a body
  out of it. Guessing the phase from the request (a `stream: true` answered with
  `200 application/json`) re-opens the unbounded-pre-body hole #96 closed. A body the
  window cuts is this proxy's own bound, so its evidence is
  `error_cause: exchange_elapsed` over `failure_origin: envelope` — never a peer read
  fault.
- **What the matrix decides, the mechanical policies only size.** A retried
  failure waits a bounded backoff (initial, doubling to a ceiling, ± jitter); a
  fallback moves immediately with no inter-candidate wait; a retryable failure
  whose candidate budget is spent takes `retries.on-exhausted`, which is also the
  answer to the one refusal no observation can describe — a transport that declined
  to dial because the envelope was already spent (`Engine.CandidateSpent`). An
  upstream `Retry-After` can only ever RAISE a wait, never past the backoff
  ceiling, the retry-after policy's own `max-delay`, the candidate's remaining
  `max-elapsed` window, or the caller's remaining deadline — whichever binds
  first; `max-delay` ceilings the DIRECTIVE, not the schedule. The `max-elapsed`
  window is measured from the candidate's FIRST attempt and checked BEFORE a wait
  is scheduled. `fallback.enabled: false` pins the primary candidate while
  same-candidate retries still apply; `fallback.max-candidates` counts candidates
  ENTERED (the primary included), never the ones the chain lists; the retry budget
  is enforced PER CANDIDATE, never shared.
- **Transport failure falls to the next candidate with NO same-candidate retry
  under the shipped default** — an operator matrix CAN ask for one, since
  `transport` and `transport-cause-*` rows are matchable.
- **Legacy spellings still load, and normalize into this one engine rather than
  running beside it.** `provider`, `endpoint`, `retries` and `provider-fallback`
  normalize into the same policy structure (the `provider`/`endpoint` forms build
  a one-candidate chain, so the handler walks one uniform structure), and the
  config layer REJECTS a legacy block paired with its `recovery` counterpart.
  There is no mode in which both run side by side.
- **The walk completes before the first client-visible byte.** The candidate whose
  answer produces that byte has produced THE response, so streaming commitment
  holds by construction, and a committed stream that dies mid-flight is truncated
  — never retried, never switched. The one thing past that line is the
  continuation loop below, which is not a retry and never moves candidate.
- **The LAST received HTTP answer wins.** Its status is preserved over the
  canonical envelope, so a later candidate's 429/503 is never collapsed into 502.
  That includes retention: a candidate whose budget ran out on a received answer,
  followed only by candidates failing BEFORE answering, relays that retained
  answer — the answering candidate becomes `final_provider` and
  `provider_exhausted` stays unset, because a provider did answer. 502
  `upstream_unreachable` happens only when NO candidate ever answered; a capture
  failure on the final retained answer answers `upstream_invalid_response`.
- **Every attempt replays the immutable client body through that candidate's own
  transform** — retries included, a fresh request each time, with nothing observed
  on an earlier attempt feeding the next. A local transform error still answers
  400 immediately: a body failing one candidate's transform fails all. Chains,
  resolved policies and transports all bind to the request's snapshot, and the
  egress closure covers every candidate's transport.

### Egress and credentials

- **The router decides WHICH provider; the transport decides HOW the request
  reaches it.** Each candidate's flattened `transport.Config` comes from the
  request's own snapshot, and the handler resolves it through the
  `transport.Resolver` seam per attempt. The lookup is content-keyed, so every
  retry and fallback of the same candidate gets the same long-lived `Doer` and its
  warm pool state for as long as the config is live.
- **Upstream HTTP statuses are answers; only network-level failures are errors.**
  `StatusCode` set with `err == nil` is an answer. Bodies are never buffered on
  `direct`/`proxy` — but `poolDoer.Do` DOES buffer the request, because a bare
  `*http.Request` cannot carry the probed stream flag the pool's eligibility gate
  needs. Routing client traffic through `Do` for a pool silently bypasses that
  gate, so it is a defect rather than a supported mode: the handler always hands a
  pool its own request facts through `Execute`.
- **`socks5` and `socks5h` are not interchangeable.** `socks5` resolves the
  upstream hostname locally and CONNECTs the IP; `socks5h` sends the hostname for
  remote DNS. An unknown transport or provider reference, a bad type, or a
  malformed proxy URL is a whole-file rejection — never a silent fallback to
  `direct`.
- **Egress pools schedule, gate, and fall back — they never retry an answer.**
  `pool` is the third transport kind: an ordered member list of direct/proxy refs
  (never pools), with `round_robin`/`weighted_round_robin` scheduling, bounded
  provably-unsent fallback and passive health. Defaults, caps and the full policy
  surface are in README "Provider transports".
- **Eligibility precedes everything, and only then does the scheduler move.**
  Static gates (the streaming gate, `max-body-bytes` against the outgoing
  post-injection body) are checked BEFORE any dial — a 6 MB request is never a 413
  on a 4.5 MB relay — then dynamic ones (health cooldown, concurrency permit). A
  skipped member consumes no scheduler turn, no attempt and no strike. Weighted
  scheduling is smooth WRR over the CURRENTLY ELIGIBLE members, and the chosen
  member's permit is acquired inside the selection step under the scheduler lock,
  so two concurrent requests can never both observe the same last permit.
- **Permitted locks: the scheduler mutex is the only outer lock over a member's
  health and limiter mutexes, and no network I/O happens under any of them.**
- **A pool never status-retries, and provider fallback never happens inside one.**
  ANY status — 429/5xx included — is one answer handed up to the handler, and only
  the request's resolved recovery policy retries or falls back on it. Egress
  fallback moves a request between network paths to the SAME provider; only the
  handler's walk moves between providers.
- **`send_state` is the pool's fallback gate, and it is derived from the failing
  WIRE OPERATION.** Only a `definitely_not_sent` failure may move the request to
  another egress member, because a `send_unknown` request may already have reached
  the upstream and replaying it would duplicate it. The classifier's class is a
  catch-all, so `ClassConnection` must never be read as "never connected": a
  `dial`/`proxyconnect` op, a typed proxy-tunnel failure, a TLS
  CERTIFICATE-VERIFICATION failure and a bare refused syscall prove no request
  byte left; everything else — a read/write op, a bare EOF, a timeout on an
  established connection — is `send_unknown`. The TLS clause is one failure, not
  the phase: any other handshake failure stays `send_unknown`, because Go produces
  those same shapes on an established connection, and the pool gives up a fallback
  rather than risk a duplicate.
- **Pool state is identity-keyed and instance-owned.** Scheduler cursor, health,
  permits and in-flight leases live in the `Registry` keyed by pool identity:
  unchanged policy across a reload stays warm, changed policy starts fresh, and a
  leased state outlives its eviction until the last request releases it. Eviction
  checks the state INSTANCE under the map key, never the key alone, so re-adding
  an identity retired in between builds a fresh generation.
- **Config rejects what cannot be honoured.** A pool whose members resolve to
  duplicate ENDPOINTS (identity is the canonical resolved endpoint with host case
  folded, never the YAML name), and a SOCKS5 userinfo credential over 255 decoded
  bytes (the RFC 1929 wire limit; the dialer re-checks it as a typed auth error).
- **Credential rotation state is per content-identity, not per name.** `PoolKey()`
  hashes the provider name, the `Spec` and the rate limit, so two providers with
  byte-identical credentials are two rotation domains and a rate-limit-only change
  moves the key. That key, the content digest and the credential values are never
  logged; only the operator-chosen key `id` from the safe charset
  `[A-Za-z0-9._:-]` may travel, as `upstream_credential_id`.

### Response transforms

- **Field stripping is config-data, byte-preserving, and runs LAST.** It excises
  configured provider-added members (`strip-fields`, syntax in README "Response
  field stripping") from every relayed 2xx response, last in the composed
  `rewriteOut` (rename → thinking synthesis → strip) so configuration can never
  strip the `model`/`usage` keys the proxy itself writes — which are rejected at
  load anyway, as an enumerated set of EXACT paths. The strip list binds per
  request like the rest of the snapshot: a model-level list replaces its
  candidates' provider lists; without one, each candidate answers under its own
  provider's list. The usage meter observes pre-rewrite bytes, so stripped fields
  are never metered. The SSE data-line gate is widened with the strip list's
  first-segment patterns (`stripKeys`), re-derived per candidate, so a chunk
  carrying only a to-be-excised key still reaches the strip.
- **Simulated thinking usage is opt-in, response-side, and fail-open.** A model's
  `thinking-usage` block (absent or null = off) enriches EXISTING client-facing
  usage objects in the API-native details field, under the API's own scope; the
  share is drawn ONCE per request, before any upstream I/O, bound to the request's
  snapshot, so every usage object — streamed chunks included — reports the same
  share. Upstream-reported reasoning always wins. It never fabricates a usage
  object. Default off is byte-identical traffic, structurally: the zero-value plan
  is inactive and the composed rewriter degenerates to the model rename. Ratios,
  thresholds and `mode: auto` are in README "Simulated thinking usage".

### Auth

- **Authentication resolves an identity through one seam, and every failure mode
  fails closed.** The handler asks the request snapshot's `auth.Provider` (nil at
  `NewHandler` means `StaticProvider`) — static mode compares against the
  snapshot's own `api-key` in padded constant time, with no length or
  mismatch-position leak, so a key rotation lands on the next request after the
  reload. Partner mode resolves hashed per-partner keys through a store that is
  opened, migrated and schema-validated at startup. The YAML `api-key` is NOT a
  wire credential in partner mode (it stays required only so the file contract is
  unchanged) and revocation cannot be bypassed through it.
- **The decision cache never becomes a bypass.** Positive decisions ≤ 60s,
  negative ≤ 5s, and backend errors are NEVER cached — a store outage denies with
  the static 401 plus WARN `auth_backend_failed` carrying `error_class` only,
  never driver text, and is retried on the next request. Unknown and revoked are
  definitive negatives with the same static `invalid_api_key` wire body, because
  why a key was rejected is enumeration material.
- **Plaintext tokens exist once.** `keys create` prints the only copy; tokens are
  crypto-random (`oaicr_` + 32 bytes) and stored as SHA-256 digests only.
  `keys list` structurally exposes no secret material, and `last_used_at` flushes
  off the request path (drop-on-full, never blocks, never fatal).
- Startup migration, schema validation, the collision-resistant ids and the
  `keys` surface are in README "Partner API keys".

### Streaming

- **Streaming branches on the URL path**, not the body: chat is `data:` lines plus
  `data: [DONE]`; Responses is `event:` + `data:` pairs with no `[DONE]`. A
  Responses `event:` line passes through untouched, but a `response.completed`
  data line is rewritten and stripped like any other payload. `CopySSE` flushes
  per event boundary (blank line) and is bounded at 1 MiB per line and 2 MiB per
  in-flight event; a breach stops the relay with outcome `stream_limit_exceeded`
  and the offending line is never forwarded. It rewrites only the `data:` lines
  its gate admits — `"model"`, `"usage"`, or a configured strip key — under the
  same acceptance rule as the buffered path.
- **The SSE keep-alive is request-owned and never pings inside or after a terminal
  event.** One ticker writes the ignorable comment `: ping\n\n` only after an
  event-boundary silence interval; any forwarded byte resets it. It starts after
  headers commit, serializes with relay writes, is joined on every stream end or
  client disconnect, and is disabled immediately when `[DONE]` or
  `response.completed` is forwarded.
- **`CopySSE` reports what it saw as `StreamStats`** — bytes, events, and
  `Terminal`, set at the one place a terminal line is admitted, since "did the
  client ever get its marker" is the fact the continuation loop is gated on. The
  `observe` seam sees the payload of EVERY data line, before the rewrite gate; the
  handler binds it to the continuation accumulator, while the usage capture rides
  the rewriter instead.
- **Post-commitment stream recovery is a SECOND orchestrator, off by default, and
  it is not a retry.** A committed SSE stream that ends without its terminal
  marker (`data: [DONE]`, `event: response.completed`) is re-asked of the SAME
  candidate and its events relayed into the same response. The loop reads
  `answer.cand.Recovery.Stream` off the request's own frozen snapshot and nothing
  else — never `recovery.Engine`, whose commitment invariant correctly refuses a
  stream a client already holds, and never the matrix.
- **The hop is the committed candidate re-asked, not a new attempt that looks
  similar.** Same endpoint and route suffix, same `transport.Doer`, same
  `*credential.Pool` INSTANCE, and the key the walk went out with as its sticky
  preference — resolved, never re-derived, because a fresh registry lookup after
  a concurrent publish would acquire from a cold pool. A hop pays its dials out of
  the request's `budget.request` envelope and counts as a provider-level attempt
  (`provider_attempts` and `upstream_exchanges` move; `candidates_entered` does
  not).
- **Four bounds are refusals, never clamps:** the safety gate, `max-recoveries`,
  `max-elapsed` (maximum upstream silence, checked BEFORE scheduling), and the
  exchange envelope. Only upstream activity moves the window: a source-body read
  and a continuation response header that arrived in time. Client writes, SSE
  event boundaries and this proxy's keep-alive ping never do. The safety gate is
  fail-closed: every data payload feeds `partialText`; tool/finish signals, non-JSON
  non-`[DONE]` data, text over `max-partial-bytes`, an empty prefix, or an
  inexpressible continuation body make it unrecoverable.
- **Hard boundaries for the continuation loop.** NO second header block and NO
  error body ever reaches a client already receiving a stream, and NO terminal
  marker is ever synthesized: a hop that truncates leaves the stream exactly as
  unterminated as it would have been. NO heuristic ever deletes client-visible
  text — a repeated paragraph at the seam is honest, overlap trimming is not. The
  heartbeat runs ACROSS hops (`stopAndWait` fires after the loop), so the idle cut
  it exists to prevent cannot fire mid-recovery, and a reload mid-stream changes
  nothing.
- **One owner per recovery stop, one field vocabulary.** README "Logging" and
  `streamrecovery_events_test.go` pin the five events. `phase` is the hop's,
  `reason` the loop's and `unsafe_reason` the accumulator's; builder refusal is
  `phase: build` plus `unsafe_reason`, never a builder token in `reason`.
  `max_elapsed`, `client_write` and `upstream_limit` are proxy-owned — never make
  their error evidence say the upstream failed.
- **A hop's body owns its dial's context, and the release rides the Close.**
  net/http aborts an UNREAD response body when the request's context is canceled,
  so the context a hop dialed under must outlive `dialContinuation`: the hop wraps
  it in `boundBody`, whose `Close` is the release, and the header watchdog's cancel
  must not fire once the body is handed off. Releasing at the dial's return
  truncates every successful hop.
  `bind` is called once per hop, but the window is created ONCE per logical
  session (at the commit), never per hop — a per-hop reset would let the last
  lever outlive the moving `max-elapsed` silence allowance. One moving window
  covers both waits: `armBody` closes a stalled body, `bind`'s own cancel
  unblocks a stalled response HEADER, and `promote` hands that cancel to the
  new body when the header arrives in time. Every lever is bound to the same
  `recoveryClock` seam, and the live interval is half-open — `now < deadline` —
  so a timer firing exactly on the instant latches the window and no later
  upstream byte can revive it.
- **The compatibility hinge is exact.** Feature off (or absent — the zero
  `StreamPolicy`) leaves the relay byte-identical, and an EOF with no marker still
  logs `stream_completed` / outcome `completed`. Feature ON reports a marker-less
  stream as truncated whether the proxy dialed for it or refused to. Every
  recovery reason is a closed-set token from a typed value, never error text, and
  no recovery event carries a body, a prefix, or a credential.

### Errors

- **3xx/204/304 forward verbatim; 4xx/5xx are normalized and loud locally.**
  Redirects are never followed. An upstream 4xx/5xx keeps its status and gets the
  canonical envelope, and the raw provider bytes reach neither client nor logs: a
  bounded 64 KiB prefix is read once under a short fixed internal capture timeout
  (a test-overridable package var, never runtime configuration) to classify
  `error_shape` and fingerprint `error_fingerprint` into one `upstream_http_error`
  event — WARN for 4xx, ERROR for 5xx. The field list is in README "Errors".
- **An unusable answer before commitment is retryable, not fatal.** A 200 that is
  not JSON, and an error-body read failure or stalled capture, are both judged by
  the matrix like any other observation: the same candidate is re-asked while its
  budget lasts, then the walk falls back. The client sees 502
  `upstream_invalid_response` only when the walk finalizes on one — outcome
  `upstream_invalid_response` for an unparseable or over-cap body,
  `upstream_read_failed` for a failed read or stalled capture. A caller cancel or
  deadline during either is `client_disconnected`, with no envelope.
- **Dial failure is 502 `upstream_unreachable`; an unmapped model is 404
  `model_not_found` and is NEVER forwarded.**

### Observability and secrets

- **Never log or leak a credential.** No `Authorization`, keys, request bodies, or
  injection prompts in logs or error text; the usage DSN, raw provider bodies, and
  driver errors likewise never appear. Upstream credential VALUES from a provider
  `auth:` block are the same class of secret: they live in the config file, in
  memory, and on the outgoing wire, and never in an error, a log line, a reload
  event, a metric label, a panic, or a response body. A quote of one of these is a
  security defect (SECURITY.md), not a typo. Log only scheme+host for upstream
  URLs; README "Safety and credentials" has the full list.
- **Three countings share names, and only one of each name is right.** The LOG
  field `egress_attempts` is the narrowest: the relayed candidate's own pool
  report, absent when that attempt was direct, NOT corrected on a retained walk.
  The usage event's `EgressKind` IS corrected to the retained answer, and its
  `egress_attempts` COLUMN is a third thing again — the request-wide exchange
  total the log calls `upstream_exchanges` (one quantity, two names, because the
  column predates the field).
- **Two counter traps.** `request_exchange_budget_remaining` reports the REQUEST
  envelope's headroom, never the tighter of the two — that would read zero on a
  candidate-envelope exhaustion. `provider_attempt_started` fires before the
  dial but carries the index the logical counter ALREADY holds, so an attempt the
  envelope refuses before any dial still reports `provider_attempt` equal to
  `provider_attempts`, with `upstream_exchanges` alone unmoved. Egress indexes
  are one-based and omitted when nothing was dialed; per-dial events also carry
  `attempt`, a legacy alias of `egress_attempt` — `egress_attempt` is
  authoritative. `request_completed` adds `retries_total` (an alias of
  `retry_attempts`), `final_candidate` (1-based) and `final_provider`.
- **`provider_attempt_failed` is transport failures ONLY**, and the received
  status (`upstream_status`) plus the `upstream_*` reason tokens belong to the
  unusable-answer and `upstream_http_error` events instead, because a transport
  failure has no status to carry. `error_class`/`error_cause`/`failure_origin`/
  `send_state`/`disposition`/`reason` are all closed sets: carry README
  "Observability"'s vocabulary rather than inventing a token, and never map a
  cause from message text.
- **`egress_attempt_failed` and `candidate_exchange_budget_spent` are not the same
  kind of event.** The first is one WARN per dialed-and-failed endpoint, pooled or
  direct. The second is a refusal, never a failed endpoint: no strike, no
  `egress_attempt_failed`, no `upstream_exchange` (nothing happened to index),
  `policy_rule_id` always `budget-candidate`, and `retries.on-exhausted` still
  owns whether the walk moves — except a refusal by the REQUEST envelope, which
  never reaches that WARN and is terminal whatever the policy says, named
  post-walk under `budget-request`.
- **Logging hot-reloads like config, and leaks nothing at any level.** A top-level
  `log-level` key lives in the runtime YAML (`debug|info|warn|error`, exact match;
  absent = `info`); there is no `LOG_LEVEL`. A valid reload applies the level
  process-wide through the poller's `onPublish` hook calling
  `zerolog.SetGlobalLevel` — no lock, no signal, no restart — and acknowledges it
  with `log_level_applied` emitted at the NEW level, so no transition, even
  `error→warn`, is ever silent. Events are JSON lines on stderr with stable
  snake_case slugs. The credential rule is level-independent, and E2E logging pins
  assert on slugs and fields, never prose.

### Process lifecycle

- **Liveness and readiness are separate: readiness turns false BEFORE the
  listener stops accepting.** `/healthz` stays 200 while the listener exists;
  `/readyz` is server-owned, 200 only while ready, otherwise 503 + state and
  `no-store`. `Run` calls `beginDraining()` first, serves the `readinessPropagation`
  head start (5s, at most `grace/2`) inside grace, then `Shutdown(grace-head)`.
  A cancelled entry context is never ready. Readiness names this process alone,
  with one atomic state and no lock/callback; the container probes `/readyz`.
- **Graceful shutdown, one signal channel.** The first SIGINT/SIGTERM runs
  `Shutdown(grace)`, forces `Close()` on overflow, closes idle connections and
  drains accepted usage events before exit 0. A second signal exits 1; late signals
  cannot overwrite the drain's exit code. Compose `stop_grace_period` (75s) covers
  default 55s grace plus the bounded shutdown defers.
- **Metering is optional, factual, and off the critical path.** Capture raw
  upstream usage BEFORE any response rewrite; absent usage stays SQL NULL. WITHIN
  one upstream call, streamed usage is last-readable-object wins, never a sum, and
  a per-member unreadable count is that member unstated rather than a discarded
  object. ACROSS the calls one request was assembled from — a stream-recovery hop
  is a new upstream response — the capture is sealed at each pass boundary and
  each count is combined by its OWN semantics: the last call that stated a prompt
  wins (the context that actually ran), completions add up (the client read every
  call's text), and total is that row's own prompt + completion, so the three
  columns cannot contradict each other and no call stating a count leaves it NULL.
  `EgressKind` names the RELAYED candidate's egress mode, the retained-answer case
  included; `Stream` records the mode actually relayed, not the probe's
  prediction. The queue drops accountably under pressure and `usage_meter_final`
  reports the totals after the shutdown drain — a database failure never delays
  or mutates a client response.
- **Healthcheck never reads YAML.** It probes `GET /readyz` (200 + `"ok\n"`), so a
  poisoned reload cannot fail the container probe and a draining instance fails it
  on purpose.

## Error envelopes

The client-facing bodies are byte-exact and live in **README "Errors"**, which
carries the full condition table — every exact body, the 413 body cap, the
capacity 503, and the relayed-header allow-list. Treat that table as the contract
and a diff against it as a breaking change; do not restate it here.

One rule is easy to get wrong: the 404 `model_not_found` body interpolates the
requested model name byte-exact, with no HTML escaping. Do not "fix" it onto a
JSON encoder that escapes `<` or `&`; a unit test pins the property.

## Testing and CI

- Unit tests are co-located under `internal/`, stdlib only, deterministic —
  except `internal/auth`, `internal/migrate` and `internal/usage`, whose
  PostgreSQL integration tests run only when `OAICR_TEST_DATABASE_URL` names a
  disposable test database (unset, they skip and CI stays hermetic).
- E2E (`e2e/`) is a black-box suite over the built binary; CI runs
  `go test ./e2e/ -count=1 -timeout 25m`. The rest of the command list and the
  aggregate gates (`ci-gate`, `analysis-gate`) are in CONTRIBUTING.md and `ci.yml`.
- **A change that fails only loudly is not tested.** This service rewrites traffic
  in flight, so pin the quiet direction: a prompt that stops being applied, a
  stream that gets buffered, a request bound to the wrong snapshot after a reload.

## Repo traps

- **The Semgrep directory has two non-obvious constraints.**
  `workflows.test.yaml` carries a top-level `rules: []` because
  `semgrep --config .github/semgrep` loads every `.yml`/`.yaml` there as a
  candidate rule config; without it the run aborts with exit 7. And every rule
  needs both halves of a fixture — the `ruleid:` case that must match (the
  load-bearing one) and `ok:` near-misses that must not, since a rule that has
  quietly stopped matching passes every scan by finding nothing.
- **The commitlint scope list has no `transport` scope.** A transport-layer commit
  takes `proxy`. A wrong scope fails the hook and the commit silently aborts.

## Conventions

- Go ≥ 1.25, with the toolchain owned by `go.mod`; dependencies are zerolog,
  `gopkg.in/yaml.v3` and jackc/pgx v5 (the database/sql driver for partner keys
  and usage events). Tests are stdlib only. There is no Makefile — commands live
  in CONTRIBUTING.md and `ci.yml`.
- Conventional Commits via lefthook + commitlint, scopes `inject`, `proxy`,
  `server`, `config`, `cmd`, `e2e`, `docs`, `deps`, `ci`, `workspace`, `release`.
  Signed commits everywhere, squash merges into main only.
- release-please (go type) owns CHANGELOG.md and the tags; the manifest tracks the
  landed version. Docker publishes `ghcr.io/ecoma-io/openai-compatible-injector`.
- AI-assisted commits carry an `Assisted-by:`/`Generated-by:` trailer on the PR's
  LAST commit — one per pull request, not per commit, since squash merges
  concatenate trailers.
