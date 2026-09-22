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

| Directory                        | Owns                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| -------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `internal/config`                | Bootstrap env parsing (`LoadBootstrap`), runtime YAML (`LoadRuntime`, strict decode via `yaml.v3` known fields, required client `api-key`, log level via `ParseLogLevel`), providers/transports tables incl. the pool schema (`buildModel`/`buildProviders`/`buildTransports` two-pass + pool policy builders, no-echo rejections, duplicate resolved-endpoint member rejection via `endpointKey`, RFC 1929 credential bound in `parseProxyURL`), egress-closure snapshot retention over model CHAINS (every candidate's transport), provider candidate chains (`runtimeModelCandidate`/`buildChain`), the `provider-fallback` policy builder, snapshot store (`Store`/`Snapshot`, atomic pointer), content-hash poller (`Poller`, `onPublish` hook) |
| `internal/inject`                | Pure request/response transforms: `Probe` (model + stream detection), `Chat`, `Responses`, `RewriteChatModel`/`RewriteResponsesModel` (byte-preserving, API-scoped), thinking plan + usage synthesizers (`ThinkingPlanFor`, `SynthesizeChat/ResponsesThinkingUsage`)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| `internal/auth`                  | Client identity: `Principal`/`Reason`/`Authenticator`/`Provider` seam, `StaticProvider` (snapshot-bound shared key, padded constant-time compare), `PartnerProvider` (store-backed, bounded positive/negative decision cache — errors never cached), crypto-random token/keyID minting + SHA-256-at-rest hashing, PostgreSQL key store (`PGStore`: lookup/create/list/revoke, off-path batched `last_used_at` flusher), embedded forward-only SQL migrations (`EnsureSchema`/`ValidateSchema` via `information_schema`)                                                                                                                                                                                                                              |
| `internal/transport`             | Outbound paths: `Doer`/`Executor`/`Resolver` seams, direct client (cloned default transport tuning), proxy client (`http.ProxyURL` or hand-rolled SOCKS5 dialer preserving socks5-vs-socks5h DNS semantics), typed proxy errors + `Classify`, egress pool runtime (`pool.go`: eligibility, scheduling, bounded fallback, health, permits, leases), `Registry` (content-keyed clients + per-identity pool state, retained on publish)                                                                                                                                                                                                                                                                                                                 |
| `internal/proxy`                 | HTTP handler wiring, client bearer authentication/Authorization stripping (auth delegated to the `auth.Provider` seam — static or partner), error envelopes, SSE copying (`CopySSE`), provider candidate walk (bounded by the snapshot's provider-fallback policy, transport-failure-only), composed response rewriter (`rewriteOut`: model rename + thinking-usage synthesis); executes upstream calls through the model's resolved `transport.Doer` — `Executor` (pool) branch handing request facts and reporting `egress_attempts`/`egress_kind`/`egress_target`/`egress_exhausted`                                                                                                                                                              |
| `internal/server`                | Listener lifecycle and graceful shutdown (`Server.Run`)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| `cmd/openai-compatible-injector` | Entrypoint: subcommands `version`, `healthcheck`, `keys create/list/revoke` (partner key lifecycle; list exposes no secrets), default serve                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| `e2e`                            | Black-box tests driving the real binary as a subprocess                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |

## Non-negotiables

- **Two config planes.** Bootstrap settings (`OAICR_LISTEN`,
  `OAICR_CONFIG_FILE`, `OAICR_CONFIG_POLL_INTERVAL`,
  `OAICR_SHUTDOWN_GRACE`, `OAICR_AUTH_DATABASE_URL` — every environment
  variable the service reads is `OAICR_`-prefixed; no unprefixed fallback
  exists) come from the environment and
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
  length — it embeds a password).
- **The router decides WHICH provider; the transport decides HOW the
  request reaches it.** The handler resolves a model's flattened
  `transport.Config` through the `transport.Resolver` seam once per request
  and executes on the returned `Doer` — no provider-specific branches, no
  global `http.Client` coupling. `direct` is the tuned Go stack (ambient
  env proxies honored, zero value of `Config`); `proxy` is exactly ONE
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
  pick), bounded pre-response fallback (`fallback.enabled`, default
  true/3, cap 16 — skipped members consume no attempt), and passive health
  (`health.enabled`/`failure-threshold`/`cooldown`, defaults true/3/30s,
  min 1s; any response resets). Eligibility precedes everything — static
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
  flag) and gets one response or one error; cancellation aborts with no
  strike, zero dials is the exhaustion sentinel answering the canonical
  502 `upstream_unreachable` with `error_class: egress_exhausted`, and any
  HTTP status — 429/5xx included — ends the loop and relays like a
  single-endpoint response. Every dialed-and-failed endpoint appends one
  sanitized `AttemptFailure` (kind, scheme+host, typed class — never error
  text, never credentials) that the handler relays as WARN
  `egress_attempt_failed`. Proxy/SOCKS failures are typed
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
- **Provider fallback is a walk over a candidate chain, bounded, and
  triggered by transport failure only.** Every model carries `Chain`
  (≥1 `Candidate`: providers-table name, endpoint, per-candidate
  upstream-model, transport); the legacy `provider`/`endpoint` forms build
  a one-candidate chain, so the handler walks one uniform structure — no
  separate legacy path. `provider-fallback` (`enabled`, default true;
  `max-attempts`, default 2, cap 8) bounds the walk to
  `min(max-attempts, len(chain))` candidates; it composes with egress
  fallback multiplicatively (worst-case dials = provider budget × egress
  budget). A candidate is skipped only when it fails BEFORE answering
  (dial/TLS/proxy/egress exhaustion — non-cancellation transport errors);
  ANY HTTP status ends the walk and is relayed as a single-provider
  answer. Never retried: local transform errors (a body failing one
  candidate's transform fails all — answer 400 immediately), client
  cancellation (client_disconnected, walk aborted), and commitment (the
  walk completes before the first response byte — the candidate that
  produces headers has produced THE response, so streaming commitment
  holds by construction). Each attempt replays the immutable client body
  through that candidate's own transform (fresh request, identical
  transformed body per candidate). Chains, policy, and per-candidate
  transports bind to the request's snapshot; the egress closure covers
  every candidate's transport. Observability: `provider_attempts`,
  `final_provider` on every completion that reached the walk,
  `provider_exhausted` on exhaustion; one WARN `provider_attempt_failed`
  per failed candidate.
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
  a bounded 64 KiB prefix is read once (`internal/proxy/upstream_error.go`)
  to classify (`error_shape`) and fingerprint (`error_fingerprint`,
  SHA-256 of the prefix) into one `upstream_http_error` evidence event
  (WARN 4xx / ERROR 5xx, token-shaped `provider_error_type`/`code` only,
  allow-listed `retry_after`/`x_ratelimit_*`, scheme+host upstream); the
  bytes themselves reach neither client nor logs. A 200 that is not JSON
  becomes 502 `upstream_invalid_response`; an error-body read failure is
  WARN `upstream_body_read_failed` + 502; a client cancel during it is
  `client_disconnected` with no envelope. Dial failure is 502
  `upstream_unreachable`; unmapped model is 404 `model_not_found` and is
  NEVER forwarded.
- **Never log or leak credentials.** No `Authorization`, keys, request
  bodies, or injection prompts in logs or error text. A quote of these is
  a security defect (SECURITY.md), not a typo.
- **Graceful shutdown.** One signal channel: first SIGINT/SIGTERM →
  `Shutdown(grace)` → force `Close()` on overflow → `CloseIdleConnections` →
  exit 0. Second signal forces exit 1; signals after the drain are ignored
  so a late duplicate cannot overwrite the exit code. Compose
  `stop_grace_period` (60s) > default `OAICR_SHUTDOWN_GRACE` (55s).
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
- 502 `upstream_error` — `code: "upstream_unreachable"` on dial failure;
  `code: "upstream_invalid_response"` on 200 + unparseable JSON. A client
  cancel while the upstream request is in flight is the WARN
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
  Exception: `internal/auth`'s PostgreSQL integration tests (store
  lifecycle, migration idempotence, foreign-schema fail-closed,
  forward-only history) run against a real server only when
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
  gopkg.in/yaml.v3 + jackc/pgx v5 (database/sql driver, partner key
  store only), stdlib tests only; no Makefile — commands live in
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
