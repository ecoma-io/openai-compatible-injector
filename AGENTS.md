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

| Directory                        | Owns                                                                                                                                                                                                                                                                                                                                      |
| -------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `internal/config`                | Bootstrap env parsing (`LoadBootstrap`), runtime YAML (`LoadRuntime`, strict decode via `yaml.v3` known fields, required client `api-key`, log level via `ParseLogLevel`), thinking-usage validation/normalization in `buildModel`, snapshot store (`Store`/`Snapshot`, atomic pointer), content-hash poller (`Poller`, `onPublish` hook) |
| `internal/inject`                | Pure request/response transforms: `Probe` (model + stream detection), `Chat`, `Responses`, `RewriteChatModel`/`RewriteResponsesModel` (byte-preserving, API-scoped), thinking plan + usage synthesizers (`ThinkingPlanFor`, `SynthesizeChat/ResponsesThinkingUsage`)                                                                      |
| `internal/proxy`                 | HTTP handler wiring, client bearer authentication/Authorization stripping, upstream client, error envelopes, SSE copying (`CopySSE`), composed response rewriter (`rewriteOut`: model rename + thinking-usage synthesis)                                                                                                                  |
| `internal/server`                | Listener lifecycle and graceful shutdown (`Server.Run`)                                                                                                                                                                                                                                                                                   |
| `cmd/openai-compatible-injector` | Entrypoint: subcommands `version`, `healthcheck`, default serve                                                                                                                                                                                                                                                                           |
| `e2e`                            | Black-box tests driving the real binary as a subprocess                                                                                                                                                                                                                                                                                   |

## Non-negotiables

- **Two config planes.** Bootstrap settings (`OAICR_LISTEN`,
  `OAICR_CONFIG_FILE`, `OAICR_CONFIG_POLL_INTERVAL`,
  `OAICR_SHUTDOWN_GRACE` — every environment variable the service reads is
  `OAICR_`-prefixed; no unprefixed fallback exists) come from the
  environment and
  are enforced by the runtime file's strict decoding: a runtime file
  defining them is rejected. The runtime file must be a single YAML
  document — a `---`-separated second document is a rejection (a decoder
  reading only the first would hide what follows). Runtime model mapping,
  the required `api-key`, and the hot-reloadable top-level
  `sse-keep-alive` block live in YAML only; the latter defaults to enabled
  at 15s and accepts a duration of at least 1s.
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
  Chat/Responses request presents the configured `api-key` as
  `Authorization: Bearer <key>`; the scheme is case-insensitive, and keys
  must be RFC 6750 bearer tokens (no whitespace or other invalid characters).
  A missing/malformed key gets the static missing-key 401, while a wrong key
  gets the static `invalid_api_key` 401. Authenticate before reading the body
  or upstream I/O; retain 405-before-401 ordering. `/healthz` and the 404
  catch-all stay unauthenticated. Consume — never forward or replace — the
  client's Authorization header; upstreams are trusted/internal.
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
- **Verbose verbatim, loud local.** 4xx/5xx upstream responses forward byte
  for byte. A 200 that is not JSON becomes 502 `upstream_invalid_response`;
  dial failure is 502 `upstream_unreachable`; unmapped model is 404
  `model_not_found` and is NEVER forwarded.
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
- Anything else from upstream (any 4xx/5xx) forwards verbatim.
- No overall request timeout; upstream timeouts surface as 502.

## Testing

- Unit tests co-located under `internal/`, stdlib only, deterministic.
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
  gopkg.in/yaml.v3, stdlib tests only; no Makefile — commands live in
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
