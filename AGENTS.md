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

| Directory                        | Owns                                                                                                                                                                                                 |
| -------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `internal/config`                | Bootstrap env parsing (`LoadBootstrap`), runtime YAML (`LoadRuntime`, strict decode via `yaml.v3` known fields), snapshot store (`Store`/`Snapshot`, atomic pointer), content-hash poller (`Poller`) |
| `internal/inject`                | Pure request transforms: `Probe` (model + stream detection), `Chat`, `Responses`, `RewriteModel` (byte-preserving)                                                                                   |
| `internal/proxy`                 | HTTP handler wiring, upstream client, error envelopes, SSE copying (`CopySSE`)                                                                                                                       |
| `internal/server`                | Listener lifecycle and graceful shutdown (`Server.Run`)                                                                                                                                              |
| `cmd/openai-compatible-injector` | Entrypoint: subcommands `version`, `healthcheck`, default serve                                                                                                                                      |
| `e2e`                            | Black-box tests driving the real binary as a subprocess                                                                                                                                              |

## Non-negotiables

- **Two config planes.** Bootstrap settings (`LISTEN`, `CONFIG_FILE`,
  `CONFIG_POLL_INTERVAL`, `SHUTDOWN_GRACE`) come from the environment and
  are enforced by the runtime file's strict decoding: a runtime file
  defining them is rejected. Runtime model mapping lives in YAML only.
- **Invalid initial config = startup failure; invalid reload = last-known-good.**
  `LoadRuntime` failure at boot exits 1. `Poller.Run` on any failure logs and
  keeps the previous snapshot.
- **One snapshot per request.** A handler calls `store.Load()` exactly once
  and binds the whole request — including any active stream — to that
  snapshot forever. Reloads never affect in-flight work.
- **Injection must never corrupt.** Chat prepends to `messages` only when it
  is a JSON array; Responses merges into `instructions` (string, array, or
  absent) and touches nothing else. Empty prompt = no injection.
- **`RewriteModel` is byte-preserving.** Only object-key `"model"` string
  values are replaced (top-level for both APIs; nested `response.model` for
  Responses envelopes) inside a string-state-aware scan. Unparseable input
  returns the input unchanged. Never re-serialize.
- **Streaming branches on the URL path**, not the body: chat = `data:`
  lines + `data: [DONE]`; responses = `event:`+`data:` pairs, no `[DONE]`
  (Responses termination events pass through untouched). `CopySSE` flushes
  per line, grows its buffer without a cap, and rewrites only `data:` lines
  containing a model string.
- **Verbose verbatim, loud local.** 4xx/5xx upstream responses forward byte
  for byte. A 200 that is not JSON becomes 502 `upstream_invalid_response`;
  dial failure is 502 `upstream_unreachable`; unmapped model is 404
  `model_not_found` and is NEVER forwarded.
- **Never log or leak credentials.** No `Authorization`, keys, request
  bodies, or injection prompts in logs or error text. A quote of these is
  a security defect (SECURITY.md), not a typo.
- **Graceful shutdown.** `signal.NotifyContext(SIGINT, SIGTERM)` →
  `Shutdown(grace)` → force `Close()` on overflow → `CloseIdleConnections` →
  exit 0. Second signal forces exit 1. Compose `stop_grace_period`
  (60s) > default `SHUTDOWN_GRACE` (55s).
- **Healthcheck never reads YAML.** It probes `GET /healthz` (200 +
  `"ok\n"`) so a poisoned reload cannot fail the container probe.

## Error envelope contract (public API)

- 400 `invalid_request_error` — body not JSON, or missing `model`.
- 404 `model_not_found` — exact shape
  `{"error":{"message":"The model '<X>' does not exist or you do not have access to it.","type":"invalid_request_error","param":null,"code":"model_not_found"}}`.
- 502 `upstream_error` — `code: "upstream_unreachable"` on dial failure;
  `code: "upstream_invalid_response"` on 200 + unparseable JSON.
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
- release-please (go type, 0.1.0 baseline) owns CHANGELOG.md/tags; Docker
  publishes ghcr.io/ecoma-io/openai-compatible-injector on release.
- AI-assisted commits carry `Assisted-by:`/`Generated-by:` trailers on the
  last commit of the PR, per CONTRIBUTING.md.
