# openai-compatible-injector

A minimal OpenAI-compatible **request/response injector proxy**. Clients talk
to it as if it were an OpenAI endpoint; it forwards to configured upstream
providers, renaming the model and injecting a per-model system prompt into
every request. Hot-reloadable model mapping, no telemetry, one static
binary.

```
 client ──POST /v1/chat/completions──▶ injector ──forward (model→upstream-model, prompt injected)──▶ upstream provider
         ◀──model rewritten to public name──●
```

Supports the two model-serving protocols:

- **Chat Completions** — `POST /v1/chat/completions`, including SSE streams.
- **Responses API** — `POST /v1/responses`, including SSE streams.

Everything else is deliberately out of scope (see [Out of scope](#out-of-scope)).

## What it is for

You run one provider gateway for several downstream providers or several
accounts, and you want every client to see a single model name no matter
which upstream actually serves it — with a mandatory system instruction
co-injected into every request for that model.

Common shapes:

- `gpt-reviewer` → `https://provider-a.example/v1`, upstream model
  `gpt-5-pro`, prompt _"review this code rigorously"_ — every client request
  for `gpt-reviewer` carries that instruction upstream, and every response
  names `gpt-reviewer`, never `gpt-5-pro`.
- A plain alias without injection: `echo-model` → `gpt-4o-mini`.

One model name, one upstream, one prompt. Mapping is per public model name;
there are no routes, weights, or per-request overrides.

## Quick start

```sh
cp config.example.yaml config.yaml   # edit the model mapping
docker compose up -d                 # listens on :8080
```

```sh
curl http://127.0.0.1:8080/v1/chat/completions \
  -H "Authorization: Bearer <client-token>" \
  -H "Content-Type: application/json" \
  -d '{"model":"gpt-reviewer","messages":[{"role":"user","content":"optimize this"}]}'
```

The upstream receives `model: gpt-5-pro` with a
`{"role":"system","content":"Review the following code…"}` message prepended.
The response says `"model":"gpt-reviewer"` whether you asked for streaming or
not.

## Configuration — two planes

Configuration is deliberately split. **Bootstrap settings** (where to listen,
which file to load, how often to poll) come from the **environment**. The
**runtime mapping** (which models, which endpoints, which prompts) comes from
the **YAML file**. The runtime file cannot redefine bootstrap settings, and
the environment cannot define models.

Division of responsibility:

| Concern                                                                   | Where it lives          |
| ------------------------------------------------------------------------- | ----------------------- |
| `LISTEN`, `CONFIG_FILE`, `CONFIG_POLL_INTERVAL`, `SHUTDOWN_GRACE`         | Environment (bootstrap) |
| `models.<name>.{endpoint,upstream-model,injection-prompt,thinking-usage}` | YAML file (runtime)     |
| `logging.level`                                                           | YAML file (runtime)     |

### Bootstrap environment

| Variable               | Default               | Meaning                                                                                                                                                                 |
| ---------------------- | --------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `LISTEN`               | `:8080`               | Address the HTTP listener binds (`host:port`; wildcard accepted)                                                                                                        |
| `CONFIG_FILE`          | `/config/config.yaml` | Path of the runtime YAML file, read at boot then polled                                                                                                                 |
| `CONFIG_POLL_INTERVAL` | `1s`                  | How often the file's content hash is re-checked                                                                                                                         |
| `SHUTDOWN_GRACE`       | `55s`                 | Drain budget on SIGTERM/SIGINT before connections are force-closed; must be greater than zero — `0` is rejected at boot (a zero grace would silently disable the drain) |

There is no `LOG_LEVEL` environment variable — it was removed together with
the introduction of `logging.level` in the runtime file, which hot-reloads.
The runtime file is mandatory at boot, so an environment override had no
window in which it could take effect; one setting has exactly one source of
truth.

### Runtime YAML

```yaml
models:
  gpt-reviewer:
    endpoint: https://api.provider.example/v1 # required; no credentials, no /chat/completions suffix
    upstream-model: gpt-5-pro # required; the model name sent upstream
    injection-prompt: | # optional; empty/omitted disables injection
      Review the following code rigorously. Report every bug you can find,
      ordered by severity, and suggest a fix for each.
    thinking-usage: # optional; absent/null = off (responses byte-identical)
      mode: auto # required when the block is present: auto | always | off
      min-ratio: 0.6 # optional; finite, 0..1
      max-ratio: 0.9 # optional; finite, 0..1; min-ratio <= max-ratio
  echo-model:
    endpoint: https://api.provider.example/v1
    upstream-model: gpt-4o-mini

logging:
  level: info # optional; debug | info | warn | warning | error (absent = info)
```

- `endpoint` — base URL of the upstream provider. Scheme `http` or `https`
  only; port and path allowed, trailing slashes ignored; URL userinfo is
  rejected. Requests are sent to `<endpoint>/chat/completions` and
  `<endpoint>/responses`.
- `upstream-model` — the `model` value actually forwarded upstream.
- `injection-prompt` — the system instruction injected into every request for
  this model. Multi-line supported; the exact text is used verbatim.
- `thinking-usage` — optional block configuring simulated thinking-usage
  synthesis: `mode` is required when the block is present (`auto` — only when
  the request signals thinking; `always` — every request, overriding the
  request's signal; `off` — never) and `min-ratio`/`max-ratio` bound the
  share of output tokens attributed to thinking (each optional, finite, in
  `[0,1]`, with `min-ratio ≤ max-ratio`; both absent → fixed `0.75`, one set →
  fixed to it). An absent or null block means off — responses stay
  byte-identical to an unconfigured deployment.

The file is validated strictly, in two layers:

- **Top-level keys** are checked against the raw YAML: only `models` and
  `logging` are legal. This is the bootstrap-plane rule — a file that tries
  to define `listen`, `config-file`, `config-poll-interval` or
  `shutdown-grace` is rejected whatever its value's shape (a strict struct
  decode alone misses a bootstrap key whose value is an empty map).
- **Model entries and the logging section** are decoded strictly (`yaml.v3`
  with known fields): any key outside `endpoint`, `upstream-model`,
  `injection-prompt` and `thinking-usage` (and, inside the block, outside
  `mode`, `min-ratio`, `max-ratio`) — including a nested bootstrap key — is a
  rejection, not a warning, and so is any key inside `logging` other than
  `level`.

The `models` table itself must contain at least one model. An empty table is
rejected — an empty file is what a truncate-then-write config edit looks
like mid-write, and accepting it would silently drop every model from the
live service; rejecting it lands the reload on the last-known-good path.

## Hot reload

The process polls `CONFIG_FILE` for a SHA-256 content change every
`CONFIG_POLL_INTERVAL`. When the content changes:

1. New content is parsed and validated.
2. **Valid** → a new snapshot is published atomically; subsequent requests
   bind to it.
3. **Invalid** → the change is logged and the **last-known-good** config
   keeps serving. A broken reload never takes the service down.

Semantics that hold:

- **One snapshot per request.** Each request binds exactly one snapshot at
  entry — including a stream. Reload `N → N+1` never affects an in-flight
  request or stream; a stream bound to `N` finishes naming models from `N`.
- **Startup is the opposite side of the coin.** An invalid initial file is a
  **startup failure** (the process exits 1) — the last-known-good rule only
  applies to reloads, because at boot there is no last-known-good.
- **Unchanged file, no churn.** If the content hash is stable, nothing is
  republished; the generation number is stable too.
- **One document per file.** A `---`-separated multi-document YAML file is
  rejected: a decoder that reads only the first document would silently
  hide the rest — including a bootstrap-plane key appended after a
  separator — which is exactly the shape a two-plane violation takes.
- **Rejection errors never quote operator input.** Error text reaches logs
  verbatim (fatal at boot, WARN on reload), and a botched paste into any
  YAML position can carry credentials — so an invalid value is reported by
  position, length, and line number, never by content.
- **The log level hot-reloads with everything else.** `logging.level` rides
  the same validate-then-publish path as the model mappings: a valid reload
  applies the new level process-wide without a restart, a restart, or any
  signal; an invalid `level` value rejects the whole file onto the
  last-known-good path. The level swap is an atomic store zerolog consults
  per event, so in-flight requests race only the old/new boundary and never
  block. Every successful swap is acknowledged by `log_level_applied`,
  emitted _after_ the swap and _at the new level_ — the only severity
  guaranteed visible under the level it announces — carrying `generation`,
  `previous_level`, and `log_level`. (The companion `config_reloaded` INFO
  line is written before the swap, so it disappears on transitions out of
  `warn`/`error`; the ack exists so no transition is ever silent.) A
  rejected file acknowledges nothing and leaves the level in force.
- **Atomic replace caveat.** The poller watches the file's content, and reads
  it by path; tools that replace a file by `mv`/rename (editor safe-save)
  swap in a new inode the read still follows — but if the process opened the
  old inode, the change can be missed until a subsequent write. Editing in
  place is the reliable path. See
  `compose.yaml` for the same caveat on the single-file bind mount.

## Injection behavior

What "inject a system prompt" means, per API. **The request is never
corrupted to inject**: if the target shape is absent or of an unexpected
type, the request passes through untouched (and the empty prompt injects
nothing).

### Chat Completions (`/v1/chat/completions`)

```json
{
  "model": "gpt-reviewer",
  "messages": [{ "role": "user", "content": "optimize this" }]
}
```

becomes, upstream:

```json
{
  "model": "gpt-5-pro",
  "messages": [
    { "role": "system", "content": "Review the following code…" },
    { "role": "user", "content": "optimize this" }
  ]
}
```

The prompt is prepended at index 0 as a `system` message. If `messages` is
absent or not a JSON array, the request is forwarded unchanged (aside from
the model rename).

### Responses API (`/v1/responses`)

`instructions` is the Responses equivalent of a system prompt, and it accepts
several shapes:

| Request `instructions` | Upstream result                                                                                                                    |
| ---------------------- | ---------------------------------------------------------------------------------------------------------------------------------- |
| absent                 | `instructions` = the prompt (as a string)                                                                                          |
| string `"text"`        | `"prompt\n\ntext"` — prompt first, blank line, existing text                                                                       |
| array                  | a developer message item is prepended: `{"type":"message","role":"developer","content":[{"type":"input_text","text":"<prompt>"}]}` |
| anything else          | untouched                                                                                                                          |

## Model mapping and rewriting

- **Forward:** a request's top-level `model` is replaced with the mapping's
  `upstream-model`.
- **Reverse,** scoped per API surface: in _responses_, the model is rewritten
  back to the public name — the top-level `model` field (chat: every streamed
  chunk) and, for the Responses API only, the nested `response.model` field
  of envelope events. A chat chunk carrying a nested `response` object is
  client data: its model is **not** ours to rewrite. `RewriteChatModel`
  owns the top-level key alone; `RewriteResponsesModel` additionally owns
  `response.model`.

Both rewrites are **byte-preserving**: only object-key `"model"` string
values are replaced inside a string-state-aware scan. Everything else — every
whitespace byte, key order, unknown fields — is forwarded exactly as
received. A response whose JSON cannot be parsed is forwarded byte-for-byte
unchanged.

## Streaming

SSE streams pass through **incrementally, line by line** — nothing is
buffered up-front and flushed at the end, so a slow upstream produces a slow,
live stream with correct per-chunk latency. Behavior:

- Lines are written out as they are read, and flushed to the client at
  every event boundary — the blank line that terminates an event, which is
  what SSE clients dispatch on. Input is **bounded**: a single line is
  capped at 1 MiB and an in-flight event (the lines since the last blank
  separator, the dispatching blank line excluded) at 2 MiB. Real provider
  events are far below both; the caps exist so a hostile or broken upstream
  cannot pin unbounded memory. Crossing a cap stops the relay cleanly — the
  offending line is never forwarded, and the request is logged with the
  `stream_limit_exceeded` outcome.
- The streaming _shape_ is decided by the **URL path**, not the body:
  - Chat Completions: `data:` lines, terminated by `data: [DONE]`.
  - Responses API: `event:`/`data:` pairs. **No `[DONE]`** — Responses
    termination events are part of the protocol and pass through untouched.
- Only `data:` lines whose JSON carries an in-scope `model` string value are
  rewritten — the top-level key (and, for responses, the envelope's
  `response.model`). Model text appearing anywhere else in the payload — a
  substring of a message, another field's value — never matches.
  `event:`, comments, and non-model `data:` lines pass through verbatim.
- Malformed lines are forwarded verbatim. We are a passthrough, not an SSE
  validator.
- Known limitation: lines are terminated by `\n` (with `\r\n` accepted) —
  the SSE standard and everything real providers emit. Bare-CR line endings
  (no `\n`) would not be treated as line boundaries.
- A request with `"stream": true` against an upstream that answers with a
  normal JSON body is handled as a plain 200 (the body is model-rewritten,
  not wrapped, not streamed).
- Known limitation: which 2xx handling applies is decided by the upstream's
  `Content-Type` alone. A 200 under `text/event-stream` is relayed line by
  line even if the body is not actually SSE — such a body carries no
  rewriteable `data:` lines, so a non-conforming upstream that mislabels a
  JSON body as `text/event-stream` would pass its upstream model alias
  through unrewritten (conforming providers never do this). Symmetrically,
  an upstream that streams SSE at a `"stream": false` request gets its body
  buffered, fails the JSON validation, and surfaces as the documented 502
  `upstream_invalid_response` — the fog belongs to the upstream, and the
  access log's outcome says so.
- Reloads never interrupt a stream: it is bound to its request's snapshot.

## Errors

Upstream and client failures are classified, never fogged:

| Condition                                                                                                       | Status                 | `error.type` / `code`                                                                                                                                                                          |
| --------------------------------------------------------------------------------------------------------------- | ---------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Body is not JSON                                                                                                | 400                    | `invalid_request_error` — exact body: `{"error":{"message":"invalid JSON in request body","type":"invalid_request_error","param":null,"code":null}}`                                           |
| Missing `model`                                                                                                 | 400                    | `invalid_request_error` — exact body: `{"error":{"message":"you must provide a model parameter","type":"invalid_request_error","param":null,"code":null}}`                                     |
| Request body over the 64 MiB cap                                                                                | 413                    | `invalid_request_error` — exact body: `{"error":{"message":"request body too large","type":"invalid_request_error","param":null,"code":null}}`                                                 |
| Request names an unmapped model                                                                                 | 404                    | `model_not_found` — exact body: `{"error":{"message":"The model '<X>' does not exist or you do not have access to it.","type":"invalid_request_error","param":null,"code":"model_not_found"}}` |
| Request path matches no route (unknown path, trailing slash, wrong case)                                        | 404                    | `invalid_request_error` — exact body: `{"error":{"message":"Invalid URL (<METHOD> <PATH>)","type":"invalid_request_error","param":null,"code":null}}`                                          |
| Upstream unreachable (dial/network)                                                                             | 502                    | `upstream_error` / `upstream_unreachable`                                                                                                                                                      |
| Upstream 200 with unparseable body (or body over the 64 MiB buffered cap, or a body read that fails mid-answer) | 502                    | `upstream_error` / `upstream_invalid_response`                                                                                                                                                 |
| Upstream answers 3xx/4xx/5xx                                                                                    | **forwarded verbatim** | status, bytes, and an allow-list of headers pass through (see below)                                                                                                                           |

Two consequences of the table:

- **An unmapped model is never forwarded.** 404 is local; the upstream never
  sees that request. This is a hard boundary (SECURITY.md treats its breach
  as a vulnerability).
- **Upstream errors are the upstream's shape.** Any 3xx/4xx/5xx — JSON, text,
  whatever the provider sent — is relayed byte-for-byte. Redirects are
  **never followed**: following one would silently convert the POST into a
  body-less GET (301/302/303) and replay the transformed request body to
  whatever the `Location` names (307/308). An unexpected 3xx is the
  upstream's answer, and the client's to judge. We only synthesize errors
  for what the upstream _did not_ deliver.
- **Error bodies can name the upstream model.** Verbatim means verbatim: an
  upstream error that quotes its own model name discloses the alias target.
  That is the price of honest passthrough; we do not rewrite error bodies.
- There is **no overall request timeout**. A slow upstream is a slow
  response, not a timeout race. Dial and TLS handshake timeouts bound the
  connection phase only.

**Relayed response headers** (an allow-list, everything else is dropped):
`Content-Type`, `Cache-Control`, `X-Request-Id`, `OpenAI-Request-Id`,
`Retry-After`, `Location`, `X-RateLimit-Limit`, `X-RateLimit-Remaining`,
`X-RateLimit-Reset`, `X-RateLimit-Reset-Requests`, `X-RateLimit-Reset-Tokens`.
Rate-limit and retry headers are load-bearing for client backoff; dropping
them would make a 429 indistinguishable from any other upstream failure.

## Safety and credentials

- The `Authorization` header is forwarded to the configured upstream
  untouched.
- **Credentials never reach logs or error text** — no `Authorization`
  values, request bodies, or injection prompts in log lines, and no
  upstream URL details beyond the endpoint's scheme+host in **any** log
  line or error text (a query-parameter API key survives even a dial
  failure). A quote of any of these is a security defect, not a typo
  (SECURITY.md).
- `endpoint` URLs with userinfo are rejected at config load; fragments are
  rejected too (a fragment is never sent to a server, so accepting one would
  silently ignore part of the configured endpoint). An endpoint's query
  string is preserved and sent with every request — that is how providers
  that authenticate via query parameter (e.g. `api-version`) work. The
  client's own query string, by contrast, is dropped: only the configured
  endpoint defines where a request goes, and a client-supplied
  `?api-key=` must never travel.

## Logging

JSON lines on stderr only (zerolog; stdout is never written). Every line is
machine-parseable and carries `level`, `time`, and a stable snake_case
`message` slug. Levels are `debug`, `info`, `warn` (the runtime file also
accepts the spelling `warning`), and `error`, defaulting to `info`; the
level is hot-reloadable through `logging.level` (see Hot reload).

What each level carries:

- **DEBUG** — the full request lifecycle, every event bound to its
  `request_id`: `request_received` (method/path/remote address),
  `probe_completed` (model + stream flag), `model_resolved` (public model,
  upstream model, upstream scheme+host origin), `request_transform_started`/
  `request_transform_completed` (byte counts around prompt injection),
  `upstream_request_started` (origin + forwarded byte count),
  `upstream_response_received` (upstream status + content type). From there
  the lifecycle forks: a buffered response continues with
  `response_transform_started`/`response_transform_completed` (byte counts
  around the model rewrite) and `client_write_completed`; a streamed
  response instead emits `stream_started`, periodic
  `stream_event_progress` heartbeats (running event/byte counts, one every
  256 dispatched events — a stuck stream shows up as a heartbeat that
  stops advancing), and `stream_completed`. Plus the poller's per-tick
  debug heartbeat while a config failure persists (the healthy unchanged
  state logs nothing at all). Detailed but never payload-bearing: request
  bodies, SSE `data:` payloads, and injection prompts do not exist at this
  level — or at any level.
- **INFO** — one `request_completed` per proxied request with the wire
  facts: `request_id` (16 hex chars, generated per request), `api`
  (`chat`/`responses`), `status`, `outcome`, `public_model`, `stream`,
  `bytes_in`, `bytes_out`, `duration_ms`, and `config_generation` (the
  snapshot generation the request bound to — correlating reloads with
  behavior). The event is emitted when the request finishes, under the
  level in effect at that moment — a reload mid-request can therefore
  change whether it appears. Also `config_reloaded` (`generation`,
  `model_count`, `log_level`), `config_file_recovered` (a file returned
  byte-identical after a failure), `service_started` (boot config
  accepted; the listener itself is announced by the DEBUG
  `listener_ready`), and `drain_started`.
- **WARN** — client disconnects and truncations (`stream_truncated` with a
  `phase` field separating `client_write` from `upstream_read` and
  `upstream_limit`, and `relay_copy_failed` with phase `client_write` on
  the verbatim and buffered paths — the buffered case covers a client whose
  cancel surfaces through the upstream body read, with no envelope written
  to the connection that is already gone), a response that never landed because the client was
  already gone — a buffered body or any locally generated error envelope
  (`client_write_failed`, outcome `client_disconnected`, superseding the
  envelope's own classification), an upstream that
  died mid-body before the answer could be parsed
  (`upstream_body_read_failed`, outcome `upstream_read_failed`), a client
  that cancels mid-request — including while the upstream request is in
  flight (`upstream_request_failed` with `error_class` `client_canceled`,
  outcome `client_disconnected`, and no error envelope, since the client is
  gone) — one warning per transition into a failed config
  state (`config_file_unreadable`, `config_reload_rejected`) — including a
  failure that changes kind, which warns again — never one per poll tick —
  plus `second_signal_forced_exit` and drain overflow.
- **ERROR** — upstream connection failures (`upstream_request_failed` with
  an `error_class` such as `connection_refused`, `timeout`, `tls`, `dial` —
  never `client_canceled`, which is the WARN disconnect above) and an
  upstream that died mid-relay on the verbatim path (`relay_copy_failed`
  with phase `upstream_read` — the one relay failure that is not a
  disconnect), plus anything fatal at startup. A 200 that is not
  parseable JSON is not an event of its own: it surfaces only as the
  `upstream_invalid_response` outcome on the INFO completion line, with
  the 502 envelope on the wire.

The credential rule is absolute: no log line, at any level, ever contains
an `Authorization` value, a request or response body, an injection prompt,
or upstream URL detail beyond scheme+host. Endpoint query strings (which
providers use for API keys) survive even a dial failure's error text —
errors are sanitized before logging. The planted-secret E2E suite
(`TestLoggingNeverLeaksSecrets`) holds this rule under success, streaming,
rejection, dial-failure, and verbatim-echo traffic at maximum verbosity.

## Healthcheck

`GET /healthz` answers `200` with the body `ok\n` whenever the HTTP listener
is up.

The `healthcheck` subcommand (used by the Docker image and compose) probes
the running service and requires `200` + `ok\n`:

```sh
./openai-compatible-injector healthcheck    # LISTEN env decides what is probed
```

It reads only the `LISTEN` environment variable (a wildcard address is
rewritten to the loopback) and **never reads the YAML file** — a poisoned
reload must not fail the container probe.

## Graceful shutdown

On SIGTERM or SIGINT the service stops accepting new connections and drains:

1. `http.Server.Shutdown(grace)` — in-flight requests and streams get up to
   `SHUTDOWN_GRACE` (default 55s) to complete.
2. If the budget runs out, `Close()` force-terminates the remainder.
3. Idle keep-alive connections are closed; the process exits `0`.

A second signal while draining forces an immediate `exit 1`. Compose's
`stop_grace_period: 60s` is deliberately larger than the default drain
budget so Docker's SIGKILL never cuts a drain short.

## Deployment

### Docker

```sh
docker build --build-arg VERSION=0.1.0-dev -t openai-compatible-injector .
docker run --rm -p 8080:8080 \
  -e LISTEN=:8080 -e CONFIG_FILE=/app/config.yaml \
  -v "$PWD/config.yaml:/app/config.yaml:ro" \
  openai-compatible-injector
```

- Published image: `ghcr.io/ecoma-io/openai-compatible-injector` (on
  release; multi-arch amd64/arm64 — see [release.yml](.github/workflows/release.yml)).
- Runs as UID 65532 on `scratch` — exactly one static binary plus CA
  certificates. No shell; the Docker `HEALTHCHECK` uses the binary's own
  `healthcheck` subcommand.

### Compose

`compose.yaml` wires the same shape: `8080:8080`, read-only bind mount of
`config.yaml`, healthcheck, `stop_grace_period: 60s`, bounded JSON logging,
`restart: unless-stopped`.

> **Compose + atomic edits:** the `config.yaml` bind mount is a single file,
> and a host-side `mv`/safe-save splices in a new inode the mount does not
> follow. Edit the file in place (the service re-reads each poll), or
> restart the container after a replace.

## Building from source

```sh
gofmt -w .
go vet ./...
go test -race ./...
go build -ldflags "-X main.version=0.1.0-dev" -o bin/openai-compatible-injector ./cmd/openai-compatible-injector
```

## Testing

- **Unit tests** (`internal/**/*_test.go`, stdlib only, race-clean) cover the
  config planes, strict-decode rejections, snapshot store concurrency,
  poller last-known-good behavior, inject transforms, model rewriting, SSE
  copying, and server shutdown ordering.
- **E2E suite** (`e2e/`) drives the real binary as a subprocess against
  in-process fake upstreams: forwarding, injection, streaming, hot reload,
  drain, startup failures, plane violations. Everything it needs is Go —
  no Docker required:

  ```sh
  go test ./e2e/ -count=1 -timeout 25m   # or -short for unit-only
  ```

- CI (`ci-gate`): gofmt, `go vet`, `golangci-lint` (checksum-pinned), race
  tests, build with `-X main.version`, pull-request title commitlint.
- CI (`analysis-gate`): CodeQL (Go + Actions), Semgrep (own rules with
  fixtures, pinned container), Gitleaks (checksum-pinned binary) — each
  aggregated into a single required check name.

## Operations

- Logs are JSON on stderr. Every reload decision is logged: generation
  number, applied/kept-last-known-good. `logging.level: debug` in the
  runtime file (hot-reloadable) adds per-request routing lines (still never
  bodies or credentials).
- `./openai-compatible-injector version` prints the build version (injected
  via `-X main.version`, or the release tag in published images).
- Sending a second SIGTERM/SIGINT during drain aborts immediately with
  exit 1 — by design, for orchestrators that need a hard stop. After the
  drain finishes, duplicate signals are ignored: the process keeps the exit
  code it earned.
- Connection hygiene is bounded: request bodies are capped at 64 MiB,
  request headers must arrive within 10s, idle keep-alive connections
  are closed after 120s, and SSE relay input is capped per line and per
  event (see [Streaming](#streaming)) — a quiet client cannot pin a
  goroutine and a file descriptor forever, and a hostile upstream cannot
  pin unbounded memory. An active response (including a long SSE stream)
  is never touched by the idle timeout.

## Out of scope

Decided, and not coming back without a design discussion:

- **More OpenAI surfaces** — embeddings, batch, assistants, etc. This is an
  injector for the two model-serving protocols.
- **Manual reload triggers** (SIGHUP, fsnotify) — the content poller is the
  design; see the atomic-replace caveat above for the blind spot no watcher
  fixes.
- **Per-request overrides** of prompt or upstream model — the mapping is
  static per public name; a request field that changes forwarding is a
  footgun.
- **Proxy and TLS configuration** — upstream connections follow the standard
  `HTTP_PROXY`/`HTTPS_PROXY`/`NO_PROXY` environment variables (inherited
  from `net/http`'s default transport) and verify chain and host with
  system roots; custom TLS setup (client certificates, custom CA pools,
  `insecure-skip-verify`) is not coming.
- **Authz on the inbound side** — requests are forwarded as received; the
  service is not an identity boundary.

## Repository layout

```
cmd/openai-compatible-injector/  entrypoint + version/healthcheck subcommands
internal/config/                 bootstrap, runtime YAML, snapshot store, poller
internal/inject/                 pure request transforms (probe, chat, responses, rewrite)
internal/proxy/                  handler, upstream client, SSE copy, error envelopes
internal/server/                 listener + graceful shutdown
e2e/                             black-box subprocess suite
config.example.yaml              documented runtime config template
compose.yaml, Dockerfile         deployment
```

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) — solving real problems, the quiet
direction, signed commits, Conventional Commits, squash merges.
[AGENTS.md](AGENTS.md) carries the working guidance and invariants for
AI-assisted development. Bugs and feature proposals go through
[issues](https://github.com/ecoma-io/openai-compatible-injector/issues);
security-shaped defects go through [SECURITY.md](SECURITY.md) and a private
advisory, never a public issue.

## License and acknowledgements

Apache License 2.0 — see [LICENSE](LICENSE). Built on Go, with
`rs/zerolog` and `gopkg.in/yaml.v3`. This project was developed with AI
assistance; the AI-assisted disclosure policy is described in
[CONTRIBUTING.md](CONTRIBUTING.md).
