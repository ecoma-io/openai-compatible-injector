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

| Concern                                                           | Where it lives          |
| ----------------------------------------------------------------- | ----------------------- |
| `LISTEN`, `CONFIG_FILE`, `CONFIG_POLL_INTERVAL`, `SHUTDOWN_GRACE` | Environment (bootstrap) |
| `models.<name>.{endpoint,upstream-model,injection-prompt}`        | YAML file (runtime)     |

### Bootstrap environment

| Variable               | Default               | Meaning                                                            |
| ---------------------- | --------------------- | ------------------------------------------------------------------ |
| `LISTEN`               | `:8080`               | Address the HTTP listener binds (`host:port`; wildcard accepted)   |
| `CONFIG_FILE`          | `/config/config.yaml` | Path of the runtime YAML file, read at boot then polled            |
| `CONFIG_POLL_INTERVAL` | `1s`                  | How often the file's content hash is re-checked                    |
| `SHUTDOWN_GRACE`       | `55s`                 | Drain budget on SIGTERM/SIGINT before connections are force-closed |
| `LOG_LEVEL`            | `info`                | `debug`, `info`, `warn`, `error` (JSON logs to stderr)             |

### Runtime YAML

```yaml
models:
  gpt-reviewer:
    endpoint: https://api.provider.example/v1 # required; no credentials, no /chat/completions suffix
    upstream-model: gpt-5-pro # required; the model name sent upstream
    injection-prompt: | # optional; empty/omitted disables injection
      Review the following code rigorously. Report every bug you can find,
      ordered by severity, and suggest a fix for each.
  echo-model:
    endpoint: https://api.provider.example/v1
    upstream-model: gpt-4o-mini
```

- `endpoint` — base URL of the upstream provider. Scheme `http` or `https`
  only; port and path allowed, trailing slashes ignored; URL userinfo is
  rejected. Requests are sent to `<endpoint>/chat/completions` and
  `<endpoint>/responses`.
- `upstream-model` — the `model` value actually forwarded upstream.
- `injection-prompt` — the system instruction injected into every request for
  this model. Multi-line supported; the exact text is used verbatim.

The file is validated strictly, in two layers:

- **Top-level keys** are checked against the raw YAML: only `models` is
  legal. This is the bootstrap-plane rule — a file that tries to define
  `listen`, `config-file`, `config-poll-interval` or `shutdown-grace` is
  rejected whatever its value's shape (a strict struct decode alone misses
  a bootstrap key whose value is an empty map).
- **Model entries** are decoded strictly (`yaml.v3` with known fields):
  any key outside `endpoint`, `upstream-model`, `injection-prompt` —
  including a nested bootstrap key — is a rejection, not a warning.

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
- **Reverse:** in _responses_, the model is rewritten back to the public
  name — the top-level `model` field (chat: every streamed chunk) and the
  nested `response.model` field of Responses envelope events.

`RewriteModel` is **byte-preserving**: only object-key `"model"` string
values are replaced inside a string-state-aware scan. Everything else — every
whitespace byte, key order, unknown fields — is forwarded exactly as
received. A response whose JSON cannot be parsed is forwarded byte-for-byte
unchanged.

## Streaming

SSE streams pass through **incrementally, line by line** — nothing is
buffered up-front and flushed at the end, so a slow upstream produces a slow,
live stream with correct per-chunk latency. Behavior:

- Every line is flushed to the client as soon as it is read. The internal
  line buffer grows without a cap: providers pad chunks and there is no line
  length ceiling to impose.
- The streaming _shape_ is decided by the **URL path**, not the body:
  - Chat Completions: `data:` lines, terminated by `data: [DONE]`.
  - Responses API: `event:`/`data:` pairs. **No `[DONE]`** — Responses
    termination events are part of the protocol and pass through untouched.
- Only `data:` lines whose JSON contains a model string are rewritten.
  `event:`, comments, and non-model `data:` lines pass through verbatim.
- Malformed lines are forwarded verbatim. We are a passthrough, not an SSE
  validator.
- A request with `"stream": true` against an upstream that answers with a
  normal JSON body is handled as a plain 200 (the body is model-rewritten,
  not wrapped, not streamed).
- Reloads never interrupt a stream: it is bound to its request's snapshot.

## Errors

Upstream and client failures are classified, never fogged:

| Condition                           | Status                 | `error.type` / `code`                                                                                                                                                                          |
| ----------------------------------- | ---------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Body is not JSON                    | 400                    | `invalid_request_error` — exact body: `{"error":{"message":"invalid JSON in request body","type":"invalid_request_error","param":null,"code":null}}`                                           |
| Missing `model`                     | 400                    | `invalid_request_error` — exact body: `{"error":{"message":"you must provide a model parameter","type":"invalid_request_error","param":null,"code":null}}`                                     |
| Request names an unmapped model     | 404                    | `model_not_found` — exact body: `{"error":{"message":"The model '<X>' does not exist or you do not have access to it.","type":"invalid_request_error","param":null,"code":"model_not_found"}}` |
| Upstream unreachable (dial/network) | 502                    | `upstream_error` / `upstream_unreachable`                                                                                                                                                      |
| Upstream 200 with unparseable body  | 502                    | `upstream_error` / `upstream_invalid_response`                                                                                                                                                 |
| Upstream answers 4xx/5xx            | **forwarded verbatim** | status, headers, and bytes pass through untouched                                                                                                                                              |

Two consequences of the table:

- **An unmapped model is never forwarded.** 404 is local; the upstream never
  sees that request. This is a hard boundary (SECURITY.md treats its breach
  as a vulnerability).
- **Upstream errors are the upstream's shape.** Any 4xx/5xx — JSON, text,
  whatever the provider sent — is relayed byte-for-byte. We only synthesize
  errors for what the upstream _did not_ deliver.
- There is **no overall request timeout**. A slow upstream is a slow
  response, not a timeout race. Dial and TLS handshake timeouts bound the
  connection phase only.

## Safety and credentials

- The `Authorization` header is forwarded to the configured upstream
  untouched.
- **Credentials never reach logs or error text** — no `Authorization`
  values, request bodies, or injection prompts in log lines, and no
  upstream URL details beyond the endpoint's scheme+host in startup logs.
  A quote of any of these is a security defect, not a typo (SECURITY.md).
- `endpoint` URLs with userinfo are rejected at config load.

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
  drain, startup failures, plane violations. Everything needs is Go —
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
  number, applied/kept-last-known-good. `LOG_LEVEL=debug` adds per-request
  routing lines (still never bodies or credentials).
- `./openai-compatible-injector version` prints the build version (injected
  via `-X main.version`, or the release tag in published images).
- Sending a second SIGTERM/SIGINT during drain aborts immediately with
  exit 1 — by design, for orchestrators that need a hard stop.

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
- **Non-HTTPS(S) upstreams, proxies, TLS config** — endpoints verify chain
  and host with system roots; no `insecure-skip-verify`.
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
