# openai-compatible-injector

A minimal OpenAI-compatible **request/response injector proxy**. Clients talk
to it as if it were an OpenAI endpoint — authenticating with the single
`api-key` from the runtime config — and it forwards to configured upstream
providers, renaming the model and injecting a per-model system prompt into
every request. Hot-reloadable model mapping, optional durable factual usage
metering, one static binary.

```
 client ──POST /v1/chat/completions (Bearer api-key)──▶ injector ──forward (model→upstream-model, prompt injected, credential consumed)──▶ upstream provider
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
cp config.example.yaml config.yaml   # edit the api-key and model mapping
docker compose up -d                 # listens on :8080
```

```sh
curl http://127.0.0.1:8080/v1/chat/completions \
  -H "Authorization: Bearer <client-token>" \
  -H "Content-Type: application/json" \
  -d '{"model":"gpt-reviewer","messages":[{"role":"user","content":"optimize this"}]}'
```

`<client-token>` must equal the `api-key` configured in `config.yaml`.

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

Every environment variable the service reads is `OAICR_`-prefixed; the
service consumes no unprefixed names of its own.

Division of responsibility:

| Concern                                                                                                                                          | Where it lives          |
| ------------------------------------------------------------------------------------------------------------------------------------------------ | ----------------------- |
| `OAICR_LISTEN`, `OAICR_CONFIG_FILE`, `OAICR_CONFIG_POLL_INTERVAL`, `OAICR_SHUTDOWN_GRACE`, `OAICR_AUTH_DATABASE_URL`, `OAICR_USAGE_DATABASE_URL` | Environment (bootstrap) |
| `api-key` (the shared inbound client credential)                                                                                                 | YAML file (runtime)     |
| `models.<name>.{endpoint,upstream-model,injection-prompt,thinking-usage,providers}`                                                              | YAML file (runtime)     |
| `provider-fallback.{enabled,max-attempts}`                                                                                                       | YAML file (runtime)     |
| `sse-keep-alive.{enabled,interval}`                                                                                                              | YAML file (runtime)     |
| `log-level`                                                                                                                                      | YAML file (runtime)     |

### Bootstrap environment

| Variable                     | Default                  | Meaning                                                                                                                                                                                                                                                                                |
| ---------------------------- | ------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `OAICR_LISTEN`               | `:8080`                  | Address the HTTP listener binds (`host:port`; wildcard accepted)                                                                                                                                                                                                                       |
| `OAICR_CONFIG_FILE`          | `/config/config.yaml`    | Path of the runtime YAML file, read at boot then polled                                                                                                                                                                                                                                |
| `OAICR_CONFIG_POLL_INTERVAL` | `1s`                     | How often the file's content hash is re-checked                                                                                                                                                                                                                                        |
| `OAICR_SHUTDOWN_GRACE`       | `55s`                    | Drain budget on SIGTERM/SIGINT before connections are force-closed; must be greater than zero — `0` is rejected at boot (a zero grace would silently disable the drain)                                                                                                                |
| `OAICR_AUTH_DATABASE_URL`    | _(empty — static mode)_  | PostgreSQL/TimescaleDB connection string of the partner key store. Absent keeps static mode; set, the process boots into partner mode (see "Partner API keys"). The value is never logged — not even its length                                                                        |
| `OAICR_USAGE_DATABASE_URL`   | _(empty — metering off)_ | PostgreSQL/TimescaleDB connection string of the durable usage-event store. Empty makes no database connection and preserves normal proxy traffic; set, startup migrates and validates the store before serving (see "Usage metering"). The value is never logged — not even its length |

There is no `LOG_LEVEL` environment variable — it was removed together with
the introduction of `log-level` in the runtime file, which hot-reloads.
The runtime file is mandatory at boot, so an environment override had no
window in which it could take effect; one setting has exactly one source of
truth.

### Runtime YAML

```yaml
# Required. Clients send this exact value as Authorization: Bearer <key>.
# It authenticates clients to this proxy only and is never forwarded upstream.
api-key: replace-with-a-secret-client-key

# Optional. Named outbound paths providers can share: direct (the default),
# exactly one proxy endpoint, or a pool of such endpoints with scheduling,
# eligibility and bounded fallback. See "Provider transports".
transports:
  egress:
    type: proxy # direct | proxy | pool; proxy requires the proxy URL below
    proxy: socks5h://user:pass@10.0.0.5:1080 # http | https | socks5 | socks5h, host + explicit port, optional userinfo auth
  lan:
    type: direct # must not set a proxy URL
  kilo-pool:
    type: pool # ordered member references to direct/proxy entries above
    members:
      - transport: lan # bare form, or a mapping with the gates below
        max-body-bytes: 4718592 # eligibility gate on the outgoing body
    strategy: round_robin # round_robin (default) | weighted_round_robin
    fallback:
      enabled: true # default true
      max-attempts: 3 # distinct endpoints dialed; default 3, cap 16
    health:
      enabled: true # default true
      failure-threshold: 3 # default 3
      cooldown: 30s # default 30s, min 1s

# Optional. Named upstream bases, each routed through a transport.
providers:
  opencode:
    base-url: https://api.opencode.example/v1 # validated like a model endpoint
    transport: egress # optional named transport; omitted = direct

models:
  gpt-reviewer:
    provider: opencode # either provider or endpoint — never both, never neither
    upstream-model: gpt-5-pro # required; the model name sent upstream
    injection-prompt: | # optional; empty/omitted disables injection
      Review the following code rigorously. Report every bug you can find,
      ordered by severity, and suggest a fix for each.
    thinking-usage: # optional; absent/null = off (responses byte-identical)
      mode: auto # required when the block is present: auto | always | off
      min-ratio: 0.6 # optional; finite, 0..1
      max-ratio: 0.9 # optional; finite, 0..1; min-ratio <= max-ratio
  echo-model:
    endpoint: https://api.provider.example/v1 # legacy inline form: an implicit direct provider
    upstream-model: gpt-4o-mini

# Optional. Client-facing SSE heartbeat; defaults to enabled: true and
# interval: 15s when this block is absent. interval must be a Go duration >= 1s.
sse-keep-alive:
  enabled: true
  interval: 15s

log-level: info # optional; debug | info | warn | error (absent = info)
```

- `api-key` — required shared [Bearer token](https://www.rfc-editor.org/rfc/rfc6750#section-2.1): non-empty ASCII letters, digits, `-`, `.`, `_`, `~`, `+`, `/`, and trailing `=` padding only. Clients present it as
  `Authorization: Bearer <key>` on both model-serving routes; the scheme is
  case-insensitive and outer spaces are ignored. It is bound
  to the [per-request config snapshot](#hot-reload), so rotating the YAML
  value affects subsequent requests without a restart. The key is credential
  material: it never appears in logs, error text, or reload metadata.
- `endpoint` — base URL of the upstream provider, legacy inline form. Scheme
  `http` or `https` only; port and path allowed, trailing slashes ignored;
  URL userinfo is rejected. Requests are sent to
  `<endpoint>/chat/completions` and `<endpoint>/responses`. Exactly one of
  `endpoint` and `provider` is required; an inline endpoint is an implicit
  provider with the direct transport.
- `provider` — reference to a `providers` entry whose `base-url` and
  `transport` the model forwards through. The reference must name an entry
  the file defines: there is no fallback to direct for an unknown name,
  because silently rerouting egress is the failure this schema exists to
  prevent.
- `providers` — optional ordered candidate chain, the alternative to the
  single `provider`/`endpoint` forms (which build an implicit
  one-candidate chain — the handler walks one uniform structure). Each
  entry carries `provider` (a reference into the top-level providers
  table, same rules as above) and `upstream-model` (the name sent to that
  candidate; required per candidate). The first entry is the primary
  route; the rest are fallbacks walked only on transport-level failure,
  bounded by [`provider-fallback`](#provider-fallback). Both forms at once
  — a `providers` list next to `provider` or `endpoint` — is a rejection,
  as is the same provider referenced twice in one chain. The
  `injection-prompt` and `thinking-usage` stay model-level: every
  candidate receives the same prompt, because injection is a property of
  the public model. See [Provider fallback](#provider-fallback).
- `upstream-model` — the `model` value actually forwarded upstream.
  Required on the single-provider and inline-endpoint forms (and per
  candidate inside a `providers` chain, where there is no model-level
  `upstream-model`).
- `injection-prompt` — the system instruction injected into every request for
  this model. Multi-line supported; the exact text is used verbatim.
- `thinking-usage` — optional block configuring simulated thinking-usage
  synthesis: `mode` is required when the block is present (`auto` — only when
  the request signals thinking; `always` — every request, overriding the
  request's signal; `off` — never) and `min-ratio`/`max-ratio` bound the
  share of output tokens attributed to thinking (each optional, finite, in
  `[0,1]`, with `min-ratio ≤ max-ratio`; both absent → fixed `0.75`, one set →
  fixed to it). An absent or null block means off — responses stay
  byte-identical to an unconfigured deployment. See
  [Simulated thinking usage](#simulated-thinking-usage).
- `sse-keep-alive` — optional block controlling client-facing SSE heartbeat
  comments for both streaming routes. Absent, `null`, or `{}` defaults to
  `enabled: true`, `interval: 15s`; `enabled: false` opts out. `interval`, if
  set, is a [Go duration](https://pkg.go.dev/time#ParseDuration) of at least
  `1s` (for example `15s` or `1m`); zero, negative, sub-second, malformed, or
  unknown nested values reject the complete file — including when the block
  says `enabled: false`. It hot-reloads with the mapping, preserving the
  complete last-known-good setting on rejection. See [Streaming](#streaming).
- `providers` — optional named table of upstream bases. Each entry carries
  `base-url` (validated exactly like a model `endpoint`) and an optional
  `transport` reference; a provider without a `transport` routes direct.
  Entries are validated even when no model references them.
- `transports` — optional named table of outbound paths. `type: direct` is
  the plain Go HTTP stack (and must not set a proxy URL); `type: proxy`
  requires `proxy: <url>` naming exactly one proxy endpoint — scheme `http`,
  `https`, `socks5` or `socks5h`, a host, an explicit port, optional
  userinfo as proxy authentication, and nothing after the authority. See
  [Provider transports](#provider-transports).
- `provider-fallback` — optional block bounding the provider candidate
  walk of chain models. Absent, `null`, or `{}` defaults to
  `enabled: true`, `max-attempts: 2`; a present block is validated even
  when it disables the feature. `max-attempts` is the per-request
  candidate budget (at least 1, at most 8) — the walk stops after this
  many candidates were tried, chain length permitting. See
  [Provider fallback](#provider-fallback).

The file is validated strictly, in two layers:

- **Top-level keys** are checked against the raw YAML: only `models`,
  `api-key`, `log-level`, `sse-keep-alive`, `providers`, `transports`, and
  `provider-fallback` are legal. This is the bootstrap-plane rule — a file
  that tries to define `listen`, `config-file`, `config-poll-interval` or
  `shutdown-grace`, `auth-database-url`, or `usage-database-url` is rejected
  whatever its value's shape (a strict struct decode alone misses a bootstrap
  key whose value is an empty map).
- **Model entries** are decoded strictly (`yaml.v3`
  with known fields): any key outside `endpoint`, `provider`,
  `upstream-model`, `injection-prompt`, `thinking-usage` and `providers`
  (and, inside the block, outside `mode`, `min-ratio`, `max-ratio`) —
  including a nested bootstrap key — is a rejection, not a warning. The
  same strictness holds inside `providers` entries (`base-url`,
  `transport`), model-chain candidate entries (`provider`,
  `upstream-model`), `transports` entries
  (`type`, `proxy`, and the pool fields `members`, `strategy`, `fallback`,
  `health`, each with their own strict field sets), and pool `members`
  entries (`transport`, `max-body-bytes`, `max-concurrency`, `streaming`,
  `weight`).

The `models` table itself must contain at least one model, and `api-key` must
be a non-empty Bearer token after trimming outer spaces (only the
Bearer-token characters documented above; no embedded whitespace). Both
failures are fail-closed: an invalid initial config exits
`1`; an invalid reload preserves
the complete last-known-good snapshot (including its prior client key). An
empty model table is what a truncate-then-write config edit looks like
mid-write, and accepting it would silently drop every model from the live
service; a missing key would silently open it.

## Hot reload

The process polls `OAICR_CONFIG_FILE` for a SHA-256 content change every
`OAICR_CONFIG_POLL_INTERVAL`. When the content changes:

1. New content is parsed and validated.
2. **Valid** → a new snapshot is published atomically; subsequent requests
   bind to it.
3. **Invalid** → the change is logged and the **last-known-good** config
   keeps serving. A broken reload never takes the service down.

Semantics that hold:

- **One snapshot per request.** Each request binds exactly one snapshot at
  entry — including a stream. Reload `N → N+1` never affects an in-flight
  request or stream; a stream bound to `N` finishes naming models from `N`
  and keeps the `sse-keep-alive` setting (enabled state and interval) it
  started with. The new setting applies to subsequent requests only.
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
- **Transport pools survive reloads that keep them.** Each distinct
  transport configuration owns one long-lived connection pool in the
  process. A reload that leaves a transport's config byte-identical keeps
  its warm pool; one that drops the last reference to a transport closes
  only that pool's idle connections — in-flight requests on it finish
  untouched. A `pool` transport adds a second layer on the same rule: its
  scheduler position, health state and concurrency permits are keyed by the
  pool's policy content, so an unchanged pool stays warm across reloads, a
  changed policy starts fresh, and a pool still executing requests is torn
  down only when its last in-flight request releases it.
- **The log level hot-reloads with everything else.** The top-level
  `log-level` key rides
  the same validate-then-publish path as the model mappings: a valid reload
  applies the new level process-wide without a restart, a restart, or any
  signal; an invalid `log-level` value rejects the whole file onto the
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

## Provider transports

Requests leave this proxy through a _transport_: the router (model mapping)
decides **which** provider serves a request, the transport decides **how**
the request reaches it.

```
            ┌────────────────┐   model mapping (WHICH provider)
client ────▶│    injector    │──────────────────────────────┐
            └────────────────┘                               ▼
                     │ transport (HOW)              provider base URL
                     │
        ┌────────────┼────────────┐
        ▼            ▼            ▼
     direct       proxy        pool ────▶ one member per request
        │            │            │        (direct or proxy endpoints,
        ▼            ▼            ▼         scheduled, with bounded
   provider    proxy endpoint  member     fallback + health)
                     │         selection
                     ▼              │
                 provider ◀─────────┘
```

Three kinds exist, configured through the `transports` table and
referenced by name from `providers`:

- **`direct`** — the standard Go HTTP stack with the same tuning the service
  always had (no overall timeout so long-lived SSE streams survive, 30s dial
  timeout, connection pooling and eager HTTP/2). It also honors the ambient
  `HTTP_PROXY`/`HTTPS_PROXY`/`NO_PROXY` environment variables, exactly like
  deployments before this table existed. Omitting a provider's `transport`,
  or using a model's inline `endpoint`, is this.
- **`proxy`** — exactly one configured proxy endpoint, shared by every model
  whose provider references the transport:
  `http://` and `https://` (HTTP forward proxy; https targets ride CONNECT
  tunnels, `https://` additionally TLS-encrypts the hop to the proxy),
  `socks5://` (the upstream hostname is resolved **locally**, the proxy sees
  only the IP), and `socks5h://` (the hostname itself is sent to the proxy —
  **remote** DNS, the form that keeps upstream hostnames out of the local
  resolver). The two SOCKS forms are deliberately not interchangeable.
  Credentials in the proxy URL's userinfo (`user:pass@host`) authenticate to
  the proxy (Basic auth for http/https, RFC 1929 for SOCKS5) and never
  appear in logs or error text. SOCKS5 credentials are RFC 1929
  single-byte-length fields, so each is bounded to 255 decoded bytes at
  config load — a longer credential rejects the whole file instead of
  failing one dial at a time.
- **`pool`** — a set of endpoint members (direct or proxy transports,
  referenced by name; pools do not nest) with per-request scheduling,
  eligibility, bounded egress fallback, and passive health. Each request is
  scheduled onto ONE member; eligibility is checked before anything dials:

  ```yaml
  transports:
    http-relay:
      type: proxy
      proxy: http://http-relay-gateway:20130
    rotation-socks:
      type: proxy
      proxy: "socks5h://user:pass@rotation-proxy-gateway:30121"
    kilo-egress:
      type: pool
      members:
        - transport: http-relay
          max-body-bytes: 4718592 # eligibility gate on the OUTGOING body
          max-concurrency: 4 # 0/unset = unlimited; held until body close
          streaming: true # default true; false = no streamed requests
          weight: 1 # weighted_round_robin shares only; >= 1
        - transport: rotation-socks # bare form: all defaults
      strategy: round_robin # default; or weighted_round_robin
      fallback:
        enabled: true # default
        max-attempts: 3 # distinct endpoints dialed; default 3, cap 16
      health:
        enabled: true # default
        failure-threshold: 3 # consecutive transport failures ...
        cooldown: 30s # ... trip this cooldown (min 1s)
  ```

  - **Eligibility before scheduling.** A member the request cannot legally
    use — `streaming: false` vs a streamed request, `max-body-bytes` below
    the outgoing body, at its concurrency cap, or in a health cooldown — is
    skipped without a dial. A 6 MB request never produces a 413 on a 4.5 MB
    relay: it was never sent there. Skipped members consume no attempt, no
    health strike, and no scheduler turn — the rotation position and the
    weighted counters only move when a member is actually taken, so a
    recovered member is scheduled immediately rather than waiting out a
    turn it never used.
  - **Scheduling.** `round_robin` rotates across the eligible members;
    `weighted_round_robin` is smooth weighted round-robin over the
    CURRENTLY eligible members: a member with weight 5 against one with
    weight 1 gets exactly `A A A B A A`, proportionality in a deterministic
    interleaving. The weighting runs on the live candidate set — an
    unavailable member accumulates no credit while it is out, a recovered
    member re-enters with none (no catch-up burst), and a member skipped
    for saturation has its round undone exactly (credit frozen, no tilt).
    Weight never affects eligibility or the fallback order — only who is
    tried first. The concurrency permit for the chosen member is acquired
    inside the same selection step, so two concurrent requests can never
    both take a member's last permit and both dial.
  - **Bounded fallback, transport failures only.** When a dialed member
    fails before any response arrives (connection, proxy connect, proxy
    auth, timeout), the next eligible member is tried, up to
    `max-attempts` distinct endpoints. A client cancellation aborts
    everything — no fallback, no strike, no penalty. **Any response — 429
    and 5xx included — ends the attempt loop**: an HTTP status is the
    upstream's answer, never a fallback trigger and never a health strike.
  - **Passive health.** `failure-threshold` consecutive fallback-eligible
    failures open a `cooldown` during which the member is skipped. Recovery
    needs no probe: any response proves the path delivered and resets the
    count.
  - **Zero eligible members** (all skipped or the fallback budget spent on
    skips) answers the canonical 502 `upstream_unreachable` envelope — the
    access log carries `egress_attempts`, `egress_kind`, `egress_target`
    and `egress_exhausted` so the pool's decision is visible per request.
    Each dialed-and-failed endpoint also emits one WARN
    `egress_attempt_failed` (kind, scheme+host target, typed
    `error_class`, attempt number) — evidence per attempt, even when a
    later member serves the request, with no error text and no
    credentials.
  - **Reload identity.** A pool whose policy bytes are unchanged across a
    reload keeps its scheduler position, health state and connection pools.
    A changed policy is a new identity: fresh state, and the old state
    drains via its in-flight requests before its idle connections close.

Semantics the transports guarantee, and that the rest of the service relies
on:

- **Upstream HTTP answers are answers.** A `429` or `500` from a provider
  arrives as a normal response with that status — never as a transport
  error. Only network-level failures (DNS, TCP, TLS, refused or broken
  connections, cancelled contexts) are errors.
- **Streaming is never buffered.** A transport hands back a live body;
  paced SSE events cross a proxy hop with their pacing intact.
- **One pool per transport, not per request.** Identical transport
  configurations share one long-lived connection pool, including across
  config reloads that keep them.
- **The request belongs to the caller.** A transport executes a request
  without rewriting it — model renaming and prompt injection happen before
  it, and nothing transport-side mutates the request's identity.

A broken `transports` or `providers` table is a whole-file rejection at
boot (exit 1) and a last-known-good on reload — an invalid proxy never
silently degrades into direct egress, and a member reference that names
nothing (or names another pool) rejects the file. So does a pool listing
the SAME endpoint twice under two names: scheduling, health, and
concurrency state for one endpoint must exist exactly once, and the
duplicate check runs on the resolved endpoint (host case and userinfo
spelling collapse), never on the YAML name.

## Provider fallback

A model may list several **provider candidates** — a primary route plus
fallbacks. The single `provider`/`endpoint` forms are the degenerate
one-candidate chain, so every request walks the same structure:

```yaml
models:
  gpt-reviewer:
    providers: # ordered; the first entry is the primary route
      - provider: provider-a # a top-level providers-table reference
        upstream-model: gpt-5-pro # required per candidate
      - provider: provider-b
        upstream-model: standard-gpt-5
provider-fallback: # optional; these are the defaults
  enabled: true
  max-attempts: 2 # per-request candidate budget, 1..8
```

Semantics, and the boundaries that keep the feature narrow:

- **Only transport failure falls back.** A candidate that answers — any
  status — ends the walk, and its answer is THE answer: a `429` or `500`
  from the primary is relayed exactly as a single-provider deployment
  would relay it. A candidate is skipped only when it fails before
  answering: dial failure, TLS, proxy failure, or its egress pool
  exhausting (`502`-class `upstream_unreachable` conditions). Egress
  fallback (between network paths) and provider fallback (between
  providers) compose but never blur: a pool moves a request between
  paths to the SAME provider; the walk moves it to the NEXT candidate
  only after that provider had no answer at all.
- **Never retried: local validation, cancellation, commitment.** A body
  that fails the request transform is answered `400` on the first
  candidate — it would fail every candidate's transform. A client that
  disconnects mid-walk gets no fallback (there is nobody left to answer).
  And the walk happens entirely before the first response byte: a `200`
  SSE stream from the primary is committed — no candidate switch after
  headers, ever.
- **Replay is fresh and identical.** Each attempt rebuilds the request
  from the same immutable client body through that candidate's own
  transform — its own `upstream-model`, the same injected prompt. Nothing
  observed on a failed attempt feeds the next one.
- **The budget is a hard product.** Worst case dials are bounded by
  `provider-fallback.max-attempts ×` the per-candidate egress fallback
  budget — with the defaults `2 × 3 = 6` dials for a two-candidate chain
  on default pools. The walk also never exceeds the chain length.
- **Observability.** Every completion event carries `provider_attempts`
  (candidates tried) and `final_provider` (the candidate that answered,
  or the last one that failed); exhaustion adds `provider_exhausted: true`.
  Each failed candidate logs one WARN `provider_attempt_failed` (provider,
  sanitized error class, attempt index), and each of its dialed-and-failed
  egress endpoints logs the existing WARN `egress_attempt_failed`.
- **Reload invariants hold.** The chain, its policy, and every candidate's
  transport bind to the request's config snapshot like everything else —
  a reload mid-walk cannot reshape the candidate list under in-flight
  work. Every candidate's transport is part of the snapshot's egress
  closure, so a fallback candidate's connection pool is warm even when
  the primary answers everything.

**Not included, by design:** automatic egress rotation over time, active
health probes, weighted or scored provider selection (the chain order is
the operator's, not computed), any provider fallback on HTTP statuses (a
rate-limited _provider_ is answered, not retried elsewhere), and any
proxy-to-direct silent downgrade: when a candidate's members are all
unusable the request fails loudly with `upstream_unreachable`.

## Simulated thinking usage

Some upstream models reason internally but never report it: their usage
objects count completion tokens with no reasoning breakdown, and client tools
that display — or gate — on `reasoning_tokens` misbehave. A model entry can
opt into synthesizing that number on the client-facing side:

```yaml
models:
  deep-thinker:
    endpoint: https://api.provider.example/v1
    upstream-model: some-reasoner
    thinking-usage:
      mode: auto # or always | off
      min-ratio: 0.6
      max-ratio: 0.9
```

The synthesis is **response-side only** — the request is forwarded untouched.

| Mode     | When the count is synthesized                             |
| -------- | --------------------------------------------------------- |
| `off`    | Never — also what an absent or `null` block means         |
| `auto`   | Only when the request itself signals thinking (see below) |
| `always` | Every request for the model, whatever the request says    |

**The number.** Each usage object in the response gains
`floor(share × completion_tokens)` in chat (`floor(share × output_tokens)` in
responses), written into the API-native details field:
`usage.completion_tokens_details.reasoning_tokens` for chat,
`usage.output_tokens_details.reasoning_tokens` for responses. The share is
drawn **once per request** from `min-ratio`/`max-ratio` — both set → uniform
in `[min, max]`; exactly one set → fixed to it; neither → fixed `0.75` — and
the draw happens before any upstream I/O. Every usage object in the request,
a stream's intermediate chunks included, reports the same share; a reload
mid-request cannot change it, because the plan is bound to the request's
[config snapshot](#hot-reload) like everything else.

**When the upstream speaks, it wins.** A usage object that already reports
reasoning — a direct `reasoning_tokens`, or a details object whose
`reasoning_tokens` is anything above zero — passes untouched. A details
`reasoning_tokens` of exactly `0` is the "never reported" case and receives
the synthesized number; a non-object details value is replaced by the details
object. Completions of 10 tokens or fewer synthesize `0` — a fraction of
nothing is nothing.

**Intent signals (`mode: auto`).** A request counts as signaling thinking
when any of: `reasoning_effort` is a string other than `"none"` (the value
`"none"` is explicitly off), `reasoning.effort` likewise, `enable_thinking`
is `true`, or `thinking.type` is `"enabled"`. Each signal is read
independently and leniently — an unparseable one is ignored, never fatal.

**Scope and limits.**

- Both API surfaces, buffered and streamed. One composed rewriter — the model
  rename plus, when the plan is active, the synthesis — serves the buffered
  and SSE paths, so a stream chunk and a buffered body carrying the same
  JSON rewrite to the same bytes, by construction.
- **Only existing usage objects are enriched.** A response with no usage
  object never gains one; the synthesis never fabricates a usage block and
  never touches `completion_tokens`, `output_tokens` or totals.
- Like the model rewrite, the edit is byte-preserving around itself: key
  order, whitespace and unknown fields all survive; a payload that cannot be
  parsed is forwarded byte-for-byte.
- Scope mirrors the model rewrite: chat owns the top-level `usage`; responses
  owns the top-level `usage` and the one inside the top-level `response`
  envelope object. Anything nested deeper is client data.
- Fail-open everywhere: a malformed usage object, non-numeric counts or a
  missing completion field leave that object untouched.

With no `thinking-usage` block on the model, the composed rewriter degenerates
to the plain model rename and both response paths are byte-identical to an
unconfigured deployment.

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
- **Keep-alive during upstream silence.** When `sse-keep-alive.enabled` is
  true (the default — the production deployment sits behind Cloudflare),
  an upstream that has sent no client-visible byte for `interval` gets one
  flushed SSE comment, exactly `: ping\n\n`, at an event boundary; every
  forwarded upstream byte resets the silence clock. SSE comments are
  ignored by every spec-compliant SSE parser, so the heartbeat is
  client-compatible and invisible to application events. The heartbeat
  stops at upstream EOF/error, on client disconnect, and as soon as the
  Chat `[DONE]` or Responses `response.completed` terminal marker is
  forwarded — it never injects inside a partial `data:` line, never splits
  an event, and never appends after the terminal event. It starts only once
  the upstream response headers are committed to the client: silence while
  waiting for upstream headers is **not** covered (that window is bounded
  by the upstream's response-header timeout, not this relay heartbeat).
  Why: Cloudflare silently cuts a client HTTP/2 stream after ~125s with
  zero bytes from origin (measured on 2026-09-22 — client
  `stream error … INTERNAL_ERROR` at 125.39s, origin-side close at
  125.06s), which long reasoning phases exceed; a `: ping` every 15s kept a
  240s silent stream alive through the same path.
- The streaming _shape_ is decided by the **URL path**, not the body:
  - Chat Completions: `data:` lines, terminated by `data: [DONE]`.
  - Responses API: `event:`/`data:` pairs. **No `[DONE]`** — Responses
    termination events are part of the protocol and pass through untouched.
- Only `data:` lines whose JSON carries an in-scope `model` string value are
  rewritten — the top-level key (and, for responses, the envelope's
  `response.model`). Model text appearing anywhere else in the payload — a
  substring of a message, another field's value — never matches.
  `event:`, comments, and other `data:` lines pass through verbatim. When
  [thinking-usage](#simulated-thinking-usage) synthesis is active for the
  model, a `data:` line carrying a `usage` object is a rewrite candidate
  too.
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

| Condition                                                                                                       | Status                 | `error.type` / `code`                                                                                                                                                                           |
| --------------------------------------------------------------------------------------------------------------- | ---------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Missing/malformed `Authorization: Bearer <key>`                                                                 | 401                    | `invalid_request_error` — exact body: `{"error":{"message":"you must provide an API key in the Authorization header (Bearer <key>)","type":"invalid_request_error","param":null,"code":null}}`  |
| Wrong bearer key (partner mode: unknown **or revoked** key, or an unavailable key store — fail closed)          | 401                    | `invalid_request_error` / `invalid_api_key` — exact body: `{"error":{"message":"invalid API key","type":"invalid_request_error","param":null,"code":"invalid_api_key"}}`                        |
| Body is not JSON                                                                                                | 400                    | `invalid_request_error` — exact body: `{"error":{"message":"invalid JSON in request body","type":"invalid_request_error","param":null,"code":null}}`                                            |
| Missing `model`                                                                                                 | 400                    | `invalid_request_error` — exact body: `{"error":{"message":"you must provide a model parameter","type":"invalid_request_error","param":null,"code":null}}`                                      |
| Request body over the 64 MiB cap                                                                                | 413                    | `invalid_request_error` — exact body: `{"error":{"message":"request body too large","type":"invalid_request_error","param":null,"code":null}}`                                                  |
| Request names an unmapped model                                                                                 | 404                    | `model_not_found` — exact body: `{"error":{"message":"The model '<X>' does not exist or you do not have access to it.","type":"invalid_request_error","param":null,"code":"model_not_found"}}`  |
| Request path matches no route (unknown path, trailing slash, wrong case)                                        | 404                    | `invalid_request_error` — exact body: `{"error":{"message":"Invalid URL (<METHOD> <PATH>)","type":"invalid_request_error","param":null,"code":null}}`                                           |
| Upstream unreachable (dial/network)                                                                             | 502                    | `upstream_error` / `upstream_unreachable`                                                                                                                                                       |
| Upstream 200 with unparseable body (or body over the 64 MiB buffered cap, or a body read that fails mid-answer) | 502                    | `upstream_error` / `upstream_invalid_response`                                                                                                                                                  |
| Upstream answers 4xx/5xx                                                                                        | same as upstream       | `upstream_error` / `upstream_http_<status>` — canonical envelope: `{"error":{"message":"upstream provider returned HTTP 429","type":"upstream_error","param":null,"code":"upstream_http_429"}}` |
| Upstream answers 3xx (redirect), 204, or 304                                                                    | **forwarded verbatim** | status, bytes, and an allow-list of headers pass through (see below)                                                                                                                            |

Consequences of the table:

- **An unmapped model is never forwarded.** 404 is local; the upstream never
  sees that request. This is a hard boundary (SECURITY.md treats its breach
  as a vulnerability).
- **Redirects are relayed, never followed.** A 3xx passes through verbatim:
  following one would silently convert the POST into a body-less GET
  (301/302/303) and replay the transformed request body to whatever the
  `Location` names (307/308). An unexpected 3xx is the upstream's answer,
  and the client's to judge. 204 and 304 relay the same way.
- **Upstream 4xx/5xx errors are normalized.** The status is preserved
  exactly (401 stays 401, 429 stays 429, 500 stays 500 — never collapsed
  into a 502; 502 is reserved for the upstream delivering nothing usable at
  all), but the body is always the canonical envelope above and
  `Content-Type` is always `application/json`. The provider's raw body —
  its message text, its HTML, even its model names — never reaches the
  client: an upstream error that quotes the alias target discloses nothing.
  A bounded prefix (64 KiB) of the error body is read once, solely to
  classify its shape and fingerprint it for the log evidence event (see
  Logging); those bytes go nowhere else — not to the client, not into any
  log line. `Retry-After` and the `X-RateLimit-*` headers still ride the
  allow-list below, so a 429 remains distinguishable and backoff-able.
  This holds for every request shape: an upstream 5xx answered as
  `text/event-stream` is normalized too (never streamed to the client),
  while a 200 SSE stream that has already committed headers is never
  converted into an error mid-flight.
- There is **no overall request timeout**. A slow upstream is a slow
  response, not a timeout race. Dial and TLS handshake timeouts bound the
  connection phase only.

**Relayed response headers** (an allow-list, everything else is dropped):
`Content-Type`, `Cache-Control`, `X-Request-Id`, `OpenAI-Request-Id`,
`Retry-After`, `Location`, `X-RateLimit-Limit`, `X-RateLimit-Remaining`,
`X-RateLimit-Reset`, `X-RateLimit-Reset-Requests`, `X-RateLimit-Reset-Tokens`.
Rate-limit and retry headers are load-bearing for client backoff; dropping
them would make a 429 indistinguishable from any other upstream failure.

## Partner API keys

Static mode (the default) authenticates every client against the runtime
YAML's `api-key` — one shared credential, no per-caller identity. Setting
`OAICR_AUTH_DATABASE_URL` opts the process into **partner mode**: `/v1`
requests authenticate against hashed per-partner keys stored in
PostgreSQL/TimescaleDB, and each request is attributed to a `partner_id` +
`key_id` pair that rides the structured logs (and, from the usage metering
layer on, the usage records).

What partner mode changes, precisely:

- **Identity, not a boolean.** Authentication resolves a `Principal`
  (partner + key). The YAML `api-key` is still required by the runtime file
  contract, but in partner mode it is **not a wire credential** — presenting
  it denies, because it was never seeded into the store. Revocation cannot
  be bypassed through the file.
- **High-entropy tokens, hashed at rest.** `keys create` mints
  `oaicr_` + 32 crypto-random bytes (base64url) and stores only its SHA-256
  digest — the plaintext is printed once on creation and is unrecoverable by
  design. Key IDs (`pak_…`) are public and safe to log.
- **Revocation without restart, within a documented bound.** A bounded LRU
  decision cache (4,096 entries) fronts the store: affirmative decisions
  live at most **60 s**, negative ones (unknown/revoked) at most **5 s**, so
  `keys revoke` takes effect within at most a minute and a freshly created
  key works within at most five seconds. The cache holds decisions the
  store made — never a bypass: backend failures are never cached, and a
  recovered store is trusted on the very next request.
- **Every failure mode fails closed.** A store outage (or a slow one)
  denies the request with the same static 401 as any other rejection —
  never a fail-open — after emitting the WARN `auth_backend_failed` event
  (error class only; driver text never reaches logs). At boot, an
  unreachable store or a schema that fails validation is a startup failure.
- **Wire parity with static mode.** Unknown and revoked keys get exactly
  the same static `invalid_api_key` 401 as a wrong static key — why a
  credential was rejected is enumeration material and never reaches the
  client. The missing/malformed-bearer 401 is unchanged.
- **Schema is migrated, validated, forward-only.** SQL migrations live in
  the binary (`internal/auth/migrations/`), run inside transactions with
  `schema_migrations` bookkeeping, and never run on the request path.
  Startup brings the schema up and validates the column contract against
  `information_schema`; a foreign or half-migrated table refuses to start.

Managing keys (a CLI beside the proxy, deliberately not an HTTP surface on
it; the connection string comes from `-database` or
`OAICR_AUTH_DATABASE_URL`):

```console
$ openai-compatible-injector keys create -partner acme-corp
key created
  key_id:     pak_…
  partner_id: acme-corp
  token:      oaicr_…        # shown exactly once — store it now

$ openai-compatible-injector keys list
key_id	partner_id	status	created_at	revoked_at	last_used_at
pak_…	acme-corp	active	2026-01-01T00:00:00Z	-	-

$ openai-compatible-injector keys revoke -key-id pak_…
key revoked: pak_…
Requests presenting it are denied within 1m0s (the positive cache bound).
```

`keys list` cannot expose secrets: the record type structurally carries no
hash and the table read selects no hash column. `last_used_at` is updated
off the request path (batched, best-effort) — it is advisory metadata, and
losing touches under load never affects a request.

## Usage metering

Usage metering is deliberately **optional** and fact-only. Set
`OAICR_USAGE_DATABASE_URL` to a PostgreSQL or TimescaleDB connection string to
enable it. Startup connects, applies the embedded forward-only migration, and
validates the `usage_events` table before accepting traffic. Empty (the
default) means no database connection, no event records, and unchanged proxy
behavior. The DSN is bootstrap infrastructure: it does not hot-reload and is
never logged, including its length.

Each request that reaches the provider path produces at most one durable event
asynchronously, with an event ID, request time and request ID, partner/key
identity (when partner auth is enabled), bound config generation, public model,
provider and upstream model, API surface, the relayed stream mode (what the
response actually was — a `stream: true` request whose upstream answered
non-SSE is recorded buffered), final client status and outcome,
upstream-reported token counts, client wire byte counts, cumulative provider
and egress attempt counters (summed across every candidate of a fallback
walk), the final candidate's egress kind (`direct`, or the last dialed pool
member's kind; empty when a pool exhausted without dialing anything), and full
request latency. Provider fallback still emits **one** event: it describes the
candidate that answered, or the final attempted candidate if all paths failed.
Failed attempts are represented only by the attempt counters.

The request handler only makes a non-blocking handoff to a bounded in-memory
queue. A dedicated batch writer inserts rows using parameterized SQL; it
retries a bounded number of times within a per-flush deadline, then explicitly
drops and reports a batch if the store remains unavailable — every accepted
event resolves into inserted-or-dropped, never a silent remainder. Metering
loss never delays, fails, or modifies a client response. Shutdown stops intake
after the HTTP drain and flushes the accepted backlog within its bounded close
window, then reports the totals as an INFO `usage_meter_final`; an expired
window waits a bounded extra grace for an in-flight insert before the pool
closes. Operators can observe `usage_flush_completed`, `usage_flush_failed`,
and `usage_events_dropped`, and read pipeline stats for queue depth, inserts,
drops, failures, retries, flushes, and last successful flush timing. (A
second forced signal exits without running defers — that abandonment of the
drain, and of this accounting, is the operator's explicit choice.)

Token facts come only from the raw upstream response before the proxy rewrites
model aliases or simulates thinking usage. Missing upstream `usage` remains
SQL `NULL`, not zero. For streams, the last readable API-scoped usage object
wins; chunks are never summed. One wrongly typed count makes that member
unstored (a stringified `"128"` or integral `1e3` still reads) — it never
discards the members that did decode. Consequently, synthesized
`reasoning_tokens` are never stored as provider usage. This layer intentionally
has no pricing, currency, invoicing, or quota enforcement.

Auth and usage schemas share a module-scoped `schema_migrations` ledger. An
existing partner-key deployment with the former global-version ledger upgrades
in place: its historical rows are retained as the `auth` module before the
`usage` module records its own migrations. No request-path DDL exists.

## Safety and credentials

- Static mode: the configured `api-key` is the one shared inbound client
  credential. Partner mode (above): per-partner hashed keys. In both, the
  credential is presented via `Authorization: Bearer <key>` and checked
  before the proxy reads the request body. The header is **consumed at the
  proxy** and never forwarded upstream. The proxy injects no replacement
  credential: upstreams are expected to be trusted/internal.
- **Credentials never reach logs or error text** — no configured `api-key`,
  `Authorization` values, request bodies, or injection prompts in log lines,
  and no upstream URL details beyond the endpoint's scheme+host in **any**
  log line or error text (a query-parameter API key survives even a dial
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
`message` slug. Levels are `debug`, `info`, `warn`, and `error` (matched
exactly — the same spelling and strictness as the organisation's other Go
services), defaulting to `info`; the
level is hot-reloadable through `log-level` (see Hot reload).

What each level carries:

- **DEBUG** — the full request lifecycle, every event bound to its
  `request_id`: `request_received` (method/path/remote address),
  `probe_completed` (model + stream flag), `model_resolved` (public model,
  upstream model, upstream scheme+host origin), `thinking_usage_resolved`
  (mode, whether the request signaled intent, whether synthesis is active —
  models with a `thinking-usage` block only), `request_transform_started`/
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
  (`chat`/`responses`), `status`, `outcome` (including `unauthorized` for a
  rejected bearer), `public_model`, `stream`,
  `bytes_in`, `bytes_out`, `duration_ms`, and `config_generation` (the
  snapshot generation the request bound to — correlating reloads with
  behavior). Requests that reached the upstream also carry the egress
  report (`egress_attempts`, `egress_kind`, `egress_target`,
  `egress_exhausted`) and the provider walk's
  (`provider_attempts`, `final_provider`, plus `provider_exhausted` when
  every budgeted candidate failed without answering). The event is emitted
  when the request finishes, under the
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
  gone), and an upstream 4xx — the `upstream_http_error` evidence event
  described below — one warning per transition into a failed config
  state (`config_file_unreadable`, `config_reload_rejected`) — including a
  failure that changes kind, which warns again — never one per poll tick —
  plus `second_signal_forced_exit` and drain overflow.
- **ERROR** — upstream connection failures (`upstream_request_failed` with
  an `error_class` such as `connection_refused`, `timeout`, `tls`, `dial` —
  never `client_canceled`, which is the WARN disconnect above), an
  upstream that died mid-relay on the verbatim path (`relay_copy_failed`
  with phase `upstream_read` — the one relay failure that is not a
  disconnect), and an upstream 5xx (`upstream_http_error` at error
  severity — 5xx is our outage even when the provider owns the cause),
  plus anything fatal at startup. A 200 that is not
  parseable JSON is not an event of its own: it surfaces only as the
  `upstream_invalid_response` outcome on the INFO completion line, with
  the 502 envelope on the wire.

**The upstream 4xx/5xx evidence event.** Every normalized upstream error
emits one `upstream_http_error` event (WARN for 4xx, ERROR for 5xx) bound
to the request's `request_id`: `api`, `public_model`, `upstream_model`,
`upstream` (scheme+host only), `upstream_status`, `content_type`,
`error_class` (`upstream_http_4xx`/`upstream_http_5xx`), `error_shape`
(`empty`, `json_error_object`, `json`, `text`, `malformed_json`,
`truncated`), `body_bytes` (the size of the captured prefix — the whole
body when it fit under the 64 KiB cap), `body_truncated`, and
`error_fingerprint` — the SHA-256 hex digest of that bounded prefix, the
join key for correlating repeated provider errors without keeping any of
their bytes. When the provider's error object carries its own
token-shaped `type`/`code` (`rate_limit_error`, `insufficient_quota`, …)
they appear as `provider_error_type`/`provider_error_code` — only when the
value is short printable ASCII, never the free-text `message`. The
allow-listed `Retry-After`/`X-RateLimit-*` headers ride along as
`retry_after`/`x_ratelimit_*` fields when present (values longer than 128
bytes are dropped from the log — nothing an upstream controls can balloon a
log line; the client-side relay is unaffected). The raw error body
itself never appears at any level: it exists only as the count, the shape,
and the fingerprint.

The credential rule is absolute: no log line, at any level, ever contains
an `Authorization` value, a request or response body, an injection prompt,
or upstream URL detail beyond scheme+host. Endpoint query strings (which
providers use for API keys) survive even a dial failure's error text —
errors are sanitized before logging — and upstream error bodies are no
exception: the raw bytes of a 4xx/5xx never reach a log line at any level,
only their count, shape, and fingerprint. Usage metering follows the same
rule: it stores only the factual event columns listed above, never an inbound
credential, request body, prompt, provider error body, or database URL; its
store failures log only a stable error class. The planted-secret E2E suite
(`TestLoggingNeverLeaksSecrets`) holds this rule under success, streaming,
rejection, dial-failure, and provider-echo traffic at maximum verbosity.

## Healthcheck

`GET /healthz` answers `200` with the body `ok\n` whenever the HTTP listener
is up.

The `healthcheck` subcommand (used by the Docker image and compose) probes
the running service and requires `200` + `ok\n`:

```sh
./openai-compatible-injector healthcheck    # OAICR_LISTEN env decides what is probed
```

It reads only the `OAICR_LISTEN` environment variable (a wildcard address is
rewritten to the loopback) and **never reads the YAML file** — a poisoned
reload must not fail the container probe.

## Graceful shutdown

On SIGTERM or SIGINT the service stops accepting new connections and drains:

1. `http.Server.Shutdown(grace)` — in-flight requests and streams get up to
   `OAICR_SHUTDOWN_GRACE` (default 55s) to complete.
2. If the budget runs out, `Close()` force-terminates the remainder.
3. If usage metering is enabled, its accepted event backlog is then flushed through its bounded close window; events that cannot be persisted are explicitly counted and reported.
4. Idle keep-alive connections are closed; the process exits `0`.

A second signal while draining forces an immediate `exit 1`. Compose's
`stop_grace_period: 60s` is deliberately larger than the default drain
budget so Docker's SIGKILL never cuts a drain short.

## Deployment

### Docker

```sh
docker build -t openai-compatible-injector . # VERSION build-arg defaults to "dev"
docker run --rm -p 8080:8080 \
  -e OAICR_LISTEN=:8080 -e OAICR_CONFIG_FILE=/app/config.yaml \
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
  number, applied/kept-last-known-good. `log-level: debug` in the
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
- **Transport pools, rotation, health checks, fallback, retries** — a
  `type: proxy` transport is exactly one endpoint (see
  [Provider transports](#provider-transports)); anything that picks between
  egresses at request time is a separate design.
- **TLS configuration** — upstream and proxy TLS verify chain and host with
  system roots; custom TLS setup (client certificates, custom CA pools,
  `insecure-skip-verify`) is not coming. (Ambient
  `HTTP_PROXY`/`HTTPS_PROXY`/`NO_PROXY` environment variables still apply
  to `direct` transports, inherited from `net/http`'s default transport.)
- **Inbound identities and authorization** — `api-key` is a single shared
  client credential, not an identity system. Per-client keys, roles, tenant
  isolation, quotas, and RBAC need a separate design.

## Repository layout

```
cmd/openai-compatible-injector/  entrypoint + version/healthcheck/keys subcommands
internal/config/                 bootstrap, runtime YAML (models, providers, transports), snapshot store, poller
internal/auth/                   client identity: static + partner key store, decision cache, SQL migrations
internal/migrate/                shared module-scoped SQL migration runner
internal/usage/                  factual upstream usage capture, async pipeline, PostgreSQL repository
internal/inject/                 pure request/response transforms (probe, chat, responses, rewrite, thinking usage)
internal/transport/              outbound paths: Doer seam, direct client, proxy (http/https/socks5/socks5h), pool registry
internal/proxy/                  handler, client auth gate, SSE copy, error envelopes
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
