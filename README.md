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
| `models.<name>.{endpoint,upstream-model,injection-prompt,thinking-usage,retries,recovery,providers}`                                             | YAML file (runtime)     |
| `provider-fallback.{enabled,max-attempts}`, `recovery` (global, and per provider/model/candidate)                                                | YAML file (runtime)     |
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

# Optional. The recovery policy every model starts from: which failures retry,
# fall back, or terminate, how the same candidate is re-asked, how far the
# candidate walk reaches, and how many real upstream exchanges one request may
# spend. Overrides of the same shape sit on a providers entry, a models entry,
# and one chain candidate; each layer merges onto the one before it. Two
# members are position-scoped and rejected elsewhere — `budget.request` only
# here, `fallback` only here or on a model entry.
recovery:
  matrix:
    http:
      exact:
        "408": retry # shorthand: exact status -> action
        "429": retry
        "401": fallback
      classes:
        "4xx": terminal # status bucket -> action
        "5xx": retry
    default: terminal # the catch-all
  retries:
    max-retries: 1 # same-candidate re-asks after the initial attempt, 0..8
    backoff:
      initial: 250ms # >= 1ms; doubles per retry, capped at backoff.max
      max: 2s # also the ceiling any Retry-After can never push past
      jitter: 0.1 # uniform ±fraction spread, 0..1
  fallback:
    max-candidates: 2 # candidates ENTERED (primary included), 1..8
  budget:
    request: # the whole request; this block is top-level only
      max-exchanges: 32 # real outbound HTTP exchanges, 1..64
    candidate:
      max-exchanges: 16 # per candidate, 1..32
  retry-after:
    mode: max # max (default) | ignore

# Optional. Named upstream bases, each routed through a transport.
providers:
  opencode:
    base-url: https://api.opencode.example/v1 # validated like a model endpoint
    transport: egress # optional named transport; omitted = direct
    recovery: # optional provider override: every model routed through here
      retries:
        max-retries: 3 # a provider that recovers slowly gets more re-asks

models:
  gpt-reviewer:
    provider: opencode # either provider or endpoint — never both, never neither
    upstream-model: gpt-5-pro # required; the model name sent upstream
    recovery: # optional model override: this model only
      matrix:
        http:
          exact:
            "429": terminal # this provider's 429 means "out of quota"
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
  route; the rest are fallbacks reached when an earlier candidate has no
  usable answer — which failures re-ask or move on is the
  [recovery matrix](#provider-recovery-policy). A candidate entry may
  carry its own `recovery` override, which applies to that hop alone.
  Both forms at once — a `providers` list next to `provider` or
  `endpoint` — is a rejection, as is the same provider referenced twice
  in one chain. The `injection-prompt` and `thinking-usage` stay
  model-level: every candidate receives the same prompt and the same
  synthesis setting, because those are properties of the public model,
  not of a hop. See [Provider recovery policy](#provider-recovery-policy).
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
- `recovery` — optional block carrying the model recovery policy: the
  `matrix` (which failure retries, falls back, or terminates), `retries`,
  `fallback`, `budget`, and `retry-after`. The same block is accepted at the
  top level (the global layer), on a `providers` entry, here, and on one
  chain candidate. The layers merge in the order global → provider → model →
  candidate, so an override states only what changes; an invalid, ambiguous,
  or over-cap block rejects the complete file. Absent, `null`, or `{}`
  inherits the layer beneath — the built-in default policy at the global
  position.
  Two members are confined to the layers whose scope they describe, and
  stating one anywhere else rejects the file rather than loading a block that
  could not mean what it says: `budget.request` is the whole request's
  envelope and belongs to the top-level block only, and `fallback` (the
  candidate walk's reach) belongs to the layers that describe a whole chain —
  the top level and a model entry. See
  [Provider recovery policy](#provider-recovery-policy).
- `retries` — the **legacy** spelling of the model layer's retry mechanics,
  normalized into the same policy engine as `recovery.retries`; stating both
  in one model entry is rejected rather than leaving the effective behavior
  to whichever the code reads first. Absent, `null`, or `{}` selects the
  defaults; `max-retries` is the number of same-candidate re-asks AFTER the
  initial attempt (0..8, default 1; `0` = one upstream exchange per
  candidate); `max-elapsed` bounds one candidate's whole retry sequence in
  time (default `10s`, at least `1ms`, at most `2m`); `backoff.initial` is
  the first retry delay (default `250ms`, at least `1ms`), `backoff.max`
  caps both the exponential growth and any effective `Retry-After` (default
  `2s`, at least `initial` — omitted while `initial` sits above it, the
  ceiling rises to the initial), and `backoff.jitter` is the uniform
  ±fraction spread (0..1, default `0.1`). See
  [Provider recovery policy](#provider-recovery-policy).
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
- `provider-fallback` — the **legacy** spelling of the global layer's
  fallback mechanics, normalized into the same policy engine as
  `recovery.fallback`; stating both in the file (with `recovery.fallback`
  present) is rejected rather than leaving the effective behavior to
  whichever the code reads first. Absent, `null`, or `{}` defaults to
  `enabled: true`, `max-attempts: 2`; a present block is validated even
  when it disables the feature. `max-attempts` becomes
  `fallback.max-candidates` (at least 1, at most 8) and counts candidates
  **entered**, the primary included. See
  [Provider recovery policy](#provider-recovery-policy).

The file is validated strictly, in two layers:

- **Top-level keys** are checked against the raw YAML: only `models`,
  `api-key`, `log-level`, `sse-keep-alive`, `providers`, `transports`,
  `provider-fallback` and `recovery` are legal. This is the bootstrap-plane
  rule — a file
  that tries to define `listen`, `config-file`, `config-poll-interval` or
  `shutdown-grace`, `auth-database-url`, or `usage-database-url` is rejected
  whatever its value's shape (a strict struct decode alone misses a bootstrap
  key whose value is an empty map).
- **Model entries** are decoded strictly (`yaml.v3`
  with known fields): any key outside `endpoint`, `provider`,
  `upstream-model`, `injection-prompt`, `thinking-usage`, `retries`,
  `recovery` and
  `providers` (and, inside the thinking-usage block, outside `mode`,
  `min-ratio`, `max-ratio`; inside `retries`, outside `max-retries`,
  `max-elapsed` and `backoff` — itself limited to `initial`, `max` and
  `jitter`; inside `recovery`, outside `matrix`, `retries`, `fallback`,
  `budget` and `retry-after` — each with its own strict field set) — including
  a nested bootstrap key — is a rejection, not a
  warning. The
  same strictness holds inside `providers` entries (`base-url`,
  `transport`, `recovery`), model-chain candidate entries (`provider`,
  `upstream-model`, `recovery`), `transports` entries
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
  - **Bounded fallback, provably-unsent failures only.** When a dialed
    member fails with positive evidence that no request byte ever left this
    process — a refused or unreachable dial, a failed TLS handshake, a
    proxy CONNECT or proxy-auth failure — the next eligible member is
    tried, up to `max-attempts` distinct endpoints. Evidence, not
    classification, is what decides it: the failure's _wire operation_ is
    read, so a connect that timed out against a blackholed path counts as
    unsent (nothing was written), while a reset, a bare EOF, or a timeout on
    an established connection reads `send_unknown` — the peer may already
    be processing a request whose answer never came back, and replaying it
    on another egress would silently duplicate it. Send-unknown stops the
    loop and travels up to the recovery policy, which owns replay; the
    per-dial evidence line carries `send_state`, so "the pool stopped short
    of its remaining members, and why" is one field away. A client
    cancellation — or an expired caller deadline, which the request context
    reports the same way — aborts everything: no fallback, no strike, no
    penalty. The request context, not the error chain, decides ownership.
    **Any response — 429 and 5xx included — ends the attempt loop**: an
    HTTP status is the upstream's answer, never a fallback trigger and
    never a health strike.
  - **Passive health.** `failure-threshold` consecutive
    `definitely_not_sent` failures open a `cooldown` during which the
    member is skipped — a `send_unknown` failure proves nothing about the
    endpoint and never strikes. Recovery
    needs no probe: any response proves the path delivered and resets the
    count.
  - **Zero eligible members** (all skipped or the fallback budget spent on
    skips) answers the canonical 502 `upstream_unreachable` envelope — the
    access log carries `egress_attempts`, `egress_kind`, `egress_target`
    and `egress_exhausted` so the pool's decision is visible per request.
    Each dialed-and-failed endpoint also emits one WARN
    `egress_attempt_failed` (kind, scheme+host target, canonical
    `error_class` with its closed-set `error_cause`, its `send_state`
    (`definitely_not_sent` / `send_unknown`), attempt number) —
    evidence per attempt, even when a later member serves the request,
    with no error text and no credentials.
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

## Provider recovery policy

A model routes through an **ordered candidate chain** — a primary route plus
fallbacks; the single `provider`/`endpoint` forms are the degenerate
one-candidate chain, so every request walks the same structure. What a failed
attempt _means_, how often the same candidate is re-asked, when the walk moves
on, and how much real upstream traffic one request may spend are decided by
one **recovery policy**: a data structure resolved from YAML at config load
and frozen onto the request's snapshot. The disposition table is policy DATA
with a shipped default, not a Go switch — a provider whose `429` means "your
account has no quota" can be told to move on, while the same status from a hop
that is merely throttled can be told to re-ask.

### The layers

A `recovery` block is accepted in four positions, and the same shape means
something different in each:

| Position                                            | Rank      | Applies to                                               |
| --------------------------------------------------- | --------- | -------------------------------------------------------- |
| top-level `recovery`                                | global    | every model in the file                                  |
| a `providers` entry's `recovery`                    | provider  | every model whose candidate routes through that provider |
| a `models` entry's `recovery`                       | model     | that model                                               |
| one entry of a model's `providers` chain `recovery` | candidate | that single hop                                          |

Not every member is legal in every position. A member whose meaning is wider
than the layer it would sit on is **rejected**, not silently ignored, because
a block that cannot be honoured is worse than no block: it reads like a
setting. Two members are scoped this way.

`fallback` decides how far the request's candidate walk reaches — a property
of the chain, not of any one hop. The walk's reach comes from the primary
candidate's policy alone, so:

- in the **global** and **model** positions it genuinely steers;
- on a **candidate** (or on a provider that is not the primary's) it could
  never matter at all;
- on the **primary's own provider** it would steer today and stop steering the
  moment that provider is used as a fallback for another model — the same
  block, two behaviours, decided by a chain position the operator cannot see
  from the provider entry.

So `fallback` is accepted at the top level and on a model entry, and stating
it on a `providers` entry or a chain candidate rejects the file.

`budget.request` bounds the whole request, which is wider than any single
layer beneath it; it is accepted in the top-level block only.

Resolution runs once, at config load, in the order **global → provider → model
→ candidate** — narrowest last, so an override always beats what it overrides.
It is per candidate rather than per model, because a chain may route through
several providers and a candidate override speaks for exactly one; the result
is frozen onto the snapshot, so a reload mid-walk (mid-wait included) cannot
reshape the budgets of work already in flight.

```yaml
recovery: # global: the layer every model starts from
  retries:
    max-retries: 3
    backoff:
      initial: 1s
  fallback:
    max-candidates: 4
  matrix:
    http:
      exact:
        "429": retry

providers:
  provider-a:
    base-url: https://a.example/v1
    recovery: # provider: applies to every model routing through provider-a
      retries:
        max-retries: 5
      matrix:
        default: fallback

models:
  gpt-reviewer:
    recovery: # model: this model only
      matrix:
        http:
          exact:
            "429": terminal
    providers:
      - provider: provider-a
        upstream-model: gpt-5-pro
        recovery: # candidate: this hop only
          retries:
            max-retries: 1
            backoff:
              jitter: 0.4
```

The candidate's effective policy is:

- `retries.max-retries: 1` — candidate (1) beats provider (5) beats global (3);
- `retries.backoff` `{initial: 1s, max: 2s, jitter: 0.4}` — `initial` from the
  global layer, `max` from the built-in default, `jitter` from the candidate:
  maps deep-merge field by field, they are not swapped wholesale;
- `retries.max-elapsed` `10s` and `retries.on-exhausted` `fallback` — no layer
  states them, so they are inherited from the defaults;
- `fallback.max-candidates: 4` — the global layer's reach, untouched;
- the `http-429` rule is `terminal` — a rule restated by identity **replaces**
  the inherited row wherever precedence had put it — while every other global
  matrix row, and the provider layer's `default: fallback`, survive untouched.

### How layers merge

The merge is semantic and deep, never an object replacement:

- **A scalar the layer states replaces the base's; a scalar it omits is
  inherited**, at every depth. A layer stating only `backoff.jitter` keeps the
  inherited `initial` and `max`.
- **Maps deep-merge**, member by member.
- **A rule with the same ID replaces** the inherited rule, wherever ordering
  had placed it; **a rule with a new ID appends**. A child layer never has to
  restate the parent's rule list to change one row.
- **Nothing is ever removed, and nothing is ever reset to a zero value by
  omission.**
- **A YAML `null` is not a statement**: `recovery:`, `max-retries: null` and an
  omitted `max-retries` all mean "not stated", and the field inherits. A scalar
  stated as its zero value (`jitter: 0`) is a statement and is honoured as
  zero — that distinction is why every optional field is pointer-typed
  internally.

Each merge step is validated before the next layer is applied, and one layer
may state a given rule ID only once. A layer that leaves the policy incoherent
is rejected even when a later layer would have repaired it: a configuration
whose middle state is meaningless is a configuration whose author was not
describing what they thought.

### The matrix

A `matrix` maps one failure to one of three actions:

- **`retry`** — re-ask the SAME candidate after a bounded wait;
- **`fallback`** — move to the next candidate immediately (there is no wait
  between candidates);
- **`terminal`** — stop: the last received answer, or the synthesized failure
  when no candidate ever answered, becomes the client's response.

The shorthand forms cover the common cases and expand into canonical rules:

```yaml
recovery:
  matrix:
    http:
      exact: # exact status -> action; keys are three-digit statuses
        "408": retry
        "429": retry
        "401": fallback
        "501": terminal
      classes: # status bucket -> action
        "4xx": terminal
        "5xx": retry
    transport: # transport class or transport cause -> action
      connection: fallback
      tls: fallback
      proxy_auth: fallback
    protocol: # unusable-answer cause -> action
      invalid_response: retry
      body_timeout: retry
    caller: # canceled | deadline; terminal only
      canceled: terminal
      deadline: terminal
    default: terminal # the catch-all, taken by any unmatched failure
```

A free-form `rules` list expresses what the shorthand cannot:

```yaml
recovery:
  matrix:
    rules:
      - id: provider-quota-exhausted # a stable identity; required
        when: # optional; an omitted `when` constrains nothing
          failure: http
          provider-error:
            code: insufficient_quota
            type: insufficient_quota
        action: fallback
      - id: streamed-503 # a new identity appends to the inherited matrix
        when:
          status: 503
          streaming: true
        action: retry
```

`id` and `action` are required on a rule entry. Predicates are typed and drawn
from a closed vocabulary — `status`, `status-class`, `failure`
(`http`/`transport`/`protocol`/`caller`), `transport-class`,
`transport-cause`, `protocol-cause`, `caller-cause`, `provider-error.type`,
`provider-error.code`, `streaming`, `candidate-index`, `retry-index` — and a
token outside it is rejected at load, because a rule that could never fire is a
misunderstanding of the schema rather than a policy. There are **no
expressions, no scripting, no regular expressions, and no body matching**: a
predicate is a bounded, validated value, never a pattern or a memory. The only
provider-supplied inputs are the upstream error object's own `type`/`code`
members, already gated to short printable tokens, so a provider's free-text
`message` can never be matched. Restricting the vocabulary to values the proxy
can prove it understands is what makes evaluation total and deterministic —
there is no operator-supplied program to time out, to argue about, or to
silently mis-evaluate.

### Precedence

Evaluation is first-match-wins over rules ordered by a precedence key derived
from the predicates themselves, highest first, then by rule identity ascending
so the order is total. From most to least specific:

1. **caller hard-stop** — a caller cancellation or expired deadline ends the
   walk before the matrix is consulted at all (see the invariants below);
2. **exact HTTP status** — `status: 429`;
3. **provider-specific error predicate** — `provider-error.type`/`.code`;
4. **status class** — `"5xx"`;
5. **failure-specific rule** — a transport, protocol or caller cause, such as
   `transport-cause: tls`;
6. **failure class / transport class** — `failure: http`,
   `transport-class: connection`;
7. the **`default`** action, under the reserved rule identity `default`.

Nothing in that order depends on Go map iteration or on the order rules happen
to appear in YAML: shorthand keys expand in sorted order into canonical rule
IDs (`http-429`, `http-class-5xx`, `transport-class-connection`,
`transport-cause-tls`, `protocol-invalid_response`, `caller-canceled`, …), the
free-form list is folded in and the whole matrix re-sorted deterministically.
Because the shorthand canonicalises to those IDs, an override may replace any
of those rows by name without restating it. There is no `priority` field:
precedence is a property of the predicate shapes, not a number an operator
assigns.

Two rules of **equal precedence that can match the same failure are rejected at
load** — an ambiguous policy has no defensible resolution, and silently
preferring one of two equipollent rows would make the effective policy depend
on something the operator cannot see. So are a missing or malformed rule
identity, a repeated identity, and a predicate set that contradicts the failure
layer it names (an HTTP status on a transport rule, two cause kinds on one
rule, a status inconsistent with its own class).

### Retry mechanics

```yaml
recovery:
  retries:
    max-retries: 1 # re-asks AFTER the initial attempt, 0..8
    max-elapsed: 10s # the candidate's whole retry window, >= 1ms, <= 2m
    on-exhausted: fallback # fallback (default) | terminal
    backoff:
      initial: 250ms # first retry delay, >= 1ms
      max: 2s # ceiling for the doubling AND for Retry-After, >= initial, <= 2m
      jitter: 0.1 # uniform +/-fraction spread, 0..1
```

Retry mechanics never decide _whether_ a failure is retryable — that is the
matrix's job — only how often, how long, and how long to wait. `max-retries`
counts re-asks after the initial attempt, per candidate: `0` means one upstream
exchange per candidate, `1` two (the default), `3` four. `max-elapsed` closes
the candidate, measured from its FIRST attempt and checked before a wait is
scheduled, so a sleep never runs past the window. The delay starts at
`initial`, doubles per retry, saturates at `max`, then spreads by a uniform
±`jitter` so a provider coming back from an outage is not re-hit by a
synchronized herd of retries. When a retryable failure arrives with the retry
budget spent, `on-exhausted` decides: `fallback` (the default) moves to the
next candidate, `terminal` relays the answer. `on-exhausted: retry` is
rejected — a retry budget that re-arms itself is not a budget.

### Fallback mechanics

```yaml
recovery:
  fallback:
    enabled: true # default true
    max-candidates: 2 # candidates ENTERED, 1..8
    on-exhausted: terminal # always terminal
```

`max-candidates` counts candidates **entered**, the primary included — not
candidates the chain happens to list. A chain of eight behind a budget of two
walks two. Once the walk has entered its last reachable candidate and that
candidate fails, the walk is over: `on-exhausted` is always `terminal`, and any
other value is rejected, because there is nowhere further to go. `enabled:
false` pins the request to the primary candidate and is exactly a one-candidate
reach, so `max-candidates` must then be `1`; a disabled walk stating a larger
reach is rejected rather than silently ignored. Same-candidate retries still
apply under the pinned candidate. How far a request may walk is a property of
the request's primary policy, not of the candidate it happens to be standing
on, so a candidate-level override cannot extend another candidate's reach.

### The exchange budget

Retries, fallbacks, and egress fallback multiply: one candidate attempt may
fan out into several pool dials, and the walk multiplies that by its candidates
and their retries. Two nested envelopes bound the product where it is actually
spent.

```yaml
recovery:
  budget:
    request: # the whole request; top-level block only
      max-exchanges: 32 # 1..64
      max-elapsed: 5m # >= 1ms, <= 5m
    candidate: # each candidate within it
      max-exchanges: 16 # 1..32, must not exceed the request envelope
      max-elapsed: 2m # must not exceed budget.request.max-elapsed
```

`max-exchanges` counts **real outbound HTTP exchanges**: a pool that falls back
across three members spends three, not one. A member skipped before dialing —
ineligible, unhealthy, saturated — spends nothing, because it never reached the
wire. The unit is claimed by the transport layer immediately before a dial,
after every eligibility gate, which is what makes the count honest: a
handler-side count would miss the exchanges a pool's fallback adds to one
candidate attempt. A refusal stops that dial; nothing is dialed and no endpoint
is blamed — an endpoint that was never reached is never struck, and a pool that
refused the claim hands its concurrency permit back. `max-elapsed` bounds each
scope in wall-clock time.

The two envelopes stop different things. A spent **request** envelope ends the
walk: no candidate may start another exchange, so the request is over. A spent
**candidate** envelope ends only _that_ candidate's turn — another exchange on
the same candidate (a retry) is refused, and the retry rule's `on-exhausted`
action decides whether the walk moves on, with the next candidate opening its
own envelope fresh. A per-candidate number therefore sizes one candidate's
retries; it never pins a chain the operator configured to fall back.

A refusal is not a failure and is never reported as one: nothing was dialed,
no endpoint is blamed, and no `provider_attempt_failed` or
`egress_attempt_failed` is emitted for it. The exhausted candidate's turn ends
on a WARN `candidate_exchange_budget_spent` carrying `policy_rule_id`
(`budget-candidate`, or `budget-request` when the request envelope is the one
that refuses — which is terminal whatever `on-exhausted` says), the closed-set
`error_cause: exchange_budget`, and the walk's position (`candidate_index`,
`candidate_attempt`, `retry_index`, `provider_attempt`,
`request_exchange_budget_remaining`). It carries no `upstream_exchange` and no
`disposition`, because no exchange happened for it to have a place in.

**A configuration above a cap is rejected, never clamped.** The absolute caps
are `64` and `32` exchanges and `5m`/`2m` elapsed for the request and candidate
envelopes respectively, `8` retries, `8` fallback candidates, and `2m` for the
backoff ceiling and the retry window. A value silently reduced to something
else (or grown to it) is a policy the operator did not write and cannot read
back from the file. The same fail-closed rule covers contradictions between
envelopes: a candidate envelope above the request's, a retry window wider than
the candidate envelope it runs inside, a retry budget (`max-retries + 1`) the
candidate's exchange envelope cannot fund, or a `backoff.max` below
`backoff.initial`. All are whole-file rejections — on reload they land on the
last-known-good snapshot.

The defaults never bind a default deployment. The default walk reaches at most
`2 candidates × 2 attempts × 3 egress attempts = 12` exchanges against a
request envelope of 32, which is why the envelope is a backstop against a
runaway walk rather than a tuning knob. An unstated `retries.max-elapsed` and
an unstated candidate envelope default to their caps for the same reason: a
default that rejected a window the vocabulary allows would make a previously
valid file unloadable.

`budget.request` may only be stated in the top-level block; a provider, model,
or candidate override that states one is rejected by position, because the
request-wide ceiling is a property of the request and two candidates must not
be able to disagree about it.

### Retry-After

```yaml
recovery:
  retry-after:
    enabled: true # default true
    mode: max # max (default) | ignore
    max-delay: 5s # this policy's own ceiling on the directive
```

An upstream `Retry-After` (delta-seconds or an HTTP-date) is honored at all
only when `enabled`; `mode: max` treats it as a **floor** — the wait becomes
the larger of the jittered backoff and the directive — while `mode: ignore`
discards it and sleeps the backoff schedule. `max-delay` bounds the
**directive**, not the schedule: a directive larger than `max-delay` is
reduced to it before it is compared with the backoff, and every wait is capped
by `backoff.max` regardless. A `max-delay` below `backoff.max` therefore
shortens how far an upstream can push the wait; it never shortens a wait the
operator's own backoff schedule asked for. The default (`5s`) sits above the
default backoff ceiling (`2s`) deliberately, so by default the backoff is what
binds. Invalid, negative, zero, unparseable, and already-past values are
ignored silently.

**An upstream can never make the gateway sleep longer than `backoff.max`, the
retry-after `max-delay`, or the caller's remaining deadline — whichever binds
first.** The remaining retry window is a fourth veto. Every cap is applied in
order, and each one bounds either the directive or the whole wait, never the
configured schedule on the directive's behalf, so a hostile or broken upstream
cannot buy itself an arbitrarily long gateway sleep.

### Answers, commitment, and reload

- **The last HTTP answer wins.** Whatever the walk's history, the relayed
  response is the LAST answer a candidate produced: its status is preserved
  and a 4xx/5xx body is the canonical envelope — never raw provider bytes. A
  `429` or `503` from a later candidate is never collapsed into a `502`. That
  includes the retained-answer rule: when a candidate's budget runs out on a
  received answer and the walk moves on, only to have every later candidate
  fail BEFORE answering (a dial failure, an exhausted pool), the client
  receives the retained answer — the candidate that actually answered
  becomes the completion record's `final_provider`, and
  `provider_exhausted` stays unset, because a provider answered. The
  `502` `upstream_unreachable` + `provider_exhausted` pair is reserved for
  the case where NO candidate ever produced an HTTP response, and a capture
  failure (timeout or read error) on the final retained answer answers
  `502` `upstream_invalid_response`.
- **Commitment is the hard boundary.** The whole walk — same-candidate
  retries and candidate fallbacks — completes before any status is written
  to the client. The candidate whose answer produces the first
  client-visible byte has produced THE response: no retry and no candidate
  switch after that, ever. A `200` SSE stream whose upstream dies before the
  first event is truncated and logged, never retried or replaced mid-flight.
- **Replay is fresh and identical.** Each attempt — a same-candidate retry
  included — rebuilds the request from the same immutable client body
  through that candidate's own transform: its own `upstream-model`, the same
  injected prompt. Nothing observed on an earlier attempt feeds the next
  one. A body that fails the transform still answers `400` on the first
  candidate — it would fail every candidate's transform, so it never spends
  budget.
- **Reload invariants hold.** The chain, every candidate's resolved recovery
  policy, and every candidate's transport bind to the request's config
  snapshot like everything else — a reload mid-walk, mid-wait included,
  cannot reshape the candidate list, the effective matrix, the envelopes, or
  the backoff of in-flight work. Every candidate's transport is part of the
  snapshot's egress closure, so a fallback candidate's connection pool is
  warm even when the primary answers everything.
  `fallback.enabled: false` pins the request to the primary candidate; the
  pinned candidate's same-candidate retries still apply under its own retry
  mechanics.

### Observability

Every decision event carries the identity of what decided the attempt, and the
walk's counters ride the completion record:

- `policy_rule_id` — the rule that decided: a canonical or operator-defined
  rule ID (`http-429`, `transport-cause-tls`, `provider-quota-exhausted`), the
  reserved `default` when no rule matched, or one of the code-owned invariant
  identities (`committed`, `caller`, `budget-request`, `budget-candidate`) when
  the engine hard-stopped before the matrix was consulted. "The matrix decided
  this" is therefore verifiable per request rather than inferred.
- `policy_hash` and `policy_generation` — the identity of the resolved policy
  the request bound to, so a behavior change can be tied to the file that
  produced it.
- the attempt identity: `candidate_index` (1-based chain position),
  `candidate_attempt` (1-based within that candidate), `retry_index`
  (`candidate_attempt − 1`; `0` = the initial attempt), `egress_attempt`
  (1-based within one candidate attempt's egress dials), `provider_attempt`
  (1-based provider-level attempt across the walk), `upstream_exchange` (the
  real outbound exchange, counted where the dial happens), and
  `request_exchange_budget_remaining` — how much of the REQUEST-wide envelope
  is still unspent. It is deliberately not the tighter of the two envelopes:
  after a candidate has spent its own, the tighter number would read zero on a
  request that still had most of its budget.
- the counters: `candidates_entered`, `candidate_attempts`, `retry_attempts`,
  `egress_attempts`, and `upstream_exchanges`. The completion record carries
  all five, `request_exchange_budget_remaining` included.

`provider_attempts` counts provider-level attempts; `upstream_exchanges` counts
real outbound exchanges — and the two differ whenever an egress pool falls
back, because one provider attempt that dials three members is three exchanges.

Each failed attempt logs one WARN `provider_attempt_failed`, and every received
4xx/5xx — attempts a retry or fallback later discarded included — logs its
`upstream_http_error` evidence event; both carry `disposition` (`retry`,
`fallback`, or `terminal` — what the walk decided about one attempt, never
`answer` or `success`), the closed-set `reason` token, and `elapsed_ms`, plus
the received status as `upstream_status` on the events that have one — a
transport failure has none. Each dialed-and-failed egress endpoint — pooled or
direct — logs one WARN `egress_attempt_failed` in the same vocabulary. An
exchange the envelope refused before a dial is not a failure and gets its own
WARN `candidate_exchange_budget_spent` instead, carrying
`policy_rule_id`/`disposition`/`reason` and the walk's position but no
`upstream_exchange` — there was no exchange for it to index. Details under
[Logging](#logging).

### Invariants that are not configurable

Everything above is data. A short list is not, because a configuration that
could weaken it would turn a client disconnect into upstream traffic, or a
committed response into a retried one:

- **a committed response is final** — once the first client-visible byte
  exists, no retry and no fallback, whatever the matrix says;
- **a caller cancellation or expired deadline is terminal**, decided from the
  request context rather than from any rule, never retried and never slept
  through;
- **the exchange envelope is absolute** — no retry, fallback, or dial happens
  once either envelope is spent;
- **replayability** — each attempt rebuilds its request from the same
  immutable client body, so no attempt can observe another's leftovers;
- **determinism** — the decision for one (policy, observation, budget state) is
  a pure function of the policy; nothing reads map iteration order;
- **immutability** — a resolved policy is a value bound to the request's
  snapshot, so a reload can never reshape in-flight decisions;
- **bounded, secret-free logging** — no event carries a body, a prompt, or a
  credential, and a `policy_rule_id` is an identity, never operator prose.

A policy may _describe_ the caller's cancellation — the default matrix does, as
a terminal row — but no configuration can make it anything else: a caller rule
that is not `terminal` is rejected at load, and so is a `caller` shorthand
entry with any other action.

### Compatibility with the legacy blocks

Two blocks predate the policy engine and still load:

- a top-level `provider-fallback` (`enabled`, `max-attempts`) — normalized into
  the **global** layer's fallback policy, where `max-attempts` becomes
  `fallback.max-candidates` and an omitted `max-attempts` keeps its historic
  default of two. A block that disables the walk states a reach of one whatever
  `max-attempts` says, because that is what the block always meant: the count
  was read only while the walk was on, so a file carrying both ran pinned and
  still does;
- a per-model `retries` (`max-retries`, `max-elapsed`, `backoff`) — normalized
  into the **model** layer's retry mechanics.

Both build the same partial a `recovery` block builds, and there is exactly
**one effective policy engine at runtime** — there is no mode in which the
legacy blocks run alongside it. The two spellings are alternatives for the same
policy _within one layer_, so a layer that states both rejects the file rather
than leaving the effective behavior to whichever the code happened to read
first. Precisely:

- a top-level `provider-fallback` next to a top-level `recovery.fallback` is
  rejected as mutually exclusive; `provider-fallback` next to a `recovery`
  block that states no `fallback` is accepted, and the legacy block becomes the
  global layer's fallback policy;
- a model `retries` next to a model `recovery.retries` is rejected as mutually
  exclusive; `retries` next to a `recovery` block stating some other section (a
  `matrix`, say) is accepted, because the two speak about different mechanics
  and both apply;
- there is no legacy spelling at the provider or candidate position.

A file that states neither runs the built-in default policy — the disposition
table this proxy has always had, now stated as data in the `internal/recovery`
domain.

**Behavior change.** Deployments upgrading from ≤ 0.7.0 will see traffic this
proxy previously relayed once now re-asked and re-routed: under the default
policy a 408, 425, 429 or 5xx (except 501/505, and a 4xx/5xx body that stalls
its capture) is retried on the same candidate — one retry by default, after a
bounded wait — and 401/403/404/405/409/422 walk to the next candidate, where
earlier versions relayed the first received status immediately. Set
`retries.max-retries: 0` to drop the same-candidate re-asks and
`provider-fallback.enabled: false` to pin the primary candidate — together they
restore the previous single-attempt walk — or write the equivalent `recovery`
block and change the rows themselves; the disposition is now an operator's to
edit, not only to size.

**Not included, by design:** automatic egress rotation over time, active
health probes, weighted or scored provider selection (the chain order is
the operator's, not computed), retries or fallback after response
commitment (once a status reaches the client the answer is final),
caller-driven retry knobs (the effective policy comes from YAML — no
per-request override), and any proxy-to-direct silent downgrade: a
candidate whose pool has no usable member is a transport failure the walk
moves past, and a walk that ends with nothing but unreachable egress
answers the canonical `502` `upstream_unreachable`.

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

| Condition                                                                                                                             | Status                 | `error.type` / `code`                                                                                                                                                                           |
| ------------------------------------------------------------------------------------------------------------------------------------- | ---------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Missing/malformed `Authorization: Bearer <key>`                                                                                       | 401                    | `invalid_request_error` — exact body: `{"error":{"message":"you must provide an API key in the Authorization header (Bearer <key>)","type":"invalid_request_error","param":null,"code":null}}`  |
| Wrong bearer key (partner mode: unknown **or revoked** key, or an unavailable key store — fail closed)                                | 401                    | `invalid_request_error` / `invalid_api_key` — exact body: `{"error":{"message":"invalid API key","type":"invalid_request_error","param":null,"code":"invalid_api_key"}}`                        |
| Body is not JSON                                                                                                                      | 400                    | `invalid_request_error` — exact body: `{"error":{"message":"invalid JSON in request body","type":"invalid_request_error","param":null,"code":null}}`                                            |
| Missing `model`                                                                                                                       | 400                    | `invalid_request_error` — exact body: `{"error":{"message":"you must provide a model parameter","type":"invalid_request_error","param":null,"code":null}}`                                      |
| Request body over the 64 MiB cap                                                                                                      | 413                    | `invalid_request_error` — exact body: `{"error":{"message":"request body too large","type":"invalid_request_error","param":null,"code":null}}`                                                  |
| Request names an unmapped model                                                                                                       | 404                    | `model_not_found` — exact body: `{"error":{"message":"The model '<X>' does not exist or you do not have access to it.","type":"invalid_request_error","param":null,"code":"model_not_found"}}`  |
| Request path matches no route (unknown path, trailing slash, wrong case)                                                              | 404                    | `invalid_request_error` — exact body: `{"error":{"message":"Invalid URL (<METHOD> <PATH>)","type":"invalid_request_error","param":null,"code":null}}`                                           |
| Upstream unreachable (dial/network, no candidate answered)                                                                            | 502                    | `upstream_error` / `upstream_unreachable`                                                                                                                                                       |
| Upstream 200 with unparseable body (or body over the 64 MiB buffered cap, or a body read that fails mid-answer), walk finalized on it | 502                    | `upstream_error` / `upstream_invalid_response`                                                                                                                                                  |
| Upstream answers 4xx/5xx                                                                                                              | same as upstream       | `upstream_error` / `upstream_http_<status>` — canonical envelope: `{"error":{"message":"upstream provider returned HTTP 429","type":"upstream_error","param":null,"code":"upstream_http_429"}}` |
| Upstream answers 3xx (redirect), 204, or 304                                                                                          | **forwarded verbatim** | status, bytes, and an allow-list of headers pass through (see below)                                                                                                                            |

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
  all), and the status preserved is the LAST answer the walk received — under
  the default recovery policy a retryable status (408/425/429, any 5xx except
  501/505) may first be re-asked within the candidate's retry budget and then
  walk to the next candidate; see
  [Provider recovery policy](#provider-recovery-policy) — but the
  body is always the canonical envelope above and
  `Content-Type` is always `application/json`. The provider's raw body —
  its message text, its HTML, even its model names — never reaches the
  client: an upstream error that quotes the alias target discloses nothing.
  A bounded prefix (64 KiB) of the error body is read once, solely to
  classify its shape and fingerprint it for the log evidence event (see
  Logging); those bytes go nowhere else — not to the client, not into any
  log line. The read is also bounded in time, under a short fixed
  internal timeout (not runtime configuration): an upstream that answers
  headers and then stalls on an error body has produced an unusable answer,
  and the walk treats it like one — the same candidate is re-asked while the
  retry budget remains, then the next candidate is tried; the canonical
  `502` `upstream_invalid_response` replaces a hung request only when the
  capture fails on the final retained answer. A caller
  whose context ends during the capture gets the disconnect outcome, no
  envelope. `Retry-After` and the `X-RateLimit-*` headers still ride the
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
and egress attempt counters (`provider_attempts` counts provider-level
attempts — same-candidate retries included — summed across every candidate of
a fallback walk; `upstream_exchanges` counts the real outbound HTTP exchanges
the walk spent, so the two differ whenever an egress pool fell back; egress
dials are summed the same way), the final candidate's
egress kind (`direct`, or the last dialed pool
member's kind; empty when a pool exhausted without dialing anything), and full
request latency. Provider fallback and same-candidate retries still emit
**one** event: it describes the
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
  `provider_attempt_started` (origin + forwarded byte count, emitted before
  the attempt's first dial on both the direct and pooled paths),
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
  `egress_exhausted`) and the recovery walk's
  (`policy_hash`, `policy_generation`, the counters `candidates_entered`,
  `candidate_attempts`, `retry_attempts`, `egress_attempts` and
  `upstream_exchanges`, `final_provider` and `final_candidate`, the relayed
  candidate's 1-based chain position, plus
  `provider_exhausted` when every budgeted candidate failed without
  answering). `provider_attempts` counts provider-level attempts and
  `upstream_exchanges` counts real outbound exchanges — they differ whenever
  an egress pool falls back. The event is emitted
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
  that goes away mid-request — a disconnect or an expired deadline,
  including while the upstream request is in flight
  (`upstream_request_failed` with `error_class` `canceled` and
  `error_cause` `caller_canceled` or `caller_deadline_exceeded`, outcome
  `client_disconnected`, and no error envelope, since the client is
  gone), and an upstream 4xx — the `upstream_http_error` evidence event
  described below — a candidate whose exchange envelope refused another dial
  (`candidate_exchange_budget_spent`, `error_class` `provider_exhausted` over
  `error_cause` `exchange_budget`: a refusal, not a failed endpoint, so no
  endpoint is blamed and no `egress_attempt_failed` accompanies it) — one
  warning per transition into a failed config
  state (`config_file_unreadable`, `config_reload_rejected`) — including a
  failure that changes kind, which warns again — never one per poll tick —
  plus `second_signal_forced_exit` and drain overflow.
- **ERROR** — upstream connection failures (`upstream_request_failed`
  with a canonical `error_class` — `connection`, `timeout`,
  `proxy_connect`, `proxy_auth` — and a closed-set `error_cause` such as
  `connection_refused`, `tls`, `dial`, `network_timeout`, `proxy_timeout`;
  a candidate budget exhausted without an answer reports `error_class`
  `provider_exhausted` over the final cause — never `canceled`, which is
  the WARN disconnect above), an
  upstream that died mid-relay on the verbatim path (`relay_copy_failed`
  with phase `upstream_read` — the one relay failure that is not a
  disconnect), and an upstream 5xx (`upstream_http_error` at error
  severity — 5xx is our outage even when the provider owns the cause),
  plus anything fatal at startup. A 200 that is not parseable JSON — or is
  over the buffered cap — logs one WARN `upstream_invalid_response` per
  discarded attempt (like the error evidence event above), and surfaces as
  the `upstream_invalid_response` outcome on the INFO completion line with
  the 502 envelope on the wire only when the walk finalizes on it: the
  candidate may still be re-asked, or the next candidate may answer.

**The upstream 4xx/5xx evidence event.** Every received upstream 4xx/5xx
emits one `upstream_http_error` event (WARN for 4xx, ERROR for 5xx) bound
to the request's `request_id` — attempts a retry or fallback later
discarded included, so a provider's flakiness stays visible even when a
later attempt answers. Still one bounded, sanitized record per status:
`api`, `public_model`, `upstream_model`,
`upstream` (scheme+host only), `upstream_status`, `content_type`,
`error_class` `upstream_error` with `error_cause`
`upstream_http_4xx`/`upstream_http_5xx`, `error_shape`
(`empty`, `json_error_object`, `json`, `text`, `malformed_json`,
`truncated`), `body_bytes` (the size of the captured prefix — the whole
body when it fit under the 64 KiB cap), `body_truncated`, and
`error_fingerprint` — the SHA-256 hex digest of that bounded prefix, the
join key for correlating repeated provider errors without keeping any of
their bytes. When the provider's error object carries its own
token-shaped `type`/`code` (`rate_limit_error`, `insufficient_quota`, …)
they appear as `provider_error_type`/`provider_error_code` — only when the
value is short printable ASCII, never the free-text `message`.
`content_type` is log-normalized to the parsed media type alone (parameters
dropped — their values are arbitrary upstream bytes), with the static
markers `invalid`/`oversized` for unparseable or oversized values; the
wire relay of response headers is untouched. The
allow-listed `Retry-After`/`X-RateLimit-*` headers ride along as
`retry_after`/`x_ratelimit_*` fields when present (values longer than 128
bytes are dropped from the log — nothing an upstream controls can balloon a
log line; the client-side relay is unaffected). The raw error body
itself never appears at any level: it exists only as the count, the shape,
and the fingerprint. Every attempt-bearing lifecycle event
(`provider_attempt_started`, `upstream_response_received`,
`upstream_request_failed`, `provider_attempt_failed`, the evidence event)
also carries the attempt identity described under
[Provider recovery policy](#provider-recovery-policy): the nested
`provider_attempt` (a one-based count of logical provider-level attempts
across the whole request — numerically the old candidate index when no retry
fires; one logical attempt may span several outbound exchanges when an
egress pool falls back),
`provider_attempts` and `upstream_exchanges` (the first counts provider-level
attempts, the second the real outbound exchanges — they differ whenever an
egress pool falls back), `policy_rule_id` (the rule or code-owned invariant
that decided the attempt), `request_exchange_budget_remaining`, and, on
the events after a dial, `egress_attempt` (`provider_attempt_started` fires
before one, so it carries the provider indexes only), plus `candidate_index`
(1-based chain position), `candidate_attempt`
(1-based within the candidate), `retry_index` (`candidate_attempt − 1`;
`0` = the initial attempt), `upstream_exchange` (the one-based real exchange
index, counted where the dial happens), `disposition` (`retry` | `fallback` |
`terminal`), `reason` (a closed token set: `http_408`,
`http_425`, `http_429`, `http_5xx`, `http_<code>` for every other status
below 500 — fallback-only and terminal rows alike — the transport cause
tokens, and `upstream_invalid_response` /
`upstream_body_timeout` / `upstream_body_read_failed` for unusable
answers, which ride the unusable-answer events rather than
`provider_attempt_failed`), `failure_origin` (`upstream_http` | `transport` |
`protocol` | `caller` | `envelope` — the layer the failure belongs to) and
`elapsed_ms`. Transport failures additionally carry `send_state`
(`definitely_not_sent` | `send_unknown`): whether this dialed attempt
provably never carried a request byte. It is evidence about a **dialed**
attempt, so a pool that skipped every member reports none — that line says
`egress_exhausted`, which is the whole truth about it. The evidence and
body-read/invalid events carry
the received HTTP status as `upstream_status` — a transport failure has no
status to carry — so failures correlate by
`request_id + candidate_index + candidate_attempt + egress_attempt`.

Two naming notes, so a dashboard is not built on the wrong reading.
`provider_attempt_started` replaced the former `upstream_request_started`
(the slug now names what it announces), and it is emitted **before** the
dial, so its `provider_attempt` is the index the attempt _will_ carry: an
attempt the exchange envelope refuses emits the marker and then no matching
attempt record. It is a marker for correlating a wait, not a count — the
counts are `provider_attempts` and `upstream_exchanges` on the records that
follow an actual dial.

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
- **More egress machinery than pools already provide** — egress pools with
  scheduling, eligibility gates, bounded fallback and passive health exist
  (see [Provider transports](#provider-transports)); what stays out is
  automatic egress rotation over time, active health probes, per-request
  egress selection, and retries after response commitment or caller-driven
  retry knobs (the recovery policy is resolved from YAML, per request — an
  operator cannot let a client ask for a retry).
- **TLS configuration** — upstream and proxy TLS verify chain and host with
  system roots; custom TLS setup (client certificates, custom CA pools,
  `insecure-skip-verify`) is not coming. (Ambient
  `HTTP_PROXY`/`HTTPS_PROXY`/`NO_PROXY` environment variables still apply
  to `direct` transports, inherited from `net/http`'s default transport.)
- **Authorization beyond key validity** — partner mode authenticates
  per-partner keys (see [Partner API keys](#partner-api-keys)); roles,
  tenant isolation, quotas, per-key model restrictions, and RBAC need a
  separate design.

## Repository layout

```
cmd/openai-compatible-injector/  entrypoint + version/healthcheck/keys subcommands
internal/config/                 bootstrap, runtime YAML (models, providers, transports, recovery policy), snapshot store, poller
internal/auth/                   client identity: static + partner key store, decision cache, SQL migrations
internal/migrate/                shared module-scoped SQL migration runner
internal/usage/                  factual upstream usage capture, async pipeline, PostgreSQL repository
internal/inject/                 pure request/response transforms (probe, chat, responses, rewrite, thinking usage)
internal/recovery/               recovery policy domain: typed failure matrix, layered resolution, retry/fallback mechanics, exchange budget
internal/transport/              outbound paths: Doer seam, direct client, proxy (http/https/socks5/socks5h), pool registry, exchange-budget seam
internal/proxy/                  handler, client auth gate, SSE copy, error envelopes, candidate walk driven by the recovery engine
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
