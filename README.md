# openai-compatible-injector

A minimal OpenAI-compatible **request/response injector proxy**. Clients talk
to it as if it were an OpenAI endpoint — authenticating with the single
`api-key` from the runtime config — and it forwards to configured upstream
providers, renaming the model and injecting a per-model system prompt into
every request. Hot-reloadable model mapping, provider retry/fallback and
egress pools, optional post-commitment SSE stream continuation, optional
durable factual usage metering, one static binary.

```
 client ──POST /v1/chat/completions (Bearer api-key)──▶ injector ──forward (model→upstream-model, prompt injected, credential consumed)──▶ upstream provider
         ◀──model rewritten to public name──●
```

Supports two model-serving protocols and one configured discovery endpoint:

- **Chat Completions** — `POST /v1/chat/completions`, including SSE streams.
- **Responses API** — `POST /v1/responses`, including SSE streams.
- **Model discovery** — authenticated `GET /v1/models`, served from the current
  runtime model mapping without an upstream request.

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
# and one chain candidate; each layer merges onto the one before it. Three
# members are position-scoped and rejected elsewhere — `budget.request` only
# here, `fallback` only here or on a model entry, `stream` only here or on a
# model entry.
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
  # Optional, OFF BY DEFAULT. Continue a COMMITTED SSE stream that ended
  # without its terminal marker, on the client's existing connection. See
  # "Stream recovery".
  # stream:
  #   enabled: false # absent means streams are never continued
  #   max-recoveries: 1 # continuation REQUESTS per stream, 0..2
  #   max-elapsed: 20s # maximum upstream silence, >= 1ms, <= 2m
  #   max-partial-bytes: 262144 # committed text held to build the hop, 1KiB..1MiB

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
  `Authorization: Bearer <key>` on every supported `/v1` endpoint, including
  `GET /v1/models`; the scheme is case-insensitive and outer spaces are
  ignored. It is bound to the [per-request config snapshot](#hot-reload), so
  rotating the YAML value affects subsequent requests without a restart. The
  key is credential material: it never appears in logs, error text, or reload
  metadata.
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
- `strip-fields` — optional list of response paths excised from this model's
  upstream 2xx responses before relay and from the model's whole candidate
  chain. Each entry is a dotted JSON path (`parent.child`); a segment
  containing a dot, space, or quote is wrapped in single quotes and decoded
  with JSON unquoting rules (`'parent name'.child`, `'a.b'.c`, `'a''b'.c`).
  The traversal is object-only (a path never descends through an array), the
  scope follows the API (chat: top-level object; responses: top-level plus one
  `response`-object descent), and stripping is byte-preserving — everything
  outside an excised member is untouched. The paths the proxy itself writes —
  `model`, `usage`, and the two synthesized reasoning-count leaves, each
  reserved in its bare and its `response.`-prefixed spelling — are rejected
  at load, while provider-added members inside those objects (`usage.is_byok`,
  `usage.cost`) stay addressable. An absent
  or null list means no stripping — responses stay byte-identical. A model
  that states a list replaces its provider's list; a model without one
  inherits each candidate's provider list per hop.
  See [Response field stripping](#response-field-stripping).
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
  `upstream-model`, `injection-prompt`, `thinking-usage`, `strip-fields`,
  `retries`, `recovery` and
  `providers` (and, inside the thinking-usage block, outside `mode`,
  `min-ratio`, `max-ratio`; inside `retries`, outside `max-retries`,
  `max-elapsed` and `backoff` — itself limited to `initial`, `max` and
  `jitter`; inside `recovery`, outside `matrix`, `retries`, `fallback`,
  `budget` and `retry-after` — each with its own strict field set) — including
  a nested bootstrap key — is a rejection, not a
  warning. The
  same strictness holds inside `providers` entries (`base-url`,
  `transport`, `recovery`, `strip-fields`), model-chain candidate entries (`provider`,
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

## Model discovery

`GET /v1/models` authenticates with the same bearer credential as the two
model-serving routes and answers from the current request's runtime config
snapshot. It **never calls an upstream provider**: the public mapping is the
catalog, while an upstream list could advertise unusable names or disclose an
alias target. Model IDs are sorted lexicographically for deterministic output;
a successful config reload affects the next list request.

The response is a JSON list envelope. Each entry deliberately contains only
the configured public `id` and `created: 0` (the injector has no creation
metadata):

```json
{ "object": "list", "data": [{ "id": "echo-model", "created": 0 }] }
```

`POST`, `PUT`, `DELETE`, and every other non-GET method return the normal JSON
405 before authentication. Missing/malformed and invalid bearer credentials
return the same exact 401 envelopes as the model-serving routes. A near-miss
path such as `/v1/models/` remains the JSON 404 catch-all.

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
    process — a refused or unreachable dial, a TLS certificate-verification
    failure, a proxy CONNECT or proxy-auth failure — the next eligible
    member is tried, up to `max-attempts` distinct endpoints. Evidence, not
    classification, is what decides it: the failure's _wire operation_ is
    read, so a connect that timed out against a blackholed path counts as
    unsent (nothing was written), while a reset, a bare EOF, or a timeout on
    an established connection reads `send_unknown` — the peer may already
    be processing a request whose answer never came back, and replaying it
    on another egress would silently duplicate it. Send-unknown stops the
    loop and travels up to the recovery policy, which owns replay; the
    per-dial evidence line carries `send_state`, so "the pool stopped short
    of its remaining members, and why" is one field away. The unsent set is
    read off _typed_ failures only, so it is narrower than "the connection
    was never established": a TLS handshake that fails for any reason other
    than certificate verification — a fatal alert from the peer, a peer
    answering the hello with plaintext HTTP, a peer that closes at accept —
    is pre-send in fact but arrives in a shape Go also produces on an
    established connection, so it reads `send_unknown` and the pool stops
    rather than risk duplicating a request the type cannot rule out. A
    client cancellation — or an expired caller deadline, which the request context
    reports the same way — aborts everything: no fallback, no strike, no
    penalty. The request context, not the error chain, decides ownership.
    **Any response — 429 and 5xx included — ends the attempt loop**: an
    HTTP status is the upstream's answer, never a fallback trigger and
    never a health strike.
    **An exchange the envelope ended before a response is `send_unknown`,
    so a pool stops there too.** Its members buy redundancy against a
    connection that provably never carried the request, not against this
    proxy's own `max-elapsed`: provider-level fallback (the next candidate)
    still applies, but member-level fallback does not, because the request
    may already have reached the member that went quiet.
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

## Provider upstream credentials

A provider may carry an optional `auth:` block: upstream API-key
authentication with **multi-key rotation** across accounts, and
**429-aware cooldown** per account.

```yaml
providers:
  opencode:
    base-url: https://api.opencode.example/v1
    transport: egress
    auth:
      type: api_key # the only accepted value today
      header:
        Authorization # required: the RFC 7230 field-name the
        # credential is sent in (Authorization,
        # X-API-Key, ...)
      prefix: "Bearer " # optional, default "" — placed before the value
      strategy:
        round_robin # required — the only accepted value today; the
        # file states it, there is no default
      rate-limit: # optional, defaults cooldown 2s / max 60s
        cooldown:
          2s #   default cooldown when the upstream sends no
          #   usable Retry-After
        max-cooldown:
          60s #   ceiling on what an upstream Retry-After may
          #   request from a key
      keys:
        - id: primary # operator-chosen name, [A-Za-z0-9._:-]; the
          # only credential-adjacent identifier that
          # ever reaches a log
          value:
            replace-me # the secret material itself; never logged,
            # never echoed by an error, memory + wire only
        - id: secondary
          value: replace-me-too
```

The block lives on a provider (all its models share the pool); the inline
`endpoint` form cannot declare one.

How it composes with the architecture:

- **Recovery stays the only authority.** The pool never retries, never
  waits, never answers a request. It only decides WHICH account the next
  attempt carries — and reports "none ready" to the recovery policy, which
  decides what the request does about it (retry after the earliest cooldown,
  fall back to another candidate, or terminate — per the operator's matrix).
- **One account per attempt.** Each attempt acquires exactly one key. A
  retry of the SAME candidate on a non-429 failure (5xx, protocol, ...)
  keeps the same key — it is the same logical attempt on the same account.
  An egress switch inside one attempt keeps it too: the credential is
  composed above the transport, so path changes never change accounts.
- **A 429 marks the account, and only the account.** On an upstream 429 the
  request's key enters cooldown (the upstream said the account is
  rate-limited) _whatever the recovery policy later does with the request_.
  The cooldown is the `Retry-After` directive when usable, capped at
  `max-cooldown`, otherwise `cooldown`. The next attempt on that candidate
  acquires the next ready key, so a provider with three keys and a 429
  walks them in rotation. The request's own retry remains a recovery
  decision — a policy whose `429` row says `terminal` still marks the key,
  but does not retry. When an attempt finds no ready key, the wait before
  the re-ask is the pool's own LOCAL readiness floor (`CredentialReadyIn` on
  the observation): the engine raises every wait to at least that remainder,
  and the `retry-after` policy has no vote over it — `mode: ignore` discards
  what the UPSTREAM asked for, never what the pool knows. The floor is still
  bounded like every other wait: the candidate's retry window and the
  caller's deadline shorten it, and a wait that exhausts the retry budget
  ends the candidate through the ordinary `on-exhausted` path.
- **Pool identity is per provider, never content alone.** The rotation and
  cooldown state is keyed by the provider's pool identity — the
  providers-table name, the credential bytes, and the `rate-limit` policy
  together. Two providers whose auth blocks are byte-identical are two
  rotation domains: one account's 429 never cools the other's cursor. A
  reload that changes ONLY `rate-limit` values starts that provider's pool
  fresh (sizing changed with the policy), while an unchanged block — name,
  bytes, and rate-limit alike — keeps its rotation state warm; in-flight
  requests keep the pool they pinned. The identity digest hashes the values
  and never appears in a log.

Secrecy is the same rule as everywhere else: values live in the config
file, in memory, and on the outgoing wire — never in an error, a log line,
a reload event, a metric label, or a response body. And the client token
stays strictly separate: `Authorization` is not a forwarded header, so on a
credential-bearing provider the ONLY Authorization on the wire is the one
the pool composed.

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

**The envelope's elapsed bound reaches a buffered body through EOF, and stops
at the commitment.** A pre-commitment exchange — a dial whose response has not
been committed to the client — is bounded end to end: acquiring a stalled
header and reading a stalled **buffered** body are both inside the same window,
so a peer that answers `200` and then goes quiet cannot hold the walk open past
its envelope. Only a response that has actually committed hands the body off
that bound: a confirmed `text/event-stream` (decided by the **response's**
content type, never by the request's `stream` flag — a streaming request
answered with buffered JSON is still a pre-commitment body) and the verbatim
3xx/204/304 relays, both of which the client holds and which are bounded by the
caller's own context instead. A body cut by the envelope this way is this
proxy's own bound, never a peer fault: it is reported as the protocol body
timeout with `error_cause: exchange_elapsed` and `failure_origin: envelope`, and
a walk that finalizes on it answers `upstream_invalid_response` with reason
`upstream_body_timeout`.

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
`budget-candidate`, the closed-set `error_cause: exchange_budget`, the
disposition the refusal produced, and the walk's position (`candidate_index`,
`candidate_attempt`, `retry_index`, `provider_attempt`,
`request_exchange_budget_remaining`). It carries no `upstream_exchange`,
because no exchange happened for it to index.

A refusal by the REQUEST envelope is a different record, because it is a
different answer: no policy can buy an exchange the request envelope has
already refused, so the walk ends there rather than asking the retry
policy's `on-exhausted`. It has no WARN of its own — the post-walk ERROR
`upstream_request_failed` names it, with `policy_rule_id: budget-request`
and `failure_origin: envelope`.

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

This block governs only UPSTREAM directives. The local credential-cooldown
readiness (the pool's earliest ready key) is a separate floor the engine
applies to every wait regardless of `enabled`/`mode`/`max-delay` — see the
provider upstream credentials section.

### Stream recovery

```yaml
recovery:
  stream:
    enabled: false # default false; absent means streams are never continued
    max-recoveries: 1 # continuation hops per stream, 0..2
    max-elapsed: 20s # maximum silence from the upstream, >= 1ms, <= 2m
    max-partial-bytes: 262144 # committed text held to build a hop, 1KiB..1MiB
```

Everything above this section is pre-commitment: it decides which candidate
answers, and it stops forever at the first client-visible byte. This block is
the one thing that runs after that boundary, and it exists because "commitment
is final" and "a cut stream is the client's problem" are not the same
statement.

When a **committed** SSE stream ends without its terminal marker — `data:
[DONE]` for Chat Completions, `event: response.completed` for Responses — the
upstream generation was cut short. With this block enabled, the proxy keeps
the client's one connection open, re-asks the **same candidate** for a
continuation carrying the text the client has already received, and relays the
new events into the same response. The client sees one SSE connection and one
terminal marker.

**This is semantic continuation, not token-level resumption.** No
OpenAI-compatible provider exposes a resume token, so the seam cannot be
exact: the upstream is handed the committed text as an assistant turn and
asked to keep writing from the point it stopped. It may rephrase slightly at
the seam. That is a real, visible cost and the reason the block is off by
default.

**It is fail-closed, and it refuses more often than it acts.** A stream is
continued only when all of the following hold; anything else ends the stream
exactly as it would with the block absent, and `stream_recovery_exhausted`
records which gate fired:

| gate            | refuses when                                                                         | reported as                                      |
| --------------- | ------------------------------------------------------------------------------------ | ------------------------------------------------ |
| terminal marker | a marker was already forwarded — the stream is over                                  | no reason (it ended on its own terms)            |
| client          | the client connection is gone, or a relay cap stopped the pass                       | no reason (`client_disconnected` on the request) |
| generation over | the upstream declared the answer finished (`finish_reason`)                          | `logical_terminal`                               |
| tool calls      | any `delta.tool_calls`/`delta.function_call` was seen                                | `unsafe_content` / `tool_calls`                  |
| stream failed   | the upstream declared the stream failed (an error event)                             | `unsafe_content` / `upstream_terminal`           |
| refusal         | the upstream used the refusal channel (`response.refusal.delta` / non-empty `.done`) | `unsafe_content` / `refusal`                     |
| output topology | Responses text not provably from ONE message output's ONE stream                     | `unsafe_content` / `multiple_outputs`            |
| readable shape  | a `data:` line was neither `[DONE]` nor recognizable JSON                            | `unsafe_content` / `not_object`/`unknown_shape`  |
| prefix bound    | the committed text reached `max-partial-bytes`                                       | `unsafe_content` / `oversize`                    |
| usable prefix   | no text was committed at all, so a hop would be a blind replay                       | `unsafe_content` / `no_prefix`                   |
| request body    | the client's body cannot express a continuation (see below)                          | `unsafe_content` / the builder's token           |
| reach           | `max-recoveries` continuation requests were already made                             | `max_recoveries`                                 |
| window          | the upstream has been silent for `max-elapsed`                                       | `max_elapsed`                                    |
| envelope        | the request's `budget.request` envelope is spent                                     | `budget_spent`                                   |

A non-null `finish_reason` is **not** an unsafe-content refusal and is never
reported as one: the generation ended, it simply ended without the marker the
client keys on. Nothing is left to continue, and no marker is invented to paper
over the missing one. The distinction is what keeps `unsafe_content` readable —
it means "this proxy could not safely continue this stream", not "the model
stopped talking".

A **refusal** is the same distinction one layer down, and it is not assistant
text: `response.refusal.delta` (or a `response.refusal.done` carrying a
non-empty refusal) means the model declined to answer, so the bytes on that
channel are not part of what the client is reading as an answer and must never
enter the continuation prefix. It is reported as its own token rather than as
a generic `upstream_terminal` because the reason a hop is refused is
diagnostic — an operator paging on `unsafe_content` needs to tell "our
continuation correctness gate" from "the model said no".

**MVP scope: plain text only.** The feature continues plain assistant text with
provably safe structure and nothing else. Tool calls are refused, and so is any
Responses stream whose text cannot be attributed to exactly one message output
and one content stream: a delta whose `item_id`, `output_index` or
`content_index` disagrees with the text already accumulated from **that upstream
response** — or a second message item, or a second text stream — refuses the
whole stream rather than concatenating two answers into one assistant turn.

That attribution is a property of **one upstream response**, not of the logical
stream, because a hop is a new response: it announces its own output item, emits
its own deltas and its own `response.output_text.done`, and every real upstream
gives it fresh `item_id`s. So a hop's identity may differ from the committed
reply's and the prefix still joins; the rule above still bites unchanged within
any single response, where two outputs really would be two answers spliced into
one turn. What stays logical-stream-scoped is the prefix itself and the
refusals: a later hop never releases a refusal an earlier one latched.

The unbounded-accumulation case is the one that matters for memory: a stream
whose text passes `max-partial-bytes` stops being a candidate for
continuation, and the rest of it relays untouched.

**What a continuation is allowed to be.** The hop is the committed candidate
re-asked, on its own endpoint, its own transport, its own credential pool, with
the walk's sticky key preferred — not a new attempt that looks similar, and
never a different candidate. It pays for its dials out of the same
`budget.request` envelope the walk spent, because a continuation is real
outbound traffic. It counts as a provider-level attempt on
`request_completed`: `provider_attempts` and `upstream_exchanges` both move,
while `candidates_entered` does not.

**`max-recoveries` counts continuation requests, not continuations that
worked.** `max-recoveries: 1` means this logical stream may make one extra
upstream request — whether that request is established, refused at the
credential or the envelope, answered with a status, or truncated mid-stream.
The slot is spent before the dial, so a stream that keeps dying cannot spend an
unbounded number of upstream requests under a bound that reads as a limit; the
cap is `2`. A hop that could not be established at all (a refused dial, a
status, a non-stream 2xx) ends the effort on the spot rather than spending the
rest of the reach: another immediate ask would not change what those answers
said, and the client's stream is open and silent the whole time. The one
failure that continues is a hop that streamed and truncated again — evidence of
progress — and only while the reach, the window and the envelope still allow
it.

**`max-elapsed` is a hard upstream-idle bound, not a check between reads.**
It is one window shared by the committed relay and every hop, and the window
moves only when the proxy receives upstream bytes: each byte restarts a full
interval of tolerated silence. A healthy generation may therefore run for an
hour without being cut, while a peer that stops talking still cannot leave the
client waiting longer than one interval. The bound deliberately counts neither
SSE event boundaries nor client writes — a fragmented event is still upstream
progress, and a slow client or this proxy's own keep-alive ping says nothing
about whether the upstream is alive. Both waits a peer can leave this proxy in
remain bounded:

- **A stalled body.** A peer that sends a partial event and then holds the TCP
  connection open cannot outlive its silence window: when it expires the proxy
  closes the body the relay is blocked on, the read returns, no further hop is
  dialed, and the stream is reported `stream_truncated` with
  `recovery_reason: max_elapsed`.
- **A stalled response header.** A peer that accepts a continuation request
  and then answers nothing has produced no body to close, so nothing but the
  request's own context can unblock it — and for a client that is still
  reading, that context would live forever. The window carries its own cancel
  for exactly this case, so the dial returns after the remaining tolerated
  silence rather than parking the request until the client gives up. Header
  arrival is itself upstream activity: a hop that answers in time moves the
  same window forward and hands the watchdog to its new body, while one that
  answers after the deadline is dropped and reported `max_elapsed` rather than
  relayed.

A later hop inherits the same moving window; it does not get a fresh interval
just because the previous upstream response ended. In every case the hop's
failure is reported as `phase: max_elapsed`, which is this proxy's own bound
and never a peer's fault — see the event matrix in [Logging](#logging). A
client that disconnects is a different thing altogether and is reported as
`client_disconnected`: the client's own context remains the higher hard stop,
and it is never confused with the operator's window. A client disconnect also
means **zero** further upstream requests — the proxy never spends an exchange
on a stream nobody is reading.

> **Size the window for the longest upstream pause you accept, not for the
> longest generation.** The window is armed on the committed stream itself,
> but a generation that keeps producing bytes keeps it alive. A peer silent for
> `max-elapsed` is cut and reported `stream_truncated` with
> `recovery_reason: max_elapsed`, even if no upstream error was observed. The
> default is `20s` and the cap is `2m`; set it above the longest token gap you
> accept from the provider. Leave the block disabled to preserve the plain
> relay, which has no recovery window at all.

**What is never done.** No ordinary retry and no fallback, before or after
commitment; the pre-commitment walk is untouched and no decision here goes
through the policy matrix. No terminal marker is ever synthesized — a hop that
truncates leaves the stream unterminated, because that is more honest than a
`[DONE]` after a partial answer. No error body and no second header block ever
reaches a client already receiving a stream. No duplicate detection or
overlap trimming: a repeated paragraph at the seam is visible and honest, and
a heuristic that deletes client-visible text is not.

**The request body has to be able to say it.** The continuation is built from
the client's own original body, which is replayed through the candidate's
transform unchanged apart from the added turn:

- **Chat Completions** — if the last message is already an `assistant` message
  with string content (a prefill), the committed text extends that content;
  otherwise an assistant message carrying the committed text is appended.
  Either way the answer-so-far ends the body, which is what lets the model
  continue it. `stream: true` is set; every other member travels as sent. A
  prefill message is **mutated, not rebuilt**: only its `content` is
  regenerated, so a `name`, a provider extension, or a field this build has
  never heard of survives into the continuation rather than being silently
  dropped.
- **Responses** — `input` is normalized into the array form: the client's own
  string input is preserved as its leading user item, and the continuation adds
  an assistant `message` item carrying the committed text followed by a user
  item carrying a fixed instruction to resume without repeating. A body
  carrying `previous_response_id` is refused outright: the upstream would
  resolve that id against a stored response whose generation never finished.

The instruction text is fixed and not configurable. It is the whole difference
between "continue this answer" and "answer this again", and a deployment that
could edit it could ask for the duplication this feature forbids.

**Position rule.** Like `fallback`, `stream` is legal only at the **global**
and **model** layers; stating it on a `providers` entry or on a single chain
candidate rejects the file. A continuation is pinned to the candidate that
produced the committed stream, so a provider-layer statement would be a promise
about every model routed through it — including models whose clients parse the
answer strictly — that the walk's own candidate swap can silently invalidate.

**Interaction with the other blocks.** `sse-keep-alive` keeps running across a
hop, so a slow continuation still pings and the idle cut it exists to prevent
cannot fire mid-recovery. Usage metering stays factual and reports the request's
whole traffic: the logical request contributes one event, and because the client
received text from every call, `completion_tokens` is those calls' answers
added up while `prompt_tokens` is the **last** call that stated one — a hop
re-asks with everything the previous call had, so the final call's prompt is the
context that actually ran and summing prompts would count the same conversation
once per hop. `total_tokens` is restated as that row's own prompt + completion,
so the three columns cannot contradict each other, and a count no call stated
stays SQL `NULL`. Within one call the rule is unchanged: the last readable
object wins and chunks are never summed. The event's `provider_attempts` and
`egress_attempts` already count every hop's dial. A reload mid-stream cannot
turn recovery on for a stream that started without it, nor reshape the bounds a
stream in flight is recovering under.

**When the block is absent, nothing changes.** An SSE stream that dies without
its terminal marker still logs `stream_completed` at DEBUG with outcome
`completed`, exactly as it always has. With the block present, a stream that
never reached its marker is reported as `stream_truncated` whether the proxy
dialed for it or refused to — that is the one fact a client cannot recover on
its own.

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
  Post-commitment [stream recovery](#stream-recovery) does not weaken this:
  it continues the same candidate on the same connection under a separate,
  off-by-default policy, and the walk is over before it starts.
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
real outbound exchanges — and the two are independent in BOTH directions,
because one provider attempt that dials three members is three exchanges,
while an attempt whose pool dials nothing at all is one attempt and zero
exchanges.

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
`upstream_exchange` — there was no exchange for it to index. That record
always names `budget-candidate`: a refusal by the REQUEST envelope is
terminal whatever the policy says, so it ends the walk and is reported by
the post-walk `upstream_request_failed` under `budget-request`. Details under
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

## Response field stripping

Some providers decorate every 2xx response with fields that are metadata, not
answer — the Kilo providers' `provider` and `service_tier` members are the
motivating case. `strip-fields` lets an operator configure the proxy to excise
those members before the client sees them, on both the buffered and streamed
relay paths, with no decode/encode round trip: an excised member's own bytes
(key, colon, value, and the comma that separates it) are removed and every
other byte — key order, whitespace, exotic non-JSON framing included —
survives.

**Where the list lives.** Both `providers.<name>.strip-fields` and
`models.<name>.strip-fields` accept it. A model that states a list replaces
its provider's list for every candidate in its chain; a model without one
inherits each candidate's own provider list per hop, so a chain whose
providers strip different fields answers under whichever provider served the
request. Absent or null = no stripping; responses stay byte-identical to an
unconfigured deployment. An empty list (`[]`) is a rejection — a block that
states nothing reads like a typo. The hot-reload rules apply exactly as to
the rest of the runtime file: a request binds its strip list to its [config
snapshot](#hot-reload), so a reload mid-stream cannot change what an
in-flight request strips.

**The path syntax.** Each entry is a dotted JSON path over object keys:
`parent.child` descends through objects. A segment containing a dot, space,
or quote is wrapped in single quotes and decoded with JSON unquoting rules
— `'parent name'.child`, `'a.b'.c`, `'a''b'.c` — so any key, whatever its
bytes, is addressable. The traversal is **object-only**: a path never
descends through an array, so `choices.provider` matches nothing (a classic
provider field inside a choice object survives; that is the documented scope,
mirroring the model and usage rewriters who never enter arrays). Duplicate
keys in a payload are all excised — one left standing is one leak. A scan
never re-serializes; malformed input is forwarded byte-for-byte.

**Scope follows the API**, exactly like the model and usage rewriters, so the
same list works across both surfaces:

- Chat Completions: the top-level object. A nested `response` object in a
  chat payload is client data and is reached only through the paths
  themselves.
- Responses: the top-level object AND the object directly inside a top-level
  `response` envelope (where `response.completed` events carry their
  provider-added fields), with the full path list reapplied inside the
  descent — the single-descent rule.

**The reserved paths.** The members the proxy itself writes into a relayed
response are rejected in a strip list — but as an enumerated set of **exact
paths**, not as forbidden segment names. That distinction is the whole point:
`usage` is reserved, while `usage.is_byok`, `usage.cost` and
`usage.cost_details.upstream_inference_cost` are not, so a provider's
billing metadata inside the usage object stays reachable.

| Reserved path                                      | Written by                                 |
| -------------------------------------------------- | ------------------------------------------ |
| `model`, `response.model`                          | the model rename                           |
| `usage`, `response.usage`                          | the usage object the synthesis writes into |
| `usage.completion_tokens_details.reasoning_tokens` | Chat thinking-usage synthesis              |
| `usage.output_tokens_details.reasoning_tokens`     | Responses thinking-usage synthesis         |

Each of those four members is reserved in **two spellings**, and both are
needed. The Responses strip descends once into a top-level `response` object
and reapplies the whole list there, so a bare `model` reaches the envelope's
renamed model; and because the scan walks object keys, `response.model`
reaches that same member from the top level with no descent involved at all.
One list is interpreted under whichever API a request arrives on, so the rule
is the union of the two — a path that is harmless on one surface can reach
proxy-owned bytes on the other. `response` is reserved at one level only:
the descent happens exactly once, so `response.response.model` is accepted.

Listing a synthesis leaf's _parent_ (`usage.completion_tokens_details`) is
allowed, and does what it says: it excises the synthesized count along with
the provider's own `audio_tokens` / `image_tokens` siblings. The direct leaf
is reserved so that outcome is never reached by accident, and the blast
radius stays bounded by `thinking-usage` being opt-in per model.

The strip runs **last** in the composed rewriter (rename → synthesis →
strip), precisely so those owned members are untouchable by configuration.
The usage **meter** is deliberately not part of this argument: it reads
pre-rewrite bytes, so excising anything under `usage` cannot change what is
attributed.

**One path class to avoid, and only with stream recovery on.** The stream
recovery accumulator also reads pre-rewrite bytes: it accumulates the text the
upstream sent, while the client is shown what survived the strip. A strip path
aimed at the streamed text itself (`delta.content`, `response.output_text`,
their like) therefore makes the continuation body disagree with what the client
read — the seam shows a divergence instead of a repetition. Nothing about it is
unsafe and the proxy does not refuse it, because excising a member is a
deliberate operator choice; if a continuation block and a strip list are
configured on the same model, keep the strip list off the text members. The
meter is unaffected either way, and with stream recovery off the two never
interact at all.

**Streaming parity.** The same composed rewriter serves both relay paths.
The SSE data-line gate, which admits a line only when its payload carries
the two keys the rewriter owns (`"model"`/`"usage"`), is widened with the
strip list's first-segment byte patterns: a chunk carrying only a
to-be-excised field still reaches the strip and comes out excised, while
lines mentioning none of the gate keys pass through untouched — the
unconfigured deployment runs the gate as before, byte for byte.

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
  an event, and never appends after the terminal event. One exception: with
  post-commitment [stream recovery](#stream-recovery) enabled, a relay pass
  that ends without its terminal marker may be followed by another hop on
  the same connection, so the heartbeat is not stopped at that EOF — it
  keeps the client connection alive across the gap and stops for good when
  the effort ends. It starts only once
  the upstream response headers are committed to the client: silence while
  waiting for upstream headers is **not** covered, and the wait is not bounded
  by this proxy at all. There is no response-header timeout on the direct
  transport, and no overall request timeout, by design — a provider that queues
  a request behind its own load legitimately takes minutes to send a status
  line, and no configuration states a time-to-first-header budget. A peer that
  accepts the connection and then answers nothing therefore holds the request
  until the caller's own context ends (its deadline, its disconnect, or
  process shutdown). A continuation hop IS bounded: it runs under the
  recovery window's remaining upstream-silence allowance, so the same peer cannot park a hop either.
  Why: Cloudflare silently cuts a client HTTP/2 stream after ~125s with
  zero bytes from origin (measured on 2026-09-22 — client
  `stream error … INTERNAL_ERROR` at 125.39s, origin-side close at
  125.06s), which long reasoning phases exceed; a `: ping` every 15s kept a
  240s silent stream alive through the same path.
- A committed stream that ends **without** its terminal marker is a stream
  the upstream cut short. By default that is the end of it — the client's
  partial answer is all it gets. With [`recovery.stream`](#stream-recovery)
  enabled the proxy continues it on the same connection, on the same
  candidate, under its own bounds; see that section for what is refused.
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
- Lines end at the earliest of `\n`, `\r\n` or `\r` — all three terminators
  the SSE grammar admits, whichever comes first in the byte stream.
  Framing is a function of the byte stream alone: the same upstream bytes
  parse to the same events however the transport split its reads, and no
  event boundary is ever derived from a read boundary. A line is relayed the
  moment its terminator arrives, so a bare CR is not held back waiting for
  the byte that might have followed it.
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

## Buffering

Two buffers on the request path are proportional to what a client or an
upstream sends: the client's **request body**, read once and replayed through
the candidate's transform on every attempt, and a buffered (**non-streaming**)
**upstream answer**. Both are capped at 64 MiB per request. A per-request cap
bounds one request; three things bound the process:

| bound                  | scope       | value                              | what it stops                                                        |
| ---------------------- | ----------- | ---------------------------------- | -------------------------------------------------------------------- |
| request body size      | one request | 64 MiB                             | one client sending an oversized body                                 |
| request body read time | one request | 5 minutes                          | one client sending a body slowly enough to pin the goroutine forever |
| buffering budget       | the process | 256 MiB outstanding, reserved live | N concurrent requests each holding a cap's worth at once             |

The third one is the gap a per-request cap cannot close. `N` concurrent
requests may each be under the cap and together exceed what the container
has; the walk's retries and the recovery loop's re-asks multiply `N` by
whatever the policy does rather than by anything an operator sets. So the
bytes are admitted against one process-wide counter before they are held, and
released when the buffer is done with — a body when the request ends, an
answer when it is relayed or discarded — on every path out, panics included.

Three properties make the refusal readable rather than mysterious:

- **Immediate, never queued.** A reservation that cannot be met is refused on
  the spot; nothing waits for room. A bounded wait would park exactly the
  resource the budget protects — the goroutine, the connection, and the bytes
  already sent — and would make the moment of refusal depend on unrelated
  traffic. Same input, same answer, regardless of what else is in flight.
- **Reserved as it grows, not up front.** A buffer reserves in blocks as it
  fills (64 KiB, doubling to 1 MiB), because a cap is what a request _may_
  hold and almost no request holds it. Reserving 64 MiB to read a 2 KiB
  prompt would convert a memory ceiling into a concurrency ceiling nobody
  configured. An ordinary request costs one small block; a request that
  really does grow towards the cap pays for it block by block.
- **Local, and outside the recovery policy.** The refusal is answered with the
  503 envelope above at both call sites. It is never a failed attempt: no
  provider is at fault, no exchange is spent, no retry or fallback can clear
  a process-wide condition, and no observation is fed to the recovery matrix.

The accounting is a bound on the bytes this process buffers, not a byte-exact
map of the heap: Go's slice growth may round a block's allocation up, and the
last block is reserved whole however little of it the input uses.

**An admitted buffer is not the only copy of itself, and the budget is sized
for that.** The transform that injects the prompt decodes the request body and
marshals it into a second buffer of the same size, live for the attempt beside
the body the replay needs; the composed response rewriter builds a same-size
copy of the answer before it is written. Neither copy is admitted separately —
each is bounded by its admitted source.

**The live bytes are the smaller half of the problem; the garbage collector is
the larger one.** These copies are _live_, and the budget is a live-bytes
bound. What reaches the cgroup is resident set size, which also counts memory
the GC has not returned to the OS. Go's default `GOGC=100` lets the heap
roughly double between collections, so a 256 MiB live set arrives as a
~512 MiB heap goal and an RSS peak near it, with a floor of free-but-unreleased
pages underneath.

Measured on this repository at 16 MiB request bodies against a local upstream
(64-bit linux, Go 1.26), peak RSS:

| Concurrency       | Budget in use            | `GOGC` default | `GOMEMLIMIT=700MiB` |
| ----------------- | ------------------------ | -------------- | ------------------- |
| 8                 | 128 MiB                  | 586 MiB        | 580 MiB             |
| 16                | 256 MiB                  | 990 MiB        | 703 MiB             |
| 32                | 512 MiB                  | —              | 703 MiB             |
| 20 + 3 s upstream | budget exhausted, 5× 503 | 914 MiB        | 714 MiB             |

The last row is the shape that matters: the budget _is_ enforced (five
`buffer_capacity_exceeded` refusals), and the peak still sat at 914 MiB — over
the `mem_limit: 1g` this repository's compose file set — with no
misconfiguration anywhere. `GOMEMLIMIT` moved the same workload to 714 MiB and
held it flat as concurrency rose, because it clamps the GC goal instead of
letting it float with the live set.

**So `mem_limit` must be sized against RSS, not against the budget, and
`GOMEMLIMIT` is what makes RSS track the budget's own arithmetic.** The budget
is a package constant (256 MiB), not runtime configuration. `GOMEMLIMIT` is a
_soft_ limit: the GC works harder to stay under it, but memory in use at the
moment of the measurement is not taken back, so a run that genuinely needs
more than its limit still exceeds it. Size `mem_limit` above `GOMEMLIMIT` with
headroom for the overshoot, TLS state, connection buffers, and in-flight
response bytes — lowering `mem_limit` below what a run can actually reach is
how a clean 503 turns into an OOM kill. Raising `mem_limit` without a
`GOMEMLIMIT` buys nothing: the GC goal still floats with the live set.

## Errors

Upstream and client failures are classified, never fogged:

| Condition                                                                                                                             | Status                 | `error.type` / `code`                                                                                                                                                                           |
| ------------------------------------------------------------------------------------------------------------------------------------- | ---------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Missing/malformed `Authorization: Bearer <key>` on any `/v1` route (including `GET /v1/models`)                                       | 401                    | `invalid_request_error` — exact body: `{"error":{"message":"you must provide an API key in the Authorization header (Bearer <key>)","type":"invalid_request_error","param":null,"code":null}}`  |
| Wrong bearer key (partner mode: unknown **or revoked** key, or an unavailable key store — fail closed)                                | 401                    | `invalid_request_error` / `invalid_api_key` — exact body: `{"error":{"message":"invalid API key","type":"invalid_request_error","param":null,"code":"invalid_api_key"}}`                        |
| Non-GET `GET /v1/models` route request                                                                                                | 405                    | `invalid_request_error` — exact body: `{"error":{"message":"method not allowed","type":"invalid_request_error","param":null,"code":null}}`; this precedes its auth check                        |
| Body is not JSON                                                                                                                      | 400                    | `invalid_request_error` — exact body: `{"error":{"message":"invalid JSON in request body","type":"invalid_request_error","param":null,"code":null}}`                                            |
| Missing `model`                                                                                                                       | 400                    | `invalid_request_error` — exact body: `{"error":{"message":"you must provide a model parameter","type":"invalid_request_error","param":null,"code":null}}`                                      |
| Request body over the 64 MiB cap                                                                                                      | 413                    | `invalid_request_error` — exact body: `{"error":{"message":"request body too large","type":"invalid_request_error","param":null,"code":null}}`                                                  |
| A buffer this request is filling cannot be funded from the process-wide buffering budget                                              | 503                    | `server_error` / `capacity_exceeded` — exact body: `{"error":{"message":"server is out of buffering capacity","type":"server_error","param":null,"code":"capacity_exceeded"}}`                  |
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
- **The 503 capacity refusal is local, deterministic, and never a walk
  outcome.** Two buffers on the request path draw from one process-wide
  ceiling: the client's request body, and a buffered (non-streaming) upstream
  answer. When either cannot be funded, the request is refused on the spot
  with the envelope above and outcome `capacity_exceeded`, and a WARN
  `buffer_capacity_exceeded` names the phase (`request_body` or
  `upstream_response`). It is deliberately not `502 upstream_unreachable`:
  nothing was dialed that failed, no provider is at fault, and the refusal is
  the same fact for every candidate — so it never enters the recovery matrix,
  never retries, never falls back, and never spends an exchange. A retry would
  walk a chain that can only end the same way. See
  [Buffering](#buffering) for the budget itself.
- There is **no overall request timeout**. A slow upstream is a slow
  response, not a timeout race. Dial and TLS handshake timeouts bound the
  connection phase only.

**Relayed response headers** (an allow-list, everything else is dropped):
`Content-Type`, `Cache-Control`, `Retry-After`, `Location`, `X-RateLimit-Limit`,
`X-RateLimit-Remaining`, `X-RateLimit-Reset`, `X-RateLimit-Reset-Requests`,
`X-RateLimit-Reset-Tokens`. Rate-limit and retry headers are load-bearing for
client backoff; dropping them would make a 429 indistinguishable from any other
upstream failure.

The list carries **no request-id header**, on any spelling. `X-Request-Id` is
the proxy's own — see [Request identity](#request-identity) — and
`OpenAI-Request-Id` is dropped outright. An upstream's id is never relayed, so
one hop presents one id and it is always the one this process minted.

## Request identity

Every request gets **one** id, minted by this process: 16 lowercase hex
characters, cryptographically random, never derived from anything the client
sent. The same value appears on all four surfaces of a request, so any one of
them resolves to the others:

- the `X-Request-Id` **response header** the client receives, on every answer —
  success, normalized upstream error, local rejection, 405, 404;
- the `request_id` field on every **log event** the request emits;
- the `X-Request-Id` **request header forwarded upstream**, on every attempt and
  every fallback candidate;
- the `request_id` column of the **usage record**, in metering mode.

So a support ticket quoting an `X-Request-Id` resolves to a log line, and an
upstream's log for the same id resolves to this proxy's.

**The client's own id is never adopted.** A request carrying `X-Request-Id` has
that header dropped like any other unlisted client header; the upstream sees
the proxy's value instead. The same is true of an upstream's id in the other
direction — `X-Request-Id` and `OpenAI-Request-Id` are not on the relay
allow-list, so a provider's value never reaches a client and never competes
with the proxy's on one name. The id is not backoff input and never is, which
is why overwriting the far end's value loses nothing load-bearing.

**Where it is stamped:** every `/v1` route and the catch-all 404. `/healthz`
and `/readyz` are exempt — they are container probes fired on an interval, they
bind no request lifecycle and log nothing, so an id on them would be a header
joining to no evidence. The catch-all 404 is the one path that carries an id
with no log line: a stable handle the client can quote, not a join key.

A stream-recovery hop forwards the **same** id as the attempt it continues; a
continuation is the same client request re-asked, so the upstream sees one value
across the original and every hop. A config reload mid-request cannot reshape
any of this: the id is minted once, before the snapshot is even loaded, and
lives exactly as long as the request.

**There is no configuration key for any of it.** The header name is a
documented internal constant (`requestIDHeader`), like the body and buffering
caps. The feature is on for every deployment; there is no way to turn it off
and no knob to rename the header. The rationale and the alternatives that were
rejected are in
[`docs/design/request-identity.md`](docs/design/request-identity.md).

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
a fallback walk; `egress_attempts` counts the real outbound HTTP exchanges
the walk spent, so the two are independent — an attempt that fanned out
across a pool's members is fewer attempts than exchanges, one whose pool
dialed nothing is more), the final candidate's
egress kind (`direct`, or the last dialed pool
member's kind; empty when a pool exhausted without dialing anything), and full
request latency. Two naming notes for anyone querying the table: the
`egress_attempts` column holds the request-wide exchange total described
above (the log record calls that quantity `upstream_exchanges`, and reserves
its own `egress_attempts` field for the relayed candidate's pool report), and
there is no `upstream_exchanges` column — a query says `egress_attempts`
where the completion line says `upstream_exchanges`. Provider fallback and
same-candidate retries still emit
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
SQL `NULL`, not zero. Within one upstream call, the last readable API-scoped
usage object wins; chunks are never summed. A request assembled from more than
one upstream call — a stream-recovery hop — reports the aggregate of those
calls instead, with each count combined by its own semantics: the last call
that stated a `prompt` wins (the context that actually ran), the calls'
`completion`s add up (the client read all of them), and `total` is that row's
own prompt + completion. One wrongly typed count makes that member unstored (a
stringified `"128"` or integral `1e3` still reads) — it never discards the
members that did decode. Consequently, synthesized `reasoning_tokens` are never
stored as provider usage. This layer intentionally has no pricing, currency,
invoicing, or quota enforcement.

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
- **The config-file path is operator input and is never a log field.**
  `OAICR_CONFIG_FILE` may carry URL-style userinfo or query material, so
  `config_file_read_failed` and `config_load_failed` are the boot events and
  `config_file_unreadable` / `config_file_recovered` are the poller's; none
  names the path, and the read-failure event carries no error either, because
  `os.ReadFile`'s is an `os.PathError` that embeds it. A rejected-reload event
  does keep its error: that text quotes position, length or line, never the
  operator's input.
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

- Every event a request emits carries the same `request_id`: the value this
  process minted, returned to the client as the `X-Request-Id` response header
  and forwarded upstream on every attempt. See
  [Request identity](#request-identity).

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
  stops advancing), and `stream_completed` (carrying `stream_recoveries`,
  and emitted only for a stream that either reached its terminal marker or
  ran with post-commitment recovery off).
  `stream_envelope_unreleased` is the one lifecycle event that reports a
  bookkeeping miss: it fires at a commitment point when the answer carried no
  releasable pre-commitment envelope — the expected case for an answer that
  never went through the transport's windowed dial, so it is DEBUG and never
  an error — and its absence would leave a committed stream that still
  truncates at the candidate's `max-elapsed` unexplained.
  Plus the poller's per-tick
  debug heartbeat while a config failure persists (the healthy unchanged
  state logs nothing at all). Detailed but never payload-bearing: request
  bodies, SSE `data:` payloads, and injection prompts do not exist at this
  level — or at any level.
- **INFO** — one `request_completed` per proxied request with the wire
  facts: `request_id` (16 hex chars, generated per request — the same value
  the client received and the upstream saw), `api`
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
  `upstream_exchanges` counts real outbound exchanges — the two are
  independent, differing in either direction (a pooled attempt that fans out
  is several exchanges, one that dials nothing is none). The event is emitted
  when the request finishes, under the
  level in effect at that moment — a reload mid-request can therefore
  change whether it appears. Also `config_reloaded` (`generation`,
  `model_count`, `log_level`), `config_file_recovered` (a file returned
  byte-identical after a failure; no fields beyond the envelope), `service_started` (boot config
  accepted; the listener itself is announced by the DEBUG
  `listener_ready`), and `drain_started`. Post-commitment recovery, when the
  block is enabled, adds `stream_recovery_started` (one per continuation hop,
  emitted before its dial: `recovery_index`, `partial_bytes`, `provider`,
  `upstream`, `policy_hash`, `policy_generation`) and
  `stream_recovery_succeeded` (the hop's events carried the stream to its
  marker: `recovered_bytes`, `recovered_events`, `upstream_exchanges`,
  `elapsed_ms`).
- **WARN** — client disconnects and truncations (`stream_truncated` with a
  `phase` field separating `client_write` from `upstream_read`,
  `upstream_limit` and `recovery` — the last for a stream that ran with
  recovery enabled and ended without its terminal marker — plus
  `stream_recoveries` and, when a gate stopped the effort, `recovery_reason`;
  and `relay_copy_failed` with phase `client_write` on
  the verbatim and buffered paths — the buffered case covers a client whose
  cancel or expired deadline surfaces through the upstream body read, which
  is read from the request context rather than the error chain, with no
  envelope written
  to the connection that is already gone), a response that never landed because the client was
  already gone — a buffered body or any locally generated error envelope
  (`client_write_failed`, outcome `client_disconnected`, superseding the
  envelope's own classification), an upstream that
  died mid-body before the answer could be parsed
  (`upstream_body_read_failed`, outcome `upstream_read_failed` — except when
  this proxy's own exchange envelope cut the read, which is
  `error_cause: exchange_elapsed` over `failure_origin: envelope` and
  finalizes as `upstream_invalid_response`, because a bound this proxy set
  is not a peer fault), a client
  that goes away mid-request — a disconnect or an expired deadline,
  including while the upstream request is in flight
  (`upstream_request_failed` with `error_class` `canceled` and
  `error_cause` `caller_canceled` or `caller_deadline_exceeded`, outcome
  `client_disconnected`, and no error envelope, since the client is
  gone), and an upstream 4xx — the `upstream_http_error` evidence event
  described below — a candidate whose exchange envelope refused another dial
  (`candidate_exchange_budget_spent`, `error_class` `provider_exhausted` over
  `error_cause` `exchange_budget`: a refusal, not a failed endpoint, so no
  endpoint is blamed and no `egress_attempt_failed` accompanies it), and the
  two post-commitment recovery events that mean "the stream did not make it":
  `stream_recovery_failed` (one per hop that refused, answered with a status
  instead of a stream, or truncated again — `recovery_index`, `provider`,
  `upstream`, `phase` from the closed set `build`/`credential`/`budget`/
  `dial`/`max_elapsed`/`upstream_status`/`upstream_read`/`upstream_limit`/
  `client_write`, `upstream_status` when the hop answered with one,
  `unsafe_reason` when the refusal was a body this proxy declined to build
  (`phase: build`), and `upstream_credential_id` when the candidate has a
  pool) and
  `stream_recovery_exhausted` (the effort stopped at a gate —
  `recoveries`, `reason` from `budget_spent`/`max_recoveries`/`max_elapsed`/
  `logical_terminal`/`unsafe_content`, and `unsafe_reason` naming the
  accumulator's own token when the gate was `unsafe_content`; a
  `logical_terminal` reason carries no `unsafe_reason`, because a generation
  the upstream declared finished is not an unsafe stream) — one
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

**Who owns a stopped recovery.** Five events describe the post-commitment
recovery loop, and an operator reads exactly one of them as the answer to
"who ended this effort":

| event                       | owner              | it means                                                                                                                                                                     |
| --------------------------- | ------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `stream_recovery_started`   | this proxy         | a hop was built and dialed — an intent, emitted before its first dial                                                                                                        |
| `stream_recovery_succeeded` | upstream           | the hop's events carried the stream to its terminal marker                                                                                                                   |
| `stream_recovery_failed`    | the hop            | one hop that did not produce a continuable stream; `phase` names what went wrong                                                                                             |
| `stream_recovery_exhausted` | this proxy         | the effort stopped at a bound; `reason` names the bound it stopped at                                                                                                        |
| `stream_truncated`          | whoever stopped it | the committed stream reached the client without its terminal marker; `phase` names the side that stopped it, and `client_disconnected` outcomes are not the provider's fault |

Three readings are wrong and the field names exist to prevent them:

- **`phase: max_elapsed` never blames a peer.** It is this proxy's own window
  closing, whether it cut a header wait or closed a parked body. The error the
  cancel produced belongs to nothing outside this process and is deliberately
  not attached, so a hop cut by our own deadline carries no `error` and no
  `upstream_status` — an operator must never read it as "upstream failed". It
  names no `upstream` either, because the window is checked before the hop
  builds its URL: a refusal that never dialed has no endpoint to name, and the
  field is omitted rather than invented.
- **`client_write` and `upstream_limit` are this proxy's own work too** (a
  client that left, a relay cap reached), and their errors are this package's
  own typed values — so they are logged as they are, not through the no-echo
  sanitizer, which would replace them with the static "upstream transport
  error" and blame a peer for a stop we caused. Only `dial`,
  `upstream_status` and `upstream_read` point at a peer, and only those go
  through the sanitizer. A client that leaves DURING a hop is one of them:
  the hop reads the request's own context (the hop runs under a context
  derived from it, so a cancellation arriving through the transport cannot be
  told apart by shape) and reports `client_write`, and the request's outcome
  becomes `client_disconnected` rather than a truncated stream the provider
  caused. The failed hop is still recorded — the cause just has one owner,
  and it is not the provider's.
- **`unsafe_content` on `stream_recovery_exhausted` with no `unsafe_reason`
  never happens**, and `unsafe_reason` never appears on `logical_terminal`. The
  two are different facts: one is a stream this proxy must not continue, the
  other is a generation the upstream declared finished.
- **One field name, one vocabulary.** `phase` is the hop's, `reason` is the
  loop's, `unsafe_reason` is the accumulator's. The three closed sets share no
  token today, and they are kept in separate fields precisely so that a future
  overlap stays legible instead of silently meaning two things at once. A body
  the continuation builder refuses is reported as `phase: build` with
  `unsafe_reason` set to the builder's own token — the same field name the
  accumulator uses for the same kind of fact — and the loop's `reason` is
  `unsafe_content`.

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
attempts, the second the real outbound exchanges — independent axes that
differ in either direction, since a pooled attempt may fan out across several
members or reach none at all), `policy_rule_id` (the rule or code-owned invariant
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
`provider_attempt_failed`), `error_cause` (on the unusable-answer events, a
closed set: `body_read_failed` (the upstream died mid-body),
`capture_deadline_exceeded` (this proxy's own 5 s error-capture timer fired on
a body that never finished) and `exchange_elapsed` (the exchange envelope cut
the read) — the last two are proxy-owned bounds, never peer faults, and both
ride `failure_origin: protocol`/`envelope` respectively), `failure_origin`
(`upstream_http` | `transport` |
`protocol` | `caller` | `credential` | `envelope` — the layer the failure
belongs to; `credential` is a candidate whose rotation pool had no usable key,
which is a local refusal rather than a wire failure) and
`elapsed_ms`. Transport failures additionally carry `send_state`
(`definitely_not_sent` | `send_unknown`): whether this dialed attempt
provably never carried a request byte. It is evidence about a **dialed**
attempt, so a pool that skipped every member reports none — that line says
`egress_exhausted`, which is the whole truth about it. The evidence and
body-read/invalid events carry
the received HTTP status as `upstream_status` — a transport failure has no
status to carry — so failures correlate by
`request_id + candidate_index + candidate_attempt + egress_attempt`.

Three naming notes, so a dashboard is not built on the wrong reading.

**The counters carry compatibility aliases, and the new names are
authoritative.** `candidate_attempts` is also emitted as
`provider_attempts`, `retry_attempts` as `retries_total`, and the per-dial
`egress_attempt` as `attempt` — each pair is one quantity under two names,
kept so a dashboard built against an earlier release keeps reading the
number it was built on. Every pair is incremented at a single site, so the
alias can never disagree with its authoritative name; where a table below
names one of them, it names the pair once rather than restating it.

`provider_attempt_started` replaced the former `upstream_request_started`
(the slug now names what it announces), and it is emitted **before** the
dial, so it announces an attempt whose outcome is not yet known — but the
index it carries is the logical attempt counter's own, already incremented,
not a projection of one. An attempt the exchange envelope refuses before
any dial therefore still reports `provider_attempt = provider_attempts = N`,
because it IS the Nth logical attempt; only `upstream_exchanges` stays
where it was, because that counts real dials and none happened. The two
counters are independent axes and neither is derived from the other.

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

Two probes, and they answer two different questions. They are separate
endpoints because they must be able to disagree:

| endpoint       | question                     | answer                                                                                            |
| -------------- | ---------------------------- | ------------------------------------------------------------------------------------------------- |
| `GET /healthz` | is this process alive?       | `200` + `ok\n` for as long as the listener exists, **including while draining**                   |
| `GET /readyz`  | should traffic be sent here? | `200` + `ok\n` while serving; `503` + the state token (`starting`/`draining`/`stopped`) otherwise |

Both are unauthenticated, exact-match (`/readyz/` is the catch-all 404, like
`/v1/models/`), and both answer `405` for a non-GET. `/readyz` is
deliberately **not** part of the OpenAI-compatible surface, so it answers a
plain-text 405 rather than the proxy's JSON envelope. Readiness responses are
sent `Cache-Control: no-store`: a cached `200` replayed after a drain began
would route traffic into a socket about to close, which is the failure the
endpoint exists to prevent.

Readiness is a statement about **this process alone**. It is not derived from
the config snapshot, from a provider, or from either database: a provider
outage is no reason to stop routing to an instance that can still serve the
models it has left, and a database blip must not empty a load balancer's
whole pool. The healthcheck subcommand's rule is unchanged — it reads no YAML
and makes no upstream or database call.

The `healthcheck` subcommand (used by the Docker image and compose) probes
**readiness** and requires `200` + `ok\n`:

```sh
./openai-compatible-injector healthcheck    # OAICR_LISTEN env decides what is probed
```

It reads only the `OAICR_LISTEN` environment variable (a wildcard address is
rewritten to the loopback) and **never reads the YAML file** — a poisoned
reload must not fail the container probe. It reads `/readyz` rather than
`/healthz` because a scheduler needs the readiness fact: a draining instance
answers `503` there while `/healthz` keeps answering `200`, so a rolling
restart stops routing to a process that is stopping correctly instead of
mistaking a clean drain for a broken process. The shipped cadence is
`interval=2s timeout=2s retries=1` with a long `start_period` (the service may
spend up to 30s migrating the partner-key store and another 30s on usage
metering before the listener exists), and the drain's readiness head start is
sized against that cadence — see below.

## Graceful shutdown

On SIGTERM or SIGINT the service takes a readiness head start, then stops
accepting and drains:

1. **Readiness goes false first, while the listener is still accepting.**
   `/readyz` starts answering `503 draining` at this instant; `/healthz` and
   the API keep serving. The head start is 5s, capped at half the drain
   budget — drawn from `grace` rather than added to it, so the time from
   signal to exit is unchanged and a container's `stop_grace_period` needs no
   adjustment. Without it, "no longer ready" and "no longer accepting" happen
   at the same instant, and every probe already scheduled, every in-flight
   check, and every load-balancer view one interval out of date lands on a
   closed port. A process signalled during startup was never advertised as
   ready at all.
2. `http.Server.Shutdown(grace - head start)` — in-flight requests and
   streams get the remainder of `OAICR_SHUTDOWN_GRACE` (default 55s) to
   complete.
3. If the drain budget runs out, `Close()` force-terminates the remainder.
4. Idle egress connections are closed — the listener is already gone, and the
   upstream pools are what is left to release.
5. Then the defers run, and they are **outside** the drain budget: if usage
   metering is enabled, its accepted event backlog is flushed through a 10s
   close window with a 5s join grace behind it (events that cannot be
   persisted are explicitly counted and reported, and `usage_meter_final`
   logs the totals); in partner mode the key store closes within its own 5s.
   The process then exits `0`.

A second signal while draining forces an immediate `exit 1` — defers do not
run, so the metering accounting above is that path's deliberate casualty.
Compose's `stop_grace_period` is deliberately larger than the drain budget,
by the defers in step 5: 55s of default grace plus 15s of metering drain plus
5s of key-store close is 75s worst case, which is why the shipped files use
`75s` and not `60s`. Docker's SIGKILL must not land on that accounting. The
head start is inside the drain budget rather than in front of it.

The lifecycle transitions are logged: DEBUG `readiness_ready`, INFO
`readiness_unready` (with `grace` and `propagation`), INFO `drain_started`
(with `drain`), and WARN `drain_deadline_exceeded` when the drain had to
force-close.

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

`compose.production.yaml` is the hardened variant, and its settings are
**coupled to the binary's own limits** — changing one without the other is the
mistake to avoid:

- `mem_limit: 1g` is sized against the process's 256 MiB buffering budget
  (see [Buffering](#buffering)): twice the budget for the derived copies an
  admitted buffer leaves behind, plus the `GOMEMLIMIT` the same service sets to
  stop the GC goal floating with the live set, plus headroom for TLS state and
  the response bytes of in-flight requests. The budget is a constant in the
  binary, so raising `mem_limit` alone buys nothing; lowering it below what a
  run can actually reach converts a clean `503 capacity_exceeded` refusal into
  an OOM kill.
- `GOMEMLIMIT: 768MiB` is set on the service, and is what makes `mem_limit`
  meaningful: without it the GC goal tracks the live set and the process
  reached 914 MiB of RSS with the budget fully enforced and no
  misconfiguration — past the 1g ceiling this file sets. It is a soft limit, so
  `mem_limit` must stay above it.
- The `healthcheck` reads `/readyz` and never `/healthz` — see
  [Healthcheck](#healthcheck).
- `read_only: true`, `cap_drop: ALL`, `no-new-privileges` and a `tmpfs` for
  `/tmp` are safe because the image is `scratch` with one binary: it reads
  its config, its CA bundle, and nothing else. The health probe is the
  binary's own subcommand, so no shell is needed.

The rollout it documents is a two-phase drain, and both phases depend on the
readiness ordering above: start the new container and wait for it to report
ready, then SIGTERM the old one and wait for the drain. An orchestrator's
`stop_grace_period` covers readiness head start and drain together, since the
head start is drawn from the grace rather than added to it.

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
- Connection hygiene is bounded in time and in aggregate, not only per
  request: request bodies are capped at 64 MiB **and** must arrive within 5
  minutes, request headers must arrive within 10s, idle keep-alive
  connections are closed after 120s, SSE relay input is capped per line and
  per event (see [Streaming](#streaming)), and the bytes every in-flight
  request is buffering are admitted against one process-wide ceiling, so
  concurrent requests cannot add up to an unbounded process (see
  [Buffering](#buffering)). A quiet client cannot pin a goroutine and a file
  descriptor forever, a slow client cannot pin one by dribbling its body, and
  a hostile upstream cannot pin unbounded memory. An active response
  (including a long SSE stream) is never touched by the idle timeout — the
  request-body deadline is armed for the body read and cleared the moment it
  returns, so it cannot reach a stream or the next request on a pooled
  connection, and no overall write deadline exists to cut a legitimate SSE
  mid-flight.

## Out of scope

Decided, and not coming back without a design discussion:

- **More OpenAI model-serving surfaces** — embeddings, batch, assistants,
  etc. `GET /v1/models` is the deliberate exception: a local discovery
  endpoint backed by the mapping, not an upstream protocol surface.
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
  egress selection, and caller-driven retry knobs (the recovery policy is
  resolved from YAML, per request — an operator cannot let a client ask for
  a retry). An egress pool still never retries a response after commitment;
  post-commitment [stream recovery](#stream-recovery) is not an egress retry
  and runs above the pool, replacing neither.
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
