# Design note — Per-provider credential pool, rotation, and 429-aware cooldown (issue #70)

Working design for provider upstream API-key authentication. Statuses marked
**open** are to be finalized against the branch's architecture audit; nothing
below is landed behavior until the matching commit lands.

## Goal

Configured providers can require an upstream API key. An operator owning
several keys for one provider must not have one account's rate limit stall the
whole provider: keys rotate, a 429'd key cools down, the next attempt carries
a different ready key — and none of that creates a second retry authority.

## Ownership (the invariant everything else serves)

```text
Recovery Engine     = the ONLY retry / fallback / terminal authority
Credential Pool     = which credential the next attempt carries
Transport Pool      = which egress that attempt dials
```

The credential layer never performs HTTP, never sleeps, never retries a
request, never increments a recovery counter, and never becomes a provider
candidate. The transport layer never learns a key exists. Selection happens
strictly above the transport seam: the credential header is part of the
outgoing request handed to the `Executor`/`Doer`, so egress fallback inside
one attempt (E1 → E2) keeps the same key by construction.

## Config surface (providers `auth` block)

```yaml
providers:
  kilo:
    base-url: "https://..."
    transport: direct-egress
    auth:
      type: api_key # phase 1: the only type
      header: Authorization # RFC 7230 field-name; also X-API-Key etc.
      prefix: "Bearer " # arbitrary prefix, "" allowed
      strategy: round_robin # phase 1: the only strategy
      keys:
        - id: kilo-1 # operator-chosen, bounded safe charset
          value: "<secret>"
```

- Absent `auth` → request bytes unchanged (unconfigured deployments keep
  byte-identical traffic).
- Malformed blocks reject the whole file, matching the no-echo convention:
  position/line only, never the secret. Key ids are bounded to a safe charset
  (no whitespace, no control characters, bounded length) so an id can never
  become a log-injection vector.
- The client's inbound `Authorization` stays consumed-at-the-proxy; the
  upstream credential header is applied AFTER forwarded headers are built, so
  a client cannot overwrite the provider credential.

## Selection semantics

Per-request, per-candidate:

- **Initial attempt**: round-robin over ready keys.
- **Retry of the same candidate** (a recovery `retry` decision): prefer the
  previously used key. A key not marked rate-limited is handed back — so
  500/408/425/5xx/protocol/body retries and retryable transport rows keep
  their key. A marked key is skipped in favor of the next ready one — so a
  429's re-ask naturally lands on a different account with NO changes to the
  engine, the matrix, the budgets, or the counters.
- **401/403 keep their current matrix meaning** (default: fallback).
  Key-disable-on-401 is a different feature with its own recovery contract.

## 429 cooldown

- The key that received a 429 is marked rate-limited for a bounded cooldown:
  the honored `Retry-After` where the policy honors one, else a configurable
  default compatible with the existing backoff scale. Marking happens on
  every path that observes the 429 — including a failed body capture and a
  downstream cancellation after the answer — because the mark describes the
  UPSTREAM's state, not the request's.
- The engine still decides `retry` exactly as today; the delay is still the
  policy's. Rotation is an emergent property of acquisition, not a decision.
- **No ready key** (all cooling): the pool reports state, never waits. The
  recovery layer owns what happens next through a typed decision seam —
  **open**: exact seam shape (new observation cause reusing the directive/cap
  machinery vs. on-exhausted semantics) is being finalized against the
  engine audit. Hard requirements either way: no fake exchange consumption,
  no fake attempt counters, no dial without a credential, no busy loop, no
  unbounded sleep; candidate windows and caller deadlines bind exactly as
  they do for backoff waits.

## Snapshot / hot reload

The pool is snapshot-bound like every other config plane. The registry is
content-keyed in the style of the transport `Registry`: unchanged credential
config across a reload keeps its rotation/cooldown state warm; changed
config starts a fresh pool instance, and an in-flight request's pinned pool
outlives any retirement. Reloads never reset or reshape another request's
key state.

## Telemetry and secrets

- Upstream key identity travels as a sanitized, bounded
  `upstream_credential_id` on the completion record (and per-attempt events
  where an acquired key exists). It is NOT the inbound partner `key_id`,
  which keeps its semantics untouched.
- Log-side only in this pass — no usage DB migration.
- The key VALUE appears nowhere: not in validation errors, startup/reload
  logs, provider/recovery/evidence logs, usage events, metric labels, panic
  messages, or error envelopes. The e2e secret sweep is extended to run the
  real binary against a config carrying the new secrets.

## Testing contract (summary)

- Credential package: one key / many keys / round-robin / preferred /
  preferred-in-cooldown / invalid config; 429 marks, rotation, cooldown
  expiry, Retry-After caps, all-cooldown, concurrent acquire+mark, reload
  isolation; non-429 retention (500/502/408/425/transport/protocol);
  401/403 do not rotate.
- Integration: K1→429→K2→200; K1→500→K1→200; K1→K2→K3 under an explicit
  retry budget; all-cooldown stays bounded; Retry-After on K1 leaves K2
  usable; body-capture failure still marks; cancel-after-429 does not retry;
  SSE commit ends rotation; egress switch keeps key; client Authorization
  cannot override the provider credential; partner `key_id` ≠
  `upstream_credential_id`.
- Invariants: no HTTP in the credential package; no self-retry; exchange/
  attempt/retry counters unchanged by rotation; one candidate stays one
  candidate; egress switch never rotates; no post-commit recovery; reload
  isolation; secret absence; 429 rotation adds no budget.
- Regression: the full existing suite must pass unmodified; any test whose
  expectation changes must be explained by named behavior, never by editing
  the assertion to make it pass.

Adversarial sequences to confirm by test after implementation:
429→retry→429→retry→fallback; 429→transport failure→retry; 429→cooldown→
reload; 429→all keys unavailable→deadline; 500→500→429→200; K1 429→K2 429→
K1 cooldown expires→retry; SSE commit→upstream error; client disconnect
while a recovery decision is pending; concurrent requests all 429ing.
