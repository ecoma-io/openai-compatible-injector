# Design note — transparent SSE stream recovery (issue #76, PR #77)

What this branch added, why it is a second orchestrator rather than a branch of
the walk, what it refuses to do, and where the code deliberately disagrees with
the plan it was written from.

## The problem it solves

A committed SSE stream that dies mid-generation is, today, exactly what it
looks like: `CopySSE` maps `io.EOF` to a nil error, so a client that received
four paragraphs of a six-paragraph answer gets four paragraphs and a closed
connection, with no signal that anything was cut. The proxy logs
`stream_completed`. The client cannot tell the difference between "the model
finished" and "the upstream died", because the only in-band signal for that —
the terminal marker — never arrived.

Two facts made that the right behavior to ship and the wrong one to keep:

- the marker is the client's ONLY way to know a stream finished, and a stream
  without one is genuinely unterminated;
- the generation that produced the partial answer is reproducible in kind: the
  same provider, asked again with the text so far as an assistant turn, will
  usually continue rather than restart.

So the branch adds an opt-in continuation, and leaves the default path
byte-identical.

## Two orchestrators, one boundary

```text
                    ┌─────────────────────────────────────────┐
  client ── one ───▶│  WALK (unchanged)                       │
   SSE             │  decide WHICH candidate answers          │
  connection       │  matrix → retry | fallback | terminal    │
                    │  stops at the first client-visible byte  │
                    └──────────────────┬──────────────────────┘
                                       │ COMMITMENT
                    ┌──────────────────▼──────────────────────┐
                    │  CONTINUATION (new, opt-in, off)         │
                    │  same candidate re-asked                 │
                    │  same connection, same marker            │
                    │  its own fail-closed gates               │
                    └─────────────────────────────────────────┘
```

The walk is untouched: same engine, same matrix, same commitment rule. The new
loop starts only after a candidate has answered and its headers have reached
the client, and it never re-enters `recovery.Engine` — the engine's commitment
invariant is code-owned and non-configurable, and it would correctly refuse
every observation about a response the client already holds. Routing a
continuation through `Observe` would have required weakening that invariant,
which is the one thing this feature must not do.

What the two share is the request's own `recovery.Budget`. A continuation is
real outbound traffic, and the envelope exists to bound traffic rather than the
intent to make it, so a hop pays for its dials out of the same budget the walk
spent. Nothing is starved: the walk is already over.

## What the hop is

`continuationHop` (`internal/proxy/streamrecovery.go`) carries only facts about
the committed candidate, captured at branch entry:

| fact                 | why it is the committed one and not a fresh one                                                                                              |
| -------------------- | -------------------------------------------------------------------------------------------------------------------------------------------- |
| `cand`               | the candidate whose answer produced the committed bytes                                                                                      |
| `transform`, `model` | the candidate view the walk used — the hop differs from the original attempt in the conversation body and nothing else                       |
| `suffix`, endpoint   | the route the walk appended; `RawPath` is cleared on both paths so `EscapedPath` cannot percent-decode the endpoint behind the caller's back |
| `pool`               | the `*credential.Pool` INSTANCE the walk resolved, not a fresh registry lookup                                                               |
| `credKey`            | the key the walk went out with — sticky, so a continuation re-asks with the credential that produced the prefix                              |
| `budget`             | the request's envelope                                                                                                                       |

The pool instance is the load-bearing one. `h.doers.Doer(...)` and the
credential registry are both content-keyed and retained on publish, but a
concurrent publish can evict an identity and rebuild it cold between the walk's
dial and the hop's. Carrying the instance means the hop acquires from the same
rotation domain, with the same cooldown state, that the walk just used. The key
the hop actually went out with (`dial.credKey`) becomes the NEXT hop's
preference, so rotation state moves forward with the flow instead of being
re-derived.

A hop that finds every key cooling stops right there: the client is holding an
open stream, and a pool cooldown exists to stop a provider being hammered, not
to be waited out inside a response.

## The gates, in the order they run

Every one is a refusal, never a clamp. Each is checked before the hop it would
permit.

| gate                   | refuses when                                                                                                       | stop reason                                                       |
| ---------------------- | ------------------------------------------------------------------------------------------------------------------ | ----------------------------------------------------------------- |
| `continuationEligible` | a terminal marker was forwarded; the client write failed; the client is gone; a bounded-relay cap stopped the pass | none (not a recovery failure — the stream ended on its own terms) |
| reach                  | `recoveries >= max-recoveries`                                                                                     | `max_recoveries`                                                  |
| caller                 | `r.Context().Err() != nil`                                                                                         | none (the client is gone; `client_disconnected` reports it)       |
| safety                 | `partial.Safe()` returns a reason                                                                                  | `unsafe_content` + `unsafe_reason`                                |
| window                 | `now − streamStart > max-elapsed`                                                                                  | `max_elapsed`                                                     |
| envelope               | `budget.Exhausted() != ExhaustionNone`                                                                             | `budget_spent`                                                    |
| body                   | the builder refuses                                                                                                | `unsafe_content` + the refusal token                              |

The safety gate is the fail-closed half. `partialText`
(`internal/proxy/continuation.go`) is fed every `data:` line the relay admits,
in hop order, through `CopySSE`'s `observe` seam — which sits BEFORE the
client-facing rewrite gate, because the accumulator has to see the lines the
gate would have skipped; the usage meter still reads inside the rewrite wrapper
it has always used, since it wants only the lines the rewriter touched. One
accumulator spans the whole logical stream, so hop 2's deltas extend hop 1's
prefix. It refuses, permanently, on:

| token               | trigger                                                    |
| ------------------- | ---------------------------------------------------------- |
| `tool_calls`        | any `delta.tool_calls` / `delta.function_call` present     |
| `finish_reason`     | any non-null `finish_reason` on a primary choice           |
| `upstream_terminal` | an upstream-declared end/failure event in the stream       |
| `not_object`        | a `data:` line that is neither `[DONE]` nor parseable JSON |
| `unknown_shape`     | a parseable line with an unrecognized `type`/shape         |
| `oversize`          | accumulation reached `max-partial-bytes`                   |
| `no_prefix`         | the stream ended with no text at all                       |

`tool_calls` is the hard case the brief called out, and refusing is the whole
answer to it: a continuation of a stream that emitted a tool call would
re-enter the model mid-tool-call, and the duplicate-call risk is exactly what
this feature must not create. `no_prefix` is the other one — a hop with no
committed text is a blind replay wearing a continuation's shape, which is the
behavior the brief forbade.

## The compatibility hinge

```go
if err != nil || (contPolicy.Enabled && !stats.Terminal) {
    // outcome = "stream_truncated", WARN stream_truncated
} else {
    // outcome = "completed", DEBUG stream_completed
}
```

- **Feature off** (absent, null, or `enabled: false` — the zero `StreamPolicy`):
  the second clause is false, so an EOF with no marker still reports
  `completed`, exactly as it has since the first release. The loop body never
  runs, `observe` is nil, and `CopySSE` with a nil observer is byte-identical
  to the relay that shipped before.
- **Feature on**: a stream that never reached its marker is reported truncated
  whether the proxy dialed for it or refused to. That is deliberate and it is
  the honest direction: with the block on, the operator has said they care
  about unterminated streams, and a client cannot recover that fact on its own.

`e2e/recovery_test.go` reads both halves side by side:
`TestE2ERecoveryCommittedSSENeverRetried` (unchanged — the same upstream, the
same `panic(http.ErrAbortHandler)`, the same one-event-then-truncation, with
the block absent) and `TestE2EStreamRecoveryContinuesACutStream` (the block
present, two dials, two events, one `[DONE]`, one connection). Neither passes
by accident if the other's behavior changes.

## What is never done

- **No ordinary retry, and no fallback, before or after commitment.** The
  pre-commitment walk is untouched and no continuation decision goes through
  the matrix. The hop is pinned to the committed candidate; `candidates_entered`
  does not move.
- **No synthesized terminal marker.** A hop that truncates leaves the stream
  unterminated. A `[DONE]` after a partial answer would make a lost tail
  indistinguishable from a finished one, which is strictly worse than the
  truncation it replaced. Chat's hop carries its own `[DONE]`; Responses' hop
  carries its own `event: response.completed`; the proxy invents neither.
- **No second header block and no error body.** A hop that answers with a
  status instead of a stream is reported as evidence and dropped — the client's
  `200` and its `text/event-stream` are long since written, and an upstream
  error object spliced into an SSE stream is not something a client can parse.
- **No overlap detection.** A duplicate paragraph at the seam is visible and
  honest; a heuristic that deletes client-visible text is not, and "data loss
  is worse than duplication" is the rule the brief set.
- **No metrics.** The repo has no metrics subsystem; none was added.

## Observability

| event                       | level | fields                                                                                                                                                                                                            |
| --------------------------- | ----- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `stream_recovery_started`   | INFO  | `recovery_index`, `partial_bytes`, `provider`, `upstream`, `policy_hash`, `policy_generation`                                                                                                                     |
| `stream_recovery_succeeded` | INFO  | `recovery_index`, `recovered_bytes`, `recovered_events`, `upstream_exchanges`, `elapsed_ms`                                                                                                                       |
| `stream_recovery_failed`    | WARN  | `recovery_index`, `phase` (`build`/`credential`/`budget`/`dial`/`upstream_status`/`upstream_read`/`client_write`), `upstream_status`, the sanitized `err`, `upstream_credential_id` when the candidate has a pool |
| `stream_recovery_exhausted` | WARN  | `recoveries`, `reason`, `unsafe_reason`                                                                                                                                                                           |
| `stream_completed`          | DEBUG | + `stream_recoveries`                                                                                                                                                                                             |
| `stream_truncated`          | WARN  | + `stream_recoveries`, `recovery_reason`, phase `recovery` for a marker-less stream                                                                                                                               |
| `egress_attempt_failed`     | WARN  | one per endpoint a hop's pool dialed and lost                                                                                                                                                                     |

Every reason is a closed-set token from a typed value, never error text — the
same rule `transport.go` states for `AttemptFailure`. No event carries a
credential, a body, a continuation prefix, or the continuation instruction; the
hop's own message name is never logged, because it is client-visible text this
proxy already relayed.

The hop's `stream_recovery_failed` is deliberately NOT the walk's
`upstream_http_error`: that record carries fields (fingerprint, error shape,
capture outcome) a hop's answer never had, and emitting a thin one under the
same slug would read as a walk event that lost its fields. The hop's status
rides `upstream_status` on its own event instead.

## Deviations from the plan this was written from

Stated rather than smoothed over:

1. **`CopySSE` gained an explicit `observe` parameter** rather than the plan's
   "no `CopySSE` change". The plan proposed riding the existing rewrite
   closure, which the usage meter is composed into — but that closure only
   sees lines that passed the client-facing gate, and the accumulator must see
   every admitted `data:` line, including the ones carrying no `model`/`usage`
   key the gate looks for. A second seam was the smaller change. It is
   nil-able, and nil is the unconfigured path: the relay stays byte-identical
   for a deployment that never enables the block.
2. **A seventh safety token, `upstream_terminal`.** The plan listed six. An
   upstream that declares the stream failed is a stream whose remaining text is
   not the answer the client asked for.
3. **`partial_bytes`, not `partial_chars`.** The accumulator bounds bytes; the
   log reports what it bounds.
4. **A new truncation `phase` token, `recovery`.** The plan's phase set was
   `client_write`/`upstream_read`/`upstream_limit`. A stream that ran with
   recovery enabled and ended without its marker is a fourth situation and
   needed a name that does not claim an upstream read failure.
5. **No `upstream_http_error` for a hop's non-2xx** (see above).
6. **The `max-recoveries: 0` with `enabled: true` rejection** is enforced in
   `Validate` rather than at parse time, so every layer benefits from it.

## Reviewed and deliberately not changed

- **The walk still replays a `send_unknown` transport failure at the provider
  layer, by policy.** Unrelated to this feature and unchanged by it.
- **`fallback.enabled: false` still pins the walk; it says nothing about
  `stream`.** They are independent blocks because they are independent
  questions: how far the walk reaches vs. whether a committed stream is
  continued.
- **Tool-call continuation in any form.** Refused, permanently, by the safety
  gate. A provider that streams a tool call and dies mid-call is a hard case
  with no safe answer this proxy can give.
- **Token-level resumption.** No OpenAI-compatible provider exposes a resume
  token over this surface, so the seam cannot be exact. The README says so
  plainly, and it is why the block is off by default.

## Test surface

- `internal/recovery` — `StreamPolicy` validation (over-cap, `enabled: false`
  with a nonzero reach, `enabled: true` with a zero reach), merge inheritance
  and the `enabled: false` co-rule, hash sensitivity per field.
- `internal/config` — every field parses at the global and model layers; the
  block is rejected on a provider entry and on a candidate; absent ⇒ disabled;
  a frozen pre-change `config.example.yaml` still loads and resolves to the
  zero policy.
- `internal/proxy/sse_test.go` — `StreamStats.Terminal` true for `data: [DONE]`
  and `event: response.completed`, false for a plain data line; the existing
  tests and the benchmark compile unchanged.
- `internal/proxy/continuation_test.go` — the safety gate per closed token;
  prefix accumulation across chunks, and that the first refusal is the one that
  sticks; the oversize bound. The accumulation ACROSS hops is exercised by the
  loop tests in `streamrecovery_test.go`.
- `internal/inject/continuation_test.go` — both builders, the prefill extension
  and the appended-turn shape, every refusal token, and that neither mutates
  its input.
- `internal/proxy/streamrecovery_test.go` — the loop end to end over both API
  surfaces: off-by-default byte-identity, a continued stream, a refusal at each
  gate, a spent budget, a spent reach, a client that leaves, an unsafe stream,
  an oversize prefix, a truncated line, a hop failure, and a stream that
  already terminated.
- `e2e/recovery_test.go` — the real binary, one connection, two hops, one
  marker, and the log evidence.

## Rollout

Disabled by default. Enable with `recovery.stream.enabled: true` and
`max-recoveries: 1` on a canary model first — a model whose clients tolerate a
visible seam. The signal to watch is the ratio of `stream_recovery_succeeded`
to `stream_recovery_exhausted`, and among the exhausted ones, `unsafe_content`.
On agent traffic `unsafe_content` should be the most common skip, and that is
the feature working, not failing: a tool-call stream is one this proxy must not
continuously re-enter.
