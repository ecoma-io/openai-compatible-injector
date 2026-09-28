# Design note — transparent SSE stream recovery (issue #76, PR #77)

What this branch added, why it is a second orchestrator rather than a branch of
the walk, what it refuses to do, and where the code deliberately disagrees with
the plan it was written from. The final section records the hardening pass that
followed the merge review and the defects it fixed; the behavior described
above it is the behavior after that pass.

## Scope, stated exactly

**This is semantic continuation, not model-state resume.** No OpenAI-compatible
provider exposes a resume token over the Chat Completions or Responses surface,
so there is no way to hand the upstream the generation it was running. The hop
instead hands the model the text the client already has as an assistant turn
and asks it to keep writing. The two are not the same thing: the seam is a new
generation, and it may rephrase slightly there. Everything below follows from
that limit, including the fact that the feature is off by default.

**The MVP only recovers plain text streams with provably safe structure. Tool
calls and ambiguous Responses output topology are fail-closed.** A stream this
proxy cannot describe as "one plain text answer" is truncated exactly as it is
with the block absent; it is never continued on a guess.

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
| window (watchdog)      | the relay was cut by the window's own body close (armed only while the block is enabled)                           | `max_elapsed`                                                     |
| caller                 | `r.Context().Err() != nil`                                                                                         | none (the client is gone; `client_disconnected` reports it)       |
| logical terminal       | `verdict.Kind == verdictTerminal` — the upstream declared the answer finished, without forwarding the marker       | `logical_terminal`                                                |
| safety                 | `verdict.Kind == verdictUnsafe`                                                                                    | `unsafe_content` + `unsafe_reason`                                |
| window (clock)         | `now − streamStart > max-elapsed`                                                                                  | `max_elapsed`                                                     |
| envelope               | `budget.Exhausted() != ExhaustionNone`                                                                             | `budget_spent`                                                    |
| body                   | the builder refuses                                                                                                | `unsafe_content` + the refusal token                              |

The watchdog gate comes before the caller gate deliberately: it is the more
specific fact — the operator's window ended the pass — and a reader that also
happened to leave is a second event this loop is not the recorder of.

The safety gate is the fail-closed half. `partialText`
(`internal/proxy/continuation.go`) is fed every `data:` line the relay admits,
in hop order, through `CopySSE`'s `observe` seam — which sits BEFORE the
client-facing rewrite gate, because the accumulator has to see the lines the
gate would have skipped; the usage meter still reads inside the rewrite wrapper
it has always used, since it wants only the lines the rewriter touched. One
accumulator spans the whole logical stream, so hop 2's deltas extend hop 1's
prefix — and its latches span the logical stream too, so a refusal a hop
latched is never released by the hop that follows it. What does NOT span the
logical stream is the Responses identity, which is scoped to one upstream
response; see "the identity is per upstream response" below. It refuses,
permanently, on:

| token               | trigger                                                     |
| ------------------- | ----------------------------------------------------------- |
| `tool_calls`        | any `delta.tool_calls` / `delta.function_call` present      |
| `upstream_terminal` | an upstream-declared end/failure event in the stream        |
| `multiple_outputs`  | Responses text not provably one output's one content stream |
| `not_object`        | a `data:` line that is neither `[DONE]` nor parseable JSON  |
| `unknown_shape`     | a parseable line with an unrecognized `type`/shape          |
| `oversize`          | accumulation reached `max-partial-bytes`                    |
| `no_prefix`         | the stream ended with no text at all                        |

`tool_calls` is the hard case the brief called out, and refusing is the whole
answer to it: a continuation of a stream that emitted a tool call would
re-enter the model mid-tool-call, and the duplicate-call risk is exactly what
this feature must not create. `no_prefix` is the other one — a hop with no
committed text is a blind replay wearing a continuation's shape, which is the
behavior the brief forbade.

### Terminal is not unsafe

A non-null Chat `finish_reason` means the generation ENDED. The stream is not
recoverable — there is nothing left to continue — but it is not unsafe, and
the two must not be conflated:

- a `finish_reason` latches `verdictTerminal`, never a refusal;
- the loop stops with `logical_terminal`, and emits no `unsafe_reason` at all,
  because no gate refused the content;
- the client's stream is left exactly as it is — no `[DONE]` is synthesized to
  paper over the missing marker, and no `upstream_terminal`/`unsafe_content`
  token appears in the record.

`upstream_terminal` remains its own refusal for a DIFFERENT fact: an upstream
that declared the stream FAILED (a chat `error` event, a Responses
`response.failed`/`response.incomplete`/`response.error`). That one is unsafe in
the sense that matters here — its remaining text is not the answer the client
asked for. `finish_reason` is a plain end; a failure event is not, and an
operator reading `unsafe_content` should never have to guess which one they got.

The distinction is pinned on both the unit and the wire:
`TestPartialTextFinishReasonIsTerminal` and
`TestStreamRecoveryFinishReasonIsNotUnsafeContent` (which also asserts the
client's body carries no `[DONE]`), plus the `logical_terminal` row of
`TestE2EStreamRecoveryRefusalMatrix`.

### Responses identity is one stream or nothing

Chat's text path needs no identity: a delta carries `content` and that is the
whole story. Responses does, because the wire states identity in two places that
must agree, and the MVP supports continuing exactly one of them:

- `response.output_item.added` announces a message output with its own `id` and
  the event's `output_index`;
- every `response.output_text.delta` / `response.refusal.delta` carries
  `item_id`, `output_index` and `content_index`.

The accumulator binds an item identity the first time it sees one — from either
place — and refuses `multiple_outputs` when:

- a message announcement arrives while an item identity is already bound
  (a second message item), or its `id`/`output_index` disagrees with the bound
  one;
- a delta's `item_id`, `output_index` or `content_index` disagrees with the
  bound values;
- a second content stream or channel switch appears at the same indices;
- the item is not a message at all (a tool-call or unknown item type).

Two things are worth being precise about. First, a message announcement WITHOUT
an `id`, or without a numeric `output_index`, is `unknown_shape` rather than
silently accepted: a stream whose identity cannot be read is a stream whose
text cannot be attributed. Second, the announcement is cross-checked against
the deltas — an announcement that disagrees with the identity the text was
already accumulated under refuses the stream, rather than being treated as a
harmless header. The alternative is splicing two answers into one assistant
turn, which hands the model a conversation that never happened and is strictly
worse than the truncation it replaces.

#### The identity is per upstream response

Every rule above is scoped to ONE upstream response, not to the logical stream.
The accumulator holds that scope in `passStart`, the offset in `text` where the
current response's own accumulation begins, and in the `beginUpstreamStream`
reset the relay calls at the top of every pass — `relay` is invoked once per
upstream HTTP response relayed into the client's one stream, so the committed
pass and each hop each get exactly one.

Three facts force it. A hop is a NEW response: it is a fresh `response.created`
carrying a fresh `id`, and the `response.output_item.added` it announces is that
response's own item. Every real upstream emits a fresh `item_id` for it. And it
emits its own `response.output_text.done`, whose `text` member states the text
of the response that sent it — which is why both branches of the `.done` check
read `passText()` and not the cross-hop prefix. A hop that emits only a `.done`
has a non-empty cross-hop prefix, so testing the wrong region refuses a
correct event `unknown_shape` for being correct.

What the reset does NOT touch is everything the client already holds: `text`
(the continuation body is built from the whole prefix), `unsafe` and `terminal`.
A hop's clean deltas cannot un-refuse a stream that carried a tool call in the
committed pass, and a response that arrives after the generation ended is not a
continuation — it is bytes relaying into a stream this proxy has already
declared over.

The measurements are deliberately unlike the identity. `Verdict().Text` and
`partialBytes()` stay cross-hop: the memory bound must bound the whole prefix,
and the continuation request needs all of it. Only the PROVENANCE is per
response.

Two consequences worth stating, because both are load-bearing in the tests.
Within a single response, every refusal above still bites unchanged — a second
message item, a disagreeing `output_index`, a `content_index` switch are all
still `multiple_outputs`, and they are pinned single-pass by
`TestPartialTextResponsesIdentityIsOneStream` and
`TestE2EStreamRecoveryResponsesIdentityFailsClosed`. And a hop that reaches its
own terminal marker ends the loop on the marker, BEFORE the accumulator is
consulted — so a black-box test of this boundary must cut the hop it wants to
observe, not let it finish. `TestStreamRecoveryTakesItsSecondResponsesHop` and
`TestE2EStreamRecoveryResponsesHopIdentity` do exactly that.

#### The meter crosses the same boundary

`usage.Capture` is the other object that describes one upstream response while
living for the whole request, and the relay closure is the same place it is
segmented: each pass calls `Capture.Seal`, which folds the call that just
finished into a running aggregate and clears the live observation. Without it,
one request's event would report only the last hop's `usage` object while the
same row's `provider_attempts` and `egress_attempts` counted every hop's dial.

The counts are combined by what they measure, not by a blanket sum.
`completion_tokens` is the answer the client read, and every call produced its
own slice of it, so those add up. `prompt_tokens` is the context, and a
continuation re-asks with everything the previous call had, so the last call
that stated one describes the context that actually ran — summing prompts would
count the same conversation once per hop. `total_tokens` is restated as that
row's own prompt + completion so the three columns cannot contradict each
other, and a count no call stated stays `NULL` rather than becoming a zero.
Within one call nothing changes: adoption is still last-wins over the final
usage object, chunks are never summed, which is why a request that never takes
a hop reports exactly what it always did.

## The two bounds, stated exactly

### `max-elapsed` is a hard runtime bound

`max-elapsed` is not a check the loop makes between reads — it is a bound on
the request's wall clock that a stalled peer cannot outlive. One instant is
computed when the committed stream begins relaying (`windowEnd`), and every
pass — the committed relay and each hop's relay — is armed against that SAME
instant through `armRecoveryWindow`. Two mechanisms read it:

- a `time.AfterFunc` watchdog closes the upstream response body at the
  deadline. `Body.Read` takes no context, so a peer that sends a partial event
  and then holds the TCP connection open parks the relay inside a read; closing
  the body is the only lever that unblocks it. When the watchdog fires it sets
  `windowClosed`, and the loop refuses every later gate with
  `max_elapsed` — no hop is dialed, and the pass's own error is suppressed,
  because that error is this proxy's own close and not something the upstream
  committed.
- the frozen clock (`now − streamStart > max-elapsed`) is checked between
  passes, so an effort that ends cleanly just past the deadline stops too, and
  `windowClosed` stays false there: the pass ended by itself, so its error — if
  any — is the upstream's and is reported as such.

Four properties make the watchdog safe, and each is deliberate:

1. It is a plain timer, NOT a `context.AfterFunc` on the request context. A
   client disconnect must stay distinguishable from the operator's window, and
   a watchdog hung off the context could not tell them apart — the outcome
   would be `max_elapsed` for a reader that simply left.
2. It creates no goroutine: `time.AfterFunc` runs on the runtime's timer
   goroutine, and the relay's deferred `stop()` releases the timer on every
   path, including the one where the pass ended long before the deadline.
3. It closes the UPSTREAM body, never the client's connection. The client's
   connection belongs to the request lifecycle and is closed by the server, not
   by a recovery bound.
4. The window is one window, not a per-hop budget: the reach cannot extend the
   wall-clock bound by taking another hop.

The distinction the record has to keep is the whole reason for the flag, and it
is asserted at the wire level: `max_elapsed` must never be reported as
`client_disconnected`, `upstream_read`, or `upstream_limit`. The client's own
context remains the higher hard stop — cancellation really unblocks the read,
and it happens without the watchdog's help — and the gates are ordered so the
window answers first when both are true at once.

Two consequences follow from the window being armed on the committed relay and
not only on hops, and both are deliberate:

- While the block is enabled, `max-elapsed` also caps how long any single
  streamed answer may take. A generation still running at the deadline is cut
  and reported `max_elapsed` even though no upstream ever failed. The
  alternative — a window that only the recovery effort observes — is a bound a
  stalled peer outlives by never dying, which is the case the bound exists for.
- With the block DISABLED there is no window at all, which is the state the
  compatibility hinge requires. The resolved policy carries a nonzero
  `max-elapsed` even when `enabled: false` (it is the value an enabling layer
  inherits — see `defaults.go`), so the proxy may not decide to arm the
  watchdog from the window's value alone: the gate is `contPolicy.Enabled`, and
  the disabled path keeps the plain relay with no timer, no deadline and no
  window state. `TestStreamRecoveryWindowIsNotArmedWhenDisabled` pins that, and
  `TestRecoveryStreamBlockParsesEveryFieldAndDefaultsOff` pins the data-side
  fact that makes it necessary.

`TestStreamRecoveryMaxElapsedCutsABlockedRead` and
`TestE2EStreamRecoveryMaxElapsedCutsAStalledUpstream` prove the hard half on a
reader that ignores its context and on a real socket respectively;
`TestE2EStreamRecoveryEnabledWindowBoundsTheCommittedRelay` proves the window
bounds a healthy generation too; `TestStreamRecoveryClientCancelBeatsTheWindow`
and `TestE2EStreamRecoverySpendsNothingForADepartedClient` prove the other
direction.

### `max-recoveries` counts continuation requests

`max-recoveries: N` means: this logical stream may INITIATE N continuation
requests. It never means "N continuations that succeeded". The slot is claimed
immediately before the dial (`recoveries++`), so a hop that is refused at the
credential, refused by the envelope, answered with a status, or truncated
mid-stream has spent one — counting successes would let a stream that keeps
dying spend an unbounded number of upstream requests under a bound that reads
as a limit. The cap is `2`, and `enabled: true` with no explicit reach means
`1`.

What a failed hop does next is a POLICY, not a bound, and it is stated here
rather than left to be inferred from the code:

- a hop that never produced a continuable stream — a refused dial, a status, a
  non-stream 2xx — ends the effort on the spot. Another immediate ask would not
  change what that answer said, and the client's stream is open and silent the
  whole time.
- a hop that streamed and truncated AGAIN is evidence of progress, and the loop
  re-evaluates: the reach, the window and the envelope decide whether the
  stream is worth continuing once more. If the reach was the next thing to
  fire, the report carries `recovery_reason: max_recoveries` alongside the
  hop's own `stream_recovery_failed`.

Neither branch can exceed `max-recoveries` requests, and neither is a retry
policy: there is no backoff, no `Retry-After`, and no second ask of a hop that
failed to establish itself.

## What the continuation body may touch

The hop's body is built from the client's own original body by
`BuildContinuationChat` / `BuildContinuationResponses`
(`internal/inject/continuation.go`), which is the fail-closed half of the
feature: a body the builder cannot read is a body it must not guess at. Its
contract for the Chat prefill case is worth stating exactly, because the
tempting implementation is the wrong one:

- the last message, when it is an assistant message with string content, is
  **mutated, not rebuilt**: only its `content` member is regenerated, as the
  original prefill plus the committed prefix.
- every other member of that message — a `name`, a provider extension, a field
  this build has never heard of — is carried through untouched, so a
  continuation cannot silently drop provider state or rewrite the client's own
  conversation.
- nothing else in the body moves except `stream: true`; the input byte slice is
  never mutated in place, because the same original bytes are replayed through
  the candidate's transform on every attempt.

The appended-turn case (no prefill) and the Responses case (the client's input
normalized into the array form, plus an assistant `message` item and a fixed
instruction item) follow the same rule: the client's own bytes are preserved and
only the continuation's own members are added.

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
- **No editing of a stream in flight.** The relay is byte-faithful over every
  hop: the proxy adds no marker and removes none, so a provider that sends its
  terminal marker twice has both relayed exactly as they would be with the
  block off. What the feature guarantees is its own contribution — nothing.
- **No upstream request for a departed client.** Once the client is gone, the
  loop stops at the caller gate and zero further exchanges are spent; a client
  write that fails ends the effort the same way. This is asserted at both
  levels (`TestStreamRecoveryStopsOnAClientWriteFailure`,
  `TestE2EStreamRecoverySpendsNothingForADepartedClient`).
- **No metrics.** The repo has no metrics subsystem; none was added.

## Observability

| event                       | level | fields                                                                                                                                                                                                            |
| --------------------------- | ----- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `stream_recovery_started`   | INFO  | `recovery_index`, `partial_bytes`, `provider`, `upstream`, `policy_hash`, `policy_generation`                                                                                                                     |
| `stream_recovery_succeeded` | INFO  | `recovery_index`, `recovered_bytes`, `recovered_events`, `upstream_exchanges`, `elapsed_ms`                                                                                                                       |
| `stream_recovery_failed`    | WARN  | `recovery_index`, `phase` (`build`/`credential`/`budget`/`dial`/`upstream_status`/`upstream_read`/`client_write`), `upstream_status`, the sanitized `err`, `upstream_credential_id` when the candidate has a pool |
| `stream_recovery_exhausted` | WARN  | `recoveries`, `reason` (`budget_spent`/`max_recoveries`/`max_elapsed`/`logical_terminal`/`unsafe_content`), `unsafe_reason` only when the reason is `unsafe_content`                                              |
| `stream_completed`          | DEBUG | + `stream_recoveries`                                                                                                                                                                                             |
| `stream_truncated`          | WARN  | + `stream_recoveries`, `recovery_reason`, phase `recovery` for a marker-less stream — or outcome `client_disconnected` when the caller left during a hop                                                          |
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

## A defect this branch found and fixed in its own review

The pooled branch of a hop can be refused before its first dial, and
`internal/transport/pool.go` reports that refusal the only way it can: a **nil
response with a nil error** and `BudgetExhausted` set. `dialContinuation`
originally named a phase only when `err != nil`, so that one outcome left both
`resp` and `phase` empty and the loop dereferenced the response that was never
produced.

It was reachable, not theoretical. The loop's envelope gate and the pool's
claim are two reads of the same clock, so an envelope whose _elapsed_ half
expires between them — the candidate envelope's default ceiling is two minutes
(`MaxCandidateElapsedCap`), and a long stream reaches it — passes the gate and
is then refused at the dial. `net/http` recovers the resulting panic per
connection, so the process survived and the request died without its completion
record: a silent failure, which is why it took a review to find rather than an
incident.

`continuationDial` now states the invariant its callers rely on — `resp` is
non-nil exactly when `phase` is empty and `err` is nil — and
`TestStreamRecoveryPooledBudgetRefusalIsAPhase` drives the real handler with a
doer that returns the pool's sentinel, so the shape is pinned rather than
assumed. The refusal reports `phase: budget`, blames no endpoint, and leaves the
stream truncated exactly as it would have been with the feature off.

And one misclassification, found the same way. The loop's caller gate stops
without a `recovery_reason`, because nothing about the recovery was wrong — the
reader left. But the truncation that follows keyed only on the relay's error, so
a client that hung up during a hop, on a stream that ended cleanly at EOF,
produced an outcome of `stream_truncated`: the report an operator reads as "the
provider cut this answer", for a hop the proxy had deliberately refused to dial.
The loop now records that it stopped at the caller gate, and a clean EOF on an
abandoned request reports `client_disconnected` — the same classification a
failed client write already got — while the phase stays `recovery`, which is
what actually stopped. `TestStreamRecoveryStopsWhenTheClientLeavesMidHop` pins
it: the cancel fires from inside the hop's dial, the hop answers with a clean
EOF so every other gate would have permitted a second hop, and the dial count
stays at two.

## The hardening pass (post-merge adversarial review)

The pass did not redesign the feature and changed no default. It fixed seven
defects, each of which is now pinned by a test, and each of which is a case
where the shipped code was reasonable-looking and wrong.

1. **`max-elapsed` was not a bound on a blocked read.** It was checked only
   after `CopySSE` returned, so an upstream that sent one event and held the
   connection open parked the relay in `ReadSlice` indefinitely: the window
   existed on paper and could not fire. The window is now armed around every
   relay pass as a body-closing watchdog — see "The two bounds" above for why
   the lever is the body and why the timer is not derived from the request
   context. The state it added is one `atomic.Bool` and one deadline.
2. **Arming that watchdog broke every deployment that did not want it.** The
   first version of the fix armed the window unconditionally, and because the
   resolved policy carries a nonzero `max-elapsed` (20s by default) even when
   the block is `enabled: false`, every healthy committed stream would have
   been cut twenty seconds in — on deployments that never configured any of
   this. The gate is `contPolicy.Enabled` and the disabled path keeps the plain
   relay. The regression test was written, verified to fail against the
   unconditional arm, and is in the suite; the first version of it did not, and
   that is why it is worth stating here.
3. **`finish_reason` was conflated with unsafe content.** A generation the
   upstream declared finished was reported as `unsafe_content`, which reads to
   an operator as "the proxy could not safely continue this" rather than "the
   model stopped talking". Terminal and unsafe are now separate verdicts and
   separate stop reasons.
4. **Responses identity was checked in one place and not the other.** A
   `response.output_item.added` announcing a second message item was accepted
   when no item had been seen yet, and was never cross-checked against the
   identity the deltas had bound — so the proxy would concatenate two message
   outputs into one continuation. Identity is now bound once, from either
   place, and any disagreement refuses the stream. This one was found by
   writing the multi-output test and watching it dial twice.
5. **A Chat prefill was rebuilt rather than mutated.** The builder reconstructed
   the assistant message from the fields it knew about, silently dropping a
   `name`, a provider extension, or any field this build has never heard of.
   It now regenerates `content` and nothing else.
6. **`max-recoveries` was ambiguous.** It now means continuation REQUESTS
   initiated, with the slot claimed before the dial, and the stop-on-hop-failure
   policy is stated in the code and pinned by tests rather than left to be
   inferred.
7. **The cancellation races had no tests.** Client-disconnect-vs-window,
   disconnect-mid-hop, write-failure, and disconnect-during-dial are now each
   pinned at the unit level, and two of them at the wire level.

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
  a disabled block still carries the stated bounds (the coupling that makes the
  proxy's `Enabled` gate load-bearing); and a frozen pre-change
  `config.example.yaml` still loads and resolves to the zero policy.
- `internal/proxy/sse_test.go` — `StreamStats.Terminal` true for `data: [DONE]`
  and `event: response.completed`, false for a plain data line; the existing
  tests and the benchmark compile unchanged.
- `internal/proxy/continuation_test.go` — the safety gate per closed token;
  prefix accumulation across chunks; `finish_reason` latching TERMINAL (never a
  refusal) for `stop`/`length`/`content_filter`/`tool_calls`; the first refusal
  sticking; the oversize bound; the twelve-row Responses identity matrix
  (item-id, output-index and content-index changes, channel switch, a second
  message item, a tool-call item, missing/non-numeric identity) plus the
  identity spanning hops. The accumulation ACROSS hops is exercised by the loop
  tests in `streamrecovery_test.go`.
- `internal/inject/continuation_test.go` — both builders, the prefill
  extension, the appended-turn shape, every refusal token, that neither mutates
  its input, and that a prefill's unknown members survive the extension.
- `internal/proxy/streamrecovery_test.go` — the loop end to end over both API
  surfaces: off-by-default byte-identity, a continued stream, a refusal at each
  gate, a spent budget, a spent reach, a client that leaves, an unsafe stream,
  an oversize prefix, a truncated line, a hop failure, a stream that already
  terminated, the window cutting a blocked read, the window covering a hop, a
  client cancel beating the window, a simultaneous window/disconnect, a client
  write failure, and the finish-reason rows — plus the no-window-when-disabled
  regression, whose fixture is a reader that HONORS its Close, because a reader
  that ignored it would pass even against the bug it exists to catch.
- `e2e/recovery_test.go` — the real binary: one connection, two hops, one
  marker, the log evidence, the refusal matrix, the stalled-upstream window,
  the enabled window bounding a healthy generation, a departed client, the
  hop-failure matrix, a clean EOF, an empty prefix, the Responses multi-output
  refusal, and that neither the client's turn nor the committed prefix reaches
  the process's output.

## Rollout

Disabled by default. Enable with `recovery.stream.enabled: true` and
`max-recoveries: 1` on a canary model first — a model whose clients tolerate a
visible seam. The signal to watch is the ratio of `stream_recovery_succeeded`
to `stream_recovery_exhausted`, and among the exhausted ones, `unsafe_content`.
On agent traffic `unsafe_content` should be the most common skip, and that is
the feature working, not failing: a tool-call stream is one this proxy must not
continuously re-enter.

Before enabling it on a model, size `max-elapsed` above the longest generation
that model produces. While the block is on, the window is armed on the committed
stream itself, so a `20s` window on a long-reasoning model will cut healthy
answers — reported, correctly, as `max_elapsed`.
