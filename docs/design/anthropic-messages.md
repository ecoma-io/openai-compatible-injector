# Design note — the Anthropic Messages surface

What this surface adds, what it deliberately does not, and the one structural
finding that decided most of the implementation.

The requirement was narrow and absolute: **a real Claude Code install must work
against this proxy with no change to the `providers:` / `models:` YAML.** Not a
second config format, not a per-route upstream block — the same entry that
serves an OpenAI client must serve an Anthropic one. Everything below follows
from that, plus two earlier decisions the user settled directly: both dialects
live under `/v1` (Anthropic's own `POST /v1/messages`, not a root-level
`/messages`), and translation is chosen over dialect pass-through — the cost of
breaking byte-preserving transforms on this route was accepted knowingly.

## The structural finding

**The existing `rewriteFunc` seam — one payload in, one payload out — cannot
express Anthropic's SSE.**

Anthropic's stream is a sequence of _named events_. One upstream Chat chunk can
owe the client up to three of them: a `content_block_stop` closing the previous
block, a `content_block_start` opening the next, and a `content_block_delta`
carrying the bytes. That is an N:1 mapping, and no `func([]byte) []byte` can
return three frames plus their `event:` and blank framing lines. The framing
lines are not reachable through `rewriteSSELine` at all — it is handed one line
and returns one line.

So the naive options were both wrong:

- **Widen `CopySSE`** to translate. This would have changed the OpenAI routes'
  most delicate contract — the pointer-identity no-op, the `"model"`/`"usage"`
  rewrite gate — for a route that does not use them, and churned roughly twenty
  direct `CopySSE(...)` call sites in tests.
- **Teach `rewriteSSELine` about events.** It would need to emit lines it was
  never given, which means either buffering across lines (losing the per-event
  flush the heartbeat and the client both depend on) or returning a variable
  number of lines (changing its signature and every caller).

The answer was a **second relay beside the first**: `CopyMessagesSSE`, sharing
`readBoundedLine`, `MaxLineBytes`, `MaxEventBytes`, `isEventBoundary`,
`StreamStats` and the CRLF accounting, and admitting every `data:` payload
because the whole payload is being translated rather than a key rewritten
inside it. `CopySSE`, its gate and its pointer-identity contract are untouched —
byte-identical on Chat and Responses, with zero call-site churn.

What the two relays share is exactly what is common: the line grammar, the
caps, the flush rule, the stats. What they do not share is the thing that was
never actually common — the mapping.

## Decisions

### The translator lives in `internal/inject`

`MessagesToChat`, `ChatToMessages`, `StripContextMarker`,
`ChatToMessagesStream` and `BuildContinuationMessages` are pure body transforms,
exactly like `Probe`, `Chat` and `Responses`. `inject` already depends on
`internal/config` and is already imported by `internal/proxy`.

A new `internal/anthropic` package was considered and rejected: it would cost an
"Layout and boundaries" bullet in AGENTS.md plus a README layout entry, out of a
byte budget that is deliberately tight, **for no boundary the existing prose
does not already describe**. The Anthropic error dialect is the one piece that
is not a pure transform — it is selected per request from live state and writes
to the response — so it stays in `internal/proxy` beside the envelope constants
it wraps.

### One dedicated relay, one extended terminal predicate

`CopyMessagesSSE` re-emits frames through `writeFrames`, which cuts by
`bytes.IndexByte(frames, '\n')` and performs **one `dst.Write` per line**. That
is a contract, not a style choice: `pingWriter` reads the tail of each buffer to
decide whether the stream sits at an event boundary, and latches `finished` when
a write's bytes are terminal. A relay that batched several lines into one write
would hide both.

`isTerminalSSELine` — the single byte predicate `CopySSE`'s `stats.Terminal`,
`pingWriter.finished` and `continuationEligible` all read — gained a third
arm: `event: message_stop`. Because all three consumers run on the same written
bytes, they agree for free, and the addition is strictly additive for the two
existing markers. The predicate is deliberately **stricter than generic SSE**:
`event:message_stop` with no separator space is a valid event name to the parser
and still not a terminal, because the predicate matches the exact bytes the
frame builder writes. The near-miss table in `sse_terminal_test.go` pins that
from both directions.

### The error dialect is a value, not a parameter

`serve` already receives `api` and already branches on it, so
`env := dialectFor(api)` at the top of `serve` threads the shape through every
`reject` site **without changing `serve`'s signature** — which matters because
two tests call `serve` directly, and a signature change would ripple into them.

`openAIDialect` wraps the pre-existing envelope constants _verbatim_; no literal
was retyped, so the OpenAI bytes cannot drift. `dialectFor` returns it for
everything that is not `apiMessages`, so a surface added later inherits the
bytes it already produces until it deliberately asks for its own.

The dialect is computed **before the method check**, which is what preserves
405-before-401 on `/v1/messages` — the Anthropic 405 is written for a request
that never presented a credential, exactly as the OpenAI one always was.

Scope is narrow and deliberate: only envelopes `serve` generates. The catch-all
404, `/healthz`'s 405 and `GET /v1/models` are separate handlers with their own
constants and do not participate — an unknown path is not a Messages request,
and the models surface is OpenAI-shaped by contract.

The Anthropic envelope is `{"type":"error","error":{"type","message"}}` and
carries **no `param`, no `code`, and no request id**. That is the API's shape,
not a truncation: the diagnosis an operator needs is in the log event, whose
vocabulary is closed-set anyway, and the id reaches the client in
`X-Request-Id` like everywhere else. The upstream-status → `error.type` map is
Anthropic's own vocabulary (`429 → rate_limit_error`, `≥500 → api_error`, …),
not a renaming of the OpenAI `upstream_error` family, because a Messages client
dispatches on these names.

### `x-api-key` is one helper, Authorization-first

`clientToken(h http.Header)` is used by both `serve` and the models handler:
Bearer first, then `X-API-Key`, both gated by the existing `validBearerToken`
(same 4 KiB cap, same character class — no new validation vocabulary).

The precedence is **forced by an existing test**: `e2e/wire_resource_test.go`
sends `X-Api-Key: drop-me-secret` alongside a valid Bearer. An
x-api-key-first implementation would authenticate with the secret and 401 that
test — and the ordering is the right one anyway: the Authorization header is
what every other route already trusts, so it keeps precedence when both appear.
Real Claude Code sends only `x-api-key` (with `ANTHROPIC_API_KEY` set it puts
**no** `Authorization` header on the wire at all), so the interesting path is
the one that had no test before.

`forwardHeaderNames` needed **no change**: `x-api-key` was never on it, so it
was never forwarded. That is worth stating because it is the failure that would
have mattered — the client's credential crossing to the provider.

### `stream_options` is injected only on this route, and only when streaming

A Claude Code client reads usage from `message_delta`. The upstream is a Chat
Completions provider that will not volunteer `include_usage`. So the translation
adds `stream_options: {"include_usage": true}` **iff `stream == true`**.

This is a client-facing wire shape only. Metering is untouched by construction:
`usageCapture.Observe` runs on the **pre-rewrite upstream bytes**, exactly as
before, and the row's "never fabricate a zero" rule still means SQL `NULL` for
an upstream that stated nothing.

Risk, recorded rather than suppressed: a strict upstream that rejects
`stream_options` fails **only** this route, surfacing as an ordinary upstream
400 the recovery walk already knows how to answer. Visible, not silent.

### The usage-placement question is answered by a real client

The load-bearing question was not "does the envelope carry usage" but "does a
real SDK _read_ it where we put it". Claude Code merges `message_start` and
`message_delta` usage and reports a total; if the counts had been placed
anywhere a real client does not look, the acceptance test's
`usage.input_tokens > 0` would fail even though every unit test passed.

It reports `input_tokens: 11, output_tokens: 2` — the upstream's own numbers.
That is the honesty check on the placement, and it is why
`TestClaudeCodeRealClient` asserts the client's totals rather than the bytes.

### Ids pass through opaquely

`chatcmpl-*` becomes the `message.id`; `call_*` becomes the `tool_use.id`. Both
APIs document ids as opaque, and the client stores exactly what we emitted — so
there is no mapping state anywhere, buffered or streamed. The alternative (a
deterministic prefix swap) is a cheap follow-up if a real client ever proves
format-sensitive; nothing observed suggests one does.

### Metering maps `messages` onto the chat extractor, with no migration

`NewCapture` uses `ExtractResponsesUsage` only for `"responses"`; everything
else — `"chat"` and `"messages"` — reads the top-level Chat usage object. That
is correct precisely _because_ `Observe` reads pre-rewrite bytes: a Messages
request's upstream traffic **is** Chat Completions, so the bytes it observes are
Chat bytes regardless of which dialect the client spoke.

`usage_events.api` is `text NOT NULL` with no CHECK constraint, so
`api: "messages"` rows are valid as written. **No migration.**

### Stream continuation is refused, not degraded

Post-commitment recovery is a second orchestrator that re-asks the committed
candidate. `BuildContinuationMessages` returns
`&ContinuationRefusal{reason: reasonUnsupportedShape}` — the existing closed
set — selected by the `apiMessages` branch of `buildCont`.

The refusal is _tested_, because a refusal that is not tested decays into an
accidental hop that would feed translated bytes back into `inject.Chat`. The
healthy path needs no recovery at all (`message_stop` → `stats.Terminal` → the
loop never runs); the refusal is what happens to a truncated stream, and the
client's stream is left exactly as unterminated as it would have been, with
`unsafe_reason: unsupported_shape` and no dial.

A deferral with a named refusal is a decision. A deferral that silently
half-works would not be.

### Thinking blocks are dropped in both directions

Anthropic `thinking` / `redacted_thinking` blocks are not forwarded upstream;
an upstream `reasoning_content` is not surfaced. Both are client-only or
provider-only vocabulary that a strict OpenAI upstream would reject.

Simulated thinking usage still activates, because `inject.ThinkingIntent` reads
the request-level `thinking: {type: "enabled"}` object, which the translation
passes through untouched. Converting `reasoning_content` into a `thinking` block
is the recorded follow-up — it would force this drop rule to become
keep-and-convert on both sides at once, which is a different change.

### `[1m]` is stripped on lookup, and proven by a direct POST

A trailing `[1m]` is the 1M-context marker some clients append to a model name.
`StripContextMarker` trims, matches case-insensitively at end-of-string, and —
on no match — returns the **original untrimmed** string, so a name that merely
contains the substring elsewhere is untouched. The 404 interpolates the
post-strip name, byte-exact and unescaped like every other 404.

Proving this needed care. This machine's Claude Code **strips the marker
client-side** before it ever reaches the wire (it sends `claude-sonnet-4-5` and
adds a `context-1m` beta header instead), so a real-client test can never put
the marker on the wire to be stripped. It is therefore proven by
`TestMessagesStripsContextMarker`, a direct POST.

And a body-wide search for `[1m]` in the real-client test fails for an unrelated
reason: **Claude Code quotes its own id in the prompt text** — a
`<system-reminder>` tells the model "the exact model ID is
`claude-sonnet-4-5[1m]`". The marker is therefore checked in the `model` field,
which is the only place a configured model name can cross.

## The URL decision

Both dialects live under `/v1`, at the paths their own SDKs already use:

| Surface                       | Path                        |
| ----------------------------- | --------------------------- |
| Anthropic Messages            | `POST /v1/messages`         |
| OpenAI Chat Completions       | `POST /v1/chat/completions` |
| OpenAI Responses              | `POST /v1/responses`        |
| Model catalog (OpenAI-shaped) | `GET /v1/models`            |

A root-level `/messages` was proposed and rejected: `/v1` is the OpenAI
convention's namespace, and putting a second dialect's route beside it rather
than inside it would make the base URL depend on which SDK you were driving. A
client points its base URL at the proxy and everything resolves.

## Test surface

- **Unit, `internal/inject`** — request translation tables (both `system`
  shapes, `cache_control` removal, tool round-trips, allowlist drops,
  `stream_options` on/off), `StripContextMarker`'s no-match-returns-original
  rule, buffered response translation, and the golden event sequences for the
  stream translator — including the truncation case that must emit **no**
  `message_stop`.
- **Unit, `internal/proxy`** — the nine Anthropic literals byte-exact with the
  OpenAI identity proven on the other two surfaces; the route-level matrix
  (405-before-401, `x-api-key` alone / wrong / absent, `[1m]`, transform 400
  with the upstream hit counter frozen); `CopyMessagesSSE` stats, flushes,
  caps, observe ordering and the two no-terminal cases; `message_stop` as a
  terminal plus its near-miss table; the keep-alive latching off it.
- **E2E, hermetic** — eleven black-box cases over the built binary asserting
  only what a real Anthropic client can observe: the request the upstream
  received, the answer the client received, and the headers each side saw.
- **E2E, real Claude Code** — the acceptance test. A genuine `claude` process,
  from an isolated temp-dir `HOME`/`CLAUDE_CONFIG_DIR`/`XDG_*` tree, asserting
  exit 0, a `PONG` result, **non-zero client-reported usage**, and on the
  upstream side: the configured alias, `parameters` (not `input_schema`) under
  OpenAI's `{type, function}` nesting, the injection prompt at `messages[0]`,
  no `[1m]` in the model field, and no `Authorization` / `X-Api-Key` /
  `anthropic-version` / `anthropic-beta` / `x-app` crossing. It skips when no
  binary is present — that is the default, and it is what keeps CI hermetic.

  Deliberately **not** asserted: a tool-call round trip under a real client
  (would require letting the client execute a tool — not hermetic), and `[1m]`
  via a real client (impossible by construction; see above). Both are covered
  elsewhere.

## Known limitations

- **`GET /v1/models` answers the OpenAI list shape** for Anthropic clients too.
  Real Anthropic clients do not call it — the Claude Code 2.1.291 capture made
  zero `/v1/models` requests — so this is a documented asymmetry rather than a
  gap a client will hit. Making it dialect-aware would mean teaching a local
  handler about a client it does not otherwise know about, for a request that
  does not occur.
- **`stream_options` rejection** by a strict upstream affects only this route,
  as an ordinary upstream 400 under the walk. Recorded, not suppressed.
- **Stream recovery is unavailable** on this surface, by explicit refusal.
- **Thinking blocks are dropped**, in both directions.
