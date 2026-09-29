# Design note — proxy-owned request identity (issue #114, PR #115)

What this branch added, what it deliberately did not add, and the one ordering
rule that makes the whole thing quiet rather than loud when it breaks.

Issue #114 proposed this as an opt-in `request-id:` YAML block. That framing
was declined during planning: the absent block was going to be the safety net
("the absent block is byte-identical traffic"), and a safety net that only
holds when nobody configures the feature is not a guarantee, it is a bet. The
feature is on for every deployment, with no configuration key at all. Everything
below describes what shipped; where issue #114 proposed something else, that is
marked as rejected and why.

## The problem

The proxy already mints a per-request id — 16 lowercase hex characters, bound
to the request-scoped logger, present on every log event. It never left the
process. The `X-Request-Id` a client actually received was whatever the
**upstream** said, because the name was on the relay allow-list alongside
`Content-Type` and `Retry-After`.

The consequence is that the two ids were unrelated. A support ticket quoting an
`X-Request-Id` resolved to nothing here, because the value came from a provider
and never appeared in a log line. And an upstream could not correlate our hop
against its own logs, because we never told it which request a call belonged
to. Both sides had a correlation id; they just were not the same one.

## The ownership decision

**The proxy mints, and nothing else may choose the value.** The id is
generated once per request from crypto/rand, before the request even loads its
config snapshot, and is bound for the life of the request. Three consequences
follow, and all three are the point:

- **A client's inbound `X-Request-Id` is dropped.** It is not on
  `forwardHeaderNames`, and the outbound request is rebuilt per attempt with
  the proxy's value set on it. The client is not told this happened; the header
  simply is not its own any more.
- **An upstream's `X-Request-Id` is not relayed.** It left the allow-list.
- **`OpenAI-Request-Id` left the allow-list too, in both directions** — as a
  relayed name and as an evidence field in `evidenceRateLimitFields`. Not
  forwarded, not relayed, not logged. There is no second correlation channel
  for a provider to fill.

One hop, one id, one meaning. The value on the wire is the value in the logs is
the value in the usage row is the value the provider saw.

## The rejected alternatives

**Adopt the client's id when it is present** (issue #114's spirit, and what
most proxies do). Rejected on two independent grounds. First, it is a log
injection vector: the value would land in every log line of the process and in
the `usage_events.request_id` column, so a caller supplying
`"x\n2026-01-01T00:00:00Z ERROR forged"` forges log lines (CWE-117), and a
caller supplying megabytes grows every line. Second, it makes cardinality
attacker-controlled: the id becomes a high-cardinality label on the hottest log
path, which is a denial-of-service surface wearing a debugging feature's
clothes. Neither is worth the ergonomic gain, which is small — a client that
wants its own id can put it in a header the proxy does relay, or in its body.

**A second header name, `X-Correlation-Id`, relayed alongside.** Rejected: it
leaves the client holding two ids that disagree, one of which is the provider's
and one of which is ours, with nothing marking which is authoritative. The
ambiguity is worse than either value alone.

**W3C `traceparent` / OpenTelemetry propagation.** Rejected for now, not on the
merits. The proxy does not sample, does not export spans, and is not part of a
trace — adopting the header name would promise a distributed-trace integration
this service does not have. If it ever gets real OTel support, the id this
design already mints is the natural trace id to derive it from, and the change
then is additive.

**Serving the id in the response body.** Rejected: it would mean synthesizing a
body member, which contradicts the byte-preserving transform discipline the
whole response path is built on, and every client SDK reads headers rather than
bodies anyway.

## Why the id is not backoff input, and why overwriting is safe

The relay allow-list keeps `Retry-After` and the `X-RateLimit-*` family
precisely because they are load-bearing: without them a 429 is
indistinguishable from any other upstream failure to a well-behaved SDK's
backoff. `X-Request-Id` is not in that class. Nobody backs off on it, no SDK
parses it, and no correct client behavior changes if it is replaced. That is
what makes it safe for the proxy to overwrite the upstream's value — and it is
why the two lists (`relayHeaderNames` and `evidenceRateLimitFields`) are kept
in step deliberately: the request-id names are absent from **both**, and
adding a name to one and not the other is a defect, because it is the one way a
header becomes readable in a log without being an allow-listed relay name.

## The ordering rule — the one that bites

`copyRelayHeaders` uses `http.Header.Set`, which overwrites. So a proxy value
written **before** it is silently replaced by the upstream's — no error, no
missing header, no visible symptom. The client holds a valid-looking id that
matches no log line and no usage row. The header is present, so any test that
only checks "is there an `X-Request-Id`?" passes, and the failure surfaces days
later as an unresolvable support ticket.

The invariant, therefore: **`setRequestID` is always called after
`copyRelayHeaders`, immediately before `WriteHeader`.** Not "somewhere in the
response path" — immediately before the commit, at every commit site. There are
five (normalized 4xx/5xx, verbatim 3xx/204/304, SSE, buffered 2xx, models 200) plus the two `reject` closures that cover every locally generated
envelope. `internal/proxy/requestid_test.go` walks all of them with an upstream
that plants its own id on every answer, so a site added later fails loudly
rather than inheriting the property by hope.

`Content-Type` on the normalized-error path is the existing precedent: the
proxy already relays a header and then overwrites it, which is the same
technique for the same reason.

## Scope: what is stamped, and what is not

- **Stamped:** every `/v1` route (chat, responses, models), both 405s, and the
  catch-all 404.
- **Not stamped:** `/healthz` and `/readyz`. They are container probes fired on
  an interval, they bind no request lifecycle, load no snapshot, and log
  nothing — an id on them would be a header that joins to no evidence at all.
  The exemption is asserted in a test, because an unasserted exemption decays.
- **The catch-all 404 is the odd one out:** stamped, but with no log line to
  join to. It is a stable handle the client can quote; adding a log event for
  it would be unbounded noise from a path scanner, for no investigative value
  the 404 body does not already provide.

The mint is hoisted above the method check in both `serve` and `models` so the
405s can carry an id. `start`, `snap`, the deferred completion log and the
status writer all stay below it, so a 405 still loads no snapshot, still writes
through the bare `ResponseWriter`, and still fires no `request_completed`. The
hoist costs the 405 nothing on the wire except the one header.

## Why there is no configuration key

The header name is a documented internal constant (`requestIDHeader`), treated
like `maxRequestBodyBytes` and the `memlimit` budget. The reasons the plan
settled on:

- **There is no setting worth making.** `enabled: false` would be a way to
  turn off the ability to correlate a support ticket with a log line — a
  capability, not a tuning knob, and one whose absence is discovered during an
  incident.
- **`forward: true|false` is not a real choice.** Forwarding the id is what
  makes the upstream's side joinable. Not forwarding it would leave the proxy
  knowing its own request id and the provider not, which is the pre-existing
  half of the problem.
- **A configurable header name cannot be validated into safety.** An
  operator-supplied name goes through `credential.ValidHeader` and the charset
  and collision checks that implies, and every one of those checks exists
  because the name then lands in a log. Keeping the name in code removes the
  entire class. The cost is a deployment in front of an upstream that wants a
  differently-named correlation header cannot rename it — accepted, because an
  id an operator can grep from a client ticket is worth more than one
  satisfying every upstream dialect.
- **It keeps the code-owned non-configurables list intact.** The recovery
  section already has a set of things no matrix can steer. Request identity
  belongs to that set: it is a property of the process, not of the traffic.

The practical signal of the decision is in the diff: `internal/config` and
`config.example.yaml` are untouched. The absence of a `request-id:` stanza is
the deliverable.

## Where the value goes

Four surfaces, one value:

1. The `X-Request-Id` **response header** on every `/v1` answer.
2. The `request_id` field on every **log event** the request emits (unchanged
   behavior — the id was already there; it just now also has a wire name).
3. The `X-Request-Id` **request header forwarded upstream**, on every attempt
   and every fallback candidate. The outbound request is rebuilt per attempt,
   so one `setRequestID` after `copyForwardHeaders` covers retries and
   fallbacks without further work.
4. The `request_id` column of the **usage record** in metering mode.

A stream-recovery hop forwards the **same** id as the attempt it continues, and
takes it as a field on `continuationHop` rather than minting one — a
continuation is the same client request re-asked, and minting inside
`dialContinuation` would split one request's upstream-side evidence in two, with
the join holding for the first dial and quietly breaking at the second. The
hop pays its dials out of the request's own envelope, so it is one request for
identity purposes exactly as it is for budget purposes.

A reload mid-request cannot reshape any of it: the id is minted before the
snapshot is loaded and lives exactly as long as the request, which is the same
binding the rest of the request has.
