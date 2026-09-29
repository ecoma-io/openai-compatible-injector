# Behavioral contract

The behavior contract is **README.md**. This file is the machine-checkable
index of it: each invariant a refactor could break, stated so precisely that
an implementation other than ours could be verified against it rather than
merely read next to it.

`docs/design/` carries the rationale. This file carries the assertions.

## How to read an entry

| Field                | Meaning                                                                                |
| -------------------- | -------------------------------------------------------------------------------------- |
| **ID**               | Stable handle. A refactor that changes an invariant must cite it.                      |
| **Description**      | One sentence, no rationale. Rationale lives in README/AGENTS/design docs.              |
| **Trigger**          | The preconditions under which the invariant applies.                                   |
| **Expected**         | What MUST happen.                                                                      |
| **Forbidden**        | What MUST NOT happen. A "forbidden" clause is a test, not a style note.                |
| **Observable**       | Where a test can see it: client response, log line, usage row, metric.                 |
| **Regression tests** | Existing tests that fail if the invariant breaks. `GAP` means no test does — see §0.3. |

## 0. Baseline

Recorded at `efb199b` ("docs: bring the agent guide back under its byte
budget (#119)"), v0.15.0, on go1.26.4 linux/amd64.

| Gate               | Command                                                   | Result                                                                     |
| ------------------ | --------------------------------------------------------- | -------------------------------------------------------------------------- |
| Format             | `gofmt -l .`                                              | clean (empty output)                                                       |
| Vet                | `go vet ./...`                                            | clean                                                                      |
| Lint               | `golangci-lint run ./...` (2.12.2, the CI-pinned version) | `0 issues.`                                                                |
| Unit + e2e         | `go test ./...`                                           | all packages ok; `e2e` 387.257s, `internal/proxy` 38.228s                  |
| Race               | `go test -race ./...`                                     | all packages ok, no race reports; `e2e` 385.770s, `internal/proxy` 42.073s |
| Agent guide budget | `./scripts/check-agents-md-budget.sh`                     | clean (`maximum_bytes=40000`)                                              |

Per-package test surface: 988 test functions, 200 subtests — `internal/proxy`
350, `e2e` 144, `internal/config` 113, `internal/transport` 103,
`internal/recovery` 94, `internal/inject` 62, `internal/usage` 36,
`internal/credential` 33, `internal/auth` 33, `internal/server` 15,
`internal/memlimit` 7.

### 0.1 What the baseline does and does not freeze

The suite is strong on **scenario** coverage: commitment, send-state
classification, the exchange envelope, credential and egress rotation, request
identity, SSE framing, and usage accounting each have named tripwires (§4
points at them). It is weak on three axes, which is why this initiative
exists:

1. **No cross-cutting fingerprint.** Each test asserts one facet. Nothing runs
   a whole request and records its observable shape — status, headers,
   semantic body, candidate path, attempt/retry/fallback counts, credential
   and egress choices, outcome, usage events, commit point, stream sequence —
   as one comparable value. A refactor cannot be diffed against "before".
2. **No fault script.** A fake upstream cannot, today, be scripted to fail in
   a chosen way at a chosen point of a chosen attempt, so the recovery
   matrix is exercised from the outside (configure a chain, point it at a
   server, observe) rather than from a deterministic script.
3. **No differential runner.** There is no way to execute a pre-refactor and a
   post-refactor build over identical inputs and compare. Review therefore
   argues equivalence from reading the diff.

`GAP` in the Regression tests field means exactly that: the invariant is real
and load-bearing, and nothing would fail if it broke. Every `GAP` is a
Phase 1 deliverable, not an optional extra.

### 0.2 Baseline drift

If a change to this file's baseline table is accompanied by a claim that the
suite was already failing, that is a rollback condition, not a note: see
`docs/design/refactor-roadmap.md` §Rollback.

### 0.3 Reading the Regression tests field

A test is a tripwire for an invariant only if removing the invariant makes
**that** test fail. Tests that exercise the same subsystem without asserting
the invariant are not listed. Where a name is listed it is the test function
to run, and it must be re-run after any refactor touching that invariant.

## 1. Request lifecycle

### INV-LIFE-01 — Authentication precedes body read and upstream I/O

- **Description**: Every `/v1` request authenticates before its body is read
  and before any upstream socket is opened.
- **Trigger**: Any request to `/v1/chat/completions`, `/v1/responses`, or
  `GET /v1/models`.
- **Expected**: The `auth.Provider` seam is consulted, and only on success
  does the handler touch `r.Body` or resolve a transport.
- **Forbidden**: Reading or buffering any byte of the request body before the
  auth decision; dialing before the auth decision. A 401 response must not
  carry a body that was read from the client.
- **Observable**: 401 envelope; absence of any outbound connection in the
  fake-upstream's request log; `unauthorized` outcome.
- **Regression tests**: `TestAuthRequiredExactBodies`, `TestClientAuthRequired`,
  `TestUpstreamReceivesNoAuthorization`, `TestHealthzOpenWithoutAuth`,
  `TestUsageUnauthorizedNotMetered`.

### INV-LIFE-02 — The client's Authorization header is consumed, never forwarded

- **Description**: The proxy consumes the client's `Authorization` header and
  substitutes its own upstream credential; the client's value never reaches an
  upstream.
- **Trigger**: Any authenticated `/v1` request.
- **Expected**: The outbound request carries the candidate's configured
  credential, not the caller's key.
- **Forbidden**: Forwarding or replacing-by-concatenation; any path that
  copies client `Authorization` onto the outbound request. Same for the
  client's `X-Request-Id`.
- **Observable**: The fake upstream's recorded request headers.
- **Regression tests**: `TestUpstreamReceivesNoAuthorization`,
  `TestClientRequestIDNeverTravelsUpstream`.

### INV-LIFE-03 — 405 precedes 401

- **Description**: A wrong method on a route is rejected 405 before its auth
  check runs.
- **Trigger**: Non-`GET` request to `GET /v1/models`; non-`POST` to
  `/v1/chat/completions` or `/v1/responses`.
- **Expected**: 405 with the canonical envelope.
- **Forbidden**: 401 for a wrong-method request; a snapshot load for a
  request that returns at the method check.
- **Observable**: 405 status and exact body.
- **Regression tests**: `TestMethodNotAllowedJSONEnvelope`,
  `TestProxyRequestIDOnThe405s`.

### INV-LIFE-04 — One request id, minted once, propagated everywhere

- **Description**: A single 16-hex id is minted by this process per request
  and is the same value in the log field, the response header, the header
  forwarded upstream, and the usage row — on every attempt and every recovery
  hop.
- **Trigger**: Every `/v1` request, every attempt, every stream-recovery hop.
- **Expected**: One value, propagated, never re-minted.
- **Forbidden**: Re-minting per attempt or per hop; adopting the client's
  `X-Request-Id`; relaying an upstream's `X-Request-Id` or
  `OpenAI-Request-Id`; the id appearing on `/healthz` or `/readyz`; the
  header being clobbered by the relay allow-list `Set`.
- **Observable**: `request_id` log field; the `X-Request-Id` response header;
  the fake upstream's recorded headers; the usage row's `RequestID`.
- **Regression tests**: `TestProxyRequestIDMatchesTheLoggedID`,
  `TestProxyRequestIDForwardedUpstream`, `TestProxyRequestIDUniquenessAcrossRequests`,
  `TestProxyRequestIDStampsEveryEnvelopeWriter`,
  `TestProxyRequestIDOnThe405s`, `TestProxyRequestIDOnModelsAndCatchAll404`,
  `TestProxyRequestIDOverwritesUpstreamOnEveryCommitPath`,
  `TestProxyRequestIDAbsentOnHealthz`, `TestStreamRecoveryHopForwardsTheSameRequestID`,
  `TestProxyRequestIDSurvivesAMidRequestReload`,
  `TestUpstreamRequestIDHeadersAreNotRelayed`,
  `TestClientRequestIDNeverReachesTheLogs`.

### INV-LIFE-05 — One request binds one immutable snapshot

- **Description**: A handler loads `store.Load()` exactly once and binds the
  whole request to that snapshot — the chain, every resolved recovery policy,
  every transport, the `api-key`, the strip list, and any active stream.
- **Trigger**: A config reload landing at any point during the request's life,
  including mid-body-read, mid-wait, and mid-stream.
- **Expected**: The in-flight request continues under the snapshot it bound at
  entry; a rotated `api-key` takes effect on the next request.
- **Forbidden**: Any re-read of the store within a request; a reload changing
  an in-flight request's candidates, matrix, envelopes, backoff, `api-key`,
  model mapping, or strip list; a usage row rebinding its generation.
- **Observable**: Candidate path and policy hash in the completion record;
  `config_generation`/`policy_generation`; usage row.
- **Regression tests**: `TestProviderChainBindsToItsSnapshot`,
  `TestProviderChainReloadMidRetryKeepsSnapshotPolicy`,
  `TestAuthUsesSnapshotKey`, `TestUsageReloadCannotRebindGeneration`,
  `TestProxyRequestIDSurvivesAMidRequestReload`, `TestConcurrentReloadAndTraffic`.

### INV-LIFE-06 — Body admission bounds size, time, and aggregate memory

- **Description**: A client body is bounded three ways: 64 MiB per body, a
  per-read timeout, and a process-wide 256 MiB live-buffer budget drawn as the
  buffer grows.
- **Trigger**: Any request whose body is read; any buffered (non-streaming)
  upstream answer.
- **Expected**: 413 at the size cap; 503 `capacity_exceeded` when the budget
  cannot fund a buffer.
- **Forbidden**: Using server `ReadTimeout`/`WriteTimeout` (they break
  keep-alive and SSE); clamping a reservation instead of refusing; a 503
  refusal entering the recovery matrix, retrying, falling back, or spending
  an exchange; the read deadline surviving the read it bounds.
- **Observable**: 413 and 503 envelopes; `buffer_capacity_exceeded` WARN
  naming `request_body` or `upstream_response`; `capacity_exceeded` outcome.
- **Regression tests**: `TestBufferBudgetRefusesARequestBody503`,
  `TestBufferBudgetRefusesABufferedResponse503`,
  `TestAdmissionBudgetHoldsUnderConcurrentRequests`, `TestBudgetNeverExceedsItsLimitUnderConcurrency`.

### INV-LIFE-07 — Caller cancellation is read from the context, not the error

- **Description**: A caller cancellation or expired deadline is terminal and
  is judged from the request context.
- **Trigger**: The client disconnects, or the caller's deadline passes, at any
  point in the request.
- **Expected**: Outcome `client_disconnected`, no envelope of its own beyond
  what was already committed, no usage event for an unmetered path.
- **Forbidden**: Deriving cancellation from error text or from walking the
  error chain for a sentinel; any retry or fallback after cancellation; a
  classification of a cancelled attempt as a transport failure.
- **Observable**: `client_disconnected` outcome; absence of retry/fallback
  events; the fake upstream's request count.
- **Regression tests**: `TestClientCancelBeforeUpstreamAnswer`,
  `TestDialWithinCallerCancellationOutranksTheWindow`,
  `TestDialWithinCallerCancellationStillOwnsBufferedBody`.

### INV-LIFE-08 — The 404 `model_not_found` body interpolates byte-exact

- **Description**: The unmapped-model 404 body embeds the requested model
  name byte-exact, with no HTML escaping.
- **Trigger**: A request naming a model absent from the public mapping.
- **Expected**: `{"error":{"message":"The model '<X>' does not exist or you
do not have access to it.", ...}}` with `<X>` exactly as sent.
- **Forbidden**: Routing the name through a JSON encoder that escapes `<`,
  `>` or `&`; forwarding the request upstream (404 is local).
- **Observable**: Exact response bytes.
- **Regression tests**: the `model_not_found` cases in `internal/proxy` and
  `e2e/models_test.go`. **GAP**: the byte-exactness-with-no-escaping
  property is pinned by a unit test per AGENTS.md but is not yet a
  fingerprint assertion.

### INV-LIFE-09 — An unmapped model is never forwarded

- **Description**: A 404 `model_not_found` is produced locally.
- **Trigger**: A request naming an unmapped model.
- **Expected**: 404, no upstream connection.
- **Forbidden**: Dialing, or reaching the transport layer, for an unmapped
  model. This is a SECURITY.md boundary.
- **Observable**: 404 envelope; the fake upstream's empty request log.
- **Regression tests**: `e2e/models_test.go`.

## 2. Injection and response transforms

### INV-INJ-01 — Injection must never corrupt the body

- **Description**: Chat prepends to `messages` only when it is a JSON array;
  Responses merges into `instructions` (string, array, or absent) and touches
  nothing else.
- **Trigger**: Any request with a configured per-model prompt.
- **Expected**: A well-formed body out, with the prompt applied.
- **Forbidden**: Injecting into a non-array `messages`; mutating any other
  member; injecting when the prompt is empty.
- **Observable**: Upstream-received body bytes.
- **Regression tests**: `internal/inject` suite (62 test functions).

### INV-INJ-02 — Transforms are byte-preserving and never re-serialized

- **Description**: `RewriteChatModel` replaces only the top-level `"model"`
  string value; `RewriteResponsesModel` additionally the `"model"` directly
  inside a top-level `"response"` object. Unparseable input returns the
  input unchanged.
- **Trigger**: Every relayed response; every request whose model is renamed.
- **Expected**: All bytes outside the replaced value are identical,
  including whitespace, key order, number formatting, and escape spelling.
- **Forbidden**: `json.Unmarshal` into a map and re-marshal; touching a chat
  payload's nested `response.model` (that is client data).
- **Observable**: Byte-exact response comparison.
- **Regression tests**: `internal/inject` model-rewrite tests; `internal/proxy`
  rewrite tests. **GAP**: no property test asserts that a whole corpus of
  well-formed bodies round-trips byte-identically outside the rewritten
  member.

### INV-INJ-03 — Field stripping is config data, byte-preserving, and last

- **Description**: `strip-fields` excises configured members from every
  relayed 2xx, last in the composed rewriter (rename → thinking synthesis →
  strip), with an excised member's key, colon, value and comma removed by one
  shared splice. Unparseable input returns the same slice.
- **Trigger**: A model or provider with configured strip paths.
- **Expected**: The listed paths are gone; everything else is byte-identical.
- **Forbidden**: Stripping `model`/`usage` keys the proxy itself writes (the
  set is rejected at load as exact paths); re-serializing; returning a copy
  of the unparseable input rather than the same slice (the SSE pointer
  fast-path depends on identity).
- **Observable**: Relayed body bytes; the fake upstream's untouched body.
- **Regression tests**: `internal/proxy/strip_test.go`, `e2e/strip_fields_test.go`.

### INV-INJ-04 — Thinking usage is opt-in, response-side, and fail-open

- **Description**: A `thinking-usage` block enriches existing client-facing
  usage objects under the API's native details field; absent or null means
  off, and the composed rewriter degenerates to the model rename.
- **Trigger**: A model with `thinking-usage` configured.
- **Expected**: Default-off is byte-identical traffic. Upstream-reported
  reasoning always wins.
- **Forbidden**: Fabricating a usage object where none exists; drawing the
  share more than once per request; letting the plan change across retries.
- **Observable**: Response usage bytes.
- **Regression tests**: `internal/proxy/thinking_test.go`, `e2e/thinking_usage_test.go`.

## 3. Candidate walk and recovery

The six states below are **distinct and must not be collapsed into a generic
error**. Any refactor that makes one of them unrepresentable has changed
behavior.

| State                           | Meaning                                      | May retry                      | May fall back   | May commit                |
| ------------------------------- | -------------------------------------------- | ------------------------------ | --------------- | ------------------------- |
| `DefinitelyNotSent`             | No request byte left this process            | yes                            | yes             | yes                       |
| `PossiblySent` (`send_unknown`) | A byte may have reached upstream             | no replay of the same exchange | yes, per policy | yes                       |
| `HTTPAnswerReceived`            | A status exists, `err == nil`                | per matrix                     | per matrix      | yes                       |
| `Committed`                     | A client-visible byte was written            | **never**                      | **never**       | —                         |
| `CallerCanceled`                | The caller's context ended                   | **never**                      | **never**       | only if already committed |
| `Terminal`                      | Policy or an envelope forbade further action | no                             | no              | yes                       |

### INV-REC-01 — An HTTP answer is never a transport failure

- **Description**: `StatusCode` set with `err == nil` is an answer. Every
  status, 429 and 5xx included, is one answer handed up to the handler.
- **Trigger**: Any upstream that returns a status line.
- **Expected**: The answer is retained and judged by the matrix; its status
  survives to the client.
- **Forbidden**: Treating an answered request as a dial failure; collapsing
  a later candidate's 429/503 into 502; a pool status-retrying internally
  (it has no policy, so it must hand the answer up).
- **Observable**: Final status; `upstream_http_<status>` envelope;
  `upstream_status` on the `upstream_http_error` event rather than on
  `provider_attempt_failed`.
- **Regression tests**: `TestMatrixPrecedenceIsExactStatusOverClass`,
  `TestMatrixProviderErrorPredicateOutranksStatusClass`,
  `TestStatusClassOfAndIsUpstreamError`, `TestDefaultMatrixClassRuleCoversUnlistedCause`,
  `e2e/recovery_test.go`.

### INV-REC-02 — `send_state` is derived from the failing wire operation

- **Description**: Only a `definitely_not_sent` failure may be replayed or
  moved to another egress member. A `send_unknown` one may already have
  reached the upstream.
- **Trigger**: Any transport-level failure.
- **Expected**: `dial`/`proxyconnect` ops, typed proxy-tunnel failures, TLS
  _certificate-verification_ failures, and bare refused syscalls are
  `definitely_not_sent`. Read/write ops, bare EOF, a timeout on an
  established connection, truncated bodies, and every other TLS handshake
  failure are `send_unknown`.
- **Forbidden**: Reading `ClassConnection` as "never connected" — it is a
  catch-all. A `send_unknown` failure being replayed to the same upstream.
- **Observable**: `send_state` on `egress_attempt_failed`; whether a retry
  occurs.
- **Regression tests**: `TestSendStateDialTimeoutIsNotSent`,
  `TestSendStateProxyTunnelFailureIsNotSent`, `TestSendStateRefusedIsNotSent`,
  `TestSendStateTLSCertificateRejectedIsNotSent`,
  `TestSendStateEOFAfterBodyIsUnknown`, `TestSendStateResetAfterBodyIsUnknown`,
  `TestSendStateTruncatedResponseIsUnknown`,
  `TestSendStateTLSHandshakeAlertIsUnknown`,
  `TestSendStateTLSEstablishedConnectionDroppedIsUnknown`,
  `TestSendStateTLSPlaintextPeerIsUnknown`, `TestSendStateRidesTransportEvidence`,
  `TestSendStateNotInheritedByLaterZeroDialAttempt`.

### INV-REC-03 — Commitment is monotonic and recovery-terminal

- **Description**: Once any client-visible byte is written, the walk is over.
- **Trigger**: The first byte of the relayed answer, including a status line.
- **Expected**: The candidate that produced that byte has produced THE
  response.
- **Forbidden**: Retry, fallback, or candidate switch after commitment; a
  second header block; an error body reaching a client already streaming; a
  synthesized terminal marker. A committed stream that dies mid-flight is
  truncated, never retried.
- **Observable**: Attempt count after the commit point; absence of
  `provider_attempt_started` post-commit; `StreamStats.Terminal`.
- **Regression tests**: `TestProviderChainStreamingCommitment`,
  `TestProviderChainStreamDeathAfterCommitment`,
  `TestStreamRecoveryWindowOwnsACommittedRelayCutIt`,
  `TestCredentialSSECommitEndsRotation`.

### INV-REC-04 — The last received answer wins, including retention

- **Description**: The relayed response is the last answer a candidate
  produced. A candidate whose budget ran out on a received answer, followed
  only by candidates failing before answering, relays that retained answer.
- **Trigger**: A walk where a later candidate fails without producing a
  status.
- **Expected**: The answering candidate becomes `final_provider`;
  `provider_exhausted` stays unset.
- **Forbidden**: 502 `upstream_unreachable` when a provider did answer; a
  capture failure on a retained answer being reported as unreachable — it is
  `upstream_invalid_response`.
- **Observable**: Final status; `final_provider`; absence of
  `provider_exhausted`; the retained answer's own egress kind.
- **Regression tests**: `internal/proxy/provider_fallback_test.go`,
  `e2e/provider_fallback_test.go`. **GAP**: the retained-answer-plus-later-
  dial-failure combination is asserted per-scenario but has no fingerprint
  assertion binding the whole observable shape.

### INV-REC-05 — The request envelope is absolute; the candidate envelope is not

- **Description**: A spent _request_ envelope ends the walk — no candidate
  can start another exchange. A spent _candidate_ envelope forbids only
  another exchange on that candidate; the walk may still fall back, because
  `EnterCandidate` opens the next envelope fresh.
- **Trigger**: Either envelope reaching its cap.
- **Expected**: A per-candidate number never pins a chain the operator
  configured to fall back from.
- **Forbidden**: Clamping a value above a cap instead of rejecting the file;
  deriving one envelope's state from the other;
  `request_exchange_budget_remaining` reporting the tighter of the two.
- **Observable**: `TestEngineRequestEnvelopeStopsTheWalk`,
  `TestEngineCandidateSpentRequestEnvelopeIsAlwaysTerminal`,
  `TestEngineExchangeEnvelopeStopsRetryingButNotTheWalk`,
  `TestProviderWalkSpentCandidateEnvelopeStillFallsBack`,
  `TestBudgetRequestRemainingIgnoresTheCandidateEnvelope`,
  `TestBudgetRequestRemainingFloorsAtZero`,
  `TestAcquireBothExchangeLimitsBindAtOnce`, `TestAcquireGrantsExactlyTheFinalExchange`,
  `TestAcquireRacesTheLastUnitOfALargerEnvelope`.
- **Observable**: `candidate_exchange_budget_spent` (rule `budget-candidate`,
  no strike, no `egress_attempt_failed`); terminal `budget-request` named
  post-walk.

### INV-REC-06 — Candidate max-elapsed is measured from the first attempt

- **Description**: `max-elapsed` is measured from the candidate's FIRST
  attempt and is checked BEFORE a wait is scheduled.
- **Trigger**: Any candidate with a `max-elapsed` window.
- **Expected**: The ceiling binds the whole candidate, retries and backoff
  included.
- **Forbidden**: Resetting the window per attempt; scheduling a wait that
  starts inside an already-spent window.
- **Observable**: Attempt count; wall-clock bound.
- **Regression tests**: `TestBudgetElapsedBoundaryIsSpentAtTheCeiling`,
  `TestBudgetElapsedEnvelopes`, `TestBudgetRemainingElapsedIsTheTighterEnvelope`,
  `TestBudgetRemainingElapsedReportsASpentWindowAsNonPositive`.

### INV-REC-07 — Retry waits; fallback moves immediately

- **Description**: A retried failure waits a bounded backoff (initial,
  doubling to a ceiling, ± jitter). A fallback moves with no inter-candidate
  wait. A retryable failure whose candidate budget is spent takes
  `retries.on-exhausted`.
- **Trigger**: A matrix disposition of `retry` or `fallback`.
- **Expected**: Fallback is immediate; retry is bounded.
- **Forbidden**: A wait before a fallback; an unbounded backoff; the
  retry budget being treated as request-wide rather than per candidate;
  `fallback.enabled: false` disabling same-candidate retries (it pins the
  candidate only).
- **Observable**: Attempt timestamps; attempt count.
- **Regression tests**: `TestProviderChainRetryBudgetIsPerCandidate`,
  `TestProviderChainRetryBudgetExact`, `TestEngineFallbackBudgetCountsCandidatesEntered`,
  `TestProviderChainBoundedByProviderAndEgressBudget`.

### INV-REC-08 — `Retry-After` can only raise a wait, within four ceilings

- **Description**: An upstream `Retry-After` may raise a wait but never past
  the backoff ceiling, the retry-after policy's `max-delay`, the candidate's
  remaining `max-elapsed`, or the caller's remaining deadline.
- **Trigger**: An answer carrying `Retry-After`.
- **Expected**: Whichever bound binds first wins; `max-delay` ceilings the
  directive, not the schedule.
- **Forbidden**: A wait past any of the four ceilings; `Retry-After`
  shortening a computed backoff.
- **Observable**: Wall-clock between attempts; the log's retry evidence.
- **Regression tests**: `internal/proxy/retryafter_test.go`. **GAP**: the
  four-way "whichever binds first" combination is unit-tested per-ceiling;
  no test binds two ceilings at once and asserts which wins.

### INV-REC-09 — Every attempt replays the immutable body through its own transform

- **Description**: Each attempt — a same-candidate retry included — rebuilds
  the request from the same immutable client body through that candidate's
  own transform.
- **Trigger**: A retry or a fallback.
- **Expected**: A fresh request each time; nothing observed on an earlier
  attempt feeds the next.
- **Forbidden**: A transform error costing budget — a body failing one
  candidate's transform fails all, so it answers 400 immediately.
- **Observable**: The fake upstream's recorded request bodies, one per
  attempt.
- **Regression tests**: `internal/proxy/provider_fallback_test.go`.
  **GAP**: the "transform once, reuse across same-candidate retries"
  optimization is _not_ currently in place; any future change to it must
  prove the upstream-received bytes are identical per attempt.

### INV-REC-10 — The matrix is deterministic and re-validates at every layer

- **Description**: Precedence is exact status > provider-error predicate >
  status class > failure cause > failure class. Equally-specific overlapping
  rules are rejected at load.
- **Trigger**: Any recovery policy with matching rules.
- **Expected**: The same file always produces the same disposition,
  independent of Go map iteration and YAML order.
- **Forbidden**: Leaving a disposition to whichever rule the runtime reaches
  first; a layer silently winning over a layer it contradicts.
- **Observable**: Disposition under a given observation; load-time rejection
  of a contradictory file.
- **Regression tests**: `internal/recovery` suite (94 test functions),
  including `TestMatrixPrecedenceIsExactStatusOverClass` and
  `TestResolveRejectsContradictoryBudgets`.

## 4. Egress, credentials, and the exchange window

### INV-EG-01 — Eligibility precedes scheduling, and a skip costs nothing

- **Description**: Static gates (the streaming gate, `max-body-bytes` against
  the outgoing post-injection body) are checked before any dial, then dynamic
  ones (health cooldown, concurrency permit).
- **Trigger**: A pooled candidate with some members ineligible.
- **Expected**: Only an eligible member is dialed.
- **Forbidden**: Dialing then rejecting — a 6 MB request must never become a
  413 from a 4.5 MB relay; a skipped member consuming a scheduler turn, an
  attempt, a strike, or budget.
- **Observable**: The fake upstream's per-member request counts;
  `egress_attempts`.
- **Regression tests**: `TestEgressPoolBodySizeGateRoutesBeforeSend`,
  `TestPoolSkippedMemberConsumesNoBudget`,
  `TestCopySSEBlankLinesAreBudgetFree`.

### INV-EG-02 — A pool never status-retries, and never moves between providers

- **Description**: Egress fallback moves a request between network paths to
  the SAME provider. Only the handler's walk moves between providers.
- **Trigger**: Any egress fallback.
- **Expected**: A 429/5xx is one answer handed up, never retried inside the
  pool.
- **Forbidden**: Status-retrying inside a pool; a provider change inside one.
- **Observable**: Per-member request counts; the handler's candidate path.
- **Regression tests**: `internal/transport/pool_test.go`,
  `internal/transport/pool_sched_test.go`, `e2e/egress_test.go`.

### INV-EG-03 — Lock order and no I/O under a lock

- **Description**: The scheduler mutex is the only outer lock over a member's
  health and limiter mutexes, and no network I/O happens under any of them.
  The chosen member's permit is acquired inside the selection step under the
  scheduler lock.
- **Trigger**: Concurrent requests against a shared pool.
- **Expected**: Two concurrent requests never both observe the same last
  permit; weighted scheduling is smooth WRR over currently eligible members.
- **Forbidden**: Nested locking in any other order; a dial, read or write
  under a member or scheduler lock.
- **Observable**: `-race` clean; distinct permit outcomes.
- **Regression tests**: `TestPoolNilBudgetDialsTheWholeChain`,
  `internal/transport/pool_sched_test.go`. **GAP**: the permit-acquisition
  race is exercised by the schedule tests but has no dedicated high-
  concurrency race test asserting two requests never share a permit.

### INV-EG-04 — Pool state is identity-keyed and instance-owned

- **Description**: Scheduler cursor, health, permits and leases live in the
  `Registry` keyed by pool identity. Eviction checks the state INSTANCE under
  the map key, never the key alone.
- **Trigger**: A reload that changes, keeps, or re-adds a pool identity.
- **Expected**: Unchanged policy stays warm; changed policy starts fresh; a
  leased state outlives eviction until the last request releases it;
  re-adding a retired identity builds a fresh generation.
- **Forbidden**: Resetting warm state on a no-op reload; evicting a state
  still in use.
- **Observable**: Request counts across a reload; the registry's generation.
- **Regression tests**: `internal/transport/pool_registry_test.go`,
  `internal/transport/pool_registry_retire_test.go`.

### INV-CRED-01 — Rotation state is per content-identity, not per name

- **Description**: `PoolKey()` hashes the provider name, the `Spec` and the
  rate limit, so byte-identical credentials on two providers are two rotation
  domains and a rate-limit-only change moves the key.
- **Trigger**: Two providers sharing credentials; a rate-limit change.
- **Expected**: The key, the content digest and the credential values are
  never logged; only the operator-chosen `id` from `[A-Za-z0-9._:-]` travels,
  as `upstream_credential_id`.
- **Forbidden**: A key or digest in a log line, metric label, or reload
  event; a credential value in an error, panic, or response body.
- **Observable**: The completion record's `upstream_credential_id`; the
  credential choices in a fingerprint.
- **Regression tests**: `internal/credential/guards_test.go` (parses source),
  `e2e/config_secret_sweep_test.go`, `internal/proxy/credential_test.go`.

### INV-CRED-02 — A failed auth does not mark or rotate

- **Description**: An upstream auth failure is classified, not treated as a
  credential-health event.
- **Trigger**: An upstream answering 401/403 to a candidate attempt.
- **Expected**: The failure is judged by the matrix like any other answer.
- **Forbidden**: Marking the credential unhealthy, or rotating off it, on an
  auth rejection.
- **Observable**: The credential chosen by the next attempt; absence of a
  rotation event.
- **Regression tests**: `TestCredentialAuthFailureDoesNotMarkOrRotate`.

### INV-CRED-03 — A committed stream ends rotation

- **Description**: Once an SSE answer commits, the candidate's credential
  rotation stops.
- **Trigger**: A committed SSE relay.
- **Expected**: No further credential acquisition for that request.
- **Forbidden**: Rotating a credential mid-stream.
- **Observable**: Credential choice across the commit point.
- **Regression tests**: `TestCredentialSSECommitEndsRotation`.

### INV-EX-01 — The exchange window reaches a buffered attempt through EOF

- **Description**: A pre-commitment exchange is bounded end to end: a stalled
  header and a stalled buffered body are the same window. Only
  `transport.HandoffStream`, called at a confirmed `text/event-stream` and at
  the verbatim 3xx/204/304 relays, hands a body out of it.
- **Trigger**: Any attempt before commitment.
- **Expected**: A window the transport cuts yields this proxy's own bound:
  `error_cause: exchange_elapsed` over `failure_origin: envelope`.
- **Forbidden**: Inferring the phase from the request's `stream` flag — a
  `stream: true` answered with `200 application/json` must still be bounded
  (re-opening that hole is the regression issue #96 closed). A window-cut
  body being reported as a peer read fault.
- **Observable**: `error_cause`/`failure_origin`; the walk's disposition.
- **Regression tests**: `TestDialWithinBoundsBufferedBodyThroughEOF`,
  `TestDialWithinCompletesBufferedBodyInsideTheWindow`,
  `TestDialWithinClosesTheBodyWhenTheWindowWinsTheCommit`,
  `TestDialWithinWindowBoundsThePreResponsePhase`,
  `TestDialWithinDisarmsTheWindowOnEveryPathThatEnds`,
  `TestDialWithinNoWindowLeavesTheDialUntouched`,
  `TestBufferedAnswerExchangeTimeoutOwnsTheCause`,
  `TestBufferedAnswerToStreamRequestHonoursTheEnvelope`,
  `TestDialWithinCommittedBodyOutlivesTheWindow`,
  `TestDialWithinReleasesOnceUnderPressure`.

### INV-EX-02 — Caller cancellation outranks the window

- **Description**: When both the caller's context and the exchange window
  would end an attempt, the cancellation owns it.
- **Trigger**: Caller cancellation concurrent with a window expiry.
- **Expected**: Outcome `client_disconnected`; the body is still owned and
  released.
- **Forbidden**: A window expiry reported as a caller cancel, or the
  reverse; a leaked body.
- **Observable**: Outcome token; `DialWithin`'s release count.
- **Regression tests**: `TestDialWithinCallerCancellationOutranksTheWindow`,
  `TestDialWithinCallerCancellationStillOwnsBufferedBody`.

### INV-EX-03 — The two envelope limits bind independently

- **Description**: `AcquireExchange` grants exactly the final unit and races
  the last unit of a larger envelope safely.
- **Trigger**: Concurrency against a nearly-spent envelope.
- **Expected**: The budget is the exchange truth; the count never exceeds its
  limit.
- **Forbidden**: Two dials claiming one unit; a budget consulted after rather
  than before the dial.
- **Observable**: `TestBudgetIsTheExchangeTruth`,
  `TestBudgetConsumesPerExchange`, `TestBudgetNeverExceedsItsLimitUnderConcurrency`,
  `TestAcquireWindowIsTheTighterEnvelopeTightest`,
  `TestBudgetNestsCandidateInsideRequest`,
  `TestProviderWalkRequestExchangeEnvelopeCountsRealPoolDials`,
  `TestPoolBudgetStopsTheLoopAfterOneExchange`,
  `TestPoolBudgetCapsTheWalkAtTwoExchanges`,
  `TestPoolBudgetRefusedBeforeAnyDialIsNotEndpointExhaustion`.

## 5. Streaming

### INV-SSE-01 — Streaming branches on the URL path, not the body

- **Description**: Chat is `data:` lines plus `data: [DONE]`; Responses is
  `event:` + `data:` pairs with no `[DONE]`.
- **Trigger**: Any relayed SSE answer.
- **Expected**: A Responses `event:` line passes through untouched; a
  `response.completed` data line is rewritten and stripped like any other
  payload.
- **Forbidden**: Treating a Responses stream as terminal at `[DONE]`, or a
  chat stream as terminal at `response.completed`.
- **Observable**: The client's received event sequence; `StreamStats.Terminal`.
- **Regression tests**: `TestCopySSEChatTerminalIsTheDONEDataLine`,
  `TestCopySSEResponsesTerminalIsTheCompletedEvent`,
  `TestCopySSETerminalIsSurfaceBlind`.

### INV-SSE-02 — A terminal marker must actually land

- **Description**: `Terminal` is set at the one place a terminal line is
  admitted; it is set once and sticks, and later bytes still relay.
- **Trigger**: A terminal line at the very end of a stream, or after
  near-miss lines.
- **Expected**: Terminal true only if the bytes reached the client.
- **Forbidden**: Treating a stream _ending_ as terminal; treating a read
  failure as terminal; terminal on a near-miss spelling.
- **Observable**: `StreamStats.Terminal`; the continuation loop's gating.
- **Regression tests**: `TestCopySSERecordsTheTerminalMarker`,
  `TestCopySSETerminalIsSetOnceAndSticks`,
  `TestCopySSETerminalIsForwardedOnceAndLaterBytesStillRelay`,
  `TestCopySSETerminalNeedsTheBytesToLand`,
  `TestCopySSETerminalNearMissesDoNotTerminate`,
  `TestCopySSETerminalMarkersWithEveryLineEnding`,
  `TestCopySSEStreamEndingIsNeverTerminal`,
  `TestCopySSEReadFailureIsNeverTerminal`.

### INV-SSE-03 — Framing handles every line terminator and chunk boundary

- **Description**: LF, CRLF and lone CR are all recognized; multi-line
  `data:` accumulates; fragmented network chunks reassemble.
- **Trigger**: Any SSE relay.
- **Expected**: The semantic frame sequence is independent of how the bytes
  were chunked.
- **Forbidden**: Requiring `\n`; losing a lone-CR line; a frame boundary
  derived from a read boundary.
- **Observable**: The client's received bytes.
- **Regression tests**: `internal/proxy/sse_test.go` (763 lines),
  `TestCopySSELoneCRReachesTheClientBeforeTheNextByte`,
  `TestCopySSEEventOverLimitTruncatesBeforeOffendingLine`.
  **GAP**: `internal/proxy/fuzz_test.go` exists; the arbitrary-chunk-boundary
  property is not yet asserted as a chunking-invariance property test.

### INV-SSE-04 — Size limits stop the relay before the client sees the line

- **Description**: `CopySSE` is bounded at 1 MiB per line and 2 MiB per
  in-flight event. A breach stops the relay with outcome
  `stream_limit_exceeded`.
- **Trigger**: An upstream emitting an oversized line or event.
- **Expected**: The offending line is never forwarded.
- **Forbidden**: Forwarding the truncated line; continuing the relay past a
  breach.
- **Observable**: `stream_limit_exceeded` outcome; the client's byte count.
- **Regression tests**: `internal/proxy/sse_limits_test.go`,
  `TestCopySSEEventOverLimitTruncatesBeforeOffendingLine`.

### INV-SSE-05 — Flush per event boundary; the rewrite gate is explicit

- **Description**: `CopySSE` flushes at each event boundary (blank line) and
  rewrites only the `data:` lines its gate admits — `"model"`, `"usage"`, or a
  configured strip key, the last re-derived per candidate from the strip
  list's first-segment patterns.
- **Trigger**: A chunk carrying only a to-be-excised key.
- **Expected**: Such a chunk still reaches the strip.
- **Forbidden**: Flushing mid-event; a gate so narrow the strip never runs.
- **Observable**: Client receive timing; the stripped body.
- **Regression tests**: `internal/proxy/sse_test.go`, `internal/proxy/strip_test.go`.

### INV-KA-01 — The keepalive is request-owned and never pings a dead stream

- **Description**: One ticker writes `: ping\n\n` only after an
  event-boundary silence interval; any forwarded byte resets it. It starts
  after headers commit, serializes with relay writes, is joined on every
  stream end or client disconnect, and is disabled immediately when
  `[DONE]` or `response.completed` is forwarded.
- **Trigger**: An SSE relay.
- **Expected**: No ping before commit, none after a terminal event, none
  concurrent with a relay write, and no goroutine left behind.
- **Forbidden**: A ping inside an in-flight event; a ping after terminal; an
  unjoined ticker.
- **Observable**: The client's received event sequence; goroutine count after
  the stream ends.
- **Regression tests**: `TestSSEKeepAlivePingsDuringUpstreamSilence`,
  `TestPingWriterStopsOnClientGone`, `TestPingWriterBoundaryRules`,
  `internal/proxy/keepalive_test.go`.

### INV-CONT-01 — Continuation is not a retry and never moves candidate

- **Description**: A committed SSE stream that ends without its terminal
  marker is re-asked of the SAME candidate, off by default, under a separate
  policy. The loop reads only `answer.cand.Recovery.Stream` off the frozen
  snapshot.
- **Trigger**: A marker-less committed stream with the feature enabled.
- **Expected**: A hop replays the same endpoint and route suffix, the same
  `transport.Doer` INSTANCE, and the same `*credential.Pool` INSTANCE with the
  walk's sticky key, resolved and never re-derived.
- **Forbidden**: Using `recovery.Engine` (whose commitment invariant correctly
  refuses a stream the client already holds); the matrix; a candidate switch; a
  cold pool acquisition after a concurrent publish.
- **Observable**: The fake upstream's request count and host/port; the hop
  count.
- **Regression tests**: `internal/proxy/continuation_test.go` (1664 lines),
  `internal/proxy/streamrecovery_test.go` (2173 lines),
  `TestStreamRecoveryHopForwardsTheSameRequestID`,
  `TestStreamRecoveryStopsAtASpentBudget`,
  `TestStreamRecoveryPooledBudgetRefusalIsAPhase`.

### INV-CONT-02 — The hop's body owns its dial's context

- **Description**: The context a hop dialed under must outlive
  `dialContinuation`; the hop wraps it in `boundBody`, whose `Close` is the
  release, and the header watchdog's cancel must not fire once the body is
  handed off. The window is created ONCE per logical session, at the commit.
- **Trigger**: A continuation hop; a header/body/timeout race.
- **Expected**: A per-hop window reset never happens; the interval is
  half-open (`now < deadline`) so a timer firing exactly on the instant
  latches the window and no later upstream byte revives it.
- **Forbidden**: A per-hop window reset letting the last lever outlive the
  moving `max-elapsed` silence allowance; a header watchdog firing after
  handoff; a timer reviving on a late byte.
- **Observable**: `streamrecovery_events_test.go` event vocabulary; hop
  outcome.
- **Regression tests**: `internal/proxy/streamrecovery_deadline_test.go` (739
  lines), `internal/proxy/streamrecovery_events_test.go` (1013 lines),
  `TestStreamRecoveryWindowOwnsACommittedRelayCutIt`.
  **GAP**: the exact interleavings (timer fires as headers arrive; timer
  fires as handoff happens; handoff concurrent with timeout; body Close
  concurrent with timeout) are exercised but not pinned as a dedicated
  deterministic race matrix.

### INV-CONT-03 — The safety gate is fail-closed and the loop never deletes text

- **Description**: Every data payload feeds `partialText`; tool/finish
  signals, non-JSON non-`[DONE]` data, text over `max-partial-bytes`, an
  empty prefix, or an inexpressible continuation body all make the stream
  unrecoverable.
- **Trigger**: A continuation-eligibility decision.
- **Expected**: A refusal ends the loop with the accumulator's reason.
- **Forbidden**: A heuristic that deletes client-visible text — a repeated
  paragraph at the seam is honest, overlap trimming is not. A second header
  block or error body reaching a streaming client. A synthesized terminal
  marker.
- **Observable**: The stream event sequence; the `phase`/`reason`/
  `unsafe_reason` vocabulary.
- **Regression tests**: `internal/proxy/continuation_test.go`,
  `TestPartialTextFinishReasonIsTerminal`,
  `TestPartialTextResponsesOutputTextDoneIsLogicalTerminal`,
  `TestPartialTextTerminalChunkIsStillRead`.

### INV-CONT-04 — Four bounds are refusals, never clamps

- **Description**: The safety gate, `max-recoveries`, `max-elapsed` (checked
  BEFORE scheduling), and the exchange envelope. Only upstream activity moves
  the window.
- **Trigger**: A hop reaching any bound.
- **Expected**: The loop stops.
- **Forbidden**: Client writes, SSE event boundaries, or this proxy's
  keep-alive ping moving the window; a clamp.
- **Observable**: `max_elapsed`, `client_write`, `upstream_limit` — all
  proxy-owned tokens, never worded as an upstream failure.
- **Regression tests**: `internal/proxy/streamrecovery_deadline_test.go`,
  `e2e/stream_reload_test.go`.

## 6. Usage metering

### INV-USG-01 — Capture is pre-rewrite and off the critical path

- **Description**: Raw upstream usage is captured BEFORE any response
  rewrite; absent usage stays SQL NULL, never zero. `Record` is
  fire-and-forget into a bounded queue.
- **Trigger**: Any request that built an outbound request and entered the
  provider path.
- **Expected**: Stripped fields are never metered; a database failure never
  delays or mutates a client response.
- **Forbidden**: Metering a request that never dialed — auth failures and
  local rejections carry no provider facts. A dropped event being reported
  as a zero-token event.
- **Observable**: The usage row; `usage_meter_final` totals.
- **Regression tests**: `TestUsageUnauthorizedNotMetered`,
  `internal/proxy/usage_test.go` (715 lines), `internal/usage` suite.

### INV-USG-02 — One usage event per request; hops do not double-count

- **Description**: A request emits exactly one usage event, including when it
  was assembled from several upstream responses by a continuation hop. Within
  one upstream call the LAST readable usage object wins, never a sum; across
  calls the capture is sealed at each pass boundary and each count combined by
  its own semantics.
- **Trigger**: Retries, fallback, or continuation hops.
- **Expected**: One row; no column contradicting another.
- **Forbidden**: A sum within one upstream call; a per-member unreadable count
  being a discarded object; two rows for one request.
- **Observable**: The usage event count and its columns.
- **Regression tests**: `internal/proxy/usage_test.go`, `internal/usage`
  suite. **GAP**: the "one event across a hop sequence" property is asserted
  per-scenario; there is no fingerprint assertion that counts events across
  a multi-hop stream.

### INV-USG-03 — The three `egress_attempts` are three different things

- **Description**: The LOG field is the relayed candidate's own pool report
  (absent when direct, NOT corrected on a retained walk); the usage event's
  `EgressKind` IS corrected to the retained answer; the usage COLUMN is the
  request-wide exchange total the log calls `upstream_exchanges`.
- **Trigger**: A walk with a retained answer.
- **Expected**: The three names never silently converge.
- **Forbidden**: Deriving one from another; letting a pooled attempt's fan-out
  change `provider_attempts`.
- **Observable**: The log fields and the usage row.
- **Regression tests**: `TestDirectClientPreservesTheExchange`,
  `TestHTTPProxyPreservesExchangeDetails`, `TestHandlerPoolExhaustionLoggedWithClass`,
  `internal/proxy/logging_test.go` (995 lines), `e2e/logging_test.go`.

### INV-USG-04 — A zero-dial walk is still a walk

- **Description**: A walk that entered a candidate but dialed nothing still
  carries its walk facts, because a policy decided it.
- **Trigger**: An envelope refusal before any dial.
- **Expected**: `final_provider`, `policy_hash`, `provider_exhausted` are
  present; `provider_attempts` does not drop to zero.
- **Forbidden**: Gating the walk facts on the exchange count; emitting
  `upstream_exchange` for a dial that never happened.
- **Observable**: `provider_attempt_started` carrying the index the logical
  counter already holds, with `provider_attempts` equal and
  `upstream_exchanges` unmoved.
- **Regression tests**: `TestProviderAttemptCounterEnvelopeRefusalAtDial`,
  `TestSendStateNotInheritedByLaterZeroDialAttempt`,
  `TestPoolBudgetRefusedBeforeAnyDialIsNotEndpointExhaustion`.

## 7. Errors and process lifecycle

### INV-ERR-01 — Upstream statuses are answers; 4xx/5xx are normalized and loud locally

- **Description**: Redirects are never followed. An upstream 4xx/5xx keeps
  its status and gets the canonical envelope; a bounded 64 KiB prefix is read
  once under a short fixed internal capture timeout to classify
  `error_shape` and fingerprint `error_fingerprint`.
- **Trigger**: Any upstream 4xx/5xx, or a 3xx/204/304.
- **Expected**: 3xx/204/304 forward verbatim with the header allow-list;
  4xx/5xx normalized to the canonical envelope with the status preserved.
- **Forbidden**: Following a redirect; raw provider bytes reaching the client
  or a log line; the capture prefix reaching anywhere else; configuring the
  capture timeout at runtime.
- **Observable**: Final status; exact body; `upstream_http_error` at WARN for
  4xx and ERROR for 5xx.
- **Regression tests**: `TestUpstreamErrorEnvelopeBytes`, `TestErrorEnvelopeWriteFailureOutcome`,
  `TestRelayOutcomeClassification`, `e2e/recovery_test.go`.

### INV-ERR-02 — An unusable answer is retryable, not fatal

- **Description**: A 200 that is not JSON, and an error-body read failure or
  stalled capture, are judged by the matrix like any other observation.
- **Trigger**: A malformed or truncated upstream answer.
- **Expected**: 502 `upstream_invalid_response` only when the walk finalizes
  on one, with outcome `upstream_invalid_response` for an unparseable or
  over-cap body and `upstream_read_failed` for a failed read or stalled
  capture.
- **Forbidden**: A caller cancel during either being reported as
  `upstream_invalid_response` — it is `client_disconnected`, with no envelope.
- **Observable**: Outcome token; the walk's attempt count.
- **Regression tests**: `TestErrorBodyStalledPastTheCandidateEnvelope`,
  `internal/proxy/upstream_error_test.go`.

### INV-ERR-03 — Dial failure is 502; an unmapped model is 404

- **Description**: A dial failure with no answering candidate is 502
  `upstream_unreachable`; an unmapped model is 404 `model_not_found`.
- **Trigger**: No candidate answered.
- **Expected**: 502 exactly when no candidate ever produced an HTTP response.
- **Forbidden**: 404 ever being forwarded upstream; 502 for a model lookup
  failure.
- **Observable**: Status and envelope.
- **Regression tests**: `e2e/models_test.go`, `e2e/recovery_test.go`.

### INV-LIFE-10 — Readiness turns false before the listener stops accepting

- **Description**: `/healthz` stays 200 while the listener exists; `/readyz` is
  200 only while ready. `Run` calls `beginDraining()` first, serves the
  `readinessPropagation` head start inside grace, then `Shutdown(grace-head)`.
- **Trigger**: SIGINT/SIGTERM; a cancelled entry context.
- **Expected**: `/readyz` reports 503 + state + `no-store` before the listener
  closes.
- **Forbidden**: A ready instance whose context is cancelled; a healthcheck
  that reads YAML.
- **Observable**: `/readyz` status; the container probe, which probes
  `GET /readyz` for `200` + `"ok\n"`.
- **Regression tests**: `TestReadyzDropsBeforeTheListenerCloses`,
  `internal/server` suite, `e2e/readiness_test.go`, `e2e/shutdown_lifecycle_test.go`.

### INV-LIFE-11 — One signal channel; a second signal exits 1

- **Description**: The first SIGINT/SIGTERM runs `Shutdown(grace)`, forces
  `Close()` on overflow, closes idle connections and drains accepted usage
  events before exit 0. A second signal exits 1; late signals cannot
  overwrite the drain's exit code.
- **Trigger**: Shutdown.
- **Expected**: Exit 0 after a clean drain.
- **Forbidden**: A late signal overwriting the exit code; a usage event lost
  to an early exit.
- **Observable**: Process exit code; `usage_meter_final` totals.
- **Regression tests**: `e2e/shutdown_lifecycle_test.go`,
  `internal/server` suite.

## 8. Secrets and observability

### INV-SEC-01 — No credential reaches a log, an error, or a metric

- **Description**: No `Authorization`, key, request body, injection prompt,
  usage DSN, raw provider body, or driver error is ever logged. Upstream
  credential VALUES from a provider `auth:` block are the same class of
  secret.
- **Trigger**: Every log line, error text, reload event, metric label, panic,
  and response body.
- **Expected**: Upstream URLs log scheme+host only; a rejection quotes
  position, length or line, never operator input; only the safe-charset
  `upstream_credential_id` travels.
- **Forbidden**: A DSN's length, since it embeds a password; a client id in a
  log line (CWE-117 plus attacker-controlled cardinality).
- **Observable**: The secret sweep over emitted log lines.
- **Regression tests**: `e2e/config_secret_sweep_test.go`,
  `internal/credential/guards_test.go`, `internal/proxy/logging_test.go`.

### INV-SEC-02 — Logging hot-reloads and never transitions silently

- **Description**: A top-level `log-level` key in the runtime YAML applies
  process-wide through the poller's `onPublish` hook, acknowledged with
  `log_level_applied` at the NEW level.
- **Trigger**: A reload carrying a valid `log-level`.
- **Expected**: The new level takes effect with no lock, signal or restart.
- **Forbidden**: A `LOG_LEVEL` environment variable; a silent transition,
  including `error`→`warn`.
- **Observable**: `log_level_applied`; the level of subsequent events.
- **Regression tests**: `internal/proxy/logging_test.go`, `e2e/logging_test.go`.

### INV-SEC-03 — Counter and token vocabularies are closed sets

- **Description**: `error_class`/`error_cause`/`failure_origin`/`send_state`/
  `disposition`/`reason` are closed sets; each event has exactly one owner.
- **Trigger**: Every emitted event.
- **Expected**: README "Provider recovery policy" → "Observability"
  vocabulary.
- **Forbidden**: A cause mapped from message text; `provider_attempt_failed`
  carrying a status; a builder token in a `reason` field instead of
  `phase: build` + `unsafe_reason`.
- **Observable**: The event stream.
- **Regression tests**: `internal/proxy/streamrecovery_events_test.go` pins
  the five recovery events; `e2e/logging_test.go` pins slugs and fields.

## 9. Config

### INV-CFG-01 — Two config planes, enforced

- **Description**: Bootstrap settings come from the environment and a runtime
  file defining one is rejected by strict decoding; everything else lives in
  YAML only as a single document. Every environment variable is
  `OAICR_`-prefixed with no unprefixed fallback.
- **Trigger**: Any load.
- **Expected**: Invalid initial config exits 1; an invalid reload keeps the
  last-known-good snapshot and logs with a boot-time hash baseline.
- **Forbidden**: A `---`-separated document; an unprefixed variable; a
  reload that clears a snapshot instead of keeping it.
- **Observable**: Exit code; the reload event; `config_generation`.
- **Regression tests**: `internal/config` suite (113 test functions).

### INV-CFG-02 — A database URL is infrastructure, and never logged

- **Description**: Empty `OAICR_AUTH_DATABASE_URL` = static mode; empty
  `OAICR_USAGE_DATABASE_URL` = metering off. Neither hot-reloads.
- **Trigger**: Startup; any logging.
- **Expected**: Partner mode opens, migrates and schema-validates the store
  at startup; metering off means no pipeline.
- **Forbidden**: Logging either DSN, not even its length; hot-reloading
  either; using the YAML `api-key` as a wire credential in partner mode.
- **Observable**: The secret sweep; startup outcome.
- **Regression tests**: `e2e/config_secret_sweep_test.go`,
  `internal/auth` suite, `internal/usage` suite.

### INV-CFG-03 — Legacy spellings normalize into the one engine

- **Description**: `provider`, `endpoint`, `retries` and `provider-fallback`
  normalize into the same policy structure; a legacy block paired with its
  `recovery` counterpart is rejected.
- **Trigger**: Loading a config using either spelling.
- **Expected**: Identical effective policy; the `provider`/`endpoint` forms
  build a one-candidate chain.
- **Forbidden**: A mode in which both run side by side; a legacy block
  silently winning.
- **Observable**: The walk's candidate path and policy hash.
- **Regression tests**: `internal/config` suite, `internal/recovery` suite.
