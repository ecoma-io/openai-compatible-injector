# Refactor roadmap

How the injector gets from a 3556-line `serve` to a subsystem boundary
structure, without changing what it does.

`behavioral-contract.md` is the constraint set; this file is the plan against
it. The rule both files share: **a behavior mismatch that has not been
explained is not allowed to proceed.**

## 0. The problem this solves

`internal/proxy/handler.go` is 3556 lines. One function, `serve`, spans lines
368–2958 — roughly 2590 lines, or 73% of the file. It holds:

- request identity, method dispatch, the 405 path
- the snapshot load and every binding to it
- auth, body read, admission, transform, egress closure
- the candidate walk and the retry/fallback state machine
- commitment, buffered relay, SSE relay, verbatim 3xx/204/304 relay
- the usage capture and the completion record
- ~20 helper functions that are all part of the same closure

The concepts are entangled by **parallel variables rather than by types**. A
first read of lines 398–491 shows a `var` block carrying `outcome`, `stream`,
`publicModel`, `bytesIn`, `meterEvent`, `usageCapture`, `principal`,
`lastCand`, `ansEgressKind`, `egress`, `eng`, `budgetStopped`,
`lastPolicyHash`, `lastCredentialID`, `streamed`, `providerAttempts`,
`retriesTotal`, `finalProvider`, `providerExhausted`, `lastCandIndex` — twenty
variables that jointly encode "how far did this request get and what should be
reported about it".

Nothing enforces the pairing. `budgetStopped` set does not clear
`providerExhausted`. `ansEgressKind` and `egress` are two answers to "which
egress?" valid in different regimes, and which one wins is decided at the
point of use, not at the point of assignment. This is the specific thing the
refactor must fix — **not** the line count, and not the function length.

The KPI, stated once: _recovery and commitment semantics must be provable
without reading all of `serve`._ If after the refactor proving "no retry
happens after commit" still requires reading 2590 lines in order, the
refactor failed even if the file is shorter.

## 1. Ground rules

These are not style preferences; each one exists because breaking it produces
the failure mode this initiative is guarding against.

1. **Baseline first.** Every island starts from a green §0 baseline in
   `behavioral-contract.md`. A red baseline is a rollback trigger, not a
   starting condition.
2. **Contract before code.** An island whose invariants are not yet written
   into the contract does not start.
3. **One island per pull request.** Never two subsystems restructured in one
   diff — a review that cannot attribute a change to a cause cannot verify it.
4. **Characterize before restructure.** New tests land in an earlier pull
   request than the refactor they protect. A test written after the refactor
   describes the refactor, not the behavior.
5. **No test edits to make a refactor pass.** A failing test after a refactor
   is evidence, not noise. The sequence is: reproduce, compare against
   `main`, read the contract, decide whether the current behavior is intended.
   Only then does either the code or the contract change.
6. **No `sync.Pool`, no dropped cleanup, no collapsed error state** as a
   performance or tidiness measure.
7. **Every PR is one of SAFETY, REFACTOR, HARDEN, OPTIMIZE** and carries the
   section list in `CONTRIBUTING.md`. A REFACTOR that changes behavior is a
   bug in the refactor, not a hybrid PR.

## 2. The safety net comes first

Islands 1–3 of the default order (`inject`, `config`, `SSE`) are low-risk
precisely because they are pure. The islands that matter — 4 through 9 — touch
the walk, and they are the ones that cannot be verified by scenario tests
alone.

So the net lands first, as SAFETY pull requests that change no production
code:

| #   | Deliverable                                                                                                                                                                                                                                                                                   | Unblocks     |
| --- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------ |
| S1  | Fault-injection harness: script `DialFail`, `DialTimeout`, `WriteFailBeforeSend`, `WriteFailAfterPartialWrite`, `ReadHeaderTimeout`, `HeaderReceived`, `BodyPartial`, `BodyTruncated`, `RSTAfterHeaders`, `SSEPartial`, `SSEMalformed`, `SlowBody`, `ClientCancel` across successive attempts | islands 4–9  |
| S2  | `ExecutionFingerprint` over observable behavior only, and a golden scenario suite                                                                                                                                                                                                             | every island |
| S3  | Differential runner: same request, snapshot, upstream script, credential state, egress state and timers against a pre- and post-refactor build                                                                                                                                                | islands 4–9  |
| S4  | Tripwire tests for every `GAP` in the contract                                                                                                                                                                                                                                                | every island |
| S5  | Benchmark and profile baseline                                                                                                                                                                                                                                                                | island 10    |

**Fingerprint shape** — deliberately excludes anything without semantic
value. No pointers, no goroutine identity or ordering, no wall-clock
timestamps, no map iteration order, no object addresses.

```
Fingerprint {
  status, headers, bodyDigest
  candidatePath, attempts, sameCandidateRetries, fallbacks
  credentialChoices[]   // by key, never by value
  egressChoices[]
  outcome, usageEventCount, commitPoint, streamEventSeq[]
}
```

`credentialChoices` records the configured key `id`, never the value. A golden
file containing a plaintext secret is a defect.

**Tripwires required** (contract `GAP` entries): the `GAP`s under INV-LIFE-08,
INV-INJ-02, INV-REC-04, INV-REC-08, INV-REC-09, INV-EG-03, INV-SSE-03,
INV-CONT-02, INV-USG-02. Each is a test whose failure mode is the invariant
leaving the code — the test is the proof that the invariant is load-bearing.

## 3. Island order

The default order is kept. It runs pure-to-impure, which means the cheapest
verifiable work builds the reflexes and tooling for the expensive work.

| #   | Island                                | Risk       | Why here                                                                                                               |
| --- | ------------------------------------- | ---------- | ---------------------------------------------------------------------------------------------------------------------- |
| 1   | `inject` transforms                   | low        | Pure functions, already byte-preserving. Characterization plus fuzz first; any optimization needs allocation evidence. |
| 2   | config normalization                  | low-medium | Separate representation → validation → normalization → snapshot. Effective semantics must not move.                    |
| 3   | SSE parser                            | medium     | Extract `FrameReader`/`Frame`/`SSEState` from HTTP orchestration. Byte-level semantics are the whole risk.             |
| 4   | `AttemptResult` / `CandidateExecutor` | **high**   | The parallel-state fix. The keystone.                                                                                  |
| 5   | recovery orchestration                | high       | Engine consumes `AttemptResult`, returns a disposition. `internal/recovery` stays pure.                                |
| 6   | response commitment                   | high       | Make commitment a type, not an ordering.                                                                               |
| 7   | exchange lifecycle                    | high       | Two-phase exchange made explicit, races pinned.                                                                        |
| 8   | stream continuation                   | high       | Split into `internal/stream/`. Ownership, not LOC.                                                                     |
| 9   | handler composition                   | medium     | `serve` becomes orchestration.                                                                                         |

The ordering is defensible on one rule: **a refactor is only as safe as the
evidence that the thing being refactored is understood, and evidence is
cheapest to gather for pure code.** Island 4 is where the real risk lives,
which is why S1–S4 exist before it rather than alongside it.

Deviation from the default order is permitted with evidence, not for
convenience — the Architecture Auditor's coupling ranking is the tiebreak.

## 4. Per-island protocol

Every island, without exception:

1. **Read** the island's contract entries and every test named in them. Re-run
   those tests on `main` first and record the result.
2. **Characterize** anything the contract does not already pin. Land it as a
   separate SAFETY commit.
3. **Baseline** the differential fingerprint for the island's scenarios, and
   the benchmark numbers if the island is on a hot path.
4. **Refactor** one island. New code, not edited-in-place logic, where the
   extraction allows it — a moved function that is textually identical is
   trivially reviewable; a rewritten one is not.
5. **Verify**: `go test ./...`, `go test -race ./...`, `go vet ./...`,
   `golangci-lint run ./...`, the e2e suite, the golden suite, the relevant
   fuzz targets, and the differential runner against the pre-refactor build.
6. **Adversarial review** by a reviewer that did not write the change, against
   the ten questions in §6.
7. **Land** only when every answer is "no change" or a change that is
   explicitly intended and documented.

Green unit tests are necessary and not sufficient. A refactor that silently
drops a retry passes every test that never counted retries.

## 5. Per-PR template

Every pull request in this initiative carries:

```markdown
## Invariants affected

Contract IDs touched, and whether each is preserved or intentionally changed.

## Behavior intentionally unchanged

The specific things a reviewer should expect NOT to see move.

## Tests protecting behavior

Contract IDs → the test that would fail if this PR broke them.

## Adversarial scenarios considered

The negative space: what this PR must not make possible, and how it was checked.

## Performance impact

Measured, or explicitly "not measured — no hot path touched".

## Resource/lifecycle impact

Goroutines, buffers, reservations, leases, and their release paths.

## Rollback condition

The observable that means revert.
```

A pull request that changes semantics says so in **Invariants affected**, in
the past tense of what changed, and links the contract amendment. It does not
present a behavior change as a refactor.

## 6. Adversarial review questions

The reviewer answers all ten. Any "significantly uncertain" answer blocks the
merge.

1. Did any invariant become implicit again?
2. Did any semantic distinction get merged — two of the six attempt states
   into one, a policy decision into a default, a refusal into a clamp?
3. Did any state ownership change — who creates it, who releases it, who may
   outlive what?
4. Did any request lifecycle change?
5. Did any retry or fallback condition change — including one that now fires
   _less_ often and is therefore invisible in a happy-path test?
6. Did any commitment boundary move — by a microsecond, or by one buffer?
7. Did any cancellation behavior change, including who wins a race?
8. Did any memory ownership change — a reservation, a buffer, a lease, a
   goroutine?
9. Did any usage event change — count, timing, or a column's value?
10. Did secret reachability increase, even transiently, even in a test?

## 7. Hardening

Runs after the structural work, because hardening a moving target is wasted
effort. Two classes, never mixed.

**Behavior-preserving**: reduce secret reachability (including whether raw
URL userinfo and passwords outlive their need in memory); structured error
classification; log redaction; hash-based internal identity where uniqueness
survives; invariant assertions at boundaries; goroutine lifecycle checks;
bounded internal metadata.

**Behavior-changing**: lowering a body limit, bounding a provider/model/prompt
length, changing a timeout, a status, an error format, or the overload
behavior. Each needs a reason, an impact statement, a compatibility
assessment, a test and documentation, and lands in its own `HARDEN` pull
request. None of them rides along in a `REFACTOR`.

`sync.Pool` is not a default answer. Any pooled buffer must be proven not to
retain request bodies, injection prompts, or credentials, and must be
benchmarked with a heap profile plus a large-body and a cancellation test.

## 8. Optimization

Only after islands 1–9 are stable, and only where S5 recorded a measurement.
Candidate work, in the order the measurements are expected to justify it:

- **transform reuse** — a same-candidate retry re-transforms the same body
  today. Reuse must prove the upstream-received bytes are byte-identical per
  attempt, and must not cross a boundary where model, transport or strip
  semantics differ.
- **JSON transformation** — only after characterization and fuzz.
- **Postgres usage flush** — a bulk update or CTE instead of a per-key
  `UPDATE`, if round trips are a measured bottleneck. Aggregation, ordering,
  durability expectations, shutdown behavior and retry/error handling all stay
  as they are. A `Close(ctx)` semantic mismatch between context cancellation
  and underlying shutdown is its own change, verified separately, never mixed
  into an unrelated refactor.
- **pool scheduler** — no locking-architecture change without a contention
  profile. Atomics because they look faster is not a justification.

A latency or memory regression beyond measurement noise is a rollback
condition.

## 9. Rollback conditions

Any one of these reverts the island:

- final status, response body or header semantics change unintentionally
- candidate path, retry count or fallback count changes
- credential or egress selection changes
- the commitment point moves
- the stream event sequence changes
- continuation behavior changes
- caller-cancellation semantics change
- a request's snapshot generation changes mid-flight
- a usage event count changes
- a race-detector report appears
- a goroutine or lease leak
- memory grows beyond measurement noise
- latency or CPU regresses significantly

"Rare edge case" is not a reason to accept a mismatch during a refactor. If a
mismatch appears in a path no test covers, that is a coverage gap to close
first — the mismatch is still a mismatch.

## 10. Island log

One entry per island, written when the island closes. An island whose
deliverable turned out to be characterization rather than a code change says
so, and records the evidence that decided it — otherwise a later reader cannot
tell "nothing to do here" from "never looked".

### Island 1 — `inject` transforms: closed, no refactor

Steps 1–3 and 5–6 ran; step 4 has no work to do, and the reason is the
allocation evidence rather than a preference for leaving code alone.

**What was pinned.** Two SAFETY-only commits, no production code touched.
`rewrite_property_test.go` closes the INV-INJ-02 GAP with a corpus of
well-formed bodies asserted by DELETION — every in-scope `"model"` value is cut
from input and output and the remainders must be byte-identical, so the
invariant is stated directly instead of against a recorded answer.
`strip_property_test.go` closes INV-INJ-03 with four properties over a ~35-case
corpus: the output is a SUBSEQUENCE of the input (which rules out
re-serialization in one check), a second pass is a byte-for-byte no-op, equal
bytes are the SAME SLICE, and a valid body stays valid. Both were
mutation-verified before landing — re-serializing, returning an equal copy,
widening the strip scope into `response`, and dropping a removal each failed a
named test, with the sources reverted pristine afterwards.

**The measured baseline** (`-benchtime 200ms -count 3`, 16-core):

| Body       | `rename_only` | allocs | `rename_strip` |
| ---------- | ------------- | ------ | -------------- |
| 1 KB       | 4.4 µs        | 4      | 9.1 µs         |
| 64 KB      | 245 µs        | 4      | 490 µs         |
| 1 MB       | 3.8 ms        | 6      | 7.9 ms         |
| 8 MB       | 29 ms         | 8      | 59 ms          |
| 64 MiB cap | 245 ms        | 16     | 457 ms         |

Allocation is flat in body size — 4 allocs at 1 KB and 8 at 8 MB, a 16× body
for two more allocations, and the only size-proportional term is the single
output copy the transform must produce. `rename_only` runs at 266–306 MB/s;
the strip roughly halves throughput, which is the cost of the second
byte-level pass, and the no-match strip gate allocates zero times. There is no
quadratic term, no per-member allocation, and no redundant copy to reclaim, so
"any optimization needs allocation evidence" resolves to: the evidence shows
none available. The 17–20 allocs on the `rename_think_strip` row belong to the
thinking synthesizer's own `encoding/json` work, not to the byte-preserving
transforms.

**Why no extraction.** The two rewriters are already the shape step 4 asks for:
`RewriteChatModel` and `RewriteResponsesModel` are one-line wrappers over a
single `rewriteModel(body, public, nestedResponse)`, sharing one scanner with
the scope as a parameter. There is no duplicated logic to separate and no
boundary to move. `StripChatFields`/`StripResponsesFields` are the same pair of
wrappers. A rewrite here would be churn against the §1 ground rules, and the
island-ordering rule in §3 — a refactor is only as safe as the invariants it
preserves — has nothing to preserve here that is not already structural.

**Verification.** `go test ./...`, `go test -race ./...`, `go vet ./...` and
`golangci-lint run ./...` (0 issues) all green; the four inject fuzz targets
(`FuzzProbe`, `FuzzRewriteModel`, `FuzzStripFields`,
`FuzzSynthesizeThinkingUsage`) pass a 10s run each; and the differential runner
against the pre-refactor build `d247969` passes all 12 scenarios with none
skipped, including the three that exercise these transforms end to end
(`TestDiffChatInjected`, `TestDiffResponsesBuffered`, `TestDiffStreamChat`,
`TestDiffStreamResponses`).

### Island 2 — config normalization: closed, no refactor

The deliverable named in §3 is "separate representation → validation →
normalization → snapshot". Those boundaries already exist, and the evidence
that they do is structural rather than stylistic.

`LoadRuntime` is the pipeline, and its phases run strictly in sequence, each
consuming only the previous one's output: the top-level key allow-list and the
strict `KnownFields` decode (representation), the second-document probe
(document discipline), `buildTransports` → `buildProviders` →
`buildGlobalRecovery` → `recovery.Resolve` (normalization), and the sorted,
deduplicated model loop that assembles the `Snapshot`. Every `build*` function
has a single caller and calls only deeper `build*`s — the call graph is a DAG
with no mutual recursion and no shared mutable state between phases.

That is the specific thing the refactor exists to fix, and this package does
not have it. §0's problem statement for the initiative is concepts "entangled
by parallel variables rather than by types"; here the phases are separated by
function call direction, there are no parallel variables, and no phase writes
anything another phase reads. Splitting a DAG into more files would add
indirection and remove a boundary that is already unambiguous.

The 882-line `recovery.go` is the one file large enough to look like a target.
It is 18 single-purpose builders in one direction, each with one caller, and
splitting it would move code without changing what decides it.

**What was pinned.** One SAFETY commit, no production code touched.
`sanitize_property_test.go` closes the INV-CFG-01 gap by stating the redaction
property over the SHAPE of a document rather than a hand-picked list of
positions: the marker is planted in 27 structural slots and no rejection may
quote it. The positive half is pinned too, since a sanitizer returning a fixed
string satisfies the negative half while leaving an operator unable to find the
line they broke.

**A finding, recorded rather than papered over.** Mutation verification showed
that deleting the `yaml: invalid map key` entry from `inputEchoingPrefixes` is
a SILENT no-op — every test in the package stays green. The entry is
unreachable from `LoadRuntime`: a malformed map key surfaces as a
`yaml.TypeError`, which the other mechanism redacts by extracting line numbers
before the prefix table is consulted. The natural coverage case for that entry
therefore passed for the wrong reason. The entry is kept as defence against a
yaml.v3 that does emit it, and `TestInputEchoingPrefixesAreReachable` names it
as unreachable so a future reader does not mistake it for coverage. This is
also the clearest illustration of why the corpus property is the primary
defense: a whitelist cannot detect an entry it is missing, only one it has.

Three mutations were each caught by the test written for them: dropping a
reachable table entry (corpus case fails by name), bypassing the TypeError
redaction (three positions fail), and keeping the redaction while discarding the
line numbers (only the positioning test fails — which is exactly why it
exists).

**Verification.** `go test ./...` and `go test -race ./internal/config/` green;
`go vet` and `golangci-lint run ./internal/config/` clean; `FuzzLoadRuntime`
passes 20s (247k executions, no crash); the differential runner against
`d247969` passes all 12 scenarios. Config is not on the per-chunk hot path —
it is loaded once per reload — so no benchmark baseline was required by §4
step 3, which scopes that to hot-path islands.

## 11. Done

The initiative is complete when the correctness, architecture, security,
performance and operations conditions in the initiative brief are all met —
in particular when recovery and commitment semantics are provable from the
contract and the subsystem boundaries, without reading the whole handler.
