# Plan: `strip-fields` — narrow reserved keys from segments to exact proxy-written paths

## Context

`strip-fields` (issue #62, shipped in #63) excises provider-added members from
relayed 2xx responses. Its load-time validation rejects **any** path carrying a
segment equal to `model` or `usage`, at any depth — a per-segment `switch` in
`validateStripSegment`.

That rule is too broad for the case the feature exists for. Providers decorate
the `usage` object itself with billing/attribution metadata that the client did
not ask for — OpenRouter-style `usage.is_byok`, `usage.cost`,
`usage.cost_details.*`, `usage.prompt_tokens_details.cached_tokens`. Because
every path under `usage` is unreachable by configuration, the proxy cannot
remove them:

```
{"level":"fatal","error":"provider entry 1: strip-fields: must not include \"usage\"","message":"config_load_failed"}
```

`README.md` documents the blanket rule ("rejected in a strip list at any
depth"), so narrowing it is a deliberate contract change, not a bug fix.

Issue: ecoma-io/openai-compatible-injector#65.

## Settled design

Change the reserved-key rule from **segment-based** to **exact-path-based**.
Only the paths the proxy itself writes stay rejected — the strip runs LAST in
the composed rewriter (model rename → thinking-usage synthesis → strip), so
those are the only members configuration must not be able to carve a hole in:

| Rejected path                                              | Why the proxy owns it                                                                                                                                                                                                 |
| ---------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `["model"]`                                                | the model rename in every path, including the Responses envelope's `response.model` (the strip's single descent reapplies the full list inside the `response` object, so the exact path `model` covers both surfaces) |
| `["usage"]`                                                | the whole usage object                                                                                                                                                                                                |
| `["usage","completion_tokens_details","reasoning_tokens"]` | the Chat Completions thinking-usage synthesis target (`chatUsageShape`, `internal/inject/usage.go`)                                                                                                                   |
| `["usage","output_tokens_details","reasoning_tokens"]`     | the Responses thinking-usage synthesis target (`responsesUsageShape`)                                                                                                                                                 |

Everything else becomes allowed, in particular `usage.is_byok`, `usage.cost`,
`usage.cost_details.upstream_inference_cost`,
`usage.prompt_tokens_details.cached_tokens`, and a nested `model` member the
proxy never writes such as `a.model`.

Explicitly unchanged:

- the per-segment checks for an empty segment and a double quote;
- the `maxStripDepth` (8) ceiling, the `maxStripPaths` (16) count, the
  explicit-empty-list reject and the duplicate-path reject;
- every other behaviour of the feature — byte-preserving excision, object-only
  traversal, per-provider + per-model override, the SSE gate widening,
  strip-last ordering, meter-before-rewrite.

Accepted quiet-direction risk (documented, not blocked): an operator may list
the _parent_ of a synthesis target (`usage.completion_tokens_details`), which
excises the synthesized `reasoning_tokens` along with the provider's
`audio_tokens`/`image_tokens`. That parent is legitimate provider-added data
too, so it is allowed and documented; the blast radius is bounded by
`thinking-usage` being opt-in per model. The two exact synthesis _leaves_ stay
rejected so the direct case cannot be configured by accident.

Error messages stay fixed text and never echo operator input. `model` and
`usage` keep their existing messages (asserted in tests); the two synthesis
leaves get a new one — `strip-fields: must not strip the synthesized reasoning
tokens`.

## Units

Each unit leaves `go build ./... && go test ./...` green and is
squash-mergeable on its own.

### Unit 1 — Config plane (`internal/config`)

**Touch:** `internal/config/runtime.go`, `internal/config/runtime_test.go`.

Move the reserved decision out of `validateStripSegment` (which sees one
segment and no context) into a path-level validator called from
`ParseStripPath`, which holds the full segment list. The empty-segment and
double-quote checks stay in `validateStripSegment`; the path-level function
runs after the depth ceiling check so a too-deep path still reports depth.

**Tests:** `TestParseStripPath` accept list gains `usage.is_byok`,
`usage.cost`, `usage.cost_details.upstream_inference_cost`,
`a.model`, `'usage'.is_byok`; reject list keeps `model`, `usage` and gains the
two exact synthesis paths plus a near-miss (`usage.completion_tokens_details`)
that must be ACCEPTED. `TestLoadRuntimeStripFieldsValidation` gains the same
accept/reject cases through the YAML surface — including the near-misses the
old segment rule rejected (`a.model`, `a.usage`-style nested names).

### Unit 2 — Inject engine proof (`internal/inject`)

**Touch:** `internal/inject/strip_test.go`.

The engine is unchanged (it never knew about reserved keys) — this unit proves
the newly reachable paths excise correctly: `usage.is_byok` removed
byte-preservingly on both chat and responses while the rest of the `usage`
object (including `completion_tokens_details.reasoning_tokens`) survives
untouched, and a nested `a.model` is stripped while a top-level `model` is
left alone by that same list.

### Unit 3 — E2E (`e2e`)

**Touch:** `e2e/strip_fields_test.go`.

Black-box case through the real binary: a streamed chunk whose `usage` object
carries both `is_byok` and `completion_tokens_details.reasoning_tokens` is
configured with `strip-fields: [usage.is_byok]`; the client must see the chunk
without `is_byok` and WITH the sibling `reasoning_tokens` intact — proving a
sibling strip does not damage synthesis-adjacent data.

### Unit 4 — Docs, same pass

**Touch:** `README.md` ("The reserved keys" paragraph), `config.example.yaml`
(the `strip-fields` comment), `AGENTS.md` (the "Response field stripping"
bullet).

State the exact-path rule and name the four rejected paths.

## Verification

1. `gofmt -l .` — prints nothing.
2. `go vet ./...` — clean.
3. `go test ./internal/... -count=1` — green.
4. `go test -race ./internal/... -count=1` — green.
5. `golangci-lint run ./...` — repo pins v2.12.2; clean.
6. `go test ./e2e/ -count=1 -timeout 25m` (not `-short`) — green.
7. CI gates `ci-gate` and `analysis-gate` green on the PR.

## Files touched

- `internal/config/runtime.go`, `internal/config/runtime_test.go`
- `internal/inject/strip_test.go`
- `e2e/strip_fields_test.go`
- `README.md`, `config.example.yaml`, `AGENTS.md`
- `docs/plans/strip-fields-nested-usage-paths.md` (this file)
