# Plan: `strip-fields` — xoá field theo JsonPath khỏi response của provider

## Context

Kilo provider luôn kèm các field thừa (`provider`, `service_tier`, ...) trong response. Người vận hành muốn cấu hình để proxy loại bỏ các field này trước khi relay cho client, bằng **JsonPath dotted** (`.` phân cấp) và đoạn segment đặt trong dấu nháy đơn (`'parent-name'.child`) để chứa key có ký tự đặc biệt. Config đặt ở **per-provider + per-model** (model override provider). Traversal **không descend qua array** — path chỉ đi qua object.

Yêu cầu của repo: byte-preserving trên response path (không unmarshal+marshal), same-slice no-op contract (SSE fast path `&out[0] == &payload[0]` phụ thuộc vào đó), `json.Valid` gate, scope theo API (chat = top-level; responses = top-level + descend 1 cấp vào `response`), meter observe pre-rewrite.

## Quyết định thiết kế (đã chốt với user)

| Decision                     | Choice                                                                                                                                                            |
| ---------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Phạm vi config               | `providers.<name>.strip-fields` + `models.<name>.strip-fields`; model override provider khi model CÓ block; model không có block thì kế thừa provider             |
| Định dạng path               | JsonPath dotted: `parent.child`; segment có key đặc biệt đặt trong nháy đơn: `'parent name'.child`, `'A'` (decode escape); `'...'` chứa `.`/dấu nháy              |
| Xử lý array                  | Object-only traversal — path qua mảng = no match tại đó; không có `[*]`                                                                                           |
| Scope theo API               | chat: top-level object; responses: top-level + 1 descent vào `response` object (giống model/usage rewrites)                                                       |
| Duplicate key trong response | Xoá MỌI occurrence (duplicate còn sót = leak)                                                                                                                     |
| Reserved keys                | `model` / `usage` bị cấm trong list (strip xoá data proxy tự áp dụng: rename + synthesis/meter)                                                                   |
| Order trong `rewriteOut`     | rename model → thinking synthesis → strip (strip chạy trên bytes cuối)                                                                                            |
| SSE gate                     | Extension: `stripKeys [][]byte` param; gate = payload chứa `"model"`/`"usage"` **hoặc** bất kỳ `stripKeys`; `nil` → không đổi hành vi/hiệu năng cho deployment cũ |
| Explicit `strip-fields: []`  | Reject (`requires at least one field`); absent/null = off                                                                                                         |
| New log event                | Không — bytes tự thể hiện; theo nguyên tắc metadata-only                                                                                                          |

## Các đơn vị triển khai (từng unit chạy `go build ./... && go test ./...` xanh, squash-mergeable)

### Unit 1 — Config plane (`internal/config`)

**Touch:** `internal/config/runtime.go`, `internal/config/snapshot.go`, `internal/config/runtime_test.go`, `internal/config/sanitize_test.go`, `config.example.yaml`, `README.md` (config reference).

1. `runtime.go` `runtimeModel` (sau `ThinkingUsage`, ~line 298) và `runtimeProvider` (sau `Recovery`, ~line 158-162):
   ```go
   StripFields []string `yaml:"strip-fields"`
   ```
   `KnownFields` strict decode tự nhận field mới; không cần đổi decode.
2. `runtime.go` — validator chung, cạnh `buildThinkingUsage`:
   ```go
   const maxStripFields = 16
   func buildStripFields(raw []string) ([]string, error)
   ```
   Validation (fixed text, không echo value):
   - `nil` → `nil` (off). `[]` → `"strip-fields requires at least one field"`.
   - Từng entry: trim; rỗng → `"strip-fields: path must not be empty"`; duplicate → `"strip-fields: duplicate path"`; `"model"` → `"strip-fields: must not include \"model\""`; `"usage"` → `"strip-fields: must not include \"usage\""`; `"` (double quote, không nháy đơn) trong segment → `"strip-fields: segment must not contain a double quote"`.
   - `len > maxStripFields` → `"strip-fields: too many paths (maximum 16)"`.
   - Parse path bằng **shared helper** (dùng `strconv.Unquote` cho `'...'`):
   ```go
   // trong internal/inject (hoặc config) — parse dotted path "a.'b.c'.d" -> []string{"a","b.c","d"}
   // validate: non-empty segment, "model"/"usage" cấm, " không hợp lệ
   func ParseStripPath(s string) ([]string, error)
   ```
   **Quyết định vị trí:** để helper trong `internal/inject` (engine cần segment list; config import inject tránh vòng lặp? — kiểm tra: `config` hiện import `transport`/`recovery`, **không** import `inject` (AGENTS.md: `inject` là pure transform). → Helper đặt trong `internal/inject`; `internal/config` gọi `inject.ParseStripPath` OK vì `inject` không import `config`... **Kiểm tra lại:** `inject` import `config` (thinking.go dùng `config.ThinkingUsage`). → Vậy **không** được để config import inject (cycle). → Helper đặt **trong `internal/config`** (parse path vocabulary là config surface), `internal/inject` nhận `[][]string` (đã split) thay vì raw strings.
   - Kết quả: `buildStripFields` trả `[]string` (raw paths đã trim, giữ thứ tự) — parse deep thực hiện ở config để validate, nhưng **lưu raw string** trên snapshot; `inject` parse lại khi request? No — tránh parse 2 lần: xem lưu dạng đã split. Xem "Data model" dưới.
3. `buildModel` (~line 1112): `sf, err := buildStripFields(rm.StripFields)`; đặt vào `Model`. `buildProviders` (~line 858): cùng helper → `providerEntry`. **Xử lý candidate chain:** mỗi candidate cần biết provider entry của nó. `providerEntry` hiện có `endpoint/transport/recovery`; thêm `strip [](path)` vào `providerEntry`, `buildModel` chạy vòng `chain[i]` set `chain[i].Strip` từ `providers[chain[i].Provider]` rồi model-override (`modelStrip` nếu có). Legacy inline form không có provider → strip chỉ từ model.
4. `snapshot.go`:
   ```go
   // path segment list type (config-owned)
   type StripPath struct { Segments []string }  // đã parse
   // trên Candidate (per-provider, object-only traversal, không array)
   Strip []StripPath
   // trên Model (đã override, đã merge — model items trước, nếu model không có thì provider items của candidate)
   Strip []StripPath
   ```
   **Merge semantics:** `Model.Strip` = `[]` nếu model CÓ block (model list thay provider list), nếu model KHÔNG có block thì mỗi `Candidate.Strip` lấy từ provider của nó. Handler dùng: `m.Strip` khi model có block, nếu rỗng-thì-model-chưa-set → dùng `cand.Strip` của candidate **đang** relay. **Làm rõ:** với chained model (không model block) mà các provider khác nhau → strip khác nhau theo candidate. Handler hiện mutate `m.Provider = cand.Provider` per candidate → thêm `m.Strip = cand.Strip` (model-level flatten theo candidate đang chạy). TODO commit ghi rõ.
5. `config.example.yaml`: thêm block `strip-fields` active (vd `echo-model` hoặc model kiểu Kilo) + comment. `TestExampleConfigLoads` cover tự động.
6. `README.md`: bullet `strip-fields` sau `thinking-usage` metric rows (~line 78); thêm vào strict-schema key list (~line 321-323); behavior section ở response rewriting.

**Tests (Unit 1):** `runtime_test.go` `TestLoadRuntimeStripFieldsValidation` (reject: `[]`, empty/whitespace path, duplicate, `model`, `usage`, segment chứa `"`, 17 items, non-string item) + `TestLoadRuntimeStripFieldsNormalization` (absent/null → nil; giữ thứ tự; model-override-provider; chained candidates mang strip riêng). `sanitize_test.go`: case path marker trong duplicate-reject → error chỉ ordinal, không tên. `ParseStripPath` unit tests: `a.b`, `'a.b'.c`, `'a''b'.c` (escaped quote), empty segment, chỉ `'` string.

### Unit 2 — `internal/inject` strip engine (new `internal/inject/strip.go`)

```go
func StripChatFields(body []byte, paths [][]string) []byte
func StripResponsesFields(body []ody []byte, paths [][]string) []byte
func StripPatterns(paths [][]string) [][]byte   // precomputed `"first-segment"` gates; nil when paths empty
```

**Scope:** chat = top-level object; responses = top-level + descent 1 cấp vào `response` object (`descendResponse` flag). **Object-only traversal**: một path match khi mọi segment đi qua OBJECT; gặp array/không-phải-object tại segment → path đó no-match tại đó.

**Algorithm** (inverse của `headMember` insertion; reuse `span`, `isKey`, `skipValue`, `collectionEnd`, `skipWS`, `valueEnd`, `splice`, `edit` từ `rewrite.go`/`usage.go`):

1. `if len(paths) == 0 || !json.Valid(body) { return body }` (gate load-bearing).
2. Cheap gate: nếu không byte `"first-segment"` nào xuất hiện trong body → return body (same slice).
3. `scanStripEdits(body, 0, len(body), paths, &edits, descendResponse)` — member loop nhưng mỗi object trong scope: match segment đầu tiên với các key đang ở depth hiện tại; khi khớp và `len(segments) == 1` → excise member; khi khớp và còn segment → object-value thì descend (đệ quy), **giới hạn depth** (vd `maxStripDepth = 8` — path sâu vô hạn là DoS) — nếu value là array → skip (object-only).
4. Excision hàng thủ công:
   - member có trailing comma → `edit{keyStart, commaEnd}`;
   - last member có member trước → `edit{commaPos, valEnd}`;
   - sole member → `edit{keyStart, valEnd}` (→ `{}`);
   - **merge overlapping/adjacent edits** (next.start <= cur.end → coalesce) — cần cho 2 member xoá liền nhau.
5. `if len(edits) == 0 { return body }` (same slice); `splice(body, edits)`.
6. Duplicate key trong object: xoá MỌI occurrence.

**Tests (Unit 2):** new `internal/inject/strip_test.go` (kiểu `rewrite_test.go`/`usage_test.go` tables):

- Excision vị trí first/middle/last/sole, whitespace-heavy, all-removed → `{}`.
- Kilo shape: `{"model":"x","provider":"kilo","service_tier":"x","choices":[]}` → junk.
- Nested: `{"a":{"provider":"x"},"provider":"y"}` strip `a.provider` chỉ xoá nested; **không qua array** (`choices[].provider` no-op); responses `response.provider` descend; `response` `metadata.provider` (2 cấp) no-op cả hai API.
- `'parent'.child`, `'a.b'.c`, escape decode.
- Duplicates, decoys ("provider" trong string value), no-op slice-identity (empty paths, absent, invalid JSON, truncated, array doc, scalar doc).
- `StripPatterns`: nil on empty.
- Fuzzy (`fuzz_test.go`): `FuzzStripFields` — invalid byte-identical; valid output valid; no-key-bytes identical; chat/responses agree khi không top-level `"response"`.

### Unit 3 — Proxy wiring (`internal/proxy`)

**Touch:** `handler.go`, `sse.go` + mechanical updates ở test/bench/fuzz call sites.

1. `handler.go` — seam:
   ```go
   type stripFunc func(body []byte, paths [][]string) []byte
   ```
   `serve(...)` thêm `strip stripFunc` param; production call sites (lines 161/165) truyền `inject.StripChatFields` / `inject.StripResponsesFields`; test call sites (`provider_fallback_test.go`, `usage_test.go`) truyền `inject.StripChatFields` (mechanical).
2. Per-request binding cạnh `plan` (~line 575, trước upstream I/O):
   ```go
   stripPaths := m.Strip            // model-level (override) — value copy từ snapshot
   stripKeys := inject.StripPatterns(stripPaths)
   ```
   Và trong walk, mỗi candidate: `m.Strip = cand.Strip` cạnh các `m.Provider = cand.Provider` khác (khi model không có model-level block).
3. `rewriteOut` mở rộng, strip **cuối**:
   ```go
   rewriteOut := func(payload []byte) []byte {
       out := rewrite(payload, m.Public)
       if plan.Active {
           out = synthesize(out, plan)
       }
       out = strip(out, m.Strip)     // thay cho stripPaths — lấy từ m.Strip hiện tại (candidate-aware)
       return out
   }
   ```
   Comment ghi rõ ordering rationale + reserved keys. Buffered (`handler.go:1830`) + SSE (`handler.go:1761`) share closure → parity by construction. Meter untouched (`Observe` pre-rewrite).
4. `sse.go` — gate extension:
   ```go
   func CopySSE(dst io.Writer, src io.Reader, rewrite func(payload []byte) []byte, flush func(), stripKeys [][]byte) (StreamStats, error)
   func rewriteSSELine(line []byte, rewrite func(payload []byte) []byte, stripKeys [][]byte) []byte
   ```
   Gate = payload chứa `"model"`/`"usage"` **hoặc** bất kỳ `stripKeys`; `stripKeys` empty → một length check, không đổi hành vi. Handler truyền `stripKeys` tại call (`handler.go:1761`). Update test/bench/fuzz call sites với `nil` (~10 sites: `sse_test.go`, `sse_limits_test.go`, `sse_bench_test.go`, `fuzz_test.go`).
5. Non-goals: strip chỉ chạy nơi `rewriteOut` chạy — buffered 2xx + SSE data lines. Verbatim 3xx/204/304, normalized 4xx/5xx envelopes không qua.

**Tests (Unit 3):** new `internal/proxy/strip_test.go` (pattern `thinking_test.go`):

- `stripStore(t, endpoint, modelFields, providerFields)` helper render block.
- Buffered chat Kilo shape → exact stripped bytes.
- Buffered responses incl. `response.service_tier` descent.
- Stream chat **strip-only chunk** (có `"service_tier"` không `"model"`/`"usage"`) → stripped (chứng minh gate mở); `[DONE]`/event lines untouched.
- Default-off byte identity (cả 2 path).
- Model-override-provider: model list thay provider list.
- Meter-stays-pre-strip: usage event vẫn ghi full upstream tokens.
- Parities: stream/buffered; chained candidates khác provider strip khác nhau.
- Gate unit test (à la `TestSSEUsageGate`): nil patterns → bypass same slice; patterns no match → same slice; match → stripped.
- `fuzz_test.go`: update 2 `rewriteSSELine` calls `nil`; add `FuzzStripSSELine`.
- `sse_bench_test.go`: update call sites `nil`; optionally add `with_strip_keys` bench row.

### Unit 4 — E2E + behavior docs

**Touch:** `e2e/strip_fields_test.go` (new), `README.md` (behavior), `AGENTS.md`.

- E2E theo `e2e/thinking_usage_test.go`: `runtimeYAML` nhận verbatim extra fields. Cases: buffered chat Kilo; stream chat strip-only; default-off byte identity; reload-binds-stream-to-old-snapshot.
- `README.md`: strip under response rewriting/streaming behavior (scope/order/reserved keys).
- `AGENTS.md`: extend "Rewrite is byte-preserving" bullet + ownership rows `internal/inject`/`internal/proxy` composed rewriter.

## Ghi chú kỹ thuật quan trọng

- **Import cycle guard:** `inject` import `config` (thinking.go). → Path parsing vocabulary đặt trong `internal/config` (hoặc sub-package). `StripPath` defined ở config; `inject` nhận `[][]string` segments. Không để config import inject.
- **Data carried:** snapshot lưu `[][]string` segment-lists (parse 1 lần tại load; request path không parse lại). `StripPatterns` chỉ precompute byte pattern của **segment đầu** cho SSE gate.
- **CopySSE signature change** chạm ~10 call sites test/bench — mechanical, `nil` argument.
- **serve signature change** chạm 4 call sites (2 prod + 2 test).

## Verification

1. `go build ./... && go vet ./...` — sạch.
2. `go test ./internal/... -count=1` — xanh (unit 1-3, fuzz seeds).
3. `go test ./internal/proxy/ -run 'TestStrip|TestSSE|TestThinking' -count=1`.
4. `go test ./internal/inject/ -run 'TestStrip|FuzzStrip' -count=1`.
5. `go test ./internal/config/ -run 'Strip|Sanitize|Example' -count=1`.
6. `go test ./e2e/ -count=1 -timeout 25m` (không `-short`).
7. Manual smoke: chạy binary với config gốc (không strip) → byte-identical; thêm `strip-fields: [provider, service_tier]` lên model LiLo + curl buffered + stream → field biến mất, mọi byte khác nguyên vẹn; config sai (duplicate/reserved `model`) → `config_reload_rejected`, last-known-good giữ.
8. CI gates: `ci-gate`, `analysis-gate`.

## Files then chạm

- `internal/config/runtime.go`, `internal/config/snapshot.go`, `internal/config/runtime_test.go`, `internal/config/sanitize_test.go`
- `internal/inject/strip.go` (new), `internal/inject/strip_test.go` (new), `internal/inject/fuzz_test.go`
- `internal/proxy/handler.go`, `internal/proxy/sse.go`, `internal/proxy/strip_test.go` (new), `internal/proxy/sse_test.go`, `internal/proxy/sse_limits_test.go`, `internal/proxy/sse_bench_test.go`, `internal/proxy/fuzz_test.go`, `internal/proxy/provider_fallback_test.go`, `internal/proxy/usage_test.go`
- `config.example.yaml`, `README.md`, `AGENTS.md`
- `e2e/strip_fields_test.go` (new)
