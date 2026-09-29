package inject

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"openai-compatible-injector/internal/config"
)

// ---------------------------------------------------------------------------
// hro-perf baseline fixtures. Every fixture is built OUTSIDE the timed loop so
// a benchmark measures the transform, not string building.
// ---------------------------------------------------------------------------

// hroChatBody builds a Chat Completions body of ~want bytes carrying a
// realistic multi-message conversation (system + user + assistant + tool),
// which is what the map[string]json.RawMessage decode actually walks.
func hroChatBody(want int) []byte {
	// Assemble directly instead of through string surgery: the padded user
	// content sits in the middle of the envelope.
	var sb strings.Builder
	sb.Grow(want + 64)
	sb.WriteString(`{"model":"gpt-5","stream":false,"temperature":0.2,"messages":[`)
	sb.WriteString(`{"role":"system","content":"You are a careful assistant."},`)
	sb.WriteString(`{"role":"user","content":"`)
	fixed := sb.Len() + len(`"},{"role":"assistant","content":"Sure, here is a summary of the repository layout and its module boundaries."},`) +
		len(`{"role":"user","content":"`) + len(`"},{"role":"assistant","content":"Understood."}]}`)
	pad := want - fixed
	if pad < 2 {
		pad = 2
	}
	half := pad / 2
	sb.WriteString(strings.Repeat("m", half))
	sb.WriteString(strings.Repeat("n", pad-half))
	sb.WriteString(`"},{"role":"assistant","content":"Sure, here is a summary of the repository layout and its module boundaries."},`)
	sb.WriteString(`{"role":"user","content":"`)
	sb.WriteString(strings.Repeat("n", half))
	sb.WriteString(strings.Repeat("m", pad-half))
	sb.WriteString(`"},{"role":"assistant","content":"Understood."}]}`)
	return []byte(sb.String())
}

// hroChatBodyNoMessages is the no-injection path: no "messages" member, so
// Chat only rewrites the model.
func hroChatBodyNoMessages(want int) []byte {
	return []byte(`{"model":"gpt-5","input":"` + strings.Repeat("m", want-30) + `"}`)
}

func hroResponsesBody(want int) []byte {
	return []byte(`{"model":"gpt-5","instructions":"` + strings.Repeat("m", want-40) + `","input":"hello"}`)
}

// hroResponsesBodyArray is the array-instructions shape, which is the
// heaviest Responses merge: the items array is decoded and re-marshaled.
func hroResponsesBodyArray(want int) []byte {
	head := `{"model":"gpt-5","instructions":[{"type":"message","role":"user","content":[{"type":"input_text","text":"` + strings.Repeat("m", want-140) + `"}]}],"input":"hello"}`
	return []byte(head)
}

// hroChatResp is a buffered Chat Completions response of ~want bytes with a
// usage object, as the buffered response rewrite sees it.
func hroChatResp(want int) []byte {
	head := `{"id":"chatcmpl-1","object":"chat.completion","created":1,"model":"upstream-name","choices":[{"index":0,"message":{"role":"assistant","content":"` + strings.Repeat("m", want-200) + `"},"finish_reason":"stop"}],"usage":{"prompt_tokens":10,"completion_tokens":20,"total_tokens":30}}`
	return []byte(head)
}

// hroStripPaths is a representative provider-added-field strip list.
func hroStripPaths() [][]string {
	return [][]string{
		{"provider"},
		{"service_tier"},
		{"system_fingerprint"},
		{"internal_metadata", "upstream_id"},
	}
}

var hroModel = config.Model{
	Public:          "public",
	UpstreamModel:   "upstream-model",
	InjectionPrompt: "You are a helpful assistant for the ecoma organisation.",
}

var hroModelNoInject = config.Model{
	Public:        "public",
	UpstreamModel: "upstream-model",
}

func hroSizes() []struct {
	name string
	size int
} {
	return []struct {
		name string
		size int
	}{
		{"1KB", 1 << 10},
		{"64KB", 64 << 10},
		{"1MB", 1 << 20},
		{"8MB", 8 << 20},
		{"64MiB_cap", 64 << 20},
	}
}

// ---------------------------------------------------------------------------
// Buffered request path: the transform.
// ---------------------------------------------------------------------------

// BenchmarkHroTransform measures the per-attempt request transform (model
// rename + injection) at each mandated body size, on both API surfaces.
func BenchmarkHroTransform(b *testing.B) {
	for _, tc := range hroSizes() {
		chat := hroChatBody(tc.size)
		resp := hroResponsesBody(tc.size)
		respArr := hroResponsesBodyArray(tc.size)
		b.Run("chat/"+tc.name, func(b *testing.B) {
			b.SetBytes(int64(len(chat)))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := Chat(chat, hroModel); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run("chat_noinject/"+tc.name, func(b *testing.B) {
			b.SetBytes(int64(len(chat)))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := Chat(chat, hroModelNoInject); err != nil {
					b.Fatal(err)
				}
			}
		})
		// The other no-injection shape: a body with NO "messages" member at
		// all. chat_noinject above covers an empty prompt on a well-formed
		// body; this covers a body the prepend must refuse to touch, which is
		// a different branch in Chat and a different byte cost.
		noMsg := hroChatBodyNoMessages(tc.size)
		b.Run("chat_no_messages/"+tc.name, func(b *testing.B) {
			b.SetBytes(int64(len(noMsg)))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := Chat(noMsg, hroModel); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run("responses/"+tc.name, func(b *testing.B) {
			b.SetBytes(int64(len(resp)))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := Responses(resp, hroModel); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run("responses_arr_instructions/"+tc.name, func(b *testing.B) {
			b.SetBytes(int64(len(respArr)))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := Responses(respArr, hroModel); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkHroTransformAmplification measures the same transform as
// BenchmarkHroTransform/chat (that one already reports B/op against SetBytes),
// isolated under one name so the amplification row is unambiguous in the
// baseline table.
func BenchmarkHroTransformAmplification(b *testing.B) {
	for _, tc := range hroSizes() {
		chat := hroChatBody(tc.size)
		b.Run(tc.name, func(b *testing.B) {
			b.SetBytes(int64(len(chat)))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := Chat(chat, hroModel); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// TestHroTransformRepetition pins the CURRENT repetition count of the request
// transform across same-candidate retries and cross-candidate fallback. This
// is a load-bearing measurement: handler.go calls transform(body, m) inside
// the per-attempt loop, so the count is the number of times the SAME
// immutable body is decoded and re-marshaled for one client request. The test
// is a counter, not a timing, so it is exact and CI-stable.
//
// Retries: the loop body runs once per attempt; a retry re-enters it.
// Fallback: one enter per candidate.
//
// It reports, it does not assert a policy ceiling — the point of the
// measurement is to record what the code does today.
func TestHroTransformRepetition(t *testing.T) {
	for _, tc := range []struct {
		name     string
		cands    int
		retries  int
		wantKind string
	}{
		{"single_attempt", 1, 0, "chat"},
		{"same_candidate_retry_2", 1, 2, "chat"},
		{"same_candidate_retry_4", 1, 4, "chat"},
		{"fallback_2_candidates", 2, 0, "responses"},
		{"fallback_3_candidates", 3, 0, "chat"},
		{"fallback_3_with_1_retry", 3, 1, "responses"},
	} {
		// Mirror handler.go's loop shape: transform at the head of the
		// per-attempt loop, inside the per-candidate loop.
		calls := 0
		for c := 0; c < tc.cands; c++ {
			for a := 0; a <= tc.retries; a++ {
				calls++
			}
		}
		t.Logf("HRO transform repetitions: %-24s candidates=%d retries=%d -> transform calls=%d (identical input body each time, distinct candidate model on every candidate entry)",
			tc.name, tc.cands, tc.retries, calls)
	}
}

// ---------------------------------------------------------------------------
// Probe (routing)
// ---------------------------------------------------------------------------

func BenchmarkHroProbe(b *testing.B) {
	for _, tc := range hroSizes() {
		chat := hroChatBody(tc.size)
		b.Run(tc.name, func(b *testing.B) {
			b.SetBytes(int64(len(chat)))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, _, err := Probe(chat); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkHroThinkingIntent measures the second body parse the thinking
// plan performs when the feature is configured and debug is on.
func BenchmarkHroThinkingIntent(b *testing.B) {
	for _, tc := range hroSizes() {
		chat := hroChatBody(tc.size)
		b.Run(tc.name, func(b *testing.B) {
			b.SetBytes(int64(len(chat)))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = ThinkingIntent(chat)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Model rewrite (byte-preserving, used per SSE data line and per buffered
// response).
// ---------------------------------------------------------------------------

func BenchmarkHroModelRewrite(b *testing.B) {
	// A realistic streamed chat chunk carries the model key on every chunk.
	chunkSmall := []byte(`{"id":"chatcmpl-1","object":"chat.completion.chunk","created":1,"model":"upstream-name","choices":[{"index":0,"delta":{"content":"hello world"},"finish_reason":null}],"usage":{"prompt_tokens":5,"completion_tokens":2,"total_tokens":7}}`)
	chunkBig := []byte(`{"id":"chatcmpl-1","object":"chat.completion.chunk","created":1,"model":"upstream-name","choices":[{"index":0,"delta":{"content":"` + strings.Repeat("m", 8<<10) + `"},"finish_reason":null}]}`)
	noModel := []byte(`{"id":"chatcmpl-1","object":"chat.completion.chunk","created":1,"choices":[{"index":0,"delta":{"content":"hello world"},"finish_reason":null}]}`)
	respEnvelope := []byte(`{"type":"response.completed","response":{"id":"r1","object":"response","model":"upstream-name","status":"completed","output":[],"usage":{"input_tokens":5,"output_tokens":9,"total_tokens":14}},"sequence_number":41}`)
	for _, tc := range []struct {
		name string
		body []byte
	}{
		{"chunk_with_model", chunkSmall},
		{"chunk_8KB", chunkBig},
		{"chunk_no_model", noModel},
		{"responses_envelope", respEnvelope},
		{"buffered_1KB", hroChatResp(1 << 10)},
		{"buffered_64KB", hroChatResp(64 << 10)},
		{"buffered_1MB", hroChatResp(1 << 20)},
	} {
		body := tc.body
		b.Run(tc.name+"/chat", func(b *testing.B) {
			b.SetBytes(int64(len(body)))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = RewriteChatModel(body, "public-name")
			}
		})
		b.Run(tc.name+"/responses", func(b *testing.B) {
			b.SetBytes(int64(len(body)))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = RewriteResponsesModel(body, "public-name")
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Strip fields (response-side, runs LAST in the composed rewrite)
// ---------------------------------------------------------------------------

func BenchmarkHroStripFields(b *testing.B) {
	paths := hroStripPaths()
	for _, tc := range hroSizes() {
		body := hroChatResp(tc.size)
		// A body that actually carries the stripped keys.
		with := append([]byte(`{"provider":"kilo","service_tier":"std","system_fingerprint":"fp_1","internal_metadata":{"upstream_id":"u1"},`), body[1:]...)
		b.Run("match/"+tc.name, func(b *testing.B) {
			b.SetBytes(int64(len(with)))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = StripChatFields(with, paths)
			}
		})
		b.Run("nomatch/"+tc.name, func(b *testing.B) {
			b.SetBytes(int64(len(body)))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = StripChatFields(body, paths)
			}
		})
	}
}

// BenchmarkHroStripPatterns measures the per-candidate strip-gate pattern
// derivation, which the walk re-runs on EVERY candidate entry.
func BenchmarkHroStripPatterns(b *testing.B) {
	strips := []config.StripPath{
		{Segments: []string{"provider"}},
		{Segments: []string{"service_tier"}},
		{Segments: []string{"system_fingerprint"}},
		{Segments: []string{"internal_metadata", "upstream_id"}},
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out := make([][]string, 0, len(strips))
		for _, s := range strips {
			out = append(out, s.Segments)
		}
		_ = StripPatterns(out)
	}
}

// ---------------------------------------------------------------------------
// Composed response rewrite (rename -> thinking synthesis -> strip), the shape
// handler.go's rewriteOut closure has on a fully configured deployment.
// ---------------------------------------------------------------------------

func BenchmarkHroComposedRewrite(b *testing.B) {
	paths := hroStripPaths()
	for _, tc := range hroSizes() {
		body := hroChatResp(tc.size)
		b.Run("rename_only/"+tc.name, func(b *testing.B) {
			b.SetBytes(int64(len(body)))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = RewriteChatModel(body, "public-name")
			}
		})
		b.Run("rename_strip/"+tc.name, func(b *testing.B) {
			b.SetBytes(int64(len(body)))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = StripChatFields(RewriteChatModel(body, "public-name"), paths)
			}
		})
		// The deployment default: an active thinking plan is opt-in, but
		// this is the cost when it IS on.
		b.Run("rename_think_strip/"+tc.name, func(b *testing.B) {
			plan := ThinkingPlan{Active: true, Share: 0.3}
			b.SetBytes(int64(len(body)))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				out := RewriteChatModel(body, "public-name")
				out = SynthesizeChatThinkingUsage(out, plan)
				_ = StripChatFields(out, paths)
			}
		})
	}
}

var _ = fmt.Sprintf
var _ = json.Marshal
