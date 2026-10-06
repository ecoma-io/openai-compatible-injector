package usage

import (
	"encoding/json"
	"testing"
)

func i64(v int64) *int64 { return &v }

// Chat surface: only the top-level usage object is the API's usage.

func TestExtractChatUsage(t *testing.T) {
	cases := []struct {
		name string
		body string
		want Tokens
		ok   bool
	}{
		{
			name: "buffered chat response",
			body: `{"id":"x","model":"public","usage":{"prompt_tokens":10,"completion_tokens":20,"total_tokens":30}}`,
			want: Tokens{PromptTokens: i64(10), CompletionTokens: i64(20), TotalTokens: i64(30)},
			ok:   true,
		},
		{
			name: "streaming chunk with usage null",
			body: `{"id":"x","model":"public","choices":[],"usage":null}`,
			ok:   false,
		},
		{
			name: "streaming chunk without usage",
			body: `{"id":"x","model":"public","choices":[{"delta":{"content":"hi"}}]}`,
			ok:   false,
		},
		{
			name: "partial usage object states only two counts",
			body: `{"usage":{"prompt_tokens":10,"total_tokens":10}}`,
			want: Tokens{PromptTokens: i64(10), TotalTokens: i64(10)},
			ok:   true,
		},
		{
			name: "nested response.model is client data, never read",
			body: `{"model":"public","choices":[{"message":{"content":"{\"response\":{\"usage\":{\"input_tokens\":999}}}"},"usage":{"prompt_tokens":7}}],"usage":{"prompt_tokens":7,"completion_tokens":3,"total_tokens":10}}`,
			want: Tokens{PromptTokens: i64(7), CompletionTokens: i64(3), TotalTokens: i64(10)},
			ok:   true,
		},
		{
			name: "not JSON",
			body: `[DONE]`,
			ok:   false,
		},
		{
			name: "usage wrongly typed",
			body: `{"usage":"10 tokens"}`,
			ok:   false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := ExtractChatUsage([]byte(tc.body))
			if ok != tc.ok {
				t.Fatalf("ok = %v, want %v", ok, tc.ok)
			}
			assertTokens(t, got, tc.want)
		})
	}
}

// Responses surface: top-level usage of a buffered body, or the usage
// inside the top-level "response" envelope of a streaming event.

func TestExtractResponsesUsage(t *testing.T) {
	cases := []struct {
		name string
		body string
		want Tokens
		ok   bool
	}{
		{
			name: "buffered response object",
			body: `{"id":"resp_1","object":"response","usage":{"input_tokens":11,"output_tokens":22,"total_tokens":33}}`,
			want: Tokens{PromptTokens: i64(11), CompletionTokens: i64(22), TotalTokens: i64(33)},
			ok:   true,
		},
		{
			name: "response.completed envelope event",
			body: `{"type":"response.completed","response":{"id":"resp_1","usage":{"input_tokens":11,"output_tokens":22,"total_tokens":33}}}`,
			want: Tokens{PromptTokens: i64(11), CompletionTokens: i64(22), TotalTokens: i64(33)},
			ok:   true,
		},
		{
			name: "envelope event without usage",
			body: `{"type":"response.created","response":{"id":"resp_1"}}`,
			ok:   false,
		},
		{
			name: "unrelated event",
			body: `{"type":"response.output_text.delta","delta":"hello"}`,
			ok:   false,
		},
		{
			name: "top level wins over envelope when both present",
			body: `{"usage":{"input_tokens":1,"total_tokens":1},"response":{"usage":{"input_tokens":999,"total_tokens":999}}}`,
			want: Tokens{PromptTokens: i64(1), TotalTokens: i64(1)},
			ok:   true,
		},
		{
			name: "not JSON",
			body: `garbage`,
			ok:   false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := ExtractResponsesUsage([]byte(tc.body))
			if ok != tc.ok {
				t.Fatalf("ok = %v, want %v", ok, tc.ok)
			}
			assertTokens(t, got, tc.want)
		})
	}
}

// Token counts arrive in the wild as JSON integers, integral floats
// (JavaScript-generated counts can arrive as 1e3), or digit strings. One
// wrongly typed member is that member unstated — never a reason to discard
// the members that did decode, and never a fabricated zero.

func TestExtractChatUsageLenientMembers(t *testing.T) {
	cases := []struct {
		name string
		body string
		want Tokens
		ok   bool
	}{
		{
			name: "stringified counts",
			body: `{"usage":{"prompt_tokens":"12","completion_tokens":"34","total_tokens":"46"}}`,
			want: Tokens{PromptTokens: i64(12), CompletionTokens: i64(34), TotalTokens: i64(46)},
			ok:   true,
		},
		{
			name: "integral float exponent",
			body: `{"usage":{"prompt_tokens":1e3,"completion_tokens":2e3,"total_tokens":3e3}}`,
			want: Tokens{PromptTokens: i64(1000), CompletionTokens: i64(2000), TotalTokens: i64(3000)},
			ok:   true,
		},
		{
			name: "one malformed member does not discard the others",
			body: `{"usage":{"prompt_tokens":"12","completion_tokens":12.5,"total_tokens":46}}`,
			want: Tokens{PromptTokens: i64(12), TotalTokens: i64(46)},
			ok:   true,
		},
		{
			name: "null members stay unstated",
			body: `{"usage":{"prompt_tokens":null,"completion_tokens":8,"total_tokens":null}}`,
			want: Tokens{CompletionTokens: i64(8)},
			ok:   true,
		},
		{
			name: "every member unreadable is no readable usage object",
			body: `{"usage":{"prompt_tokens":12.5,"completion_tokens":true,"total_tokens":{"a":1}}}`,
			ok:   false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := ExtractChatUsage([]byte(tc.body))
			if ok != tc.ok {
				t.Fatalf("ok = %v, want %v", ok, tc.ok)
			}
			assertTokens(t, got, tc.want)
		})
	}
}

// Capture: last-wins across a stream, never summed; the prefilter skips
// usage-less payloads without touching the previous observation.

func TestCaptureLastWinsNeverSums(t *testing.T) {
	c := NewCapture("chat")
	c.Observe([]byte(`{"usage":{"prompt_tokens":10,"completion_tokens":20,"total_tokens":30}}`))
	c.Observe([]byte(`{"usage":{"prompt_tokens":10,"completion_tokens":20,"total_tokens":30}}`))
	tokens, ok := c.Tokens()
	if !ok {
		t.Fatal("no usage after two observations")
	}
	assertTokens(t, tokens, Tokens{PromptTokens: i64(10), CompletionTokens: i64(20), TotalTokens: i64(30)})

	// Cumulative chunks update: the final object wins whole.
	c.Observe([]byte(`{"usage":{"prompt_tokens":15,"completion_tokens":25,"total_tokens":40}}`))
	tokens, _ = c.Tokens()
	assertTokens(t, tokens, Tokens{PromptTokens: i64(15), CompletionTokens: i64(25), TotalTokens: i64(40)})

	// A later chunk without a readable usage object changes nothing —
	// including a null usage, which is the common per-chunk shape.
	c.Observe([]byte(`{"usage":null}`))
	tokens, _ = c.Tokens()
	assertTokens(t, tokens, Tokens{PromptTokens: i64(15), CompletionTokens: i64(25), TotalTokens: i64(40)})
}

func TestCaptureAbsentStaysAbsent(t *testing.T) {
	c := NewCapture("chat")
	c.Observe([]byte(`{"model":"public","choices":[]}`))
	if _, ok := c.Tokens(); ok {
		t.Fatal("capture invented usage from a usage-less body")
	}
}

func TestCaptureAPIScope(t *testing.T) {
	// A chat capture must not descend into a "response" envelope — that is
	// the Responses surface's shape, and a chat payload's nested response
	// member is client data.
	chat := NewCapture("chat")
	chat.Observe([]byte(`{"response":{"usage":{"input_tokens":999}}}`))
	if _, ok := chat.Tokens(); ok {
		t.Fatal("chat capture read the responses envelope")
	}

	resp := NewCapture("responses")
	resp.Observe([]byte(`{"response":{"usage":{"input_tokens":5,"output_tokens":6,"total_tokens":11}}}`))
	tokens, ok := resp.Tokens()
	if !ok {
		t.Fatal("responses capture missed the envelope usage")
	}
	assertTokens(t, tokens, Tokens{PromptTokens: i64(5), CompletionTokens: i64(6), TotalTokens: i64(11)})
}

// The Anthropic "messages" surface meters the SAME bytes a chat request
// would: Observe reads pre-rewrite upstream payloads, and a Messages
// request's upstream traffic is Chat Completions. Giving it its own
// extractor — or worse, defaulting the unknown surface to the Responses
// one — would silently NULL every messages row's token columns, which is the
// quiet direction: nothing fails, the numbers just stop being there.
func TestCaptureMessagesUsesChatExtractor(t *testing.T) {
	c := NewCapture("messages")
	c.Observe([]byte(`{"usage":{"prompt_tokens":11,"completion_tokens":22,"total_tokens":33}}`))
	tokens, ok := c.Tokens()
	if !ok {
		t.Fatal("messages capture read no usage from a chat-shaped object")
	}
	assertTokens(t, tokens, Tokens{PromptTokens: i64(11), CompletionTokens: i64(22), TotalTokens: i64(33)})

	// ...and it still does not descend into a Responses envelope, which is
	// not the shape any messages upstream sends.
	other := NewCapture("messages")
	other.Observe([]byte(`{"response":{"usage":{"input_tokens":999}}}`))
	if _, ok := other.Tokens(); ok {
		t.Fatal("messages capture read the responses envelope")
	}
}

// Seal: the boundary between two upstream calls answering one request. The
// two token counts are deliberately NOT combined the same way — a hop re-asks
// with the same conversation, so its prompt is the context seen again and the
// last call that stated one wins, while its completion is text the client
// also received and the two add up. A blank Seal must stay a no-op so a
// caller can invoke it unconditionally at every boundary.
func TestCaptureSealsAcrossUpstreamCalls(t *testing.T) {
	c := NewCapture("chat")
	c.Observe([]byte(`{"usage":{"prompt_tokens":10,"completion_tokens":20,"total_tokens":30}}`))
	c.Seal()
	c.Observe([]byte(`{"usage":{"prompt_tokens":5,"completion_tokens":7,"total_tokens":12}}`))

	tokens, ok := c.Tokens()
	if !ok {
		t.Fatal("no usage after two sealed calls")
	}
	// prompt: the LAST call's context, never the two added. completion: the
	// two calls' answers added. total: the row's own sum of the two counts it
	// reports, never either upstream's own total (each describes one call).
	assertTokens(t, tokens, Tokens{PromptTokens: i64(5), CompletionTokens: i64(27), TotalTokens: i64(32)})
}

func TestCaptureSealIsAnIdempotentBoundary(t *testing.T) {
	// A boundary with nothing to fold must be a no-op, and must not
	// manufacture usage: the feature-off path and a hop that states nothing
	// both go through this.
	c := NewCapture("chat")
	c.Seal()
	c.Seal()
	if _, ok := c.Tokens(); ok {
		t.Fatal("a capture that never observed usage reported some")
	}

	c.Observe([]byte(`{"usage":{"prompt_tokens":10,"completion_tokens":20,"total_tokens":30}}`))
	c.Seal()
	c.Seal()
	tokens, ok := c.Tokens()
	if !ok {
		t.Fatal("double Seal lost the sealed observation")
	}
	assertTokens(t, tokens, Tokens{PromptTokens: i64(10), CompletionTokens: i64(20), TotalTokens: i64(30)})
}

// The second job Seal does: a hop that never states usage must not leave the
// PREVIOUS hop's object standing as the current observation. Without the
// boundary the next hop's first usage-bearing payload would be read as a
// continuation of the previous hop's counts.
func TestCaptureSealSegmentsTheCurrentCall(t *testing.T) {
	c := NewCapture("chat")
	c.Observe([]byte(`{"usage":{"prompt_tokens":10,"completion_tokens":20,"total_tokens":30}}`))
	c.Seal()

	// The new call has stated nothing yet. The request's usage is the sealed
	// call's — reported as an aggregate, not as a live observation that the
	// next payload would silently extend.
	tokens, ok := c.Tokens()
	if !ok {
		t.Fatal("the sealed call's usage disappeared")
	}
	assertTokens(t, tokens, Tokens{PromptTokens: i64(10), CompletionTokens: i64(20), TotalTokens: i64(30)})

	// The new call's own object is its own: sealed before it, so the sum is
	// the two calls with the second call's prompt as the context.
	c.Observe([]byte(`{"usage":{"prompt_tokens":4,"completion_tokens":1,"total_tokens":5}}`))
	tokens, _ = c.Tokens()
	assertTokens(t, tokens, Tokens{PromptTokens: i64(4), CompletionTokens: i64(21), TotalTokens: i64(25)})

	// A cumulative chunk inside that call still replaces, never adds.
	c.Observe([]byte(`{"usage":{"prompt_tokens":4,"completion_tokens":2,"total_tokens":6}}`))
	tokens, _ = c.Tokens()
	assertTokens(t, tokens, Tokens{PromptTokens: i64(4), CompletionTokens: i64(22), TotalTokens: i64(26)})
}

func TestFoldTokensCombinesCountsByTheirOwnSemantics(t *testing.T) {
	cases := []struct {
		name string
		a, b Tokens
		want Tokens
	}{
		{
			name: "complete objects on both sides",
			a:    Tokens{PromptTokens: i64(100), CompletionTokens: i64(10), TotalTokens: i64(110)},
			b:    Tokens{PromptTokens: i64(120), CompletionTokens: i64(5), TotalTokens: i64(125)},
			want: Tokens{PromptTokens: i64(120), CompletionTokens: i64(15), TotalTokens: i64(135)},
		},
		{
			name: "neither side stated anything",
			want: Tokens{},
		},
		{
			name: "a nil side is the zero-value observation",
			a:    Tokens{PromptTokens: i64(100), CompletionTokens: i64(10), TotalTokens: i64(110)},
			want: Tokens{PromptTokens: i64(100), CompletionTokens: i64(10), TotalTokens: i64(110)},
		},
		{
			name: "a nil side never clears what the other stated",
			b:    Tokens{PromptTokens: i64(100), CompletionTokens: i64(10), TotalTokens: i64(110)},
			want: Tokens{PromptTokens: i64(100), CompletionTokens: i64(10), TotalTokens: i64(110)},
		},
		{
			name: "the later prompt wins",
			a:    Tokens{PromptTokens: i64(100), CompletionTokens: i64(10), TotalTokens: i64(110)},
			b:    Tokens{PromptTokens: i64(7), CompletionTokens: i64(1), TotalTokens: i64(8)},
			want: Tokens{PromptTokens: i64(7), CompletionTokens: i64(11), TotalTokens: i64(18)},
		},
		{
			name: "an unstated later prompt keeps the earlier context",
			a:    Tokens{PromptTokens: i64(100), CompletionTokens: i64(10), TotalTokens: i64(110)},
			b:    Tokens{CompletionTokens: i64(1)},
			want: Tokens{PromptTokens: i64(100), CompletionTokens: i64(11), TotalTokens: i64(111)},
		},
		{
			name: "a stated zero is a count, not an absence",
			a:    Tokens{CompletionTokens: i64(0), TotalTokens: i64(0)},
			b:    Tokens{PromptTokens: i64(9), CompletionTokens: i64(0), TotalTokens: i64(9)},
			want: Tokens{PromptTokens: i64(9), CompletionTokens: i64(0), TotalTokens: i64(9)},
		},
		{
			name: "a total-only call contributes its total when no count is known",
			a:    Tokens{TotalTokens: i64(30)},
			b:    Tokens{TotalTokens: i64(12)},
			want: Tokens{TotalTokens: i64(12)},
		},
		{
			name: "a stated count replaces a lone unpaired total",
			a:    Tokens{TotalTokens: i64(30)},
			b:    Tokens{PromptTokens: i64(4)},
			want: Tokens{PromptTokens: i64(4), TotalTokens: i64(4)},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assertTokens(t, foldTokens(tc.a, tc.b), tc.want)
		})
	}
}

func assertTokens(t *testing.T, got, want Tokens) {
	t.Helper()
	if !samePtr(got.PromptTokens, want.PromptTokens) ||
		!samePtr(got.CompletionTokens, want.CompletionTokens) ||
		!samePtr(got.TotalTokens, want.TotalTokens) {
		t.Fatalf("tokens = %s, want %s", render(got), render(want))
	}
}

func samePtr(a, b *int64) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}

func render(t Tokens) string {
	out, _ := json.Marshal(t)
	return string(out)
}
