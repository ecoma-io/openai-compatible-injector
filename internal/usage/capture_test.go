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
