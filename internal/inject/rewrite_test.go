package inject

import "testing"

func TestRewriteModel(t *testing.T) {
	tests := []struct {
		name   string
		body   string
		public string
		want   [2]string // chat, responses
	}{
		{
			"top-level",
			`{"model":"gpt-5"}`,
			"reviewer",
			[2]string{`{"model":"reviewer"}`, `{"model":"reviewer"}`},
		},
		{
			"nested response.model",
			`{"response":{"model":"gpt-5"}}`,
			"reviewer",
			// Deepest change: Chat does NOT rewrite response.model — the exact
			// case that motivated the API split.
			[2]string{`{"response":{"model":"gpt-5"}}`, `{"response":{"model":"reviewer"}}`},
		},
		{
			"both in one payload",
			`{"model":"gpt-5","response":{"model":"gpt-5"}}`,
			"reviewer",
			[2]string{`{"model":"reviewer","response":{"model":"gpt-5"}}`, `{"model":"reviewer","response":{"model":"reviewer"}}`},
		},
		{
			"whitespace around key and colon preserved",
			`{ "model" : "gpt-5" }`,
			"reviewer",
			[2]string{`{ "model" : "reviewer" }`, `{ "model" : "reviewer" }`},
		},
		{
			"chat chunk shape",
			`{"id":"chatcmpl-9","object":"chat.completion.chunk","created":123,"model":"gpt-5","choices":[{"index":0,"delta":{"role":"assistant","content":"hello \"world\""},"finish_reason":null}],"usage":{"prompt_tokens":10}}`,
			"reviewer",
			[2]string{
				`{"id":"chatcmpl-9","object":"chat.completion.chunk","created":123,"model":"reviewer","choices":[{"index":0,"delta":{"role":"assistant","content":"hello \"world\""},"finish_reason":null}],"usage":{"prompt_tokens":10}}`,
				// responses does NOT rewrite deeply nested "model" — same output.
				`{"id":"chatcmpl-9","object":"chat.completion.chunk","created":123,"model":"reviewer","choices":[{"index":0,"delta":{"role":"assistant","content":"hello \"world\""},"finish_reason":null}],"usage":{"prompt_tokens":10}}`,
			},
		},
		{
			"responses envelope shape",
			`{"type":"response.created","response":{"id":"resp_1","object":"response","created_at":1700000000,"model":"gpt-5","status":"completed","output":[]},"sequence_number":1}`,
			"reviewer",
			[2]string{
				// Chat leaves response.model untouched.
				`{"type":"response.created","response":{"id":"resp_1","object":"response","created_at":1700000000,"model":"gpt-5","status":"completed","output":[]},"sequence_number":1}`,
				`{"type":"response.created","response":{"id":"resp_1","object":"response","created_at":1700000000,"model":"reviewer","status":"completed","output":[]},"sequence_number":1}`,
			},
		},
		{
			"model text inside string literal not rewritten",
			`{"a":"the \"model\" key","model":"x"}`,
			"reviewer",
			[2]string{`{"a":"the \"model\" key","model":"reviewer"}`, `{"a":"the \"model\" key","model":"reviewer"}`},
		},
		{
			"escaped quotes and unicode escapes in value handled",
			`{"model":"say \"hi\" \u0041!","b":1}`,
			"reviewer",
			[2]string{`{"model":"reviewer","b":1}`, `{"model":"reviewer","b":1}`},
		},
		{
			"public value needing escaping",
			`{"model":"gpt-5"}`,
			`a"b\c`,
			[2]string{`{"model":"a\"b\\c"}`, `{"model":"a\"b\\c"}`},
		},
		{
			"two siblings both rewritten",
			`{"model":"a","model":"b"}`,
			"reviewer",
			[2]string{`{"model":"reviewer","model":"reviewer"}`, `{"model":"reviewer","model":"reviewer"}`},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := [2]string{
				string(RewriteChatModel([]byte(tt.body), tt.public)),
				string(RewriteResponsesModel([]byte(tt.body), tt.public)),
			}
			if got != tt.want {
				t.Fatalf("rewrite(%s, %q) =\n  chat=%s\n  resp=%s\nwant\n  chat=%s\n  resp=%s",
					tt.body, tt.public, got[0], got[1], tt.want[0], tt.want[1])
			}
		})
	}
}

// TestRewriteModelNestedChatUntouched pins the exact motivating example: a
// chat chunk carrying a nested "response" object must NOT have that nested
// model rewritten, while the Responses API must. This is the two scopes.
func TestRewriteModelNestedChatUntouched(t *testing.T) {
	body := `{"model":"old","response":{"model":"nested"}}`
	if got := string(RewriteChatModel([]byte(body), "public")); got != `{"model":"public","response":{"model":"nested"}}` {
		t.Fatalf("RewriteChatModel =\n  %s", got)
	}
	if got := string(RewriteResponsesModel([]byte(body), "public")); got != `{"model":"public","response":{"model":"public"}}` {
		t.Fatalf("RewriteResponsesModel =\n  %s", got)
	}
}

func TestRewriteModelUnchanged(t *testing.T) {
	bodies := []string{
		`{"id":"chatcmpl-1"}`,          // no model field
		`[DONE]`,                       // SSE terminator, not JSON
		`not json`,                     // invalid JSON
		`{"model":`,                    // truncated JSON
		`{"model":123}`,                // non-string value: number
		`{"model":true}`,               // non-string value: bool
		`{"model":null}`,               // non-string value: null
		`{"model":["x"]}`,              // non-string value: array
		`{"a":"model"}`,                // string VALUE reading "model"
		`{"a":"he said \"model\" ok"}`, // quoted text inside a value
		`["model"]`,                    // array element
		`{"my_model":"x"}`,             // key merely containing "model"
		`{"mod\u0065l":"x"}`,           // escaped key decodes to "model" — the scan is byte-level by design
		`""`,                           // bare string document
		``,                             // empty body
	}
	for _, body := range bodies {
		t.Run(body, func(t *testing.T) {
			for name, got := range map[string]string{
				"chat":      string(RewriteChatModel([]byte(body), "reviewer")),
				"responses": string(RewriteResponsesModel([]byte(body), "reviewer")),
			} {
				if got != body {
					t.Fatalf("%s must return input unchanged:\n got  %s\n want %s", name, got, body)
				}
			}
		})
	}
}

func TestRewriteModelPreservesUnrewrittenRegions(t *testing.T) {
	body := `{"id" : "chatcmpl-1","model" : "gpt-5","n" : 1,"flag" : true,"arr" : [1,2],"obj":{"k":"v"},"text":"model not here"}`
	want := `{"id" : "chatcmpl-1","model" : "reviewer","n" : 1,"flag" : true,"arr" : [1,2],"obj":{"k":"v"},"text":"model not here"}`
	for name, got := range map[string]string{
		"chat":      string(RewriteChatModel([]byte(body), "reviewer")),
		"responses": string(RewriteResponsesModel([]byte(body), "reviewer")),
	} {
		if got != want {
			t.Fatalf("%s:\n got  %s\n want %s", name, got, want)
		}
	}
}

// TestRewriteModelScopePinned pins the documented rewrite scope: only the
// top-level "model" key and the "model" key directly inside a top-level
// "response" object (Responses API only) are rewritten. Any deeper "model"
// key belongs to the client's own payload — metadata tags, tool output,
// usage breakdowns — and rewriting it would mutate client data in transit.
func TestRewriteModelScopePinned(t *testing.T) {
	tests := []struct {
		name     string
		body     string
		wantChat string
		wantResp string
	}{
		{
			"nested metadata untouched, response.model rewritten (responses only)",
			`{"response":{"metadata":{"model":"trace-1"},"model":"up"}}`,
			`{"response":{"metadata":{"model":"trace-1"},"model":"up"}}`,
			`{"response":{"metadata":{"model":"trace-1"},"model":"reviewer"}}`,
		},
		{
			"arbitrary nesting untouched",
			`{"outer":{"inner":{"model":"x"}}}`,
			`{"outer":{"inner":{"model":"x"}}}`,
			`{"outer":{"inner":{"model":"x"}}}`,
		},
		{
			"usage breakdown untouched",
			`{"usage":{"model_breakdown":{"model":"gpt-x"}}}`,
			`{"usage":{"model_breakdown":{"model":"gpt-x"}}}`,
			`{"usage":{"model_breakdown":{"model":"gpt-x"}}}`,
		},
		{
			"array element untouched",
			`[{"model":"x"}]`,
			`[{"model":"x"}]`,
			`[{"model":"x"}]`,
		},
		{
			"response two levels down untouched",
			`{"response":{"wrapper":{"model":"x"}}}`,
			`{"response":{"wrapper":{"model":"x"}}}`,
			`{"response":{"wrapper":{"model":"x"}}}`,
		},
		{
			"data envelope untouched",
			`{"data":[{"model":"x"}]}`,
			`{"data":[{"model":"x"}]}`,
			`{"data":[{"model":"x"}]}`,
		},
		{
			"model value object untouched",
			`{"model":{"model":"inner"}}`,
			`{"model":{"model":"inner"}}`,
			`{"model":{"model":"inner"}}`,
		},
		{
			"response twin model untouched, response.model rewritten",
			`{"response":{"model":"up","twin":{"model":"up2"}}}`,
			`{"response":{"model":"up","twin":{"model":"up2"}}}`,
			`{"response":{"model":"reviewer","twin":{"model":"up2"}}}`,
		},
		{
			"response string containing brace-quote does not end the object early",
			`{"response":{"a":"x\"}y\"}","model":"up"}}`,
			`{"response":{"a":"x\"}y\"}","model":"up"}}`,
			`{"response":{"a":"x\"}y\"}","model":"reviewer"}}`,
		},
		{
			"whitespace-heavy response object",
			`{ "response" : { "model" : "up" } }`,
			`{ "response" : { "model" : "up" } }`,
			`{ "response" : { "model" : "reviewer" } }`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := string(RewriteChatModel([]byte(tt.body), "reviewer")); got != tt.wantChat {
				t.Fatalf("RewriteChatModel:\n got  %s\n want %s", got, tt.wantChat)
			}
			if got := string(RewriteResponsesModel([]byte(tt.body), "reviewer")); got != tt.wantResp {
				t.Fatalf("RewriteResponsesModel:\n got  %s\n want %s", got, tt.wantResp)
			}
		})
	}
}

// TestRewriteModelDuplicateResponseKeys pins duplicate-key handling per API:
// within the scope each variant claims, every matching in-scope span is
// rewritten — mirroring the duplicate top-level "model" behavior.
func TestRewriteModelDuplicateResponseKeys(t *testing.T) {
	body := `{"response":{"model":"a"},"response":{"model":"b"}}`
	if got := string(RewriteChatModel([]byte(body), "reviewer")); got != body {
		t.Fatalf("RewriteChatModel must not touch response scopes, got:\n  %s", got)
	}
	want := `{"response":{"model":"reviewer"},"response":{"model":"reviewer"}}`
	if got := string(RewriteResponsesModel([]byte(body), "reviewer")); got != want {
		t.Fatalf("\n got  %s\n want %s", got, want)
	}

	// A duplicate top-level "model" is rewritten by BOTH scopes.
	top := `{"model":"a","model":"b"}`
	if got := string(RewriteChatModel([]byte(top), "reviewer")); got != `{"model":"reviewer","model":"reviewer"}` {
		t.Fatalf("RewriteChatModel duplicate top-level: %s", got)
	}
	if got := string(RewriteResponsesModel([]byte(top), "reviewer")); got != `{"model":"reviewer","model":"reviewer"}` {
		t.Fatalf("RewriteResponsesModel duplicate top-level: %s", got)
	}
}
