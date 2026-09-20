package inject

import "testing"

func TestRewriteModel(t *testing.T) {
	tests := []struct {
		name   string
		body   string
		public string
		want   string
	}{
		{
			"top-level",
			`{"model":"gpt-5"}`,
			"reviewer",
			`{"model":"reviewer"}`,
		},
		{
			"nested response.model",
			`{"response":{"model":"gpt-5"}}`,
			"reviewer",
			`{"response":{"model":"reviewer"}}`,
		},
		{
			"both in one payload",
			`{"model":"gpt-5","response":{"model":"gpt-5"}}`,
			"reviewer",
			`{"model":"reviewer","response":{"model":"reviewer"}}`,
		},
		{
			"whitespace around key and colon preserved",
			`{ "model" : "gpt-5" }`,
			"reviewer",
			`{ "model" : "reviewer" }`,
		},
		{
			"chat chunk shape",
			`{"id":"chatcmpl-9","object":"chat.completion.chunk","created":123,"model":"gpt-5","choices":[{"index":0,"delta":{"role":"assistant","content":"hello \"world\""},"finish_reason":null}],"usage":{"prompt_tokens":10}}`,
			"reviewer",
			`{"id":"chatcmpl-9","object":"chat.completion.chunk","created":123,"model":"reviewer","choices":[{"index":0,"delta":{"role":"assistant","content":"hello \"world\""},"finish_reason":null}],"usage":{"prompt_tokens":10}}`,
		},
		{
			"responses envelope shape",
			`{"type":"response.created","response":{"id":"resp_1","object":"response","created_at":1700000000,"model":"gpt-5","status":"completed","output":[]},"sequence_number":1}`,
			"reviewer",
			`{"type":"response.created","response":{"id":"resp_1","object":"response","created_at":1700000000,"model":"reviewer","status":"completed","output":[]},"sequence_number":1}`,
		},
		{
			"model text inside string literal not rewritten",
			`{"a":"the \"model\" key","model":"x"}`,
			"reviewer",
			`{"a":"the \"model\" key","model":"reviewer"}`,
		},
		{
			"escaped quotes and unicode escapes in value handled",
			`{"model":"say \"hi\" \u0041!","b":1}`,
			"reviewer",
			`{"model":"reviewer","b":1}`,
		},
		{
			"public value needing escaping",
			`{"model":"gpt-5"}`,
			`a"b\c`,
			`{"model":"a\"b\\c"}`,
		},
		{
			"two siblings both rewritten",
			`{"model":"a","model":"b"}`,
			"reviewer",
			`{"model":"reviewer","model":"reviewer"}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := RewriteModel([]byte(tt.body), tt.public)
			if string(got) != tt.want {
				t.Fatalf("RewriteModel(%s, %q) =\n  %s\nwant\n  %s", tt.body, tt.public, got, tt.want)
			}
		})
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
		`""`,                           // bare string document
		``,                             // empty body
	}
	for _, body := range bodies {
		t.Run(body, func(t *testing.T) {
			got := RewriteModel([]byte(body), "reviewer")
			if string(got) != body {
				t.Fatalf("must return input unchanged:\n got  %s\n want %s", got, body)
			}
		})
	}
}

func TestRewriteModelPreservesUnrewrittenRegions(t *testing.T) {
	body := `{"id" : "chatcmpl-1","model" : "gpt-5","n" : 1,"flag" : true,"arr" : [1,2],"obj":{"k":"v"},"text":"model not here"}`
	want := `{"id" : "chatcmpl-1","model" : "reviewer","n" : 1,"flag" : true,"arr" : [1,2],"obj":{"k":"v"},"text":"model not here"}`
	got := RewriteModel([]byte(body), "reviewer")
	if string(got) != want {
		t.Fatalf("\n got  %s\n want %s", got, want)
	}
}
