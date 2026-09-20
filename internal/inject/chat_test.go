package inject

import (
	"bytes"
	"encoding/json"
	"testing"

	"openai-compatible-injector/internal/config"
)

const testPrompt = "You are a helpful assistant."

// testModel builds a config.Model with a fixed public/upstream pair and the
// given injection prompt.
func testModel(prompt string) config.Model {
	return config.Model{Public: "public", UpstreamModel: "upstream-model", InjectionPrompt: prompt}
}

func decodeMap(t *testing.T, b []byte) map[string]json.RawMessage {
	t.Helper()
	var m map[string]json.RawMessage
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatalf("decode output: %v\n%s", err, b)
	}
	return m
}

func chatMessages(t *testing.T, m map[string]json.RawMessage) []json.RawMessage {
	t.Helper()
	raw, ok := m["messages"]
	if !ok {
		t.Fatalf("no messages field in transformed body: %s", m["model"])
	}
	var msgs []json.RawMessage
	if err := json.Unmarshal(raw, &msgs); err != nil {
		t.Fatalf("decode messages: %v", err)
	}
	return msgs
}

func TestChatInjectsSystemMessage(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{"no existing messages", `{"model":"gpt-5","messages":[]}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			out, err := Chat([]byte(tt.body), testModel(testPrompt))
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			m := decodeMap(t, out)

			var model string
			if err := json.Unmarshal(m["model"], &model); err != nil || model != "upstream-model" {
				t.Fatalf("model = %q (err %v), want upstream-model", model, err)
			}

			msgs := chatMessages(t, m)
			if len(msgs) != 1 {
				t.Fatalf("got %d messages, want 1", len(msgs))
			}
			var first struct {
				Role    string `json:"role"`
				Content string `json:"content"`
			}
			if err := json.Unmarshal(msgs[0], &first); err != nil {
				t.Fatalf("decode injected message: %v", err)
			}
			if first.Role != "system" || first.Content != testPrompt {
				t.Fatalf("injected message = %+v, want system/%q", first, testPrompt)
			}
		})
	}
}

// TestChatMessagesAbsentSkipsInjection pins the contract that injection is
// skipped (not synthesized) when the request has no "messages" array at all;
// the model rewrite still applies and no messages field is added.
func TestChatMessagesAbsentSkipsInjection(t *testing.T) {
	body := `{"model":"gpt-5"}`
	out, err := Chat([]byte(body), testModel(testPrompt))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	m := decodeMap(t, out)
	if _, ok := m["messages"]; ok {
		t.Fatalf("messages field must stay absent, got %s", m["messages"])
	}
	var model string
	if err := json.Unmarshal(m["model"], &model); err != nil || model != "upstream-model" {
		t.Fatalf("model = %q (err %v), want upstream-model", model, err)
	}
}

func TestChatPreservesMessages(t *testing.T) {
	originals := []string{
		`{"role":"user","content":"hi"}`,
		`{"role":"system","content":"be nice"}`,
		`{"role":"assistant","content":"ok"}`,
		`{"role":"tool","tool_call_id":"call_1","content":"42"}`,
		`{"role":"assistant","content":"","tool_calls":[{"id":"call_2","type":"function","function":{"name":"get_weather","arguments":"{\"city\":\"SF\"}"}}]}`,
		`{"role":"user","content":[{"type":"text","text":"look"},{"type":"image_url","image_url":{"url":"data:image/png;base64,AAA"}}]}`,
	}
	body := `{"model":"gpt-5","messages":[` + joinRaw(originals) + `]}`

	out, err := Chat([]byte(body), testModel(testPrompt))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	m := decodeMap(t, out)
	msgs := chatMessages(t, m)

	if len(msgs) != len(originals)+1 {
		t.Fatalf("got %d messages, want %d", len(msgs), len(originals)+1)
	}

	// Injected system message sits at messages[0]; every original follows in
	// order and is byte-identical.
	var injected struct {
		Role    string `json:"role"`
		Content string `json:"content"`
	}
	if err := json.Unmarshal(msgs[0], &injected); err != nil {
		t.Fatalf("decode injected message: %v", err)
	}
	if injected.Role != "system" || injected.Content != testPrompt {
		t.Fatalf("injected message = %+v, want system/%q", injected, testPrompt)
	}
	for i, orig := range originals {
		if string(msgs[i+1]) != orig {
			t.Errorf("message %d changed:\n got  %s\n want %s", i, msgs[i+1], orig)
		}
	}
}

func joinRaw(msgs []string) string {
	out := msgs[0]
	for _, m := range msgs[1:] {
		out += "," + m
	}
	return out
}

func TestChatPreservesUnknownFields(t *testing.T) {
	body := `{"model":"gpt-5","messages":[],"temperature":0.7,"stream":true,"n":2,"stream_options":{"include_usage":true},"x-custom":{"deep":[1,2]}}`
	in := decodeMap(t, []byte(body))
	out, err := Chat([]byte(body), testModel(testPrompt))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	m := decodeMap(t, out)

	for _, key := range []string{"temperature", "stream", "n", "stream_options", "x-custom"} {
		if !bytes.Equal(m[key], in[key]) {
			t.Errorf("field %q changed:\n got  %s\n want %s", key, m[key], in[key])
		}
	}
	chatMessages(t, m) // injection still happened
}

func TestChatStreamDetection(t *testing.T) {
	tests := []struct {
		name     string
		body     string
		wantBool bool
	}{
		{"stream true", `{"model":"gpt-5","stream":true}`, true},
		{"stream false", `{"model":"gpt-5","stream":false}`, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			in := decodeMap(t, []byte(tt.body))
			out, err := Chat([]byte(tt.body), testModel(testPrompt))
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			m := decodeMap(t, out)
			if !bytes.Equal(m["stream"], in["stream"]) {
				t.Errorf("stream field changed: got %s, want %s", m["stream"], in["stream"])
			}
			model, stream, err := Probe(out)
			if err != nil {
				t.Fatalf("probe transformed body: %v", err)
			}
			if model != "upstream-model" || stream != tt.wantBool {
				t.Fatalf("probe = (%q, %v), want (upstream-model, %v)", model, stream, tt.wantBool)
			}
		})
	}
}

func TestChatEmptyPromptNoInjection(t *testing.T) {
	body := `{"model":"gpt-5","messages":[{"role":"user","content":"hi"}]}`
	out, err := Chat([]byte(body), testModel(""))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	m := decodeMap(t, out)

	var model string
	if err := json.Unmarshal(m["model"], &model); err != nil || model != "upstream-model" {
		t.Fatalf("model = %q (err %v), want upstream-model", model, err)
	}
	in := decodeMap(t, []byte(body))
	if !bytes.Equal(m["messages"], in["messages"]) {
		t.Errorf("messages changed without prompt:\n got  %s\n want %s", m["messages"], in["messages"])
	}
}

func TestChatMalformedMessagesSkipsInjection(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{"messages object", `{"model":"gpt-5","messages":{"role":"user","content":"hi"}}`},
		{"messages null", `{"model":"gpt-5","messages":null}`},
		{"messages string", `{"model":"gpt-5","messages":"hi"}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			in := decodeMap(t, []byte(tt.body))
			out, err := Chat([]byte(tt.body), testModel(testPrompt))
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			m := decodeMap(t, out)
			if !bytes.Equal(m["messages"], in["messages"]) {
				t.Errorf("malformed messages were touched:\n got  %s\n want %s", m["messages"], in["messages"])
			}
			var model string
			if err := json.Unmarshal(m["model"], &model); err != nil || model != "upstream-model" {
				t.Fatalf("model = %q (err %v), want upstream-model", model, err)
			}
		})
	}
}

// TestChatEdgeArrays pins the array-shape edges of the injection rule: an
// empty messages array is a perfectly good injection target (the system
// message becomes messages[0]), and an array whose members are not objects
// still gets the prepend with every original member carried through as raw
// JSON — the injected message must never displace or re-type client data.
func TestChatEdgeArrays(t *testing.T) {
	t.Run("empty array receives the system message", func(t *testing.T) {
		out, err := Chat([]byte(`{"model":"gpt-5","messages":[]}`), testModel(testPrompt))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		m := decodeMap(t, out)
		var msgs []json.RawMessage
		if err := json.Unmarshal(m["messages"], &msgs); err != nil {
			t.Fatalf("decode messages: %v", err)
		}
		if len(msgs) != 1 {
			t.Fatalf("got %d messages, want 1 (the injected system message)", len(msgs))
		}
		var first map[string]any
		if err := json.Unmarshal(msgs[0], &first); err != nil {
			t.Fatalf("decode messages[0]: %v", err)
		}
		if first["role"] != "system" || first["content"] != testPrompt {
			t.Fatalf("messages[0] = %v, want the injected system message", first)
		}
	})

	t.Run("non-object members preserved through the prepend", func(t *testing.T) {
		body := `{"model":"gpt-5","messages":["plain",123,null,{"role":"user","content":"hi"}]}`
		out, err := Chat([]byte(body), testModel(testPrompt))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		m := decodeMap(t, out)
		var msgs []json.RawMessage
		if err := json.Unmarshal(m["messages"], &msgs); err != nil {
			t.Fatalf("decode messages: %v", err)
		}
		if len(msgs) != 5 {
			t.Fatalf("got %d messages, want 5", len(msgs))
		}
		// First is the injection; the original members follow byte-preserved
		// in their original order.
		if string(msgs[1]) != `"plain"` || string(msgs[2]) != `123` || string(msgs[3]) != `null` {
			t.Fatalf("original members not preserved: %s %s %s", msgs[1], msgs[2], msgs[3])
		}
		var last map[string]any
		if err := json.Unmarshal(msgs[4], &last); err != nil || last["content"] != "hi" {
			t.Fatalf("messages[4] = %v, want the original user message", msgs[4])
		}
	})
}

func TestChatErrors(t *testing.T) {
	for _, body := range []string{`{bad`, `null`, `[1,2]`} {
		if _, err := Chat([]byte(body), testModel(testPrompt)); err == nil {
			t.Errorf("expected error for body %q", body)
		}
	}
}
