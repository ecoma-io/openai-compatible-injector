package inject

import (
	"strings"
	"testing"
)

// visionBody is a chat request whose first user message content is the given
// parts JSON (or a plain string when parts is empty).
func visionBody(parts string) []byte {
	if parts == "" {
		return []byte(`{"model":"gpt-5","messages":[{"role":"user","content":"plain text"}]}`)
	}
	return []byte(`{"model":"gpt-5","messages":[{"role":"user","content":[` + parts + `]}]}`)
}

// TestHasImageChatShape pins the chat shapes the scanner reads. A body
// carrying image_url reports true; a parts array of text-only parts and a
// plain string content report false even when their bytes contain the
// gate's substring (a string that merely spells "image_url" is prose, not a
// part).
func TestHasImageChatShape(t *testing.T) {
	cases := []struct {
		name string
		body []byte
		want bool
	}{
		{"text only parts", visionBody(`{"type":"text","text":"hello"}`), false},
		{"image_url part", visionBody(`{"type":"image_url","image_url":{"url":"https://a/x.png"}}`), true},
		{"image_url quoted in prose", []byte(`{"model":"gpt-5","messages":[{"role":"user","content":"how do I spell image_url in json?"}]}`), false},
		{"image_url in tool schema", []byte(`{"model":"gpt-5","messages":[{"role":"user","content":"hi"}],"tools":[{"type":"function","function":{"name":"f","parameters":{"type":"image_url"}}}]}`), false},
		{"data uri string content", []byte(`{"model":"gpt-5","messages":[{"role":"user","content":"look at data:image/png;base64,AAAA"}]}`), true},
		{"data uri in another url", visionBody(`{"type":"text","text":"https://a/?r=data:image/png;base64,AAAA"}`), false},
		// Ollama/Hermes message-level images array.
		{"message images array", []byte(`{"model":"gpt-5","messages":[{"role":"user","content":"what is this?","images":["base64data"]}]}`), true},
		{"message images empty", []byte(`{"model":"gpt-5","messages":[{"role":"user","content":"what is this?","images":[]}]}`), false},
		// Anthropic Messages shape (the body before translation).
		{"anthropic image block", []byte(`{"model":"claude-x","messages":[{"role":"user","content":[{"type":"image","source":{"type":"base64","media_type":"image/png","data":"AAAA"}}]}]}`), true},
		{"anthropic text block", []byte(`{"model":"claude-x","messages":[{"role":"user","content":[{"type":"text","text":"look at this"}]}]}`), false},
		// Responses shape.
		{"responses input_image", []byte(`{"model":"gpt-5","input":[{"type":"input_image","image_url":"https://a/x.png"}]}`), true},
		{"responses nested in message", []byte(`{"model":"gpt-5","input":[{"type":"message","content":[{"type":"input_image","image_url":"https://a/x.png"}]}]}`), true},
		{"responses text only", []byte(`{"model":"gpt-5","input":[{"role":"user","content":"plain"}]}`), false},
		// Non-object and invalid JSON, and absent messages.
		{"empty object", []byte(`{}`), false},
		{"array body", []byte(`[1,2]`), false},
		{"invalid json", []byte(`{"model":"gpt-5","messages":[{"role":"user","content":[{"type":"image_url"`), false},
		{"messages not array", []byte(`{"messages":{"role":"user","content":[{"type":"image_url","image_url":{"url":"x"}}]}}`), false},
		{"spaced type image", []byte(`{"model":"gpt-5","messages":[{"role":"user","content":[{"type": "image"}]}]}`), true},
	}
	for _, tc := range cases {
		if got := HasImage(tc.body); got != tc.want {
			t.Errorf("%s: HasImage = %v, want %v (body: %s)", tc.name, got, tc.want, tc.body)
		}
	}
}

// TestHasImageNoAllocationOrCorruptionOnLargeText exercises the substring
// gate's cost on a body whose text is long but carries no marker — the scan
// must stay cheap and must never touch the input.
func TestHasImageDoesNotModifyBody(t *testing.T) {
	body := []byte(`{"model":"gpt-5","messages":[{"role":"user","content":"` +
		strings.Repeat("no markers here. ", 500) + `"}]}`)
	before := string(body)
	HasImage(body)
	if string(body) != before {
		t.Error("HasImage modified its input")
	}
}
