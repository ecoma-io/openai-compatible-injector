package inject

import (
	"bytes"
	"encoding/json"
	"testing"
)

func TestResponsesInstructions(t *testing.T) {
	t.Run("absent sets prompt", func(t *testing.T) {
		body := `{"model":"gpt-5","input":"Explain X"}`
		out, err := Responses([]byte(body), testModel(testPrompt))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		m := decodeMap(t, out)

		var ins string
		if err := json.Unmarshal(m["instructions"], &ins); err != nil || ins != testPrompt {
			t.Fatalf("instructions = %q (err %v), want %q", ins, err, testPrompt)
		}
		var model string
		if err := json.Unmarshal(m["model"], &model); err != nil || model != "upstream-model" {
			t.Fatalf("model = %q (err %v), want upstream-model", model, err)
		}
	})

	t.Run("string prepends prompt", func(t *testing.T) {
		body := `{"model":"gpt-5","instructions":"Do X","input":"hi"}`
		out, err := Responses([]byte(body), testModel(testPrompt))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		m := decodeMap(t, out)

		var ins string
		if err := json.Unmarshal(m["instructions"], &ins); err != nil {
			t.Fatalf("decode instructions: %v", err)
		}
		if want := testPrompt + "\n\n" + "Do X"; ins != want {
			t.Fatalf("instructions = %q, want %q", ins, want)
		}
	})

	t.Run("array prepends developer item", func(t *testing.T) {
		orig := `{"type":"message","role":"user","content":[{"type":"input_text","text":"hi"}]}`
		body := `{"model":"gpt-5","instructions":[` + orig + `],"input":"hi"}`
		out, err := Responses([]byte(body), testModel(testPrompt))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		m := decodeMap(t, out)

		var items []json.RawMessage
		if err := json.Unmarshal(m["instructions"], &items); err != nil {
			t.Fatalf("decode instructions: %v", err)
		}
		if len(items) != 2 {
			t.Fatalf("got %d instruction items, want 2", len(items))
		}
		var dev struct {
			Type    string `json:"type"`
			Role    string `json:"role"`
			Content []struct {
				Type string `json:"type"`
				Text string `json:"text"`
			} `json:"content"`
		}
		if err := json.Unmarshal(items[0], &dev); err != nil {
			t.Fatalf("decode developer item: %v", err)
		}
		if dev.Type != "message" || dev.Role != "developer" || len(dev.Content) != 1 ||
			dev.Content[0].Type != "input_text" || dev.Content[0].Text != testPrompt {
			t.Fatalf("developer item = %+v", dev)
		}
		if string(items[1]) != orig {
			t.Errorf("original item changed:\n got  %s\n want %s", items[1], orig)
		}
	})

	t.Run("object untouched", func(t *testing.T) {
		body := `{"model":"gpt-5","instructions":{"mode":"auto"},"input":"hi"}`
		out, err := Responses([]byte(body), testModel(testPrompt))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		m := decodeMap(t, out)
		if want := `{"mode":"auto"}`; string(m["instructions"]) != want {
			t.Fatalf("instructions = %s, want %s", m["instructions"], want)
		}
	})

	t.Run("number untouched", func(t *testing.T) {
		body := `{"model":"gpt-5","instructions":5}`
		out, err := Responses([]byte(body), testModel(testPrompt))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		m := decodeMap(t, out)
		if want := `5`; string(m["instructions"]) != want {
			t.Fatalf("instructions = %s, want %s", m["instructions"], want)
		}
	})
}

func TestResponsesPreservesFields(t *testing.T) {
	input := `[{"type":"message","role":"user","content":[{"type":"input_text","text":"hi"},{"type":"input_image","image_url":"data:image/png;base64,AAA"}]}]`
	tools := `[{"type":"function","name":"get_weather","description":"Get weather","parameters":{"type":"object","properties":{"city":{"type":"string"}},"required":["city"]}}]`
	body := `{"model":"gpt-5","input":` + input + `,"tools":` + tools + `,"store":true,"user":"u-1","instructions":"Do X"}`
	in := decodeMap(t, []byte(body))
	out, err := Responses([]byte(body), testModel(testPrompt))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	m := decodeMap(t, out)

	for _, key := range []string{"input", "tools", "store", "user"} {
		if !bytes.Equal(m[key], in[key]) {
			t.Errorf("field %q changed:\n got  %s\n want %s", key, m[key], in[key])
		}
	}
}

func TestResponsesStringInputPreserved(t *testing.T) {
	body := `{"model":"gpt-5","input":"Explain in one sentence"}`
	in := decodeMap(t, []byte(body))
	out, err := Responses([]byte(body), testModel(testPrompt))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	m := decodeMap(t, out)
	if !bytes.Equal(m["input"], in["input"]) {
		t.Errorf("string input changed: got %s, want %s", m["input"], in["input"])
	}
}

func TestResponsesEmptyPromptSkipsOnlyInjection(t *testing.T) {
	// The model rewrite is unconditional — a model without an injection
	// prompt must still be mapped to its upstream name. Only the
	// instructions merge is skipped.
	body := `{"model":"gpt-5","instructions":"Do X","input":"hi"}`
	out, err := Responses([]byte(body), testModel(""))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if bytes.Equal(out, []byte(body)) {
		t.Fatal("model must be rewritten even with an empty prompt")
	}
	m := decodeMap(t, out)
	var model string
	if err := json.Unmarshal(m["model"], &model); err != nil || model != "upstream-model" {
		t.Fatalf("model = %q (err %v), want upstream-model", model, err)
	}
	if !bytes.Equal(m["instructions"], []byte(`"Do X"`)) {
		t.Fatalf("instructions changed with empty prompt: %s", m["instructions"])
	}
}

func TestResponsesErrors(t *testing.T) {
	for _, body := range []string{`{bad`, `null`, `[1,2]`} {
		if _, err := Responses([]byte(body), testModel(testPrompt)); err == nil {
			t.Errorf("expected error for body %q", body)
		}
	}
}
