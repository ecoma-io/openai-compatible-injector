package inject

import "testing"

func TestProbeModel(t *testing.T) {
	tests := []struct {
		name string
		body string
		want string
	}{
		{"present", `{"model":"gpt-5","stream":true}`, "gpt-5"},
		{"absent", `{"stream":false}`, ""},
		{"empty object", `{}`, ""},
		{"non-object array", `[{"model":"x"}]`, ""},
		{"bare string document", `"hi"`, ""},
		{"model non-string number", `{"model":123}`, ""},
		{"model null", `{"model":null}`, ""},
		{"model nested only", `{"response":{"model":"x"}}`, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, _, err := Probe([]byte(tt.body))
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Fatalf("model = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestProbeStream(t *testing.T) {
	tests := []struct {
		name string
		body string
		want bool
	}{
		{"absent defaults false", `{"model":"gpt-5"}`, false},
		{"true", `{"model":"gpt-5","stream":true}`, true},
		{"false", `{"model":"gpt-5","stream":false}`, false},
		{"null reads as false", `{"model":"gpt-5","stream":null}`, false},
		{"non-object array", `[1,2]`, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, got, err := Probe([]byte(tt.body))
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Fatalf("stream = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestProbeErrors(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{"truncated JSON", `{"model":`},
		{"garbage", `not json`},
		{"stream string", `{"stream":"true"}`},
		{"stream number", `{"stream":123}`},
		{"stream array", `{"stream":[]}`},
		{"stream object", `{"stream":{}}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, _, err := Probe([]byte(tt.body)); err == nil {
				t.Fatalf("expected error for %q", tt.body)
			}
		})
	}
}
