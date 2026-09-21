package inject

import (
	"testing"

	"openai-compatible-injector/internal/config"
)

func TestThinkingIntent(t *testing.T) {
	tests := []struct {
		name string
		body string
		want bool
	}{
		// reasoning_effort: a string other than "none".
		{"reasoning_effort high", `{"model":"x","reasoning_effort":"high"}`, true},
		{"reasoning_effort minimal", `{"reasoning_effort":"minimal"}`, true},
		{"reasoning_effort none", `{"reasoning_effort":"none"}`, false},
		{"reasoning_effort not a string", `{"reasoning_effort":5}`, false},
		{"reasoning_effort null", `{"reasoning_effort":null}`, false},
		// reasoning.effort: the Responses API shape.
		{"reasoning effort medium", `{"model":"x","reasoning":{"effort":"medium"}}`, true},
		{"reasoning effort none", `{"reasoning":{"effort":"none"}}`, false},
		{"reasoning effort not a string", `{"reasoning":{"effort":3}}`, false},
		{"reasoning not an object", `{"reasoning":"high"}`, false},
		{"reasoning object without effort", `{"reasoning":{"summary":"auto"}}`, false},
		// enable_thinking: literal true only.
		{"enable_thinking true", `{"model":"x","enable_thinking":true}`, true},
		{"enable_thinking false", `{"enable_thinking":false}`, false},
		{"enable_thinking string", `{"enable_thinking":"true"}`, false},
		// thinking.type: the string "enabled".
		{"thinking type enabled", `{"model":"x","thinking":{"type":"enabled"}}`, true},
		{"thinking type disabled", `{"thinking":{"type":"disabled"}}`, false},
		{"thinking type not a string", `{"thinking":{"type":1}}`, false},
		{"thinking not an object", `{"thinking":true}`, false},
		// Absence, wrong shapes, fail-open.
		{"no signal members", `{"model":"x","messages":[]}`, false},
		{"empty object", `{}`, false},
		{"empty body", ``, false},
		{"non-object document", `[{"reasoning_effort":"high"}]`, false},
		{"invalid json", `{"reasoning_effort":"high"`, false},
		{"signal at wrong depth", `{"choices":[{"reasoning_effort":"high"}]}`, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ThinkingIntent([]byte(tt.body)); got != tt.want {
				t.Fatalf("ThinkingIntent(%s) = %v, want %v", tt.body, got, tt.want)
			}
		})
	}
}

func TestThinkingPlanFor(t *testing.T) {
	active := ThinkingPlan{Active: true, Share: 0.75}
	tests := []struct {
		name string
		cfg  config.ThinkingUsage
		body string
		// draw, when non-nil, replaces the RNG and must be called exactly
		// drawCalls times.
		draw      func() float64
		drawCalls int
		want      ThinkingPlan
	}{
		{
			name: "off ignores intent and never draws",
			cfg:  config.ThinkingUsage{Mode: config.ThinkingOff, Lo: 0.9, Hi: 0.9},
			body: `{"reasoning_effort":"high"}`,
			want: ThinkingPlan{},
		},
		{
			name: "auto without intent",
			cfg:  config.ThinkingUsage{Mode: config.ThinkingAuto, Lo: 0.75, Hi: 0.75},
			body: `{"model":"x"}`,
			want: ThinkingPlan{},
		},
		{
			name: "auto with intent, fixed share",
			cfg:  config.ThinkingUsage{Mode: config.ThinkingAuto, Lo: 0.6, Hi: 0.6},
			body: `{"enable_thinking":true}`,
			want: ThinkingPlan{Active: true, Share: 0.6},
		},
		{
			name: "always without intent overrides the request",
			cfg:  config.ThinkingUsage{Mode: config.ThinkingAlways, Lo: 0.75, Hi: 0.75},
			body: `{}`,
			want: active,
		},
		{
			name: "auto with unreadable body fails open",
			cfg:  config.ThinkingUsage{Mode: config.ThinkingAuto, Lo: 0.75, Hi: 0.75},
			body: `not json`,
			want: ThinkingPlan{},
		},
		{
			name:      "ranged share draws once at the low end",
			cfg:       config.ThinkingUsage{Mode: config.ThinkingAlways, Lo: 0.2, Hi: 0.8},
			draw:      func() float64 { return 0 },
			drawCalls: 1,
			want:      ThinkingPlan{Active: true, Share: 0.2},
		},
		{
			name:      "ranged share draws once at the high end",
			cfg:       config.ThinkingUsage{Mode: config.ThinkingAlways, Lo: 0.2, Hi: 0.8},
			draw:      func() float64 { return 1 },
			drawCalls: 1,
			want:      ThinkingPlan{Active: true, Share: 0.8},
		},
		{
			name:      "ranged share draws once mid-range",
			cfg:       config.ThinkingUsage{Mode: config.ThinkingAlways, Lo: 0.2, Hi: 0.8},
			draw:      func() float64 { return 0.5 },
			drawCalls: 1,
			want:      ThinkingPlan{Active: true, Share: 0.5},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			calls := 0
			var draw func() float64
			if tt.draw != nil {
				draw = func() float64 {
					calls++
					return tt.draw()
				}
			}
			got := ThinkingPlanFor(tt.cfg, []byte(tt.body), draw)
			if got != tt.want {
				t.Fatalf("ThinkingPlanFor = %+v, want %+v", got, tt.want)
			}
			if calls != tt.drawCalls {
				t.Fatalf("draw called %d times, want %d", calls, tt.drawCalls)
			}
		})
	}
}

// TestThinkingPlanForZeroConfigIsOff pins the default: a model entry without
// a thinking-usage block is the zero value, and the zero value is an
// inactive plan for every body — the structural guarantee that absent
// configuration keeps responses byte-identical.
func TestThinkingPlanForZeroConfigIsOff(t *testing.T) {
	var zero config.ThinkingUsage
	if zero.Mode != config.ThinkingOff {
		t.Fatalf("zero-value ThinkingUsage mode = %v, want ThinkingOff", zero.Mode)
	}
	calls := 0
	got := ThinkingPlanFor(zero, []byte(`{"reasoning_effort":"high"}`), func() float64 { calls++; return 0 })
	if got.Active {
		t.Fatalf("zero-value config produced an active plan: %+v", got)
	}
	if calls != 0 {
		t.Fatalf("zero-value config drew a share %d times", calls)
	}
}
