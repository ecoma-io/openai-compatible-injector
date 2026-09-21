package inject

import (
	"encoding/json"
	"math/rand/v2"

	"openai-compatible-injector/internal/config"
)

// ThinkingPlan is the per-request synthesis decision the response paths
// share: whether simulated thinking-usage synthesis is active, and the share
// of the upstream-reported output tokens attributed to thinking. The handler
// resolves it once per request, before any upstream I/O, so every usage
// object that request returns — buffered or streamed, first chunk or last —
// reports the same share, and a config reload mid-request cannot change it.
type ThinkingPlan struct {
	Active bool
	Share  float64
}

// ThinkingIntent reports whether a client request body signals thinking. It
// is the auto mode's gate, read from the body the client sent. Any one of
// these top-level members counts as a signal:
//
//   - "reasoning_effort" — a JSON string other than "none"
//   - "reasoning" — an object whose "effort" member is a string other than
//     "none" (the Responses API shape)
//   - "enable_thinking" — the JSON boolean true
//   - "thinking" — an object whose "type" member is the string "enabled"
//
// A member of the wrong JSON type is not a signal (a number where a string is
// expected means some other dialect, not thinking), and nothing here ever
// errors: any parse trouble reports no intent — fail-open, so an unreadable
// body can only lose the synthesis, never corrupt anything.
func ThinkingIntent(body []byte) bool {
	if !json.Valid(body) {
		return false
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(body, &fields); err != nil || fields == nil {
		return false
	}
	if s, ok := jsonString(fields["reasoning_effort"]); ok && s != "none" {
		return true
	}
	if raw := fields["reasoning"]; len(raw) > 0 && firstByte(raw) == '{' {
		var inner map[string]json.RawMessage
		if err := json.Unmarshal(raw, &inner); err == nil && inner != nil {
			if s, ok := jsonString(inner["effort"]); ok && s != "none" {
				return true
			}
		}
	}
	var enable *bool
	if err := json.Unmarshal(fields["enable_thinking"], &enable); err == nil && enable != nil && *enable {
		return true
	}
	if raw := fields["thinking"]; len(raw) > 0 && firstByte(raw) == '{' {
		var inner map[string]json.RawMessage
		if err := json.Unmarshal(raw, &inner); err == nil && inner != nil {
			if s, ok := jsonString(inner["type"]); ok && s == "enabled" {
				return true
			}
		}
	}
	return false
}

// ThinkingPlanFor resolves a model's thinking-usage config against a request
// body into the per-request plan. off and always are operator overrides of
// the request's own signal — the body is not even scanned for them; auto
// follows the signal. The share is drawn once, here: Lo == Hi is a fixed
// share with no draw, otherwise the draw function is consulted a single time
// and must return a value in [0,1] (math/rand/v2 Float64 does; tests inject
// stubs; nil falls back to it). Every seam of the request reuses the
// returned plan verbatim.
func ThinkingPlanFor(cfg config.ThinkingUsage, body []byte, draw func() float64) ThinkingPlan {
	switch cfg.Mode {
	case config.ThinkingOff:
		return ThinkingPlan{}
	case config.ThinkingAuto:
		if !ThinkingIntent(body) {
			return ThinkingPlan{}
		}
	}
	if cfg.Lo == cfg.Hi {
		return ThinkingPlan{Active: true, Share: cfg.Lo}
	}
	if draw == nil {
		draw = rand.Float64
	}
	return ThinkingPlan{Active: true, Share: cfg.Lo + draw()*(cfg.Hi-cfg.Lo)}
}

// jsonString decodes raw as a JSON string; ok reports whether it is one.
// JSON null decodes into a string as "" without error, so it is rejected
// explicitly: an absent-minded null must not read as an (empty) signal.
func jsonString(raw json.RawMessage) (s string, ok bool) {
	if len(raw) == 0 || string(raw) == "null" {
		return "", false
	}
	if err := json.Unmarshal(raw, &s); err != nil {
		return "", false
	}
	return s, true
}
