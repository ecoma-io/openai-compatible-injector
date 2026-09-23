package usage

import (
	"bytes"
	"encoding/json"
	"math"
	"strconv"
	"strings"
)

// Capture accumulates the usage facts of one request, extracted from the
// upstream's own bytes BEFORE the client-facing rewrite — the synthesized
// thinking-usage fields the proxy writes for its clients must never leak
// into the metered counts, and reading pre-rewrite is what makes that
// structural.
//
// Streaming adoption is last-wins: every usage-bearing payload replaces the
// previous observation, never sums into it. Cumulative usage chunks (some
// providers resend a running total per chunk; the OpenAI contract sends one
// final usage chunk) then converge on the authoritative final object, and
// double-counting is impossible by construction. A payload that carries no
// readable usage object leaves the capture untouched — absent usage stays
// absent, and a mid-stream parse failure is fail-open by definition.
type Capture struct {
	extract func(body []byte) (Tokens, bool)
	tokens  Tokens
	found   bool
}

// NewCapture builds the request's usage capture for one API surface:
// "chat" reads the top-level usage object (prompt/completion/total);
// "responses" reads the top-level usage of a buffered Response object, or
// the usage inside a top-level "response" envelope (the streaming events) —
// input/output/total. It descends no further: a nested "usage" inside
// client or provider payload data is not the API's usage.
func NewCapture(api string) *Capture {
	extract := ExtractResponsesUsage
	if api == "chat" {
		extract = ExtractChatUsage
	}
	return &Capture{extract: extract}
}

// Observe reads one upstream payload. Called from the relay's rewrite hook
// (the same goroutine that later reads the capture), so there is no
// synchronization; a cheap substring prefilter keeps model-bearing
// usage-less chunks — most of any stream — off the JSON path entirely.
func (c *Capture) Observe(body []byte) {
	if !bytes.Contains(body, usageKey) {
		return
	}
	tokens, ok := c.extract(body)
	if !ok {
		return
	}
	c.tokens = tokens
	c.found = true
}

// Tokens returns the observed usage: ok is false when no upstream usage
// object was seen, and the caller records NULLs.
func (c *Capture) Tokens() (Tokens, bool) { return c.tokens, c.found }

// Tokens is one usage object as the upstream reported it. Fields are
// pointers: nil is the upstream's own "this count is not stated".
type Tokens struct {
	PromptTokens     *int64 `json:"prompt_tokens"`
	CompletionTokens *int64 `json:"completion_tokens"`
	TotalTokens      *int64 `json:"total_tokens"`
}

var usageKey = []byte(`"usage"`)

// wireUsage is the union of the two API surfaces' usage objects. Each
// surface reads only its own members; the shared total_tokens is spelled
// identically by both. Members stay raw JSON: one wrongly typed count is
// that member unstated, never a reason to discard the members that did
// decode.
type wireUsage struct {
	// chat surface
	PromptTokens     json.RawMessage `json:"prompt_tokens"`
	CompletionTokens json.RawMessage `json:"completion_tokens"`
	// responses surface
	InputTokens  json.RawMessage `json:"input_tokens"`
	OutputTokens json.RawMessage `json:"output_tokens"`
	// both surfaces
	TotalTokens json.RawMessage `json:"total_tokens"`
}

// wireBody is the slice of a response document this package reads: the
// top-level usage member, and — for the Responses envelope events — the
// usage member inside the top-level response object. Everything else in
// the document is invisible here.
type wireBody struct {
	Usage    *wireUsage `json:"usage"`
	Response *struct {
		Usage *wireUsage `json:"usage"`
	} `json:"response"`
}

// ExtractChatUsage reads the Chat Completions usage object: the top-level
// "usage" member only. ok is false when the document carries no readable
// usage object (absent, null, not JSON, or wrongly typed) — the caller
// keeps whatever it had.
func ExtractChatUsage(body []byte) (Tokens, bool) {
	var wire wireBody
	if err := json.Unmarshal(body, &wire); err != nil {
		return Tokens{}, false
	}
	return tokensFromWire(wire.Usage)
}

// ExtractResponsesUsage reads the Responses usage object: the top-level
// "usage" of a buffered Response body, or — envelope event shape — the
// "usage" inside the top-level "response" object. The top level wins when
// both are present; a deterministic choice, never a sum.
func ExtractResponsesUsage(body []byte) (Tokens, bool) {
	var wire wireBody
	if err := json.Unmarshal(body, &wire); err != nil {
		return Tokens{}, false
	}
	usage := wire.Usage
	if usage == nil && wire.Response != nil {
		usage = wire.Response.Usage
	}
	return tokensFromWire(usage)
}

// tokensFromWire projects the wire union onto the surface-neutral Tokens:
// prompt/input and completion/output map onto the same pair of counts.
// ok requires at least one readable member — an object whose every count
// is unreadable is no readable usage object at all, and the caller keeps
// whatever it had.
func tokensFromWire(w *wireUsage) (Tokens, bool) {
	if w == nil {
		return Tokens{}, false
	}
	t := Tokens{
		PromptTokens:     tokenCount(w.PromptTokens),
		CompletionTokens: tokenCount(w.CompletionTokens),
		TotalTokens:      tokenCount(w.TotalTokens),
	}
	if v := tokenCount(w.InputTokens); v != nil {
		t.PromptTokens = v
	}
	if v := tokenCount(w.OutputTokens); v != nil {
		t.CompletionTokens = v
	}
	return t, t.PromptTokens != nil || t.CompletionTokens != nil || t.TotalTokens != nil
}

// tokenCount decodes one token count the way OpenAI-compatible backends
// actually emit them: a JSON integer, a float with no fractional part
// (JavaScript-generated integers can arrive as 1e3), or a string of digits.
// Anything else — a fraction, a bool, a nested object, null — is a member
// left unstated (nil), never zero.
func tokenCount(raw json.RawMessage) *int64 {
	if len(raw) == 0 || string(raw) == "null" {
		return nil
	}
	var n int64
	if err := json.Unmarshal(raw, &n); err == nil {
		return &n
	}
	var f float64
	if err := json.Unmarshal(raw, &f); err == nil && f == math.Trunc(f) &&
		f >= math.MinInt64 && f <= math.MaxInt64 {
		n = int64(f)
		return &n
	}
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		if v, perr := strconv.ParseInt(strings.TrimSpace(s), 10, 64); perr == nil {
			return &v
		}
	}
	return nil
}
