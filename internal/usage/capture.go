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
// Streaming adoption is last-wins WITHIN one upstream call: every
// usage-bearing payload replaces the previous observation, never sums into
// it. Cumulative usage chunks (some providers resend a running total per
// chunk; the OpenAI contract sends one final usage chunk) then converge on
// the authoritative final object, and double-counting is impossible by
// construction. A payload that carries no readable usage object leaves the
// capture untouched — absent usage stays absent, and a mid-stream parse
// failure is fail-open by definition.
//
// Across upstream calls the rule is different, and Seal is the boundary
// between the two. One request may be answered by more than one upstream
// call: the post-commitment stream-continuation loop re-asks the committed
// candidate and relays its events into the same client stream, so the client
// received text from every call. The caller calls Seal once at the start of
// each upstream response body; what a sealed call observed is folded into a
// running aggregate, and Tokens reports the aggregate of every call. The
// counts are treated differently on purpose — see foldTokens.
type Capture struct {
	extract func(body []byte) (Tokens, bool)
	// tokens/found are the CURRENT upstream call's observation, last-wins.
	tokens Tokens
	found  bool
	// sealed/sealedFound are every PREVIOUS call of this request, already
	// folded. They stay the zero value for a request answered by one call,
	// which is every request that does not use stream recovery.
	sealed      Tokens
	sealedFound bool
}

// NewCapture builds the request's usage capture for one API surface:
// "responses" reads the top-level usage of a buffered Response object, or
// the usage inside a top-level "response" envelope (the streaming events) —
// input/output/total; everything else — "chat" and the Anthropic
// "messages" surface — reads the top-level usage object
// (prompt/completion/total). Messages rides the chat extractor because the
// bytes Observe reads are ALWAYS pre-rewrite upstream bytes, and a
// Messages request's upstream traffic is Chat Completions. It descends no
// further: a nested "usage" inside client or provider payload data is not
// the API's usage.
func NewCapture(api string) *Capture {
	extract := ExtractChatUsage
	if api == "responses" {
		extract = ExtractResponsesUsage
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

// Seal closes the current upstream call's observation and folds it into the
// request's aggregate. The caller invokes it exactly once per upstream
// response body, at the start of the pass — never at the end — so the call
// that is still running stays live and its own usage is not folded twice.
//
// It does two jobs, and the second is not a formality. Folding is the first:
// a settled call's counts join the aggregate instead of replacing it. The
// second is that a call which never states usage must not leave the PREVIOUS
// call's object standing as the current observation — after a Seal, the next
// call's own report is the first thing this capture has seen since, so
// last-wins means last-wins within one call and cannot leak a hop's usage
// into the hop that follows it.
//
// Sealing an observation that never happened is a no-op, so a caller may
// call it unconditionally at every boundary.
func (c *Capture) Seal() {
	if c.found {
		c.sealed = foldTokens(c.sealed, c.tokens)
		c.sealedFound = true
	}
	c.tokens, c.found = Tokens{}, false
}

// Tokens returns the request's observed usage: the aggregate of every sealed
// upstream call folded with the call still in flight. ok is false only when
// NO call stated a readable usage object, and the caller records NULLs — an
// unstated count is never reported as a zero.
func (c *Capture) Tokens() (Tokens, bool) {
	switch {
	case c.found && c.sealedFound:
		return foldTokens(c.sealed, c.tokens), true
	case c.found:
		return c.tokens, true
	case c.sealedFound:
		return c.sealed, true
	}
	return Tokens{}, false
}

// Tokens is one usage object as the upstream reported it. Fields are
// pointers: nil is the upstream's own "this count is not stated".
type Tokens struct {
	PromptTokens     *int64 `json:"prompt_tokens"`
	CompletionTokens *int64 `json:"completion_tokens"`
	TotalTokens      *int64 `json:"total_tokens"`
}

// foldTokens merges two usage observations into one aggregate, applying the
// counts' own semantics rather than a blanket sum. b is the later
// observation and wins wherever the two disagree about a quantity that is
// not additive.
//
// The two counts are NOT treated alike, because they do not measure the same
// thing across calls. A continuation call re-asks with everything the
// previous call had, so the prompt is the same conversation seen again: the
// last call that stated one describes the context that actually ran, and
// summing prompts would count that conversation once per hop. The completion
// is what the client got: each call produced its own slice of the answer, so
// the slices add up and the total the client read is their sum.
//
// total_tokens is therefore not carried across calls either — it is restated
// as the sum of the two counts the row itself reports, so the three columns
// can never contradict each other. When that sum is unknowable because
// neither count is stated, the most recently stated total is the only fact
// left and is kept: a call that stated only a total still contributes it.
func foldTokens(a, b Tokens) Tokens {
	var out Tokens
	out.PromptTokens = b.PromptTokens
	if out.PromptTokens == nil {
		out.PromptTokens = a.PromptTokens
	}
	out.CompletionTokens = addCount(a.CompletionTokens, b.CompletionTokens)
	out.TotalTokens = addCount(out.PromptTokens, out.CompletionTokens)
	if out.TotalTokens == nil {
		out.TotalTokens = b.TotalTokens
		if out.TotalTokens == nil {
			out.TotalTokens = a.TotalTokens
		}
	}
	return out
}

// addCount adds two token counts, keeping the pointer rule the rest of this
// package follows: a count either side states is stated in the result, and a
// count NEITHER states stays nil. Never a fabricated zero — an unstated
// count is not a zero — while a stated zero folds as the count it is.
func addCount(a, b *int64) *int64 {
	switch {
	case a == nil && b == nil:
		return nil
	case a == nil:
		return b
	case b == nil:
		return a
	}
	n := *a + *b
	return &n
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
