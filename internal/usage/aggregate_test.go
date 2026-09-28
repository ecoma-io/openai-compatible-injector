package usage

import (
	"fmt"
	"testing"
)

// The aggregation contract, clause by clause. AGENTS.md states it as: the
// capture is sealed at each upstream-call boundary and the event reports the
// aggregate of every call the request was assembled from — the prompt from the
// LAST call that stated one, the completion SUMMED across calls, the total
// RESTATED as that row's own sum, never a fabricated zero; and WITHIN one
// upstream call, streamed usage is last-readable-object wins, never a sum.
//
// Every test below names the clause it pins and what would break if it
// regressed. The scenarios are ordered as the brief lists them.

// Scenario 1: one call, one usage object — no aggregation happens at all, so
// every stated count is passed through unchanged. If this regressed, the
// common case (a buffered response, or a stream whose only usage chunk is its
// last) would be metered as something other than what the upstream said.
func TestCaptureSingleCallPassesUsageThrough(t *testing.T) {
	c := NewCapture("chat")
	c.Observe([]byte(`{"id":"chatcmpl-1","model":"public","usage":{"prompt_tokens":10,"completion_tokens":20,"total_tokens":30}}`))

	tokens, ok := c.Tokens()
	if !ok {
		t.Fatal("a single readable usage object was not reported")
	}
	assertTokens(t, tokens, Tokens{PromptTokens: i64(10), CompletionTokens: i64(20), TotalTokens: i64(30)})

	// The Responses surface gets the same treatment through its own extractor.
	r := NewCapture("responses")
	r.Observe([]byte(`{"usage":{"input_tokens":11,"output_tokens":22,"total_tokens":33}}`))
	tokens, ok = r.Tokens()
	if !ok {
		t.Fatal("a single readable usage object was not reported on the responses surface")
	}
	assertTokens(t, tokens, Tokens{PromptTokens: i64(11), CompletionTokens: i64(22), TotalTokens: i64(33)})
}

// Scenario 2: one call, several usage objects — the cumulative-stream shape.
// Last-readable-object wins WHOLE; a sum would double-count a provider that
// resends a running total per chunk, which is the failure this rule exists to
// make impossible.
func TestCaptureSingleCallLastUsageObjectWinsNeverSums(t *testing.T) {
	c := NewCapture("chat")
	c.Observe([]byte(`{"usage":{"prompt_tokens":10,"completion_tokens":5,"total_tokens":15}}`))
	c.Observe([]byte(`{"usage":{"prompt_tokens":10,"completion_tokens":12,"total_tokens":22}}`))
	c.Observe([]byte(`{"usage":{"prompt_tokens":10,"completion_tokens":20,"total_tokens":30}}`))

	tokens, ok := c.Tokens()
	if !ok {
		t.Fatal("no usage after a cumulative stream")
	}
	// 10/20/30 — NOT 30/37/67, which is what a summing capture would report.
	assertTokens(t, tokens, Tokens{PromptTokens: i64(10), CompletionTokens: i64(20), TotalTokens: i64(30)})
}

// Scenario 3: a usage-LESS payload must leave the earlier object standing.
// Every non-final chunk of an OpenAI stream is this shape, and the capture is
// fed every data line; if a usage-less payload cleared the capture, streaming
// metering would report NULL for almost every real response.
func TestCaptureUsageLessPayloadLeavesEarlierUsageStanding(t *testing.T) {
	want := Tokens{PromptTokens: i64(10), CompletionTokens: i64(20), TotalTokens: i64(30)}
	c := NewCapture("chat")
	c.Observe([]byte(`{"usage":{"prompt_tokens":10,"completion_tokens":20,"total_tokens":30}}`))

	for _, payload := range []string{
		`{"id":"x","choices":[{"delta":{"content":"hi"}}]}`,
		`{"id":"x","choices":[],"usage":null}`,
		`{"id":"x","usage":{}}`,
		`{"id":"x","usage":"30 tokens"}`,
		`{"id":"x","usage":10}`,
		`{"id":"x","usage":[]}`,
	} {
		c.Observe([]byte(payload))
		tokens, ok := c.Tokens()
		if !ok {
			t.Fatalf("payload %s cleared the capture", payload)
		}
		assertTokens(t, tokens, want)
	}
}

// Scenario 4a: a usage object followed by an UNREADABLE usage object — the
// earlier object stands. Mid-stream garbage is fail-open by definition: a
// relay that meters a malformed chunk must not lose the real usage it already
// read, and must never invent a zero for it.
func TestCaptureUnreadableUsageLeavesEarlierUsageStanding(t *testing.T) {
	want := Tokens{PromptTokens: i64(10), CompletionTokens: i64(20), TotalTokens: i64(30)}
	c := NewCapture("chat")
	c.Observe([]byte(`{"usage":{"prompt_tokens":10,"completion_tokens":20,"total_tokens":30}}`))

	for _, payload := range []string{
		`{"usage":{`, // malformed JSON
		`{"usage":{"prompt_tokens":12.5,"completion_tokens":true,"total_tokens":{"a":1}}}`, // every member unreadable
		`{"usage":true}`,  // wrong type
		`{"usage":"10"}`,  // wrong type
		`{"usage":null}`,  // the explicit "not stated"
		`{"usage":[1,2]}`, // wrong type
	} {
		c.Observe([]byte(payload))
		tokens, ok := c.Tokens()
		if !ok {
			t.Fatalf("payload %s cleared the capture", payload)
		}
		assertTokens(t, tokens, want)
	}
}

// Scenario 4b: a per-member unreadable count leaves THAT member unstated
// rather than zero. The object is still a readable usage object (it stated
// some member), so it wins whole — and the member it could not read must be
// NULL in the event, never 0. A fabricated zero here would understate
// consumption on exactly the providers whose counts are hardest to parse.
func TestCaptureUnreadableMemberIsUnstatedNotZero(t *testing.T) {
	c := NewCapture("chat")
	c.Observe([]byte(`{"usage":{"prompt_tokens":10,"completion_tokens":12.5,"total_tokens":30}}`))

	tokens, ok := c.Tokens()
	if !ok {
		t.Fatal("one unreadable member discarded the whole usage object")
	}
	assertTokens(t, tokens, Tokens{PromptTokens: i64(10), TotalTokens: i64(30)})

	// The same rule inside one call: last-readable-object wins WHOLE, so a
	// later object that cannot state a member does not inherit the earlier
	// one's. The member it could not read is unstated — nil, never 0 and never
	// the earlier value silently carried forward.
	d := NewCapture("chat")
	d.Observe([]byte(`{"usage":{"prompt_tokens":10,"completion_tokens":20,"total_tokens":30}}`))
	d.Observe([]byte(`{"usage":{"prompt_tokens":10,"completion_tokens":12.5,"total_tokens":10}}`))
	tokens, _ = d.Tokens()
	assertTokens(t, tokens, Tokens{PromptTokens: i64(10), TotalTokens: i64(10)})
}

// Scenario 5: two calls, both stating a prompt — the LAST call's prompt wins
// (a continuation re-asks with the same conversation, so summing prompts
// would count the context once per hop), the completions add up (the client
// read every call's text), and the total is restated as that row's own sum
// rather than either upstream's per-call total.
func TestCaptureSecondCallPromptWinsAndCompletionsSum(t *testing.T) {
	c := NewCapture("chat")
	c.Observe([]byte(`{"usage":{"prompt_tokens":100,"completion_tokens":10,"total_tokens":110}}`))
	c.Seal()
	c.Observe([]byte(`{"usage":{"prompt_tokens":120,"completion_tokens":5,"total_tokens":125}}`))

	tokens, ok := c.Tokens()
	if !ok {
		t.Fatal("no usage after two calls")
	}
	// prompt: 120 (last stated), completion: 15 (10+5), total: 135 (the row's
	// own 120+15) — never 220, never 110+125.
	assertTokens(t, tokens, Tokens{PromptTokens: i64(120), CompletionTokens: i64(15), TotalTokens: i64(135)})
}

// Scenario 6: two calls, only the FIRST stating a prompt. The later call not
// stating one must not erase it — "the last call that STATED one" is not "the
// last call", and a continuation that reports only its own completion still
// describes a conversation whose context is the prompt the first call ran.
func TestCaptureSecondCallWithoutPromptKeepsTheFirst(t *testing.T) {
	c := NewCapture("chat")
	c.Observe([]byte(`{"usage":{"prompt_tokens":100,"completion_tokens":10,"total_tokens":110}}`))
	c.Seal()
	c.Observe([]byte(`{"usage":{"completion_tokens":7,"total_tokens":7}}`))

	tokens, ok := c.Tokens()
	if !ok {
		t.Fatal("no usage after two calls")
	}
	// prompt: 100 retained; completion: 17; total restated as 100+17.
	assertTokens(t, tokens, Tokens{PromptTokens: i64(100), CompletionTokens: i64(17), TotalTokens: i64(117)})
}

// Scenario 7: the retained-total rule and its boundaries. A total only counts
// when no count is knowable: once either count is stated the total is
// restated as the row's own sum, and a later stated total supersedes an
// earlier one only while the sum stays unknowable.
func TestCaptureRetainedTotalBoundaries(t *testing.T) {
	cases := []struct {
		name     string
		first    string
		second   string
		want     Tokens
		whyTheUp string
	}{
		{
			name:   "a silent later call keeps a lone stated total",
			first:  `{"usage":{"total_tokens":30}}`,
			second: `{"usage":{"other":1}}`,
			want:   Tokens{TotalTokens: i64(30)},
		},
		{
			name:   "the last stated total wins while no count is known",
			first:  `{"usage":{"total_tokens":30}}`,
			second: `{"usage":{"total_tokens":12}}`,
			want:   Tokens{TotalTokens: i64(12)},
		},
		{
			name:   "a stated count retires a lone unpaired total",
			first:  `{"usage":{"total_tokens":30}}`,
			second: `{"usage":{"prompt_tokens":4}}`,
			want:   Tokens{PromptTokens: i64(4), TotalTokens: i64(4)},
		},
		{
			name:   "a retained total is discarded once both counts are known",
			first:  `{"usage":{"total_tokens":30}}`,
			second: `{"usage":{"prompt_tokens":4,"completion_tokens":6}}`,
			want:   Tokens{PromptTokens: i64(4), CompletionTokens: i64(6), TotalTokens: i64(10)},
		},
		{
			name:   "a retained total survives a later call that states no total",
			first:  `{"usage":{"prompt_tokens":4,"total_tokens":4}}`,
			second: `{"usage":{"completion_tokens":6}}`,
			want:   Tokens{PromptTokens: i64(4), CompletionTokens: i64(6), TotalTokens: i64(10)},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := NewCapture("chat")
			c.Observe([]byte(tc.first))
			c.Seal()
			c.Observe([]byte(tc.second))
			tokens, ok := c.Tokens()
			if !ok {
				t.Fatal("no usage after two calls")
			}
			assertTokens(t, tokens, tc.want)
		})
	}
}

// Scenario 8: a call stating a count of ZERO folds as the count it is, and is
// never confused with "unstated". `completion_tokens: 0` is a real answer (a
// refused generation, an empty completion) and must reach the event as 0 —
// while a member the upstream did not state must stay NULL.
func TestCaptureStatedZeroIsACountNotAnAbsence(t *testing.T) {
	c := NewCapture("chat")
	c.Observe([]byte(`{"usage":{"prompt_tokens":0,"completion_tokens":0,"total_tokens":0}}`))
	tokens, ok := c.Tokens()
	if !ok {
		t.Fatal("an all-zero usage object is still a usage object")
	}
	assertTokens(t, tokens, Tokens{PromptTokens: i64(0), CompletionTokens: i64(0), TotalTokens: i64(0)})

	// A zero states itself across calls: the sum is a real 0, not an absence.
	c.Seal()
	c.Observe([]byte(`{"usage":{"completion_tokens":0,"total_tokens":0}}`))
	tokens, ok = c.Tokens()
	if !ok {
		t.Fatal("stated zeros were treated as unstated")
	}
	assertTokens(t, tokens, Tokens{PromptTokens: i64(0), CompletionTokens: i64(0), TotalTokens: i64(0)})

	// And the contrast: a call that states nothing at all contributes nothing,
	// so the aggregate keeps the count it was given — its total restated as the
	// row's own sum — rather than inventing a completion.
	d := NewCapture("chat")
	d.Observe([]byte(`{"usage":{"prompt_tokens":5}}`))
	d.Seal()
	d.Observe([]byte(`{"id":"x","choices":[]}`))
	tokens, ok = d.Tokens()
	if !ok {
		t.Fatal("a call stating nothing erased the earlier call")
	}
	assertTokens(t, tokens, Tokens{PromptTokens: i64(5), TotalTokens: i64(5)})
}

// Scenario 9: a call stating nothing at all leaves the aggregate unchanged and
// fabricates no zero. The boundary (Seal) must be able to run against an
// observation that never happened, on every hop, without manufacturing usage.
func TestCaptureCallStatingNothingLeavesAggregateUnchanged(t *testing.T) {
	c := NewCapture("chat")
	c.Observe([]byte(`{"usage":{"prompt_tokens":10,"completion_tokens":20,"total_tokens":30}}`))
	c.Seal()
	// A whole call that states nothing — no usage object anywhere in any of
	// its payloads. It contributes no counts and no zero.
	c.Observe([]byte(`{"id":"x","choices":[{"delta":{"content":"more"}}]}`))
	c.Observe([]byte(`{"id":"x","choices":[],"usage":null}`))
	c.Seal()

	tokens, ok := c.Tokens()
	if !ok {
		t.Fatal("the silent call erased the request's usage")
	}
	assertTokens(t, tokens, Tokens{PromptTokens: i64(10), CompletionTokens: i64(20), TotalTokens: i64(30)})

	// Nothing observed, ever: the caller must be told "unstated", not handed a
	// zeroed Tokens that reads as a real 0/0/0 measurement.
	empty := NewCapture("responses")
	empty.Seal()
	empty.Observe([]byte(`{"id":"x"}`))
	if tokens, ok := empty.Tokens(); ok {
		t.Fatalf("a capture that never observed usage reported %s", render(tokens))
	}
}

// Scenario 10: the three columns can never contradict each other. Whenever the
// row states a prompt or a completion, its total is that row's OWN sum — never
// a per-call total carried across a hop, and never absent when a count exists.
// The invariant is checked exhaustively over every combination of stated
// / zero / nonzero on both sides, so an arithmetic change cannot satisfy one
// column by breaking another.
func TestCaptureColumnsNeverContradict(t *testing.T) {
	states := []struct {
		name string
		v    *int64
	}{
		{"unstated", nil},
		{"zero", i64(0)},
		{"count", i64(5)},
	}
	for _, p1 := range states {
		for _, c1 := range states {
			for _, t1 := range states {
				for _, p2 := range states {
					for _, c2 := range states {
						for _, t2 := range states {
							name := fmt.Sprintf("a{p:%s,c:%s,t:%s}/b{p:%s,c:%s,t:%s}",
								p1.name, c1.name, t1.name, p2.name, c2.name, t2.name)
							t.Run(name, func(t *testing.T) {
								a := Tokens{PromptTokens: p1.v, CompletionTokens: c1.v, TotalTokens: t1.v}
								b := Tokens{PromptTokens: p2.v, CompletionTokens: c2.v, TotalTokens: t2.v}
								out := foldTokens(a, b)

								// stated counts imply a stated, correct total.
								if out.PromptTokens != nil || out.CompletionTokens != nil {
									if out.TotalTokens == nil {
										t.Fatalf("counts stated with no total: %s", render(out))
									}
									var want int64
									if out.PromptTokens != nil {
										want += *out.PromptTokens
									}
									if out.CompletionTokens != nil {
										want += *out.CompletionTokens
									}
									if *out.TotalTokens != want {
										t.Fatalf("total %d contradicts its own counts (want %d): %s",
											*out.TotalTokens, want, render(out))
									}
								}
								// no count and no total anywhere -> nothing is stated.
								if a.PromptTokens == nil && a.CompletionTokens == nil && a.TotalTokens == nil &&
									b.PromptTokens == nil && b.CompletionTokens == nil && b.TotalTokens == nil {
									if out.TotalTokens != nil {
										t.Fatalf("a fabricated total appeared: %s", render(out))
									}
								} else if out.PromptTokens == nil && out.CompletionTokens == nil && out.TotalTokens == nil {
									t.Fatalf("a stated count or total vanished: %s", render(out))
								}
							})
						}
					}
				}
			}
		}
	}
}
