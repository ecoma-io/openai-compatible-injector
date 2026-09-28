package inject

import (
	"bytes"
	"encoding/json"
	"errors"
	"testing"
)

// refusalReason returns the closed-set token a builder declined with, and
// fails the test when the builder did not decline.
func refusalReason(t *testing.T, err error) string {
	t.Helper()
	if err == nil {
		t.Fatal("the builder accepted a body it cannot continue")
	}
	var refusal *ContinuationRefusal
	if !errors.As(err, &refusal) {
		t.Fatalf("error is not a *ContinuationRefusal: %v", err)
	}
	return refusal.Reason()
}

// chatMsg decodes the messages array of a built continuation body.
func chatMsg(t *testing.T, body []byte) []map[string]json.RawMessage {
	t.Helper()
	m := decodeMap(t, body)
	var msgs []map[string]json.RawMessage
	if err := json.Unmarshal(m["messages"], &msgs); err != nil {
		t.Fatalf("decode messages: %v", err)
	}
	return msgs
}

// responsesItems decodes the input array of a built continuation body.
func responsesItems(t *testing.T, body []byte) []map[string]json.RawMessage {
	t.Helper()
	m := decodeMap(t, body)
	var items []map[string]json.RawMessage
	if err := json.Unmarshal(m["input"], &items); err != nil {
		t.Fatalf("decode input: %v", err)
	}
	return items
}

func textOf(t *testing.T, raw json.RawMessage) string {
	t.Helper()
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		t.Fatalf("decode text: %v", err)
	}
	return s
}

// TestBuildContinuationChatAppendsTheAssistantTurn: the ordinary shape. The
// client's body ends with a user message, the answer was never in the body,
// so the assistant turn is appended carrying exactly what the client saw.
func TestBuildContinuationChatAppendsTheAssistantTurn(t *testing.T) {
	orig := []byte(`{"model":"public","stream":true,"temperature":0.2,"max_tokens":64,` +
		`"tools":[{"type":"function","function":{"name":"ls"}}],` +
		`"messages":[{"role":"system","content":"be brief"},{"role":"user","content":"hi"}]}`)

	built, err := BuildContinuationChat(orig, "Hello, I was say")
	if err != nil {
		t.Fatalf("BuildContinuationChat: %v", err)
	}
	msgs := chatMsg(t, built)
	if len(msgs) != 3 {
		t.Fatalf("messages = %d, want the 2 the client sent plus one assistant turn", len(msgs))
	}
	if got := textOf(t, msgs[0]["content"]); got != "be brief" {
		t.Fatalf("messages[0].content = %q, want the client's own first message", got)
	}
	if got := textOf(t, msgs[1]["content"]); got != "hi" {
		t.Fatalf("messages[1].content = %q, want the client's own second message", got)
	}
	if got := textOf(t, msgs[2]["role"]); got != "assistant" {
		t.Fatalf("appended role = %q, want assistant", got)
	}
	if got := textOf(t, msgs[2]["content"]); got != "Hello, I was say" {
		t.Fatalf("appended content = %q, want the committed prefix", got)
	}

	// Every other member travels as sent: the hop differs from the original
	// attempt in the assistant turn and nothing else.
	m := decodeMap(t, built)
	for _, key := range []string{"temperature", "max_tokens", "tools"} {
		var before, after any
		if err := json.Unmarshal(decodeMap(t, orig)[key], &before); err != nil {
			t.Fatalf("decode original %s: %v", key, err)
		}
		if err := json.Unmarshal(m[key], &after); err != nil {
			t.Fatalf("decode built %s: %v", key, err)
		}
		if !jsonEqual(before, after) {
			t.Fatalf("%s changed: %v → %v", key, before, after)
		}
	}
	if !bytes.Equal(m["model"], []byte(`"public"`)) {
		t.Fatalf("the builder rewrote model = %s, want it left for Chat to rewrite", m["model"])
	}
	if string(m["stream"]) != "true" {
		t.Fatalf("stream = %s, want true", m["stream"])
	}
}

// jsonEqual compares two decoded JSON values.
func jsonEqual(a, b any) bool {
	ab, _ := json.Marshal(a)
	bb, _ := json.Marshal(b)
	return bytes.Equal(ab, bb)
}

// TestBuildContinuationChatExtendsAPrefill: a request whose last message is
// already an assistant message is asking the upstream to CONTINUE that text,
// so the streamed prefix extends it. Replacing the content instead would drop
// text the upstream already produced and the client may already have seen.
func TestBuildContinuationChatExtendsAPrefill(t *testing.T) {
	orig := []byte(`{"messages":[{"role":"user","content":"count"},{"role":"assistant","content":"one, "}]}`)
	built, err := BuildContinuationChat(orig, "two, three")
	if err != nil {
		t.Fatalf("BuildContinuationChat: %v", err)
	}
	msgs := chatMsg(t, built)
	if len(msgs) != 2 {
		t.Fatalf("messages = %d, want the prefill extended in place, not appended to", len(msgs))
	}
	if got := textOf(t, msgs[1]["content"]); got != "one, two, three" {
		t.Fatalf("prefill content = %q, want the prefill plus the prefix", got)
	}
}

// TestBuildContinuationChatPreservesThePrefillMessage is the other half of
// extending a prefill: the message is MUTATED, not REBUILT. Only `content` is
// the builder's to touch. Every other member of the client's assistant message
// — a `name`, a provider extension, a field this build has never heard of —
// must survive into the continuation by value, because the alternative is
// a continuation that silently rewrites the client's own conversation.
//
// A rebuild from the two fields this package knows about would pass a test
// that only checked role and content. That is exactly the bug this pins.
func TestBuildContinuationChatPreservesThePrefillMessage(t *testing.T) {
	orig := []byte(`{"model":"public","stream":true,"messages":[` +
		`{"role":"user","content":"count"},` +
		`{"role":"assistant","content":"one, ","name":"narrator",` +
		`"x_provider_extension":{"nested":[1,2],"flag":true},"prefix":true,` +
		`"x_future_field":"keep me"}]}`)

	built, err := BuildContinuationChat(orig, " two")
	if err != nil {
		t.Fatalf("BuildContinuationChat: %v", err)
	}
	msgs := chatMsg(t, built)
	if len(msgs) != 2 {
		t.Fatalf("messages = %d, want the client's two", len(msgs))
	}
	prefill := msgs[1]

	// The member the builder owns, and only it, moved.
	if got := textOf(t, prefill["content"]); got != "one,  two" {
		t.Fatalf("content = %q, want the prefill extended", got)
	}
	// Every other member is the client's own bytes.
	for member, want := range map[string]string{
		"role":                 `"assistant"`,
		"name":                 `"narrator"`,
		"x_provider_extension": `{"nested":[1,2],"flag":true}`,
		"prefix":               `true`,
		"x_future_field":       `"keep me"`,
	} {
		raw, ok := prefill[member]
		if !ok {
			t.Errorf("the continuation dropped the prefill's %q member: %s", member, built)
			continue
		}
		// Compared as decoded JSON so the assertion is about the VALUE the
		// upstream sees, not about the member order json.Marshal happens to
		// emit maps in.
		if !sameJSON(t, raw, json.RawMessage(want)) {
			t.Errorf("member %q = %s, want %s", member, raw, want)
		}
	}

	// The client's other members and its first message are untouched.
	m := decodeMap(t, built)
	if string(m["model"]) != `"public"` {
		t.Errorf("model = %s, want the client's", m["model"])
	}
	if got := textOf(t, msgs[0]["content"]); got != "count" {
		t.Errorf("the user turn was rewritten: %q", got)
	}
}

// sameJSON compares two raw JSON values by decoded value, so a member re-emitted
// in a different key order still counts as preserved.
func sameJSON(t *testing.T, a, b json.RawMessage) bool {
	t.Helper()
	var av, bv any
	if err := json.Unmarshal(a, &av); err != nil {
		return false
	}
	if err := json.Unmarshal(b, &bv); err != nil {
		return false
	}
	return jsonEqual(av, bv)
}

// TestBuildContinuationChatRefusals: every way the builder declines. Each one
// means "do not recover this stream", and each is a shape where guessing
// would splice repeated or misordered text into the client's answer.
func TestBuildContinuationChatRefusals(t *testing.T) {
	cases := []struct {
		name   string
		orig   string
		prefix string
		want   string
	}{
		{"not an object", `[]`, "x", refusalNotObject},
		{"not json", `}{`, "x", refusalNotObject},
		{"no messages", `{"model":"public"}`, "x", refusalNoMessages},
		{"messages is an object", `{"messages":{"role":"user"}}`, "x", refusalNoMessages},
		{"empty messages", `{"messages":[]}`, "x", refusalNoMessages},
		{"empty prefix", `{"messages":[{"role":"user","content":"hi"}]}`, "", refusalNoOp},
		{"last message is not an object", `{"messages":["hi"]}`, "x", refusalUnsupportedShape},
		{"last message has no role", `{"messages":[{"content":"hi"}]}`, "x", refusalUnsupportedShape},
		{"last message role is not a string", `{"messages":[{"role":7,"content":"hi"}]}`, "x", refusalUnsupportedShape},
		{"assistant content is structured", `{"messages":[{"role":"assistant","content":[{"type":"text","text":"hi"}]}]}`, "x", refusalUnsupportedShape},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := BuildContinuationChat([]byte(tc.orig), tc.prefix)
			if got := refusalReason(t, err); got != tc.want {
				t.Fatalf("reason = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestBuildContinuationChatRefusalEchoesNothing: the token is the whole
// message. A refusal can be logged by a deployment that logs everything, and
// a request body must never come with it.
func TestBuildContinuationChatRefusalEchoesNothing(t *testing.T) {
	secret := "sk-live-do-not-log"
	body := `{"messages":[{"role":"assistant","content":[{"type":"text","text":"` + secret + `"}]}]}`
	_, err := BuildContinuationChat([]byte(body), "x")
	if err == nil {
		t.Fatal("expected a refusal")
	}
	if bytes.Contains([]byte(err.Error()), []byte(secret)) {
		t.Fatalf("the refusal error quotes request content: %v", err)
	}
}

// TestBuildContinuationResponsesAppendsTheContinuationItems: the array-input
// shape. The client's items are preserved in order and the two continuation
// items land last.
func TestBuildContinuationResponsesAppendsTheContinuationItems(t *testing.T) {
	orig := []byte(`{"model":"public","stream":true,"instructions":"be brief",` +
		`"input":[{"type":"message","role":"user","content":[{"type":"input_text","text":"hi"}]}]}`)
	built, err := BuildContinuationResponses(orig, "partial answer")
	if err != nil {
		t.Fatalf("BuildContinuationResponses: %v", err)
	}
	items := responsesItems(t, built)
	if len(items) != 3 {
		t.Fatalf("input items = %d, want the client's one plus two continuation items", len(items))
	}
	if got := textOf(t, items[0]["role"]); got != "user" {
		t.Fatalf("items[0].role = %q, want the client's own item first", got)
	}
	if got := textOf(t, items[1]["role"]); got != "assistant" {
		t.Fatalf("items[1].role = %q, want assistant", got)
	}
	if got := continuationText(t, items[1]); got != "partial answer" {
		t.Fatalf("the assistant item text = %q, want the committed prefix", got)
	}
	if got := continuationText(t, items[2]); got != continuationInstruction {
		t.Fatalf("the instruction item carries %q, want the fixed instruction", got)
	}
	if got := textOf(t, items[1]["type"]); got != "message" {
		t.Fatalf("items[1].type = %q, want message", got)
	}
	m := decodeMap(t, built)
	if !bytes.Equal(m["instructions"], []byte(`"be brief"`)) {
		t.Fatalf("instructions = %s, want the client's own untouched", m["instructions"])
	}
	if string(m["stream"]) != "true" {
		t.Fatalf("stream = %s, want true", m["stream"])
	}
}

// TestBuildContinuationResponsesStringInput: a plain-string input means one
// user turn. The continuation keeps it as that turn and adds the assistant
// answer plus the instruction, rather than dropping what the client said.
func TestBuildContinuationResponsesStringInput(t *testing.T) {
	orig := []byte(`{"input":"what is 2+2?"}`)
	built, err := BuildContinuationResponses(orig, "Four")
	if err != nil {
		t.Fatalf("BuildContinuationResponses: %v", err)
	}
	items := responsesItems(t, built)
	if len(items) != 3 {
		t.Fatalf("input items = %d, want 3", len(items))
	}
	if got := textOf(t, items[0]["role"]); got != "user" {
		t.Fatalf("items[0].role = %q, want the string input preserved as a user turn", got)
	}
	if got := continuationText(t, items[0]); got != "what is 2+2?" {
		t.Fatalf("items[0] text = %q, want the client's own string", got)
	}
	if got := textOf(t, items[1]["role"]); got != "assistant" {
		t.Fatalf("items[1].role = %q, want assistant", got)
	}
	if got := continuationText(t, items[1]); got != "Four" {
		t.Fatalf("the assistant item text = %q, want the committed prefix", got)
	}
	if got := textOf(t, items[2]["role"]); got != "user" {
		t.Fatalf("items[2].role = %q, want the instruction turn", got)
	}
	if got := continuationText(t, items[2]); got != continuationInstruction {
		t.Fatalf("the instruction item carries %q, want the fixed instruction", got)
	}
}

// continuationText reads the text of the first content part of a Responses
// item.
func continuationText(t *testing.T, item map[string]json.RawMessage) string {
	t.Helper()
	var parts []struct {
		Type string `json:"type"`
		Text string `json:"text"`
	}
	if err := json.Unmarshal(item["content"], &parts); err != nil {
		t.Fatalf("decode content parts: %v", err)
	}
	if len(parts) == 0 {
		t.Fatal("item has no content parts")
	}
	return parts[0].Text
}

// TestBuildContinuationResponsesAbsentInput: no input at all — the answer
// came from instructions alone, so the continuation is the whole input.
func TestBuildContinuationResponsesAbsentInput(t *testing.T) {
	built, err := BuildContinuationResponses([]byte(`{"instructions":"write a poem"}`), "Roses are")
	if err != nil {
		t.Fatalf("BuildContinuationResponses: %v", err)
	}
	items := responsesItems(t, built)
	if len(items) != 2 {
		t.Fatalf("input items = %d, want 2", len(items))
	}
	if got := continuationText(t, items[0]); got != "Roses are" {
		t.Fatalf("items[0] text = %q, want the prefix", got)
	}
}

// TestBuildContinuationResponsesRefusals: the fail-closed half. The
// previous_response_id refusal is the one an operator is most likely to
// disagree with and the one most worth refusing: the hop would name a stored
// response whose generation never finished.
func TestBuildContinuationResponsesRefusals(t *testing.T) {
	cases := []struct {
		name   string
		orig   string
		prefix string
		want   string
	}{
		{"not an object", `"hi"`, "x", refusalNotObject},
		{"empty prefix", `{"input":"hi"}`, "", refusalNoOp},
		{"previous_response_id", `{"previous_response_id":"resp_1","input":"hi"}`, "x", refusalPreviousResponseID},
		{"input is a number", `{"input":7}`, "x", refusalUnsupportedShape},
		{"input is an object", `{"input":{"role":"user"}}`, "x", refusalUnsupportedShape},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := BuildContinuationResponses([]byte(tc.orig), tc.prefix)
			if got := refusalReason(t, err); got != tc.want {
				t.Fatalf("reason = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestBuildContinuationResponsesNullInputIsAbsent: a null input is the zero
// value, not an unreadable shape — the same treatment the transforms give a
// null member everywhere else.
func TestBuildContinuationResponsesNullInputIsAbsent(t *testing.T) {
	built, err := BuildContinuationResponses([]byte(`{"input":null}`), "text")
	if err != nil {
		t.Fatalf("BuildContinuationResponses: %v", err)
	}
	if items := responsesItems(t, built); len(items) != 2 {
		t.Fatalf("input items = %d, want 2", len(items))
	}
}

// TestContinuationInstructionIsFixed pins the instruction's content. It is
// deliberately not configurable: it is the whole difference between "continue
// this answer" and "answer this again", and a deployment that could edit it
// could ask for the duplication the feature's contract forbids.
func TestContinuationInstructionIsFixed(t *testing.T) {
	for _, must := range []string{"Continue it from the exact point", "Do not repeat"} {
		if !bytes.Contains([]byte(continuationInstruction), []byte(must)) {
			t.Fatalf("the continuation instruction no longer says %q: %s", must, continuationInstruction)
		}
	}
}

// TestBuildContinuationIsIdempotentInItsInputs: neither builder mutates the
// body it was handed. The original bytes are replayed through the candidate's
// transform on every attempt, and a builder that edited them in place would
// corrupt the retry path it is not part of.
func TestBuildContinuationIsIdempotentInItsInputs(t *testing.T) {
	chat := []byte(`{"messages":[{"role":"user","content":"hi"}]}`)
	chatCopy := append([]byte(nil), chat...)
	if _, err := BuildContinuationChat(chat, "prefix"); err != nil {
		t.Fatalf("BuildContinuationChat: %v", err)
	}
	if !bytes.Equal(chat, chatCopy) {
		t.Fatalf("the chat builder mutated its input: %s", chat)
	}

	responses := []byte(`{"input":[{"role":"user"}]}`)
	responsesCopy := append([]byte(nil), responses...)
	if _, err := BuildContinuationResponses(responses, "prefix"); err != nil {
		t.Fatalf("BuildContinuationResponses: %v", err)
	}
	if !bytes.Equal(responses, responsesCopy) {
		t.Fatalf("the responses builder mutated its input: %s", responses)
	}
}
