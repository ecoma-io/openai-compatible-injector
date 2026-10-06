package inject

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

// --- StripContextMarker -------------------------------------------------
//
// Claude Code appends "[1m]" to a model name when it wants the one-million
// token context window. The machine that produced this repo's capture runs
// with CLAUDE_CODE_DISABLE_1M_CONTEXT=1, so the marker never appeared on the
// wire and cannot be observed end to end here — these tables are the proof.

func TestStripContextMarker(t *testing.T) {
	cases := []struct {
		name    string
		in      string
		want    string
		matched bool
	}{
		{"plain", "claude-sonnet-4-5", "claude-sonnet-4-5", false},
		{"marker", "claude-sonnet-4-5[1m]", "claude-sonnet-4-5", true},
		{"marker upper", "claude-sonnet-4-5[1M]", "claude-sonnet-4-5", true},
		{"marker trailing space", "claude-sonnet-4-5[1m]  ", "claude-sonnet-4-5", true},
		{"marker leading space", "  claude-sonnet-4-5[1m]", "claude-sonnet-4-5", true},
		{"marker alone", "[1m]", "", true},
		// A miss returns the ORIGINAL, untrimmed: trimming a name that had no
		// marker would change which configured model resolves.
		{"untrimmed miss", "  claude-sonnet-4-5  ", "  claude-sonnet-4-5  ", false},
		{"marker not at end", "claude-sonnet-4-5[1m]beta", "claude-sonnet-4-5[1m]beta", false},
		{"marker partial", "claude-sonnet-4-5[1", "claude-sonnet-4-5[1", false},
		{"whitespace only", "   ", "   ", false},
		{"empty", "", "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, matched := StripContextMarker(tc.in)
			if got != tc.want || matched != tc.matched {
				t.Errorf("StripContextMarker(%q) = (%q, %v), want (%q, %v)", tc.in, got, matched, tc.want, tc.matched)
			}
		})
	}
}

// Matching is case-insensitive over the marker as a whole, and the surviving
// prefix keeps its own casing.
func TestStripContextMarkerCaseInsensitive(t *testing.T) {
	got, matched := StripContextMarker("CLAUDE-SONNET-4-5[1M]")
	if !matched || got != "CLAUDE-SONNET-4-5" {
		t.Errorf("got (%q, %v), want (%q, true)", got, matched, "CLAUDE-SONNET-4-5")
	}
}

// The slice is anchored to the END, not to the first occurrence: a name that
// carries the marker in the middle keeps it, and only the trailing marker
// comes off.
func TestStripContextMarkerOnlySuffix(t *testing.T) {
	got, matched := StripContextMarker("weird[1m]name[1m]")
	if !matched || got != "weird[1m]name" {
		t.Errorf("got (%q, %v), want (%q, true)", got, matched, "weird[1m]name")
	}
	got, matched = StripContextMarker("weird[1m]name")
	if matched || got != "weird[1m]name" {
		t.Errorf("got (%q, %v), want (%q, false)", got, matched, "weird[1m]name")
	}
}

// --- MessagesToChat ------------------------------------------------------

func decodeObj(t *testing.T, raw []byte) map[string]any {
	t.Helper()
	var v map[string]any
	if err := json.Unmarshal(raw, &v); err != nil {
		t.Fatalf("decode output: %v\n%s", err, raw)
	}
	return v
}

func messagesOf(t *testing.T, out map[string]any) []any {
	t.Helper()
	raw, ok := out["messages"].([]any)
	if !ok {
		t.Fatalf("messages is not an array: %#v", out["messages"])
	}
	return raw
}

// asObj narrows a decoded value to a map, failing with the value in hand.
func asObj(t *testing.T, v any) map[string]any {
	t.Helper()
	m, ok := v.(map[string]any)
	if !ok {
		t.Fatalf("not an object: %#v", v)
	}
	return m
}

func TestMessagesToChatMinimal(t *testing.T) {
	out, err := MessagesToChat([]byte(`{
		"model":"claude-sonnet-4-5",
		"max_tokens":1024,
		"messages":[{"role":"user","content":"hi"}]
	}`))
	if err != nil {
		t.Fatalf("MessagesToChat: %v", err)
	}
	obj := decodeObj(t, out)

	if obj["model"] != "claude-sonnet-4-5" {
		t.Errorf("model = %#v, want the public name passed through", obj["model"])
	}
	if obj["max_tokens"] != float64(1024) {
		t.Errorf("max_tokens = %#v, want 1024", obj["max_tokens"])
	}
	msgs := messagesOf(t, obj)
	if len(msgs) != 1 {
		t.Fatalf("len(messages) = %d, want 1", len(msgs))
	}
	m := asObj(t, msgs[0])
	if m["role"] != "user" || m["content"] != "hi" {
		t.Errorf("message = %#v", m)
	}

	// The allow-list: nothing client-only survives.
	for _, banned := range []string{"context_management", "output_config", "thinking", "top_k", "stop_sequences", "metadata"} {
		if _, ok := obj[banned]; ok {
			t.Errorf("%q leaked through the allow-list", banned)
		}
	}
	// A buffered request gains no stream_options.
	if _, ok := obj["stream_options"]; ok {
		t.Errorf("stream_options injected on a non-streaming request")
	}
}

func TestMessagesToChatSystemShapes(t *testing.T) {
	cases := []struct {
		name string
		body string
		want string // exact system content
	}{
		{
			name: "string",
			body: `"be brief"`,
			want: "be brief",
		},
		{
			name: "empty string yields no system message",
			body: `""`,
			want: "",
		},
		{
			name: "blocks joined with one newline, cache_control dropped",
			body: `[{"type":"text","text":"first"},{"type":"text","text":"second","cache_control":{"type":"ephemeral"}}]`,
			want: "first\nsecond",
		},
		{
			name: "non-text block dropped",
			body: `[{"type":"image","source":{"type":"url","url":"x"}},{"type":"text","text":"kept"}]`,
			want: "kept",
		},
		{
			name: "all non-text yields no system message",
			body: `[{"type":"image","source":{"type":"url","url":"x"}}]`,
			want: "",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			body := `{"model":"m","system":` + tc.body + `,"messages":[{"role":"user","content":"hi"}]}`
			out, err := MessagesToChat([]byte(body))
			if err != nil {
				t.Fatalf("MessagesToChat: %v", err)
			}
			msgs := messagesOf(t, decodeObj(t, out))
			if tc.want == "" {
				if len(msgs) != 1 {
					t.Fatalf("len(messages) = %d, want 1 (no system message)", len(msgs))
				}
				return
			}
			if len(msgs) != 2 {
				t.Fatalf("len(messages) = %d, want 2", len(msgs))
			}
			sys := asObj(t, msgs[0])
			if sys["role"] != "system" {
				t.Errorf("messages[0].role = %#v, want system", sys["role"])
			}
			if sys["content"] != tc.want {
				t.Errorf("system content = %q, want %q", sys["content"], tc.want)
			}
			if _, ok := sys["cache_control"]; ok {
				t.Errorf("cache_control survived into the system message")
			}
		})
	}
}

// The capture's real system array: three blocks of very different lengths.
// The joined text must contain each block in order, separated once.
func TestMessagesToChatSystemCaptureShape(t *testing.T) {
	body := `{"model":"m","system":[
		{"type":"text","text":"x-anthropic-billing-header: cc_version=2.1.291.926; cc_entry=1"},
		{"type":"text","text":"You are a Claude agent, built on Anthropic's Claude Agent SDK.","cache_control":{"type":"ephemeral"}},
		{"type":"text","text":"\nYou are an interactive agent that helps users with software.","cache_control":{"type":"ephemeral"}}
	],"messages":[{"role":"user","content":"hi"}]}`
	out, err := MessagesToChat([]byte(body))
	if err != nil {
		t.Fatalf("MessagesToChat: %v", err)
	}
	sys := asObj(t, messagesOf(t, decodeObj(t, out))[0])
	got, ok := sys["content"].(string)
	if !ok {
		t.Fatalf("system content = %#v, want a string", sys["content"])
	}

	// Each block appears in order, separated by exactly one newline, and the
	// leading newline that block 3 carries on its own is preserved as-is.
	want := "x-anthropic-billing-header: cc_version=2.1.291.926; cc_entry=1\n" +
		"You are a Claude agent, built on Anthropic's Claude Agent SDK.\n" +
		"\nYou are an interactive agent that helps users with software."
	if got != want {
		t.Errorf("joined system:\n got %q\nwant %q", got, want)
	}
	if strings.Contains(got, "cache_control") {
		t.Errorf("cache_control leaked: %q", got)
	}
	// The blocks must not be glued: a ';' must not run straight into 'Y'.
	if strings.Contains(got, ";You") {
		t.Errorf("blocks were concatenated without a separator: %q", got)
	}
}

func TestMessagesToChatAllowListAndScalarPassthrough(t *testing.T) {
	body := `{
		"model":"public-model",
		"max_tokens":2048,
		"temperature":0.4,
		"top_p":0.9,
		"stream":false,
		"parallel_tool_calls":true,
		"stop_sequences":["END","STOP"],
		"metadata":{"user_id":"u-1","other":"drop"},
		"thinking":{"type":"enabled","budget_tokens":31999},
		"context_management":{"foo":1},
		"output_config":{"bar":2},
		"top_k":5,
		"service_tier":"auto",
		"anthropic-beta":["a","b"],
		"messages":[{"role":"user","content":"hi"}]
	}`
	out, err := MessagesToChat([]byte(body))
	if err != nil {
		t.Fatalf("MessagesToChat: %v", err)
	}
	obj := decodeObj(t, out)

	wantPresent := map[string]any{
		"max_tokens":          float64(2048),
		"temperature":         float64(0.4),
		"top_p":               float64(0.9),
		"stream":              false,
		"parallel_tool_calls": true,
		"model":               "public-model",
	}
	for k, want := range wantPresent {
		if want == nil {
			continue
		}
		if !reflect.DeepEqual(obj[k], want) {
			t.Errorf("%s = %#v, want %#v", k, obj[k], want)
		}
	}
	for _, banned := range []string{
		"thinking", "context_management", "output_config", "top_k",
		"service_tier", "anthropic-beta", "stop_sequences", "metadata", "stream_options",
	} {
		if v, ok := obj[banned]; ok {
			t.Errorf("%q must be dropped, got %#v", banned, v)
		}
	}
	if got, ok := obj["stop"]; !ok || !reflect.DeepEqual(got, []any{"END", "STOP"}) {
		t.Errorf("stop = %#v, want the stop_sequences array", got)
	}
	if obj["user"] != "u-1" {
		t.Errorf("user = %#v, want metadata.user_id", obj["user"])
	}
}

func TestMessagesToChatStreamOptions(t *testing.T) {
	cases := []struct {
		name string
		body string
		want bool
	}{
		{"stream true", `"stream":true`, true},
		{"stream false", `"stream":false`, false},
		{"stream null", `"stream":null`, false},
		{"stream absent", ``, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			body := `{"model":"m","messages":[{"role":"user","content":"hi"}]`
			if tc.body != "" {
				body += "," + tc.body
			}
			body += `}`
			out, err := MessagesToChat([]byte(body))
			if err != nil {
				t.Fatalf("MessagesToChat: %v", err)
			}
			obj := decodeObj(t, out)
			got, ok := obj["stream_options"].(map[string]any)
			if tc.want {
				if !ok {
					t.Fatalf("stream_options missing")
				}
				if got["include_usage"] != true {
					t.Errorf("include_usage = %#v, want true", got["include_usage"])
				}
				if obj["stream"] != true {
					t.Errorf("stream = %#v, want true", obj["stream"])
				}
			} else if ok {
				t.Errorf("stream_options = %#v, want absent", got)
			}
		})
	}
}

func TestMessagesToChatTools(t *testing.T) {
	body := `{
		"model":"m",
		"tools":[
			{"name":"read_file","description":"Read a file","input_schema":{"type":"object","properties":{"path":{"type":"string"}}},"cache_control":{"type":"ephemeral"}},
			{"name":"web_search","description":"Server side tool"},
			{"name":"","input_schema":{"type":"object"}}
		],
		"messages":[{"role":"user","content":"hi"}]
	}`
	out, err := MessagesToChat([]byte(body))
	if err != nil {
		t.Fatalf("MessagesToChat: %v", err)
	}
	obj := decodeObj(t, out)
	tools, ok := obj["tools"].([]any)
	if !ok {
		t.Fatalf("tools missing: %#v", obj["tools"])
	}
	if len(tools) != 1 {
		t.Fatalf("len(tools) = %d, want 1 (server-side and unnamed tools dropped)", len(tools))
	}
	tool := asObj(t, tools[0])
	if tool["type"] != "function" {
		t.Errorf("tool.type = %#v, want function", tool["type"])
	}
	fn := asObj(t, tool["function"])
	if fn["name"] != "read_file" {
		t.Errorf("fn.name = %#v", fn["name"])
	}
	if fn["description"] != "Read a file" {
		t.Errorf("fn.description = %#v", fn["description"])
	}
	params, ok := fn["parameters"].(map[string]any)
	if !ok {
		t.Fatalf("parameters missing: %#v", fn["parameters"])
	}
	if params["type"] != "object" {
		t.Errorf("parameters.type = %#v", params["type"])
	}
	if _, ok := params["cache_control"]; ok {
		t.Errorf("cache_control leaked into parameters")
	}
	if _, ok := tool["input_schema"]; ok {
		t.Errorf("input_schema survived instead of becoming parameters")
	}
	if _, ok := tool["cache_control"]; ok {
		t.Errorf("cache_control survived on the tool")
	}
}

// No tool yields a function entry — an all-server-side list must disappear
// rather than reach the upstream as a malformed array.
func TestMessagesToChatAllToolsDropped(t *testing.T) {
	body := `{"model":"m","tools":[{"name":"web_search"}],"messages":[{"role":"user","content":"hi"}]}`
	out, err := MessagesToChat([]byte(body))
	if err != nil {
		t.Fatalf("MessagesToChat: %v", err)
	}
	if _, ok := decodeObj(t, out)["tools"]; ok {
		t.Errorf("tools present although no tool had an input_schema")
	}
}

func TestMessagesToChatToolChoice(t *testing.T) {
	cases := []struct {
		name            string
		body            string
		wantChoice      string
		wantChoiceIsObj bool
		wantChoiceObj   map[string]any
		wantParallel    any
		wantParallelSet bool
	}{
		{name: "auto string", body: `"tool_choice":"auto"`, wantChoice: "auto"},
		{name: "none string", body: `"tool_choice":"none"`, wantChoice: "none"},
		{name: "auto object", body: `"tool_choice":{"type":"auto"}`, wantChoice: "auto"},
		{name: "any object", body: `"tool_choice":{"type":"any"}`, wantChoice: "required"},
		{name: "none object", body: `"tool_choice":{"type":"none"}`, wantChoice: "none"},
		{
			name:            "tool object",
			body:            `"tool_choice":{"type":"tool","name":"read_file"}`,
			wantChoiceIsObj: true,
			wantChoiceObj:   map[string]any{"type": "function", "function": map[string]any{"name": "read_file"}},
		},
		{
			name:            "disable parallel",
			body:            `"tool_choice":{"type":"auto","disable_parallel_tool_use":true}`,
			wantChoice:      "auto",
			wantParallel:    false,
			wantParallelSet: true,
		},
		{
			name:            "parallel not disabled by default",
			body:            `"tool_choice":"auto"`,
			wantChoice:      "auto",
			wantParallel:    nil,
			wantParallelSet: false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			body := `{"model":"m",` + tc.body + `,"messages":[{"role":"user","content":"hi"}]}`
			out, err := MessagesToChat([]byte(body))
			if err != nil {
				t.Fatalf("MessagesToChat: %v", err)
			}
			obj := decodeObj(t, out)
			got, present := obj["tool_choice"]
			if !present {
				t.Fatalf("tool_choice missing")
			}
			if tc.wantChoiceIsObj {
				if !reflect.DeepEqual(got, tc.wantChoiceObj) {
					t.Errorf("tool_choice = %#v, want %#v", got, tc.wantChoiceObj)
				}
			} else if got != tc.wantChoice {
				t.Errorf("tool_choice = %#v, want %#v", got, tc.wantChoice)
			}
			parallel, parallelPresent := obj["parallel_tool_calls"]
			if tc.wantParallelSet {
				if !parallelPresent || parallel != tc.wantParallel {
					t.Errorf("parallel_tool_calls = %#v (present=%v), want %#v", parallel, parallelPresent, tc.wantParallel)
				}
			} else if parallelPresent && tc.name == "parallel not disabled by default" {
				t.Errorf("parallel_tool_calls = %#v, want absent", parallel)
			}
		})
	}
}

// A tool_choice the translation cannot express is dropped rather than
// guessed at — a wrong tool_choice silently changes what the model may do.
func TestMessagesToChatUnexpressableToolChoiceDropped(t *testing.T) {
	for _, body := range []string{
		`{"type":"anything_else"}`,
		`{"type":"tool"}`,
		`"not-a-real-choice"`,
		`{"type":"tool","name":""}`,
	} {
		out, err := MessagesToChat([]byte(`{"model":"m","tool_choice":` + body + `,"messages":[{"role":"user","content":"hi"}]}`))
		if err != nil {
			t.Fatalf("MessagesToChat: %v", err)
		}
		if v, ok := decodeObj(t, out)["tool_choice"]; ok {
			t.Errorf("tool_choice %s survived as %#v", body, v)
		}
	}
}

func TestMessagesToChatToolRoundTrip(t *testing.T) {
	body := `{
		"model":"m",
		"messages":[
			{"role":"user","content":"what is the weather?"},
			{"role":"assistant","content":[
				{"type":"text","text":"Checking."},
				{"type":"tool_use","id":"toolu_01","name":"get_weather","input":{"city":"Paris"}}
			]},
			{"role":"user","content":[
				{"type":"tool_result","tool_use_id":"toolu_01","content":"sunny"},
				{"type":"text","text":"and now?"}
			]}
		]
	}`
	out, err := MessagesToChat([]byte(body))
	if err != nil {
		t.Fatalf("MessagesToChat: %v", err)
	}
	msgs := messagesOf(t, decodeObj(t, out))
	if len(msgs) != 4 {
		t.Fatalf("len(messages) = %d, want 4 (user, assistant, tool, user)", len(msgs))
	}

	// Turn 2: assistant with text and one tool call.
	asst := asObj(t, msgs[1])
	if asst["role"] != "assistant" || asst["content"] != "Checking." {
		t.Errorf("assistant = %#v", asst)
	}
	calls, ok := asst["tool_calls"].([]any)
	if !ok || len(calls) != 1 {
		t.Fatalf("tool_calls = %#v, want one", asst["tool_calls"])
	}
	call := asObj(t, calls[0])
	if call["id"] != "toolu_01" || call["type"] != "function" {
		t.Errorf("tool_call = %#v", call)
	}
	fn := asObj(t, call["function"])
	if fn["name"] != "get_weather" {
		t.Errorf("function.name = %#v", fn["name"])
	}
	// arguments is the JSON STRING of the input object, not the object.
	args, ok := fn["arguments"].(string)
	if !ok {
		t.Fatalf("arguments = %#v, want a string", fn["arguments"])
	}
	var decoded map[string]any
	if err := json.Unmarshal([]byte(args), &decoded); err != nil {
		t.Fatalf("arguments not JSON: %v (%q)", err, args)
	}
	if decoded["city"] != "Paris" {
		t.Errorf("arguments = %#v", decoded)
	}

	// Turn 3: the tool result becomes a tool message emitted BEFORE the user
	// text that followed it in the source array.
	toolMsg := asObj(t, msgs[2])
	if toolMsg["role"] != "tool" {
		t.Fatalf("messages[2].role = %#v, want tool", toolMsg["role"])
	}
	if toolMsg["tool_call_id"] != "toolu_01" {
		t.Errorf("tool_call_id = %#v", toolMsg["tool_call_id"])
	}
	if toolMsg["content"] != "sunny" {
		t.Errorf("tool content = %#v", toolMsg["content"])
	}
	// The same turn's trailing user text follows the tool message — the
	// ordering OpenAI's history shape requires.
	userMsg := asObj(t, msgs[3])
	if userMsg["role"] != "user" || userMsg["content"] != "and now?" {
		t.Errorf("messages[3] = %#v", userMsg)
	}
}

// The tool_result-first rule holds even when text appears BEFORE the tool
// result in the source array — OpenAI requires the tool message directly
// after the assistant tool_calls turn.
func TestMessagesToChatToolResultEmittedBeforeUserText(t *testing.T) {
	body := `{"model":"m","messages":[
		{"role":"user","content":[
			{"type":"text","text":"running"},
			{"type":"tool_result","tool_use_id":"t1","content":"done"}
		]}
	]}`
	out, err := MessagesToChat([]byte(body))
	if err != nil {
		t.Fatalf("MessagesToChat: %v", err)
	}
	msgs := messagesOf(t, decodeObj(t, out))
	if len(msgs) != 2 {
		t.Fatalf("len(messages) = %d, want 2", len(msgs))
	}
	first := asObj(t, msgs[0])
	second := asObj(t, msgs[1])
	if first["role"] != "tool" {
		t.Errorf("messages[0].role = %#v, want tool (must precede the user text)", first["role"])
	}
	if second["role"] != "user" || second["content"] != "running" {
		t.Errorf("messages[1] = %#v", second)
	}
}

func TestMessagesToChatToolResultContentShapes(t *testing.T) {
	cases := []struct {
		name string
		body string
		want any
	}{
		{"string", `"just text"`, "just text"},
		{"null", `null`, ""},
		{"absent", ``, ""},
		{"text blocks collapse", `[{"type":"text","text":"a"},{"type":"text","text":"b"}]`, "ab"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			block := `{"type":"tool_result","tool_use_id":"t1"`
			if tc.body != "" {
				block += `,"content":` + tc.body
			}
			block += `}`
			body := `{"model":"m","messages":[{"role":"user","content":[` + block + `]}]}`
			out, err := MessagesToChat([]byte(body))
			if err != nil {
				t.Fatalf("MessagesToChat: %v", err)
			}
			msgs := messagesOf(t, decodeObj(t, out))
			if len(msgs) != 1 {
				t.Fatalf("len(messages) = %d, want 1", len(msgs))
			}
			msg := asObj(t, msgs[0])
			if msg["role"] != "tool" {
				t.Fatalf("role = %#v", msg["role"])
			}
			if !reflect.DeepEqual(msg["content"], tc.want) {
				t.Errorf("content = %#v, want %#v", msg["content"], tc.want)
			}
		})
	}
}

func TestMessagesToChatToolResultWithoutIDIsDropped(t *testing.T) {
	body := `{"model":"m","messages":[{"role":"user","content":[
		{"type":"tool_result","content":"orphan"},
		{"type":"text","text":"still here"}
	]}]}`
	out, err := MessagesToChat([]byte(body))
	if err != nil {
		t.Fatalf("MessagesToChat: %v", err)
	}
	msgs := messagesOf(t, decodeObj(t, out))
	if len(msgs) != 1 {
		t.Fatalf("len(messages) = %d, want 1 (the orphan tool_result dropped)", len(msgs))
	}
	m := asObj(t, msgs[0])
	if m["role"] != "user" || m["content"] != "still here" {
		t.Errorf("message = %#v", m)
	}
}

// thinking and redacted_thinking have no OpenAI content part: dropped on the
// way in, never forwarded as something the upstream would reject.
func TestMessagesToChatDropsThinkingBlocks(t *testing.T) {
	body := `{"model":"m","messages":[
		{"role":"assistant","content":[
			{"type":"thinking","thinking":"let me think","signature":"sig"},
			{"type":"redacted_thinking","data":"blob"},
			{"type":"text","text":"the answer"}
		]}
	]}`
	out, err := MessagesToChat([]byte(body))
	if err != nil {
		t.Fatalf("MessagesToChat: %v", err)
	}
	msgs := messagesOf(t, decodeObj(t, out))
	if len(msgs) != 1 {
		t.Fatalf("len(messages) = %d, want 1", len(msgs))
	}
	m := asObj(t, msgs[0])
	if m["content"] != "the answer" {
		t.Errorf("content = %#v", m["content"])
	}
	if _, ok := m["tool_calls"]; ok {
		t.Errorf("tool_calls present with no tool_use block")
	}
	blob, _ := json.Marshal(m)
	if strings.Contains(string(blob), "thinking") || strings.Contains(string(blob), "sig") {
		t.Errorf("thinking leaked: %s", blob)
	}
}

// A turn that translates to nothing is dropped entirely — an OpenAI
// assistant message with neither content nor tool_calls is not legal.
func TestMessagesToChatEmptyAssistantTurnDropped(t *testing.T) {
	body := `{"model":"m","messages":[
		{"role":"user","content":"hi"},
		{"role":"assistant","content":[{"type":"thinking","thinking":"..."}]},
		{"role":"user","content":"again"}
	]}`
	out, err := MessagesToChat([]byte(body))
	if err != nil {
		t.Fatalf("MessagesToChat: %v", err)
	}
	msgs := messagesOf(t, decodeObj(t, out))
	if len(msgs) != 2 {
		t.Fatalf("len(messages) = %d, want 2", len(msgs))
	}
	if got := asObj(t, msgs[1])["content"]; got != "again" {
		t.Errorf("messages[1].content = %#v", got)
	}
}

// Text-only user content collapses to a plain string — the shape every
// upstream accepts — while an image forces the parts array.
func TestMessagesToChatUserContentShapes(t *testing.T) {
	t.Run("single text block becomes a string", func(t *testing.T) {
		out, err := MessagesToChat([]byte(`{"model":"m","messages":[{"role":"user","content":[
			{"type":"text","text":"hello"}
		]}]}`))
		if err != nil {
			t.Fatalf("MessagesToChat: %v", err)
		}
		m := asObj(t, messagesOf(t, decodeObj(t, out))[0])
		if m["content"] != "hello" {
			t.Errorf("content = %#v, want a plain string", m["content"])
		}
	})

	t.Run("base64 image becomes a data uri", func(t *testing.T) {
		out, err := MessagesToChat([]byte(`{"model":"m","messages":[{"role":"user","content":[
			{"type":"image","source":{"type":"base64","media_type":"image/png","data":"aGVsbG8="}},
			{"type":"text","text":"what is this?"}
		]}]}`))
		if err != nil {
			t.Fatalf("MessagesToChat: %v", err)
		}
		m := asObj(t, messagesOf(t, decodeObj(t, out))[0])
		parts, ok := m["content"].([]any)
		if !ok {
			t.Fatalf("content = %#v, want a parts array", m["content"])
		}
		if len(parts) != 2 {
			t.Fatalf("len(parts) = %d, want 2", len(parts))
		}
		img := asObj(t, parts[0])
		if img["type"] != "image_url" {
			t.Errorf("part.type = %#v", img["type"])
		}
		url := asObj(t, img["image_url"])["url"]
		if url != "data:image/png;base64,aGVsbG8=" {
			t.Errorf("url = %#v", url)
		}
	})

	t.Run("url image passes through", func(t *testing.T) {
		out, err := MessagesToChat([]byte(`{"model":"m","messages":[{"role":"user","content":[
			{"type":"image","source":{"type":"url","url":"https://example.com/a.png"}}
		]}]}`))
		if err != nil {
			t.Fatalf("MessagesToChat: %v", err)
		}
		m := asObj(t, messagesOf(t, decodeObj(t, out))[0])
		parts := m["content"].([]any)
		url := asObj(t, asObj(t, parts[0])["image_url"])["url"]
		if url != "https://example.com/a.png" {
			t.Errorf("url = %#v", url)
		}
	})

	t.Run("unknown image source dropped", func(t *testing.T) {
		out, err := MessagesToChat([]byte(`{"model":"m","messages":[{"role":"user","content":[
			{"type":"image","source":{"type":"file","path":"/etc/passwd"}}
		]}]}`))
		if err != nil {
			t.Fatalf("MessagesToChat: %v", err)
		}
		msgs := messagesOf(t, decodeObj(t, out))
		if len(msgs) != 0 {
			t.Errorf("messages = %#v, want none (unrecognizable source dropped)", msgs)
		}
	})
}

func TestMessagesToChatErrors(t *testing.T) {
	cases := []struct {
		name string
		body string
	}{
		{"not json", `not json`},
		{"null", `null`},
		{"array", `[{"role":"user","content":"hi"}]`},
		{"no messages", `{"model":"m"}`},
		{"messages not array", `{"model":"m","messages":{"role":"user"}}`},
		{"messages null", `{"model":"m","messages":null}`},
		{"messages empty", `{"model":"m","messages":[]}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := MessagesToChat([]byte(tc.body)); err == nil {
				t.Fatalf("MessagesToChat(%s) = nil error, want an error", tc.body)
			}
		})
	}
}

// The failure messages must be static: this package holds request bodies,
// and an error that quotes one puts client content in a log line.
func TestMessagesToChatErrorsDoNotEchoInput(t *testing.T) {
	const secret = "SUPER-SECRET-CLIENT-CONTENT"
	bodies := []string{
		`not json ` + secret,
		`{"model":"m","messages":` + secret + `}`,
		`{"model":"m","system":` + secret + `,"messages":[]}`,
		`{"model":"m","messages":[{"role":"user","content":[` + secret + `]}]}`,
	}
	for _, body := range bodies {
		_, err := MessagesToChat([]byte(body))
		if err == nil {
			continue
		}
		if strings.Contains(err.Error(), secret) {
			t.Errorf("error echoes client content: %v", err)
		}
	}
}

// --- ChatToMessages ------------------------------------------------------

func TestChatToMessagesText(t *testing.T) {
	body := []byte(`{
		"id":"chatcmpl-abc",
		"object":"chat.completion",
		"model":"upstream-alias",
		"choices":[{"index":0,"message":{"role":"assistant","content":"Hello there"},"finish_reason":"stop"}],
		"usage":{"prompt_tokens":11,"completion_tokens":7}
	}`)
	out, ok := ChatToMessages(body, "claude-sonnet-4-5")
	if !ok {
		t.Fatalf("ok = false, want true")
	}
	obj := decodeObj(t, out)

	if obj["type"] != "message" {
		t.Errorf("type = %#v", obj["type"])
	}
	if obj["role"] != "assistant" {
		t.Errorf("role = %#v", obj["role"])
	}
	if obj["id"] != "chatcmpl-abc" {
		t.Errorf("id = %#v, want the upstream id passed through opaquely", obj["id"])
	}
	// The PUBLIC name, never the upstream's spelling.
	if obj["model"] != "claude-sonnet-4-5" {
		t.Errorf("model = %#v, want claude-sonnet-4-5", obj["model"])
	}
	if obj["stop_reason"] != "end_turn" {
		t.Errorf("stop_reason = %#v", obj["stop_reason"])
	}
	if obj["stop_sequence"] != nil {
		t.Errorf("stop_sequence = %#v, want null", obj["stop_sequence"])
	}

	content := obj["content"].([]any)
	if len(content) != 1 {
		t.Fatalf("len(content) = %d, want 1", len(content))
	}
	blk := asObj(t, content[0])
	if blk["type"] != "text" || blk["text"] != "Hello there" {
		t.Errorf("content[0] = %#v", blk)
	}

	usage := asObj(t, obj["usage"])
	if usage["input_tokens"] != float64(11) || usage["output_tokens"] != float64(7) {
		t.Errorf("usage = %#v", usage)
	}
}

// The 404 / model_not_found path interpolates the requested model
// byte-exact. A buffered answer echoes the same public name, so it must
// survive the round trip unescaped too.
func TestChatToMessagesPreservesHTMLBearingModelName(t *testing.T) {
	const model = `<script>&model`
	out, ok := ChatToMessages([]byte(`{"choices":[{"message":{"role":"assistant","content":"x"},"finish_reason":"stop"}]}`), model)
	if !ok {
		t.Fatalf("ok = false")
	}
	got := decodeObj(t, out)["model"]
	if got != model {
		t.Errorf("model = %#v, want %q", got, model)
	}
	// The default encoder spells these with \u escapes: same string after
	// decoding, different bytes on the wire. The name must travel back
	// exactly as it came.
	const escapedLT, escapedAMP = "\\u003c", "\\u0026"
	if strings.Contains(string(out), escapedLT) || strings.Contains(string(out), escapedAMP) {
		t.Errorf("model name was HTML-escaped: %s", out)
	}
}

func TestChatToMessagesToolCalls(t *testing.T) {
	body := []byte(`{
		"id":"chatcmpl-xyz",
		"choices":[{
			"message":{
				"role":"assistant",
				"content":"Let me check.",
				"tool_calls":[
					{"id":"call_1","type":"function","function":{"name":"get_weather","arguments":"{\"city\":\"Paris\"}"}},
					{"id":"call_2","type":"function","function":{"name":"get_time","arguments":"{}"}}
				]
			},
			"finish_reason":"tool_calls"
		}],
		"usage":{"prompt_tokens":5,"completion_tokens":9}
	}`)
	out, ok := ChatToMessages(body, "public-model")
	if !ok {
		t.Fatalf("ok = false")
	}
	obj := decodeObj(t, out)

	if obj["stop_reason"] != "tool_use" {
		t.Errorf("stop_reason = %#v, want tool_use", obj["stop_reason"])
	}
	content := obj["content"].([]any)
	if len(content) != 3 {
		t.Fatalf("len(content) = %d, want 3 (text + 2 tool_use)", len(content))
	}
	text := asObj(t, content[0])
	if text["type"] != "text" || text["text"] != "Let me check." {
		t.Errorf("content[0] = %#v", text)
	}
	first := asObj(t, content[1])
	if first["type"] != "tool_use" || first["id"] != "call_1" || first["name"] != "get_weather" {
		t.Errorf("content[1] = %#v", first)
	}
	input := asObj(t, first["input"])
	if input["city"] != "Paris" {
		t.Errorf("input = %#v", input)
	}
	second := asObj(t, content[2])
	if second["id"] != "call_2" || second["name"] != "get_time" {
		t.Errorf("content[2] = %#v", second)
	}
	if !reflect.DeepEqual(asObj(t, second["input"]), map[string]any{}) {
		t.Errorf("input = %#v, want {}", second["input"])
	}
}

// Unparseable arguments degrade to an empty object: an upstream's oddity
// must not turn a readable answer into a local failure.
func TestChatToMessagesBadArgumentsDegradeToEmptyObject(t *testing.T) {
	for _, args := range []string{`"not json"`, `"[]"`, `""`, `null`, `42`} {
		body := `{"choices":[{"message":{"role":"assistant","content":null,"tool_calls":[
			{"id":"c1","type":"function","function":{"name":"t","arguments":` + args + `}}
		]},"finish_reason":"tool_calls"}]}`
		out, ok := ChatToMessages([]byte(body), "m")
		if !ok {
			t.Fatalf("args %s: ok = false", args)
		}
		content := decodeObj(t, out)["content"].([]any)
		blk := asObj(t, content[0])
		if blk["type"] != "tool_use" {
			t.Fatalf("args %s: block = %#v", args, blk)
		}
		if !reflect.DeepEqual(asObj(t, blk["input"]), map[string]any{}) {
			t.Errorf("args %s: input = %#v, want {}", args, blk["input"])
		}
	}
}

// An arguments member that is absent altogether is the same empty input.
func TestChatToMessagesAbsentArgumentsDegradeToEmptyObject(t *testing.T) {
	body := `{"choices":[{"message":{"role":"assistant","content":null,"tool_calls":[
		{"id":"c1","type":"function","function":{"name":"t"}}
	]},"finish_reason":"tool_calls"}]}`
	out, ok := ChatToMessages([]byte(body), "m")
	if !ok {
		t.Fatalf("ok = false")
	}
	content := decodeObj(t, out)["content"].([]any)
	blk := asObj(t, content[0])
	if !reflect.DeepEqual(asObj(t, blk["input"]), map[string]any{}) {
		t.Errorf("input = %#v, want {}", blk["input"])
	}
}

func TestChatToMessagesStopReasonTable(t *testing.T) {
	cases := []struct {
		finish string
		want   string
	}{
		{"stop", "end_turn"},
		{"length", "max_tokens"},
		{"tool_calls", "tool_use"},
		{"function_call", "tool_use"},
		{"content_filter", "refusal"},
		{"something_new", "end_turn"},
	}
	for _, tc := range cases {
		body := `{"choices":[{"message":{"role":"assistant","content":"x"},"finish_reason":"` + tc.finish + `"}]}`
		out, ok := ChatToMessages([]byte(body), "m")
		if !ok {
			t.Fatalf("finish_reason %s: ok = false", tc.finish)
		}
		if got := decodeObj(t, out)["stop_reason"]; got != tc.want {
			t.Errorf("finish_reason %q -> stop_reason %#v, want %q", tc.finish, got, tc.want)
		}
	}
}

func TestChatToMessagesStopReasonAbsentIsNull(t *testing.T) {
	out, ok := ChatToMessages([]byte(`{"choices":[{"message":{"role":"assistant","content":"x"}}]}`), "m")
	if !ok {
		t.Fatalf("ok = false")
	}
	if got := decodeObj(t, out)["stop_reason"]; got != nil {
		t.Errorf("stop_reason = %#v, want null", got)
	}
}

// An object that is merely missing choices degrades — empty content, null
// stop_reason — rather than failing. A non-OBJECT is the only refusal.
func TestChatToMessagesDegradesMissingChoices(t *testing.T) {
	out, ok := ChatToMessages([]byte(`{"id":"chatcmpl-1","object":"chat.completion"}`), "m")
	if !ok {
		t.Fatalf("ok = false, want true for a well-formed object")
	}
	obj := decodeObj(t, out)
	if obj["stop_reason"] != nil {
		t.Errorf("stop_reason = %#v, want null", obj["stop_reason"])
	}
	content, ok := obj["content"].([]any)
	if !ok || len(content) != 0 {
		t.Errorf("content = %#v, want an empty block array", obj["content"])
	}
	usage := asObj(t, obj["usage"])
	if usage["input_tokens"] != float64(0) || usage["output_tokens"] != float64(0) {
		t.Errorf("usage = %#v, want zeros when the upstream stated none", usage)
	}
}

func TestChatToMessagesRefusesNonObject(t *testing.T) {
	for _, body := range []string{
		`[{"role":"assistant"}]`,
		`"a string"`,
		`42`,
		`null`,
		`true`,
		`not json`,
		``,
	} {
		if out, ok := ChatToMessages([]byte(body), "m"); ok {
			t.Errorf("ChatToMessages(%s) = (%s, true), want ok=false", body, out)
		}
	}
}

// reasoning_content has no Anthropic block: dropped, never echoed back into
// the next request's history as something the client must round-trip.
func TestChatToMessagesDropsReasoningContent(t *testing.T) {
	body := `{"choices":[{"message":{"role":"assistant","content":"the answer","reasoning_content":"internal monologue"},"finish_reason":"stop"}]}`
	out, ok := ChatToMessages([]byte(body), "m")
	if !ok {
		t.Fatalf("ok = false")
	}
	if strings.Contains(string(out), "internal monologue") || strings.Contains(string(out), "reasoning") {
		t.Errorf("reasoning_content leaked: %s", out)
	}
	content := decodeObj(t, out)["content"].([]any)
	if len(content) != 1 {
		t.Fatalf("len(content) = %d, want 1", len(content))
	}
	if got := asObj(t, content[0])["text"]; got != "the answer" {
		t.Errorf("text = %#v", got)
	}
}

// An upstream that omits usage yields zeros in the CLIENT view (a buffered
// answer's counts are a fact about a completed response) — distinct from the
// usage row, which stays NULL. That distinction lives in internal/usage, so
// this test only pins the client-facing half.
func TestChatToMessagesUsageFallbacks(t *testing.T) {
	cases := []struct {
		name  string
		body  string
		wantI any
		wantO any
	}{
		{"absent", `{"choices":[]}`, float64(0), float64(0)},
		{"null", `{"choices":[],"usage":null}`, float64(0), float64(0)},
		{"normal", `{"choices":[],"usage":{"prompt_tokens":3,"completion_tokens":4}}`, float64(3), float64(4)},
		{"string numbers", `{"choices":[],"usage":{"prompt_tokens":"6","completion_tokens":"7"}}`, float64(6), float64(7)},
		{"partial", `{"choices":[],"usage":{"prompt_tokens":9}}`, float64(9), float64(0)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out, ok := ChatToMessages([]byte(tc.body), "m")
			if !ok {
				t.Fatalf("ok = false")
			}
			usage := asObj(t, decodeObj(t, out)["usage"])
			if usage["input_tokens"] != tc.wantI || usage["output_tokens"] != tc.wantO {
				t.Errorf("usage = %#v, want input=%#v output=%#v", usage, tc.wantI, tc.wantO)
			}
		})
	}
}

// Tool calls with no text still produce a valid Anthropic message: one
// tool_use block and no text block at all.
func TestChatToMessagesToolCallsWithoutText(t *testing.T) {
	body := `{"choices":[{"message":{"role":"assistant","content":null,"tool_calls":[
		{"id":"c1","type":"function","function":{"name":"t","arguments":"{}"}}
	]},"finish_reason":"tool_calls"}]}`
	out, ok := ChatToMessages([]byte(body), "m")
	if !ok {
		t.Fatalf("ok = false")
	}
	content := decodeObj(t, out)["content"].([]any)
	if len(content) != 1 {
		t.Fatalf("len(content) = %d, want 1", len(content))
	}
	if got := asObj(t, content[0])["type"]; got != "tool_use" {
		t.Errorf("content[0].type = %#v", got)
	}
}
