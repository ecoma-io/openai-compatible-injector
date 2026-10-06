package e2e_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// The 100%-real-client acceptance test: a genuine Claude Code process, in an
// isolated HOME, talking Anthropic Messages to the built binary, which
// translates to an OpenAI upstream. Everything the suite asserts elsewhere
// with synthetic bodies is re-asserted here against bytes an actual client
// chose — and the two things a synthetic body cannot know (which headers a
// real install puts on the wire, and whether its usage survives the dialect
// change) are asserted for the first time.
//
// The test SKIPS when no Claude Code is available — CI has no binary — and
// never reads or writes the machine's real install: HOME, CLAUDE_CONFIG_DIR
// and both XDG roots point into a temp dir, so ~/.claude, ~/.claude.json and
// the machine's settings.json are all unreachable. Settings arrive only
// through --settings, whose env block is what forces the model and the base
// URL regardless of anything configured on the host.
//
// [1m] is NOT proven here. This client strips the marker before it reaches
// the wire even when --model asks for it, so a passing run cannot
// distinguish "Claude Code stripped it" from "the proxy stripped it".
// TestMessagesStripsContextMarker POSTs the marker directly and proves the
// proxy's own strip; this test passes the suffixed --model anyway, because
// "a real client configured for 1M context works end to end" is the
// condition an operator actually runs into.

const (
	// claudePublicModel is the name in the runtime YAML — the name Claude
	// Code asks for. claudeUpstreamModel is the alias the upstream must see,
	// and the difference between the two is the rename this test proves on
	// the real wire.
	claudePublicModel   = "claude-sonnet-4-5"
	claudeUpstreamModel = "upstream-cc"

	// claudeInjectionMarker sits at messages[0] of every upstream body if the
	// injection prompt still applies after translation. The quiet failure
	// this pins — a prompt that silently stops being prepended — would
	// otherwise leave the upstream with a healthy-looking request and a
	// differently-behaved model.
	claudeInjectionMarker = "INJECTION-MARKER must be messages[0]"

	claudePrompt = "Reply with exactly: PONG"
)

// claudeAnswerStream is the upstream's Chat reply to a streamed request:
// role, the answer, the finish chunk with real usage, and [DONE]. The usage
// counts are load-bearing twice over — the Anthropic envelope must report
// them, and this test asserts that the CLIENT's own totals came out non-zero,
// which only holds if message_delta carries them where a real SDK reads them.
const claudeAnswerStream = "data: {\"id\":\"chatcmpl-cc\",\"object\":\"chat.completion.chunk\",\"model\":\"upstream-cc\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\"}}]}\n\n" +
	"data: {\"id\":\"chatcmpl-cc\",\"object\":\"chat.completion.chunk\",\"model\":\"upstream-cc\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"PONG\"}}]}\n\n" +
	"data: {\"id\":\"chatcmpl-cc\",\"object\":\"chat.completion.chunk\",\"model\":\"upstream-cc\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":11,\"completion_tokens\":2,\"total_tokens\":13}}\n\n" +
	"data: [DONE]\n\n"

// claudeAnswer is the buffered counterpart, for the non-streaming retry a
// real client falls back to when its streaming request fails.
const claudeAnswer = `{"id":"chatcmpl-cc","object":"chat.completion","model":"upstream-cc","choices":[{"index":0,"message":{"role":"assistant","content":"PONG"},"finish_reason":"stop"}],"usage":{"prompt_tokens":11,"completion_tokens":2,"total_tokens":13}}`

// claudeUpstream answers every request with PONG, choosing SSE or buffered
// JSON from the request it just recorded. fakeUpstream drains r.Body before
// invoking the handler, so the recorded copy is the only one left to read.
func claudeUpstream(t *testing.T, up *fakeUpstream) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if upstreamSaysStream(t, up) {
			chatStreamUpstream(claudeAnswerStream)(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, claudeAnswer)
	}
}

// excerptAround bounds a failure dump. A real Claude Code request body is
// tens of kilobytes — it carries the client's entire system prompt — and an
// assertion that prints all of it buries the handful of bytes it is
// complaining about. The window is centred on the needle when the needle is
// there, and falls back to the head when it is not (the "did not appear"
// case, where only the head tells the reader what body they are looking at).
func excerptAround(body []byte, needle string) string {
	const window = 400
	lo, hi := 0, min(len(body), window)
	if i := bytes.Index(body, []byte(needle)); i >= 0 {
		lo, hi = i-120, i+len(needle)+260
		if lo < 0 {
			lo = 0
		}
		if hi > len(body) {
			hi = len(body)
		}
	}
	return string(body[lo:hi])
}

// claudeCommand resolves the argv prefix that runs Claude Code, or skips.
// The explicit override wins, then PATH, then — only when the operator
// asked for it — a throwaway npm install. The default everywhere else is a
// skip, which is what keeps CI hermetic: no binary is installed on the
// runner's behalf, ever.
func claudeCommand(t *testing.T) []string {
	t.Helper()
	if p := os.Getenv("OAICR_E2E_CLAUDE_BIN"); p != "" {
		return []string{p}
	}
	if p, err := exec.LookPath("claude"); err == nil {
		return []string{p}
	}
	if os.Getenv("OAICR_E2E_CLAUDE_INSTALL") == "1" {
		return installClaude(t)
	}
	t.Skip("claude binary not present — set OAICR_E2E_CLAUDE_BIN, put claude on PATH, " +
		"or set OAICR_E2E_CLAUDE_INSTALL=1 to fetch one into a temp dir")
	return nil
}

// installClaude fetches @anthropic-ai/claude-code into a temp prefix and
// returns the argv that runs it. The install is opt-in because it costs a
// download and a network round trip; it lands in t.TempDir() so the machine
// gets neither a new global package nor a changed ~/.claude.
func installClaude(t *testing.T) []string {
	t.Helper()
	npm, err := exec.LookPath("npm")
	if err != nil {
		t.Skip("OAICR_E2E_CLAUDE_INSTALL=1 but npm is not on PATH")
	}
	prefix := t.TempDir()
	cmd := exec.Command(npm, "i", "--prefix", prefix,
		"--no-audit", "--no-fund", "--loglevel=error", "@anthropic-ai/claude-code")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("npm install @anthropic-ai/claude-code: %v\n%s", err, out)
	}
	// npm drops the shim in node_modules/.bin and the entry point beside the
	// package itself; either shape runs, and the first that exists wins.
	for _, argv := range [][]string{
		{filepath.Join(prefix, "node_modules", ".bin", "claude")},
		{"node", filepath.Join(prefix, "node_modules", "@anthropic-ai", "claude-code", "cli.js")},
	} {
		if _, err := os.Stat(argv[len(argv)-1]); err == nil {
			return argv
		}
	}
	t.Fatalf("npm install reported success but no claude entry point appeared under %s", prefix)
	return nil
}

// claudeEnv builds the child's environment: the parent's, minus every
// credential and identity path that could reach the machine's real Claude
// Code configuration, plus a fresh HOME tree of our own. Filtering the
// ANTHROPIC_*/CLAUDE_* names rather than appending over them is deliberate —
// a variable the host exports for a different endpoint must not be one
// duplicate key away from redirecting the run.
func claudeEnv(home string) []string {
	var env []string
	for _, kv := range os.Environ() {
		k, _, ok := strings.Cut(kv, "=")
		if !ok {
			continue
		}
		switch {
		case strings.HasPrefix(k, "ANTHROPIC_"), strings.HasPrefix(k, "CLAUDE_"), strings.HasPrefix(k, "XDG_"):
			continue
		case k == "HOME" || k == "CLAUDE_CONFIG_DIR":
			continue
		}
		env = append(env, kv)
	}
	return append(env,
		"HOME="+home,
		"CLAUDE_CONFIG_DIR="+filepath.Join(home, ".claude"),
		"XDG_CONFIG_HOME="+filepath.Join(home, ".config"),
		"XDG_DATA_HOME="+filepath.Join(home, ".local", "share"),
	)
}

// claudeSettings is the --settings payload: the ONLY channel through which
// this run is told where to go and what to call itself. Its env block
// overrides the host's settings.json env block — the failure mode that made
// an earlier attempt dial Anthropic for real — and the same keys are set
// again in claudeEnv, so neither layer alone can be bypassed.
func claudeSettings(t *testing.T, addr string) string {
	t.Helper()
	wire := claudePublicModel + "[1m]"
	b, err := json.Marshal(map[string]any{"env": map[string]string{
		"ANTHROPIC_BASE_URL":                       "http://" + addr,
		"ANTHROPIC_API_KEY":                        e2eAPIKey,
		"ANTHROPIC_MODEL":                          wire,
		"ANTHROPIC_DEFAULT_HAIKU_MODEL":            wire,
		"ANTHROPIC_DEFAULT_SONNET_MODEL":           wire,
		"ANTHROPIC_DEFAULT_OPUS_MODEL":             wire,
		"ANTHROPIC_DEFAULT_FABLE_MODEL":            wire,
		"CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC": "1",
		"DISABLE_NON_ESSENTIAL_MODEL_CALLS":        "1",
		"DISABLE_TELEMETRY":                        "1",
		"DISABLE_ERROR_REPORTING":                  "1",
		"DISABLE_AUTOUPDATER":                      "1",
	}})
	if err != nil {
		t.Fatalf("marshal settings: %v", err)
	}
	return string(b)
}

// TestClaudeCodeRealClient is the acceptance test the whole surface exists
// for: Claude Code itself, isolated, against the built binary. It asserts
// what only a real client can establish — the exact headers that leave the
// install, the tool schemas after translation, and that the usage counts
// survive a dialect change well enough for the CLIENT to report them.
func TestClaudeCodeRealClient(t *testing.T) {
	argv0 := claudeCommand(t)

	up := newFakeUpstream(t)
	up.setHandler(claudeUpstream(t, up))
	p := startSubprocess(t, startOpts{
		yaml: runtimeYAML(claudePublicModel, up.url()+"/v1", claudeUpstreamModel, claudeInjectionMarker),
		// request_completed is INFO, and this test wants the same log field
		// the rest of the suite reads off a real exchange.
		logLevel: "info",
	})

	home := t.TempDir()
	work := filepath.Join(home, "work")
	if err := os.MkdirAll(work, 0o755); err != nil {
		t.Fatalf("create work dir: %v", err)
	}

	args := append([]string{}, argv0[1:]...)
	args = append(args,
		"-p",
		"--output-format", "json",
		"--settings", claudeSettings(t, p.addr),
		"--model", claudePublicModel+"[1m]",
		// Print mode's default permission answerer is an SDK host that is not
		// here; naming "none" makes an attempted tool call a denial rather
		// than a prompt nobody can answer.
		"--permission-prompts", "none",
		claudePrompt,
	)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, argv0[0], args...)
	cmd.Dir = work
	cmd.Env = claudeEnv(home)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		switch {
		case ctx.Err() == context.DeadlineExceeded:
			t.Fatalf("claude did not finish within 2 minutes\nstdout:\n%s\nstderr:\n%s",
				stdout.String(), stderr.String())
		default:
			t.Fatalf("claude exited non-zero: %v\nstdout:\n%s\nstderr:\n%s",
				err, stdout.String(), stderr.String())
		}
	}

	// ---- what the client itself concluded ----
	var res struct {
		Result   string `json:"result"`
		IsError  bool   `json:"is_error"`
		Subtype  string `json:"subtype"`
		NumTurns int    `json:"num_turns"`
		Usage    struct {
			InputTokens  int `json:"input_tokens"`
			OutputTokens int `json:"output_tokens"`
		} `json:"usage"`
	}
	if err := json.Unmarshal(stdout.Bytes(), &res); err != nil {
		t.Fatalf("claude stdout is not JSON: %v\n%s", err, stdout.String())
	}
	if res.IsError || res.Subtype != "success" {
		t.Fatalf("claude reported a failure (is_error=%v subtype=%q): %s",
			res.IsError, res.Subtype, res.Result)
	}
	if !strings.Contains(res.Result, "PONG") {
		t.Fatalf("result = %q, want it to contain PONG", res.Result)
	}
	// The honesty check on the usage placement. The upstream's counts reach
	// the client only through message_start/message_delta; a client that
	// reads the counts from where a real Anthropic stream puts them would
	// report zeros if this proxy put them anywhere else.
	if res.Usage.InputTokens <= 0 {
		t.Errorf("usage.input_tokens = %d, want > 0 (the counts never reached the client)", res.Usage.InputTokens)
	}
	if res.Usage.OutputTokens <= 0 {
		t.Errorf("usage.output_tokens = %d, want > 0 (the counts never reached the client)", res.Usage.OutputTokens)
	}
	t.Logf("claude: %q after %d turn(s), usage in=%d out=%d",
		res.Result, res.NumTurns, res.Usage.InputTokens, res.Usage.OutputTokens)

	// ---- what crossed the wire ----
	reqs := up.requests()
	if len(reqs) == 0 {
		t.Fatal("the upstream recorded no request — Claude Code never reached the proxy")
	}
	for i, req := range reqs {
		if req.Method != http.MethodPost || req.Path != "/v1/chat/completions" {
			t.Errorf("request %d = %s %s, want POST …/v1/chat/completions", i, req.Method, req.Path)
		}
		// The client's credentials and dialect markers are consumed by the
		// proxy. A real install puts every one of these on the wire, so a
		// leak here is a disclosure on live traffic rather than a theoretical
		// one — the forward allow-list is what must be shown to hold.
		for _, name := range []string{"Authorization", "X-Api-Key", "Anthropic-Version", "Anthropic-Beta", "X-App"} {
			if v := req.Headers.Get(name); v != "" {
				t.Errorf("request %d upstream %s = %q — a client credential crossed to the provider", i, name, v)
			}
		}
		if id := req.Headers.Get("X-Request-Id"); !e2eRequestID.MatchString(id) {
			t.Errorf("request %d upstream X-Request-Id = %q, want the proxy's own 16 hex chars", i, id)
		}

		var doc map[string]any
		if err := json.Unmarshal(req.Body, &doc); err != nil {
			t.Errorf("request %d upstream body is not JSON: %v", i, err)
			continue
		}
		if doc["model"] != claudeUpstreamModel {
			t.Errorf("request %d upstream model = %v, want %s (the configured alias)",
				i, doc["model"], claudeUpstreamModel)
		}
		// The marker is checked in the model FIELD, never in the raw body:
		// this client's own system prompt QUOTES its id — a system-reminder
		// tells the model "the exact model ID is claude-sonnet-4-5[1m]" — so
		// the bytes legitimately appear in prompt text whatever either layer
		// does. The model field is the only place a configured model name can
		// cross, and the proxy's own strip is proven separately by
		// TestMessagesStripsContextMarker, which POSTs the marker itself.
		if m, _ := doc["model"].(string); strings.Contains(m, "[1m]") {
			t.Errorf("request %d upstream model = %q carries the [1m] context marker", i, m)
		}
		// Real Claude Code sends 20+ tools with Anthropic's input_schema, and
		// a strict OpenAI upstream rejects that key outright — so the tools
		// are read structurally, not grepped out of the body: a zero-tool
		// request would otherwise make this assertion vacuous, and prompt
		// text is free to quote either key. The schema is OpenAI's own
		// nesting — {type, function:{name, parameters}} — which is itself
		// part of the proof: Anthropic's flat spelling has neither of these
		// two keys.
		tools, _ := doc["tools"].([]any)
		if len(tools) == 0 {
			t.Errorf("request %d carried no tools — Claude Code's schemas never reached the translation", i)
		}
		for _, ti := range tools {
			tool, _ := ti.(map[string]any)
			fn, _ := tool["function"].(map[string]any)
			name, _ := fn["name"].(string)
			if tool["type"] != "function" {
				t.Errorf("request %d tool %q type = %v, want \"function\"", i, name, tool["type"])
			}
			if _, bad := fn["input_schema"]; bad {
				t.Errorf("request %d tool %q still uses input_schema: …%s…",
					i, name, excerptAround(req.Body, "input_schema"))
			}
			if _, ok := fn["parameters"]; !ok {
				t.Errorf("request %d tool %q has no function.parameters — the schema did not survive translation: …%s…",
					i, name, excerptAround(req.Body, `"tools"`))
			}
		}
		msgs, _ := doc["messages"].([]any)
		if len(msgs) == 0 {
			t.Errorf("request %d carried no messages", i)
			continue
		}
		first, _ := msgs[0].(map[string]any)
		if first["role"] != "system" || first["content"] != claudeInjectionMarker {
			t.Errorf("request %d messages[0] = role %v content %.80v, want the injection prompt first",
				i, first["role"], first["content"])
		}
	}

	// ---- the same exchange, from the proxy's own log ----
	ev := waitForLogEvent(t, p, func(e logEvent) bool {
		return e["message"] == "request_completed" && e["api"] == "messages"
	}, "request_completed with api=messages")
	if id, _ := ev["request_id"].(string); !e2eRequestID.MatchString(id) {
		t.Errorf("request_completed request_id = %q, want 16 hex chars", ev["request_id"])
	}
	if out, _ := ev["outcome"].(string); out != "completed" {
		t.Errorf("outcome = %q, want completed (%v)", out, ev["outcome"])
	}
}
