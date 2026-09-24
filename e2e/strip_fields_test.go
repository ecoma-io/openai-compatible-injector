package e2e_test

import (
	"bufio"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

// stripYAML renders a model entry with a strip-fields block. fields is the
// literal verbatim list (each one item, no leading dashes); the block is
// injected as extra model members so runtimeYAML indents them correctly.
// Empty fields = no block (default-off).
func stripYAML(publicName, endpoint, upstreamModel string, fields ...string) string {
	var extra []string
	if len(fields) > 0 {
		extra = append(extra, "strip-fields:")
		for _, f := range fields {
			extra = append(extra, "  - "+f)
		}
	}
	return runtimeYAML(publicName, endpoint, upstreamModel, "", extra...)
}

// stripFieldsOf digs a top-level field name out of a buffered JSON object;
// empty string when absent.
func stripFieldOf(t *testing.T, body []byte, key string) string {
	t.Helper()
	var m map[string]json.RawMessage
	if err := json.Unmarshal(body, &m); err != nil {
		t.Fatalf("unmarshal %s: %v", body, err)
	}
	if raw, ok := m[key]; ok {
		var s string
		if json.Unmarshal(raw, &s) != nil {
			return "<non-string>"
		}
		return s
	}
	return ""
}

// TestStripFieldsBufferedChat pins the buffered chat path: both Kilo
// provider-added fields are excised and the model rename still applies.
func TestStripFieldsBufferedChat(t *testing.T) {
	up := newFakeUpstream(t)
	up.setHandler(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"id":"c1","model":"upstream-chat","provider":"kilo","service_tier":"std","choices":[{"index":0,"message":{"role":"assistant","content":"hi"}}],"usage":{"prompt_tokens":10,"completion_tokens":20,"total_tokens":30}}`)
	})
	p := startSubprocess(t, startOpts{
		yaml: stripYAML("strip-public", up.url()+"/v1", "upstream-chat", "provider", "service_tier"),
	})

	status, _, body := postJSON(t, p.addr, "/v1/chat/completions",
		`{"model":"strip-public","messages":[]}`, nil)
	if status != http.StatusOK {
		t.Fatalf("status %d, want 200", status)
	}
	m := decodeMap(t, body)
	if _, ok := m["provider"]; ok {
		t.Fatalf("provider survived stripping: %s", body)
	}
	if _, ok := m["service_tier"]; ok {
		t.Fatalf("service_tier survived stripping: %s", body)
	}
	if m["model"] != "strip-public" {
		t.Fatalf("model %v, want strip-public (rename still applies)", m["model"])
	}
}

// TestStripFieldsDefaultOff pins byte identity with no strip config: the
// provider field must survive untouched on buffered chat.
func TestStripFieldsDefaultOff(t *testing.T) {
	up := newFakeUpstream(t)
	up.setHandler(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"model":"upstream-chat","provider":"kilo","service_tier":"std","choices":[]}`)
	})
	p := startSubprocess(t, startOpts{
		yaml: stripYAML("strip-public", up.url()+"/v1", "upstream-chat"),
	})

	status, _, body := postJSON(t, p.addr, "/v1/chat/completions",
		`{"model":"strip-public","messages":[]}`, nil)
	if status != http.StatusOK {
		t.Fatalf("status %d, want 200", status)
	}
	if got := stripFieldOf(t, body, "provider"); got != "kilo" {
		t.Fatalf("provider field absent without strip config: %s", body)
	}
	if got := stripFieldOf(t, body, "service_tier"); got != "std" {
		t.Fatalf("service_tier field absent without strip config: %s", body)
	}
}

// TestStripFieldsStreamedChat pins the widened SSE gate end to end: a
// streamed chunk carrying ONLY the to-be-excised key (no model/usage) must
// be stripped on the wire, and [DONE] must pass through untouched.
func TestStripFieldsStreamedChat(t *testing.T) {
	up := newFakeUpstream(t)
	up.setHandler(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: {\"service_tier\":\"std\",\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n")
		_, _ = io.WriteString(w, "data: [DONE]\n\n")
	})
	p := startSubprocess(t, startOpts{
		yaml: stripYAML("strip-public", up.url()+"/v1", "upstream-chat", "service_tier"),
	})

	resp := openJSON(t, p.addr, "/v1/chat/completions",
		`{"model":"strip-public","messages":[],"stream":true}`, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d, want 200", resp.StatusCode)
	}
	br := bufio.NewReader(resp.Body)
	lines, eof := nextSSEEvent(t, br, 3*time.Second)
	if eof {
		t.Fatalf("stream ended before the content event")
	}
	for _, ln := range lines {
		payload := sseDataContent(t, ln)
		if strings.Contains(payload, "service_tier") {
			t.Fatalf("streamed chunk still carries service_tier: %s", payload)
		}
		if !strings.Contains(payload, `"choices"`) {
			t.Fatalf("streamed chunk lost its choices payload: %s", payload)
		}
	}
	lines, _ = nextSSEEvent(t, br, 3*time.Second)
	foundDone := false
	for _, ln := range lines {
		if ln == "data: [DONE]" {
			foundDone = true
		}
	}
	if !foundDone {
		t.Fatalf("terminator lost: %v", lines)
	}
}

// TestStripFieldsBufferedResponses pins the Responses-scope descent: the
// service_tier inside the response envelope is stripped, and the model
// rewrite still lands the public name.
func TestStripFieldsBufferedResponses(t *testing.T) {
	up := newFakeUpstream(t)
	up.setHandler(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"type":"response.completed","response":{"id":"r","model":"upstream-chat","provider":"kilo","service_tier":"std"},"sequence_number":1}`)
	})
	p := startSubprocess(t, startOpts{
		yaml: stripYAML("strip-public", up.url()+"/v1", "upstream-chat", "provider", "service_tier"),
	})

	status, _, body := postJSON(t, p.addr, "/v1/responses",
		`{"model":"strip-public","input":""}`, nil)
	if status != http.StatusOK {
		t.Fatalf("status %d, want 200", status)
	}
	var parsed struct {
		Response map[string]json.RawMessage `json:"response"`
	}
	if err := json.Unmarshal(body, &parsed); err != nil {
		t.Fatalf("unmarshal %s: %v", body, err)
	}
	if _, ok := parsed.Response["provider"]; ok {
		t.Fatalf("response.provider survived stripping: %s", body)
	}
	if _, ok := parsed.Response["service_tier"]; ok {
		t.Fatalf("response.service_tier survived stripping: %s", body)
	}
	var model string
	if err := json.Unmarshal(parsed.Response["model"], &model); err != nil || model != "strip-public" {
		t.Fatalf("response.model %q, want strip-public", model)
	}
}

// TestStripFieldsReloadSwitchesList pins reload binding: a strip-fields
// block added by a runtime reload on a model that previously had none
// applies new requests and leaves the running stream on its own snapshot.
func TestStripFieldsReloadBinding(t *testing.T) {
	up := newFakeUpstream(t)
	up.setHandler(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"model":"upstream-chat","provider":"kilo","service_tier":"std","choices":[]}`)
	})
	p := startSubprocess(t, startOpts{
		yaml:     stripYAML("strip-public", up.url()+"/v1", "upstream-chat"),
		logLevel: "info",
	})

	// First request: no strip config, provider survives.
	status, _, body := postJSON(t, p.addr, "/v1/chat/completions",
		`{"model":"strip-public","messages":[]}`, nil)
	if status != http.StatusOK {
		t.Fatalf("status %d, want 200", status)
	}
	if got := stripFieldOf(t, body, "provider"); got != "kilo" {
		t.Fatalf("pre-reload provider field absent: %s", body)
	}

	// Reload with the strip block.
	rewriteConfig(t, p.cfgPath,
		stripYAML("strip-public", up.url()+"/v1", "upstream-chat", "provider", "service_tier"))
	waitForLogEvent(t, p, func(ev logEvent) bool { return ev["message"] == "config_reloaded" },
		"config_reloaded")

	status, _, body = postJSON(t, p.addr, "/v1/chat/completions",
		`{"model":"strip-public","messages":[]}`, nil)
	if status != http.StatusOK {
		t.Fatalf("status %d, want 200", status)
	}
	if got := stripFieldOf(t, body, "provider"); got != "" {
		t.Fatalf("post-reload provider survived: %s", body)
	}
}

// TestStripFieldsUsageChildren pins the narrow reserved rule end to end on
// the motivating case: the provider's billing metadata INSIDE the usage
// object is addressable ("usage.is_byok", "usage.cost", "usage.cost_details")
// while the token counts beside it, and the reasoning count the synthesis
// writes among them, survive. Before the exact-path rule those children were
// unreachable by configuration — a segment-level "usage" ban rejected the
// whole list — so this is issue #65's motivating case.
func TestStripFieldsUsageChildren(t *testing.T) {
	up := newFakeUpstream(t)
	up.setHandler(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"id":"c1","model":"upstream-chat",`+
			`"choices":[{"index":0,"message":{"role":"assistant","content":"hi"}}],`+
			`"usage":{"prompt_tokens":10,"completion_tokens":100,"total_tokens":110,`+
			`"cost":0,"is_byok":false,"cost_details":{"upstream_inference_cost":0}}}`)
	})
	p := startSubprocess(t, startOpts{
		yaml: runtimeYAML("strip-public", up.url()+"/v1", "upstream-chat", "",
			"strip-fields:",
			"  - usage.is_byok",
			"  - usage.cost",
			"  - usage.cost_details",
			"thinking-usage:",
			"  mode: always",
		),
	})

	status, _, body := postJSON(t, p.addr, "/v1/chat/completions",
		`{"model":"strip-public","messages":[]}`, nil)
	if status != http.StatusOK {
		t.Fatalf("status %d, want 200", status)
	}
	var parsed struct {
		Usage map[string]json.RawMessage `json:"usage"`
	}
	if err := json.Unmarshal(body, &parsed); err != nil {
		t.Fatalf("unmarshal %s: %v", body, err)
	}
	if parsed.Usage == nil {
		t.Fatalf("usage object lost entirely: %s", body)
	}
	for _, gone := range []string{"is_byok", "cost", "cost_details"} {
		if _, ok := parsed.Usage[gone]; ok {
			t.Errorf("usage.%s survived stripping: %s", gone, body)
		}
	}
	for _, kept := range []string{"prompt_tokens", "completion_tokens", "total_tokens"} {
		if _, ok := parsed.Usage[kept]; !ok {
			t.Errorf("usage.%s was stripped, want it kept: %s", kept, body)
		}
	}
	// The synthesis runs BEFORE the strip and its leaf is reserved, so the
	// sibling excisions must not have touched it.
	raw, ok := parsed.Usage["completion_tokens_details"]
	if !ok {
		t.Fatalf("synthesized usage.completion_tokens_details lost: %s", body)
	}
	var details struct {
		ReasoningTokens int `json:"reasoning_tokens"`
	}
	if err := json.Unmarshal(raw, &details); err != nil || details.ReasoningTokens != 75 {
		t.Fatalf("synthesized reasoning_tokens = %+v (%v), want 75", details, err)
	}
}

// TestStripFieldsUsageChildrenResponses pins the same rule inside the
// envelope: one "usage.is_byok" entry reaches the usage object nested in a
// top-level "response" through the single descent, while the token counts in
// that same object survive.
func TestStripFieldsUsageChildrenResponses(t *testing.T) {
	up := newFakeUpstream(t)
	up.setHandler(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"type":"response.completed","response":{"id":"r",`+
			`"model":"upstream-chat","usage":{"input_tokens":5,"output_tokens":20,"is_byok":false}},`+
			`"sequence_number":1}`)
	})
	p := startSubprocess(t, startOpts{
		yaml: stripYAML("strip-public", up.url()+"/v1", "upstream-chat", "usage.is_byok"),
	})

	status, _, body := postJSON(t, p.addr, "/v1/responses",
		`{"model":"strip-public","input":""}`, nil)
	if status != http.StatusOK {
		t.Fatalf("status %d, want 200", status)
	}
	var parsed struct {
		Response struct {
			Usage map[string]json.RawMessage `json:"usage"`
		} `json:"response"`
	}
	if err := json.Unmarshal(body, &parsed); err != nil {
		t.Fatalf("unmarshal %s: %v", body, err)
	}
	if _, ok := parsed.Response.Usage["is_byok"]; ok {
		t.Fatalf("response.usage.is_byok survived stripping: %s", body)
	}
	for _, kept := range []string{"input_tokens", "output_tokens"} {
		if _, ok := parsed.Response.Usage[kept]; !ok {
			t.Errorf("response.usage.%s was stripped, want it kept: %s", kept, body)
		}
	}
}
