// Keep-alive E2E scenarios: the SSE heartbeat the injector injects while a
// streamed response is silent, driven through the real binary. Durations
// are scaled to the config floor (1s) to keep the suite fast; the 15s
// default and its deployment rationale are pinned by the unit suite and
// documented in the README.
package e2e_test

import (
	"bufio"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

// chatSilenceHandler serves a chat SSE stream that emits one event, sits
// silent for the given stretch, then emits the second event and [DONE].
func chatSilenceHandler(silent time.Duration) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		fl := w.(http.Flusher)
		_, _ = io.WriteString(w, "data: {\"id\":\"s1\",\"model\":\"upstream-chat\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"chunk1\"}}]}\n\n")
		fl.Flush()
		time.Sleep(silent)
		_, _ = io.WriteString(w, "data: {\"id\":\"s2\",\"model\":\"upstream-chat\",\"choices\":[{\"index\":1,\"delta\":{\"content\":\"chunk2\"}}]}\n\n")
		fl.Flush()
		_, _ = io.WriteString(w, "data: [DONE]\n\n")
		fl.Flush()
	}
}

// responsesSilenceHandler serves a Responses-API SSE stream with the same
// silent stretch shape.
func responsesSilenceHandler(silent time.Duration) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		fl := w.(http.Flusher)
		_, _ = io.WriteString(w, "event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"model\":\"upstream-chat\",\"delta\":\"chunk1\"}\n\n")
		fl.Flush()
		time.Sleep(silent)
		_, _ = io.WriteString(w, "event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"model\":\"upstream-chat\"}}\n\n")
		fl.Flush()
	}
}

// readFullStream drains an SSE response into its events (each event the
// trimmed non-empty lines it carried).
func readFullStream(t *testing.T, resp *http.Response) [][]string {
	t.Helper()
	defer func() { _ = resp.Body.Close() }()
	br := bufio.NewReader(resp.Body)
	var events [][]string
	for {
		lines, eof := nextSSEEvent(t, br, 30*time.Second)
		if len(lines) > 0 {
			events = append(events, lines)
		}
		if eof {
			return events
		}
	}
}

// pingEvents counts the keep-alive comment events among the stream's
// events.
func pingEvents(events [][]string) int {
	n := 0
	for _, ev := range events {
		if len(ev) == 1 && ev[0] == ": ping" {
			n++
		}
	}
	return n
}

// assertPingBounds is the ping-count window a silent stretch of several
// intervals may produce: the ticker's phase starts at the header commit
// and the silence clock at the first forwarded byte, so pings land in
// [interval, 2*interval) after the last write.
func assertPingBounds(t *testing.T, pings, lo, hi int) {
	t.Helper()
	if pings < lo || pings > hi {
		t.Fatalf("keep-alive pings = %d, want %d-%d", pings, lo, hi)
	}
}

// assertPublicModel checks that every JSON data payload with a model field
// names the public model. Chat puts it at the top level while the Responses
// completion event nests it under response; the byte-preserving rewriters
// own both scopes. Chat's [DONE] is a non-JSON terminator and passes through.
func assertPublicModel(t *testing.T, lines []string) {
	t.Helper()
	for _, ln := range lines {
		if !strings.HasPrefix(ln, "data:") || ln == "data: [DONE]" || !strings.Contains(ln, `"model"`) {
			continue
		}
		if !strings.Contains(ln, `"model":"`+chatPublic+`"`) {
			t.Fatalf("SSE model not rewritten to %q (line %q)", chatPublic, ln)
		}
	}
}

// TestKeepAlivePingsDuringUpstreamSilence drives the headline scenario
// through the real binary on both API surfaces: a silent stretch longer
// than the interval earns comment pings, and the real events still arrive
// — unmodified apart from the model rewrite — once the upstream speaks.
func TestKeepAlivePingsDuringUpstreamSilence(t *testing.T) {
	cases := []struct {
		name    string
		path    string
		body    string
		handler http.HandlerFunc
	}{
		{
			name:    "chat",
			path:    "/v1/chat/completions",
			body:    chatStreamRequest(chatPublic),
			handler: chatSilenceHandler(3200 * time.Millisecond),
		},
		{
			name:    "responses",
			path:    "/v1/responses",
			body:    fmt.Sprintf(`{"model":%q,"input":"hello","stream":true}`, chatPublic),
			handler: responsesSilenceHandler(3200 * time.Millisecond),
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			up := newFakeUpstream(t)
			up.setHandler(tc.handler)
			p := startSubprocess(t, startOpts{
				yaml: runtimeYAML(chatPublic, up.url()+"/v1", chatUpstream, "") +
					"sse-keep-alive:\n  interval: 1s\n",
			})

			resp := openJSON(t, p.addr, tc.path, tc.body,
				map[string]string{"Accept": "text/event-stream"})
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("status = %d, want 200", resp.StatusCode)
			}
			events := readFullStream(t, resp)
			assertPingBounds(t, pingEvents(events), 1, 3)
			// Apart from the ignorable comments, every event carries real
			// upstream payload with the public model — nothing else was
			// injected.
			real := 0
			for _, ev := range events {
				if len(ev) == 1 && ev[0] == ": ping" {
					continue
				}
				real++
				assertPublicModel(t, ev)
			}
			if real == 0 {
				t.Fatalf("no real events relayed: %v", events)
			}
		})
	}
}

// TestKeepAliveQuietOnActiveStream pins the reset rule's other half: an
// upstream that keeps emitting never lets the client go silent, so not one
// ping may appear and the relay stays byte-exact.
func TestKeepAliveQuietOnActiveStream(t *testing.T) {
	up := newFakeUpstream(t)
	up.setHandler(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		fl := w.(http.Flusher)
		for i := range 6 {
			_, _ = fmt.Fprintf(w, "data: {\"id\":\"s%d\",\"model\":\"upstream-chat\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"c%d\"}}]}\n\n", i, i)
			fl.Flush()
			time.Sleep(300 * time.Millisecond)
		}
		_, _ = io.WriteString(w, "data: [DONE]\n\n")
		fl.Flush()
	})
	p := startSubprocess(t, startOpts{
		yaml: runtimeYAML(chatPublic, up.url()+"/v1", chatUpstream, "") +
			"sse-keep-alive:\n  interval: 1s\n",
	})

	resp := openJSON(t, p.addr, "/v1/chat/completions", chatStreamRequest(chatPublic),
		map[string]string{"Accept": "text/event-stream"})
	events := readFullStream(t, resp)
	if pings := pingEvents(events); pings != 0 {
		t.Fatalf("active stream received %d keep-alive pings, want 0", pings)
	}
	if len(events) != 7 { // six data events + [DONE]
		t.Fatalf("events = %d, want 7: %v", len(events), events)
	}
}

// TestKeepAliveDisabledByConfig pins the opt-out through the real binary:
// enabled: false leaves even a long silent stream untouched.
func TestKeepAliveDisabledByConfig(t *testing.T) {
	up := newFakeUpstream(t)
	up.setHandler(chatSilenceHandler(2200 * time.Millisecond))
	p := startSubprocess(t, startOpts{
		yaml: runtimeYAML(chatPublic, up.url()+"/v1", chatUpstream, "") +
			"sse-keep-alive:\n  enabled: false\n",
	})

	resp := openJSON(t, p.addr, "/v1/chat/completions", chatStreamRequest(chatPublic),
		map[string]string{"Accept": "text/event-stream"})
	events := readFullStream(t, resp)
	if pings := pingEvents(events); pings != 0 {
		t.Fatalf("disabled heartbeat produced %d pings", pings)
	}
}

// TestKeepAliveNonStreamingUntouched pins the scope rule end to end: with
// the heartbeat on by default (no block in the config), a non-streaming
// request/response pair is byte-for-byte what an unconfigured deployment
// serves — the feature never touches buffered traffic.
func TestKeepAliveNonStreamingUntouched(t *testing.T) {
	up := newFakeUpstream(t)
	up.setHandler(jsonChatHandler(chatUpstream))
	p := startSubprocess(t, startOpts{
		yaml: runtimeYAML(chatPublic, up.url()+"/v1", chatUpstream, ""),
	})

	request := fmt.Sprintf(`{"model":%q,"messages":[{"role":"user","content":"hello"}]}`, chatPublic)
	status, _, body := postJSON(t, p.addr, "/v1/chat/completions", request, nil)
	if status != http.StatusOK {
		t.Fatalf("status = %d, want 200", status)
	}
	if strings.Contains(string(body), ": ping") {
		t.Fatal("ping bytes in a buffered response")
	}
	if m := decodeMap(t, body); m["model"] != chatPublic {
		t.Fatalf("buffered model = %v, want %q", m["model"], chatPublic)
	}
}

// TestKeepAliveReloadTogglesHeartbeat pins the hot-reload contract through
// the real binary: the block rides the same validate-then-publish path as
// the model mapping — a valid rewrite flips the heartbeat without a
// restart, and an invalid one keeps the last-known-good settings serving.
func TestKeepAliveReloadTogglesHeartbeat(t *testing.T) {
	up := newFakeUpstream(t)
	up.setHandler(chatSilenceHandler(2200 * time.Millisecond))
	p := startSubprocess(t, startOpts{
		yaml: runtimeYAML(chatPublic, up.url()+"/v1", chatUpstream, "") +
			"sse-keep-alive:\n  enabled: false\n",
		logLevel: "info",
	})

	// Off at boot: a silent stream earns nothing.
	resp := openJSON(t, p.addr, "/v1/chat/completions", chatStreamRequest(chatPublic),
		map[string]string{"Accept": "text/event-stream"})
	if pings := pingEvents(readFullStream(t, resp)); pings != 0 {
		t.Fatalf("heartbeat before reload produced %d pings, want 0", pings)
	}

	// An invalid interval rejects the file wholesale: the last-known-good
	// (off) settings keep serving.
	rewriteConfig(t, p.cfgPath, runtimeYAML(chatPublic, up.url()+"/v1", chatUpstream, "")+
		"sse-keep-alive:\n  interval: 0s\n")
	waitForLogEvent(t, p, func(ev logEvent) bool { return ev["message"] == "config_reload_rejected" },
		"config_reload_rejected")
	resp = openJSON(t, p.addr, "/v1/chat/completions", chatStreamRequest(chatPublic),
		map[string]string{"Accept": "text/event-stream"})
	if pings := pingEvents(readFullStream(t, resp)); pings != 0 {
		t.Fatalf("rejected reload changed the heartbeat: %d pings, want 0", pings)
	}

	// A valid rewrite turns the heartbeat on at 1s for subsequent requests.
	rewriteConfig(t, p.cfgPath, runtimeYAML(chatPublic, up.url()+"/v1", chatUpstream, "")+
		"sse-keep-alive:\n  enabled: true\n  interval: 1s\n")
	waitForLogEvent(t, p, func(ev logEvent) bool { return ev["message"] == "config_reloaded" },
		"config_reloaded")
	resp = openJSON(t, p.addr, "/v1/chat/completions", chatStreamRequest(chatPublic),
		map[string]string{"Accept": "text/event-stream"})
	events := readFullStream(t, resp)
	assertPingBounds(t, pingEvents(events), 1, 3)
}
