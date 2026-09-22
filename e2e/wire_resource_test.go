package e2e_test

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"syscall"
	"testing"
	"time"
)

// Wire-fidelity and resource scenarios: header allow-lists in both
// directions, redirects relayed verbatim (never followed), a long sequenced
// stream arriving complete and in order, and the listener/port actually
// released by a graceful stop.

// TestRedirectRelayedVerbatim: an upstream 302 is the upstream's answer, not
// a routing instruction. The client receives the redirect with its Location,
// and the injector never contacts the Location target.
func TestRedirectRelayedVerbatim(t *testing.T) {
	up := newFakeUpstream(t)
	bait := make(chan string, 1)
	up.setHandler(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/bait") {
			bait <- r.Method
			w.WriteHeader(http.StatusOK)
			return
		}
		target := "http://" + r.Host + "/bait"
		w.Header().Set("Location", target)
		w.Header().Set("Set-Cookie", "followed=1; Path=/") // must NOT be relayed
		w.WriteHeader(http.StatusFound)
	})
	p := startSubprocess(t, startOpts{
		yaml:     runtimeYAML(chatPublic, up.url()+"/v1", chatUpstream, ""),
		logLevel: "info",
	})

	// The test's own client must not follow the redirect either — the point
	// is to observe the 302 exactly as the injector relays it.
	noFollow := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}}
	req, err := http.NewRequest(http.MethodPost, "http://"+p.addr+"/v1/chat/completions",
		strings.NewReader(chatBody))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+e2eAPIKey)
	resp, err := noFollow.Do(req)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	body := readBody(t, resp) // readBody closes the body
	hdr := resp.Header.Clone()
	status := resp.StatusCode
	if status != http.StatusFound {
		t.Fatalf("status = %d, want 302 relayed verbatim", status)
	}
	if loc := hdr.Get("Location"); !strings.HasSuffix(loc, "/bait") {
		t.Fatalf("Location = %q, want the upstream's bait URL", loc)
	}
	if hdr.Get("Set-Cookie") != "" {
		t.Error("Set-Cookie relayed from upstream — outside the relay allow-list")
	}
	if len(body) != 0 {
		t.Errorf("redirect carried a body: %q", body)
	}
	// Give any (wrong) follow-up ample time to arrive, then require silence.
	time.Sleep(300 * time.Millisecond)
	if n := up.count(); n != 1 {
		t.Fatalf("upstream received %d requests, want exactly 1 (redirect must not be followed)", n)
	}
	select {
	case method := <-bait:
		t.Fatalf("the injector followed the redirect with %s", method)
	default:
	}
}

// TestForwardHeaderAllowList: only Content-Type, Accept and OpenAI-Beta
// travel upstream. Authorization authenticates the client TO the proxy and is
// consumed there — never forwarded — and every other client header, including
// credential-shaped ones, is dropped.
func TestForwardHeaderAllowList(t *testing.T) {
	up := newFakeUpstream(t)
	up.setHandler(jsonChatHandler(chatUpstream))
	p := startSubprocess(t, startOpts{
		yaml:     runtimeYAML(chatPublic, up.url()+"/v1", chatUpstream, ""),
		logLevel: "error",
	})

	hdr := map[string]string{
		"Authorization":   "Bearer " + e2eAPIKey, // consumed by the proxy
		"Accept":          "application/vnd.custom",
		"OpenAI-Beta":     "assistants=v2",
		"X-Api-Key":       "drop-me-secret",
		"Cookie":          "session=drop-me",
		"Idempotency-Key": "drop-me",
		"X-Custom":        "drop-me",
	}
	if status, _, _ := postJSON(t, p.addr, "/v1/chat/completions", chatBody, hdr); status != http.StatusOK {
		t.Fatalf("status = %d, want 200", status)
	}
	req, ok := up.last()
	if !ok {
		t.Fatal("upstream recorded no request")
	}
	for name, want := range map[string]string{
		"Accept":      "application/vnd.custom",
		"OpenAI-Beta": "assistants=v2",
	} {
		if got := req.Headers.Get(name); got != want {
			t.Errorf("upstream %s = %q, want %q", name, got, want)
		}
	}
	for _, name := range []string{"Authorization", "X-Api-Key", "Cookie", "Idempotency-Key", "X-Custom"} {
		if got := req.Headers.Get(name); got != "" {
			t.Errorf("upstream received dropped header %s = %q", name, got)
		}
	}
}

// TestRelayHeaderAllowListAnd429: rate-limit and retry headers are
// load-bearing and must survive; upstream set-cookies and fingerprint headers
// must not.
func TestRelayHeaderAllowListAnd429(t *testing.T) {
	up := newFakeUpstream(t)
	up.setHandler(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Retry-After", "7")
		w.Header().Set("X-RateLimit-Limit", "100")
		w.Header().Set("X-RateLimit-Remaining", "0")
		w.Header().Set("X-Request-Id", "rid-42")
		w.Header().Set("Set-Cookie", "sid=1; Path=/")
		w.Header().Set("X-Powered-By", "sneaky")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`{"error":{"message":"quota exhausted"}}`))
	})
	p := startSubprocess(t, startOpts{
		yaml:     runtimeYAML(chatPublic, up.url()+"/v1", chatUpstream, ""),
		logLevel: "error",
	})

	status, hdr, body := postJSON(t, p.addr, "/v1/chat/completions", chatBody, nil)
	if status != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429 forwarded verbatim", status)
	}
	for name, want := range map[string]string{
		"Retry-After":           "7",
		"X-RateLimit-Limit":     "100",
		"X-RateLimit-Remaining": "0",
		"X-Request-Id":          "rid-42",
	} {
		if got := hdr.Get(name); got != want {
			t.Errorf("client %s = %q, want %q", name, got, want)
		}
	}
	for _, name := range []string{"Set-Cookie", "X-Powered-By"} {
		if got := hdr.Get(name); got != "" {
			t.Errorf("client received non-allow-listed header %s = %q", name, got)
		}
	}
	if !strings.Contains(string(body), "quota exhausted") {
		t.Errorf("429 body not forwarded verbatim: %q", body)
	}
}

// TestLongStreamStabilityAndOrdering: 1500 sequenced events survive the proxy
// complete, ordered, and with every model rewritten — and the completion is
// logged, not truncated.
func TestLongStreamStabilityAndOrdering(t *testing.T) {
	const events = 1500
	up := newFakeUpstream(t)
	up.setHandler(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fl, _ := w.(http.Flusher)
		for i := 0; i < events; i++ {
			_, _ = fmt.Fprintf(w, "data: {\"model\":%q,\"i\":%d}\n\n", chatUpstream, i)
			if i%25 == 0 && fl != nil {
				fl.Flush()
			}
		}
		_, _ = io.WriteString(w, "data: [DONE]\n\n")
		if fl != nil {
			fl.Flush()
		}
	})
	p := startSubprocess(t, startOpts{
		yaml:     runtimeYAML(chatPublic, up.url()+"/v1", chatUpstream, ""),
		logLevel: "debug",
	})

	resp := openJSON(t, p.addr, "/v1/chat/completions",
		`{"model":"chat-public","stream":true,"messages":[{"role":"user","content":"go"}]}`, nil)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	br := bufio.NewReader(resp.Body)
	deadline := 30 * time.Second
	var got []string
	for {
		line, err := readSSELine(t, br, deadline)
		if err != nil {
			t.Fatalf("stream ended early after %d events: %v", len(got), err)
		}
		trimmed := strings.TrimRight(line, "\r\n")
		if trimmed == "data: [DONE]" {
			break
		}
		if strings.HasPrefix(trimmed, "data:") {
			got = append(got, strings.TrimPrefix(trimmed, "data: "))
		}
	}
	if len(got) != events {
		t.Fatalf("received %d events, want %d", len(got), events)
	}
	for i, payload := range got {
		want := fmt.Sprintf(`{"model":"%s","i":%d}`, chatPublic, i)
		if payload != want {
			t.Fatalf("event %d = %s, want %s (order or rewrite broken)", i, payload, want)
		}
	}

	waitForLogEvent(t, p, func(ev logEvent) bool {
		return ev["message"] == "stream_completed"
	}, "stream_completed")
}

// TestPortReleasedAfterShutdown: after a graceful stop the port is actually
// gone — a dial is refused and another process can bind the same address.
func TestPortReleasedAfterShutdown(t *testing.T) {
	up := newFakeUpstream(t)
	up.setHandler(jsonChatHandler(chatUpstream))
	port := freePort(t)
	listen := "127.0.0.1:" + port
	p := startSubprocess(t, startOpts{
		yaml:     runtimeYAML(chatPublic, up.url()+"/v1", chatUpstream, ""),
		listen:   listen,
		logLevel: "error",
	})
	if status, _, _ := postJSON(t, p.addr, "/v1/chat/completions", chatBody, nil); status != http.StatusOK {
		t.Fatalf("status = %d, want 200", status)
	}

	p.signal(syscall.SIGTERM)
	if code, err := p.waitExit(t, 8*time.Second); err != nil || code != 0 {
		t.Fatalf("shutdown: code=%d err=%v", code, err)
	}

	// A refused dial proves the listener is closed, not merely quiet.
	deadline := time.Now().Add(3 * time.Second)
	for {
		conn, err := net.DialTimeout("tcp", listen, 300*time.Millisecond)
		if err != nil {
			break // refused: the port is released
		}
		_ = conn.Close()
		if time.Now().After(deadline) {
			t.Fatal("port still accepting connections after graceful shutdown")
		}
		time.Sleep(50 * time.Millisecond)
	}

	// And the address is immediately reusable by a fresh instance.
	second := startSubprocess(t, startOpts{
		yaml:     runtimeYAML(chatPublic, up.url()+"/v1", chatUpstream, ""),
		listen:   listen,
		logLevel: "error",
	})
	if status, _, _ := postJSON(t, second.addr, "/v1/chat/completions", chatBody, nil); status != http.StatusOK {
		t.Fatalf("restart status = %d, want 200 on the same port", status)
	}
}
