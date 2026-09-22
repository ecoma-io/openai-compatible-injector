package e2e_test

import (
	"bufio"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// forwardingProxy is an in-test HTTP forward proxy: the injector's proxied
// egress lands here in absolute-form and is relayed to the origin with an
// X-Via-Proxy marker added. That marker is the traversal proof — the origin
// is directly reachable from the test process, so the only way the marker
// arrives is by riding the configured proxy.
type forwardingProxy struct {
	srv  *httptest.Server
	mu   sync.Mutex
	hits int
}

func newForwardingProxy(t *testing.T) *forwardingProxy {
	t.Helper()
	fp := &forwardingProxy{}
	hop := &http.Client{Timeout: 30 * time.Second}
	fp.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodConnect {
			// Not exercised here: the suite's origins are http-form.
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		fp.mu.Lock()
		fp.hits++
		fp.mu.Unlock()
		out, err := http.NewRequest(r.Method, r.URL.String(), r.Body)
		if err != nil {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		out.Header = r.Header.Clone()
		out.Header.Set("X-Via-Proxy", "1")
		resp, err := hop.Do(out)
		if err != nil {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		defer func() { _ = resp.Body.Close() }()
		for k, vv := range resp.Header {
			for _, v := range vv {
				w.Header().Add(k, v)
			}
		}
		w.WriteHeader(resp.StatusCode)
		flushCopy(w, resp.Body)
	}))
	t.Cleanup(fp.srv.Close)
	return fp
}

func (fp *forwardingProxy) count() int {
	fp.mu.Lock()
	defer fp.mu.Unlock()
	return fp.hits
}

// flushCopy relays a body flushing after every read, so paced SSE rides
// this hop live instead of being buffered into one delivery.
func flushCopy(w io.Writer, r io.Reader) {
	f, canFlush := w.(http.Flusher)
	buf := make([]byte, 8192)
	for {
		n, err := r.Read(buf)
		if n > 0 {
			if _, werr := w.Write(buf[:n]); werr != nil {
				return
			}
			if canFlush {
				f.Flush()
			}
		}
		if err != nil {
			return
		}
	}
}

// transportsYAML renders a runtime file with one proxied provider model
// and one legacy direct model sharing the same upstream, so each test gets
// both forms in one process.
func transportsYAML(proxyURL, upstreamBase string) string {
	return fmt.Sprintf(`api-key: %s
transports:
  egress:
    type: proxy
    proxy: %s
providers:
  opencode:
    base-url: %s/v1
    transport: egress
models:
  proxied-model:
    provider: opencode
    upstream-model: up-model
  direct-model:
    endpoint: %s/v1
    upstream-model: up-model
`, e2eAPIKey, proxyURL, upstreamBase, upstreamBase)
}

// TestProviderRoutesThroughConfiguredProxy pins the whole chain end to
// end against the real binary: a provider+proxy model's request traverses
// the configured proxy (the origin sees the proxy's marker and nothing
// else changed — upstream model, path, auth), the model rename and prompt
// injection still apply, and the legacy endpoint model in the same process
// never touches the proxy. The quiet direction is proxy egress silently
// becoming direct egress.
func TestProviderRoutesThroughConfiguredProxy(t *testing.T) {
	up := newFakeUpstream(t)
	up.setHandler(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"id":"1","object":"chat.completion","model":"up-model","choices":[{"index":0,"message":{"role":"assistant","content":"ok"}}]}`)
	})
	fp := newForwardingProxy(t)
	p := startSubprocess(t, startOpts{yaml: transportsYAML(fp.srv.URL, up.url())})

	code, _, body := postJSON(t, p.addr, "/v1/chat/completions",
		`{"model":"proxied-model","messages":[{"role":"user","content":"hi"}]}`, nil)
	if code != http.StatusOK {
		t.Fatalf("status = %d, body %s", code, body)
	}
	if fp.count() != 1 {
		t.Fatalf("proxy traversed %d times, want exactly 1", fp.count())
	}
	req, ok := up.last()
	if !ok {
		t.Fatal("upstream received no request")
	}
	if req.Headers.Get("X-Via-Proxy") != "1" {
		t.Errorf("upstream request lacks the proxy marker — egress did not traverse the configured proxy")
	}
	if !strings.Contains(string(req.Body), `"model":"up-model"`) {
		t.Errorf("upstream body = %s, want the rewritten upstream model", req.Body)
	}
	// The client-facing rename still applies on the proxied path.
	m := decodeMap(t, body)
	if m["model"] != "proxied-model" {
		t.Errorf("client-facing model = %v, want proxied-model", m["model"])
	}

	// Control: the legacy direct model in the same process stays direct.
	code, _, _ = postJSON(t, p.addr, "/v1/chat/completions",
		`{"model":"direct-model","messages":[{"role":"user","content":"hi"}]}`, nil)
	if code != http.StatusOK {
		t.Fatalf("legacy model status = %d", code)
	}
	if fp.count() != 1 {
		t.Errorf("legacy endpoint model traversed the proxy (%d hits)", fp.count())
	}
	req, _ = up.last()
	if req.Headers.Get("X-Via-Proxy") != "" {
		t.Errorf("direct model's request carries the proxy marker")
	}
}

// TestProxyTransportStreamsSSEEndToEnd pins live streaming through the
// proxied path: three SSE events paced 250ms apart must arrive spread over
// real time, not as one buffered delivery at stream end.
func TestProxyTransportStreamsSSEEndToEnd(t *testing.T) {
	up := newFakeUpstream(t)
	up.setHandler(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		f := w.(http.Flusher)
		for i := 1; i <= 3; i++ {
			_, _ = fmt.Fprintf(w, "data: {\"i\":%d}\n\n", i)
			f.Flush()
			time.Sleep(250 * time.Millisecond)
		}
		_, _ = io.WriteString(w, "data: [DONE]\n\n")
		f.Flush()
	})
	fp := newForwardingProxy(t)
	p := startSubprocess(t, startOpts{yaml: transportsYAML(fp.srv.URL, up.url())})

	resp := openJSON(t, p.addr, "/v1/chat/completions",
		`{"model":"proxied-model","stream":true,"messages":[{"role":"user","content":"hi"}]}`,
		map[string]string{"Accept": "text/event-stream"})
	defer func() { _ = resp.Body.Close() }()
	if ct := resp.Header.Get("Content-Type"); !strings.Contains(ct, "text/event-stream") {
		t.Fatalf("content type = %q", ct)
	}

	br := bufio.NewReader(resp.Body)
	var first, last time.Time
	events := 0
	seenDone := false
	deadline := time.Now().Add(20 * time.Second)
	for !seenDone && time.Now().Before(deadline) {
		lines, eof := nextSSEEvent(t, br, 10*time.Second)
		for _, l := range lines {
			payload := strings.TrimSpace(strings.TrimPrefix(l, "data:"))
			if strings.HasPrefix(l, "data:") && payload == "[DONE]" {
				seenDone = true
				continue
			}
			if strings.HasPrefix(l, "data:") {
				events++
				if first.IsZero() {
					first = time.Now()
				} else {
					last = time.Now()
				}
			}
		}
		if eof && !seenDone {
			t.Fatalf("stream ended before [DONE] (events so far: %d)", events)
		}
	}
	if !seenDone {
		t.Fatal("no [DONE] within the deadline")
	}
	if events != 3 {
		t.Fatalf("received %d data events, want 3", events)
	}
	if elapsed := last.Sub(first); elapsed < 350*time.Millisecond {
		t.Errorf("events arrived within %v of each other — the stream was buffered", elapsed)
	}
}

// TestInvalidTransportConfigFailsBoot pins that a broken transports table
// is a startup failure, never a silent direct fallback.
func TestInvalidTransportConfigFailsBoot(t *testing.T) {
	code, stderr := startSubprocessExpectExit(t, startOpts{
		yaml: fmt.Sprintf(`api-key: %s
transports:
  egress:
    type: proxy
providers:
  opencode:
    base-url: http://127.0.0.1:1/v1
    transport: egress
models:
  m:
    provider: opencode
    upstream-model: up
`, e2eAPIKey),
	})
	if code != 1 {
		t.Fatalf("exit code = %d, want 1", code)
	}
	if !strings.Contains(stderr, "config_load_failed") {
		t.Errorf("stderr = %s", stderr)
	}
}

// TestInvalidTransportReloadKeepsLastKnownGood pins the reload half: a
// rewrite that breaks the transports table is rejected with
// config_reload_rejected and the process keeps serving the previous
// snapshot — requests still traverse the previously configured proxy.
func TestInvalidTransportReloadKeepsLastKnownGood(t *testing.T) {
	up := newFakeUpstream(t)
	up.setHandler(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"id":"1","object":"chat.completion","model":"up-model","choices":[{"index":0,"message":{"role":"assistant","content":"ok"}}]}`)
	})
	fp := newForwardingProxy(t)
	valid := transportsYAML(fp.srv.URL, up.url())
	// log-level info so the WARN rejection is visible at the default gate.
	p := startSubprocess(t, startOpts{yaml: withLoggingLevel(valid, "info")})

	code, _, _ := postJSON(t, p.addr, "/v1/chat/completions",
		`{"model":"proxied-model","messages":[{"role":"user","content":"hi"}]}`, nil)
	if code != http.StatusOK {
		t.Fatalf("pre-reload status = %d", code)
	}

	broken := fmt.Sprintf(`api-key: %s
transports:
  egress:
    type: warp
providers:
  opencode:
    base-url: %s/v1
    transport: egress
models:
  proxied-model:
    provider: opencode
    upstream-model: up-model
`, e2eAPIKey, up.url())
	rewriteConfig(t, p.cfgPath, broken)
	waitForEventCount(t, p, "config_reload_rejected", 1)

	code, _, _ = postJSON(t, p.addr, "/v1/chat/completions",
		`{"model":"proxied-model","messages":[{"role":"user","content":"hi"}]}`, nil)
	if code != http.StatusOK {
		t.Fatalf("post-reload status = %d, want the last-known-good snapshot", code)
	}
	if fp.count() != 2 {
		t.Errorf("proxy traversed %d times, want 2 (pre- and post-reload)", fp.count())
	}
}
