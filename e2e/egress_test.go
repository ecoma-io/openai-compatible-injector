package e2e_test

import (
	"bufio"
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// The egress pool contract, black-box: a pool transport schedules each
// request onto ONE member endpoint, skipping members the request cannot use
// (stream flag, body size) without a dial, falling back across pre-response
// transport failures only, and treating every HTTP status — 429 and 5xx
// included — as an answer. Every scenario drives the real binary over real
// proxy hops stubbed in this package: an HTTP forward proxy (absolute-form),
// a SOCKS5 CONNECT stub, an accept-and-close endpoint, and a refusing
// proxy. Counters on those stubs are the traversal proof — the upstreams are
// directly reachable from the test process, so a hit can only mean the
// request rode the configured egress.

// egressUpstreamModel is the upstream model name every pooled model in this
// file forwards as; asserting it in recorded upstream bodies proves the pool
// path ran the ordinary request transform.
const egressUpstreamModel = "pool-up-model"

// envelopeUpstreamUnreachable is the canonical 502 body the pool's failure
// paths must produce, kept literal: the wire bytes are the contract.
const envelopeUpstreamUnreachable = `{"error":{"message":"upstream request failed","type":"upstream_error","code":"upstream_unreachable"}}`

// egressChatOK is the minimal chat completion answer the suite's upstream
// serves on the happy paths.
func egressChatOK(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_, _ = io.WriteString(w, `{"id":"e1","object":"chat.completion","model":"`+egressUpstreamModel+`","choices":[{"index":0,"message":{"role":"assistant","content":"ok"}}]}`)
}

// ---- in-package egress stubs ----

// deadEgress is an egress endpoint that accepts every TCP connection and
// closes it at once — a hop that dies before it can carry anything. The
// accept counter is the only observable record of a dial: nothing answers,
// so no upstream-side count can exist. Use proxyURL for the member URL.
type deadEgress struct {
	ln    net.Listener
	mu    sync.Mutex
	dials int
}

// proxyURL is the URL a pool member must be configured with to exercise
// this endpoint. The scheme is load-bearing rather than cosmetic: an HTTP
// forward proxy member is handed the REQUEST itself, so its Accept-then-
// close failure is send-unknown — the injector cannot prove the member did
// not read what it wrote — and the pool must not replay it. Only a failure
// in the tunnel phase provably precedes the request, and the SOCKS5
// handshake is where that evidence lives (its I/O failures are the typed
// ProxyConnectError the send-state rule reads).
func (d *deadEgress) proxyURL() string { return "socks5://" + d.addr() }

func newDeadEgress(t *testing.T) *deadEgress {
	t.Helper()
	d := &deadEgress{}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	d.ln = ln
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			d.mu.Lock()
			d.dials++
			d.mu.Unlock()
			_ = conn.Close()
		}
	}()
	return d
}

func (d *deadEgress) addr() string { return d.ln.Addr().String() }

func (d *deadEgress) count() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.dials
}

// socks5Egress is a minimal SOCKS5 server (RFC 1928 no-auth greeting,
// CONNECT) with a hit counter: every CONNECT it serves is one request that
// traversed this egress. It relays each tunnel to the CONNECT target, so the
// upstream sees the request exactly as if the caller had dialed it directly.
type socks5Egress struct {
	ln    net.Listener
	mu    sync.Mutex
	hits  int
	conns []net.Conn
}

func newSocks5Egress(t *testing.T) *socks5Egress {
	t.Helper()
	s := &socks5Egress{}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s.ln = ln
	t.Cleanup(func() {
		_ = ln.Close()
		s.mu.Lock()
		for _, c := range s.conns {
			_ = c.Close()
		}
		s.mu.Unlock()
	})
	go s.accept()
	return s
}

func (s *socks5Egress) addr() string { return s.ln.Addr().String() }

func (s *socks5Egress) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.hits
}

func (s *socks5Egress) accept() {
	for {
		conn, err := s.ln.Accept()
		if err != nil {
			return
		}
		s.mu.Lock()
		s.conns = append(s.conns, conn)
		s.mu.Unlock()
		go func() { _ = s.handle(conn) }()
	}
}

func (s *socks5Egress) handle(conn net.Conn) error {
	head := make([]byte, 2)
	if _, err := io.ReadFull(conn, head); err != nil {
		return err
	}
	methods := make([]byte, int(head[1]))
	if _, err := io.ReadFull(conn, methods); err != nil {
		return err
	}
	if err := socksWriteAll(conn, []byte{0x05, 0x00}); err != nil { // no-auth
		return err
	}
	hdr := make([]byte, 4)
	if _, err := io.ReadFull(conn, hdr); err != nil {
		return err
	}
	if hdr[0] != 0x05 || hdr[1] != 0x01 { // only CONNECT is spoken here
		return nil
	}
	var target string
	switch hdr[3] {
	case 0x01:
		b := make([]byte, 4)
		if _, err := io.ReadFull(conn, b); err != nil {
			return err
		}
		target = net.IP(b).String()
	case 0x03:
		l := make([]byte, 1)
		if _, err := io.ReadFull(conn, l); err != nil {
			return err
		}
		d := make([]byte, int(l[0]))
		if _, err := io.ReadFull(conn, d); err != nil {
			return err
		}
		target = string(d)
	case 0x04:
		b := make([]byte, 16)
		if _, err := io.ReadFull(conn, b); err != nil {
			return err
		}
		target = net.IP(b).String()
	default:
		return nil
	}
	pb := make([]byte, 2)
	if _, err := io.ReadFull(conn, pb); err != nil {
		return err
	}
	port := int(pb[0])<<8 | int(pb[1])

	s.mu.Lock()
	s.hits++
	s.mu.Unlock()

	dst, err := net.Dial("tcp", net.JoinHostPort(target, strconv.Itoa(port)))
	if err != nil {
		return socksWriteAll(conn, []byte{0x05, 0x04, 0x00, 0x01, 0, 0, 0, 0, 0, 0})
	}
	if err := socksWriteAll(conn, []byte{0x05, 0x00, 0x00, 0x01, 0, 0, 0, 0, 0, 0}); err != nil {
		_ = dst.Close()
		return err
	}
	// Either side finishing tears down BOTH conns, so no copy goroutine
	// outlives its tunnel.
	var once sync.Once
	teardown := func() { _ = conn.Close(); _ = dst.Close() }
	go func() { defer once.Do(teardown); _, _ = io.Copy(dst, conn) }()
	go func() { defer once.Do(teardown); _, _ = io.Copy(conn, dst) }()
	return nil
}

func socksWriteAll(conn net.Conn, b []byte) error {
	for len(b) > 0 {
		n, err := conn.Write(b)
		if err != nil {
			return err
		}
		b = b[n:]
	}
	return nil
}

// rejectingProxy is an HTTP egress that answers every request with a fixed
// status and never forwards — a member that is reachable and refusing. Its
// counter is the pre-send gate's proof: an ineligible request must never be
// dialed at all, let alone retried on it.
type rejectingProxy struct {
	srv  *httptest.Server
	mu   sync.Mutex
	hits int
}

func newRejectingProxy(t *testing.T, code int) *rejectingProxy {
	t.Helper()
	rp := &rejectingProxy{}
	rp.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		rp.mu.Lock()
		rp.hits++
		rp.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(code)
		_, _ = io.WriteString(w, `{"relay":"refusing"}`)
	}))
	t.Cleanup(rp.srv.Close)
	return rp
}

func (rp *rejectingProxy) count() int {
	rp.mu.Lock()
	defer rp.mu.Unlock()
	return rp.hits
}

// ---- runtime-YAML rendering ----

// egressYAML renders a runtime file for the pool suite: a transports table
// (one proxy entry per name→URL, then bare direct entries), one pool entry
// per poolBodies key (the value is the entry's pre-rendered body, its lines
// indented four spaces), and for every pool a provider and model named
// <pool>-provider / <pool>-model over the shared upstream. bystanderUp is
// the upstream-model of an unrelated direct endpoint model — the field a
// reload rewrite touches to move the content hash without touching the pool.
func egressYAML(proxies map[string]string, directs []string, poolBodies map[string]string, upstream, bystanderUp string) string {
	poolNames := make([]string, 0, len(poolBodies))
	for n := range poolBodies {
		poolNames = append(poolNames, n)
	}
	sort.Strings(poolNames)

	var sb strings.Builder
	fmt.Fprintf(&sb, "api-key: %s\n", e2eAPIKey)
	if len(proxies)+len(directs)+len(poolBodies) > 0 {
		sb.WriteString("transports:\n")
		proxyNames := make([]string, 0, len(proxies))
		for n := range proxies {
			proxyNames = append(proxyNames, n)
		}
		sort.Strings(proxyNames)
		for _, n := range proxyNames {
			fmt.Fprintf(&sb, "  %s:\n    type: proxy\n    proxy: %s\n", n, proxies[n])
		}
		for _, n := range directs {
			fmt.Fprintf(&sb, "  %s:\n    type: direct\n", n)
		}
		for _, n := range poolNames {
			fmt.Fprintf(&sb, "  %s:\n    type: pool\n%s", n, poolBodies[n])
		}
	}
	sb.WriteString("providers:\n")
	for _, n := range poolNames {
		fmt.Fprintf(&sb, "  %s-provider:\n    base-url: %s/v1\n    transport: %s\n", n, upstream, n)
	}
	sb.WriteString("models:\n")
	for _, n := range poolNames {
		fmt.Fprintf(&sb, "  %s-model:\n    provider: %s-provider\n    upstream-model: %s\n", n, n, egressUpstreamModel)
	}
	fmt.Fprintf(&sb, "  bystander-model:\n    endpoint: %s/v1\n    upstream-model: %s\n", upstream, bystanderUp)
	return sb.String()
}

// poolMembersLine renders the members key in list form — bare member
// references, every per-member default in force.
func poolMembersLine(refs ...string) string {
	return "    members: [" + strings.Join(refs, ", ") + "]\n"
}

// poolMemberMapping renders one mapping-form member: the transport reference
// plus pre-indented per-member gate lines ("" for a bare mapping member).
func poolMemberMapping(ref, gates string) string {
	return "      - transport: " + ref + "\n" + gates
}

// poolChatBody renders a chat request for a pooled model.
func poolChatBody(model, content string) string {
	return fmt.Sprintf(`{"model":%q,"messages":[{"role":"user","content":%q}]}`, model, content)
}

// proxyTarget renders the log-safe egress_target form of a proxy URL:
// scheme://host, never userinfo.
func proxyTarget(t *testing.T, raw string) string {
	t.Helper()
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("parse proxy URL: %v", err)
	}
	return u.Scheme + "://" + u.Host
}

// ---- scheduling ----

// TestEgressPoolRoundRobinAcrossProxyAndSocks pins dual-egress traversal:
// two sequential requests over a pool of one HTTP forward proxy and one
// SOCKS5 endpoint hit each member exactly once, in declaration order, and
// the pooled path still renames the model. The quiet direction is traffic
// silently bypassing a configured egress or both requests landing on the
// first member.
func TestEgressPoolRoundRobinAcrossProxyAndSocks(t *testing.T) {
	up := newFakeUpstream(t)
	up.setHandler(egressChatOK)
	fp := newForwardingProxy(t)
	sx := newSocks5Egress(t)

	relayURL := fp.srv.URL
	socksURL := "socks5://" + sx.addr()
	body := egressYAML(
		map[string]string{"relay": relayURL, "socks": socksURL},
		nil,
		map[string]string{"pool-egress": poolMembersLine("relay", "socks")},
		up.url(), "bystander-up")
	p := startSubprocess(t, startOpts{yaml: body, logLevel: "info"})

	for i := 0; i < 2; i++ {
		code, _, respBody := postJSON(t, p.addr, "/v1/chat/completions",
			poolChatBody("pool-egress-model", "hi"), nil)
		if code != http.StatusOK {
			t.Fatalf("request %d: status = %d, body %s", i, code, respBody)
		}
		if m := decodeMap(t, respBody); m["model"] != "pool-egress-model" {
			t.Errorf("request %d: client-facing model = %v, want pool-egress-model", i, m["model"])
		}
	}
	if fp.count() != 1 || sx.count() != 1 {
		t.Fatalf("member hits = http %d / socks %d, want exactly 1/1", fp.count(), sx.count())
	}
	if up.count() != 2 {
		t.Fatalf("upstream requests = %d, want 2", up.count())
	}
	reqs := up.requests()
	if reqs[0].Headers.Get("X-Via-Proxy") != "1" {
		t.Error("request 1 did not traverse the HTTP forward proxy")
	}
	if reqs[1].Headers.Get("X-Via-Proxy") != "" {
		t.Error("request 2 traversed the HTTP proxy — the rotation did not advance")
	}
	if !strings.Contains(string(reqs[0].Body), `"model":"`+egressUpstreamModel+`"`) {
		t.Errorf("upstream body = %s, want the renamed upstream model", reqs[0].Body)
	}

	evs := waitForEventCount(t, p, "request_completed", 2)
	want := []string{proxyTarget(t, relayURL), proxyTarget(t, socksURL)}
	for i, ev := range evs {
		if ev["egress_attempts"] != float64(1) || ev["egress_exhausted"] != false {
			t.Errorf("request %d: attempts/exhausted = %v/%v, want 1/false", i, ev["egress_attempts"], ev["egress_exhausted"])
		}
		if ev["egress_target"] != want[i] {
			t.Errorf("request %d: egress_target = %v, want %v (round-robin order)", i, ev["egress_target"], want[i])
		}
		if kind := strings.SplitN(want[i], ":", 2)[0]; ev["egress_kind"] != kind {
			t.Errorf("request %d: egress_kind = %v, want %q", i, ev["egress_kind"], kind)
		}
	}
}

// ---- eligibility gates ----

// TestEgressPoolBodySizeGateRoutesBeforeSend pins the pre-send gate: a large
// request never touches the small member at all (no dial, no 413, no retry)
// and is served by the member that can carry it, while a small request CAN
// reach the capped member — and the refusal it gets back is an answer,
// relayed as the canonical envelope, not a fallback trigger.
func TestEgressPoolBodySizeGateRoutesBeforeSend(t *testing.T) {
	up := newFakeUpstream(t)
	up.setHandler(egressChatOK)
	narrow := newRejectingProxy(t, http.StatusRequestEntityTooLarge)

	poolBody := "    members:\n" +
		poolMemberMapping("narrow", "        max-body-bytes: 1000\n") +
		"      - wide\n"
	body := egressYAML(
		map[string]string{"narrow": narrow.srv.URL},
		[]string{"wide"},
		map[string]string{"pool-egress": poolBody},
		up.url(), "bystander-up")
	p := startSubprocess(t, startOpts{yaml: body, logLevel: "info"})

	big := fmt.Sprintf(`{"model":"pool-egress-model","messages":[{"role":"user","content":"%s"}]}`,
		strings.Repeat("x", 4000))
	code, _, respBody := postJSON(t, p.addr, "/v1/chat/completions", big, nil)
	if code != http.StatusOK {
		t.Fatalf("large request: status = %d, body %s", code, respBody)
	}
	if narrow.count() != 0 {
		t.Fatalf("oversized request dialed the capped member %d times — the gate is not pre-send", narrow.count())
	}
	if up.count() != 1 {
		t.Fatalf("large request upstream count = %d, want 1 (served by the uncapped member)", up.count())
	}

	small := poolChatBody("pool-egress-model", "hi")
	code, _, respBody = postJSON(t, p.addr, "/v1/chat/completions", small, nil)
	if code != http.StatusRequestEntityTooLarge {
		t.Fatalf("small request: status = %d, body %s", code, respBody)
	}
	if !strings.Contains(string(respBody), `"message":"upstream provider returned HTTP 413"`) ||
		!strings.Contains(string(respBody), `"code":"upstream_http_413"`) {
		t.Errorf("413 body = %s, want the canonical envelope", respBody)
	}
	if narrow.count() != 1 {
		t.Errorf("small request reached the capped member %d times, want 1", narrow.count())
	}
	if up.count() != 1 {
		t.Errorf("the refused request leaked to the upstream behind the refusing member")
	}

	evs := waitForEventCount(t, p, "request_completed", 2)
	for i, ev := range evs {
		if ev["egress_attempts"] != float64(1) {
			t.Errorf("request %d: egress_attempts = %v, want 1 (a refusal is not retried)", i, ev["egress_attempts"])
		}
		if ev["egress_exhausted"] != false {
			t.Errorf("request %d: egress_exhausted = %v", i, ev["egress_exhausted"])
		}
	}
}

// ---- fallback ----

// TestEgressPoolFallsBackWhenFirstMemberIsDead pins the core fallback: the
// first member's pre-response transport failure hands the request to the
// next member, the answer comes back through it, and the access log reports
// two distinct dials with the serving member as the last egress.
func TestEgressPoolFallsBackWhenFirstMemberIsDead(t *testing.T) {
	up := newFakeUpstream(t)
	up.setHandler(egressChatOK)
	fp := newForwardingProxy(t)

	body := egressYAML(
		map[string]string{"relay": fp.srv.URL, "dead-relay": "http://127.0.0.1:1"},
		nil,
		map[string]string{"pool-egress": poolMembersLine("dead-relay", "relay")},
		up.url(), "bystander-up")
	p := startSubprocess(t, startOpts{yaml: body, logLevel: "info"})

	code, _, respBody := postJSON(t, p.addr, "/v1/chat/completions",
		poolChatBody("pool-egress-model", "hi"), nil)
	if code != http.StatusOK {
		t.Fatalf("status = %d, body %s — the dead first member must fall back", code, respBody)
	}
	if fp.count() != 1 {
		t.Fatalf("live member hits = %d, want 1", fp.count())
	}
	req, ok := up.last()
	if !ok || req.Headers.Get("X-Via-Proxy") != "1" {
		t.Error("the fallback request did not traverse the live member's proxy")
	}

	ev := waitForEventCount(t, p, "request_completed", 1)[0]
	if ev["outcome"] != "completed" {
		t.Errorf("outcome = %v, want completed", ev["outcome"])
	}
	if ev["egress_attempts"] != float64(2) {
		t.Errorf("egress_attempts = %v, want 2", ev["egress_attempts"])
	}
	if ev["egress_exhausted"] != false {
		t.Errorf("egress_exhausted = %v, want false", ev["egress_exhausted"])
	}
	if ev["egress_kind"] != "http" || ev["egress_target"] != proxyTarget(t, fp.srv.URL) {
		t.Errorf("egress kind/target = %v/%v, want the serving member", ev["egress_kind"], ev["egress_target"])
	}
}

// TestEgressPoolMaxAttemptsCapsDistinctDials pins the fallback bound: two
// dead members with fallback max-attempts 2 produce exactly two dials — the
// third member is never touched — and the client gets the canonical 502
// upstream_unreachable envelope.
func TestEgressPoolMaxAttemptsCapsDistinctDials(t *testing.T) {
	up := newFakeUpstream(t)
	up.setHandler(egressChatOK)
	d1, d2 := newDeadEgress(t), newDeadEgress(t)
	fp := newForwardingProxy(t) // the member the cap must protect

	poolBody := poolMembersLine("down-a", "down-b", "relay") +
		"    fallback:\n      max-attempts: 2\n"
	body := egressYAML(
		map[string]string{"down-a": d1.proxyURL(), "down-b": d2.proxyURL(), "relay": fp.srv.URL},
		nil,
		map[string]string{"pool-egress": poolBody},
		up.url(), "bystander-up")
	p := startSubprocess(t, startOpts{yaml: body, logLevel: "info"})

	code, hdrs, respBody := postJSON(t, p.addr, "/v1/chat/completions",
		poolChatBody("pool-egress-model", "hi"), nil)
	if code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502", code)
	}
	if ct := hdrs.Get("Content-Type"); !strings.Contains(ct, "application/json") {
		t.Errorf("content type = %q, want application/json", ct)
	}
	if got := strings.TrimSpace(string(respBody)); got != envelopeUpstreamUnreachable {
		t.Errorf("body = %q, want the canonical upstream_unreachable envelope", got)
	}
	if d1.count() != 1 || d2.count() != 1 {
		t.Errorf("dead member dials = %d/%d, want exactly 1/1", d1.count(), d2.count())
	}
	if fp.count() != 0 {
		t.Errorf("max-attempts breached: the third member was dialed %d times", fp.count())
	}
	if up.count() != 0 {
		t.Errorf("the failed request reached the upstream %d times", up.count())
	}

	ev := waitForEventCount(t, p, "request_completed", 1)[0]
	if ev["outcome"] != "upstream_unreachable" {
		t.Errorf("outcome = %v, want upstream_unreachable", ev["outcome"])
	}
	if ev["egress_attempts"] != float64(2) {
		t.Errorf("egress_attempts = %v, want 2", ev["egress_attempts"])
	}
	if ev["egress_exhausted"] != false {
		t.Errorf("egress_exhausted = %v, want false (two real dials happened)", ev["egress_exhausted"])
	}
}

// ---- statuses are answers ----

// TestEgressPoolUpstreamStatusIsAnAnswer pins the load-bearing seam behind a
// real proxy hop: a 429 from the first member is returned as the client's
// answer — status preserved, canonical envelope body, relay headers intact —
// with no second dial anywhere.
func TestEgressPoolUpstreamStatusIsAnAnswer(t *testing.T) {
	up := newFakeUpstream(t)
	up.setHandler(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Retry-After", "7")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = io.WriteString(w, `{"error":{"message":"provider rate limit","param":null,"code":"rate_limit_exceeded"}}`)
	})
	fpA := newForwardingProxy(t)
	fpB := newForwardingProxy(t)

	body := egressYAML(
		map[string]string{"relay-a": fpA.srv.URL, "relay-b": fpB.srv.URL},
		nil,
		map[string]string{"pool-egress": poolMembersLine("relay-a", "relay-b")},
		up.url(), "bystander-up")
	// The provider retry layer is off for this model: the dial-count
	// assertions below pin the POOL seam — one Execute, one member, and a
	// status never moves a request between members INSIDE that Execute.
	// (With the default policy the handler would legitimately re-execute,
	// a second pool dial included.)
	body = strings.Replace(body,
		"  pool-egress-model:\n",
		"  pool-egress-model:\n    retries:\n      max-retries: 0\n", 1)
	p := startSubprocess(t, startOpts{yaml: body, logLevel: "info"})

	code, hdrs, respBody := postJSON(t, p.addr, "/v1/chat/completions",
		poolChatBody("pool-egress-model", "hi"), nil)
	if code != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want the upstream's own 429", code)
	}
	if !strings.Contains(string(respBody), `"message":"upstream provider returned HTTP 429"`) ||
		!strings.Contains(string(respBody), `"code":"upstream_http_429"`) {
		t.Errorf("body = %s, want the canonical envelope", respBody)
	}
	if hdrs.Get("Retry-After") != "7" {
		t.Errorf("Retry-After = %q, want the relayed upstream header", hdrs.Get("Retry-After"))
	}
	if fpA.count() != 1 || fpB.count() != 0 {
		t.Errorf("member hits = %d/%d, want 1/0 — a status must never fall back inside the pool", fpA.count(), fpB.count())
	}
	if up.count() != 1 {
		t.Errorf("upstream requests = %d, want 1", up.count())
	}

	ev := waitForEventCount(t, p, "request_completed", 1)[0]
	if ev["egress_attempts"] != float64(1) {
		t.Errorf("egress_attempts = %v, want 1", ev["egress_attempts"])
	}
	if ev["egress_exhausted"] != false {
		t.Errorf("egress_exhausted = %v, want false", ev["egress_exhausted"])
	}
}

// ---- streaming gate ----

// TestEgressPoolStreamingSkipsNonStreamingMember pins the stream gate: a
// stream request skips the streaming:false member entirely and streams live
// through the other one, while a plain request may still use the skipped
// member — the gate is eligibility, not failure.
func TestEgressPoolStreamingSkipsNonStreamingMember(t *testing.T) {
	up := newFakeUpstream(t)
	up.setHandler(func(w http.ResponseWriter, r *http.Request) {
		// fakeUpstream consumes the body before delegating, so the two
		// request shapes in this test branch on the forwarded Accept header.
		if r.Header.Get("Accept") != "text/event-stream" {
			// The plain request shares the upstream; it must get a parseable
			// JSON answer, or the buffered path would 502 before any egress
			// assertion could run.
			egressChatOK(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		f := w.(http.Flusher)
		for i := 1; i <= 3; i++ {
			_, _ = fmt.Fprintf(w, "data: {\"i\":%d}\n\n", i)
			f.Flush()
			time.Sleep(60 * time.Millisecond)
		}
		_, _ = io.WriteString(w, "data: [DONE]\n\n")
		f.Flush()
	})
	noStream := newForwardingProxy(t)
	streamy := newForwardingProxy(t)

	poolBody := "    members:\n" +
		poolMemberMapping("nostream-relay", "        streaming: false\n") +
		"      - streamy-relay\n"
	body := egressYAML(
		map[string]string{"nostream-relay": noStream.srv.URL, "streamy-relay": streamy.srv.URL},
		nil,
		map[string]string{"pool-egress": poolBody},
		up.url(), "bystander-up")
	p := startSubprocess(t, startOpts{yaml: body, logLevel: "info"})

	resp := openJSON(t, p.addr, "/v1/chat/completions",
		`{"model":"pool-egress-model","stream":true,"messages":[{"role":"user","content":"hi"}]}`,
		map[string]string{"Accept": "text/event-stream"})
	if ct := resp.Header.Get("Content-Type"); !strings.Contains(ct, "text/event-stream") {
		t.Fatalf("content type = %q", ct)
	}
	br := bufio.NewReader(resp.Body)
	events, seenDone := 0, false
	deadline := time.Now().Add(10 * time.Second)
	for !seenDone && time.Now().Before(deadline) {
		lines, eof := nextSSEEvent(t, br, 5*time.Second)
		for _, l := range lines {
			if strings.HasPrefix(l, "data:") {
				if strings.TrimSpace(strings.TrimPrefix(l, "data:")) == "[DONE]" {
					seenDone = true
					continue
				}
				events++
			}
		}
		if eof && !seenDone {
			t.Fatal("stream ended before [DONE]")
		}
	}
	_ = resp.Body.Close()
	if !seenDone {
		t.Fatal("no [DONE] within the deadline")
	}
	if events != 3 {
		t.Fatalf("received %d data events, want 3", events)
	}
	if noStream.count() != 0 {
		t.Errorf("stream request dialed the streaming:false member %d times", noStream.count())
	}
	if streamy.count() != 1 {
		t.Errorf("stream request through the streaming member = %d, want 1", streamy.count())
	}

	code, _, respBody := postJSON(t, p.addr, "/v1/chat/completions",
		poolChatBody("pool-egress-model", "plain"), nil)
	if code != http.StatusOK {
		t.Fatalf("plain request: status = %d, body %s", code, respBody)
	}
	if noStream.count() != 1 {
		t.Errorf("plain request skipped the non-streaming member (%d hits)", noStream.count())
	}
	if streamy.count() != 1 {
		t.Errorf("streamy member hits = %d, want 1 (rotation, not fallback)", streamy.count())
	}
}

// ---- passive health ----

// TestEgressPoolHealthCooldownSkipsAndRecovers pins the passive health
// lifecycle over real dials: two consecutive transport failures open a
// 1s cooldown, during which the dead member is skipped WITHOUT a dial and
// the live one serves alone; after the cooldown the member is scheduled —
// and dialed — again. Per-request attempt counts come from the
// request_completed log fields.
func TestEgressPoolHealthCooldownSkipsAndRecovers(t *testing.T) {
	up := newFakeUpstream(t)
	up.setHandler(egressChatOK)
	dead := newDeadEgress(t)
	live := newForwardingProxy(t)

	poolBody := poolMembersLine("dead-relay", "live-relay") +
		"    fallback:\n      max-attempts: 2\n" +
		"    health:\n      failure-threshold: 2\n      cooldown: 1s\n"
	body := egressYAML(
		map[string]string{"dead-relay": dead.proxyURL(), "live-relay": live.srv.URL},
		nil,
		map[string]string{"pool-egress": poolBody},
		up.url(), "bystander-up")
	p := startSubprocess(t, startOpts{yaml: body, logLevel: "info"})

	// R1 and R2 land on the dead member on their alternating turns; the
	// second failure trips the cooldown. Every request must still be served
	// by the live member.
	wantAttempts := []float64{2, 1, 2, 1, 1}
	for i := 0; i < 5; i++ {
		code, _, respBody := postJSON(t, p.addr, "/v1/chat/completions",
			poolChatBody("pool-egress-model", "hi"), nil)
		if code != http.StatusOK {
			t.Fatalf("request %d: status = %d, body %s", i, code, respBody)
		}
		evs := waitForEventCount(t, p, "request_completed", i+1)
		if got := evs[i]["egress_attempts"]; got != wantAttempts[i] {
			t.Errorf("request %d: egress_attempts = %v, want %v", i, got, wantAttempts[i])
		}
	}
	if dead.count() != 2 {
		t.Fatalf("dead member dials = %d, want exactly 2 (threshold 2)", dead.count())
	}
	if live.count() != 5 {
		t.Errorf("live member requests = %d, want 5 (it served every fallback)", live.count())
	}

	// Skip phase: the cooling member is not dialed, and a skip consumes no
	// attempt.
	code, _, respBody := postJSON(t, p.addr, "/v1/chat/completions",
		poolChatBody("pool-egress-model", "hi"), nil)
	if code != http.StatusOK {
		t.Fatalf("skip-phase request: status = %d, body %s", code, respBody)
	}
	if dead.count() != 2 {
		t.Errorf("cooling member was dialed again (%d dials)", dead.count())
	}
	evs := waitForEventCount(t, p, "request_completed", 6)
	if evs[5]["egress_attempts"] != float64(1) {
		t.Errorf("skip-phase egress_attempts = %v, want 1 (a skip consumes no attempt)", evs[5]["egress_attempts"])
	}

	// Recovery: after the cooldown window the member is scheduled — and
	// dialed — again. The live member keeps the request succeeding.
	time.Sleep(1300 * time.Millisecond)
	recoverDeadline := time.Now().Add(3 * time.Second)
	for dead.count() < 3 {
		if time.Now().After(recoverDeadline) {
			t.Fatalf("dead member was never re-scheduled after its cooldown (dials = %d)", dead.count())
		}
		code, _, respBody := postJSON(t, p.addr, "/v1/chat/completions",
			poolChatBody("pool-egress-model", "hi"), nil)
		if code != http.StatusOK {
			t.Fatalf("post-cooldown request: status = %d, body %s", code, respBody)
		}
	}
	if live.count() < 6 {
		t.Errorf("live member requests = %d, want at least 6", live.count())
	}
}

// ---- cancellation ----

// TestEgressPoolClientCancelAbortsCleanly pins the canceled class black-box:
// a client that goes away while the first member holds the request gets no
// envelope and no fallback — the spare member is never dialed, the access
// log records the client's own event with the canceled class — and the
// aborted attempt is not held against the member's health: the rotation
// comes back to it and it serves again.
func TestEgressPoolClientCancelAbortsCleanly(t *testing.T) {
	up := newFakeUpstream(t)
	var (
		mu   sync.Mutex
		hold bool
	)
	arrived := make(chan struct{})
	release := make(chan struct{})
	up.setHandler(func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		blocked := hold
		mu.Unlock()
		if !blocked {
			egressChatOK(w, nil)
			return
		}
		close(arrived) // exactly one blocked request in this test
		<-release
		egressChatOK(w, nil)
	})
	fpSlow := newForwardingProxy(t)
	fpSpare := newForwardingProxy(t)

	poolBody := poolMembersLine("slow-relay", "spare-relay") +
		"    health:\n      failure-threshold: 1\n      cooldown: 30s\n"
	body := egressYAML(
		map[string]string{"slow-relay": fpSlow.srv.URL, "spare-relay": fpSpare.srv.URL},
		nil,
		map[string]string{"pool-egress": poolBody},
		up.url(), "bystander-up")
	p := startSubprocess(t, startOpts{yaml: body, logLevel: "info"})

	// Fire the request, wait until the upstream holds it, then cancel.
	mu.Lock()
	hold = true
	mu.Unlock()
	// The held upstream answers only when this test ENDS. Releasing it
	// earlier races the assertions below: the injector would receive the 200
	// and complete a request the client had already canceled — the write to a
	// half-closed client socket succeeds, so nothing there marks the
	// disconnect — and the test would read `outcome: completed` for a cancel
	// it did observe client-side. Parked at the end, the only way the request
	// can settle while those assertions read the log is the disconnect they
	// mean to pin. (Reproduced under CPU load before this: issue #60.)
	defer close(release)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	type outcome struct {
		err error
	}
	done := make(chan outcome, 1)
	go func() {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost,
			"http://"+p.addr+"/v1/chat/completions",
			strings.NewReader(poolChatBody("pool-egress-model", "hi")))
		if err != nil {
			done <- outcome{err: err}
			return
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+e2eAPIKey)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			done <- outcome{err: err}
			return
		}
		_ = resp.Body.Close()
		done <- outcome{err: fmt.Errorf("unexpected HTTP %d response", resp.StatusCode)}
	}()
	select {
	case <-arrived:
	case <-time.After(5 * time.Second):
		t.Fatal("the request never reached the upstream through the first member")
	}
	cancel()
	select {
	case o := <-done:
		if o.err == nil {
			t.Fatal("the canceled request returned a response instead of aborting")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the canceled request never returned to the client")
	}

	// Stop holding NEW requests so the rotation checks below answer fast (the
	// one already blocked inside the upstream stays blocked until the defer
	// above releases it), then pin the abort's shape: no fallback dial, the
	// disconnect outcome, and the canceled class with its caller_canceled
	// cause on the WARN.
	mu.Lock()
	hold = false
	mu.Unlock()

	if fpSpare.count() != 0 {
		t.Errorf("spare member dialed %d times after cancellation — a cancel must not fall back", fpSpare.count())
	}
	evs := waitForEventCount(t, p, "request_completed", 1)
	if evs[0]["outcome"] != "client_disconnected" {
		t.Errorf("outcome = %v, want client_disconnected", evs[0]["outcome"])
	}
	if evs[0]["egress_attempts"] != float64(1) {
		t.Errorf("egress_attempts = %v, want 1 (the one real dial)", evs[0]["egress_attempts"])
	}
	failed := eventsWithMessage(parseLogEvents(t, p.stderr.String()), "upstream_request_failed")
	if len(failed) != 1 {
		t.Fatalf("upstream_request_failed events = %d, want 1", len(failed))
	}
	if failed[0]["error_class"] != "canceled" {
		t.Errorf("error_class = %v, want canceled", failed[0]["error_class"])
	}
	if failed[0]["error_cause"] != "caller_canceled" {
		t.Errorf("error_cause = %v, want caller_canceled", failed[0]["error_cause"])
	}

	// No strike: with threshold 1 a counted failure would cool the first
	// member for 30s and the rotation would land on the spare twice. It must
	// come back to the first member instead.
	for i := 0; i < 2; i++ {
		code, _, respBody := postJSON(t, p.addr, "/v1/chat/completions",
			poolChatBody("pool-egress-model", "hi"), nil)
		if code != http.StatusOK {
			t.Fatalf("post-cancel request %d: status = %d, body %s", i, code, respBody)
		}
	}
	if fpSlow.count() != 2 || fpSpare.count() != 1 {
		t.Errorf("post-cancel hits = slow %d / spare %d, want 2/1 — the canceled attempt counted against health",
			fpSlow.count(), fpSpare.count())
	}
}

// ---- reload invariants ----

// TestEgressPoolReloadKeepsWarmStateThenSurvivesRemoval pins the reload
// halves black-box: an unchanged pool keeps its scheduler position across a
// reload (the rotation continues rather than restarting), a changed policy
// starts fresh but keeps serving, and a removed pool leaves the process
// healthy and serving through the new snapshot.
func TestEgressPoolReloadKeepsWarmStateThenSurvivesRemoval(t *testing.T) {
	up := newFakeUpstream(t)
	up.setHandler(egressChatOK)
	fpA := newForwardingProxy(t)
	fpB := newForwardingProxy(t)

	proxies := map[string]string{"relay-a": fpA.srv.URL, "relay-b": fpB.srv.URL}
	pools := map[string]string{"pool-egress": poolMembersLine("relay-a", "relay-b")}
	valid := func(bystander string) string {
		return egressYAML(proxies, nil, pools, up.url(), bystander)
	}
	p := startSubprocess(t, startOpts{yaml: valid("bystander-up"), logLevel: "info"})

	post := func() int {
		code, _, respBody := postJSON(t, p.addr, "/v1/chat/completions",
			poolChatBody("pool-egress-model", "hi"), nil)
		if code != http.StatusOK {
			t.Fatalf("status = %d, body %s", code, respBody)
		}
		return code
	}

	if code := post(); code != http.StatusOK {
		t.Fatal("first request failed")
	}
	if fpA.count() != 1 || fpB.count() != 0 {
		t.Fatalf("hits after request 1 = %d/%d, want 1/0", fpA.count(), fpB.count())
	}

	// Unchanged pool, touched bystander field: the reload must NOT reset the
	// rotation — request 2 goes to the second member.
	rewriteConfig(t, p.cfgPath, valid("bystander-up-v2"))
	waitForEventCount(t, p, "config_reloaded", 1)
	if code := post(); code != http.StatusOK {
		t.Fatal("post-reload request failed")
	}
	if fpB.count() != 1 || fpA.count() != 1 {
		t.Errorf("hits after the unchanged reload = %d/%d, want 1/1 — the scheduler position was lost",
			fpA.count(), fpB.count())
	}

	// Changed policy: a fresh pool state, but serving is undisturbed.
	changedPools := map[string]string{
		"pool-egress": poolMembersLine("relay-a", "relay-b") + "    fallback:\n      max-attempts: 2\n",
	}
	rewriteConfig(t, p.cfgPath, egressYAML(proxies, nil, changedPools, up.url(), "bystander-up-v3"))
	waitForEventCount(t, p, "config_reloaded", 2)
	if code := post(); code != http.StatusOK {
		t.Fatal("post-policy-change request failed")
	}

	// Removed pool: the model rewires to a direct endpoint and the process
	// stays healthy.
	removed := fmt.Sprintf("api-key: %s\nmodels:\n  pool-egress-model:\n    endpoint: %s/v1\n    upstream-model: %s\n",
		e2eAPIKey, up.url(), egressUpstreamModel)
	rewriteConfig(t, p.cfgPath, removed)
	waitForEventCount(t, p, "config_reloaded", 3)
	if code := post(); code != http.StatusOK {
		t.Fatal("request after pool removal failed")
	}
	p.waitHealth(t, 2*time.Second)
	if up.count() != 4 {
		t.Errorf("upstream requests = %d, want 4 (one per request, before and after every reload)", up.count())
	}
}

// ---- exhaustion ----

// TestEgressPoolAllMembersIneligibleIsExhausted pins zero dials: a stream
// request against a pool whose every member is streaming:false is answered
// with the canonical upstream_unreachable envelope, nothing is dialed, and
// both log events carry the exhaustion report.
func TestEgressPoolAllMembersIneligibleIsExhausted(t *testing.T) {
	up := newFakeUpstream(t)
	up.setHandler(egressChatOK)
	fpA := newForwardingProxy(t)
	fpB := newForwardingProxy(t)

	poolBody := "    members:\n" +
		poolMemberMapping("relay-a", "        streaming: false\n") +
		poolMemberMapping("relay-b", "        streaming: false\n")
	body := egressYAML(
		map[string]string{"relay-a": fpA.srv.URL, "relay-b": fpB.srv.URL},
		nil,
		map[string]string{"pool-egress": poolBody},
		up.url(), "bystander-up")
	p := startSubprocess(t, startOpts{yaml: body, logLevel: "info"})

	code, _, respBody := postJSON(t, p.addr, "/v1/chat/completions",
		`{"model":"pool-egress-model","stream":true,"messages":[{"role":"user","content":"hi"}]}`, nil)
	if code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502", code)
	}
	if got := strings.TrimSpace(string(respBody)); got != envelopeUpstreamUnreachable {
		t.Errorf("body = %q, want the canonical upstream_unreachable envelope", got)
	}
	if fpA.count() != 0 || fpB.count() != 0 {
		t.Errorf("ineligible request dialed members: %d/%d", fpA.count(), fpB.count())
	}
	if up.count() != 0 {
		t.Errorf("upstream requests = %d, want 0", up.count())
	}

	ev := waitForEventCount(t, p, "request_completed", 1)[0]
	if ev["outcome"] != "upstream_unreachable" {
		t.Errorf("outcome = %v, want upstream_unreachable", ev["outcome"])
	}
	if ev["egress_attempts"] != float64(0) {
		t.Errorf("egress_attempts = %v, want 0", ev["egress_attempts"])
	}
	if ev["egress_exhausted"] != true {
		t.Errorf("egress_exhausted = %v, want true", ev["egress_exhausted"])
	}
	failed := eventsWithMessage(parseLogEvents(t, p.stderr.String()), "upstream_request_failed")
	if len(failed) != 1 {
		t.Fatalf("upstream_request_failed events = %d, want 1", len(failed))
	}
	if failed[0]["error_class"] != "egress_exhausted" {
		t.Errorf("error_class = %v, want egress_exhausted", failed[0]["error_class"])
	}
}

// ---- credential hygiene ----

// The planted proxy credentials for the log sweep. Distinct markers, so a
// single substring check covers username and password per hop.
const (
	egressSecretHTTPUser = "s3cr3t-relay-user"
	egressSecretHTTPPass = "s3cr3t-relay-pass"
	egressSecretSocksUsr = "s3cr3t-socks-user"
	egressSecretSocksPas = "s3cr3t-socks-pass"
)

// TestEgressLogsNeverCarryProxyCredentials sweeps the whole stderr of a run
// that exercises every pool path — success through each egress kind,
// fallback off a dead member, and exhaustion — while the configured proxy
// URLs carry user:pass userinfo. No line may contain a credential in any
// form (raw, or the Basic header's base64 of user:pass), and every
// egress_target must be scheme://host only.
func TestEgressLogsNeverCarryProxyCredentials(t *testing.T) {
	up := newFakeUpstream(t)
	up.setHandler(egressChatOK)
	live := newForwardingProxy(t)
	dead := newDeadEgress(t)
	sx := newSocks5Egress(t)

	httpAuth := egressSecretHTTPUser + ":" + egressSecretHTTPPass + "@" + strings.TrimPrefix(live.srv.URL, "http://")
	socksAuth := egressSecretSocksUsr + ":" + egressSecretSocksPas + "@" + sx.addr()
	deadAuth := egressSecretHTTPUser + ":" + egressSecretHTTPPass + "@" + dead.addr()
	relayURL := "http://" + httpAuth
	socksURL := "socks5://" + socksAuth
	deadURL := "socks5://" + deadAuth

	poolBodies := map[string]string{
		"ok-pool":       poolMembersLine("auth-relay", "auth-socks"),
		"fallback-pool": poolMembersLine("auth-dead", "auth-relay"),
		"gated-pool": "    members:\n" +
			poolMemberMapping("auth-relay", "        streaming: false\n"),
	}
	body := egressYAML(
		map[string]string{"auth-relay": relayURL, "auth-socks": socksURL, "auth-dead": deadURL},
		nil,
		poolBodies,
		up.url(), "bystander-up")
	p := startSubprocess(t, startOpts{yaml: body, logLevel: "info"})

	// Success through each egress kind.
	for i := 0; i < 2; i++ {
		code, _, respBody := postJSON(t, p.addr, "/v1/chat/completions",
			poolChatBody("ok-pool-model", "hi"), nil)
		if code != http.StatusOK {
			t.Fatalf("ok-pool request %d: status = %d, body %s", i, code, respBody)
		}
	}
	// Fallback off a dead (credentialed) member onto a live one.
	code, _, respBody := postJSON(t, p.addr, "/v1/chat/completions",
		poolChatBody("fallback-pool-model", "hi"), nil)
	if code != http.StatusOK {
		t.Fatalf("fallback-pool request: status = %d, body %s", code, respBody)
	}
	// Exhaustion: zero dials.
	code, _, respBody = postJSON(t, p.addr, "/v1/chat/completions",
		`{"model":"gated-pool-model","stream":true,"messages":[{"role":"user","content":"hi"}]}`, nil)
	if code != http.StatusBadGateway {
		t.Fatalf("gated-pool request: status = %d, body %s", code, respBody)
	}

	evs := waitForEventCount(t, p, "request_completed", 4)
	allowed := map[string]bool{
		proxyTarget(t, relayURL): true,
		proxyTarget(t, socksURL): true,
		proxyTarget(t, deadURL):  true,
	}
	for i, ev := range evs {
		target, _ := ev["egress_target"].(string)
		if strings.Contains(target, "@") {
			t.Errorf("event %d: egress_target %q carries userinfo", i, target)
		}
		if ev["egress_attempts"] == float64(0) {
			// Zero dials (the exhausted request) names no endpoint at all.
			if target != "" {
				t.Errorf("event %d: egress_target = %q with zero attempts, want empty", i, target)
			}
			continue
		}
		if !allowed[target] {
			t.Errorf("event %d: egress_target = %q, want one of the configured scheme://host endpoints", i, target)
		}
	}

	stderr := p.stderr.String()
	secrets := []string{
		egressSecretHTTPUser, egressSecretHTTPPass,
		egressSecretSocksUsr, egressSecretSocksPas,
		"s3cr3t",
		base64.StdEncoding.EncodeToString([]byte(egressSecretHTTPUser + ":" + egressSecretHTTPPass)),
		base64.StdEncoding.EncodeToString([]byte(egressSecretSocksUsr + ":" + egressSecretSocksPas)),
	}
	for _, secret := range secrets {
		if strings.Contains(stderr, secret) {
			t.Errorf("stderr leaks %q across the pool paths:\n%s", secret, stderr)
		}
	}
}

// ---- config rejection ----

// TestEgressPoolUnknownMemberReferenceFailsBoot pins that a pool member
// reference that cannot resolve is a startup failure, never a silent direct
// fallback.
func TestEgressPoolUnknownMemberReferenceFailsBoot(t *testing.T) {
	body := egressYAML(
		map[string]string{"relay": "http://127.0.0.1:1"},
		nil,
		map[string]string{"pool-egress": poolMembersLine("ghost-egress")},
		"http://127.0.0.1:1", "bystander-up")
	code, stderr := startSubprocessExpectExit(t, startOpts{yaml: body})
	if code != 1 {
		t.Fatalf("exit code = %d, want 1 for invalid initial config", code)
	}
	if !strings.Contains(stderr, "config_load_failed") {
		t.Errorf("stderr = %s, want a config_load_failed event", stderr)
	}
}

// TestEgressPoolInvalidReloadKeepsLastKnownGood pins the reload half: a
// rewrite that breaks the pool table is rejected and the previous snapshot
// keeps serving — through the same pool, not a fallback.
func TestEgressPoolInvalidReloadKeepsLastKnownGood(t *testing.T) {
	up := newFakeUpstream(t)
	up.setHandler(egressChatOK)
	fp := newForwardingProxy(t)

	valid := egressYAML(
		map[string]string{"relay": fp.srv.URL},
		nil,
		map[string]string{"pool-egress": poolMembersLine("relay")},
		up.url(), "bystander-up")
	p := startSubprocess(t, startOpts{yaml: valid, logLevel: "info"})

	code, _, respBody := postJSON(t, p.addr, "/v1/chat/completions",
		poolChatBody("pool-egress-model", "hi"), nil)
	if code != http.StatusOK {
		t.Fatalf("pre-reload status = %d, body %s", code, respBody)
	}

	broken := egressYAML(
		map[string]string{"relay": fp.srv.URL},
		nil,
		map[string]string{"pool-egress": poolMembersLine("ghost-egress")},
		up.url(), "bystander-up")
	rewriteConfig(t, p.cfgPath, broken)
	waitForEventCount(t, p, "config_reload_rejected", 1)

	code, _, _ = postJSON(t, p.addr, "/v1/chat/completions",
		poolChatBody("pool-egress-model", "hi"), nil)
	if code != http.StatusOK {
		t.Fatalf("post-reload status = %d, want the last-known-good snapshot", code)
	}
	if fp.count() != 2 {
		t.Errorf("proxy hits = %d, want 2 (pre- and post-reload, same pool)", fp.count())
	}
}
