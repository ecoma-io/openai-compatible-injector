package proxy

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// TestRequestBodyReadIsBoundedInTime is issue #94's bound, on a real socket
// and a real handler: a client that sends its headers, a few body bytes, and
// then nothing must not be able to hold the handler open. Before the read
// deadline existed there was nothing on the path that could end this — no
// ReadTimeout on the server, no timer in the handler — so the only thing the
// test could do is wait forever.
//
// The client-side read deadline is the test's safety net, not the assertion:
// if the handler never returns, the read fails after five seconds and the
// connection is closed, which unblocks the parked read and lets the suite
// end. A regression fails here rather than hanging CI.
func TestRequestBodyReadIsBoundedInTime(t *testing.T) {
	oldTimeout := requestBodyReadTimeout
	requestBodyReadTimeout = 250 * time.Millisecond
	defer func() { requestBodyReadTimeout = oldTimeout }()

	buf, log := captureLog(zerolog.InfoLevel)
	// The upstream is unreachable on purpose: a body that never arrives must
	// be answered without ever reaching the provider path.
	h := NewHandler(newTestStore(t, "http://127.0.0.1:9/v1"), directResolver(), nil, nil, nil, log)
	srv := httptest.NewServer(h)
	// Registered first, so it runs last: the client connection is closed
	// before the server waits on it.
	defer srv.Close()

	conn, err := net.Dial("tcp", srv.Listener.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = conn.Close() }()

	// A complete, authenticatable request whose body is announced in full and
	// then never delivered.
	_, err = fmt.Fprintf(conn,
		"POST /v1/chat/completions HTTP/1.1\r\nHost: injector.test\r\n"+
			"Authorization: Bearer %s\r\nContent-Type: application/json\r\n"+
			"Content-Length: %d\r\n\r\n",
		testAPIKey, maxRequestBodyBytes)
	if err != nil {
		t.Fatalf("write headers: %v", err)
	}
	if _, err := conn.Write([]byte(`{"model":"test-model"`)); err != nil {
		t.Fatalf("write partial body: %v", err)
	}
	// ... and not another byte, with the connection still open.

	start := time.Now()
	if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatalf("the handler never answered a stalled body: %v", err)
	}
	body, err := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if err != nil {
		t.Fatalf("read response body: %v", err)
	}
	elapsed := time.Since(start)

	// Nothing but the deadline can have ended this read: the client never
	// sent EOF and never exceeded the size cap.
	if elapsed < requestBodyReadTimeout {
		t.Errorf("answered after %s, before the %s bound — something other than the read deadline ended it",
			elapsed, requestBodyReadTimeout)
	}
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
	if got := string(body); got != envelopeInvalidReq {
		t.Errorf("body = %q, want the invalid-JSON envelope %q", got, envelopeInvalidReq)
	}
	// A stalled read is a read that never completed: it lands on the read
	// failure's outcome, not on an outcome invented for the clock. Every one
	// of the debug/info events the handler emitted is also checked for body
	// bytes by the no-echo rule's own tests; here the assertion is the
	// classification.
	evs := buf.events(t, "request_completed")
	if len(evs) != 1 {
		t.Fatalf("request_completed events = %d, want 1: %s", len(evs), buf.String())
	}
	if evs[0]["outcome"] != "body_read_error" {
		t.Errorf("outcome = %v, want body_read_error", evs[0]["outcome"])
	}
	if evs[0]["bytes_in"] != float64(0) {
		t.Errorf("bytes_in = %v, want 0 — a refused read holds nothing", evs[0]["bytes_in"])
	}
}

// TestRequestBodyReadTimeoutAdmitsAMaximumBody pins the two directions of
// the bound's derivation, so neither can drift silently: too low an implied
// throughput makes a legitimate maximum-size upload unreliable, and too long
// a window stops bounding anything at all.
func TestRequestBodyReadTimeoutAdmitsAMaximumBody(t *testing.T) {
	const minThroughput = 200 << 10 // 200 KiB/s ≈ 1.6 Mbit/s
	implied := maxRequestBodyBytes / int64(requestBodyReadTimeout/time.Second)
	if implied < minThroughput {
		t.Fatalf("requestBodyReadTimeout %s implies %d B/s for a %d-byte body; a legitimate body must complete at %d B/s or better",
			requestBodyReadTimeout, implied, maxRequestBodyBytes, minThroughput)
	}
	if requestBodyReadTimeout > time.Hour {
		t.Fatalf("requestBodyReadTimeout %s is long enough to stop bounding anything", requestBodyReadTimeout)
	}
}

// TestRequestBodyReadAdmitsARealisticBody is the quiet direction: the bound
// must not reap the traffic it exists to protect. A body of the size this
// service actually sees — a large tool schema, a multimodal request — goes
// through the real server, the real reader and the real deadline, and has to
// come out the other side untouched.
func TestRequestBodyReadAdmitsARealisticBody(t *testing.T) {
	var got int64
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n, _ := io.Copy(io.Discard, r.Body)
		got = n
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"model":"upstream-name","choices":[]}`)
	}))
	defer upstream.Close()

	h := newTestHandler(t, newTestStore(t, upstream.URL+"/v1"))
	srv := httptest.NewServer(h)
	defer srv.Close()

	payload := strings.Repeat("x", 8<<20) // 8 MiB
	body := `{"model":"test-model","messages":[{"role":"user","content":"` + payload + `"}]}`
	req, err := http.NewRequest(http.MethodPost, srv.URL+"/v1/chat/completions", strings.NewReader(body))
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+testAPIKey)
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if _, err := io.Copy(io.Discard, resp.Body); err != nil {
		t.Fatalf("read response: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200 — the read deadline reaped a legitimate body", resp.StatusCode)
	}
	if got == 0 {
		t.Fatal("the upstream received nothing")
	}
}
