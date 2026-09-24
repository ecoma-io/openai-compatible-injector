package server

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"openai-compatible-injector/internal/config"
	"openai-compatible-injector/internal/transport"
)

func testLogger(t *testing.T) zerolog.Logger {
	t.Helper()
	return zerolog.New(zerolog.TestWriter{T: t}).Level(zerolog.Disabled)
}

// serverTestAPIKey is the bearer credential the server tests configure and
// present.
const serverTestAPIKey = "server-test-key"

func testStore(t *testing.T, endpoint string) *config.Store {
	t.Helper()
	yaml := fmt.Sprintf("api-key: %s\nmodels:\n  test-model:\n    endpoint: %s\n    upstream-model: upstream-name\n    injection-prompt: \"\"\n", serverTestAPIKey, endpoint)
	snap, err := config.LoadRuntime([]byte(yaml))
	if err != nil {
		t.Fatalf("LoadRuntime: %v", err)
	}
	return config.NewStore(snap)
}

// freeAddr reserves an ephemeral loopback port and releases it for the
// server under test to bind.
func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve port: %v", err)
	}
	addr := ln.Addr().String()
	if err := ln.Close(); err != nil {
		t.Fatalf("release port: %v", err)
	}
	return addr
}

func waitForHealthy(t *testing.T, addr string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		resp, err := http.Get("http://" + addr + "/healthz")
		if err == nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("server at %s never became healthy (last err: %v)", addr, err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestRunDrainsInFlightRequest(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	upstream := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		<-release // hold the proxied request in flight
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"id":"1","model":"upstream-name"}`)
	})
	upSrv := &http.Server{Addr: "127.0.0.1:0", Handler: upstream}
	upLn, err := net.Listen("tcp", upSrv.Addr)
	if err != nil {
		t.Fatalf("listen upstream: %v", err)
	}
	defer func() { _ = upSrv.Close() }()
	go func() { _ = upSrv.Serve(upLn) }()
	upstreamURL := "http://" + upLn.Addr().String() + "/v1"

	addr := freeAddr(t)
	srv := New(testStore(t, upstreamURL), transport.NewRegistry(), nil, nil, nil, addr, 10*time.Second, testLogger(t))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runErr := make(chan error, 1)
	go func() { runErr <- srv.Run(ctx) }()
	waitForHealthy(t, addr)

	type clientResult struct {
		status int
		body   string
		err    error
	}
	clientDone := make(chan clientResult, 1)
	go func() {
		req, err := http.NewRequest(http.MethodPost, "http://"+addr+"/v1/chat/completions",
			strings.NewReader(`{"model":"test-model","messages":[{"role":"user","content":"hi"}]}`))
		if err != nil {
			clientDone <- clientResult{err: err}
			return
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+serverTestAPIKey)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			clientDone <- clientResult{err: err}
			return
		}
		defer func() { _ = resp.Body.Close() }()
		b, _ := io.ReadAll(resp.Body)
		clientDone <- clientResult{status: resp.StatusCode, body: string(b)}
	}()

	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("proxied request never reached upstream")
	}

	cancel()       // shutdown starts while the request is in flight
	close(release) // upstream now answers; drain must let it complete

	select {
	case res := <-clientDone:
		if res.err != nil {
			t.Fatalf("in-flight request failed during drain: %v", res.err)
		}
		if res.status != http.StatusOK {
			t.Fatalf("in-flight status = %d, want 200 (body %q)", res.status, res.body)
		}
		var got map[string]any
		if err := json.Unmarshal([]byte(res.body), &got); err != nil {
			t.Fatalf("in-flight body not valid JSON: %v (%q)", err, res.body)
		}
		if got["model"] != "test-model" {
			t.Errorf("in-flight body model = %v, want test-model (rewritten): %q", got["model"], res.body)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("in-flight request did not complete after drain")
	}

	select {
	case err := <-runErr:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("Run did not return after drain")
	}
}

func TestRunForceClosesPastGrace(t *testing.T) {
	entered := make(chan struct{})
	upstream := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		<-r.Context().Done() // stuck: never answers on its own
	})
	upSrv := &http.Server{Addr: "127.0.0.1:0", Handler: upstream}
	upLn, err := net.Listen("tcp", upSrv.Addr)
	if err != nil {
		t.Fatalf("listen upstream: %v", err)
	}
	defer func() { _ = upSrv.Close() }()
	go func() { _ = upSrv.Serve(upLn) }()
	upstreamURL := "http://" + upLn.Addr().String() + "/v1"

	const grace = 300 * time.Millisecond
	addr := freeAddr(t)
	srv := New(testStore(t, upstreamURL), transport.NewRegistry(), nil, nil, nil, addr, grace, testLogger(t))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runErr := make(chan error, 1)
	go func() { runErr <- srv.Run(ctx) }()
	waitForHealthy(t, addr)

	clientDone := make(chan error, 1)
	go func() {
		req, err := http.NewRequest(http.MethodPost, "http://"+addr+"/v1/chat/completions",
			strings.NewReader(`{"model":"test-model"}`))
		if err != nil {
			clientDone <- err
			return
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+serverTestAPIKey)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			clientDone <- err // force-close terminates the client conn
			return
		}
		defer func() { _ = resp.Body.Close() }()
		_, _ = io.Copy(io.Discard, resp.Body)
		clientDone <- nil
	}()

	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("proxied request never reached upstream")
	}

	start := time.Now()
	cancel()

	select {
	case err := <-runErr:
		elapsed := time.Since(start)
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
		if elapsed > 20*time.Second {
			t.Errorf("Run took %v with a %v grace; stuck upstream was not force-closed", elapsed, grace)
		}
		t.Logf("Run returned %v after cancel with %v grace", elapsed, grace)
	case <-time.After(25 * time.Second):
		t.Fatal("Run hung past grace with a stuck upstream; force-close did not happen")
	}

	select {
	case <-clientDone:
	case <-time.After(10 * time.Second):
		t.Fatal("stuck client request never terminated")
	}
}

func TestRunRefusesNewConnsAfterShutdown(t *testing.T) {
	upstream := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"model":"upstream-name"}`)
	})
	upSrv := &http.Server{Addr: "127.0.0.1:0", Handler: upstream}
	upLn, err := net.Listen("tcp", upSrv.Addr)
	if err != nil {
		t.Fatalf("listen upstream: %v", err)
	}
	defer func() { _ = upSrv.Close() }()
	go func() { _ = upSrv.Serve(upLn) }()
	upstreamURL := "http://" + upLn.Addr().String() + "/v1"

	addr := freeAddr(t)
	srv := New(testStore(t, upstreamURL), transport.NewRegistry(), nil, nil, nil, addr, time.Second, testLogger(t))

	ctx, cancel := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() { runErr <- srv.Run(ctx) }()
	waitForHealthy(t, addr)

	cancel()
	select {
	case err := <-runErr:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("Run did not return after cancel")
	}

	_, err = http.Get("http://" + addr + "/healthz")
	if err == nil {
		t.Fatal("new connection accepted after shutdown; listener should be closed")
	}
	t.Logf("post-shutdown dial failed as expected: %v", err)
}

func TestRunListenError(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve port: %v", err)
	}
	defer func() { _ = ln.Close() }()
	occupied := ln.Addr().String()

	srv := New(testStore(t, "http://127.0.0.1:9/v1"), transport.NewRegistry(), nil, nil, nil, occupied, time.Second, testLogger(t))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := srv.Run(ctx); err == nil {
		t.Fatalf("Run on occupied %s: want listen error, got nil", occupied)
	} else {
		t.Logf("Run returned listen error as expected: %v", err)
	}
}

// TestServerHasReadAndIdleTimeouts pins the connection hygiene: a server
// without a ReadHeaderTimeout accepts slowloris writes forever, and one
// without an IdleTimeout lets a client that opens a keep-alive connection
// and goes quiet pin a goroutine and a file descriptor for the process's
// whole life. Waiting out a real idle deadline in CI is not viable, so the
// pin is on the configuration itself.
func TestServerHasReadAndIdleTimeouts(t *testing.T) {
	srv := New(testStore(t, "http://127.0.0.1:9/v1"), transport.NewRegistry(), nil, nil, nil, "127.0.0.1:0", time.Second, testLogger(t))
	if srv.http.ReadHeaderTimeout <= 0 {
		t.Errorf("ReadHeaderTimeout = %v, want a positive bound", srv.http.ReadHeaderTimeout)
	}
	if srv.http.IdleTimeout <= 0 {
		t.Errorf("IdleTimeout = %v, want a positive bound so quiet keep-alive connections are reaped", srv.http.IdleTimeout)
	}
}
