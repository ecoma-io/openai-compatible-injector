package proxy

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestNewSharedClientTransportTuning(t *testing.T) {
	c := NewSharedClient()
	tr, ok := c.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("transport is %T, want *http.Transport", c.Transport)
	}
	if got := tr.TLSHandshakeTimeout; got != 10*time.Second {
		t.Errorf("TLSHandshakeTimeout = %v, want 10s", got)
	}
	if got := tr.ResponseHeaderTimeout; got != 0 {
		t.Errorf("ResponseHeaderTimeout = %v, want 0 (long-lived SSE streams)", got)
	}
	if got := tr.IdleConnTimeout; got != 90*time.Second {
		t.Errorf("IdleConnTimeout = %v, want 90s", got)
	}
	if got := tr.MaxIdleConns; got != 100 {
		t.Errorf("MaxIdleConns = %v, want 100", got)
	}
	if got := tr.MaxIdleConnsPerHost; got != 100 {
		t.Errorf("MaxIdleConnsPerHost = %v, want 100", got)
	}
	if !tr.ForceAttemptHTTP2 {
		t.Error("ForceAttemptHTTP2 = false, want true")
	}
	if c.Timeout != 0 {
		t.Errorf("client.Timeout = %v, want 0 (no overall request deadline)", c.Timeout)
	}
}

func TestNewSharedClientServesRequests(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"ok":true}`)
	}))
	defer srv.Close()

	c := NewSharedClient()
	resp, err := c.Get(srv.URL)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if string(b) != `{"ok":true}` {
		t.Errorf("body = %q, want %q", b, `{"ok":true}`)
	}
}
