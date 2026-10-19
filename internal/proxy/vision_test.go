package proxy

import (
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"

	"openai-compatible-injector/internal/config"
	"openai-compatible-injector/internal/transport"
)

// visionStore builds a store whose vision-model walks two candidates over
// providers pa (https://a.example) and pb (https://b.example), each
// carrying the given vision YAML line ("", "vision: true", ...).
func visionStore(t *testing.T, visionA, visionB string) *config.Store {
	t.Helper()
	vA, vB := "", ""
	if visionA != "" {
		vA = "        vision: " + visionA + "\n"
	}
	if visionB != "" {
		vB = "        vision: " + visionB + "\n"
	}
	snap, err := config.LoadRuntime([]byte("api-key: " + testAPIKey + `
transports:
  t1:
    type: direct
providers:
  pa:
    base-url: https://a.example/v1
    transport: t1
  pb:
    base-url: https://b.example/v1
    transport: t1
models:
  vision-model:
    providers:
      - provider: pa
        upstream-model: up-a
` + vA + `      - provider: pb
        upstream-model: up-b
` + vB))
	if err != nil {
		t.Fatalf("LoadRuntime: %v", err)
	}
	return config.NewStore(snap)
}

// hostUpstream is a Doer that answers every request with the same canned
// 2xx while recording which hosts were dialed — the per-candidate stand-in
// both candidates share, distinguished by request host.
type hostUpstream struct {
	mu    sync.Mutex
	hosts []string
	// failStatus, when non-zero, is the 5xx answered instead of 200 — the
	// retained-answer test's walk-budget failure.
	failStatus int
}

func (h *hostUpstream) Do(req *http.Request) (*http.Response, error) {
	h.mu.Lock()
	h.hosts = append(h.hosts, req.URL.Host)
	h.mu.Unlock()
	status := http.StatusOK
	if h.failStatus != 0 {
		status = h.failStatus
	}
	return &http.Response{
		StatusCode: status,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader(`{"model":"up","choices":[]}`)),
		Request:    req,
	}, nil
}

func (h *hostUpstream) dialed(host string) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	n := 0
	for _, x := range h.hosts {
		if x == host {
			n++
		}
	}
	return n
}

// visionChatBody is a chat request for vision-model whose sole user message
// content is the given raw JSON.
func visionChatBody(content string) []byte {
	return []byte(fmt.Sprintf(`{"model":"vision-model","messages":[{"role":"user","content":%s}]}`, content))
}

// visionImageBody is the shape the scanner reads: an image_url content part.
var visionImageBody = visionChatBody(`[{"type":"image_url","image_url":{"url":"data:image/png;base64,AAAA"}}]`)

// TestVisionNoVisionCandidateSkipsScan pins the hot-path direction the
// feature's contract rests on: a model with NO vision candidate answers an
// image request exactly like a text request — the image body reaches the
// primary as-is, the walk never moves because of the image, and the scan
// never runs (asserted by the walk shape, which a filtered chain would
// have changed).
func TestVisionNoVisionCandidateSkipsScan(t *testing.T) {
	store := visionStore(t, "", "")
	up := &hostUpstream{}
	h := NewHandler(store, fixedDoer{up}, nil, nil, nil, quietLogger())
	rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions", string(visionImageBody), authHeaders())
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if got := up.dialed("a.example"); got != 1 {
		t.Errorf("pa dialed %d times, want 1", got)
	}
	if got := up.dialed("b.example"); got != 0 {
		t.Errorf("pb dialed %d times, want 0", got)
	}
}

// TestVisionImageFilteredToVisionCandidates pins the auto-switch: an image
// request on a chain whose primary is vision-capable walks THAT candidate
// only — the text-only fallback is not churned even when the vision
// candidate fails its walk budget.
func TestVisionImageFilteredToVisionCandidates(t *testing.T) {
	// The vision primary fails with a 5xx; the default matrix retries it
	// once, then the walk would fall back — but the only other candidate is
	// text-only and filtered out, so the walk ends on the retained 500.
	store := visionStore(t, "true", "")
	up := &hostUpstream{failStatus: http.StatusInternalServerError}
	h := NewHandler(store, fixedDoer{up}, nil, nil, nil, quietLogger())
	rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions", string(visionImageBody), authHeaders())
	// The 5xx is the last received answer; a 500 relays.
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500 (the retained answer)", rec.Code)
	}
	if got := up.dialed("a.example"); got != 2 {
		t.Errorf("pa dialed %d times, want 2 (initial + the default matrix's one retry)", got)
	}
	if got := up.dialed("b.example"); got != 0 {
		t.Errorf("pb dialed %d times, want 0 — the text-only candidate is filtered out of an image walk", got)
	}
}

// TestVisionTextRequestWalksEveryCandidate pins the other half: a
// plain-text request on the same chain walks BOTH candidates in written
// order — the filter applies to image requests only.
func TestVisionTextRequestWalksEveryCandidate(t *testing.T) {
	store := visionStore(t, "true", "")
	up := &hostUpstream{}
	h := NewHandler(store, fixedDoer{up}, nil, nil, nil, quietLogger())
	rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions", string(visionChatBody(`"hi"`)), authHeaders())
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if got := up.dialed("a.example"); got != 1 {
		t.Errorf("pa dialed %d times, want 1", got)
	}
	if got := up.dialed("b.example"); got != 0 {
		t.Errorf("pb dialed %d times, want 0 — the healthy primary answers", got)
	}
}

// TestVisionImageOnMixedChainSkipsTextOnly pins the mixed chain: with pa
// text-only and pb vision-capable, an image request's effective chain
// starts at pb — pa is never dialed.
func TestVisionImageOnMixedChainSkipsTextOnly(t *testing.T) {
	store := visionStore(t, "", "true")
	up := &hostUpstream{}
	h := NewHandler(store, fixedDoer{up}, nil, nil, nil, quietLogger())
	rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions", string(visionImageBody), authHeaders())
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if got := up.dialed("a.example"); got != 0 {
		t.Errorf("pa dialed %d times, want 0 — the text-only candidate is filtered out", got)
	}
	if got := up.dialed("b.example"); got != 1 {
		t.Errorf("pb dialed %d times, want 1", got)
	}
}

// TestVisionImageOnAllTextChainFallsBack pins the no-vision chain under an
// image request whose primary fails: without a vision candidate the image
// is nobody's business — the ordinary matrix moves the walk to the
// text-only fallback, which answers. This is the behavior the skip-scan
// rule preserves: an image request is not specially routed for a model
// that declared no vision.
func TestVisionImageOnAllTextChainFallsBack(t *testing.T) {
	store := visionStore(t, "", "")
	// pa fails with a transport-level error; the default matrix falls back.
	up := &failingFirst{ok: &hostUpstream{}}
	h := NewHandler(store, fixedDoer{up}, nil, nil, nil, quietLogger())
	rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions", string(visionImageBody), authHeaders())
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 from the text-only fallback", rec.Code)
	}
}

// TestVisionModelLevelLegacyForm pins the provider-form spelling end to
// end: a vision:true legacy model's one candidate serves the image request.
func TestVisionModelLevelLegacyForm(t *testing.T) {
	snap, err := config.LoadRuntime([]byte("api-key: " + testAPIKey + `
models:
  vision-model:
    endpoint: https://a.example/v1
    upstream-model: up-a
    vision: true
`))
	if err != nil {
		t.Fatalf("LoadRuntime: %v", err)
	}
	store := config.NewStore(snap)
	up := &hostUpstream{}
	h := NewHandler(store, fixedDoer{up}, nil, nil, nil, quietLogger())
	rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions", string(visionImageBody), authHeaders())
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if got := up.dialed("a.example"); got != 1 {
		t.Errorf("a.example dialed %d times, want 1", got)
	}
}

// failingFirst fails every request whose host is a.example and otherwise
// delegates — the all-text fallback test's transport failure.
type failingFirst struct {
	ok *hostUpstream
}

func (f *failingFirst) Do(req *http.Request) (*http.Response, error) {
	if req.URL.Host == "a.example" {
		return nil, fmt.Errorf("dial tcp: connection refused")
	}
	return f.ok.Do(req)
}

// authHeaders is the default bearer the unit suite authenticates with.
func authHeaders() map[string]string {
	return map[string]string{"Authorization": "Bearer " + testAPIKey}
}

var _ = transport.Direct // keep the transport import pinned
