package proxy

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"
)

// Pure-function coverage for the upstream error machinery: classification,
// the provider-token gate, fingerprint determinism, and the envelope shape.
// The handler-level behavior (status preservation, header relay, outcomes,
// log evidence) lives in handler_test.go.

func TestIsUpstreamHTTPError(t *testing.T) {
	for _, tc := range []struct {
		status int
		want   bool
	}{
		{199, false}, {200, false}, {204, false}, {299, false}, {302, false}, {304, false},
		{400, true}, {401, true}, {404, true}, {429, true}, {499, true},
		{500, true}, {503, true}, {599, true},
		{600, false}, // no spec defines it; the verbatim branch keeps it
	} {
		if got := isUpstreamHTTPError(tc.status); got != tc.want {
			t.Errorf("isUpstreamHTTPError(%d) = %v, want %v", tc.status, got, tc.want)
		}
	}
}

func TestClassifyErrorBody(t *testing.T) {
	for _, tc := range []struct {
		name      string
		body      string
		wantShape string
		wantType  string
		wantCode  string
	}{
		{"empty", "", shapeEmpty, "", ""},
		{"openai shaped", `{"error":{"message":"m","type":"rate_limit_error","code":"insufficient_quota"}}`, shapeJSONObject, "rate_limit_error", "insufficient_quota"},
		{"error member is a string", `{"error":"busy"}`, shapeJSON, "", ""},
		{"error member is null", `{"error":null}`, shapeJSONObject, "", ""},
		{"json without error member", `{"detail":"nope"}`, shapeJSON, "", ""},
		{"json array", `[1,2,3]`, shapeJSON, "", ""},
		{"json string", `"oops"`, shapeJSON, "", ""},
		{"json number", `42`, shapeJSON, "", ""},
		{"text", "internal gateway failure", shapeText, "", ""},
		{"html text", "<html>Service Unavailable</html>", shapeText, "", ""},
		{"malformed object", `{"error":`, shapeMalformed, "", ""},
		{"malformed array", `["unclosed`, shapeMalformed, "", ""},
		{"malformed with leading space", "  {\"error\":", shapeMalformed, "", ""},
		{"non-token type kept out", `{"error":{"type":"not a token","code":"ok"}}`, shapeJSONObject, "", "ok"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			shape, ptype, pcode := classifyErrorBody([]byte(tc.body))
			if shape != tc.wantShape || ptype != tc.wantType || pcode != tc.wantCode {
				t.Errorf("classifyErrorBody(%q) = (%q, %q, %q), want (%q, %q, %q)",
					tc.body, shape, ptype, pcode, tc.wantShape, tc.wantType, tc.wantCode)
			}
		})
	}
}

func TestPrintableToken(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want string
	}{
		{"", ""},
		{"rate_limit_error", "rate_limit_error"},
		{"429", "429"},
		{strings.Repeat("c", maxProviderTokenBytes), strings.Repeat("c", maxProviderTokenBytes)},
		{strings.Repeat("c", maxProviderTokenBytes+1), ""},
		{"has space", ""},
		{"has\ttab", ""},
		{"has\nnewline", ""},
		{"hög", ""}, // non-ASCII (multibyte) is not a token
	} {
		if got := printableToken(tc.in); got != tc.want {
			t.Errorf("printableToken(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

// TestCaptureUpstreamErrorEvidence pins the capture contract on a synthetic
// response: cap+1 read (never a full body), truncation flag, fingerprint
// over the bounded prefix, class bucketing, and the allow-listed rate-limit
// harvest.
func TestCaptureUpstreamErrorEvidence(t *testing.T) {
	body := `{"error":{"message":"m","type":"server_error"}}`
	// Header.Set canonicalizes the keys — a map literal with as-written
	// spellings ("X-RateLimit-…") would store non-canonical keys that
	// Header.Get never finds, and real responses always arrive canonical.
	hdr := http.Header{}
	hdr.Set("Content-Type", "application/json")
	hdr.Set("Retry-After", "30")
	hdr.Set("X-RateLimit-Reset-Tokens", "1.5s")
	hdr.Set("X-Session", "dropped")
	resp := &http.Response{
		StatusCode: http.StatusServiceUnavailable,
		Header:     hdr,
		Body:       io.NopCloser(strings.NewReader(body)),
	}
	ev, cerr := captureUpstreamErrorEvidence(context.Background(), resp)
	if cerr != captureOK {
		t.Fatalf("capture: %v", cerr)
	}
	if ev.status != http.StatusServiceUnavailable || ev.class != "upstream_http_5xx" {
		t.Errorf("status/class = %d/%q", ev.status, ev.class)
	}
	if ev.shape != shapeJSONObject || ev.providerType != "server_error" {
		t.Errorf("shape/providerType = %q/%q", ev.shape, ev.providerType)
	}
	if ev.bodyBytes != int64(len(body)) || ev.truncated {
		t.Errorf("bodyBytes/truncated = %d/%v, want %d/false", ev.bodyBytes, ev.truncated, len(body))
	}
	sum := sha256.Sum256([]byte(body))
	if ev.fingerprint != hex.EncodeToString(sum[:]) {
		t.Errorf("fingerprint = %q, want sha256 of the whole body", ev.fingerprint)
	}
	if ev.rateLimit["Retry-After"] != "30" || ev.rateLimit["X-RateLimit-Reset-Tokens"] != "1.5s" {
		t.Errorf("rateLimit = %v", ev.rateLimit)
	}
	if _, ok := ev.rateLimit["X-Session"]; ok {
		t.Errorf("non-allow-listed header harvested into evidence: %v", ev.rateLimit)
	}
}

// TestCaptureUpstreamErrorEvidenceTruncated pins the cap arithmetic: reading
// cap+1 bytes, clamping the capture to the cap, and fingerprinting only the
// prefix.
func TestCaptureUpstreamErrorEvidenceTruncated(t *testing.T) {
	old := maxUpstreamErrorBodyBytes
	maxUpstreamErrorBodyBytes = 8
	defer func() { maxUpstreamErrorBodyBytes = old }()

	resp := &http.Response{
		StatusCode: http.StatusTooManyRequests,
		Header:     http.Header{},
		Body:       io.NopCloser(strings.NewReader("0123456789abcdef")),
	}
	ev, cerr := captureUpstreamErrorEvidence(context.Background(), resp)
	if cerr != captureOK {
		t.Fatalf("capture: %v", cerr)
	}
	if !ev.truncated {
		t.Error("truncated = false, want true (body crossed the cap)")
	}
	if ev.bodyBytes != 8 {
		t.Errorf("bodyBytes = %d, want 8 (the capped prefix)", ev.bodyBytes)
	}
	if ev.shape != shapeTruncated {
		t.Errorf("shape = %q, want truncated", ev.shape)
	}
	sum := sha256.Sum256([]byte("01234567"))
	if ev.fingerprint != hex.EncodeToString(sum[:]) {
		t.Errorf("fingerprint = %q, want sha256 of the 8-byte prefix", ev.fingerprint)
	}
}

func TestUpstreamErrorEnvelopeBytes(t *testing.T) {
	for _, status := range []int{400, 401, 403, 404, 408, 409, 413, 422, 429, 500, 502, 503, 504} {
		ev := upstreamErrorEvidence{status: status}
		got, err := ev.envelopeBytes()
		if err != nil {
			t.Fatalf("envelope(%d): %v", status, err)
		}
		want := `{"error":{"message":"upstream provider returned HTTP ` + strconv.Itoa(status) +
			`","type":"upstream_error","param":null,"code":"upstream_http_` + strconv.Itoa(status) + `"}}`
		if string(got) != want {
			t.Errorf("status %d envelope:\n got %s\nwant %s", status, got, want)
		}
	}
}

// TestCaptureUpstreamErrorRateLimitBound pins the harvest bound: an oversized
// allow-listed header value is dropped wholesale from the evidence — a
// hostile peer cannot balloon one log line per request — while a real-sized
// value rides along. The client-side relay is a separate allow-list and is
// deliberately untouched.
func TestCaptureUpstreamErrorRateLimitBound(t *testing.T) {
	hdr := http.Header{}
	hdr.Set("Retry-After", strings.Repeat("9", maxRateLimitHeaderBytes+1))
	hdr.Set("X-RateLimit-Limit", "100")
	resp := &http.Response{
		StatusCode: http.StatusTooManyRequests,
		Header:     hdr,
		Body:       io.NopCloser(strings.NewReader(`{}`)),
	}
	ev, cerr := captureUpstreamErrorEvidence(context.Background(), resp)
	if cerr != captureOK {
		t.Fatalf("capture: %v", cerr)
	}
	if _, ok := ev.rateLimit["Retry-After"]; ok {
		t.Errorf("oversized Retry-After harvested into evidence: %d bytes", maxRateLimitHeaderBytes+1)
	}
	if ev.rateLimit["X-RateLimit-Limit"] != "100" {
		t.Errorf("X-RateLimit-Limit = %v, want 100 (under the bound)", ev.rateLimit["X-RateLimit-Limit"])
	}
}

// TestLogSafeContentType pins the log-only content-type normalizer: media
// type only (parameters are arbitrary upstream bytes), static markers for
// the absent/unparseable/oversized cases. The wire relay is a different
// surface and stays untouched.
func TestLogSafeContentType(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want string
	}{
		{"", ""},
		{"application/json", "application/json"},
		{"text/html; charset=utf-8", "text/html"},
		{"APPLICATION/Json", "application/json"}, // ParseMediaType canonicalizes case
		{"not a media type", "invalid"},
		{">>>", "invalid"},
		{";" + strings.Repeat("p", 40) + "=" + strings.Repeat("v", 100), "oversized"},
	} {
		if got := logSafeContentType(tc.in); got != tc.want {
			t.Errorf("logSafeContentType(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
	if got := logSafeContentType(strings.Repeat("x", maxLogContentTypeBytes+1)); got != "oversized" {
		t.Errorf("oversized content type = %q, want the static marker", got)
	}
}

// blockingBody never yields a byte: Read blocks until the body is closed.
type blockingBody struct{ pr *io.PipeReader }

func (b blockingBody) Read(p []byte) (int, error) { return b.pr.Read(p) }
func (b blockingBody) Close() error               { return b.pr.Close() }

// TestCaptureUpstreamErrorEvidenceCallerEnded pins the ownership half of the
// capture contract: the caller's cancellation closes the stalled body and
// the capture reports caller-ended — the handler answers with the
// disconnect outcome and no envelope, never a 502.
func TestCaptureUpstreamErrorEvidenceCallerEnded(t *testing.T) {
	pr, pw := io.Pipe()
	defer func() { _ = pw.Close() }()
	resp := &http.Response{
		StatusCode: http.StatusBadGateway,
		Header:     http.Header{"Content-Type": []string{"text/html"}},
		Body:       blockingBody{pr: pr},
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	ev, cerr := captureUpstreamErrorEvidence(ctx, resp)
	if cerr != captureCallerEnded {
		t.Fatalf("capture outcome = %v, want captureCallerEnded", cerr)
	}
	if ev.status != 0 || ev.shape != "" || ev.fingerprint != "" || len(ev.rateLimit) != 0 {
		t.Errorf("evidence = %+v, want the zero value", ev)
	}
	// The AfterFunc must have closed the stalled body — no detached reader.
	if _, err := pr.Read(make([]byte, 1)); !errors.Is(err, io.ErrClosedPipe) {
		t.Errorf("body not closed by the capture: %v", err)
	}
}

// TestCaptureUpstreamErrorEvidenceDeadline pins the timer half: a stalled
// body outliving the fixed capture deadline is closed and reported as
// captureDeadline — terminal for the answering candidate (the handler turns
// it into the canonical 502), never a reason to fall back.
func TestCaptureUpstreamErrorEvidenceDeadline(t *testing.T) {
	old := upstreamErrorCaptureTimeout
	upstreamErrorCaptureTimeout = 50 * time.Millisecond
	defer func() { upstreamErrorCaptureTimeout = old }()

	pr, pw := io.Pipe()
	defer func() { _ = pw.Close() }()
	resp := &http.Response{
		StatusCode: http.StatusServiceUnavailable,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       blockingBody{pr: pr},
	}

	ev, cerr := captureUpstreamErrorEvidence(context.Background(), resp)
	if cerr != captureDeadline {
		t.Fatalf("capture outcome = %v, want captureDeadline", cerr)
	}
	if ev.status != 0 || ev.shape != "" || ev.fingerprint != "" || len(ev.rateLimit) != 0 {
		t.Errorf("evidence = %+v, want the zero value", ev)
	}
	if _, err := pr.Read(make([]byte, 1)); !errors.Is(err, io.ErrClosedPipe) {
		t.Errorf("body not closed by the capture: %v", err)
	}
}

// readFailBody fails every read with a fixed error, mimicking a reset or
// truncated error-body connection.
type readFailBody struct{ err error }

func (b readFailBody) Read([]byte) (int, error) { return 0, b.err }
func (b readFailBody) Close() error             { return nil }

// TestCaptureUpstreamErrorEvidenceReadFailed pins the generic read-failure
// outcome: an error that is neither the caller's (live context) nor the
// capture timer's reports captureReadFailed and no evidence — the handler's
// 502-with-upstream_error WARN, never a raw error string.
func TestCaptureUpstreamErrorEvidenceReadFailed(t *testing.T) {
	resp := &http.Response{
		StatusCode: http.StatusInternalServerError,
		Header:     http.Header{},
		Body:       readFailBody{err: errors.New("http2: connection error: read failure")},
	}
	ev, cerr := captureUpstreamErrorEvidence(context.Background(), resp)
	if cerr != captureReadFailed {
		t.Fatalf("capture outcome = %v, want captureReadFailed", cerr)
	}
	if ev.status != 0 || ev.shape != "" || ev.fingerprint != "" || len(ev.rateLimit) != 0 {
		t.Errorf("evidence = %+v, want the zero value", ev)
	}
}
