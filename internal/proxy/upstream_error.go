package proxy

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
)

// Upstream HTTP errors (any 4xx/5xx) are normalized, not relayed: the
// client-facing body is always the canonical OpenAI-compatible envelope and
// the raw provider bytes never pass through in either direction. This file
// owns the pure half of that — bounded capture, classification, evidence, and
// envelope construction; the handler owns lifecycle (status commit, outcome
// accounting, logging).

// maxUpstreamErrorBodyBytes bounds the body captured from an upstream 4xx/5xx.
// It exists to fingerprint and classify, not to relay: an error body needs
// only enough bytes to identify its shape, so the cap sits three orders of
// magnitude below the buffered 2xx cap and a hostile peer runs into the same
// wall a cooperative one does. A var (like the other caps) so tests can pin
// the truncation edge without megabytes per case.
var maxUpstreamErrorBodyBytes int64 = 64 << 10 // 64 KiB

// evidenceRateLimitFields pairs the operational response headers copied into
// the log event (when present) with their snake_case field names. The header
// side sits on the relay allow-list (relayHeaderNames) too — this is the
// evidence view of the same names. Allow-listed only: arbitrary upstream
// headers never reach logs.
var evidenceRateLimitFields = []struct{ header, field string }{
	{"Retry-After", "retry_after"},
	{"X-RateLimit-Limit", "x_ratelimit_limit"},
	{"X-RateLimit-Remaining", "x_ratelimit_remaining"},
	{"X-RateLimit-Reset", "x_ratelimit_reset"},
	{"X-RateLimit-Reset-Requests", "x_ratelimit_reset_requests"},
	{"X-RateLimit-Reset-Tokens", "x_ratelimit_reset_tokens"},
}

// maxProviderTokenBytes bounds a provider error's type/code carried into the
// log event. These are metadata, not payloads: anything longer than this is
// not a token, and a value that fails the printable gate is dropped wholesale
// rather than truncated (a truncated fragment of a prompt echo is still a
// prompt echo).
const maxProviderTokenBytes = 64

// maxRateLimitHeaderBytes bounds a rate-limit header value carried into the
// log event. Real values are short (counts, epoch seconds, HTTP-dates), and
// Go caps no response-header length — so an oversized value is dropped
// wholesale, not truncated, keeping a hostile peer from ballooning one log
// line per request. The client-side relay is untouched: it is the
// allow-list's pre-existing behavior, not the log's.
const maxRateLimitHeaderBytes = 128

// isUpstreamHTTPError reports whether an upstream status is an HTTP error
// this proxy normalizes: every 4xx and 5xx. Outside the range nothing changes
// — 3xx (redirects, never followed), 204/304, and 2xx keep their existing
// branches.
func isUpstreamHTTPError(status int) bool {
	return status >= http.StatusBadRequest && status <= 599
}

// Error body shapes. The point is to know what kind of thing the provider
// sent without keeping the thing.
const (
	shapeEmpty      = "empty"             // no body at all
	shapeJSONObject = "json_error_object" // JSON object with an `error` member
	shapeJSON       = "json"              // any other valid JSON
	shapeText       = "text"              // non-JSON not starting like JSON
	shapeMalformed  = "malformed_json"    // non-JSON starting like JSON
	shapeTruncated  = "truncated"         // crossed the capture cap; unclassified
)

// upstreamErrorEvidence is the sanitized record of one upstream 4xx/5xx:
// everything an investigation needs, nothing the credential rule forbids.
// The captured bytes are reduced to a fingerprint before this struct is
// returned and are never stored on it.
type upstreamErrorEvidence struct {
	status      int
	contentType string
	// class buckets the status: upstream_http_4xx or upstream_http_5xx.
	class string
	// shape classifies the bounded body (shape* constants above).
	shape string
	// bodyBytes is the size of the captured, fingerprinted prefix — the
	// whole body when it fit under the cap, the first cap bytes when it did
	// not (truncated then reports that more existed).
	bodyBytes   int64
	truncated   bool
	fingerprint string
	// providerType and providerCode are the upstream error object's own
	// type/code strings when the body is an OpenAI-shaped error object and
	// the values pass the printable-token gate; empty otherwise. Never the
	// message.
	providerType string
	providerCode string
	// rateLimit carries the allow-listed Retry-After/X-RateLimit-* response
	// header values, keyed by header name; present members only.
	rateLimit map[string]string
}

// captureUpstreamErrorEvidence reads a bounded prefix of an upstream 4xx/5xx
// body and reduces it to evidence. The read is cap+1 bytes so truncation is
// detectable without trusting Content-Length; the extra byte never enters the
// capture, the fingerprint, or the classification. A read error is returned
// untouched for the handler to classify with the same body-read vocabulary
// the buffered 2xx path applies: clientSide means the client is gone,
// anything else is the upstream dying mid-answer.
func captureUpstreamErrorEvidence(resp *http.Response) (upstreamErrorEvidence, error) {
	ev := upstreamErrorEvidence{
		status:      resp.StatusCode,
		contentType: resp.Header.Get(contentTypeHeader),
		rateLimit:   map[string]string{},
	}
	for _, f := range evidenceRateLimitFields {
		if v := resp.Header.Get(f.header); v != "" && len(v) <= maxRateLimitHeaderBytes {
			ev.rateLimit[f.header] = v
		}
	}
	if ev.status >= 500 {
		ev.class = "upstream_http_5xx"
	} else {
		ev.class = "upstream_http_4xx"
	}

	captured, err := io.ReadAll(io.LimitReader(resp.Body, maxUpstreamErrorBodyBytes+1))
	if err != nil {
		return upstreamErrorEvidence{}, err
	}
	if int64(len(captured)) > maxUpstreamErrorBodyBytes {
		ev.truncated = true
		captured = captured[:maxUpstreamErrorBodyBytes]
	}
	ev.bodyBytes = int64(len(captured))
	sum := sha256.Sum256(captured)
	ev.fingerprint = hex.EncodeToString(sum[:])

	if ev.truncated {
		// A partial body cannot be classified honestly — the bytes that
		// would decide the shape were never read.
		ev.shape = shapeTruncated
		return ev, nil
	}
	ev.shape, ev.providerType, ev.providerCode = classifyErrorBody(captured)
	return ev, nil
}

// classifyErrorBody shapes a complete (non-truncated) bounded error body and,
// when it is an OpenAI-shaped error object, extracts the provider's own
// type/code tokens. The message is never extracted: a provider message can
// echo request material, and one that reaches a log line is a leak by the
// credential rule, truncation or not.
func classifyErrorBody(body []byte) (shape, providerType, providerCode string) {
	if len(body) == 0 {
		return shapeEmpty, "", ""
	}
	if !json.Valid(body) {
		trimmed := bytes.TrimLeft(body, " \t\r\n")
		if len(trimmed) > 0 && (trimmed[0] == '{' || trimmed[0] == '[') {
			return shapeMalformed, "", ""
		}
		return shapeText, "", ""
	}
	var top map[string]json.RawMessage
	if err := json.Unmarshal(body, &top); err != nil {
		// Valid JSON that is not an object (an array, string, or number)
		// lands here — it is still plain JSON, just not the error shape.
		return shapeJSON, "", ""
	}
	errMember, ok := top["error"]
	if !ok {
		return shapeJSON, "", ""
	}
	var errObj map[string]json.RawMessage
	if uerr := json.Unmarshal(errMember, &errObj); uerr != nil {
		// An `error` member that is not an object ("error": "busy" and
		// friends) is provider JSON, not the OpenAI error shape.
		return shapeJSON, "", ""
	}
	providerType = printableToken(rawValue(errObj["type"]))
	providerCode = printableToken(rawValue(errObj["code"]))
	return shapeJSONObject, providerType, providerCode
}

// rawValue returns the Go string behind a JSON string member, or "" for any
// other shape (absent, null, number, object, ...).
func rawValue(raw json.RawMessage) string {
	var s string
	if len(raw) == 0 || json.Unmarshal(raw, &s) != nil {
		return ""
	}
	return s
}

// printableToken gates a provider-supplied string for the log event: a value
// is carried only when it already looks like a token — 1..64 bytes of
// printable ASCII with no whitespace. Anything else returns "". The gate is
// deliberately all-or-nothing: half of a prompt echo is still a prompt echo,
// and a value with a newline or control byte would break the JSON-lines log
// contract — or smuggle a forged line.
func printableToken(s string) string {
	if len(s) == 0 || len(s) > maxProviderTokenBytes {
		return ""
	}
	for i := range len(s) {
		if s[i] < 0x21 || s[i] > 0x7e {
			return ""
		}
	}
	return s
}

// envelopeBytes renders the canonical client-facing envelope for the
// evidence's status. The message carries the status number only — no provider
// text, no model names — and the code derives from the status the same way,
// so one helper stays safer than a per-status table ever would.
func (ev upstreamErrorEvidence) envelopeBytes() ([]byte, error) {
	env := openAIError{Error: openAIErrorBody{
		Message: "upstream provider returned HTTP " + strconv.Itoa(ev.status),
		Type:    "upstream_error",
		Code:    "upstream_http_" + strconv.Itoa(ev.status),
	}}
	return marshalEnvelopeJSON(env)
}
