package proxy

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"syscall"
	"time"

	"github.com/rs/zerolog"

	"openai-compatible-injector/internal/config"
	"openai-compatible-injector/internal/inject"
)

// Error envelopes. The 4xx rejection envelopes carry param/code as explicit
// nulls; the 5xx upstream envelopes carry no param field at all. Bodies are
// literal constants so the wire format is byte-exact; only the 404 model
// message is assembled at runtime (JSON-escaped) because it embeds input.
const (
	contentTypeHeader  = "Content-Type"
	eventStreamType    = "text/event-stream"
	forwardJSONType    = "application/json"
	envelopeJSONType   = "application/json"
	envelopeInvalidReq = `{"error":{"message":"invalid JSON in request body","type":"invalid_request_error","param":null,"code":null}}`
	envelopeMissingMod = `{"error":{"message":"you must provide a model parameter","type":"invalid_request_error","param":null,"code":null}}`
	envelopeTooLarge   = `{"error":{"message":"request body too large","type":"invalid_request_error","param":null,"code":null}}`
	envelopeUpInvalid  = `{"error":{"message":"upstream returned an invalid response","type":"upstream_error","code":"upstream_invalid_response"}}`
	envelopeUpUnreach  = `{"error":{"message":"upstream request failed","type":"upstream_error","code":"upstream_unreachable"}}`
	envelopeBadMethod  = `{"error":{"message":"method not allowed","type":"invalid_request_error","param":null,"code":null}}`
)

// client headers forwarded upstream. Every other client header is dropped
// deliberately: credentials must never be forwarded beyond Authorization.
var forwardHeaderNames = []string{"Authorization", "Content-Type", "Accept", "OpenAI-Beta"}

// response headers relayed back to the client from upstream. Rate-limit and
// retry headers are load-bearing for well-behaved client SDK backoff; a 429
// without Retry-After is indistinguishable from any other upstream error.
var relayHeaderNames = []string{
	"Content-Type",
	"Cache-Control",
	"X-Request-Id",
	"OpenAI-Request-Id",
	"Retry-After",
	"Location",
	"X-RateLimit-Limit",
	"X-RateLimit-Remaining",
	"X-RateLimit-Reset",
	"X-RateLimit-Reset-Requests",
	"X-RateLimit-Reset-Tokens",
}

// Body size caps. Requests are bounded so a single client cannot pin
// unbounded memory; buffered upstream responses are bounded so a broken or
// hostile peer cannot do the same from the other side. Both are generous —
// multimodal requests and tool schemas are legitimately large — and exist
// to turn a memory-amplification attack into a clean error.
var (
	maxRequestBodyBytes      int64 = 64 << 20 // 64 MiB
	maxBufferedResponseBytes int64 = 64 << 20 // 64 MiB
)

// openAIError is the envelope shape for model-not-found responses, whose
// message interpolates the requested model.
type openAIError struct {
	Error openAIErrorBody `json:"error"`
}

type openAIErrorBody struct {
	Message string      `json:"message"`
	Type    string      `json:"type"`
	Param   interface{} `json:"param"`
	Code    interface{} `json:"code"`
}

// NewHandler assembles the injector's HTTP surface: a health endpoint and
// the two OpenAI-compatible chat routes. Every proxied request reads the
// store once and binds the entire request lifetime to that snapshot.
func NewHandler(store *config.Store, client *http.Client, log zerolog.Logger) http.Handler {
	h := &injectorHandler{store: store, client: client, log: log}
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", h.healthz)
	// The routes are method-agnostic patterns so that wrong methods reach
	// our handler and receive the JSON 405 envelope instead of the mux's
	// plain-text default.
	mux.HandleFunc("/v1/chat/completions", h.chatCompletions)
	mux.HandleFunc("/v1/responses", h.responses)
	return mux
}

type injectorHandler struct {
	store  *config.Store
	client *http.Client
	log    zerolog.Logger
}

func (h *injectorHandler) healthz(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeEnvelope(w, http.StatusMethodNotAllowed, envelopeBadMethod)
		return
	}
	w.Header().Set(contentTypeHeader, "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = io.WriteString(w, "ok\n")
}

func (h *injectorHandler) chatCompletions(w http.ResponseWriter, r *http.Request) {
	h.serve(w, r, "chat", inject.Chat, "/chat/completions")
}

func (h *injectorHandler) responses(w http.ResponseWriter, r *http.Request) {
	h.serve(w, r, "responses", inject.Responses, "/responses")
}

type transformFunc func(body []byte, m config.Model) ([]byte, error)

// serve runs the full injector flow for one request. One snapshot is loaded
// at entry and every later step (resolution, transformation, forwarding,
// trailing rewrite) binds to it.
//
// Logging rides the same flow: a debug-level request_received, one INFO
// request_completed per request with the wire facts (status, outcome,
// duration, byte counts, snapshot generation), and WARN-level stream
// truncation split by phase. Metadata only — bodies, prompts, payloads,
// and Authorization never enter any log event at any level.
func (h *injectorHandler) serve(w http.ResponseWriter, r *http.Request, api string, transform transformFunc, suffix string) {
	start := time.Now()
	if r.Method != http.MethodPost {
		// Outside the request lifecycle: no snapshot is loaded and no
		// generation exists to bind, and a wrong method is a client bug
		// rather than proxy traffic — debug is the honest level.
		h.log.Debug().Str("api", api).Str("method", r.Method).
			Str("path", r.URL.Path).Str("remote_addr", r.RemoteAddr).
			Msg("request_method_not_allowed")
		writeEnvelope(w, http.StatusMethodNotAllowed, envelopeBadMethod)
		return
	}

	snap := h.store.Load()
	defer func() { _ = r.Body.Close() }()

	sw := &statusWriter{ResponseWriter: w}
	log := h.log.With().Str("request_id", newRequestID()).Str("api", api).Logger()
	log.Debug().Str("method", r.Method).Str("path", r.URL.Path).
		Str("remote_addr", r.RemoteAddr).Msg("request_received")

	var (
		outcome     = "completed"
		stream      bool
		publicModel string
		bytesIn     int64
	)
	complete := func() {
		log.Info().
			Int("status", sw.status).
			Str("outcome", outcome).
			Str("public_model", publicModel).
			Bool("stream", stream).
			Int64("bytes_in", bytesIn).
			Int64("bytes_out", sw.bytes).
			Int64("duration_ms", time.Since(start).Milliseconds()).
			Uint64("config_generation", snap.Gen()).
			Msg("request_completed")
	}

	// Bound the request body before reading it: without a cap, a single
	// oversized client request pins unbounded memory in the proxy.
	r.Body = http.MaxBytesReader(sw, r.Body, maxRequestBodyBytes)
	body, err := io.ReadAll(r.Body)
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			outcome = "body_too_large"
			writeEnvelope(sw, http.StatusRequestEntityTooLarge, envelopeTooLarge)
			complete()
			return
		}
		outcome = "body_read_error"
		writeEnvelope(sw, http.StatusBadRequest, envelopeInvalidReq)
		complete()
		return
	}
	bytesIn = int64(len(body))

	model, stream, err := inject.Probe(body)
	if err != nil {
		outcome = "invalid_json"
		writeEnvelope(sw, http.StatusBadRequest, envelopeInvalidReq)
		complete()
		return
	}
	publicModel = model
	if model == "" {
		outcome = "missing_model"
		writeEnvelope(sw, http.StatusBadRequest, envelopeMissingMod)
		complete()
		return
	}

	m, ok := snap.Model(model)
	if !ok {
		outcome = "model_not_found"
		h.writeModelNotFound(sw, model)
		complete()
		return
	}

	out, err := transform(body, m)
	if err != nil {
		outcome = "transform_error"
		writeEnvelope(sw, http.StatusBadRequest, envelopeInvalidReq)
		complete()
		return
	}

	upstream := *m.Endpoint
	// Trim every trailing slash ("trailing slashes ignored" holds for any
	// number) and clear RawPath: mutating Path can leave a RawPath that no
	// longer matches, which makes EscapedPath silently percent-decode the
	// endpoint path. Encoded endpoint paths are normalized, not preserved.
	upstream.Path = strings.TrimRight(upstream.Path, "/") + suffix
	upstream.RawPath = ""

	req, err := http.NewRequestWithContext(r.Context(), http.MethodPost, upstream.String(), bytes.NewReader(out))
	if err != nil {
		// Unreachable by construction (the endpoint was validated to a
		// *url.URL at config load), but if it ever fires the raw error text
		// must still not reach logs — it would embed the full URL.
		log.Error().Str("model", model).
			Str("upstream", origin(&upstream)).
			Str("error_class", "request_build").
			Msg("upstream_request_build_failed")
		outcome = "upstream_unreachable"
		writeEnvelope(sw, http.StatusBadGateway, envelopeUpUnreach)
		complete()
		return
	}
	copyForwardHeaders(req.Header, r.Header)

	resp, err := h.client.Do(req)
	if err != nil {
		// The *url.Error from client.Do embeds the full request URL —
		// query string included, which is how query-authenticated
		// providers leak credentials. Log the sanitized error and the
		// scheme+host origin only, per the credential rule.
		event := log.Error()
		if errors.Is(err, context.Canceled) {
			// The client went away mid-request; that is an operational
			// warning, not an upstream failure.
			event = log.Warn()
		}
		event.Err(sanitizeUpstreamError(err, &upstream)).
			Str("public_model", model).
			Str("upstream", origin(&upstream)).
			Str("error_class", upstreamErrorClass(err)).
			Msg("upstream_request_failed")
		outcome = "upstream_unreachable"
		writeEnvelope(sw, http.StatusBadGateway, envelopeUpUnreach)
		complete()
		return
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode >= http.StatusMultipleChoices || resp.StatusCode == http.StatusNoContent || resp.StatusCode == http.StatusNotModified {
		// Verbatim relay: anything outside a body-bearing 2xx — upstream
		// errors (any 4xx/5xx), redirects (3xx, which CheckRedirect never
		// follows), and the two body-less statuses, which are valid upstream
		// answers. A 204 or an unexpected 3xx is the upstream's answer;
		// turning it into a 502 would fog the root cause. Status and body
		// relayed byte for byte, whatever the content type.
		copyRelayHeaders(sw.Header(), resp.Header)
		sw.WriteHeader(resp.StatusCode)
		if _, err := io.Copy(sw, resp.Body); err != nil {
			event := log.Error()
			if errors.Is(err, context.Canceled) {
				event = log.Warn()
			}
			event.Err(err).Str("public_model", model).Msg("relay_copy_failed")
		}
		outcome = "relayed"
		complete()
		return
	}

	if stream && strings.Contains(strings.ToLower(resp.Header.Get(contentTypeHeader)), eventStreamType) {
		// Incremental SSE passthrough. The 2xx status is committed here; any
		// subsequent failure only truncates the stream, never switches the
		// response to an error body.
		copyRelayHeaders(sw.Header(), resp.Header)
		sw.WriteHeader(resp.StatusCode)
		log.Debug().Str("public_model", model).Msg("stream_started")
		stats, err := CopySSE(sw, resp.Body, m.Public, flusher(sw))
		if err != nil {
			phase := "upstream_read"
			outcome = "stream_truncated"
			var swe *streamWriteError
			if errors.As(err, &swe) {
				// The client connection broke mid-stream; the upstream may
				// have been fine. An operational warning, not an error.
				phase = "client_write"
			} else if errors.Is(err, ErrSSELineTooLong) || errors.Is(err, ErrSSEEventTooLarge) {
				// The upstream crossed a bounded-relay cap: a hostile or
				// broken peer, stopped cleanly at the wall. The logged
				// error carries counts only, never the bytes themselves.
				phase = "upstream_limit"
				outcome = "stream_limit_exceeded"
			}
			log.Warn().Err(err).Str("public_model", model).Str("phase", phase).
				Int64("bytes_out", stats.Bytes).Int("events", stats.Events).
				Msg("stream_truncated")
		} else {
			log.Debug().Str("public_model", model).
				Int64("bytes_out", stats.Bytes).Int("events", stats.Events).
				Msg("stream_completed")
		}
		complete()
		return
	}

	// Buffered non-stream path (client stream=false, or upstream ignored the
	// stream flag): read the whole body, validate it, rewrite the model, and
	// emit a single buffered response. The read is bounded — an over-cap
	// body is treated like any other unparseable upstream answer.
	upstreamBody, err := io.ReadAll(io.LimitReader(resp.Body, maxBufferedResponseBytes+1))
	if err != nil || len(upstreamBody) > int(maxBufferedResponseBytes) || !json.Valid(upstreamBody) {
		outcome = "upstream_invalid_response"
		writeEnvelope(sw, http.StatusBadGateway, envelopeUpInvalid)
		complete()
		return
	}
	rewritten := inject.RewriteModel(upstreamBody, m.Public)
	copyRelayHeaders(sw.Header(), resp.Header)
	sw.WriteHeader(resp.StatusCode)
	_, _ = sw.Write(rewritten)
	complete()
}

func (h *injectorHandler) writeModelNotFound(w http.ResponseWriter, model string) {
	env := openAIError{Error: openAIErrorBody{
		Message: "The model '" + model + "' does not exist or you do not have access to it.",
		Type:    "invalid_request_error",
		Code:    "model_not_found",
	}}
	body, err := json.Marshal(env)
	writeEnvelopeErr(w, http.StatusNotFound, body, err)
}

// origin renders the endpoint's scheme+host — the only upstream URL detail
// that may ever reach logs. Query strings, paths, and userinfo are
// credential- or traffic-bearing and stay out.
func origin(u *url.URL) string {
	return u.Scheme + "://" + u.Host
}

// sanitizeUpstreamError rebuilds a client.Do error without the full request
// URL: *url.Error.Error() quotes it verbatim, query string included.
func sanitizeUpstreamError(err error, endpoint *url.URL) error {
	var ue *url.Error
	if errors.As(err, &ue) {
		return &url.Error{Op: ue.Op, URL: origin(endpoint), Err: ue.Err}
	}
	return err
}

// upstreamErrorClass buckets a client.Do error for logging. The error is
// classified in its raw form — sanitization only strips the URL text.
func upstreamErrorClass(err error) string {
	if errors.Is(err, context.Canceled) {
		return "client_canceled"
	}
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		return "timeout"
	}
	var te *tls.CertificateVerificationError
	if errors.As(err, &te) {
		return "tls"
	}
	var oe *net.OpError
	if errors.As(err, &oe) && errors.Is(oe.Err, syscall.ECONNREFUSED) {
		return "connection_refused"
	}
	return "dial"
}

func writeEnvelope(w http.ResponseWriter, status int, body string) {
	w.Header().Set(contentTypeHeader, envelopeJSONType)
	w.WriteHeader(status)
	_, _ = io.WriteString(w, body)
}

// writeEnvelopeErr writes a marshaled envelope, falling back to a static
// body if marshaling somehow fails.
func writeEnvelopeErr(w http.ResponseWriter, status int, b []byte, err error) {
	if err != nil {
		writeEnvelope(w, status, envelopeInvalidReq)
		return
	}
	w.Header().Set(contentTypeHeader, envelopeJSONType)
	w.WriteHeader(status)
	_, _ = w.Write(b)
}

// copyForwardHeaders copies exactly the allow-listed client headers onto the
// upstream request, defaulting Content-Type to application/json.
func copyForwardHeaders(dst, src http.Header) {
	for _, name := range forwardHeaderNames {
		if v := src.Get(name); v != "" {
			dst.Set(name, v)
		}
	}
	if dst.Get(contentTypeHeader) == "" {
		dst.Set(contentTypeHeader, forwardJSONType)
	}
}

// copyRelayHeaders copies exactly the allow-listed upstream response headers
// back to the client.
func copyRelayHeaders(dst, src http.Header) {
	for _, name := range relayHeaderNames {
		if v := src.Get(name); v != "" {
			dst.Set(name, v)
		}
	}
}

// flusher returns a flushing closure for streaming responses when the
// ResponseWriter supports it, else a no-op.
func flusher(w http.ResponseWriter) func() {
	if f, ok := w.(http.Flusher); ok {
		return f.Flush
	}
	return func() {}
}

// statusWriter records the committed status and total bytes written for the
// access log, forwarding everything else. The status falls back to 200 when
// a handler writes without an explicit WriteHeader — net/http's own rule.
type statusWriter struct {
	http.ResponseWriter
	status int
	bytes  int64
}

func (w *statusWriter) WriteHeader(code int) {
	if w.status == 0 {
		w.status = code
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w *statusWriter) Write(b []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	n, err := w.ResponseWriter.Write(b)
	w.bytes += int64(n)
	return n, err
}

func (w *statusWriter) Flush() {
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// requestIDSource is 8 bytes of crypto/rand per request — 16 hex characters.
// Full UUIDs cost an order of magnitude more for the same operational value:
// the id only needs to be unique within this process's log stream.
const requestIDSource = 8

func newRequestID() string {
	var b [requestIDSource]byte
	if _, err := rand.Read(b[:]); err != nil {
		// A dead entropy source must not fail requests; the all-zero id
		// stays a valid, if colliding, correlation key.
		return "0000000000000000"
	}
	return hex.EncodeToString(b[:])
}
