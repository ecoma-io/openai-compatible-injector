package proxy

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"

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
	h.serve(w, r, inject.Chat, "/chat/completions")
}

func (h *injectorHandler) responses(w http.ResponseWriter, r *http.Request) {
	h.serve(w, r, inject.Responses, "/responses")
}

type transformFunc func(body []byte, m config.Model) ([]byte, error)

// serve runs the full injector flow for one request. One snapshot is loaded
// at entry and every later step (resolution, transformation, forwarding,
// trailing rewrite) binds to it.
func (h *injectorHandler) serve(w http.ResponseWriter, r *http.Request, transform transformFunc, suffix string) {
	if r.Method != http.MethodPost {
		writeEnvelope(w, http.StatusMethodNotAllowed, envelopeBadMethod)
		return
	}
	snap := h.store.Load()
	defer func() { _ = r.Body.Close() }()

	// Bound the request body before reading it: without a cap, a single
	// oversized client request pins unbounded memory in the proxy.
	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBodyBytes)
	body, err := io.ReadAll(r.Body)
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			writeEnvelope(w, http.StatusRequestEntityTooLarge, envelopeTooLarge)
			return
		}
		writeEnvelope(w, http.StatusBadRequest, envelopeInvalidReq)
		return
	}

	model, stream, err := inject.Probe(body)
	if err != nil {
		writeEnvelope(w, http.StatusBadRequest, envelopeInvalidReq)
		return
	}
	if model == "" {
		writeEnvelope(w, http.StatusBadRequest, envelopeMissingMod)
		return
	}

	m, ok := snap.Model(model)
	if !ok {
		h.writeModelNotFound(w, model)
		return
	}

	out, err := transform(body, m)
	if err != nil {
		writeEnvelope(w, http.StatusBadRequest, envelopeInvalidReq)
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
		h.log.Error().Err(err).Str("model", model).Msg("proxy: building upstream request failed")
		writeEnvelope(w, http.StatusBadGateway, envelopeUpUnreach)
		return
	}
	copyForwardHeaders(req.Header, r.Header)

	resp, err := h.client.Do(req)
	if err != nil {
		h.log.Error().Err(err).Str("model", model).Str("upstream", upstream.String()).Msg("proxy: upstream request failed")
		writeEnvelope(w, http.StatusBadGateway, envelopeUpUnreach)
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
		copyRelayHeaders(w.Header(), resp.Header)
		w.WriteHeader(resp.StatusCode)
		_, _ = io.Copy(w, resp.Body)
		return
	}

	if stream && strings.Contains(strings.ToLower(resp.Header.Get(contentTypeHeader)), eventStreamType) {
		// Incremental SSE passthrough. The 2xx status is committed here; any
		// subsequent failure only truncates the stream, never switches the
		// response to an error body.
		copyRelayHeaders(w.Header(), resp.Header)
		w.WriteHeader(resp.StatusCode)
		if err := CopySSE(w, resp.Body, m.Public, flusher(w)); err != nil {
			h.log.Error().Err(err).Str("model", model).Msg("proxy: SSE passthrough truncated")
		}
		return
	}

	// Buffered non-stream path (client stream=false, or upstream ignored the
	// stream flag): read the whole body, validate it, rewrite the model, and
	// emit a single buffered response. The read is bounded — an over-cap
	// body is treated like any other unparseable upstream answer.
	upstreamBody, err := io.ReadAll(io.LimitReader(resp.Body, maxBufferedResponseBytes+1))
	if err != nil || len(upstreamBody) > int(maxBufferedResponseBytes) || !json.Valid(upstreamBody) {
		writeEnvelope(w, http.StatusBadGateway, envelopeUpInvalid)
		return
	}
	rewritten := inject.RewriteModel(upstreamBody, m.Public)
	copyRelayHeaders(w.Header(), resp.Header)
	w.WriteHeader(resp.StatusCode)
	_, _ = w.Write(rewritten)
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
