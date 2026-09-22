package proxy

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	mrand "math/rand/v2"
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
	// The two 401s are static for the same reason: nothing from the client's
	// Authorization header — presented or configured — is ever interpolated
	// into an error body.
	envelopeAuthMissing = `{"error":{"message":"you must provide an API key in the Authorization header (Bearer <key>)","type":"invalid_request_error","param":null,"code":null}}`
	envelopeAuthInvalid = `{"error":{"message":"invalid API key","type":"invalid_request_error","param":null,"code":"invalid_api_key"}}`
)

// client headers forwarded upstream. Every other client header is dropped
// deliberately, Authorization included: the client's credential
// authenticates it to this proxy only — it is consumed by the auth gate and
// never forwarded, and nothing is injected in its place (upstreams are
// trusted/internal).
var forwardHeaderNames = []string{"Content-Type", "Accept", "OpenAI-Beta"}

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

// sseProgressEvery is how many dispatched events separate two DEBUG
// stream_event_progress heartbeats.
const sseProgressEvery = 256

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
	// The catch-all keeps the same promise for unknown paths — trailing
	// slashes, wrong case, anything unmatched: an OpenAI SDK client always
	// gets a parseable JSON error body, never the mux's plain text.
	mux.HandleFunc("/", h.notFound)
	return mux
}

type injectorHandler struct {
	store  *config.Store
	client *http.Client
	log    zerolog.Logger
}

func (h *injectorHandler) healthz(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		// Outside the request lifecycle: nothing to account if the write
		// fails.
		_ = writeEnvelope(w, http.StatusMethodNotAllowed, envelopeBadMethod)
		return
	}
	w.Header().Set(contentTypeHeader, "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = io.WriteString(w, "ok\n")
}

func (h *injectorHandler) chatCompletions(w http.ResponseWriter, r *http.Request) {
	h.serve(w, r, "chat", inject.Chat, inject.RewriteChatModel, inject.SynthesizeChatThinkingUsage, "/chat/completions")
}

func (h *injectorHandler) responses(w http.ResponseWriter, r *http.Request) {
	h.serve(w, r, "responses", inject.Responses, inject.RewriteResponsesModel, inject.SynthesizeResponsesThinkingUsage, "/responses")
}

// notFound is the catch-all for paths no route matched. The interpolated
// method and path are client-supplied and go to the client only — they never
// reach logs, so the no-echo rule is not at stake here.
func (h *injectorHandler) notFound(w http.ResponseWriter, r *http.Request) {
	env := openAIError{Error: openAIErrorBody{
		Message: "Invalid URL (" + r.Method + " " + r.URL.Path + ")",
		Type:    "invalid_request_error",
	}}
	body, err := marshalEnvelopeJSON(env)
	// Outside the request lifecycle: no outcome to keep honest on a failed
	// write.
	_ = writeEnvelopeErr(w, http.StatusNotFound, body, err)
}

type transformFunc func(body []byte, m config.Model) ([]byte, error)

// rewriteFunc is the API-scoped response rewrite (RewriteChatModel or
// RewriteResponsesModel): the chat surface owns only the top-level model,
// the Responses surface also owns response.model in envelope events. Both
// scopes obey the same byte-preserving acceptance rule.
type rewriteFunc func(body []byte, public string) []byte

// synthesizeFunc is the API-scoped thinking-usage synthesizer
// (SynthesizeChatThinkingUsage or SynthesizeResponsesThinkingUsage), the
// response paths' second, optional transform: when the per-request plan is
// active it splices a reasoning_tokens count into existing usage objects,
// with the same API-scope split and byte-preserving discipline as
// rewriteFunc.
type synthesizeFunc func(body []byte, plan inject.ThinkingPlan) []byte

// thinkingDraw is the share draw the plan resolver consults for ranged
// ratios — a package var so tests can pin the draw-once contract without
// reaching into math/rand/v2's process-global state.
var thinkingDraw = mrand.Float64

// thinkingModeName renders a thinking-usage mode for log metadata.
func thinkingModeName(mode config.ThinkingMode) string {
	switch mode {
	case config.ThinkingAuto:
		return "auto"
	case config.ThinkingAlways:
		return "always"
	default:
		return "off"
	}
}

// serve runs the full injector flow for one request. One snapshot is loaded
// at entry and every later step (authentication, resolution, transformation,
// forwarding, trailing rewrite) binds to it: the request is authenticated
// against that snapshot's API key before its body is read, so a rotated key
// applies to subsequent requests only and an unauthenticated request never
// pins memory or reaches the upstream.
//
// Logging rides the same flow: the DEBUG lifecycle chain (request_received,
// probe_completed, model_resolved, thinking_usage_resolved when the model
// configures the feature, request_transform_started/completed,
// upstream_request_started, upstream_response_received,
// response_transform_started/completed, client_write_completed — and for
// streams stream_started, periodic stream_event_progress, stream_completed),
// one INFO request_completed per request with the wire facts (status,
// outcome, duration, byte counts, snapshot generation), and WARN-level
// failures split by phase. Metadata only — bodies, prompts, payloads, and
// Authorization never enter any log event at any level.
func (h *injectorHandler) serve(w http.ResponseWriter, r *http.Request, api string, transform transformFunc, rewrite rewriteFunc, synthesize synthesizeFunc, suffix string) {
	start := time.Now()
	if r.Method != http.MethodPost {
		// Outside the request lifecycle: no snapshot is loaded and no
		// generation exists to bind, and a wrong method is a client bug
		// rather than proxy traffic — debug is the honest level.
		h.log.Debug().Str("api", api).Str("method", r.Method).
			Str("path", r.URL.Path).Str("remote_addr", r.RemoteAddr).
			Msg("request_method_not_allowed")
		// Outside the request lifecycle — nothing below can observe or
		// report a failed write, so the error has nowhere to land.
		_ = writeEnvelope(w, http.StatusMethodNotAllowed, envelopeBadMethod)
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

	// reject writes a locally generated error envelope and completes the
	// request. A failed write means the envelope never reached its
	// recipient — the connection is gone — so the outcome becomes the
	// disconnect rather than the classification, however certain the local
	// decision was. The envelope's own cause still shows in the status and,
	// on the failure, in the WARN.
	reject := func(status int, b []byte, err error) {
		if werr := writeEnvelopeErr(sw, status, b, err); werr != nil {
			outcome = "client_disconnected"
			log.Warn().Err(werr).Str("public_model", publicModel).
				Int64("bytes_out", sw.bytes).Msg("client_write_failed")
		}
		complete()
	}

	// Client authentication, before any body is read. The presented bearer
	// token is compared against the snapshot's key in constant time; a
	// missing or malformed Authorization header and a wrong key share the
	// static-envelope discipline — no fragment of the presented credential
	// is ever echoed, and the configured key never reaches logs.
	token, ok := bearerToken(r.Header.Get("Authorization"))
	if !ok || !keyMatches(token, snap.APIKey()) {
		outcome = "unauthorized"
		env := envelopeAuthInvalid
		if !ok {
			env = envelopeAuthMissing
		}
		reject(http.StatusUnauthorized, []byte(env), nil)
		return
	}

	// Bound the request body before reading it: without a cap, a single
	// oversized client request pins unbounded memory in the proxy.
	r.Body = http.MaxBytesReader(sw, r.Body, maxRequestBodyBytes)
	body, err := io.ReadAll(r.Body)
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			outcome = "body_too_large"
			reject(http.StatusRequestEntityTooLarge, []byte(envelopeTooLarge), nil)
			return
		}
		outcome = "body_read_error"
		reject(http.StatusBadRequest, []byte(envelopeInvalidReq), nil)
		return
	}
	bytesIn = int64(len(body))

	model, stream, err := inject.Probe(body)
	if err != nil {
		outcome = "invalid_json"
		reject(http.StatusBadRequest, []byte(envelopeInvalidReq), nil)
		return
	}
	publicModel = model
	if model == "" {
		outcome = "missing_model"
		reject(http.StatusBadRequest, []byte(envelopeMissingMod), nil)
		return
	}
	log.Debug().Str("public_model", model).Bool("stream", stream).Msg("probe_completed")

	m, ok := snap.Model(model)
	if !ok {
		outcome = "model_not_found"
		body, merr := modelNotFoundEnvelope(model)
		reject(http.StatusNotFound, body, merr)
		return
	}
	log.Debug().Str("public_model", m.Public).Str("upstream_model", m.UpstreamModel).
		Str("upstream", origin(m.Endpoint)).Msg("model_resolved")

	// The thinking-usage plan is resolved once, here, from the same snapshot
	// and the same request body — before any upstream I/O — so every usage
	// object this request returns (buffered, or any chunk of the stream)
	// reports the same share, and a config reload mid-request cannot change
	// what applies. Metadata only in the event: mode, the request's signal,
	// the decision. The intent re-read is guarded so a deployment with the
	// feature configured but debug off never pays a second body parse.
	plan := inject.ThinkingPlanFor(m.ThinkingUsage, body, thinkingDraw)
	if m.ThinkingUsage.Mode != config.ThinkingOff {
		if event := log.Debug(); event.Enabled() {
			event.Str("mode", thinkingModeName(m.ThinkingUsage.Mode)).
				Bool("intent", inject.ThinkingIntent(body)).
				Bool("active", plan.Active).
				Msg("thinking_usage_resolved")
		}
	}
	// rewriteOut is the single response rewriter both response paths share —
	// parity by construction: the model rewrite, then, when the plan is
	// active, the usage synthesis. Under an inactive plan it is exactly
	// today's model-rewrite closure, byte for byte.
	rewriteOut := func(payload []byte) []byte {
		out := rewrite(payload, m.Public)
		if plan.Active {
			out = synthesize(out, plan)
		}
		return out
	}

	log.Debug().Int64("bytes_in", bytesIn).Msg("request_transform_started")
	out, err := transform(body, m)
	if err != nil {
		outcome = "transform_error"
		reject(http.StatusBadRequest, []byte(envelopeInvalidReq), nil)
		return
	}
	log.Debug().Int64("bytes_out", int64(len(out))).Msg("request_transform_completed")

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
		reject(http.StatusBadGateway, []byte(envelopeUpUnreach), nil)
		return
	}
	copyForwardHeaders(req.Header, r.Header)
	log.Debug().Str("upstream", origin(&upstream)).
		Int64("bytes_out", int64(len(out))).Msg("upstream_request_started")

	resp, err := h.client.Do(req)
	if err == nil {
		log.Debug().Int("status", resp.StatusCode).
			Str("content_type", resp.Header.Get(contentTypeHeader)).
			Msg("upstream_response_received")
	}
	if err != nil {
		// The *url.Error from client.Do embeds the full request URL —
		// query string included, which is how query-authenticated
		// providers leak credentials. Log the sanitized error and the
		// scheme+host origin only, per the credential rule.
		event := log.Error()
		if errors.Is(err, context.Canceled) {
			// The client went away before the upstream answered. The
			// outcome is the disconnect — an upstream_unreachable 502
			// would misreport a client-side event as an upstream
			// failure — and there is no response left to write.
			event = log.Warn()
			outcome = "client_disconnected"
			event.Err(sanitizeUpstreamError(err, &upstream)).
				Str("public_model", model).
				Str("upstream", origin(&upstream)).
				Str("error_class", upstreamErrorClass(err)).
				Msg("upstream_request_failed")
			complete()
			return
		}
		event.Err(sanitizeUpstreamError(err, &upstream)).
			Str("public_model", model).
			Str("upstream", origin(&upstream)).
			Str("error_class", upstreamErrorClass(err)).
			Msg("upstream_request_failed")
		outcome = "upstream_unreachable"
		reject(http.StatusBadGateway, []byte(envelopeUpUnreach), nil)
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
		if _, err := copyVerbatim(sw, resp.Body); err != nil {
			// The relay did not finish — the outcome says so. A failure on
			// the client side (write error, or the canceled request context
			// surfacing through the upstream read) is a disconnect; anything
			// else died reading the upstream.
			level, phase := log.Error(), "upstream_read"
			if clientSide(err) {
				level, phase, outcome = log.Warn(), "client_write", "client_disconnected"
			} else {
				outcome = "upstream_read_failed"
			}
			level.Err(err).Str("public_model", model).Str("phase", phase).
				Msg("relay_copy_failed")
			complete()
			return
		}
		outcome = "relayed"
		log.Debug().Int64("bytes_out", sw.bytes).Msg("client_write_completed")
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
		// Progress rides the flush: CopySSE invokes it exactly once per
		// dispatched event, so the wrapper counts events for free and emits
		// a periodic DEBUG heartbeat — a stuck stream shows up as a heartbeat
		// that stops advancing. Counts only, never event payloads.
		flush := flusher(sw)
		events := 0
		stats, err := CopySSE(sw, resp.Body, rewriteOut, func() {
			events++
			if events%sseProgressEvery == 0 {
				log.Debug().Str("public_model", model).
					Int("events", events).Int64("bytes_out", sw.bytes).
					Msg("stream_event_progress")
			}
			flush()
		})
		if err != nil {
			phase := "upstream_read"
			outcome = "stream_truncated"
			var swe *streamWriteError
			switch {
			case errors.As(err, &swe), clientSide(err):
				// The client connection broke mid-stream — as a failed
				// write (streamWriteError), or as the canceled request
				// context surfacing through the next upstream read — and
				// the upstream may have been fine. The outcome names the
				// disconnect, the same accounting the buffered and verbatim
				// paths apply, while the WARN keeps the truncation's phase.
				phase = "client_write"
				outcome = "client_disconnected"
			case errors.Is(err, ErrSSELineTooLong) || errors.Is(err, ErrSSEEventTooLarge):
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
	// body is treated like any other unparseable upstream answer, while a
	// read that fails mid-body is the upstream dying mid-answer and gets
	// its own outcome; the client-visible 502 envelope is the same either
	// way.
	upstreamBody, err := io.ReadAll(io.LimitReader(resp.Body, maxBufferedResponseBytes+1))
	if err != nil {
		if clientSide(err) {
			// The canceled request context surfaced through the upstream
			// read: the client is gone mid-answer and the upstream may be
			// fine. No envelope write is attempted — nobody is left to
			// receive it — and the WARN keeps the relay's phase vocabulary
			// instead of blaming the upstream.
			outcome = "client_disconnected"
			log.Warn().Err(err).Str("public_model", model).
				Str("phase", "client_write").Msg("relay_copy_failed")
			complete()
			return
		}
		outcome = "upstream_read_failed"
		log.Warn().Err(err).Str("public_model", model).Msg("upstream_body_read_failed")
		reject(http.StatusBadGateway, []byte(envelopeUpInvalid), nil)
		return
	}
	if len(upstreamBody) > int(maxBufferedResponseBytes) || !json.Valid(upstreamBody) {
		outcome = "upstream_invalid_response"
		reject(http.StatusBadGateway, []byte(envelopeUpInvalid), nil)
		return
	}
	log.Debug().Int64("bytes_in", int64(len(upstreamBody))).Msg("response_transform_started")
	rewritten := rewriteOut(upstreamBody)
	log.Debug().Int64("bytes_out", int64(len(rewritten))).Msg("response_transform_completed")
	copyRelayHeaders(sw.Header(), resp.Header)
	sw.WriteHeader(resp.StatusCode)
	if _, err := sw.Write(rewritten); err != nil {
		// The status committed and the rewrite is done; the client went
		// away before the body could land. A disconnect, not a completion.
		outcome = "client_disconnected"
		log.Warn().Err(err).Str("public_model", model).
			Int64("bytes_out", sw.bytes).Msg("client_write_failed")
		complete()
		return
	}
	log.Debug().Int64("bytes_out", sw.bytes).Msg("client_write_completed")
	complete()
}

// modelNotFoundEnvelope builds the 404 envelope whose message interpolates
// the requested model, byte-exact per the documented shape.
func modelNotFoundEnvelope(model string) ([]byte, error) {
	env := openAIError{Error: openAIErrorBody{
		Message: "The model '" + model + "' does not exist or you do not have access to it.",
		Type:    "invalid_request_error",
		Code:    "model_not_found",
	}}
	return marshalEnvelopeJSON(env)
}

// marshalEnvelopeJSON marshals an error envelope without HTML escaping:
// json.Marshal would turn the < > & characters in interpolated values (the
// requested model, the request path) into their unicode escape sequences
// (e.g. "<" becomes six bytes, not one), breaking the byte-exact envelope
// contract for names the configuration is free to use.
func marshalEnvelopeJSON(v any) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	// json.Encoder appends a newline json.Marshal would not have written.
	return bytes.TrimSuffix(buf.Bytes(), []byte("\n")), nil
}

// origin renders the endpoint's scheme+host — the only upstream URL detail
// that may ever reach logs. Query strings, paths, and userinfo are
// credential- or traffic-bearing and stay out.
func origin(u *url.URL) string {
	return u.Scheme + "://" + u.Host
}

// sanitizeUpstreamError rebuilds a client.Do error without the full request
// URL: *url.Error.Error() quotes it verbatim, query string included. The
// nested url parse/escape errors quote raw bytes too (the offending escape
// sequence, the rejected host), so they are replaced with static text under
// the same no-echo rule.
func sanitizeUpstreamError(err error, endpoint *url.URL) error {
	var ue *url.Error
	if errors.As(err, &ue) {
		inner := ue.Err
		var ee url.EscapeError
		if errors.As(inner, &ee) {
			inner = errors.New("invalid URL escape")
		}
		var he url.InvalidHostError
		if errors.As(inner, &he) {
			inner = errors.New("invalid host")
		}
		return &url.Error{Op: ue.Op, URL: origin(endpoint), Err: inner}
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

// writeEnvelope writes a locally generated error envelope and reports the
// write outcome: a client that disconnected before the envelope landed is
// the caller's signal to account the request as a disconnect instead of the
// envelope's cause. The ignored-error call sites (healthz, the catch-all)
// sit outside the request lifecycle and have no outcome to keep honest.
func writeEnvelope(w http.ResponseWriter, status int, body string) error {
	w.Header().Set(contentTypeHeader, envelopeJSONType)
	w.WriteHeader(status)
	_, err := io.WriteString(w, body)
	return err
}

// writeEnvelopeErr writes a marshaled envelope, falling back to a static
// body if marshaling somehow fails.
func writeEnvelopeErr(w http.ResponseWriter, status int, b []byte, err error) error {
	if err != nil {
		return writeEnvelope(w, status, envelopeInvalidReq)
	}
	w.Header().Set(contentTypeHeader, envelopeJSONType)
	w.WriteHeader(status)
	_, werr := w.Write(b)
	return werr
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

// bearerToken extracts the bearer credential from an Authorization header
// value. The scheme match is case-insensitive per RFC 9110 (clients send
// "Bearer" and "bearer" alike); the token is used as trimmed. A missing
// header, a non-bearer scheme, or an empty token is reported as ok=false and
// answered like a missing header. Nothing about the header's text is echoed
// or logged.
func bearerToken(header string) (string, bool) {
	scheme, rest, _ := strings.Cut(header, " ")
	if !strings.EqualFold(scheme, "bearer") {
		return "", false
	}
	token := strings.TrimSpace(rest)
	if token == "" {
		return "", false
	}
	return token, true
}

// keyMatches compares a presented bearer token against the configured API
// key in constant time. Both sides are hashed first: a bare bytes comparison
// is only constant-time for equal lengths and would leak the configured
// key's length through timing on mismatches; digests are always the same
// length, so the comparison is constant-time for any input.
func keyMatches(presented, configured string) bool {
	if configured == "" {
		// No snapshot carries an empty key (LoadRuntime rejects it); fail
		// closed anyway rather than ever match an empty presentation.
		return false
	}
	presentedSum := sha256.Sum256([]byte(presented))
	configuredSum := sha256.Sum256([]byte(configured))
	return subtle.ConstantTimeCompare(presentedSum[:], configuredSum[:]) == 1
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

// clientWriteError marks a relay failure that happened writing to the
// client — the connection broke mid-body — as opposed to a failure reading
// from upstream. io.Copy flattens the two sides into one error; the access
// log must not report a vanished client as an upstream failure or the
// reverse.
type clientWriteError struct{ err error }

func (e *clientWriteError) Error() string {
	return "writing response to client: " + e.err.Error()
}
func (e *clientWriteError) Unwrap() error { return e.err }

// clientSide reports whether a relay error happened on the client side: an
// explicit write failure, or the request context surfacing canceled through
// the upstream read — the context cancels when the client goes away, never
// on an upstream hiccup.
func clientSide(err error) bool {
	var cwe *clientWriteError
	if errors.As(err, &cwe) {
		return true
	}
	return errors.Is(err, context.Canceled)
}

// copyVerbatim relays src to dst byte for byte with the same io.Copy
// semantics (n bytes from a failed Read are relayed before the error), but
// with every dst failure marked client-side — including a short write with
// a nil error, which is io.ErrShortWrite. The explicit loop is deliberate:
// io.Copy's WriterTo/ReaderFrom fast paths would bypass the tagging writer.
func copyVerbatim(dst io.Writer, src io.Reader) (int64, error) {
	var written int64
	buf := make([]byte, 32<<10)
	for {
		n, rerr := src.Read(buf)
		if n > 0 {
			wn, werr := dst.Write(buf[:n])
			written += int64(wn)
			switch {
			case werr != nil:
				return written, &clientWriteError{err: werr}
			case wn < n:
				return written, &clientWriteError{err: io.ErrShortWrite}
			}
		}
		if rerr != nil {
			if errors.Is(rerr, io.EOF) {
				return written, nil
			}
			return written, rerr
		}
	}
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
