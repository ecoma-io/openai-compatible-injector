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
	mrand "math/rand/v2"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"syscall"
	"time"

	"github.com/rs/zerolog"

	"openai-compatible-injector/internal/auth"
	"openai-compatible-injector/internal/config"
	"openai-compatible-injector/internal/inject"
	"openai-compatible-injector/internal/transport"
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
// store once and binds the entire request lifetime to that snapshot — its
// outbound execution included: the request resolves its provider's
// transport through the resolver and holds the returned Doer for its whole
// lifetime, so a reload never swaps the path under in-flight work.
//
// authProvider selects the credential model: nil keeps static mode (the
// snapshot's own api-key authenticates every client); a non-nil provider —
// the partner key store — resolves per-caller identities instead, and the
// snapshot's api-key is not honored on the wire.
func NewHandler(store *config.Store, doers transport.Resolver, authProvider auth.Provider, log zerolog.Logger) http.Handler {
	provider := auth.Provider(auth.StaticProvider{})
	if authProvider != nil {
		provider = authProvider
	}
	h := &injectorHandler{store: store, doers: doers, auth: provider, log: log}
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
	store *config.Store
	doers transport.Resolver
	auth  auth.Provider
	log   zerolog.Logger
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
		// egress carries the pool's attempt report of the LAST provider
		// candidate this request executed through (nil when none of them
		// routed through a pool): how many distinct endpoints were actually
		// dialed, the kind and scheme+host of the last one, and whether the
		// loop ended without dialing anything. The target is log-safe by
		// construction — scheme+host only, the same surface origin() allows;
		// userinfo never enters AttemptInfo.
		egress *transport.AttemptInfo
		// providerAttempts/finalProvider report the provider walk: how many
		// chain candidates were tried and the identity (providers-table
		// name, or endpoint origin) of the last one — the candidate that
		// answered, or the last one that failed. providerExhausted marks
		// the walk ending with no candidate answering.
		providerAttempts  int
		finalProvider     string
		providerExhausted bool
	)
	withEgress := func(ev *zerolog.Event) *zerolog.Event {
		if egress == nil {
			return ev
		}
		return ev.Int("egress_attempts", egress.Attempts).
			Str("egress_kind", egress.Kind).
			Str("egress_target", egress.Target).
			Bool("egress_exhausted", egress.Exhausted)
	}
	withProviders := func(ev *zerolog.Event) *zerolog.Event {
		if providerAttempts == 0 {
			return ev
		}
		ev = ev.Int("provider_attempts", providerAttempts).
			Str("final_provider", finalProvider)
		if providerExhausted {
			ev = ev.Bool("provider_exhausted", true)
		}
		return ev
	}
	complete := func() {
		withProviders(withEgress(log.Info())).
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
	// token is resolved through the request's authenticator — the snapshot
	// in static mode, the partner key store in partner mode. A missing or
	// malformed Authorization header, a wrong key, an unknown or revoked
	// partner key, and even an unreachable credential store all share the
	// static-envelope discipline: the same two 401 bodies as ever, no
	// fragment of the presented credential ever echoed, and — the
	// fail-closed shape — a store outage denies the request instead of
	// letting it through, with no upstream I/O either way.
	token, ok := bearerToken(r.Header.Get("Authorization"))
	if !ok {
		outcome = "unauthorized"
		reject(http.StatusUnauthorized, []byte(envelopeAuthMissing), nil)
		return
	}
	principal, reason, aerr := h.auth.For(snap).Authenticate(r.Context(), token)
	if aerr != nil {
		// Backend failure: infrastructure, not a key judgment. Logged for
		// operators via its class alone — the driver's error text can
		// embed infrastructure detail, so it never rides the event.
		log.Warn().Str("error_class", auth.StoreErrorClass(aerr)).
			Msg("auth_backend_failed")
	}
	if reason != auth.ReasonOK {
		outcome = "unauthorized"
		reject(http.StatusUnauthorized, []byte(envelopeAuthInvalid), nil)
		return
	}
	if principal.PartnerID != "" || principal.KeyID != "" {
		log = log.With().Str("partner_id", principal.PartnerID).
			Str("key_id", principal.KeyID).Logger()
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
		Str("upstream", origin(m.Endpoint)).
		Int("provider_count", len(m.Chain)).Msg("model_resolved")

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

	// The provider walk. The model's candidate chain is tried primary
	// first under the snapshot's provider-fallback policy. Each attempt
	// replays the same immutable client body through the candidate's own
	// transform (its upstream model name and endpoint differ — replay
	// safety: nothing observed on an earlier attempt feeds the next), and
	// every candidate's response — any status — ends the walk. Only
	// transport-level failures (dial, TLS, proxy, egress exhaustion) move
	// to the next candidate: a 429 or a 500 is the upstream's answer, and
	// relaying it is the contract. Nothing is retried after the client is
	// committed, either: the walk happens entirely before the first
	// response byte, so streaming commitment holds by construction — the
	// candidate that produces headers has produced THE response.
	//
	// Local validation never falls back: a body that fails one candidate's
	// transform fails every candidate's transform (the transform sees only
	// the body and model mapping, never the network), so the 400 is
	// answered on the first attempt.
	policy := snap.ProviderFallback()
	budget := 1
	if policy.Enabled {
		budget = policy.MaxAttempts
	}
	if budget > len(m.Chain) {
		budget = len(m.Chain)
	}

	var (
		resp *http.Response
		// finalCand is the candidate that produced the response. On
		// exhaustion it stays zero and the last-attempt fields below carry
		// the failure report.
		finalCand config.Candidate
		// lastUpstream is the request URL of the most recent attempt — the
		// answering candidate's URL on success, the last failed one's on
		// exhaustion. Log-safe surfaces only (origin()).
		lastUpstream *url.URL
		// lastUerr is the most recent transport failure; the exhaustion
		// report after the walk carries it.
		lastUerr error
	)
	for i := range m.Chain {
		if providerAttempts >= budget {
			break
		}
		cand := m.Chain[i]
		providerAttempts++
		finalProvider = cand.Label()
		egress = nil

		// The candidate view: same public model, same injection prompt,
		// same thinking plan — only the upstream identity changes. m is a
		// per-request value copy (Snapshot.Model returns a value), so
		// mutating it cannot touch the snapshot.
		m.Provider = cand.Provider
		m.Endpoint = cand.Endpoint
		m.UpstreamModel = cand.UpstreamModel
		m.Transport = cand.Transport

		out, terr := transform(body, m)
		if terr != nil {
			outcome = "transform_error"
			reject(http.StatusBadRequest, []byte(envelopeInvalidReq), nil)
			return
		}
		log.Debug().Int64("bytes_out", int64(len(out))).Msg("request_transform_completed")

		upstream := *cand.Endpoint
		// Trim every trailing slash ("trailing slashes ignored" holds for any
		// number) and clear RawPath: mutating Path can leave a RawPath that no
		// longer matches, which makes EscapedPath silently percent-decode the
		// endpoint path. Encoded endpoint paths are normalized, not preserved.
		upstream.Path = strings.TrimRight(upstream.Path, "/") + suffix
		upstream.RawPath = ""

		req, rerr := http.NewRequestWithContext(r.Context(), http.MethodPost, upstream.String(), bytes.NewReader(out))
		if rerr != nil {
			// Unreachable by construction (the endpoint was validated to a
			// *url.URL at config load), but if it ever fires the raw error
			// text must still not reach logs — it would embed the full URL.
			log.Error().Str("model", model).
				Str("upstream", origin(&upstream)).
				Str("error_class", "request_build").
				Msg("upstream_request_build_failed")
			outcome = "upstream_unreachable"
			reject(http.StatusBadGateway, []byte(envelopeUpUnreach), nil)
			return
		}
		copyForwardHeaders(req.Header, r.Header)
		log.Debug().Str("provider", cand.Label()).
			Str("upstream", origin(&upstream)).
			Int64("bytes_out", int64(len(out))).Msg("upstream_request_started")
		lastUpstream = &upstream

		// The outbound hop: the candidate's provider transport, resolved
		// from the request's snapshot. Any HTTP status — 429, 5xx, an
		// unexpected 3xx — is the upstream's answer and returns as a
		// response; only transport-level failures (dial, TLS, cancellation
		// before headers) return an error, classified below.
		//
		// A pool transport owns the egress attempt loop past this point:
		// the handler hands it the request facts its eligibility gates need
		// (the outgoing body, the client-declared stream flag) and the pool
		// returns one response or one error — selection, bounded fallback,
		// and exhaustion are its business, and no egress retry exists after
		// it returns. The single-endpoint path is exactly the historical
		// Do(req).
		var uerr error
		d := h.doers.Doer(cand.Transport)
		if ex, ok := d.(transport.Executor); ok {
			var info transport.AttemptInfo
			resp, info, uerr = ex.Execute(&transport.AttemptRequest{
				Ctx:       r.Context(),
				Method:    http.MethodPost,
				URL:       &upstream,
				Header:    req.Header.Clone(),
				Body:      out,
				Streaming: stream,
			})
			egress = &info
			// Per-attempt evidence, bounded by the fallback budget: one WARN per
			// dialed-and-failed endpoint, correlated by this request's request_id.
			// Typed class and scheme+host only — the error text, any credential
			// material, and skipped members (no dial, no event) stay out.
			for j, f := range info.Failures {
				log.Warn().Str("public_model", model).
					Str("provider", cand.Label()).
					Str("egress_kind", f.Kind).Str("egress_target", f.Target).
					Str("error_class", f.Class).
					Int("attempt", j+1).
					Msg("egress_attempt_failed")
			}
		} else {
			resp, uerr = d.Do(req)
		}
		if uerr == nil {
			finalCand = cand
			withEgress(log.Debug()).Int("status", resp.StatusCode).
				Str("content_type", resp.Header.Get(contentTypeHeader)).
				Msg("upstream_response_received")
			break
		}
		lastUerr = uerr
		if errors.Is(uerr, context.Canceled) {
			// The client went away before any upstream answered. No
			// fallback — there is nobody left to answer — and the outcome
			// is the disconnect: an upstream_unreachable 502 would
			// misreport a client-side event as an upstream failure.
			outcome = "client_disconnected"
			withEgress(log.Warn()).Err(sanitizeUpstreamError(uerr, &upstream)).
				Str("public_model", model).
				Str("provider", cand.Label()).
				Str("upstream", origin(&upstream)).
				Str("error_class", upstreamErrorClass(uerr)).
				Msg("upstream_request_failed")
			complete()
			return
		}
		// Transport-level failure with the client still present: one WARN
		// per failed candidate — the *url.Error from client.Do embeds the
		// full request URL, query string included, which is how
		// query-authenticated providers leak credentials, so the
		// sanitized error and the scheme+host origin only — then, policy
		// permitting, the next candidate. Exhaustion is reported after
		// the walk.
		withEgress(log.Warn()).Err(sanitizeUpstreamError(uerr, &upstream)).
			Str("public_model", model).
			Str("provider", cand.Label()).
			Str("upstream", origin(&upstream)).
			Str("error_class", upstreamErrorClass(uerr)).
			Int("provider_attempt", providerAttempts).
			Msg("provider_attempt_failed")
	}

	if resp == nil {
		// Every budgeted candidate failed without answering. The client
		// gets the canonical unreachable envelope; the ERROR carries the
		// last attempt's failure, sanitized, with the pool's report when
		// that candidate routed through one.
		providerExhausted = true
		class := upstreamErrorClass(lastUerr)
		if egress != nil && egress.Exhausted {
			// Zero dials is a pool-level condition — no member was reachable
			// for this request — and gets its own class token; the error text
			// is the pool's static sentinel.
			class = "egress_exhausted"
		}
		withProviders(withEgress(log.Error())).Err(sanitizeUpstreamError(lastUerr, lastUpstream)).
			Str("public_model", model).
			Str("provider", finalProvider).
			Str("upstream", origin(lastUpstream)).
			Str("error_class", class).
			Bool("provider_exhausted", true).
			Msg("upstream_request_failed")
		outcome = "upstream_unreachable"
		reject(http.StatusBadGateway, []byte(envelopeUpUnreach), nil)
		return
	}
	defer func() { _ = resp.Body.Close() }()

	if isUpstreamHTTPError(resp.StatusCode) {
		// Upstream HTTP errors are normalized, never relayed raw: the
		// provider's status survives (a 429 answers 429 — collapsing it into
		// a 502 would fog the root cause the same way it would for a 3xx),
		// but the client body is always the canonical JSON envelope. HTML,
		// text, or provider JSON — none of it passes through: an error body
		// can echo request material and break SDK error decoding alike.
		ev, err := captureUpstreamErrorEvidence(resp)
		if err != nil {
			// The same body-read vocabulary the buffered path applies: a
			// read that died on the client side is a disconnect — no
			// envelope is attempted for a connection that is already gone —
			// and anything else is the upstream dying mid-answer, answered
			// with the synthetic 502 rather than half a provider body.
			if clientSide(err) {
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
		// Structured evidence at the phase split the transport failures
		// already use: a 4xx is the provider answering (WARN), a 5xx is the
		// provider failing (ERROR). Metadata only — the bounded body was
		// reduced to shape + fingerprint inside the capture, the provider's
		// own message never leaves it, and the endpoint appears as its
		// scheme+host origin.
		event := log.Warn()
		if ev.status >= http.StatusInternalServerError {
			event = log.Error()
		}
		event = event.Str("public_model", model).Str("upstream_model", finalCand.UpstreamModel).
			Str("upstream", origin(lastUpstream)).
			Int("upstream_status", ev.status).
			Str("content_type", ev.contentType).
			Str("error_class", ev.class).
			Str("error_shape", ev.shape).
			Int64("body_bytes", ev.bodyBytes).
			Bool("body_truncated", ev.truncated).
			Str("error_fingerprint", ev.fingerprint)
		for _, f := range evidenceRateLimitFields {
			if v := ev.rateLimit[f.header]; v != "" {
				event = event.Str(f.field, v)
			}
		}
		if ev.providerType != "" {
			event = event.Str("provider_error_type", ev.providerType)
		}
		if ev.providerCode != "" {
			event = event.Str("provider_error_code", ev.providerCode)
		}
		event.Msg("upstream_http_error")

		// The client answer: the upstream's own status, the operational
		// headers from the relay allow-list, and a Content-Type that
		// describes the body the client actually receives.
		body, berr := ev.envelopeBytes()
		if berr != nil {
			// Unreachable for an all-string envelope, but the fallback must
			// still be a canonical body — never raw upstream bytes.
			body = []byte(envelopeUpInvalid)
		}
		copyRelayHeaders(sw.Header(), resp.Header)
		sw.Header().Set(contentTypeHeader, envelopeJSONType)
		sw.WriteHeader(ev.status)
		if _, werr := sw.Write(body); werr != nil {
			// The status committed and the envelope is canonical; a failed
			// write means the client went away — a disconnect, not an
			// upstream error.
			outcome = "client_disconnected"
			log.Warn().Err(werr).Str("public_model", model).
				Int64("bytes_out", sw.bytes).Msg("client_write_failed")
			complete()
			return
		}
		outcome = "upstream_http_error"
		log.Debug().Int64("bytes_out", sw.bytes).Msg("client_write_completed")
		complete()
		return
	}

	if resp.StatusCode >= http.StatusMultipleChoices || resp.StatusCode == http.StatusNoContent || resp.StatusCode == http.StatusNotModified {
		// Verbatim relay: redirects (3xx, which CheckRedirect never follows)
		// and the two body-less statuses, which are valid upstream answers.
		// A 204 or an unexpected 3xx is the upstream's answer; turning it
		// into a 502 would fog the root cause. Status and body relayed byte
		// for byte, whatever the content type. (4xx/5xx no longer reach this
		// branch — the normalized-error branch above owns them. A status
		// above 599, which no spec defines but a broken peer can emit, stays
		// here: it is not ours to reshape either.)
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
		// The keep-alive heartbeat binds to the request's snapshot like
		// everything else: an in-flight stream keeps the interval it
		// started with across a reload, and the goroutine is joined before
		// this handler returns. When the feature is off the relay writes to
		// the client writer directly and the path is byte-identical to an
		// unconfigured deployment.
		flush := flusher(sw)
		var heartbeat *pingWriter
		var dst io.Writer = sw
		afterEvent := flush
		if ka := snap.SSEKeepAlive(); ka.Enabled {
			heartbeat = newPingWriter(sw, flush, ka.Interval, time.Now())
			heartbeat.start()
			dst, afterEvent = heartbeat, heartbeat.Flush
		}
		// Progress rides the flush: CopySSE invokes it exactly once per
		// dispatched event, so the wrapper counts events for free and emits
		// a periodic DEBUG heartbeat — a stuck stream shows up as a heartbeat
		// that stops advancing. Counts only, never event payloads.
		events := 0
		stats, err := CopySSE(dst, resp.Body, rewriteOut, func() {
			events++
			if events%sseProgressEvery == 0 {
				log.Debug().Str("public_model", model).
					Int("events", events).Int64("bytes_out", sw.bytes).
					Msg("stream_event_progress")
			}
			afterEvent()
		})
		var pings int
		if heartbeat != nil {
			heartbeat.stopAndWait()
			pings = heartbeat.pingCount()
		}
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
				Int("keep_alive_pings", pings).
				Msg("stream_truncated")
		} else {
			log.Debug().Str("public_model", model).
				Int64("bytes_out", stats.Bytes).Int("events", stats.Events).
				Int("keep_alive_pings", pings).
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
// the same no-echo rule — and so is every nested error whose text is not
// known to be echo-free: the transport's response-parsing failures (a
// malformed MIME header line, a bad chunk size) quote the upstream's own
// bytes verbatim, and those reach no log line at any level (#37).
func sanitizeUpstreamError(err error, endpoint *url.URL) error {
	var ue *url.Error
	if errors.As(err, &ue) {
		inner := ue.Err
		var ee url.EscapeError
		var he url.InvalidHostError
		switch {
		case errors.As(inner, &ee):
			inner = errors.New("invalid URL escape")
		case errors.As(inner, &he):
			inner = errors.New("invalid host")
		case !transportErrorTextSafe(inner):
			inner = errors.New("upstream transport error")
		}
		return &url.Error{Op: ue.Op, URL: origin(endpoint), Err: inner}
	}
	return err
}

// transportErrorTextSafe reports whether an error's text is known to quote
// nothing the upstream sent. The allow-listed shapes carry only our side of
// the wire — addresses, syscall names, deadline markers — or upstream
// identity (the TLS certificate chain): cancellation, deadlines, plain EOF,
// net.Error timeouts, *net.OpError, TLS verification failures, and raw
// errnos. The transport package's typed proxy errors are static text by
// construction — the auth variants carry fixed messages, and a connect
// error's message names no upstream bytes (a socks5h resolve failure names
// the target host, the same scheme+host surface origin() allows) — though a
// connect error's cause must itself be safe. Everything else (notably every
// net/textproto and HTTP/2 parse failure, which interpolate the offending
// upstream bytes into their message) collapses to static text in
// sanitizeUpstreamError; the error_class token carries the classification
// either way.
func transportErrorTextSafe(err error) bool {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, os.ErrDeadlineExceeded) || errors.Is(err, io.EOF) ||
		errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	var pae *transport.ProxyAuthError
	if errors.As(err, &pae) {
		return true
	}
	var pce *transport.ProxyConnectError
	if errors.As(err, &pce) {
		cause := pce.Unwrap()
		return cause == nil || transportErrorTextSafe(cause)
	}
	var ne net.Error
	var oe *net.OpError
	var te *tls.CertificateVerificationError
	var errno syscall.Errno
	return errors.As(err, &ne) || errors.As(err, &oe) ||
		errors.As(err, &te) || errors.As(err, &errno)
}

// upstreamErrorClass buckets a client.Do error for logging. The error is
// classified in its raw form — sanitization only strips the URL text. The
// transport's typed proxy errors classify from their type: a proxy that
// demanded or refused authentication is proxy_auth; a failed connection to
// or through the proxy is proxy_connect.
func upstreamErrorClass(err error) string {
	if errors.Is(err, context.Canceled) {
		return "client_canceled"
	}
	var pae *transport.ProxyAuthError
	if errors.As(err, &pae) {
		return "proxy_auth"
	}
	var pce *transport.ProxyConnectError
	if errors.As(err, &pce) {
		return "proxy_connect"
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
// "Bearer" and "bearer" alike); outer spaces are tolerated, but a bearer
// credential cannot contain ASCII whitespace. A missing header,
// a non-bearer scheme, or a malformed/empty token is reported as ok=false and
// answered like a missing header. Nothing about the header's text is echoed
// or logged.
func bearerToken(header string) (string, bool) {
	scheme, rest, _ := strings.Cut(header, " ")
	if !strings.EqualFold(scheme, "bearer") {
		return "", false
	}
	token := strings.Trim(rest, " ")
	if !validBearerToken(token) {
		return "", false
	}
	return token, true
}

// maxBearerTokenBytes bounds the credential material held per request. The
// same cap on the config plane makes constant-time comparison practical.
const maxBearerTokenBytes = 4 << 10

// validBearerToken reports whether token is an RFC 6750 b64token. Its
// counterpart in config keeps accepted configured keys representable in a
// valid Authorization: Bearer header.
func validBearerToken(token string) bool {
	if token == "" || len(token) > maxBearerTokenBytes {
		return false
	}
	padding := false
	hasTokenChar := false
	for i := range len(token) {
		c := token[i]
		if c == '=' {
			if !hasTokenChar {
				return false
			}
			padding = true
			continue
		}
		if padding || !isBearerTokenChar(c) {
			return false
		}
		hasTokenChar = true
	}
	return true
}

func isBearerTokenChar(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
		(c >= '0' && c <= '9') || c == '-' || c == '.' || c == '_' ||
		c == '~' || c == '+' || c == '/'
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
