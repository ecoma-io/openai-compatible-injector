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
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/rs/zerolog"

	"openai-compatible-injector/internal/auth"
	"openai-compatible-injector/internal/config"
	"openai-compatible-injector/internal/credential"
	"openai-compatible-injector/internal/inject"
	"openai-compatible-injector/internal/recovery"
	"openai-compatible-injector/internal/transport"
	"openai-compatible-injector/internal/usage"
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
//
// meter selects the usage-metering model: nil keeps metering off (no
// events, byte-identical behavior); a non-nil pipeline records one factual
// event per request that reaches the provider path, written off the
// response path — Record never blocks and never fails a request.
//
// creds selects the upstream-credential model: nil keeps every candidate
// unauthenticated upstream (byte-identical behavior); a non-nil resolver
// hands each credential-bearing candidate its rotation pool, and the walk
// acquires one key per attempt, re-acquires on cooldown, and marks keys
// rate-limited on a 429 — see the credential seam at the attempt loop.
func NewHandler(store *config.Store, doers transport.Resolver, creds CredentialResolver, authProvider auth.Provider, meter usage.Ingest, log zerolog.Logger) http.Handler {
	provider := auth.Provider(auth.StaticProvider{})
	if authProvider != nil {
		provider = authProvider
	}
	h := &injectorHandler{store: store, doers: doers, creds: creds, auth: provider, meter: meter, log: log}
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", h.healthz)
	// The routes are method-agnostic patterns so that wrong methods reach
	// our handler and receive the JSON 405 envelope instead of the mux's
	// plain-text default.
	mux.HandleFunc("/v1/chat/completions", h.chatCompletions)
	mux.HandleFunc("/v1/responses", h.responses)
	mux.HandleFunc("/v1/models", h.models)
	// The catch-all keeps the same promise for unknown paths — trailing
	// slashes, wrong case, anything unmatched: an OpenAI SDK client always
	// gets a parseable JSON error body, never the mux's plain text.
	mux.HandleFunc("/", h.notFound)
	return mux
}

type injectorHandler struct {
	store *config.Store
	doers transport.Resolver
	creds CredentialResolver
	auth  auth.Provider
	meter usage.Ingest
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
	h.serve(w, r, "chat", inject.Chat, inject.RewriteChatModel, inject.SynthesizeChatThinkingUsage, inject.StripChatFields, "/chat/completions")
}

func (h *injectorHandler) responses(w http.ResponseWriter, r *http.Request) {
	h.serve(w, r, "responses", inject.Responses, inject.RewriteResponsesModel, inject.SynthesizeResponsesThinkingUsage, inject.StripResponsesFields, "/responses")
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

// stripFunc is the API-scoped response strip (StripChatFields or
// StripResponsesFields): the third, likewise optional transform that excises
// configured provider-added members after the rewrite and synthesis have
// run. It takes the config-provided segment lists — never the raw paths —
// so the request path never re-parses.
type stripFunc func(body []byte, paths [][]string) []byte

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
// provider_attempt_started, upstream_response_received,
// response_transform_started/completed, client_write_completed — and for
// streams stream_started, periodic stream_event_progress, stream_completed),
// one INFO request_completed per request with the wire facts (status,
// outcome, duration, byte counts, snapshot generation), and WARN-level
// failures split by phase. Metadata only — bodies, prompts, payloads, and
// Authorization never enter any log event at any level.
func (h *injectorHandler) serve(w http.ResponseWriter, r *http.Request, api string, transform transformFunc, rewrite rewriteFunc, synthesize synthesizeFunc, strip stripFunc, suffix string) {
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
	requestID := newRequestID()
	log := h.log.With().Str("request_id", requestID).Str("api", api).Logger()
	log.Debug().Str("method", r.Method).Str("path", r.URL.Path).
		Str("remote_addr", r.RemoteAddr).Msg("request_received")

	var (
		outcome     = "completed"
		stream      bool
		publicModel string
		bytesIn     int64
		// meterEvent gates the usage record: only requests that built an
		// outbound request and entered the provider path are metered —
		// authentication failures and local rejections (bad JSON, missing or
		// unknown model, a first-candidate transform/build failure) carry no
		// provider facts.
		meterEvent bool
		// usageCapture accumulates the upstream-reported token usage from
		// the pre-rewrite response bytes; nil observation stays NULL in the
		// event. Created only when metering is on.
		usageCapture *usage.Capture
		// principal is the auth-time identity, captured once — a reload can
		// never attach an event to another partner's credentials.
		principal auth.Principal
		// lastCand is the most recent candidate of the provider walk: the
		// answering one on success, the last attempted one on exhaustion
		// (whose upstream_model the usage record then carries — no answer
		// arrived, so there are no usage tokens to report either).
		lastCand config.Candidate
		// ansEgressKind is the RELAYED answer's own egress mode, captured
		// when its candidate was left behind and its answer retained: the
		// last-attempt candidate's egress report would name the one that
		// failed, not the one the client's response came from. Empty when
		// the answer came from the walk's final candidate.
		ansEgressKind string
		// egress carries the pool's attempt report of the LAST provider
		// candidate this request executed through (nil when none of them
		// routed through a pool): how many distinct endpoints were actually
		// dialed, the kind and scheme+host of the last one, and whether the
		// loop ended without dialing anything. The target is log-safe by
		// construction — scheme+host only, the same surface origin() allows;
		// userinfo never enters AttemptInfo.
		egress *transport.AttemptInfo
		// eng is the request's recovery engine: the snapshot-bound policy
		// that decides what one failed attempt means, plus the exchange
		// envelope every dial claims from. It is created at the walk's start
		// and nil before then, and the completion closures below treat a nil
		// engine as "no provider path was reached".
		eng *recovery.Engine
		// budgetStopped records that the exchange envelope, not a provider,
		// ended the walk: the transport refused (or the handler declined) the
		// next outbound exchange. It outranks every other explanation of a
		// walk that received no answer, because none of them happened — no
		// endpoint was dialed and no member was blamed.
		budgetStopped bool
		// lastPolicyHash is the effective policy hash of the most recently
		// entered candidate: the policy the last attempt ran under, which is
		// what the completion evidence names.
		lastPolicyHash string
		// lastCredentialID is the id of the credential the most recent
		// attempt went out with — the answering attempt's key on success,
		// the last failed one's on exhaustion, empty when the candidate
		// carries no credential pool. It feeds the completion record's
		// upstream_credential_id, which names the CONFIGURED key id — never
		// the key value, and never the caller's partner key id (a different
		// axis that rides KeyID on the usage record).
		lastCredentialID string
		// streamed is the mode the response was actually relayed in, which
		// the probe's stream flag only predicts: a stream=true request whose
		// upstream answered non-SSE is relayed buffered, and the record says
		// what happened, not what was asked.
		streamed bool
		// providerAttempts counts the walk's LOGICAL provider-level attempts
		// — each candidate attempt the walk began, whether or not the
		// transport then managed to dial anything — and retriesTotal counts
		// the re-asks among them. Both are incremented at the attempt's
		// start, so retriesTotal can never report a re-ask the attempt
		// counter does not contain.
		//
		// This is one of two independent axes, and the split is the point:
		// upstream_exchanges counts real outbound dials and is claimed by
		// the transport's budget at each one, so one pooled attempt may be
		// several exchanges and an attempt the pool refuses before any dial
		// is one attempt with zero exchanges. Neither number may be derived
		// from the other — a logical attempt is what the recovery policy
		// decides on, an exchange is what the wire carried.
		// finalProvider is the identity (providers-table name, or endpoint
		// origin) of the candidate whose answer was committed or, on
		// exhaustion/disconnect, of the last one attempted.
		// providerExhausted marks the walk ending with no candidate
		// answering. lastCandIndex is the one-based chain position of the
		// most recent candidate — the answering one whenever an answer was
		// committed (an answer is always committed for the current
		// candidate), else the last attempted one.
		providerAttempts  int
		retriesTotal      int
		finalProvider     string
		providerExhausted bool
		lastCandIndex     int
	)
	// exchanges is how many real outbound exchanges the request has spent so
	// far. It is read from the engine's live envelope rather than counted
	// here: the transport claims the units where the dials happen (a pool's
	// fallback included), so one number serves the log, the usage record, and
	// the budget decision without any of them being able to drift.
	exchanges := func() int {
		if eng == nil {
			return 0
		}
		return eng.Budget().RequestExchanges()
	}
	withEgress := func(ev *zerolog.Event) *zerolog.Event {
		if egress == nil {
			return ev
		}
		return ev.Int("egress_attempts", egress.Attempts).
			Str("egress_kind", egress.Kind).
			Str("egress_target", egress.Target).
			Bool("egress_exhausted", egress.Exhausted)
	}
	// withProviders adds the walk's completion facts. candidate_attempts is
	// the same total as provider_attempts seen from the other scope — one
	// provider attempt IS one attempt on some candidate — while
	// upstream_exchanges is the count of real dials, an independent axis:
	// higher than the attempts when one pooled attempt fans out across
	// members, and lower — even zero — when an attempt was refused before
	// any dial. provider_attempts therefore never drops to zero for an
	// attempted candidate just because the transport dialed nothing.
	// provider_attempts/retries_total stay as compatibility aliases of
	// candidate_attempts/retry_attempts, the same way attempt aliases
	// egress_attempt on the per-dial events; the new names are authoritative.
	//
	// request_exchange_budget_remaining reports the REQUEST-wide envelope's
	// headroom, not the tighter of the two: a finished walk has usually spent
	// its candidate envelope, so the tighter number would read zero for every
	// ordinary request.
	//
	// A walk that entered a candidate but dialed nothing still carries its
	// walk facts: the gate is the engine, not the exchange count, because a
	// zero-dial walk had a policy decide it and the operator needs the same
	// identity — final_provider, policy_hash, provider_exhausted — that any
	// other walk reports.
	withProviders := func(ev *zerolog.Event) *zerolog.Event {
		if eng == nil || eng.CandidatesEntered() == 0 {
			return ev
		}
		entered := eng.CandidatesEntered()
		ev = ev.Int("provider_attempts", providerAttempts).
			Int("retries_total", retriesTotal).
			Int("candidate_attempts", providerAttempts).
			Int("retry_attempts", retriesTotal).
			Int("candidates_entered", entered).
			Int("upstream_exchanges", exchanges()).
			Int("request_exchange_budget_remaining", eng.Budget().RequestRemaining()).
			Str("policy_hash", lastPolicyHash).
			Uint64("policy_generation", snap.Gen()).
			Int("final_candidate", lastCandIndex).
			Str("final_provider", finalProvider)
		// The configured key id the answering (or last-attempted) credential
		// went out under — a rotation fact, never a secret and never the
		// caller's partner key id. Empty, and omitted, without credentials.
		if lastCredentialID != "" {
			ev = ev.Str("upstream_credential_id", lastCredentialID)
		}
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
		// The usage record, from the facts this request already bound: the
		// auth-time principal, the snapshot generation, the walk's answering
		// (or last-attempted) candidate, the pre-rewrite usage observation.
		// Record is fire-and-forget into a bounded queue — a full queue, a
		// dead database, nothing here can fail the request or alter the
		// response the client already received.
		if h.meter != nil && meterEvent {
			var prompt, completion, total *int64
			if usageCapture != nil {
				if tokens, ok := usageCapture.Tokens(); ok {
					prompt, completion, total = tokens.PromptTokens, tokens.CompletionTokens, tokens.TotalTokens
				}
			}
			egressKind := "direct"
			if egress != nil {
				egressKind = egress.Kind
			}
			if ansEgressKind != "" {
				// The relayed answer was retained from a candidate the walk
				// later left: its own egress mode names where the client's
				// response came from, not where the last dial failed.
				egressKind = ansEgressKind
			}
			h.meter.Record(usage.Event{
				EventID:          usage.NewEventID(),
				OccurredAt:       start,
				PartnerID:        principal.PartnerID,
				KeyID:            principal.KeyID,
				RequestID:        requestID,
				ConfigGeneration: snap.Gen(),
				PublicModel:      publicModel,
				Provider:         finalProvider,
				UpstreamModel:    lastCand.UpstreamModel,
				API:              api,
				Stream:           streamed,
				HTTPStatus:       sw.status,
				Outcome:          outcome,
				PromptTokens:     prompt,
				CompletionTokens: completion,
				TotalTokens:      total,
				BytesIn:          bytesIn,
				BytesOut:         sw.bytes,
				ProviderAttempts: providerAttempts,
				EgressAttempts:   exchanges(),
				EgressKind:       egressKind,
				LatencyMS:        time.Since(start).Milliseconds(),
			})
		}
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

	// Prepare the API-scoped capture before entering the provider walk. The
	// event gate itself is set once a candidate has transformed and built its
	// outbound request: a local transform or build failure BEFORE that first
	// build reached no provider and produces no usage event. On a later
	// candidate such a failure would produce an event naming the provider it
	// was attributed to — and both are unreachable after a first success, the
	// transform reading only the body and model mapping, the endpoints
	// validated URLs.
	if h.meter != nil {
		usageCapture = usage.NewCapture(api)
	}

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
	// The strip list binds to the request like everything else on the
	// snapshot: the model's own list when it states one — forced onto every
	// candidate at load — else the relayed candidate's provider list, which
	// the candidate view below writes into m.Strip per candidate. The SSE
	// gate's first-segment byte patterns derive from m.Strip the same way:
	// initialized here from the model's own list (often nil), then refreshed
	// in the candidate view whenever the walk swaps in a provider's list, so
	// the stream finalizes on exactly the relayed candidate's patterns —
	// never a pre-walk nil for a model whose providers strip. The relay reads
	// them once, at the CopySSE call after the walk's outcome is known.
	stripKeys := inject.StripPatterns(stripSegments(m.Strip))
	// rewriteOut is the single response rewriter both response paths share —
	// parity by construction: the model rewrite, then, when the plan is
	// active, the usage synthesis, and finally the strip. The order is
	// deliberate: rewrite and synthesis OWN the members the strip list
	// reserves, so the strip runs on the bytes that will
	// actually be relayed. Under an inactive plan and an empty strip list it
	// is exactly today's model-rewrite closure, byte for byte.
	rewriteOut := func(payload []byte) []byte {
		out := rewrite(payload, m.Public)
		if plan.Active {
			out = synthesize(out, plan)
		}
		if len(m.Strip) > 0 {
			out = strip(out, stripSegments(m.Strip))
		}
		return out
	}

	log.Debug().Int64("bytes_in", bytesIn).Msg("request_transform_started")

	// The provider walk. The model's candidate chain is tried primary first,
	// and the engine built here decides what one finished attempt means —
	// from the snapshot-bound recovery policy, which is the closed matrix of
	// dispositions plus the retry, fallback, retry-after, and exchange
	// envelope numbers, as data:
	//
	//   - a retryable result — 408/425/429, every 5xx except the 501/505
	//     carve-outs, and a malformed or incomplete response received before
	//     commitment — re-asks the SAME candidate while its retry budget
	//     lasts (bounded count, bounded elapsed window, capped backoff, capped
	//     Retry-After), then moves to the next candidate;
	//   - a fallback-only result — 401/403/404/405/409/422 — moves to the next
	//     candidate immediately, never re-asking the same one;
	//   - a terminal result (every other 4xx/5xx) and an answer (2xx/3xx/
	//     204/304) end the walk;
	//   - a transport-level failure (dial, TLS, proxy, egress exhaustion)
	//     carries the class its own cause belongs to and, by default, moves to
	//     the next candidate.
	//
	// The engine is handed the request context, the walk's clock, and the
	// walk's jitter draw, so one request is measured and spread by one clock
	// and one draw whichever layer asks. It owns the fallback budget
	// (EnterCandidate), the retry budget, and the exchange envelope
	// (Budget), and it is a pure decision maker: it never dials, never
	// sleeps, never writes, and never logs. The handler executes.
	//
	// Each attempt replays the same immutable client body through the
	// candidate's own transform (its upstream model name and endpoint
	// differ — replay safety: nothing observed on an earlier attempt feeds
	// the next). Nothing is ever written to the client until the walk
	// commits one final result — the walk happens entirely before the
	// first response byte, so streaming commitment holds by construction:
	// the candidate that produces headers has produced THE response, and
	// no retry follows commitment. A caller cancellation or deadline is
	// terminal everywhere: it ends the attempt, the walk, and any pending
	// retry wait.
	//
	// Local validation never falls back: a body that fails one candidate's
	// transform fails every candidate's transform (the transform sees only
	// the body and model mapping, never the network), so the 400 is
	// answered on the first attempt.
	eng = recovery.NewEngine(r.Context(), m.Recovery,
		recovery.WithClock(retryNow),
		recovery.WithJitterSource(retryJitterDraw))

	// answerKind is the committed result's shape, chosen by the post-walk
	// relay branches. answerSSE and answerVerbatim hold the live response;
	// every other kind was fully consumed (or synthesized) inside the walk
	// and carries only retained facts.
	type answerKind int
	const (
		answerSSE answerKind = iota
		answerVerbatim
		answerBuffered
		answerHTTPError
		answerInvalid
	)
	// walkAnswer is the one result the walk commits — or none, on
	// exhaustion, disconnect, or local rejection. Discarded attempts
	// retain no bytes: by the time the next attempt starts, the failed
	// one's evidence is already reduced to the bounded evidence struct
	// (or the closed tokens) and its body is closed.
	type walkAnswer struct {
		kind      answerKind
		resp      *http.Response // live response (answerSSE, answerVerbatim)
		body      []byte         // validated 2xx body (answerBuffered)
		header    http.Header    // the committed answer's upstream headers
		status    int
		ev        upstreamErrorEvidence // answerHTTPError
		invalid   int                   // answerInvalid flavor (invalid*)
		cand      config.Candidate
		candIndex int // one-based chain position
		// egressKind is this attempt's own egress mode — the pool's last
		// dialed member's kind, or direct. It rides along so a retained
		// answer's usage record names ITS egress, not the last failed
		// candidate's.
		egressKind string
		// credKey is the id of the credential this attempt went out with,
		// empty on a candidate without a pool. It rides for the same reason
		// egressKind does: a retained answer's completion record must name
		// the key that PRODUCED it, not whichever key the last failed
		// attempt used.
		credKey string
	}
	// answerInvalid flavors: what made a 200-shaped response unusable.
	// They share the 502 upstream_invalid_response envelope but not the
	// outcome token.
	const (
		invalidBody        = iota + 1 // unparseable or over-cap 2xx body
		invalidReadFailed             // the 2xx body read failed mid-answer
		invalidBodyTimeout            // an error body's capture stalled past its deadline
	)

	var (
		answer *walkAnswer
		// retained is the walk's last received HTTP answer that did NOT end
		// the walk (its candidate's retry budget ran out and the walk moved
		// on). If every remaining candidate then fails before answering,
		// this answer — not a synthesized 502 — is the client's response:
		// the final error is the last received HTTP answer, and the
		// unreachable synthesis is reserved for walks that never received
		// one. Only the retained facts survive: evidence struct, envelope,
		// relay headers. One slot, replaced per newer answer.
		retained *walkAnswer
		// lastUpstream is the request URL of the most recent attempt — the
		// answering candidate's URL on success, the last failed one's on
		// exhaustion. Log-safe surfaces only (origin()).
		lastUpstream *url.URL
		// lastUerr is the most recent transport failure; the exhaustion
		// report after the walk carries it.
		lastUerr error
		// lastEgressAttempt is the final egress dial index of the most
		// recent attempt — 1 on the single-endpoint path, the pool's dial
		// count on a pooled one, 0 when a pool dialed nothing. It feeds the
		// attempt indexes on the events after the walk, and is never
		// invented for a zero-dial exhaustion.
		lastEgressAttempt int
		// lastFailureCause carries the last endpoint-owned failure's cause
		// token into the post-walk exhaustion report, classified when the
		// failure happened: re-classifying after the walk could let a client
		// disconnect that raced the loop's end re-own an endpoint failure.
		lastFailureCause string
		// lastSendState carries that failure's send state alongside it: the
		// exhaustion report says whether the walk ended on a request that
		// provably never left (definitely_not_sent) or one that may already
		// have reached an upstream (send_unknown). Empty when the walk ended
		// on something that was not a transport failure.
		lastSendState string
		// lastRuleID is the identity of the decision that ended the last
		// attempt — a matrix rule, or one of the engine's reserved invariant
		// identities. It rides the exhaustion report so an operator can tell
		// "the 429 row said fall back" from "the envelope stopped the walk".
		lastRuleID string
	)

	// waitOut sleeps a same-candidate retry's bounded delay, cancellable.
	// False means the caller went away during the wait: the request is
	// done — no further attempt, no envelope, only the completion record.
	waitOut := func(dec recovery.Decision) bool {
		if dec.Action != recovery.ActionRetry {
			return true
		}
		// retryWait is the only sleep site and reports the context's
		// liveness. A delay capped to zero has nothing to sleep for, but the
		// context is still consulted — jitter can floor a backoff at zero,
		// and a caller already gone must not get another dial.
		var live bool
		if dec.Delay > 0 {
			live = retryWait(r.Context(), dec.Delay)
		} else {
			live = r.Context().Err() == nil
		}
		if !live {
			outcome = "client_disconnected"
			complete()
			return false
		}
		return true
	}
	// answerFor reduces one captured 4xx/5xx answer to the facts the walk
	// retains: the evidence struct and relay headers on a complete capture,
	// or the typed invalid flavor when the capture itself failed or
	// stalled. Both the finalize and the retention routes go through it, so
	// the answer the client sees is the same shape however the walk ends.
	answerFor := func(cerr captureError, ev upstreamErrorEvidence, resp *http.Response, status int, cand config.Candidate, i int, egressKind, credKey string) *walkAnswer {
		if cerr == captureOK {
			return &walkAnswer{kind: answerHTTPError, ev: ev, header: resp.Header, status: status, cand: cand, candIndex: i + 1, egressKind: egressKind, credKey: credKey}
		}
		if cerr == captureDeadline {
			return &walkAnswer{kind: answerInvalid, invalid: invalidBodyTimeout, cand: cand, candIndex: i + 1, egressKind: egressKind, credKey: credKey}
		}
		return &walkAnswer{kind: answerInvalid, invalid: invalidReadFailed, cand: cand, candIndex: i + 1, egressKind: egressKind, credKey: credKey}
	}
walk:
	for i := range m.Chain {
		cand := m.Chain[i]
		// EnterCandidate is the fallback budget's gate. The first candidate is
		// always enterable; every later one spends a fallback slot, opens the
		// candidate's own exchange envelope, and starts its retry window. A
		// false means the walk may not reach another candidate at all — the
		// finalize path below still prefers a retained answer over a
		// synthesized 502.
		if !eng.EnterCandidate(cand.Recovery) {
			break
		}
		lastCandIndex = i + 1
		finalProvider = cand.Label()
		lastCand = cand
		lastPolicyHash = cand.RecoveryHash
		egress = nil
		lastCredentialID = "" // a candidate without a credential pool must not inherit the previous one's id
		candStart := eng.Now()

		// The candidate view: same public model, same injection prompt,
		// same thinking plan — only the upstream identity changes. m is a
		// per-request value copy (Snapshot.Model returns a value), so
		// mutating it cannot touch the snapshot. Strip is overwritten per
		// candidate too: when the model states no model-level list, each hop
		// answers under its own provider's list, and the rewrite reads the
		// candidate in scope. A model-level list was already forced onto every
		// candidate at load, so this assignment only ever narrows to the
		// candidate's own when the model had nothing to say.
		m.Provider = cand.Provider
		m.Endpoint = cand.Endpoint
		m.UpstreamModel = cand.UpstreamModel
		m.Transport = cand.Transport
		m.Strip = cand.Strip
		// The SSE gate follows the same swap: the stream's strip patterns
		// are re-derived from the candidate in scope so this hop's provider
		// list — present when the model states none — gates its own chunks.
		stripKeys = inject.StripPatterns(stripSegments(m.Strip))

		// The candidate's credential pool, resolved once per candidate and
		// held for its whole walk: rotation and cooldown state survives
		// every retry of this candidate and is never rebuilt mid-request,
		// even across a reload. Nil when the candidate carries no auth
		// block — the entire credential seam below is skipped and the
		// attempt runs byte for byte as it always has.
		var pool *credential.Pool
		if cand.Cred != nil && h.creds != nil {
			pool = h.creds.Pool(cand.Cred)
		}
		// credKey is the sticky key id: the key the candidate's attempts go
		// out with until something moves them off it. A same-candidate retry
		// re-acquires preferring the key it just used (a 500 must re-ask
		// with the same key, not rotate), while a key marked rate-limited
		// makes the next Acquire hand back a different ready key.
		credKey := ""

		attempt := 0
		for {
			attemptStart := eng.Now()

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
			// COUNTER AXIS 1 OF 2 — LOGICAL PROVIDER ATTEMPTS. One candidate
			// attempt begins here: the candidate is entered, the request is
			// built, and the transport is about to be asked. Counted here
			// rather than at the loop head so a transform or request-build
			// failure above — which never reaches a provider — reports no
			// attempt that never happened; counted BEFORE the transport runs
			// so the counter can never disagree with the provider_attempt
			// marker emitted below, whether or not the transport ends up
			// reaching the wire.
			//
			// It counts an ATTEMPT, not an exchange: an attempt is what the
			// recovery policy decides on, and a pooled attempt that dials
			// nothing (every member gated out, an exhausted envelope) is
			// still an attempt the walk made and answered for. The count of
			// real dials is a separate axis entirely — see upstream_exchanges,
			// claimed by the budget at each dial. attempt is candidate-local
			// (its own retries included); providerAttempts is the walk's
			// global provider-attempt index, and the retry inside an attempt
			// is counted beside it so retries_total can never report a re-ask
			// the attempt counter does not contain.
			attempt++
			retryIndex := attempt - 1
			providerAttempts++
			if attempt > 1 {
				retriesTotal++
			}
			// fallBackOnSpentCandidate answers the one refusal no observation
			// can describe: the transport declined to dial because this
			// candidate's own exchange envelope is spent. It asks the engine —
			// whose answer is the retry policy's on-exhausted action — and
			// reports whether the walk may move on.
			//
			// A refusal by the REQUEST envelope is not this case: it is
			// terminal whatever the policy says, it is reported by the
			// post-walk record under the request identity, and this returns
			// false so the caller stops the walk exactly as it always has.
			//
			// The record carries no upstream_exchange index, because the
			// refusal claimed no exchange: naming one would invent an exchange
			// that never happened. provider_attempt carries the attempts made
			// so far — the same number the other events' one-based index is
			// derived from — and request_exchange_budget_remaining is the
			// headroom the refusal left for the rest of the walk.
			fallBackOnSpentCandidate := func() bool {
				if eng.Budget().Exhausted() == recovery.ExhaustionRequest {
					return false
				}
				dec := eng.CandidateSpent(recovery.CauseExchangeBudget)
				act := walkAction(dec.Action, i+1 < len(m.Chain))
				log.Warn().
					Str("public_model", model).
					Str("provider", cand.Label()).
					Str("error_class", "provider_exhausted").
					Str("error_cause", recovery.CauseExchangeBudget).
					Str("disposition", act.String()).
					Str("failure_origin", "envelope").
					Str("reason", dec.Reason).
					Str("policy_rule_id", dec.RuleID).
					Str("policy_hash", lastPolicyHash).
					Uint64("policy_generation", snap.Gen()).
					Int("provider_attempt", providerAttempts).
					Int("candidate_index", i+1).
					Int("candidate_attempt", attempt).
					Int("retry_index", retryIndex).
					Int("request_exchange_budget_remaining", eng.Budget().RequestRemaining()).
					Int64("elapsed_ms", eng.Now().Sub(attemptStart).Milliseconds()).
					Msg("candidate_exchange_budget_spent")
				return act == recovery.ActionFallback
			}
			// This request has now reached the provider path. Any subsequent
			// dial failure still yields one event with the walk's final facts.
			meterEvent = true
			lastUpstream = &upstream

			// THE CREDENTIAL SEAM. One key per attempt, acquired before the
			// transport is resolved: the attempt below dials with the key
			// this hands back, and every retry re-acquires. Nothing here
			// dials, sleeps unbounded, or invents an outcome — when no ready
			// key exists the engine DECIDES, through the same
			// observation → decision → bounded wait → action path every
			// other failure takes.
			//
			// The header is composed here and only here: header name, prefix
			// and key value come from the validated spec, and the value
			// never reaches a log, an error, or the transport layer — req is
			// the request the transport sends as-is (the pooled path clones
			// it per member, carrying the credential along).
			if pool != nil {
				if k, ok := pool.Acquire(eng.Now(), credKey); ok {
					credKey = k.ID
					lastCredentialID = k.ID
					req.Header.Set(cand.Cred.Spec.Header, cand.Cred.Spec.Prefix+k.Value)
				} else {
					// No ready key: every key of this provider is cooling
					// under a 429 mark. The observation rides the earliest
					// ready time as its Retry-After — the matrix re-caps it
					// like any directive before the wait, so a long provider
					// cooldown cannot stretch one retry's sleep — and the
					// candidate's REAL retry budget bounds the whole cycle:
					// exhausted, the policy's on-exhausted action moves the
					// walk on or ends it. No busy loop, no synthesized
					// response, no invented counter.
					obs := recovery.Observation{
						Class:            recovery.FailureCredential,
						CredentialCause:  recovery.CredentialCooldown,
						Streaming:        stream,
						Committed:        answer != nil,
						CandidateIndex:   i + 1,
						CandidateAttempt: attempt,
						RetryIndex:       retryIndex,
						Elapsed:          eng.Now().Sub(candStart).Milliseconds(),
					}
					if next, ok := pool.NextReady(eng.Now()); ok {
						obs.RetryAfter = next.Sub(eng.Now())
					}
					dec := eng.Observe(obs)
					lastRuleID = dec.RuleID
					act := walkAction(dec.Action, i+1 < len(m.Chain))
					// The refusal dialed nothing, so the event carries no
					// upstream_exchange index — naming one would invent an
					// exchange that never happened, exactly as for an
					// envelope refusal. upstream_credential_id names the key
					// the previous attempt went out with (the one Acquire
					// preferred); absent on a first attempt that found the
					// pool already cold.
					event := log.Warn().
						Str("public_model", model).
						Str("provider", cand.Label()).
						Str("error_class", "credential").
						Str("error_cause", recovery.CredentialCooldown).
						Str("failure_origin", "credential").
						Str("disposition", act.String()).
						Str("reason", dec.Reason).
						Str("policy_rule_id", dec.RuleID).
						Str("policy_hash", lastPolicyHash).
						Uint64("policy_generation", snap.Gen()).
						Int("provider_attempt", providerAttempts).
						Int("candidate_index", i+1).
						Int("candidate_attempt", attempt).
						Int("retry_index", retryIndex).
						Int("request_exchange_budget_remaining", eng.Budget().RequestRemaining()).
						Int64("elapsed_ms", eng.Now().Sub(attemptStart).Milliseconds())
					if credKey != "" {
						event = event.Str("upstream_credential_id", credKey)
					}
					event.Msg("credential_unavailable")
					if !waitOut(dec) {
						return
					}
					if act == recovery.ActionRetry {
						continue
					}
					if act == recovery.ActionFallback {
						break
					}
					break walk
				}
			}

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
			var resp *http.Response
			d := h.doers.Doer(cand.Transport)
			ex, pooled := d.(transport.Executor)
			// The exchanges this request had already spent when this attempt
			// began. The attempt's own upstream_exchange index is this plus its
			// place in the attempt's own dial order, because a pooled attempt is
			// several real exchanges where a direct one is a single exchange.
			exchangeBefore := eng.Budget().RequestExchanges()
			// This is an attempt-local index. In particular, an envelope
			// refusal before a direct dial must not inherit the previous
			// attempt's egress index into its final evidence.
			lastEgressAttempt = 0
			// And the send state is attempt-local for the same reason, reset
			// alongside the index it describes. A pool that dials nothing this
			// attempt must not republish the PREVIOUS attempt's wire evidence:
			// an earlier dial's definitely_not_sent beside this attempt's
			// egress_exhausted would read as "the pool proved nothing left",
			// which is a claim about a dial it never made. Only an attempt that
			// dialed sets it again below.
			lastSendState = ""
			if pooled {
				// provider_attempt_started precedes the attempt's first dial:
				// the pool may still refuse the attempt before ANY dial
				// (eligibility gate, health, envelope), and the refusal's own
				// event — candidate_exchange_budget_spent or
				// provider_attempt_failed — names the reason, so the marker
				// stays honest: an attempt that begins is what it names, and
				// the index it carries is the counter's own, already counted
				// above, not an optimistic guess at what the counter will say
				// if a dial happens to succeed. A zero-dial attempt therefore
				// reports provider_attempt = provider_attempts = N with
				// upstream_exchanges unchanged: the attempt is a logical fact,
				// the exchange is a wire fact, and neither implies the other.
				log.Debug().Str("provider", cand.Label()).
					Str("upstream", origin(&upstream)).
					Int64("bytes_out", int64(len(out))).
					Int("provider_attempt", providerAttempts).
					Int("candidate_index", i+1).
					Int("candidate_attempt", attempt).
					Int("retry_index", retryIndex).
					Msg("provider_attempt_started")
				var info transport.AttemptInfo
				resp, info, uerr = ex.Execute(&transport.AttemptRequest{
					Ctx:       r.Context(),
					Method:    http.MethodPost,
					URL:       &upstream,
					Header:    req.Header.Clone(),
					Body:      out,
					Streaming: stream,
					Budget:    eng.Budget(),
				})
				egress = &info
				lastEgressAttempt = info.Attempts
				// The provider attempt was counted before Execute, and
				// deliberately not again here: info.Attempts is this
				// attempt's EGRESS count (how many members the pool dialed),
				// a different axis that the budget already claimed at each
				// dial. Gating the attempt counter on it — as this code once
				// did — made a zero-dial attempt report the contradictory
				// pair provider_attempt_started = N, provider_attempts = N-1.
				// Per-attempt evidence, bounded by the fallback budget: one WARN
				// per dialed-and-failed endpoint, correlated by this request's
				// request_id and its one-based provider/egress attempt indexes.
				// Typed class, closed-set cause, and scheme+host only — the error
				// text, any credential material, and skipped members (no dial, no
				// event) stay out. attempt rides along as the pre-existing alias
				// for egress_attempt; the new field is authoritative. No decision
				// produced these records — the dial failed before any disposition
				// was reached — so policy_rule_id is empty here by construction.
				// failure_origin is transport: these are dial failures, not
				// answers.
				for j, fl := range info.Failures {
					event := withPolicyFields(withAttemptFields(log.Warn().Str("public_model", model).
						Str("provider", cand.Label()).
						Str("egress_kind", fl.Kind).Str("egress_target", fl.Target).
						Str("failure_origin", "transport").
						Str("error_class", fl.Class).Str("error_cause", fl.Cause).
						Str("send_state", fl.SendState).
						Int("egress_attempt", j+1).
						Int("attempt", j+1),
						providerAttempts, i+1, attempt, eng.Now().Sub(attemptStart)),
						"", lastPolicyHash, snap.Gen(), exchangeBefore+j+1, eng.Budget().RequestRemaining())
					event.Msg("egress_attempt_failed")
				}
				if info.BudgetExhausted {
					// The envelope refused the pool's next dial, so no member
					// was struck for a dial that never started and the pool
					// handed the refused member's permit back before
					// returning.
					//
					// Which envelope refused it decides the walk. A spent
					// REQUEST envelope ends everything: no candidate may spend
					// another exchange, so the walk stops with the envelope
					// named rather than an endpoint. A spent CANDIDATE envelope
					// forbids only another exchange on THIS candidate, and the
					// walk keeps its ordinary options — a dial did fail here
					// (the pool returns that last error with the flag set), so
					// the failure is observed and decided like any other, and
					// the engine turns a retry into the retry policy's
					// on-exhausted action. A per-candidate number must never
					// silently pin a chain the operator configured to fall
					// back.
					if eng.Budget().Exhausted() == recovery.ExhaustionRequest {
						budgetStopped = true
						break walk
					}
					if uerr == nil {
						// The refusal came before this attempt's first dial,
						// so there is no failure to observe: the engine is
						// asked the candidate-envelope question directly.
						if !fallBackOnSpentCandidate() {
							budgetStopped = true
							break walk
						}
						break
					}
				}
			} else {
				// provider_attempt_started precedes the single dial of a
				// direct transport, matching the pooled path's placement and
				// carrying the counter's own index, counted at the attempt's
				// start above. The envelope claim follows: a refusal fires
				// candidate_exchange_budget_spent with its own identity, and
				// the pair stays honest in either direction — the attempt is
				// reported whether or not the envelope let the dial happen,
				// while upstream_exchanges moves only when one did.
				log.Debug().Str("provider", cand.Label()).
					Str("upstream", origin(&upstream)).
					Int64("bytes_out", int64(len(out))).
					Int("provider_attempt", providerAttempts).
					Int("candidate_index", i+1).
					Int("candidate_attempt", attempt).
					Int("retry_index", retryIndex).
					Msg("provider_attempt_started")
				// The exchange envelope is claimed here for a single-endpoint
				// candidate: the transport claims its own dials, and this path is
				// one dial. A refusal is not a transport failure — nothing was
				// dialed, no endpoint is to blame — so no observation is built:
				// the walk stops with the envelope named, exactly as it does for
				// a pool that refused a dial.
				if !eng.Budget().ConsumeExchange() {
					if !fallBackOnSpentCandidate() {
						budgetStopped = true
						break walk
					}
					break
				}
				resp, uerr = d.Do(req)
				lastEgressAttempt = 1
			}
			// This attempt's own egress mode — the pool's last dialed
			// member's kind, or direct. It rides with any answer the attempt
			// leaves behind, so a retained answer's records name ITS egress.
			kindHere := "direct"
			if pooled {
				kindHere = egress.Kind
			}

			// Transport-level failure. One classification, one owner: the
			// context the attempt ran under decides whether the client's own
			// cancellation or deadline ended the request — terminal, no
			// fallback, no envelope, no retry — or the failure is the
			// endpoint's and moves the walk to its next candidate. A caller
			// deadline must never read as a provider-local timeout, and a
			// transport failure never re-asks the same endpoint: the retry
			// budgets are for answers, not for dead sockets.
			if uerr != nil {
				// A request this process failed to CONSTRUCT is a local fault,
				// not an endpoint's: the pool never dialed, so no member may be
				// blamed, struck, or named with a wire state. It is reported
				// under the single-endpoint path's own build vocabulary and
				// stops the walk — the request itself is what is wrong, so no
				// other candidate does better with it — matching the transform
				// error above.
				//
				// Unlike that path, the attempt here is already counted and its
				// provider_attempt_started already emitted: a pool builds per
				// member, inside Execute, which is past the marker. The attempt
				// stays counted rather than being retroactively denied, because
				// the marker and the counter may never disagree.
				var rbe *transport.RequestBuildError
				if errors.As(uerr, &rbe) {
					log.Error().Str("model", model).
						Str("upstream", origin(&upstream)).
						Str("error_class", "request_build").
						Int("provider_attempt", providerAttempts).
						Int("candidate_index", i+1).
						Msg("upstream_request_build_failed")
					outcome = "upstream_unreachable"
					reject(http.StatusBadGateway, []byte(envelopeUpUnreach), nil)
					return
				}
				lastUerr = uerr
				f := transport.ClassifyAttempt(r.Context(), uerr)
				class, cause := f.Class.String(), f.Cause
				if !f.CallerTerminated && pooled && egress.Exhausted {
					// Zero dials is a pool-level condition — no member was
					// blamed — so the endpoint vocabulary would be an
					// invention.
					class, cause = "egress_exhausted", recovery.CauseNoEligibleEndpoint
				}
				lastFailureCause = cause
				// The send state is evidence about a DIALED attempt, so it is
				// recorded only when one happened. A pool that skipped every
				// member dialed nothing for it to describe, and the sentinel
				// that reports that is not an endpoint failure at all — its
				// classification must not be read as wire evidence. The
				// exhaustion line already says it: egress_exhausted.
				//
				// This is also the ONE value every record for this attempt
				// publishes — the per-dial record, provider_attempt_failed and
				// the post-walk exhaustion report all read it back rather than
				// re-deriving it from f, so a state that was not earned by a
				// dial can never be attached to a record by a second code path
				// that forgot the guard.
				if dialedAttempt(pooled, egress) && !f.CallerTerminated {
					lastSendState = f.SendState.String()
				}
				// The observation the engine decides on. A caller-terminated
				// failure leaves the transport vocabulary entirely: its owner
				// is the request context, never the wire, and the engine
				// hard-stops on that class whatever the matrix says.
				obs := recovery.Observation{
					Class:          recovery.FailureTransport,
					TransportClass: transportClassOf(f.Class),
					TransportCause: f.Cause,
					Streaming:      stream,
					// A committed answer means the upstream already produced
					// this result — as opposed to a decision not to ask it. The
					// walk commits one answer per request, before the first byte
					// reaches the client, so nothing here can observe a failure
					// on a committed result: the invariant holds structurally,
					// and the engine may not retry one.
					Committed:        answer != nil,
					CandidateIndex:   i + 1,
					CandidateAttempt: attempt,
					RetryIndex:       retryIndex,
					Elapsed:          eng.Now().Sub(candStart).Milliseconds(),
				}
				if f.CallerTerminated {
					obs.Class = recovery.FailureCaller
					obs.CallerCause = f.Cause
					obs.TransportClass = recovery.TransportClassNone
					obs.TransportCause = ""
				} else if pooled && egress.Exhausted {
					// A pool that dialed nothing has no endpoint to blame, and
					// no_eligible_endpoint is the token that says so.
					obs.TransportCause = recovery.CauseNoEligibleEndpoint
					obs.TransportClass = recovery.TransportClassOfCause(recovery.CauseNoEligibleEndpoint)
				}
				dec := eng.Observe(obs)
				lastRuleID = dec.RuleID
				// The decision folded against the chain: a fallback with no
				// candidate left to fall to is a terminal outcome, and an
				// exhausted chain must never be reported as a fallback that
				// happened.
				act := walkAction(dec.Action, i+1 < len(m.Chain))
				if f.CallerTerminated {
					// The client went away — canceled, or done waiting —
					// before any upstream answered. No fallback: there is
					// nobody left to answer. The outcome is the disconnect
					// either way; an upstream_unreachable 502 would misreport
					// a client-side event as an upstream failure.
					outcome = "client_disconnected"
					event := withEgress(log.Warn()).Err(sanitizeUpstreamError(uerr, &upstream)).
						Str("public_model", model).
						Str("provider", cand.Label()).
						Str("upstream", origin(&upstream)).
						Str("error_class", class).
						Str("error_cause", cause).
						Str("disposition", act.String()).
						Str("reason", dec.Reason).
						Str("failure_origin", "caller")
					if lastEgressAttempt > 0 {
						event = event.Int("egress_attempt", lastEgressAttempt)
					}
					event = withPolicyFields(withAttemptFields(event, providerAttempts, i+1, attempt, eng.Now().Sub(attemptStart)),
						dec.RuleID, lastPolicyHash, snap.Gen(), exchangeBefore+lastEgressAttempt, eng.Budget().RequestRemaining())
					withCredentialFields(event, credKey).Msg("upstream_request_failed")
					complete()
					return
				}
				// Uniform egress evidence on the single-endpoint path: a
				// direct failure gets the same per-dial record a pool
				// member's failure gets (kind direct, first egress attempt),
				// so downstream queries need no knowledge of which transport
				// served the candidate. A pool emitted its own records as the
				// dials failed, before this point.
				if !pooled {
					event := withPolicyFields(withAttemptFields(log.Warn().Str("public_model", model).
						Str("provider", cand.Label()).
						Str("egress_kind", "direct").Str("egress_target", "direct").
						Str("failure_origin", "transport").
						Str("error_class", class).Str("error_cause", cause).
						Str("send_state", lastSendState).
						Int("egress_attempt", 1),
						providerAttempts, i+1, attempt, eng.Now().Sub(attemptStart)),
						dec.RuleID, lastPolicyHash, snap.Gen(), exchangeBefore+1, eng.Budget().RequestRemaining())
					event.Msg("egress_attempt_failed")
				}
				// Transport-level failure with the client still present: one
				// WARN per failed attempt — the *url.Error from client.Do
				// embeds the full request URL, query string included, which
				// is how query-authenticated providers leak credentials, so
				// the sanitized error and the scheme+host origin only — then
				// the next candidate (or the exhaustion report after the
				// walk, when the budget is spent).
				event := withEgress(log.Warn()).Err(sanitizeUpstreamError(uerr, &upstream)).
					Str("public_model", model).
					Str("provider", cand.Label()).
					Str("upstream", origin(&upstream)).
					Str("error_class", class).
					Str("error_cause", cause).
					Str("failure_origin", "transport").
					Str("disposition", act.String()).
					Str("reason", dec.Reason)
				// A zero-dial attempt has no wire state to report — the pool
				// dialed nothing, so a state here would be evidence about a
				// request that was never sent anywhere. lastSendState is empty
				// exactly then.
				if lastSendState != "" {
					event = event.Str("send_state", lastSendState)
				}
				if lastEgressAttempt > 0 {
					event = event.Int("egress_attempt", lastEgressAttempt)
				}
				event = withPolicyFields(withAttemptFields(event, providerAttempts, i+1, attempt, eng.Now().Sub(attemptStart)),
					dec.RuleID, lastPolicyHash, snap.Gen(), exchangeBefore+lastEgressAttempt, eng.Budget().RequestRemaining())
				withCredentialFields(event, credKey).Msg("provider_attempt_failed")
				if !waitOut(dec) {
					return
				}
				if act == recovery.ActionRetry {
					// Only a configured matrix can ask for this — the default
					// rows move a transport failure on rather than re-asking a
					// dead socket — and the engine's budget already sized it.
					continue
				}
				if act == recovery.ActionFallback {
					break
				}
				break walk
			}

			// An HTTP answer arrived. Logged per exchange (discarded
			// retries included), then classified by shape BEFORE any body
			// read, in the same precedence the relay branches have always
			// used: normalized error, verbatim status, SSE, buffered 2xx.
			withEgress(log.Debug()).Int("status", resp.StatusCode).
				Str("content_type", logSafeContentType(resp.Header.Get(contentTypeHeader))).
				Int("provider_attempt", providerAttempts).
				Int("egress_attempt", lastEgressAttempt).
				Int("candidate_index", i+1).
				Int("candidate_attempt", attempt).
				Int("retry_index", retryIndex).
				Msg("upstream_response_received")
			status := resp.StatusCode

			if isUpstreamHTTPError(status) {
				// A 429 marks the KEY, not the request — an account fact
				// taken off the raw status, before the body capture can
				// fail, stall, or be abandoned by a departing caller (all
				// three reclassify the observation, but none un-say the
				// provider's 429). The mark is unconditional on the
				// recovery matrix: a policy that ignores Retry-After for
				// its request waits must not also leave the key in
				// rotation. The key that cools is the one this attempt
				// went out with; the cooldown folds the parsed directive
				// under the provider's ceiling (credentialCooldown), and
				// the engine's own decision below still governs whether
				// THIS request retries — on it, the next Acquire finds the
				// marked key cooling and rotates.
				if status == http.StatusTooManyRequests && pool != nil && credKey != "" {
					pool.MarkRateLimited(eng.Now(), credKey,
						credentialCooldown(parseRetryAfter(resp.Header.Get("Retry-After"), eng.Now()), cand.Cred.RateLimit))
				}
				// The evidence capture is reused unchanged: one bounded read
				// reduces the error body to shape + fingerprint before any
				// decision, and the capture's own failure modes are
				// retryable results — the candidate has not produced a
				// usable answer.
				ev, cerr := captureUpstreamErrorEvidence(r.Context(), resp)
				_ = resp.Body.Close()
				if cerr == captureCallerEnded {
					// The caller's context ended mid-capture — canceled, or
					// done waiting. Ownership stays with the caller: no
					// envelope for a client that is gone, and no retry burns
					// budget for nobody.
					outcome = "client_disconnected"
					log.Warn().Str("public_model", model).
						Str("phase", "client_write").Msg("relay_copy_failed")
					complete()
					return
				}
				// The observation the matrix decides on. A complete capture
				// classified the status it received; a stalled or failed
				// capture never really received a usable answer, so the
				// status's own row does not apply — an unusable answer is
				// whatever the protocol cause says it is, whatever code the
				// response was carrying.
				obs := recovery.Observation{
					Streaming:        stream,
					Committed:        answer != nil,
					CandidateIndex:   i + 1,
					CandidateAttempt: attempt,
					RetryIndex:       retryIndex,
					Elapsed:          eng.Now().Sub(candStart).Milliseconds(),
					// A bounded directive, honored only for same-candidate
					// retries and always re-capped by the policy before any
					// sleep. It rides the response, not the body: a stalled
					// capture still read the headers.
					RetryAfter: parseRetryAfter(resp.Header.Get("Retry-After"), eng.Now()),
				}
				switch cerr {
				case captureOK:
					obs.Class = recovery.FailureHTTP
					obs.HTTPStatus = status
					obs.StatusClass = recovery.StatusClassOf(status)
					obs.ProviderErrorType = ev.providerType
					obs.ProviderErrorCode = ev.providerCode
				case captureDeadline:
					obs.Class = recovery.FailureProtocol
					obs.ProtocolCause = recovery.ProtocolBodyTimeout
				default:
					obs.Class = recovery.FailureProtocol
					obs.ProtocolCause = recovery.ProtocolBodyReadFailed
				}
				dec := eng.Observe(obs)
				lastRuleID = dec.RuleID
				act := walkAction(dec.Action, i+1 < len(m.Chain))
				// One evidence event per received error response — the
				// discarded attempts included; the canonical envelope write
				// happens after the walk, from the retained evidence.
				elapsed := eng.Now().Sub(attemptStart)
				if cerr == captureOK {
					event := log.Warn()
					if status >= http.StatusInternalServerError {
						event = log.Error()
					}
					event = withPolicyFields(withAttemptFields(event.Str("public_model", model).
						Str("upstream_model", cand.UpstreamModel).
						Str("upstream", origin(&upstream)).
						Int("upstream_status", ev.status).
						Str("content_type", ev.contentType).
						Str("error_class", "upstream_error").
						Str("error_cause", ev.class).
						Str("error_shape", ev.shape).
						Int64("body_bytes", ev.bodyBytes).
						Bool("body_truncated", ev.truncated).
						Str("error_fingerprint", ev.fingerprint).
						Int("egress_attempt", lastEgressAttempt),
						providerAttempts, i+1, attempt, elapsed),
						dec.RuleID, lastPolicyHash, snap.Gen(), exchangeBefore+lastEgressAttempt, eng.Budget().RequestRemaining()).
						Str("disposition", act.String()).
						Str("reason", dec.Reason).
						Str("failure_origin", "upstream_http")
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
					withCredentialFields(event, credKey).Msg("upstream_http_error")
				} else {
					w := log.Warn().Str("public_model", model)
					if cerr == captureDeadline {
						w = w.Str("error_class", "upstream_error_body_timeout").
							Str("error_cause", "capture_deadline_exceeded").
							Str("failure_origin", "protocol")
					} else {
						w = w.Str("error_class", "upstream_error").
							Str("error_cause", "body_read_failed").
							Str("failure_origin", "protocol")
					}
					w = withPolicyFields(withAttemptFields(w, providerAttempts, i+1, attempt, elapsed),
						dec.RuleID, lastPolicyHash, snap.Gen(), exchangeBefore+lastEgressAttempt, eng.Budget().RequestRemaining()).
						Str("disposition", act.String()).
						Str("reason", dec.Reason).
						Int("upstream_status", status)
					withCredentialFields(w, credKey).Msg("upstream_body_read_failed")
				}
				if !waitOut(dec) {
					return
				}
				if act == recovery.ActionRetry {
					continue
				}
				if act == recovery.ActionFallback {
					// The candidate's budget is spent, but it DID answer —
					// remember that answer: if every remaining candidate
					// then fails before answering, this one becomes the
					// client's response.
					retained = answerFor(cerr, ev, resp, status, cand, i, kindHere, credKey)
					break
				}
				// Finalize: the last received HTTP answer wins. Only the
				// retained facts survive the walk — the evidence struct and
				// the relay headers, never the raw bytes.
				answer = answerFor(cerr, ev, resp, status, cand, i, kindHere, credKey)
				break walk
			}

			if status >= http.StatusMultipleChoices || status == http.StatusNoContent || status == http.StatusNotModified {
				// Verbatim answer: redirects (3xx, never followed) and the
				// two body-less statuses. Committed as-is, body untouched.
				answer = &walkAnswer{kind: answerVerbatim, resp: resp, header: resp.Header, status: status, cand: cand, candIndex: i + 1, credKey: credKey}
				break walk
			}

			if stream && strings.Contains(strings.ToLower(resp.Header.Get(contentTypeHeader)), eventStreamType) {
				// SSE answer: the headers are the commitment. The body is
				// not read here — it streams after the walk, and anything
				// that kills it later truncates the committed stream.
				answer = &walkAnswer{kind: answerSSE, resp: resp, header: resp.Header, status: status, cand: cand, candIndex: i + 1, credKey: credKey}
				break walk
			}

			// Buffered 2xx: read and validated INSIDE the walk, so a
			// malformed or incomplete answer is a retryable result before
			// commitment instead of a 502 after it.
			bodyBytes, rerr := io.ReadAll(io.LimitReader(resp.Body, maxBufferedResponseBytes+1))
			_ = resp.Body.Close()
			if rerr != nil {
				// Ownership is the request context's, never the error chain's:
				// a canceled OR expired caller surfaces through the upstream
				// read as its own sentinel, and neither may be reported as an
				// upstream failure or spend the retry budget.
				if r.Context().Err() != nil || clientSide(rerr) {
					// The client is gone mid-answer and the upstream may be
					// fine. No envelope write is attempted. The caller owns
					// this branch — the direct context check decides it before
					// any error-chain classification can call it an upstream
					// body failure.
					outcome = "client_disconnected"
					log.Warn().Err(sanitizeUpstreamError(rerr, &upstream)).Str("public_model", model).
						Str("phase", "client_write").Msg("relay_copy_failed")
					complete()
					return
				}
				elapsed := eng.Now().Sub(attemptStart)
				dec := eng.Observe(recovery.Observation{
					Class:            recovery.FailureProtocol,
					ProtocolCause:    recovery.ProtocolBodyReadFailed,
					Streaming:        stream,
					Committed:        answer != nil,
					CandidateIndex:   i + 1,
					CandidateAttempt: attempt,
					RetryIndex:       retryIndex,
					Elapsed:          eng.Now().Sub(candStart).Milliseconds(),
				})
				lastRuleID = dec.RuleID
				act := walkAction(dec.Action, i+1 < len(m.Chain))
				event := withPolicyFields(withAttemptFields(log.Warn().Err(sanitizeUpstreamError(rerr, &upstream)).Str("public_model", model).
					Str("upstream", origin(&upstream)).
					Int("upstream_status", status).
					Str("error_class", "upstream_error").
					Str("error_cause", "body_read_failed").
					Str("failure_origin", "protocol"),
					providerAttempts, i+1, attempt, elapsed),
					dec.RuleID, lastPolicyHash, snap.Gen(), exchangeBefore+lastEgressAttempt, eng.Budget().RequestRemaining()).
					Str("disposition", act.String()).
					Str("reason", dec.Reason)
				withCredentialFields(event, credKey).Msg("upstream_body_read_failed")
				if !waitOut(dec) {
					return
				}
				if act == recovery.ActionRetry {
					continue
				}
				if act == recovery.ActionFallback {
					// The read failed, but the upstream answered 2xx — keep
					// the shape: later candidates failing before answering
					// fall back to this answer over a synthesized 502.
					retained = &walkAnswer{kind: answerInvalid, invalid: invalidReadFailed, cand: cand, candIndex: i + 1, egressKind: kindHere, credKey: credKey}
					break
				}
				answer = &walkAnswer{kind: answerInvalid, invalid: invalidReadFailed, cand: cand, candIndex: i + 1, credKey: credKey}
				break walk
			}
			if len(bodyBytes) > int(maxBufferedResponseBytes) || !json.Valid(bodyBytes) {
				// Over-cap and unparseable are separate protocol causes, and
				// the policy can name them apart: both are unusable answers
				// that share a retry-by-default disposition and the same
				// upstream_invalid_response reason token, but a configuration
				// that says "stop on an unparseable body, move on when a
				// provider sends more than this proxy will hold" is a policy
				// the matrix must be able to state.
				protocolCause := recovery.ProtocolInvalidResponse
				if len(bodyBytes) > int(maxBufferedResponseBytes) {
					protocolCause = recovery.ProtocolOversizedResponse
				}
				elapsed := eng.Now().Sub(attemptStart)
				dec := eng.Observe(recovery.Observation{
					Class:            recovery.FailureProtocol,
					ProtocolCause:    protocolCause,
					Streaming:        stream,
					Committed:        answer != nil,
					CandidateIndex:   i + 1,
					CandidateAttempt: attempt,
					RetryIndex:       retryIndex,
					Elapsed:          eng.Now().Sub(candStart).Milliseconds(),
				})
				lastRuleID = dec.RuleID
				act := walkAction(dec.Action, i+1 < len(m.Chain))
				// No error object rides this event on purpose: the body was
				// read whole, it simply is not usable. The pair below names
				// which way it is unusable — the same closed-set token the
				// matrix row keyed on, and the same error_class the 502
				// envelope reports when the walk finalizes here.
				event := withPolicyFields(withAttemptFields(log.Warn().Str("public_model", model).
					Str("upstream", origin(&upstream)).
					Int("upstream_status", status).
					Str("content_type", logSafeContentType(resp.Header.Get(contentTypeHeader))).
					Str("error_class", "upstream_invalid_response").
					Str("error_cause", protocolCause).
					Str("failure_origin", "protocol"),
					providerAttempts, i+1, attempt, elapsed),
					dec.RuleID, lastPolicyHash, snap.Gen(), exchangeBefore+lastEgressAttempt, eng.Budget().RequestRemaining()).
					Str("disposition", act.String()).
					Str("reason", dec.Reason)
				withCredentialFields(event, credKey).Msg("upstream_invalid_response")
				if !waitOut(dec) {
					return
				}
				if act == recovery.ActionRetry {
					continue
				}
				if act == recovery.ActionFallback {
					// Same as the read failure: a 2xx arrived, only the body
					// is unusable — the shape stays the retained answer.
					retained = &walkAnswer{kind: answerInvalid, invalid: invalidBody, cand: cand, candIndex: i + 1, egressKind: kindHere, credKey: credKey}
					break
				}
				answer = &walkAnswer{kind: answerInvalid, invalid: invalidBody, cand: cand, candIndex: i + 1, credKey: credKey}
				break walk
			}
			// A valid 2xx answer: committed. The body rides the answer
			// struct to the rewrite; no retry follows commitment.
			answer = &walkAnswer{kind: answerBuffered, body: bodyBytes, header: resp.Header, status: status, cand: cand, candIndex: i + 1, credKey: credKey}
			break walk
		}
	}

	if answer == nil && retained != nil {
		// The last answering candidate's budget ran out, the walk moved on,
		// and every remaining candidate failed before answering (transport
		// error, exhausted pool). The client's answer is the last received
		// HTTP answer — the retained one — not a synthesized 502: the walk
		// never went unreachable, a provider answered and this proxy hands
		// it through. The retained answer's candidate becomes the final
		// provider for the completion record, and provider_exhausted stays
		// false — the exhaustion vocabulary names walks that received no
		// answer at all.
		answer = retained
		finalProvider = answer.cand.Label()
		lastCand = answer.cand
		lastCandIndex = answer.candIndex
		// The completion record describes the candidate whose answer the
		// client actually receives. A later candidate may have failed before
		// answering, but its policy did not produce the retained response and
		// must not overwrite the answer's policy identity.
		lastPolicyHash = answer.cand.RecoveryHash
		// And the same for the credential: the answer's own key, recorded
		// on the answer when it was produced, not the last failed
		// attempt's.
		lastCredentialID = answer.credKey
		ansEgressKind = answer.egressKind
	}
	if answer == nil {
		// Every budgeted candidate failed without answering. The client
		// gets the canonical unreachable envelope; the ERROR carries the
		// last attempt's failure, sanitized, with the pool's report when
		// that candidate routed through one. The class names the layer
		// that ran out — provider_exhausted for the walk, egress_exhausted
		// for a pool that could not dial at all — and the cause stays the
		// final bounded token.
		providerExhausted = true
		class, cause := "provider_exhausted", lastFailureCause
		ruleID := lastRuleID
		// The walk exhausted through one of three origins: the exchange
		// envelope refused another dial (envelope), a pool could not dial at
		// all (transport), or repeated transport failures spent the budgets
		// (transport).
		failureOrigin := "transport"
		switch {
		case budgetStopped || (egress != nil && egress.BudgetExhausted):
			// The exchange envelope, not a provider, ended the walk: the next
			// outbound exchange was refused before it was dialed, so there is
			// no endpoint to blame and no transport cause to report. The
			// engine's own envelope decision supplies the identity — request
			// scope when the request's ceiling was the binding one, candidate
			// scope otherwise — and neither is a matrix rule.
			class, cause = "provider_exhausted", recovery.CauseExchangeBudget
			failureOrigin = "envelope"
			if eng.Budget().Exhausted() == recovery.ExhaustionRequest {
				ruleID = recovery.RuleIDBudgetRequest
			} else {
				ruleID = recovery.RuleIDBudgetCandidate
			}
		case egress != nil && egress.Exhausted:
			// A pool that dialed nothing: every member was skipped, so no
			// endpoint owns the failure.
			class, cause = "egress_exhausted", recovery.CauseNoEligibleEndpoint
		}
		event := withProviders(withEgress(log.Error())).Err(sanitizeUpstreamError(lastUerr, lastUpstream)).
			Str("public_model", model).
			Str("provider", finalProvider).
			Str("upstream", origin(lastUpstream)).
			Str("error_class", class).
			Str("error_cause", cause).
			Str("failure_origin", failureOrigin).
			Str("policy_rule_id", ruleID).
			Int("provider_attempt", providerAttempts)
		// The send state rides only a transport-origin exhaustion: an
		// envelope refusal dialed nothing, and an answer was committed.
		if failureOrigin == "transport" && lastSendState != "" {
			event = event.Str("send_state", lastSendState)
		}
		if lastEgressAttempt > 0 {
			event = event.Int("egress_attempt", lastEgressAttempt)
		}
		event.Bool("provider_exhausted", true).
			Msg("upstream_request_failed")
		outcome = "upstream_unreachable"
		reject(http.StatusBadGateway, []byte(envelopeUpUnreach), nil)
		return
	}
	if answer.resp != nil {
		defer func() { _ = answer.resp.Body.Close() }()
	}

	if answer.kind == answerInvalid {
		// The final retained answer was a 200-shaped response nobody can
		// use — unparseable, over-cap, a read that failed mid-answer, or
		// an error body whose capture stalled. Its evidence event already
		// fired in-walk with the terminal disposition; the client answer
		// is the canonical 502, never half a provider body.
		if answer.invalid == invalidBody {
			outcome = "upstream_invalid_response"
		} else {
			outcome = "upstream_read_failed"
		}
		reject(http.StatusBadGateway, []byte(envelopeUpInvalid), nil)
		return
	}

	if answer.kind == answerHTTPError {
		// Upstream HTTP errors are normalized, never relayed raw: the
		// provider's status survives (a 429 answers 429 — collapsing it
		// into a 502 would fog the root cause), but the client body is
		// always the canonical JSON envelope. The evidence event with the
		// terminal disposition already fired in-walk; what remains is the
		// client answer: the upstream's own status, the operational
		// headers from the relay allow-list, and a Content-Type that
		// describes the body the client actually receives.
		body, berr := answer.ev.envelopeBytes()
		if berr != nil {
			// Unreachable for an all-string envelope, but the fallback must
			// still be a canonical body — never raw upstream bytes.
			body = []byte(envelopeUpInvalid)
		}
		copyRelayHeaders(sw.Header(), answer.header)
		sw.Header().Set(contentTypeHeader, envelopeJSONType)
		sw.WriteHeader(answer.status)
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

	if answer.kind == answerVerbatim {
		// Verbatim relay: redirects (3xx, which CheckRedirect never follows)
		// and the two body-less statuses, which are valid upstream answers.
		// A 204 or an unexpected 3xx is the upstream's answer; turning it
		// into a 502 would fog the root cause. Status and body relayed byte
		// for byte, whatever the content type. (4xx/5xx no longer reach this
		// branch — the normalized-error path owns them. A status above 599,
		// which no spec defines but a broken peer can emit, stays here: it
		// is not ours to reshape either.)
		copyRelayHeaders(sw.Header(), answer.resp.Header)
		sw.WriteHeader(answer.resp.StatusCode)
		if _, err := copyVerbatim(sw, answer.resp.Body); err != nil {
			// The relay did not finish — the outcome says so. A failure on
			// the client side (write error, or the canceled request context
			// surfacing through the upstream read) is a disconnect; anything
			// else died reading the upstream.
			// Same split as the SSE path: the client-side branch carries this
			// package's own clientWriteError (static text), the upstream one
			// carries whatever the transport surfaced — which may quote the
			// upstream's bytes, so only that branch takes the no-echo rule.
			level, phase := log.Error(), "upstream_read"
			eventErr := sanitizeUpstreamError(err, lastUpstream)
			if clientSide(err) {
				level, phase, outcome = log.Warn(), "client_write", "client_disconnected"
				eventErr = err
			} else {
				outcome = "upstream_read_failed"
			}
			level.Err(eventErr).Str("public_model", model).Str("phase", phase).
				Msg("relay_copy_failed")
			complete()
			return
		}
		outcome = "relayed"
		log.Debug().Int64("bytes_out", sw.bytes).Msg("client_write_completed")
		complete()
		return
	}

	if answer.kind == answerSSE {
		// Incremental SSE passthrough. The candidate was committed in-walk
		// (headers selected, no retry follows); the 2xx status is written
		// here, and any subsequent failure only truncates the stream,
		// never switches the response to an error body.
		copyRelayHeaders(sw.Header(), answer.resp.Header)
		sw.WriteHeader(answer.resp.StatusCode)
		streamed = true
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
		//
		// When metering is on, the relay rewrite is wrapped so each gated
		// data line is observed BEFORE the client-facing rewrite: the meter
		// reads the upstream's own usage object, never the synthesized
		// reasoning tokens the rewriter may add for the client. Streaming
		// adoption inside the capture is last-wins, so cumulative usage
		// chunks converge on the authoritative final object.
		relayRewrite := rewriteOut
		if usageCapture != nil {
			relayRewrite = func(payload []byte) []byte {
				usageCapture.Observe(payload)
				return rewriteOut(payload)
			}
		}
		events := 0
		stats, err := CopySSE(dst, answer.resp.Body, relayRewrite, func() {
			events++
			if events%sseProgressEvery == 0 {
				log.Debug().Str("public_model", model).
					Int("events", events).Int64("bytes_out", sw.bytes).
					Msg("stream_event_progress")
			}
			afterEvent()
		}, stripKeys)
		var pings int
		if heartbeat != nil {
			heartbeat.stopAndWait()
			pings = heartbeat.pingCount()
		}
		if err != nil {
			phase := "upstream_read"
			outcome = "stream_truncated"
			// Only the upstream_read phase carries a peer-derived error, and
			// that is the one the no-echo rule governs: a truncated read
			// surfaces the transport's own parse failures, which interpolate
			// the upstream's bytes. The other two cases are this package's
			// typed errors with static text — collapsing them would replace
			// "the client went away" with "upstream transport error".
			eventErr := sanitizeUpstreamError(err, lastUpstream)
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
				eventErr = err
			case errors.Is(err, ErrSSELineTooLong) || errors.Is(err, ErrSSEEventTooLarge):
				// The upstream crossed a bounded-relay cap: a hostile or
				// broken peer, stopped cleanly at the wall. The logged
				// error carries counts only, never the bytes themselves.
				phase = "upstream_limit"
				outcome = "stream_limit_exceeded"
				eventErr = err
			}
			log.Warn().Err(eventErr).Str("public_model", model).Str("phase", phase).
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

	// Buffered answer (answerBuffered): the committed 2xx body was already
	// read and validated inside the walk — that is what made a malformed
	// answer retryable before commitment. What remains is the meter
	// observation, the rewrite, and the single buffered write.
	log.Debug().Int64("bytes_in", int64(len(answer.body))).Msg("response_transform_started")
	// The meter reads the upstream's own usage object from the raw body,
	// before any rewrite: what the client sees after the synthesized
	// reasoning tokens are spliced in is never what the meter records.
	if usageCapture != nil {
		usageCapture.Observe(answer.body)
	}
	rewritten := rewriteOut(answer.body)
	log.Debug().Int64("bytes_out", int64(len(rewritten))).Msg("response_transform_completed")
	copyRelayHeaders(sw.Header(), answer.header)
	sw.WriteHeader(answer.status)
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

// stripSegments flattens a configured strip list into the segment-list shape
// the inject engine walks, without re-parsing: each StripPath already carries
// its decoded segments. A nil or empty list has no segments and is handled
// before this is ever called.
func stripSegments(strips []config.StripPath) [][]string {
	out := make([][]string, 0, len(strips))
	for _, s := range strips {
		out = append(out, s.Segments)
	}
	return out
}

// walkAction folds a decided action against the chain it will be executed
// on. The engine cannot see the chain — it decides what a failure means, not
// what is left to try — so the one case it cannot know is a fallback with no
// candidate left to fall to. That is a terminal outcome (the walk ends on
// this attempt), and reporting it as a fallback would claim a move that
// never happened. The fold is what the emitted disposition token carries,
// and what the walk's control flow follows.
func walkAction(a recovery.Action, hasNext bool) recovery.Action {
	if a == recovery.ActionFallback && !hasNext {
		return recovery.ActionTerminal
	}
	return a
}

// transportClassOf maps the transport package's failure class onto the
// recovery vocabulary the matrix matches on. The mapping lives here, on the
// handler's side of the seam: the transport layer reports what happened to a
// socket and never learns what a provider policy is, and the recovery
// package never learns what a socket is. ClassCanceled has no transport
// class here because a caller-terminated failure is reclassified as
// FailureCaller outright — it belongs to the request context, not to the
// endpoint.
func transportClassOf(c transport.Class) recovery.TransportClass {
	switch c {
	case transport.ClassConnection:
		return recovery.TransportClassConnection
	case transport.ClassTimeout:
		return recovery.TransportClassTimeout
	case transport.ClassProxyConnect:
		return recovery.TransportClassProxyConnect
	case transport.ClassProxyAuth:
		return recovery.TransportClassProxyAuth
	default:
		return recovery.TransportClassNone
	}
}

// withPolicyFields adds the recovery-policy facts one attempt-scoped
// evidence event carries: the identity of the decision it produced (empty
// when no decision did — a dial that failed before any disposition was
// reached), the effective policy hash and snapshot generation the attempt
// ran under, the one-based index of the real outbound exchange across the
// whole request, and how much of the REQUEST-wide exchange envelope was left
// after the attempt — the envelope the field is named for, never the tighter
// of the two: a candidate that has spent its own envelope would otherwise
// report zero headroom on a request with most of its budget untouched.
func withPolicyFields(ev *zerolog.Event, ruleID, policyHash string, generation uint64, exchange, remaining int) *zerolog.Event {
	return ev.Str("policy_rule_id", ruleID).
		Str("policy_hash", policyHash).
		Uint64("policy_generation", generation).
		Int("upstream_exchange", exchange).
		Int("request_exchange_budget_remaining", remaining)
}

// withAttemptFields adds the per-attempt identity every evidence event
// carries: the one-based PROVIDER attempt (provider_attempt) — which is NOT
// the exchange count, since one provider attempt may fan out across several
// pooled egress dials, each of those being its own upstream_exchange — the
// candidate's one-based chain position, the attempt's place in that
// candidate's own budget (retry_index = candidate_attempt−1), and how long
// the attempt took to reach its outcome.
func withAttemptFields(ev *zerolog.Event, providerAttempt, candIndex, candAttempt int, elapsed time.Duration) *zerolog.Event {
	return ev.Int("provider_attempt", providerAttempt).
		Int("candidate_index", candIndex).
		Int("candidate_attempt", candAttempt).
		Int("retry_index", candAttempt-1).
		Int64("elapsed_ms", elapsed.Milliseconds())
}

// withCredentialFields adds the id of the upstream credential one
// attempt-scoped event's attempt went out with: a rotation fact the
// operator correlates 429 marks and per-key behavior against. The id is
// configuration data validated to bounded token characters — never the
// key value, never a caller's partner key id (a different axis), and
// omitted entirely on candidates without a credential pool, whose events
// keep their historical shape.
func withCredentialFields(ev *zerolog.Event, credKey string) *zerolog.Event {
	if credKey == "" {
		return ev
	}
	return ev.Str("upstream_credential_id", credKey)
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

// dialedAttempt reports whether the attempt that just failed reached the
// wire. The direct path always dialed — its Do error is the dial's own — and
// a pooled path dialed when the pool reports at least one attempt; a pool
// that skipped every member (static gates, health, concurrency) returns its
// exhaustion sentinel with zero attempts, and that sentinel is a pool-level
// condition rather than an endpoint's failure. Reading its classification as
// wire evidence would attach a send state to a dial that never happened.
func dialedAttempt(pooled bool, info *transport.AttemptInfo) bool {
	if !pooled || info == nil {
		return true
	}
	return info.Attempts > 0
}

// sanitizeUpstreamError rebuilds a client.Do error without the full request
// URL: *url.Error.Error() quotes it verbatim, query string included. The
// nested url parse/escape errors quote raw bytes too (the offending escape
// sequence, the rejected host), so they are replaced with static text under
// the same no-echo rule — and so is every nested error whose text is not
// known to be echo-free: the transport's response-parsing failures (a
// malformed MIME header line, a bad chunk size) quote the upstream's own
// bytes verbatim, and those reach no log line at any level (#37).
//
// The no-echo rule is TOTAL: a value that arrives without a request URL
// attached (an upstream body read surfacing through net/textproto, a pool's
// attempt error, a wrapped shape this function does not know) is held to the
// same allow-list rather than passed through on the assumption that only
// client.Do errors reach here. Text that is not provably echo-free collapses
// to static text; the error_class/error_cause tokens carry the meaning.
func sanitizeUpstreamError(err error, endpoint *url.URL) error {
	if err == nil {
		return nil
	}
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
	if !transportErrorTextSafe(err) {
		return errors.New("upstream transport error")
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
	// The transport package's own sentinels are `errors.New` literals — static
	// by construction — and reach this allow-list explicitly, because a pool
	// that dialed nothing is the one exhaustion an operator reads by name.
	if errors.Is(err, transport.ErrExhausted) {
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
// explicit write failure, or the request context surfacing through the
// upstream read — the context ends when the client goes away or its deadline
// expires, never on an upstream hiccup.
//
// Cancellation and an expired deadline are the SAME ownership answer here, as
// they are everywhere else in this service: ownership is read from the
// request context, never from the error chain. Reporting an expired caller
// deadline as an upstream read failure would blame the provider for a
// request nobody is waiting for any more.
func clientSide(err error) bool {
	var cwe *clientWriteError
	if errors.As(err, &cwe) {
		return true
	}
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
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

// models is GET /v1/models. It lists public models from the snapshot's
// model mapping. Auth is required; non-GET → 405. Each model object has
// only id and created (0).
func (h *injectorHandler) models(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	if r.Method != http.MethodGet {
		// Keep the 405-before-401 ordering and outside-lifecycle semantics of
		// the model-serving POST routes.
		h.log.Debug().Str("api", "models").Str("method", r.Method).
			Str("path", r.URL.Path).Str("remote_addr", r.RemoteAddr).
			Msg("request_method_not_allowed")
		_ = writeEnvelope(w, http.StatusMethodNotAllowed, envelopeBadMethod)
		return
	}

	snap := h.store.Load()
	defer func() { _ = r.Body.Close() }()
	sw := &statusWriter{ResponseWriter: w}
	log := h.log.With().Str("request_id", newRequestID()).Str("api", "models").Logger()
	log.Debug().Str("method", r.Method).Str("path", r.URL.Path).
		Str("remote_addr", r.RemoteAddr).Msg("request_received")

	outcome := "listed"
	complete := func() {
		log.Info().Int("status", sw.status).Str("outcome", outcome).
			Bool("stream", false).Int64("bytes_in", 0).Int64("bytes_out", sw.bytes).
			Int64("duration_ms", time.Since(start).Milliseconds()).
			Uint64("config_generation", snap.Gen()).Msg("request_completed")
	}
	reject := func(status int, body string) {
		if err := writeEnvelope(sw, status, body); err != nil {
			outcome = "client_disconnected"
			log.Warn().Err(err).Int64("bytes_out", sw.bytes).Msg("client_write_failed")
		}
		complete()
	}

	token, ok := bearerToken(r.Header.Get("Authorization"))
	if !ok {
		outcome = "unauthorized"
		reject(http.StatusUnauthorized, envelopeAuthMissing)
		return
	}
	_, reason, aerr := h.auth.For(snap).Authenticate(r.Context(), token)
	if aerr != nil {
		log.Warn().Str("error_class", auth.StoreErrorClass(aerr)).Msg("auth_backend_failed")
	}
	if reason != auth.ReasonOK {
		outcome = "unauthorized"
		reject(http.StatusUnauthorized, envelopeAuthInvalid)
		return
	}

	// Public model names, sorted for determinism. No upstream call: the
	// mapping is the catalog, and proxying the upstream's list would
	// advertise names the mapping does not cover.
	models := snap.Models()
	ids := make([]string, 0, len(models))
	for k := range models {
		ids = append(ids, k)
	}
	sort.Strings(ids)

	// Wire shape: each entry carries only id and created (the injector has
	// no creation metadata, so created is always 0). The struct keeps the
	// field order stable on the wire.
	type modelItem struct {
		ID      string `json:"id"`
		Created int64  `json:"created"`
	}
	data := make([]modelItem, 0, len(ids))
	for _, id := range ids {
		data = append(data, modelItem{ID: id, Created: 0})
	}
	type modelsList struct {
		Object string      `json:"object"`
		Data   []modelItem `json:"data"`
	}
	// json.Marshal cannot fail for this fixed string-and-integer shape. The
	// full JSON response is generated locally, so no error propagation is
	// possible after a successful write — a failed write is client-owned.
	body, _ := json.Marshal(modelsList{Object: "list", Data: data})
	sw.Header().Set(contentTypeHeader, envelopeJSONType)
	sw.WriteHeader(http.StatusOK)
	if _, err := sw.Write(body); err != nil {
		outcome = "client_disconnected"
		log.Warn().Err(err).Int64("bytes_out", sw.bytes).Msg("client_write_failed")
	}
	complete()
}
