package proxy

import "strconv"

// The client-facing error dialect, selected once per request from the API
// surface being served. The proxy answers two shapes: the OpenAI envelope
// every Chat Completions/Responses client (and every OpenAI SDK's error
// parser) already pins byte-exact, and the Anthropic error envelope a
// Messages client expects.
//
// This is a VALUE, not a parameter: serve already receives the api argument
// and already branches on it, so resolving the dialect once at the top of
// serve threads it through every reject site without touching serve's
// signature — the two direct test callers of serve keep compiling, and no
// call site has to remember which shape it is on.
//
// Two properties make the substitution safe for the OpenAI routes:
// openAIDialect wraps the existing envelope constants verbatim (no literal
// is retyped, so the bytes cannot drift), and dialectFor returns it for
// everything that is not the Messages surface — so a chat or responses
// request answers exactly the bytes it answered before.
//
// Scope: this covers only the envelopes serve generates. The catch-all 404,
// the /healthz 405 and the models route are separate handlers with their own
// (OpenAI) constants and deliberately do not participate — an unknown path
// is not a Messages request, and the models surface is OpenAI-shaped by
// contract.
type envelopeDialect struct {
	badMethod   string
	authMissing string
	authInvalid string
	invalidReq  string
	missingMod  string
	tooLarge    string
	atCapacity  string
	upInvalid   string
	upUnreach   string

	// modelNotFound assembles the 404 whose message interpolates the
	// requested model; both arms must go through marshalEnvelopeJSON so the
	// name reaches the client byte-exact and unescaped.
	modelNotFound func(model string) ([]byte, error)

	// upstreamErr renders the normalized envelope for a received upstream
	// 4xx/5xx. Only the status is carried — never provider text.
	upstreamErr func(ev upstreamErrorEvidence) ([]byte, error)
}

var openAIDialect = envelopeDialect{
	badMethod:   envelopeBadMethod,
	authMissing: envelopeAuthMissing,
	authInvalid: envelopeAuthInvalid,
	invalidReq:  envelopeInvalidReq,
	missingMod:  envelopeMissingMod,
	tooLarge:    envelopeTooLarge,
	atCapacity:  envelopeAtCapacity,
	upInvalid:   envelopeUpInvalid,
	upUnreach:   envelopeUpUnreach,

	modelNotFound: modelNotFoundEnvelope,
	upstreamErr:   func(ev upstreamErrorEvidence) ([]byte, error) { return ev.envelopeBytes() },
}

// The Anthropic dialect. The shape is {"type":"error","error":{"type","message"}}
// — an Anthropic error object carries a type and a message and nothing else,
// so there is no param and no code member to carry; the diagnosis an
// operator needs lives in the log event, whose vocabulary is closed-set
// anyway. No body carries the request id: every one of these is a static
// literal (or, for the two assembled ones, a literal plus an interpolation
// the same no-echo rules already govern), and the id reaches the client
// through the X-Request-Id header like every other surface.
var anthropicDialect = envelopeDialect{
	badMethod:   `{"type":"error","error":{"type":"invalid_request_error","message":"method not allowed"}}`,
	authMissing: `{"type":"error","error":{"type":"authentication_error","message":"you must provide an API key in the Authorization header (Bearer <key>) or the x-api-key header"}}`,
	authInvalid: `{"type":"error","error":{"type":"authentication_error","message":"invalid API key"}}`,
	invalidReq:  `{"type":"error","error":{"type":"invalid_request_error","message":"invalid JSON in request body"}}`,
	missingMod:  `{"type":"error","error":{"type":"invalid_request_error","message":"you must provide a model parameter"}}`,
	tooLarge:    `{"type":"error","error":{"type":"request_too_large","message":"request body too large"}}`,
	atCapacity:  `{"type":"error","error":{"type":"api_error","message":"server is out of buffering capacity"}}`,
	upInvalid:   `{"type":"error","error":{"type":"api_error","message":"upstream returned an invalid response"}}`,
	upUnreach:   `{"type":"error","error":{"type":"api_error","message":"upstream request failed"}}`,

	modelNotFound: anthropicModelNotFound,
	upstreamErr:   anthropicUpstreamErr,
}

// dialectFor resolves the error dialect for an API surface. Everything that
// is not the Messages surface answers the OpenAI envelope, so a surface
// added later inherits the bytes it already produces until it deliberately
// asks for its own.
func dialectFor(api string) envelopeDialect {
	if api == apiMessages {
		return anthropicDialect
	}
	return openAIDialect
}

// anthropicErrorEnvelope is the Anthropic error document. Field order is
// declaration order, so the marshaled bytes are stable: "type" first, then
// the nested error object.
type anthropicErrorEnvelope struct {
	Type  string             `json:"type"`
	Error anthropicErrorBody `json:"error"`
}

type anthropicErrorBody struct {
	Type    string `json:"type"`
	Message string `json:"message"`
}

// anthropicModelNotFound mirrors modelNotFoundEnvelope's message exactly —
// the same interpolation, the same byte-exact and unescaped name — under the
// Anthropic wrapper. Do not route it through a JSON encoder that escapes
// "<" or "&": the model name is configuration, and the 404 body must quote
// it byte-for-byte.
func anthropicModelNotFound(model string) ([]byte, error) {
	return marshalEnvelopeJSON(anthropicErrorEnvelope{
		Type: "error",
		Error: anthropicErrorBody{
			Type:    "not_found_error",
			Message: "The model '" + model + "' does not exist or you do not have access to it.",
		},
	})
}

// anthropicUpstreamErr renders the normalized upstream-error envelope in the
// Anthropic shape: the provider's status picks the error type, the message
// is the same static string the OpenAI arm uses, and no provider byte is
// carried. The type table is Anthropic's own vocabulary, not a mapping of
// the OpenAI "upstream_error" family, because a Messages client dispatches
// on these names.
func anthropicUpstreamErr(ev upstreamErrorEvidence) ([]byte, error) {
	return marshalEnvelopeJSON(anthropicErrorEnvelope{
		Type: "error",
		Error: anthropicErrorBody{
			Type:    anthropicStatusType(ev.status),
			Message: "upstream provider returned HTTP " + strconv.Itoa(ev.status),
		},
	})
}

// anthropicStatusType maps a received upstream status onto an Anthropic
// error type. Classified from the status alone — a closed set, no provider
// input — exactly like the OpenAI arm's code, which is derived the same way.
func anthropicStatusType(status int) string {
	switch {
	case status == 401:
		return "authentication_error"
	case status == 403:
		return "permission_error"
	case status == 404:
		return "not_found_error"
	case status == 413:
		return "request_too_large"
	case status == 429:
		return "rate_limit_error"
	case status >= 500:
		return "api_error"
	default:
		return "invalid_request_error"
	}
}
