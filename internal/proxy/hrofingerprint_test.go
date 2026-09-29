package proxy

// ExecutionFingerprint: a semantic, noise-free record of
// what a request OBSERVABLY did, suitable for differential (old-vs-new)
// comparison across a refactor.
//
// The design rule, and how it is enforced:
//
//	An ExecutionFingerprint records only what a CLIENT, a LOG READER or a
//	METERED-EVENT READER can observe, and only in a form independent of
//	identity, ordering and time. Concretely:
//
//	  - no pointer fields, and no capture path may store one
//	  - no time.Time, no measured duration, no elapsed value
//	  - no goroutine ids, no channel identity, no interleaving-dependent seq
//	  - no Go map in the stored form: every map becomes a SORTED slice
//	    (NewHeader sorts; Canonical re-sorts)
//	  - no raw error text and no raw provider bytes: bodies are exact under
//	    a byte ceiling and digested above it
//	  - no credential VALUES, ever: only the configured key id, which is
//	    operator-chosen from [A-Za-z0-9._:-]
//
// Determinism mechanism: Fingerprint.Canonical() renders a newline-delimited
// key=value document in a FIXED key order, with json.Marshal (which sorts map
// keys) and explicit sorts on every slice. SHA is sha256 of that. Two runs
// of one scenario agree; two different scenarios differ; a reordering, an
// address, a re-timed microsecond, or a fresh request id cannot move it.

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"
	"strings"

	"openai-compatible-injector/internal/usage"
)

// ExactLimit is the byte ceiling under which a body is kept verbatim.
// Above it only a digest survives, so a 4 MiB SSE stream cannot make the
// fingerprint unreadable while a one-byte change still moves the SHA.
const ExactLimit = 4096

// Body is a response body in a diffable, bounded form.
type Body struct {
	Exact     []byte `json:"exact,omitempty"`
	Digest    string `json:"sha256"`
	Len       int    `json:"len"`
	Truncated bool   `json:"truncated,omitempty"`
}

// NewBody chooses exact-or-digest.
func NewBody(b []byte) Body {
	out := Body{Len: len(b)}
	sum := sha256.Sum256(b)
	out.Digest = hex.EncodeToString(sum[:])
	if len(b) <= ExactLimit {
		out.Exact = append([]byte(nil), b...)
	} else {
		out.Truncated = true
	}
	return out
}

// Field is one recorded header: canonical (lowercased) name, sorted values.
type Field struct {
	Name   string   `json:"name"`
	Values []string `json:"values"`
}

// Header is a reduced, ordered http.Header. volatileHeaders are dropped by
// VALUE but recorded by NAME in Dropped, because their presence is
// observable while their value is a function of wall clock or per-process
// randomness — the precise combination that would make every run differ.
type Header struct {
	Fields  []Field  `json:"fields"`
	Dropped []string `json:"dropped,omitempty"`
}

var volatileHeaders = map[string]bool{
	"date":                           true,
	"x-request-id":                   true,
	"traceparent":                    true,
	"tracestate":                     true,
	"x-amzn-trace-id":                true,
	"x-b3-traceid":                   true,
	"cf-ray":                         true,
	"x-envoy-upstream-service-time":  true,
	"x-envoy-response-time":          true,
	"x-envoy-attempt-count":          true,
	"content-length":                 true, // derived from Len, recorded in Body
	"transfer-encoding":              true, // framing, not semantics
	"connection":                     true,
	"keep-alive":                     true,
	"proxy-authenticate":             true,
	"proxy-connection":               true,
	"upgrade":                        true,
	"trailer":                        true,
	"server":                         true, // identifies the peer, not the behaviour
	"x-envoy-expected-rq-timeout-ms": true,
	"x-request-duration":             true,
}

// NewHeader reduces a header map to its observable, ordered form.
func NewHeader(h map[string][]string) Header {
	var out Header
	names := make([]string, 0, len(h))
	for k := range h {
		names = append(names, k)
	}
	sort.Strings(names)
	for _, k := range names {
		lk := strings.ToLower(k)
		if volatileHeaders[lk] {
			out.Dropped = append(out.Dropped, lk)
			continue
		}
		vals := append([]string(nil), h[k]...)
		sort.Strings(vals)
		out.Fields = append(out.Fields, Field{Name: lk, Values: vals})
	}
	sort.Strings(out.Dropped)
	return out
}

// Attempt is one logical outbound attempt as observed. Every field is a
// closed-set token, a count, or an operator-chosen name.
type Attempt struct {
	Index         int    `json:"i"` // 1-based, matches log provider_attempt
	Candidate     int    `json:"c"` // 1-based chain position
	CandidateName string `json:"cand,omitempty"`
	UpstreamModel string `json:"upstream_model,omitempty"`
	// SentBody is the exact bytes the attempt went out with. It is the
	// injection/rename regression detector and is bounded by the fact that
	// request bodies in this suite are small.
	SentBody string `json:"sent_body,omitempty"`
	// Method is the outbound HTTP method.
	Method string `json:"method,omitempty"`
	// CredentialKey is the CONFIGURED key id from the sent header, never
	// the value. Resolved by the harness, not by the handler.
	CredentialKey string `json:"cred,omitempty"`
	// Result is a closed set:
	//   answer | transport_failure | refused | local
	Result string `json:"result"`
	Status int    `json:"status,omitempty"`
	// ErrorClass / ErrorCause / SendState are transport's own closed sets
	// (internal/transport/errors.go:61-80, :182-200, :280-294).
	ErrorClass string `json:"err_class,omitempty"`
	ErrorCause string `json:"err_cause,omitempty"`
	SendState  string `json:"send_state,omitempty"`
	// Disposition is the recovery Action: commit | retry | fallback | terminal
	Disposition string `json:"disposition,omitempty"`
	RuleID      string `json:"rule,omitempty"`
}

// StreamEvent is one SSE frame as observed by the relay: the event name and
// a digest of the payload, never the payload. Ten thousand deltas stay a
// bounded, order-sensitive record.
type StreamEvent struct {
	Name string `json:"name"`
	// SHA is over the payload as RELAYED to the client (post-rewrite,
	// post-strip), which is the only form a client can observe.
	SHA string `json:"sha"`
	Len int    `json:"len"`
	// Terminal marks data: [DONE] (chat) or event: response.completed.
	Terminal bool `json:"terminal,omitempty"`
}

// UsageFacts is the semantic projection of a usage.Event. Absent usage stays
// nil, never 0 — the rule the whole metering surface rests on.
type UsageFacts struct {
	Count            int    `json:"events"`
	API              string `json:"api,omitempty"`
	Stream           bool   `json:"stream"`
	Provider         string `json:"provider,omitempty"`
	UpstreamModel    string `json:"upstream_model,omitempty"`
	PublicModel      string `json:"public_model,omitempty"`
	HTTPStatus       int    `json:"status"`
	Outcome          string `json:"outcome,omitempty"`
	PromptTokens     *int64 `json:"prompt,omitempty"`
	CompletionTokens *int64 `json:"completion,omitempty"`
	TotalTokens      *int64 `json:"total,omitempty"`
	BytesIn          int64  `json:"bytes_in"`
	BytesOut         int64  `json:"bytes_out"`
	ProviderAttempts int    `json:"provider_attempts"`
	EgressAttempts   int    `json:"egress_attempts"`
	EgressKind       string `json:"egress_kind,omitempty"`
	PartnerID        string `json:"partner_id,omitempty"`
	KeyID            string `json:"key_id,omitempty"`
}

// LogEvent is one emitted log line reduced to its semantic identity. Only
// the fields in logFields are captured; request_id, remote_addr,
// duration_ms, elapsed_ms and every free-form string are structurally
// excluded, which is what makes the log projection reproducible.
type LogEvent struct {
	Slug   string            `json:"slug"`
	Level  string            `json:"level"`
	Fields map[string]string `json:"fields,omitempty"`
}

// Fingerprint is the whole observable surface of one request.
type Fingerprint struct {
	Status  int    `json:"status"`
	Headers Header `json:"headers"`
	Body    Body   `json:"body"`
	// Commit is one of none | headers | first_byte, recorded from the
	// ResponseWriter's own transitions rather than from handler state.
	Commit string `json:"commit"`
	Stream bool   `json:"stream"`

	Attempts             []Attempt `json:"attempts,omitempty"`
	CandidatesEntered    int       `json:"candidates_entered"`
	SameCandidateRetries int       `json:"same_candidate_retries"`
	Fallbacks            int       `json:"fallbacks"`
	Exchanges            int       `json:"exchanges"`
	FinalCandidate       int       `json:"final_candidate"`
	FinalProvider        string    `json:"final_provider,omitempty"`
	ProviderExhausted    bool      `json:"provider_exhausted"`
	Outcome              string    `json:"outcome,omitempty"`

	// CredentialKeys is the ORDERED list of configured key ids the request
	// went out under, repeats included: rotation order is observable.
	// Values never appear.
	CredentialKeys []string `json:"credential_keys,omitempty"`
	// EgressTargets is the ordered list of egress members dialed.
	EgressTargets []string `json:"egress_targets,omitempty"`

	StreamEvents []StreamEvent `json:"stream_events,omitempty"`
	Usage        []UsageFacts  `json:"usage,omitempty"`
	Logs         []LogEvent    `json:"logs,omitempty"`
}

// Canonical renders a deterministic byte document. The key order is FIXED
// here and is the only ordering that matters. Logs are re-sorted by
// (slug, level, fields) so two emitters of the same slug interleaving
// differently cannot move the digest, while a genuine insertion or deletion
// of a log line still does.
func (f Fingerprint) Canonical() string {
	var b strings.Builder
	w := func(k string, v any) {
		enc, _ := json.Marshal(v)
		b.WriteString(k)
		b.WriteByte('=')
		b.Write(enc)
		b.WriteByte('\n')
	}
	w("status", f.Status)
	w("commit", f.Commit)
	w("stream", f.Stream)
	w("headers", f.Headers)
	w("body", f.Body)
	w("candidates_entered", f.CandidatesEntered)
	w("same_candidate_retries", f.SameCandidateRetries)
	w("fallbacks", f.Fallbacks)
	w("exchanges", f.Exchanges)
	w("final_candidate", f.FinalCandidate)
	w("final_provider", f.FinalProvider)
	w("provider_exhausted", f.ProviderExhausted)
	w("outcome", f.Outcome)
	w("attempts", f.Attempts)
	w("credential_keys", f.CredentialKeys)
	w("egress_targets", f.EgressTargets)
	w("stream_events", f.StreamEvents)
	w("usage", f.Usage)

	logs := append([]LogEvent(nil), f.Logs...)
	sort.SliceStable(logs, func(i, j int) bool {
		a, _ := json.Marshal(logs[i].Fields)
		c, _ := json.Marshal(logs[j].Fields)
		if logs[i].Slug != logs[j].Slug {
			return logs[i].Slug < logs[j].Slug
		}
		if logs[i].Level != logs[j].Level {
			return logs[i].Level < logs[j].Level
		}
		return string(a) < string(c)
	})
	w("logs", logs)
	return b.String()
}

// SHA is the stable one-line identity for differential comparison.
func (f Fingerprint) SHA() string {
	sum := sha256.Sum256([]byte(f.Canonical()))
	return hex.EncodeToString(sum[:])
}

// JSON is the human-diffable rendering used in a golden test.
func (f Fingerprint) JSON() string {
	enc, _ := json.MarshalIndent(f, "", "  ")
	return string(enc)
}

// projectUsage reduces a usage.Event to its semantic facts, dropping
// identity and time at the boundary so no downstream code can reintroduce
// them.
func projectUsage(ev usage.Event) UsageFacts {
	return UsageFacts{
		Count:            1,
		API:              ev.API,
		Stream:           ev.Stream,
		Provider:         ev.Provider,
		UpstreamModel:    ev.UpstreamModel,
		PublicModel:      ev.PublicModel,
		HTTPStatus:       ev.HTTPStatus,
		Outcome:          ev.Outcome,
		PromptTokens:     ev.PromptTokens,
		CompletionTokens: ev.CompletionTokens,
		TotalTokens:      ev.TotalTokens,
		BytesIn:          ev.BytesIn,
		BytesOut:         ev.BytesOut,
		ProviderAttempts: ev.ProviderAttempts,
		EgressAttempts:   ev.EgressAttempts,
		EgressKind:       ev.EgressKind,
		PartnerID:        ev.PartnerID,
		KeyID:            ev.KeyID,
	}
}
