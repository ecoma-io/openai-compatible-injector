// Package usage is the metering foundation: one factual record per proxied
// request that reached the provider path, written asynchronously so the
// client response never waits on — or is ever mutated by — the meter.
//
// Facts only, by design: token counts the upstream itself reported, byte
// counts, outcomes, attempt counters. The synthesized thinking-usage the
// proxy shows its clients is never metered; there is no pricing, no
// currency, no quota. Ingestion (Pipeline) and reporting (PGRepository
// queries) are separate surfaces.
package usage

import (
	"crypto/rand"
	"encoding/hex"
	"time"
)

// Event is one metered request. Token fields are deliberately pointers:
// nil means the upstream reported no usage object (or none readable), and
// that absence must survive as SQL NULL rather than a fabricated zero.
type Event struct {
	// EventID is a UUIDv4 generated in Go — stable identity without a
	// database-side extension.
	EventID string
	// OccurredAt is when the request entered the handler.
	OccurredAt time.Time
	// PartnerID/KeyID identify the caller in partner mode; both empty in
	// static mode.
	PartnerID string
	KeyID     string
	// RequestID is the proxy's own per-request identifier — the same one
	// request_completed carries.
	RequestID string
	// ConfigGeneration is the snapshot generation the whole request was
	// bound to (one store.Load() per request).
	ConfigGeneration uint64
	PublicModel      string
	// Provider is the candidate that answered — or, when none did, the
	// last one attempted. UpstreamModel is that candidate's mapped name.
	Provider      string
	UpstreamModel string
	// API is "chat" or "responses".
	API    string
	Stream bool
	// HTTPStatus is the status the client received (the upstream's own on
	// relay, the proxy's on normalization/outcome envelopes).
	HTTPStatus int
	Outcome    string
	// Token counts as the upstream reported them; nil = absent upstream
	// usage, never zero.
	PromptTokens     *int64
	CompletionTokens *int64
	TotalTokens      *int64
	// BytesIn/BytesOut are the client-facing request/response wire sizes.
	BytesIn  int64
	BytesOut int64
	// ProviderAttempts counts the candidate-chain attempts of the walk.
	// EgressAttempts sums the endpoints actually dialed across every
	// candidate — pool reports where there was a pool, the single dial
	// where there was not. EgressKind is the last candidate's egress mode
	// ("direct", the last dialed pool member's kind, or empty for a pool
	// that exhausted without dialing anything). Failed attempts live only
	// here — a failed attempt never produces its own event.
	ProviderAttempts int
	EgressAttempts   int
	EgressKind       string
	// LatencyMS is the request's full handler duration.
	LatencyMS int64
}

// NewEventID returns a random RFC 4122 version-4 UUID string.
func NewEventID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		// crypto/rand.Read is documented to never fail; a failure means the
		// process's randomness source is broken, and a collision-prone
		// fallback would silently corrupt the event identity contract. Do not
		// append err.Error(): fatal/panic renderings can become test or
		// operator artifacts, and the meter must retain the same no-echo
		// discipline as every other credential-adjacent path.
		panic("usage: crypto/rand failed")
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	dst := make([]byte, 36)
	hex.Encode(dst[0:8], b[0:4])
	dst[8] = '-'
	hex.Encode(dst[9:13], b[4:6])
	dst[13] = '-'
	hex.Encode(dst[14:18], b[6:8])
	dst[18] = '-'
	hex.Encode(dst[19:23], b[8:10])
	dst[23] = '-'
	hex.Encode(dst[24:36], b[10:16])
	return string(dst)
}
