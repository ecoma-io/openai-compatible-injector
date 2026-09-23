package recovery

import (
	"crypto/sha256"
	"encoding/hex"
	"strconv"
	"time"
)

// hashTag versions the canonical encoding below. A future change to what the
// encoding writes must bump this tag, so two policies hashed by different
// builds of this package can never be mistaken for the same policy.
const hashTag = "recovery-policy-v1"

// Hash returns a short, stable identity for the policy's DATA.
//
// It is what an evidence event carries so two requests can be told apart as
// "ran under the same recovery policy" without putting the whole policy on a
// log line. The encoding is written field by field in a fixed order into a
// byte buffer and digested with SHA-256; the first 16 hex characters are
// returned. Nothing here iterates a map, formats a struct with %v, or
// reflects over one — every field is appended by hand, in the order written
// below, so the same policy hashes byte-identically on every run, in every
// process, on every architecture.
//
// What the hash identifies is the policy as data: two policies with the same
// hash have the same rules (in the matrix's own evaluation order), the same
// defaults, and the same budgets, retries, and retry-after settings. It does
// NOT assert behavioural equivalence between differing policies, and it does
// not collapse a policy with the same hash that was reached by a different
// layer chain — the resolver's layers are not part of the data it hashes,
// only the frozen result is.
func (p Policy) Hash() string {
	h := sha256.New()
	w := &hashWriter{buf: make([]byte, 0, 1024)}
	w.str(hashTag)

	// The matrix: its default action, then every rule in STORED order — the
	// order the matrix evaluates in, which NewMatrix derived from the rules
	// themselves, so it never depends on how the operator wrote them.
	w.action(p.Matrix.Default())
	rules := p.Matrix.Rules()
	w.int(len(rules))
	for i := range rules {
		r := &rules[i]
		w.str(r.ID)
		w.int(int(r.Match.Class))
		w.int(r.Match.Status)
		w.int(int(r.Match.StatusClass))
		w.int(int(r.Match.TransportClass))
		w.str(r.Match.TransportCause)
		w.str(r.Match.ProtocolCause)
		w.str(r.Match.CallerCause)
		w.str(r.Match.ProviderErrorType)
		w.str(r.Match.ProviderErrorCode)
		w.optBool(r.Match.Streaming)
		w.int(r.Match.CandidateIndex)
		w.optInt(r.Match.RetryIndex)
		w.action(r.Action)
	}

	// The retry mechanics, in full: the count, the window, the whole backoff
	// schedule including the jitter fraction, and the on-exhausted action.
	w.int(p.Retry.MaxRetries)
	w.duration(p.Retry.MaxElapsed)
	w.duration(p.Retry.Backoff.Initial)
	w.duration(p.Retry.Backoff.Max)
	w.float(p.Retry.Backoff.Jitter)
	w.action(p.Retry.OnExhausted)

	// The walk bound.
	w.bool(p.Fallback.Enabled)
	w.int(p.Fallback.MaxCandidates)
	w.action(p.Fallback.OnExhausted)

	// Both exchange envelopes, request scope first.
	w.int(p.Budget.Request.MaxExchanges)
	w.duration(p.Budget.Request.MaxElapsed)
	w.int(p.Budget.Candidate.MaxExchanges)
	w.duration(p.Budget.Candidate.MaxElapsed)

	// The Retry-After policy.
	w.bool(p.RetryAfter.Enabled)
	w.int(int(p.RetryAfter.Mode))
	w.duration(p.RetryAfter.MaxDelay)

	h.Write(w.buf)
	return hex.EncodeToString(h.Sum(nil))[:16]
}

// hashWriter is the canonical encoding's byte sink. Every append is
// unambiguous because strings carry their length and every other member is
// written as a fixed-shape token followed by a separator — so no two
// different field sequences can produce the same bytes.
type hashWriter struct {
	buf []byte
}

// sep terminates one member. It is written after every value so that a member
// can never run into the next one.
func (w *hashWriter) sep() { w.buf = append(w.buf, ';') }

func (w *hashWriter) str(s string) {
	w.buf = strconv.AppendInt(w.buf, int64(len(s)), 10)
	w.buf = append(w.buf, ':')
	w.buf = append(w.buf, s...)
	w.sep()
}

func (w *hashWriter) int(n int) {
	w.buf = strconv.AppendInt(w.buf, int64(n), 10)
	w.sep()
}

func (w *hashWriter) optInt(n *int) {
	if n == nil {
		w.buf = append(w.buf, 'n')
		w.sep()
		return
	}
	w.int(*n)
}

func (w *hashWriter) bool(b bool) {
	if b {
		w.buf = append(w.buf, '1')
	} else {
		w.buf = append(w.buf, '0')
	}
	w.sep()
}

func (w *hashWriter) optBool(b *bool) {
	if b == nil {
		w.buf = append(w.buf, 'n')
		w.sep()
		return
	}
	w.bool(*b)
}

// duration hashes the exact nanosecond count: two durations are the same
// policy member only when they are the same number, whatever spelling the
// file used.
func (w *hashWriter) duration(d time.Duration) {
	w.buf = strconv.AppendInt(w.buf, int64(d), 10)
	w.sep()
}

// float hashes the shortest representation that round-trips, which is
// deterministic for a given float64 on every platform Go supports.
func (w *hashWriter) float(f float64) {
	w.buf = strconv.AppendFloat(w.buf, f, 'g', -1, 64)
	w.sep()
}

// action hashes an action by its numeric value, not its token: the token
// mapping is a presentation concern and a future renaming must not silently
// change a policy's identity.
func (w *hashWriter) action(a Action) { w.int(int(a)) }
