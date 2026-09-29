package proxy

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	"openai-compatible-injector/internal/config"
	"openai-compatible-injector/internal/credential"
	"openai-compatible-injector/internal/recovery"
	"openai-compatible-injector/internal/transport"
)

// The recovery telemetry ownership matrix, and the tests that hold it.
//
// Five events describe one logical stream that ends without its terminal
// marker while the feature is on. They answer four DIFFERENT questions, and
// the whole point of the matrix is that no two of them answer the same one —
// an operator paged at 3am reads one line and has to know which layer to look
// at:
//
//	event                      level  one per                     owner
//	stream_recovery_started    INFO   hop INITIATED (slot claimed) loop
//	stream_recovery_failed     WARN   hop that did not produce a    the stage
//	                                  continuable stream, plus the  that refused
//	                                  pre-hop build refusal
//	stream_recovery_succeeded  INFO   hop whose relay reached the  the hop
//	                                  terminal marker
//	stream_recovery_exhausted  WARN   loop that stopped short of a  the BOUND
//	                                  terminal stream
//	stream_truncated           WARN   stream the client cannot     the
//	                                  tell from a finished answer   classification
//
// Ownership is the axis that matters, and it splits cleanly:
//
//   - `stream_recovery_started` and `stream_recovery_exhausted` are the
//     LOOP's records. They say what this proxy decided to do and which of its
//     own bounds stopped it; neither ever carries a peer's error text, and
//     the exhausted reason is always a closed-set bound token.
//   - `stream_recovery_failed` is a HOP's record, keyed by `phase`: the stage
//     that refused. This is the only recovery event that carries an error or
//     an `upstream_status`, and it carries them only when the stage named is
//     a wire stage. A phase this proxy produced — `build`, `credential`,
//     `budget`, `max_elapsed` — carries NEITHER, because nothing was dialed
//     and no endpoint is at fault.
//   - `stream_recovery_succeeded` is the hop's own claim that the client's
//     stream is finished on its terms, and it is suppressed when the window
//     cut the pass, however the partial bytes landed.
//   - `stream_truncated` is the only event that describes the CLIENT'S
//     stream rather than an attempt, and its `phase` is the same closed set:
//     `upstream_read` (a peer), `client_write` (the reader), `upstream_limit`
//     (this proxy's relay cap), or `recovery` (no wire failure at all — the
//     loop refused, or its bound ended the effort). `recovery_reason` is set
//     exactly when a bound stopped the loop, and NEVER when the loop simply
//     refused a hop: a failed hop is a failure, not a bound.
//
// Three readings the matrix exists to forbid:
//
//  1. A `stream_recovery_failed` with phase `max_elapsed` is THIS proxy's own
//     deadline. The window closes the body a relay is parked on, so the read
//     error that follows is this process's closed body; attributing it to
//     `upstream_read` would send an operator after a peer that behaved
//     perfectly, and the same cause must not then also appear as the loop's
//     bound. Exactly one owner: the phase and the reason both say
//     `max_elapsed`, the error is dropped rather than invented, and the bound
//     is reported once.
//  2. A `recovery_reason` on `stream_truncated` with no `stream_recovery_failed`
//     above it means the loop refused to make a hop — the budget envelope was
//     spent, the body could not express a continuation. Nothing was tried and
//     nothing failed; the reason is the whole story.
//  3. A hop that ended cleanly at EOF (`phase: upstream_read`, no error) is a
//     truncated stream, not a provider fault: `upstream_read` names the stage
//     the pass died at, and the error field is present only when there
//     actually was one.
//
// One field name, one vocabulary: `reason` on `stream_recovery_exhausted` is
// the LOOP's stop reason, and a gate's own token — a shape this build declined
// to continue — always travels as `unsafe_reason`, on whichever of the two
// events reports it. The two closed sets share no token today, which is
// precisely why a reader would not notice the day they do.
//
// The vocabulary below is enforced, not merely written down: the const block
// in streamrecovery.go owns the reason tokens and the string literals in
// streamrecovery.go/handler.go own the phases, so
// TestRecoveryTokenSetsAreTheDocumentedMatrix fails when either grows a value
// this file does not document, and TestUnsafeReasonTokensAreTheDocumentedMatrix
// does the same for `unsafe_reason` across the two packages that can produce
// one. A token an operator can be sent without a meaning on this page is a
// token they cannot act on.

// recoveryReasonTokens is the closed set of `stream_recovery_exhausted.reason`
// values: every bound the loop can stop at. A stream that ends on its own
// terms produces none of them, and the client leaving produces none either —
// that stop has no `recovery_reason` at all, because nothing about the
// recovery was wrong.
var recoveryReasonTokens = []string{
	recoveryBudgetSpent,
	recoveryMaxRecoveries,
	recoveryMaxElapsed,
	recoveryLogicalTerminal,
	recoveryUnsafeContent,
}

// recoveryPhaseTokens is the closed set of `stream_recovery_failed.phase`
// values: the stage that refused, whether the stage is on the wire or inside
// this process.
var recoveryPhaseTokens = []string{
	"build",            // the continuation body could not be built or transformed
	"budget",           // the exchange envelope refused the hop before any wire
	"client_write",     // the relay's write to the client failed
	"credential",       // every key on the candidate is cooling
	recoveryMaxElapsed, // this proxy's own window, never an endpoint
	"dial",             // the dial or the response-header wait failed
	"upstream_limit",   // this proxy's bounded-relay cap stopped the pass
	"upstream_read",    // the hop's body ended or failed mid-stream
	"upstream_status",  // the hop answered with a status that is not a stream
}

// proxyOwnedPhases is the subset of phases this proxy produced rather than a
// peer: nothing was dialed for them, so neither an error nor an
// `upstream_status` may accompany them. `max_elapsed` also names no ENDPOINT,
// because the window is checked before the hop builds its URL — so the
// `upstream` field is absent on that record too, which is asserted where the
// backstop is driven.
var proxyOwnedPhases = map[string]bool{
	"build":            true,
	"budget":           true,
	"credential":       true,
	recoveryMaxElapsed: true,
}

// unsafeReasonTokens is the closed set of `unsafe_reason` values, and it is
// the one closed set this service has that is produced by TWO packages: the
// accumulator's shape refusals in continuation.go and the continuation
// builders' body refusals in internal/inject. Both arrive on the same field,
// because from an operator's side "the stream was not continued and here is
// why" is one question — so the union is what has to stay documented, and the
// two halves are the two ways it can grow.
//
// `not_object` is deliberately in both halves: a `data:` line the accumulator
// cannot parse and a client body that is not a JSON object are the same
// statement about readability arriving from opposite ends of the request, and
// collapsing them into one token is intended rather than an oversight.
var unsafeReasonTokens = []string{
	// The accumulator, on the stream it is reading.
	reasonToolCalls,
	reasonRefusal,
	reasonUpstreamTerminal,
	reasonNotObject,
	reasonUnknownShape,
	reasonMultipleOutputs,
	reasonOversize,
	reasonNoPrefix,
	// The builders, on the client's own body.
	"no_messages",
	"unsupported_shape",
	"previous_response_id",
	"no_op",
}

// TestRecoveryTokenSetsAreTheDocumentedMatrix reads the package's own source
// and fails if either token set has grown a value the matrix above does not
// document. It is the gate that keeps the matrix from quietly becoming
// fiction: a new phase or stop reason lands only with a row here, and a row
// here is what an operator reading a log line will find.
func TestRecoveryTokenSetsAreTheDocumentedMatrix(t *testing.T) {
	reasons, phases := scanRecoveryTokens(t)

	if got, want := sortedKeys(reasons), sortedCopy(recoveryReasonTokens); !equalStrings(got, want) {
		t.Errorf("stop reasons in the source = %v, want the documented %v\n"+
			"a new bound needs a row in the matrix above and a test in TestRecoveryStopReasonOwnsItsEvents", got, want)
	}
	if got, want := sortedKeys(phases), sortedCopy(recoveryPhaseTokens); !equalStrings(got, want) {
		t.Errorf("hop phases in the source = %v, want the documented %v\n"+
			"a new phase needs a row in the matrix above, and an owner: does it carry an error, or is it this proxy's own?", got, want)
	}
}

// TestUnsafeReasonTokensAreTheDocumentedMatrix does for `unsafe_reason` what
// the test above does for the loop's own vocabulary, over the two const blocks
// that can produce one. It is a separate scan because it reads a file outside
// this package: the builders that refuse the client's body live in
// internal/inject, and a refusal token that landed there without a row on the
// page is exactly the drift this catches — the accumulator's own set is
// pinned in the same test so the union cannot be half-checked.
func TestUnsafeReasonTokensAreTheDocumentedMatrix(t *testing.T) {
	got := map[string]bool{}
	for _, f := range []struct{ file, prefix string }{
		{"continuation.go", "reason"},            // the accumulator
		{"../inject/continuation.go", "refusal"}, // the builders
	} {
		pat := regexp.MustCompile(f.prefix + `[A-Z]\w*\s*=\s*"([a-z_]+)"`)
		for _, m := range pat.FindAllStringSubmatch(readPackageFile(t, f.file), -1) {
			got[m[1]] = true
		}
	}
	if g, want := sortedKeys(got), sortedCopy(unsafeReasonTokens); !equalStrings(g, want) {
		t.Errorf("unsafe reasons in the source = %v, want the documented %v\n"+
			"a new refusal token needs a row in the documented set (and, if it is a "+
			"content shape, a test in TestRecoveryStopReasonOwnsItsEvents)", g, want)
	}
}

// scanRecoveryTokens extracts the stop reasons the loop assigns to
// `stopReason` and the phases it reports on a hop failure, resolving the
// `recoveryXxx` identifiers through the const block that defines them. It
// reads the two files that own the loop, so a token introduced anywhere in
// them is seen.
func scanRecoveryTokens(t *testing.T) (reasons, phases map[string]bool) {
	t.Helper()
	handler := readPackageFile(t, "handler.go")
	loop := readPackageFile(t, "streamrecovery.go")

	// The const block is the one place a local token's string value is
	// written: every other use is the identifier.
	consts := map[string]string{}
	for _, m := range regexp.MustCompile(`(recovery[A-Z]\w*)\s*=\s*"([a-z_]+)"`).FindAllStringSubmatch(loop, -1) {
		consts[m[1]] = m[2]
	}

	resolve := func(tok string) (string, bool) {
		if lit, ok := consts[tok]; ok {
			return lit, true
		}
		return "", false
	}

	reasons = map[string]bool{}
	for _, line := range strings.Split(handler, "\n") {
		if !strings.Contains(line, "stopReason") {
			continue
		}
		for _, ident := range regexp.MustCompile(`recovery[A-Z]\w*`).FindAllString(line, -1) {
			lit, ok := resolve(ident)
			if !ok {
				t.Fatalf("stopReason line names %s, which the const block does not define: %s", ident, line)
			}
			reasons[lit] = true
		}
	}

	phases = map[string]bool{}
	assign := regexp.MustCompile(`(?:dial\.phase|hopPhase)\s*(?:,\s*dial\.err\s*)?:?=\s*([^,\n]+)`)
	for _, m := range assign.FindAllStringSubmatch(loop+"\n"+handler, -1) {
		rhs := strings.TrimSpace(m[1])
		if lit, ok := unquote(rhs); ok {
			phases[lit] = true
			continue
		}
		if ident := regexp.MustCompile(`^(recovery[A-Z]\w*)$`).FindStringSubmatch(rhs); ident != nil {
			lit, ok := resolve(ident[1])
			if !ok {
				t.Fatalf("phase assignment names %s, which the const block does not define", ident[1])
			}
			phases[lit] = true
		}
	}
	// The recoveryFailed helper is called with the phase as its first
	// argument on the paths where the stage is known at the call site.
	for _, m := range regexp.MustCompile(`recoveryFailed\(\s*(?:"([a-z_]+)"|(recovery[A-Z]\w*))`).FindAllStringSubmatch(loop+"\n"+handler, -1) {
		if m[1] != "" {
			phases[m[1]] = true
			continue
		}
		lit, ok := resolve(m[2])
		if !ok {
			t.Fatalf("recoveryFailed names %s, which the const block does not define", m[2])
		}
		phases[lit] = true
	}
	// ... and once as a structured field, on the pre-hop build refusal. This
	// pattern is scoped to the five recovery events, by the message a field
	// chain ends in: `phase` is a name this package also uses for closed sets
	// that have nothing to do with a continuation hop — the capacity refusal
	// names the buffer it refused — and those tokens must not be dragged into
	// a matrix that would then be describing something else.
	src := loop + "\n" + handler
	recoveryEvents := map[string]bool{
		"stream_recovery_started":   true,
		"stream_recovery_failed":    true,
		"stream_recovery_succeeded": true,
		"stream_recovery_exhausted": true,
		"stream_truncated":          true,
	}
	type msgSite struct {
		at   int
		slug string
	}
	var msgs []msgSite
	for _, m := range regexp.MustCompile(`Msg\("([a-z_.]+)"\)`).FindAllStringSubmatchIndex(src, -1) {
		msgs = append(msgs, msgSite{at: m[0], slug: src[m[2]:m[3]]})
	}
	for _, m := range regexp.MustCompile(`Str\("phase",\s*"([a-z_]+)"\)`).FindAllStringSubmatchIndex(src, -1) {
		// A field is set before the Msg that emits it, so the owning event is
		// the first message site after the field.
		owner := ""
		for _, msg := range msgs {
			if msg.at > m[0] {
				owner = msg.slug
				break
			}
		}
		if !recoveryEvents[owner] {
			continue
		}
		phases[src[m[2]:m[3]]] = true
	}
	return reasons, phases
}

// noEchoPhases is the subset of phases whose errors may reach a record RAW.
// The no-echo rule governs one class of error only — the ones that come off
// the wire, where a truncated read surfaces the transport's own parse
// failures and those interpolate the upstream's bytes. Every other phase
// produced its error inside this process, from a typed value with static
// text, so sanitizing it would be pure loss: `sanitizeUpstreamError`
// replaces anything it does not recognize with "upstream transport error",
// which on a `client_write` or `upstream_limit` record would blame a peer for
// a stop the reader or this proxy caused, one field away from the phase token
// that says otherwise. The committed pass already logs its equivalent
// truncation raw for exactly this reason, and the two paths must not disagree.
var noEchoPhases = map[string]bool{
	"dial":            true,
	"upstream_read":   true,
	"upstream_status": true,
}

// TestHopFailureCauseIsOwnedByThePhase pins the same rule as a unit, over
// every phase, so it does not need a stream to fail in each of them: an error
// is attached, omitted, or passed through according to the phase alone.
func TestHopFailureCauseIsOwnedByThePhase(t *testing.T) {
	// A real wrapped shape, so the sanitizer has something to collapse: this
	// is a value it does not recognize, and its text is not provably
	// echo-free.
	peerText := errors.New("malformed MIME header line from peer")
	peer := &url.Error{Op: "Post", URL: "https://up.example/v1/chat/completions?k=v", Err: peerText}
	upstream := &url.URL{Scheme: "https", Host: "up.example"}

	// Local causes: typed errors whose text is this package's own and never
	// carries a peer's bytes. A short write is the one a client-side relay
	// produces with a nil error of its own, and the sanitizer does not
	// recognize it.
	local := &streamWriteError{err: io.ErrShortWrite}

	cases := []struct {
		phase   string
		err     error
		want    string // "" means no cause may be attached at all
		present bool
	}{
		{"build", errors.New("continuation refused: no_messages"), "", false},
		{"credential", errors.New("no usable credential"), "", false},
		{"budget", errors.New("exchange envelope spent"), "", false},
		{recoveryMaxElapsed, context.DeadlineExceeded, "", false},
		{"upstream_limit", local, local.Error(), true},
		{"client_write", local, local.Error(), true},
		{"dial", peer, sanitizeUpstreamError(peer, upstream).Error(), true},
		{"upstream_read", peer, sanitizeUpstreamError(peer, upstream).Error(), true},
		{"upstream_status", peer, sanitizeUpstreamError(peer, upstream).Error(), true},
	}
	for _, tc := range cases {
		t.Run(tc.phase, func(t *testing.T) {
			got := hopFailureCause(tc.phase, tc.err, upstream)
			if !tc.present {
				if got != nil {
					t.Errorf("hopFailureCause(%q) = %v, want no cause: nothing was dialed", tc.phase, got)
				}
				return
			}
			if got == nil {
				t.Fatalf("hopFailureCause(%q) = nil, want a cause", tc.phase)
			}
			if !noEchoPhases[tc.phase] && got.Error() == "upstream transport error" {
				t.Errorf("a local cause collapsed to the wire's static text: %v", got)
			}
			if got.Error() != tc.want {
				t.Errorf("hopFailureCause(%q).Error() = %q, want %q", tc.phase, got.Error(), tc.want)
			}
			if strings.Contains(got.Error(), "up.example/v1/chat/completions?k=v") {
				t.Errorf("a wire cause echoed the request URL: %q", got.Error())
			}
		})
	}
}

func readPackageFile(t *testing.T, name string) string {
	t.Helper()
	b, err := os.ReadFile(name)
	if err != nil {
		t.Fatalf("read %s: %v", name, err)
	}
	return string(b)
}

func unquote(s string) (string, bool) {
	if len(s) >= 2 && strings.HasPrefix(s, `"`) && strings.HasSuffix(s, `"`) {
		return s[1 : len(s)-1], true
	}
	return "", false
}

func sortedKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func sortedCopy(in []string) []string {
	out := append([]string(nil), in...)
	sort.Strings(out)
	return out
}

// TestRecoveryStopReasonOwnsItsEvents drives one stream per stop reason in
// the closed set and pins the whole event set each one produces: who logged
// what, with which vocabulary, and — the invariant the feature is judged on —
// that a stop this proxy caused never wears a peer's error.
//
// Every row is deterministic: each scenario's upstream ends its stream the
// way the row needs and nothing in the test races a clock except the one
// window the `max_elapsed` row is about.
func TestRecoveryStopReasonOwnsItsEvents(t *testing.T) {
	const finishChunk = `data: {"choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}` + "\n\n"
	const toolChunk = `data: {"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"call_1"}]}}]}` + "\n\n"

	cases := []struct {
		name  string
		block string
		// script is built per run: a row that needs a parked body must not
		// share one with another run of the same table.
		script func(t *testing.T) []dialFunc
		// wantStarted counts hops the loop INITIATED, wantFailed the hop
		// records it emitted, and wantPhase the stage the last one named.
		wantStarted int
		wantFailed  int
		wantPhase   string
		// wantUnsafe is the `unsafe_reason` the exhausted event must carry,
		// empty when the row is not a content refusal.
		wantUnsafe string
	}{
		{
			name:  recoveryMaxRecoveries,
			block: recoveryBlock(t, "    enabled: true\n    max-recoveries: 1\n"),
			script: func(*testing.T) []dialFunc {
				return []dialFunc{sseStream(sseChat("Hello")), sseStream(sseChat(", world"))}
			},
			// The first pass and the one hop both ended at EOF without their
			// marker, so the hop is a failure — a stream that did not finish
			// is not a success just because nothing errored.
			wantStarted: 1, wantFailed: 1, wantPhase: "upstream_read",
		},
		{
			name: recoveryBudgetSpent,
			block: "recovery:\n" +
				"  retries:\n    max-retries: 0\n" +
				"  budget:\n    request:\n      max-exchanges: 1\n    candidate:\n      max-exchanges: 1\n" +
				"  stream:\n    enabled: true\n",
			script: func(*testing.T) []dialFunc { return []dialFunc{sseStream(sseChat("Hello"))} },
			// Zero hops: the envelope the walk already spent refuses the
			// continuation outright, and the record says so rather than
			// blaming an endpoint that was never asked.
			wantStarted: 0, wantFailed: 0,
		},
		{
			name:  recoveryMaxElapsed,
			block: recoveryBlock(t, "    enabled: true\n    max-elapsed: 150ms\n"),
			script: func(*testing.T) []dialFunc {
				_, dial := sseParked(context.Background(), sseChat("Hello"))
				return []dialFunc{dial}
			},
			// The watchdog closed the body the committed relay was parked on,
			// so zero hops were initiated — and the read error that follows is
			// this proxy's own closed body, which the truncation record must
			// not wear.
			wantStarted: 0, wantFailed: 0,
		},
		{
			name:  recoveryLogicalTerminal,
			block: recoveryBlock(t, "    enabled: true\n"),
			script: func(*testing.T) []dialFunc {
				return []dialFunc{sseStream(sseChat("Hello") + finishChunk)}
			},
			wantStarted: 0, wantFailed: 0,
		},
		{
			name:  recoveryUnsafeContent,
			block: recoveryBlock(t, "    enabled: true\n"),
			script: func(*testing.T) []dialFunc {
				return []dialFunc{sseStream(sseChat("Hello") + toolChunk)}
			},
			wantStarted: 0, wantFailed: 0, wantUnsafe: "tool_calls",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h, logBuf, pa, pb := recoveryHandler(t, tc.block)
			pa.script = tc.script(t)

			rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions", chatRequest, nil)
			if rec.Code != http.StatusOK {
				t.Fatalf("status = %d, want the already-committed 200", rec.Code)
			}
			if pb.dials() != 0 {
				t.Errorf("the fallback candidate was dialed %d times: recovery never moves the request", pb.dials())
			}

			started := logBuf.events(t, "stream_recovery_started")
			if len(started) != tc.wantStarted {
				t.Errorf("stream_recovery_started = %d, want %d", len(started), tc.wantStarted)
			}

			failed := logBuf.events(t, "stream_recovery_failed")
			if len(failed) != tc.wantFailed {
				t.Fatalf("stream_recovery_failed = %v, want %d", failed, tc.wantFailed)
			}
			for _, ev := range failed {
				phase, _ := ev["phase"].(string)
				if !containsString(recoveryPhaseTokens, phase) {
					t.Errorf("hop failure phase %q is not in the documented set %v", phase, recoveryPhaseTokens)
					continue
				}
				// The ownership rule, on the hop's own record: a stage this
				// proxy produced dialed nothing, so it can name neither a
				// peer's error nor a peer's status.
				if proxyOwnedPhases[phase] {
					if _, has := ev["error"]; has {
						t.Errorf("phase %q blamed an endpoint with an error: %v", phase, ev)
					}
					if _, has := ev["upstream_status"]; has {
						t.Errorf("phase %q carried a status for a hop that never dialed: %v", phase, ev)
					}
				}
			}
			if tc.wantPhase != "" && failed[len(failed)-1]["phase"] != tc.wantPhase {
				t.Errorf("last hop failure = %v, want phase %q", failed[len(failed)-1], tc.wantPhase)
			}

			if ev := logBuf.events(t, "stream_recovery_succeeded"); len(ev) != 0 {
				t.Errorf("a stream that needed recovery reported success: %v", ev)
			}

			exh := logBuf.events(t, "stream_recovery_exhausted")
			if len(exh) != 1 {
				t.Fatalf("stream_recovery_exhausted = %v, want exactly one bound", exh)
			}
			if exh[0]["reason"] != tc.name {
				t.Errorf("reason = %v, want %q", exh[0]["reason"], tc.name)
			}
			if !containsString(recoveryReasonTokens, tc.name) {
				t.Errorf("%q is not in the documented reason set", tc.name)
			}
			if exh[0]["recoveries"] != float64(tc.wantStarted) {
				t.Errorf("exhausted recoveries = %v, want the %d hops that were initiated", exh[0]["recoveries"], tc.wantStarted)
			}
			if tc.wantUnsafe == "" {
				if _, has := exh[0]["unsafe_reason"]; has {
					t.Errorf("a non-content refusal carried an unsafe_reason: %v", exh[0])
				}
			} else if exh[0]["unsafe_reason"] != tc.wantUnsafe {
				t.Errorf("unsafe_reason = %v, want %q", exh[0]["unsafe_reason"], tc.wantUnsafe)
			}

			trunc := logBuf.events(t, "stream_truncated")
			if len(trunc) != 1 {
				t.Fatalf("stream_truncated = %v, want exactly one", trunc)
			}
			if trunc[0]["recovery_reason"] != tc.name {
				t.Errorf("recovery_reason = %v, want %q", trunc[0]["recovery_reason"], tc.name)
			}
			if trunc[0]["stream_recoveries"] != float64(tc.wantStarted) {
				t.Errorf("stream_recoveries = %v, want %d", trunc[0]["stream_recoveries"], tc.wantStarted)
			}
			// THE INVARIANT THIS TABLE EXISTS FOR. Every row above stops at a
			// refusal or a bound this process made, next to a pass that ended
			// without a wire failure — so the client-visible truncation is
			// phase `recovery` and carries no error. A peer's error here would
			// be an attribution this proxy did not have.
			if trunc[0]["phase"] != "recovery" {
				t.Errorf("truncation phase = %v, want recovery (no wire failure produced this stop)", trunc[0]["phase"])
			}
			if _, has := trunc[0]["error"]; has {
				t.Errorf("a stop this proxy owns carried an error field: %v", trunc[0])
			}
			if ev := logBuf.events(t, "stream_completed"); len(ev) != 0 {
				t.Errorf("an unterminated stream was reported as completed: %v", ev)
			}
			done := logBuf.events(t, "request_completed")
			if len(done) != 1 || done[0]["outcome"] != "stream_truncated" {
				t.Errorf("request_completed = %v, want outcome stream_truncated", done)
			}
		})
	}
}

// TestStreamRecoveryBuildRefusalIsAPreHopRefusal covers the one
// `stream_recovery_failed` that has no `stream_recovery_started` beside it.
//
// The loop refuses to build a continuation for a body that cannot express one
// — here a Responses request carrying `previous_response_id`, whose upstream
// would resolve the continuation against a response whose generation never
// finished. The refusal happens before the slot is claimed, so no hop index
// was ever handed out: the started count stays zero, the failure names the
// stage that refused (`build`) and the refusal token, and the loop stops as
// the content refusal it is.
//
// Nothing reached the wire: one dial for the walk, none for the refusal.
func TestStreamRecoveryBuildRefusalIsAPreHopRefusal(t *testing.T) {
	h, logBuf, pa, pb := recoveryHandler(t, recoveryBlock(t, "    enabled: true\n"))
	pa.script = []dialFunc{sseStream(sseResponses("Once"))}

	rec := doRequest(t, h, http.MethodPost, "/v1/responses",
		`{"model":"chain-model","stream":true,"previous_response_id":"resp_1","input":"hi"}`, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want the already-committed 200", rec.Code)
	}
	if pa.dials() != 1 || pb.dials() != 0 {
		t.Fatalf("dials = %d/%d, want the walk's one and nothing for the refusal", pa.dials(), pb.dials())
	}
	if ev := logBuf.events(t, "stream_recovery_started"); len(ev) != 0 {
		t.Errorf("a refusal that never reached the wire claimed a hop slot: %v", ev)
	}
	failed := logBuf.events(t, "stream_recovery_failed")
	if len(failed) != 1 {
		t.Fatalf("stream_recovery_failed = %v, want the one build refusal", failed)
	}
	if failed[0]["phase"] != "build" {
		t.Errorf("phase = %v, want build", failed[0]["phase"])
	}
	// The refusal token travels as `unsafe_reason`, never as `reason`: `reason`
	// belongs to the exhausted event, where it names the loop's own stop reason
	// from a different closed set.
	if _, has := failed[0]["reason"]; has {
		t.Errorf("a hop event carried the loop's stop-reason field: %v", failed[0])
	}
	if failed[0]["unsafe_reason"] != "previous_response_id" {
		t.Errorf("refusal = %v, want the typed previous_response_id token", failed[0]["unsafe_reason"])
	}
	if _, has := failed[0]["error"]; has {
		t.Errorf("a pre-hop refusal carried an error: %v", failed[0])
	}
	exh := logBuf.events(t, "stream_recovery_exhausted")
	if len(exh) != 1 {
		t.Fatalf("stream_recovery_exhausted = %v, want one", exh)
	}
	if exh[0]["reason"] != recoveryUnsafeContent || exh[0]["unsafe_reason"] != "previous_response_id" {
		t.Errorf("exhausted = %v, want unsafe_content/previous_response_id", exh[0])
	}
	if exh[0]["recoveries"] != float64(0) {
		t.Errorf("recoveries = %v, want 0 — no hop was initiated", exh[0]["recoveries"])
	}
	// The client's stream is exactly as unterminated as it arrived, and the
	// truncation names the bound without inventing a peer failure: the
	// committed pass itself ended cleanly.
	if strings.Contains(rec.Body.String(), "[DONE]") || strings.Contains(rec.Body.String(), "response.completed") {
		t.Errorf("a marker was synthesized for a stream that never had one: %s", rec.Body.String())
	}
	trunc := logBuf.events(t, "stream_truncated")
	if len(trunc) != 1 || trunc[0]["recovery_reason"] != recoveryUnsafeContent {
		t.Fatalf("stream_truncated = %v, want the unsafe_content reason", trunc)
	}
	if _, has := trunc[0]["error"]; has {
		t.Errorf("the refusal was reported as a wire failure: %v", trunc[0])
	}
}

// TestContinuationCredentialRefusalBlamesNoEndpoint drives the hop's
// credential gate directly: with every key on the candidate cooling, the hop
// is refused before anything is built or dialed.
//
// A pool reaches this state in production without any fault to report: a
// concurrent request on the same provider takes a 429 on the key this
// stream's walk went out with, and the pool — which is process-wide state,
// not per-request — is suddenly out of usable keys. The hop then has nothing
// to blame and must say so: the phase is `credential`, no error is attached,
// no exchange was claimed, and the sticky preference is left exactly as it
// was found so the next attempt still prefers the key that produced the
// prefix.
func TestContinuationCredentialRefusalBlamesNoEndpoint(t *testing.T) {
	now := time.Now()
	spec := credential.Spec{
		Header: "Authorization",
		Prefix: "Bearer ",
		Keys: []credential.Key{
			{ID: "k1", Value: "v1"},
			{ID: "k2", Value: "v2"},
		},
	}
	pool := credential.NewPool(spec)
	pool.MarkRateLimited(now, "k1", time.Hour)
	pool.MarkRateLimited(now, "k2", time.Hour)

	upstream, err := url.Parse("https://a.example/v1")
	if err != nil {
		t.Fatalf("parse endpoint: %v", err)
	}
	doer := &scriptedDoer{}
	budget := recovery.NewBudget(recovery.Envelope{MaxExchanges: 4}, func() time.Time { return now })
	h := &injectorHandler{doers: kindResolver{direct: doer, proxied: doer}}

	client, err := http.NewRequest(http.MethodPost, "http://proxy.invalid/v1/chat/completions", nil)
	if err != nil {
		t.Fatalf("build client request: %v", err)
	}
	dial := h.dialContinuation(continuationHop{
		ctx:    context.Background(),
		window: newRecoveryWindow(wallRecoveryClock{}, time.Minute),
		client: client,
		cand: config.Candidate{
			Endpoint: upstream,
			Cred:     &credential.Provider{Identity: "pa", Spec: spec},
		},
		pool:      pool,
		credKey:   "k1",
		transform: func(body []byte, _ config.Model) ([]byte, error) { return body, nil },
		body:      []byte(`{"model":"up-a"}`),
		suffix:    "/chat/completions",
		budget:    budget,
		now:       now,
	})

	if dial.phase != "credential" {
		t.Fatalf("phase = %q, want credential", dial.phase)
	}
	if dial.err != nil {
		t.Errorf("a refusal this proxy made carried an error: %v", dial.err)
	}
	if dial.resp != nil {
		t.Errorf("a hop that never dialed produced a response: %v", dial.resp)
	}
	if doer.dials() != 0 {
		t.Errorf("upstream dials = %d, want none: every key was cooling", doer.dials())
	}
	if n := budget.RequestExchanges(); n != 0 {
		t.Errorf("exchanges claimed = %d, want 0 — the refusal never reached the wire", n)
	}
	if dial.credKey != "k1" {
		t.Errorf("credkey = %q, want the preferred k1 left in place", dial.credKey)
	}
	if !containsString(recoveryPhaseTokens, dial.phase) {
		t.Errorf("phase %q is not in the documented set", dial.phase)
	}
}

// TestContinuationBuildFailureIsAPhaseNotAnError pins the other
// proxy-owned phase at the same seam: a transform that cannot produce a body
// is the `build` stage refusing, and the phase — not a wire failure — is what
// the record says. The cause is carried for the operator, but nothing is
// dialed and no exchange is claimed for it.
func TestContinuationBuildFailureIsAPhaseNotAnError(t *testing.T) {
	now := time.Now()
	upstream, err := url.Parse("https://a.example/v1")
	if err != nil {
		t.Fatalf("parse endpoint: %v", err)
	}
	doer := &scriptedDoer{}
	budget := recovery.NewBudget(recovery.Envelope{MaxExchanges: 4}, func() time.Time { return now })
	h := &injectorHandler{doers: kindResolver{direct: doer, proxied: doer}}

	client, err := http.NewRequest(http.MethodPost, "http://proxy.invalid/v1/chat/completions", nil)
	if err != nil {
		t.Fatalf("build client request: %v", err)
	}
	dial := h.dialContinuation(continuationHop{
		ctx:    context.Background(),
		window: newRecoveryWindow(wallRecoveryClock{}, time.Minute),
		client: client,
		// No credential block: the build stage refuses before the credential
		// seam is ever reached, which is the ordering this test also pins.
		cand:      config.Candidate{Endpoint: upstream},
		transform: func([]byte, config.Model) ([]byte, error) { return nil, errors.New("body is not JSON") },
		body:      []byte(`not json`),
		suffix:    "/chat/completions",
		budget:    budget,
		now:       now,
	})

	if dial.phase != "build" {
		t.Fatalf("phase = %q, want build", dial.phase)
	}
	if dial.err == nil {
		t.Error("a build failure reported no cause at all")
	}
	if dial.resp != nil {
		t.Errorf("a hop that never dialed produced a response: %v", dial.resp)
	}
	if doer.dials() != 0 {
		t.Errorf("upstream dials = %d, want none", doer.dials())
	}
	if n := budget.RequestExchanges(); n != 0 {
		t.Errorf("exchanges claimed = %d, want 0", n)
	}
}

// TestContinuationWindowRefusalAtTheDialIsTheBound is the third proxy-owned
// phase, at the seam the loop depends on: a hop the loop was about to make
// into an already-shut window never reaches the wire, never claims an
// exchange, and names `max_elapsed` rather than a dial failure. The loop's
// own gate catches this case first in production; the refusal here is the
// backstop beneath it, and it must agree with the same owner.
func TestContinuationWindowRefusalAtTheDialIsTheBound(t *testing.T) {
	now := time.Now()
	upstream, err := url.Parse("https://a.example/v1")
	if err != nil {
		t.Fatalf("parse endpoint: %v", err)
	}
	doer := &scriptedDoer{}
	budget := recovery.NewBudget(recovery.Envelope{MaxExchanges: 4}, func() time.Time { return now })
	h := &injectorHandler{doers: kindResolver{direct: doer, proxied: doer}}

	client, err := http.NewRequest(http.MethodPost, "http://proxy.invalid/v1/chat/completions", nil)
	if err != nil {
		t.Fatalf("build client request: %v", err)
	}
	// The window opened a minute before the hop asks to dial it: the instant
	// has passed, so no attempt may start past it. The clock is advanced
	// explicitly, so the refusal is caused by elapsed idle time and not by a
	// construction-time reading.
	clock := newManualRecoveryClock(now)
	window := newRecoveryWindow(clock, 30*time.Second)
	clock.Advance(time.Minute)
	dial := h.dialContinuation(continuationHop{
		ctx:       context.Background(),
		window:    window,
		client:    client,
		cand:      config.Candidate{Endpoint: upstream},
		transform: func(body []byte, _ config.Model) ([]byte, error) { return body, nil },
		body:      []byte(`{"model":"up-a"}`),
		suffix:    "/chat/completions",
		budget:    budget,
		now:       now,
	})

	if dial.phase != recoveryMaxElapsed {
		t.Fatalf("phase = %q, want %q", dial.phase, recoveryMaxElapsed)
	}
	if dial.err != nil {
		t.Errorf("this proxy's own bound was reported with an error: %v", dial.err)
	}
	if doer.dials() != 0 {
		t.Errorf("upstream dials = %d, want none past the window", doer.dials())
	}
	if n := budget.RequestExchanges(); n != 0 {
		t.Errorf("exchanges claimed = %d, want 0", n)
	}
	if !window.shut() {
		t.Error("a refusal by the window left it open: a later hop could dial past the bound")
	}
	// The bound is reported through the one vocabulary, wherever it is read.
	if !containsString(recoveryReasonTokens, dial.phase) {
		t.Errorf("phase %q is not in the documented reason set either", dial.phase)
	}
}

// TestRecoverySuppressedSucceededNeverFiresUnderABound is the matrix's
// negative half for the one event that claims an outcome: a hop cut by the
// window must never be reported as a success, and the loop must not then also
// report the same stop as a failure of the read it cut.
//
// This is the same ownership rule the window tests pin from the other side —
// the point of repeating it here is that the four events are asserted
// TOGETHER, as the matrix claims they are: one started, one failed with the
// bound's phase, no succeeded, one exhausted, one truncation.
func TestRecoverySuppressedSucceededNeverFiresUnderABound(t *testing.T) {
	h, logBuf, pa, _ := recoveryHandler(t,
		recoveryBlock(t, "    enabled: true\n    max-elapsed: 200ms\n    max-recoveries: 2\n"))
	// The committed pass relays one event and ends cleanly, so the window that
	// fires is cutting the HOP's read.
	committed := sseStream(sseChat("Hello"))
	_, parked := sseParked(context.Background(), sseChat(", world"))
	pa.script = []dialFunc{committed, parked}

	if rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions", chatRequest, nil); rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want the already-committed 200", rec.Code)
	}

	if n := len(logBuf.events(t, "stream_recovery_started")); n != 1 {
		t.Errorf("stream_recovery_started = %d, want the one hop", n)
	}
	if ev := logBuf.events(t, "stream_recovery_succeeded"); len(ev) != 0 {
		t.Fatalf("a hop the bound cut was reported as a success: %v", ev)
	}
	failed := logBuf.events(t, "stream_recovery_failed")
	if len(failed) != 1 || failed[0]["phase"] != recoveryMaxElapsed {
		t.Fatalf("stream_recovery_failed = %v, want one max_elapsed", failed)
	}
	if _, has := failed[0]["error"]; has {
		t.Errorf("the bound's own cut was reported as a peer error: %v", failed[0])
	}
	exh := logBuf.events(t, "stream_recovery_exhausted")
	if len(exh) != 1 || exh[0]["reason"] != recoveryMaxElapsed {
		t.Fatalf("stream_recovery_exhausted = %v, want exactly one max_elapsed", exh)
	}
	if exh[0]["recoveries"] != float64(1) {
		t.Errorf("exhausted recoveries = %v, want the cut hop counted", exh[0]["recoveries"])
	}
	trunc := logBuf.events(t, "stream_truncated")
	if len(trunc) != 1 || trunc[0]["recovery_reason"] != recoveryMaxElapsed {
		t.Fatalf("stream_truncated = %v, want the max_elapsed reason", trunc)
	}
	if _, has := trunc[0]["error"]; has {
		t.Errorf("a stop this proxy owns carried an error field: %v", trunc[0])
	}
	if trunc[0]["stream_recoveries"] != float64(1) {
		t.Errorf("stream_recoveries = %v, want 1", trunc[0]["stream_recoveries"])
	}
}

// TestContinuationBodyOutlivesTheDial is the regression for a defect no
// scripted doer could see: a hop's derived context — the one that bounds its
// wait for response HEADERS — was released the moment the dial returned, and
// net/http ties the lifetime of a response body to the context of the request
// that produced it. Every unit test in this package answers dials with
// in-memory bodies that never read a context, so all of them passed while a
// REAL transport lost every byte the upstream had not already buffered with
// the headers: the hop's text was dropped, and the read failed with a canceled
// context, which the loop then reported as the CLIENT leaving.
//
// The transport here is the production direct client against a real server,
// and the server deliberately writes its events after the headers are flushed
// and a beat has passed — so the bytes provably do not exist when Do returns,
// and the only way they can reach the caller is for the context to still be
// alive when they do.
func TestContinuationBodyOutlivesTheDial(t *testing.T) {
	const gap = 50 * time.Millisecond
	served := make(chan struct{}, 1)
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		// Past the point of no return: the dial has returned by now, and the
		// context it ran under has been through every release path there is.
		time.Sleep(gap)
		for _, ev := range []string{sseChat("Hello"), sseChat(", world")} {
			_, _ = io.WriteString(w, ev)
			w.(http.Flusher).Flush()
		}
		served <- struct{}{}
	}))
	defer up.Close()

	endpoint, err := url.Parse(up.URL)
	if err != nil {
		t.Fatalf("parse test endpoint: %v", err)
	}
	now := time.Now()
	// Both envelopes funded: a hop pays out of the candidate's, which the walk
	// opens when it enters the candidate. An envelope with a zero elapsed half
	// is spent the instant it opens (a >= comparison), so both carry a window.
	funded := recovery.Envelope{MaxExchanges: 4, MaxElapsed: time.Minute}
	budget := recovery.NewBudget(funded, func() time.Time { return now })
	budget.BeginCandidate(funded)
	h := &injectorHandler{doers: kindResolver{direct: transport.NewDirectClient(), proxied: transport.NewDirectClient()}}
	client, err := http.NewRequest(http.MethodPost, "http://proxy.invalid/v1/chat/completions", nil)
	if err != nil {
		t.Fatalf("build client request: %v", err)
	}

	dial := h.dialContinuation(continuationHop{
		ctx:       context.Background(),
		window:    newRecoveryWindow(wallRecoveryClock{}, time.Minute),
		client:    client,
		cand:      config.Candidate{Endpoint: endpoint},
		transform: func(body []byte, _ config.Model) ([]byte, error) { return body, nil },
		body:      []byte(`{"model":"up-a"}`),
		suffix:    "/chat/completions",
		budget:    budget,
		now:       now,
	})
	if dial.phase != "" || dial.err != nil {
		t.Fatalf("the hop did not dial: phase=%q err=%v", dial.phase, dial.err)
	}
	if dial.resp == nil || dial.resp.Body == nil {
		t.Fatal("the hop answered without a body to relay")
	}

	// The caller's read, exactly as the loop performs it: after the dial
	// returned, on a context the dial must not have released.
	body, rerr := io.ReadAll(dial.resp.Body)
	_ = dial.resp.Body.Close()
	if rerr != nil {
		t.Fatalf("reading the hop's body failed with %v; a hop that answered headers must stream, not truncate", rerr)
	}
	got := string(body)
	if !strings.Contains(got, "Hello") || !strings.Contains(got, ", world") {
		t.Errorf("relayed %q, want both events; the proxy's own context bound cut the continuation", got)
	}
	<-served
	if n := budget.RequestExchanges(); n != 1 {
		t.Errorf("exchanges claimed = %d, want the one real dial", n)
	}
}

func containsString(set []string, want string) bool {
	for _, s := range set {
		if s == want {
			return true
		}
	}
	return false
}
