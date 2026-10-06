package proxy

import (
	"bytes"
	"strings"
	"testing"

	"openai-compatible-injector/internal/inject"
)

// StreamStats.Terminal is "did the client ever receive its terminal marker" —
// the ONE fact that separates a stream that ENDED from one that was CUT, since
// CopySSE maps io.EOF to a nil error in both cases. The post-commitment
// recovery loop and the keep-alive heartbeat are both gated on it, so the
// predicate has to be exact in both directions: a real terminal that is missed
// reports a finished stream as truncated (and an operator is sent after a
// provider that behaved), and a non-terminal that is accepted reports a cut
// stream as finished (and the service tells the precise lie the feature exists
// to stop).
//
// The predicate is deliberately STRICTER than generic SSE. The SSE grammar
// admits `data:[DONE]` (no separator space), `data:  [DONE]` (two spaces), and
// arbitrary field/comment padding; this relay recognizes the exact bytes its
// two upstream surfaces actually terminate with, and everything else is a
// payload. Broadening that would let provider output — or a client-visible
// string that merely quotes a token — pass as the end of a generation. The
// strictness is the contract, and the near-miss table below pins it.

// responsesRewriter is sseRewriter's counterpart for the Responses surface,
// mirroring how the relay picks its rewriter from the request's URL path.
func responsesRewriter(public string) func([]byte) []byte {
	return func(p []byte) []byte { return inject.RewriteResponsesModel(p, public) }
}

// copySSEStatsFor runs CopySSE over input with the given surface's rewriter and
// returns the stats, the exact relayed bytes, and the error.
func copySSEStatsFor(t *testing.T, rewrite func([]byte) []byte, input string) (StreamStats, string, error) {
	t.Helper()
	var buf bytes.Buffer
	stats, err := CopySSE(&buf, strings.NewReader(input), rewrite, func() {}, nil, nil)
	return stats, buf.String(), err
}

// TestCopySSEStreamEndingIsNeverTerminal pins the case the whole recovery
// feature exists for: the upstream connection simply closes, with no marker.
// io.EOF is a nil error there, so Terminal is the only signal that the
// generation did not finish — a regression that marked an EOF terminal would
// report every cut stream as completed and disable recovery entirely.
func TestCopySSEStreamEndingIsNeverTerminal(t *testing.T) {
	for _, tc := range []struct {
		name  string
		input string
	}{
		{"clean close after one event", "data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n"},
		{"clean close mid-event", "data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n"},
		{"empty body", ""},
		{"comment only", ": keep-alive\n\n"},
		{"responses delta then close", "event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\"}\n\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stats, out, err := copySSEStatsFor(t, responsesRewriter("public"), tc.input)
			if err != nil {
				t.Fatalf("CopySSE: %v", err)
			}
			if out != tc.input {
				t.Fatalf("relayed bytes changed: %q", out)
			}
			if stats.Terminal {
				t.Fatal("a stream was marked terminal merely because the upstream closed the connection")
			}
		})
	}
}

// TestCopySSEReadFailureIsNeverTerminal covers the other cut shape: the
// upstream dies mid-answer (a reset connection) rather than closing cleanly.
// CopySSE returns the read error, so the caller has a second signal here — but
// the terminal record must still be false, because a byte count and an error
// that both describe a dead generation must not be paired with "the client got
// its marker".
func TestCopySSEReadFailureIsNeverTerminal(t *testing.T) {
	var buf bytes.Buffer
	stats, err := CopySSE(&buf, &errBody{data: []byte("data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n")},
		sseRewriter("public"), nil, nil, nil)
	if err == nil {
		t.Fatal("a read failure was not propagated")
	}
	if stats.Terminal {
		t.Fatal("a stream that died on the wire was marked terminal")
	}
	if buf.String() != "data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n" {
		t.Fatalf("the bytes received before the failure were not relayed: %q", buf.String())
	}
}

// TestCopySSEChatTerminalIsTheDONEDataLine pins the chat surface's one
// terminal: the exact `data: [DONE]` line. A [DONE] that is not a chat data
// line is not a terminator.
func TestCopySSEChatTerminalIsTheDONEDataLine(t *testing.T) {
	stats, out, err := copySSEStatsFor(t, sseRewriter("public"),
		"data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\ndata: [DONE]\n\n")
	if err != nil {
		t.Fatalf("CopySSE: %v", err)
	}
	if !stats.Terminal {
		t.Fatal("a relayed chat [DONE] did not set Terminal")
	}
	if out != "data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\ndata: [DONE]\n\n" {
		t.Fatalf("relayed bytes changed: %q", out)
	}
}

// TestCopySSEResponsesTerminalIsTheCompletedEvent pins the Responses surface's
// terminal: the `event: response.completed` EVENT line. A data payload that
// merely mentions the type — the shape a delta or a client-quoted string can
// carry — is not the event, and must not terminate the stream.
func TestCopySSEResponsesTerminalIsTheCompletedEvent(t *testing.T) {
	stats, out, err := copySSEStatsFor(t, responsesRewriter("public"),
		"event: response.completed\ndata: {\"type\":\"response.completed\"}\n\n")
	if err != nil {
		t.Fatalf("CopySSE: %v", err)
	}
	if !stats.Terminal {
		t.Fatal("a relayed response.completed event did not set Terminal")
	}
	if out != "event: response.completed\ndata: {\"type\":\"response.completed\"}\n\n" {
		t.Fatalf("relayed bytes changed: %q", out)
	}

	// The same bytes as a DATA line only — no event line. This is not a
	// terminal event; it is a payload that names one.
	stats, _, err = copySSEStatsFor(t, responsesRewriter("public"),
		"data: {\"type\":\"response.completed\"}\n\n")
	if err != nil {
		t.Fatalf("CopySSE: %v", err)
	}
	if stats.Terminal {
		t.Fatal("a data payload naming response.completed was accepted as the terminal event")
	}
}

// TestCopySSETerminalNearMissesDoNotTerminate pins the exact-match rule from
// the other side: every line below is one byte — or one field — away from a
// terminal marker, and none of them is one. Accepting any of these would let
// model output or provider decoration end a generation on the client's behalf.
func TestCopySSETerminalNearMissesDoNotTerminate(t *testing.T) {
	cases := []struct {
		name  string
		input string
	}{
		{"trailing space after [DONE]", "data: [DONE] \n\n"},
		{"trailing tab after [DONE]", "data: [DONE]\t\n\n"},
		{"lower-case [done]", "data: [done]\n\n"},
		{"mixed-case [Done]", "data: [Done]\n\n"},
		{"no separator space", "data:[DONE]\n\n"},
		{"two separator spaces", "data:  [DONE]\n\n"},
		{"[DONE] inside a JSON string value", "data: {\"choices\":[{\"delta\":{\"content\":\"data: [DONE]\"}}]}\n\n"},
		{"[DONE] as part of a longer payload", "data: [DONE] please continue\n\n"},
		{"[DONE] prefixed in the payload", "data: NOT [DONE]\n\n"},
		{"comment line", ": [DONE]\n\n"},
		{"[DONE] as an event line", "event: [DONE]\n\n"},
		{"[DONE] as an id line", "id: [DONE]\n\n"},
		{"response.completed with trailing space", "event: response.completed \n\n"},
		{"response.completed prefixed", "event: xresponse.completed\n\n"},
		{"response.completed in a comment", ": event: response.completed\n\n"},
		{"response.completed in a data payload", "data: event: response.completed\n\n"},
		{"torn final line", "data: [DO"},
		{"torn final event line", "event: response.compl"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stats, out, err := copySSEStatsFor(t, sseRewriter("public"), tc.input)
			if err != nil {
				t.Fatalf("CopySSE: %v", err)
			}
			if out != tc.input {
				t.Fatalf("relayed bytes changed: %q", out)
			}
			if stats.Terminal {
				t.Fatalf("near-miss %q was accepted as a terminal marker", tc.input)
			}
		})
	}
}

// TestCopySSETerminalMarkersWithEveryLineEnding pins the positive controls the
// near-miss table must not be confused with: a real marker terminates under
// LF, CR, and CRLF, and the relay emits the terminator's own bytes unchanged.
// The parser already recognizes all three endings; a terminal check that only
// matched LF would report a CRLF stream as cut.
func TestCopySSETerminalMarkersWithEveryLineEnding(t *testing.T) {
	cases := []struct {
		name  string
		input string
	}{
		{"chat LF", "data: [DONE]\n"},
		{"chat CR", "data: [DONE]\r"},
		{"chat CRLF", "data: [DONE]\r\n"},
		{"responses LF", "event: response.completed\n"},
		{"responses CR", "event: response.completed\r"},
		{"responses CRLF", "event: response.completed\r\n"},
		// A final line without a terminator: the relay forwards it and the
		// client got the marker's bytes, so the stream is terminated.
		{"chat unterminated EOF", "data: [DONE]"},
		{"responses unterminated EOF", "event: response.completed"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stats, out, err := copySSEStatsFor(t, sseRewriter("public"), tc.input)
			if err != nil {
				t.Fatalf("CopySSE: %v", err)
			}
			if !stats.Terminal {
				t.Fatalf("a complete marker under this ending was missed: %q", tc.input)
			}
			if out != tc.input {
				t.Fatalf("marker bytes were rewritten: got %q want %q", out, tc.input)
			}
		})
	}
}

// TestCopySSETerminalIsSurfaceBlind documents the ACTUAL scope of the
// predicate, which is shared by both surfaces: it recognizes either marker on
// either relay, because CopySSE is handed a rewriter and no API identity. The
// two markers cannot appear in the other surface's legitimate stream, and an
// upstream that wants to fake a termination can already emit its own surface's
// marker, so there is no adversarial delta here — but it IS a surface-blind
// acceptance, not the per-path detection a stricter reading would expect.
// Scoping it to the request's API would mean threading the surface through
// CopySSE (a handler.go signature change); see the audit report.
func TestCopySSETerminalIsSurfaceBlind(t *testing.T) {
	stats, _, err := copySSEStatsFor(t, sseRewriter("public"), "event: response.completed\n")
	if err != nil {
		t.Fatalf("CopySSE: %v", err)
	}
	if !stats.Terminal {
		t.Fatal("current behaviour changed: a responses marker on a chat relay is accepted")
	}

	stats, _, err = copySSEStatsFor(t, responsesRewriter("public"), "data: [DONE]\n")
	if err != nil {
		t.Fatalf("CopySSE: %v", err)
	}
	if !stats.Terminal {
		t.Fatal("current behaviour changed: a chat marker on a responses relay is accepted")
	}
}

// TestCopySSETerminalIsForwardedOnceAndLaterBytesStillRelay pins the tail
// contract: the relay never stops at the marker and never synthesizes
// anything around it. Whatever the upstream sent after the terminal line is
// relayed exactly as it arrived — the record is a statement about what reached
// the client, not a licence to drop or invent bytes.
func TestCopySSETerminalIsForwardedOnceAndLaterBytesStillRelay(t *testing.T) {
	input := "data: [DONE]\n\n" + ": a late comment\n" + "data: after-the-end\n\n"
	stats, out, err := copySSEStatsFor(t, sseRewriter("public"), input)
	if err != nil {
		t.Fatalf("CopySSE: %v", err)
	}
	if !stats.Terminal {
		t.Fatal("the forwarded marker did not set Terminal")
	}
	if out != input {
		t.Fatalf("bytes after the marker were not relayed verbatim:\n got %q\nwant %q", out, input)
	}
	if n := strings.Count(out, "data: [DONE]"); n != 1 {
		t.Fatalf("the marker was duplicated or lost: %d occurrences in %q", n, out)
	}
}

// TestCopySSEMessagesTerminalIsTheMessageStopEvent pins the third marker:
// the Anthropic surface's `event: message_stop`, which CopyMessagesSSE
// synthesizes. It is recognized at the EVENT line, like the Responses
// marker, because that is the line the frame builder writes first and the
// line the keep-alive latches on.
func TestCopySSEMessagesTerminalIsTheMessageStopEvent(t *testing.T) {
	stats, out, err := copySSEStatsFor(t, sseRewriter("public"), "event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n")
	if err != nil {
		t.Fatalf("CopySSE: %v", err)
	}
	if !stats.Terminal {
		t.Fatal("a message_stop event did not set Terminal")
	}
	if out != "event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n" {
		t.Fatalf("relayed bytes changed: %q", out)
	}

	// The data payload naming the type — a shape a client string or a
	// provider decoration could carry — is not the event.
	stats, _, err = copySSEStatsFor(t, sseRewriter("public"), "data: {\"type\":\"message_stop\"}\n\n")
	if err != nil {
		t.Fatalf("CopySSE: %v", err)
	}
	if stats.Terminal {
		t.Fatal("a data payload naming message_stop was accepted as the terminal event")
	}
}

// TestCopySSEMessagesTerminalNearMissesDoNotTerminate pins message_stop's
// exact-match rule from the near side: everything below is one byte — or one
// field — away from the marker, and none of them is it. Note that
// `event:message_stop` without the separator space is a VALID event name to
// the SSE parser and is still not a terminal: the predicate matches the
// exact bytes the frame builder writes, and the parser's tolerance is a
// separate question.
func TestCopySSEMessagesTerminalNearMissesDoNotTerminate(t *testing.T) {
	cases := []struct {
		name  string
		input string
	}{
		{"trailing space", "event: message_stop \n\n"},
		{"trailing tab", "event: message_stop\t\n\n"},
		{"no separator space", "event:message_stop\n\n"},
		{"two separator spaces", "event:  message_stop\n\n"},
		{"lower case", "event: Message_Stop\n\n"},
		{"prefixed", "event: xmessage_stop\n\n"},
		{"suffixed", "event: message_stop_suffix\n\n"},
		{"hyphenated", "event: message-stop\n\n"},
		{"in a comment", ": event: message_stop\n\n"},
		{"in a data payload", "data: event: message_stop\n\n"},
		{"as an id line", "id: message_stop\n\n"},
		{"torn final event line", "event: message_sto"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stats, out, err := copySSEStatsFor(t, sseRewriter("public"), tc.input)
			if err != nil {
				t.Fatalf("CopySSE: %v", err)
			}
			if out != tc.input {
				t.Fatalf("relayed bytes changed: %q", out)
			}
			if stats.Terminal {
				t.Fatalf("near-miss %q was accepted as a terminal marker", tc.input)
			}
		})
	}
}

// TestCopySSEMessagesTerminalWithEveryLineEnding pins the positive controls
// the near-miss table must not be confused with: a complete message_stop
// terminates under LF, CR, CRLF, and with no terminator at all.
func TestCopySSEMessagesTerminalWithEveryLineEnding(t *testing.T) {
	for _, tc := range []struct {
		name  string
		input string
	}{
		{"LF", "event: message_stop\n"},
		{"CR", "event: message_stop\r"},
		{"CRLF", "event: message_stop\r\n"},
		{"unterminated EOF", "event: message_stop"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stats, out, err := copySSEStatsFor(t, sseRewriter("public"), tc.input)
			if err != nil {
				t.Fatalf("CopySSE: %v", err)
			}
			if !stats.Terminal {
				t.Fatalf("a complete marker under this ending was missed: %q", tc.input)
			}
			if out != tc.input {
				t.Fatalf("marker bytes were rewritten: got %q want %q", out, tc.input)
			}
		})
	}
}
