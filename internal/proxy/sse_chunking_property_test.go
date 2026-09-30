package proxy

import (
	"bytes"
	"fmt"
	"io"
	"strings"
	"testing"
)

// chunkedReader hands out at most n bytes per Read, so one byte stream can be
// replayed through every transport fragmentation the relay might see. TCP
// makes no promise about where a message boundary lands, so a parser whose
// framing moves with it is a parser whose framing is wrong.
type chunkedReader struct {
	src io.Reader
	n   int
}

func (c *chunkedReader) Read(p []byte) (int, error) {
	if len(p) > c.n {
		p = p[:c.n]
	}
	return c.src.Read(p)
}

// sseFraming is everything about a relayed stream that a reader of the client
// connection can observe: the bytes themselves, the events those bytes
// dispatched, and the payloads the observer saw. Two runs of the same upstream
// bytes must agree on all three — the relayed bytes trivially, and the framing
// only if the parser reads the stream rather than the transport.
type sseFraming struct {
	bytes    string
	events   int
	terminal bool
	payloads []string
}

// sameFraming reports whether two runs put the same stream on the wire: the
// bytes, the events they dispatched, the terminal fact, and the payloads the
// observer saw. The payloads are joined so the comparison is a plain string
// equality and a failure prints the two sides.
func (f sseFraming) sameFraming(o sseFraming) bool {
	return f.bytes == o.bytes &&
		f.events == o.events &&
		f.terminal == o.terminal &&
		strings.Join(f.payloads, "|") == strings.Join(o.payloads, "|")
}

func (f sseFraming) String() string {
	return fmt.Sprintf("bytes=%q events=%d terminal=%v payloads=%q", f.bytes, f.events, f.terminal, f.payloads)
}

// relayChunked runs input through CopySSE with a source that never returns
// more than chunk bytes at a time, collecting the framing the client saw.
func relayChunked(t *testing.T, input string, chunk int) sseFraming {
	t.Helper()
	var dst bytes.Buffer
	var f sseFraming
	stats, err := CopySSE(&dst, &chunkedReader{src: strings.NewReader(input), n: chunk},
		sseRewriter("public-name"), func() {}, nil,
		func(_ []byte, payload []byte) { f.payloads = append(f.payloads, string(payload)) })
	if err != nil {
		t.Fatalf("CopySSE(chunk=%d): %v", chunk, err)
	}
	f.bytes = dst.String()
	f.events = stats.Events
	f.terminal = stats.Terminal
	return f
}

// relayWhole runs input through CopySSE from an in-memory source, which bufio
// fills in a single Read. This is the run that puts every byte of the stream
// in the buffer at once — the fragmentation the bytewise tests never reach.
func relayWhole(t *testing.T, input string) sseFraming {
	t.Helper()
	var dst bytes.Buffer
	var f sseFraming
	stats, err := CopySSE(&dst, strings.NewReader(input),
		sseRewriter("public-name"), func() {}, nil,
		func(_ []byte, payload []byte) { f.payloads = append(f.payloads, string(payload)) })
	if err != nil {
		t.Fatalf("CopySSE(whole): %v", err)
	}
	f.bytes = dst.String()
	f.events = stats.Events
	f.terminal = stats.Terminal
	return f
}

// TestCopySSEIsInvariantUnderChunking is the property the relay owes its
// client: framing is a function of the byte stream alone, so the same upstream
// bytes parse to the same events however the transport split its reads.
//
// The relayed bytes are identical in every case, which is exactly why a
// byte-equality assertion cannot catch a framing defect. The three things that
// move instead are the event count (the flushes a client dispatches on), the
// terminal fact, and the payload each data line carried — the last of which
// the stream-continuation accumulator records, so a payload that folds a CR
// into its text is a continuation that resumes at the wrong offset.
//
// The corpus deliberately mixes terminator styles WITHIN one stream. A stream
// that is uniformly LF, or uniformly CRLF, never places a CR in front of a
// buffered LF, so it cannot reach the defect.
func TestCopySSEIsInvariantUnderChunking(t *testing.T) {
	for _, tc := range []struct {
		name  string
		input string
	}{
		{
			// A CR between two data lines: read whole, the LF at the end is
			// buffered and the CR is data; read bytewise, the CR ends its
			// line immediately. One event or two, purely by read timing.
			name:  "mixed_terminators",
			input: "data: one\rdata: two\n\n",
		},
		{
			// A CR inside a single line's content. Folded into the payload
			// whole, split into a second line bytewise.
			name:  "cr_then_content",
			input: "data: a\rb\n\n",
		},
		{
			// CR CR LF: bytewise this is a data line, a boundary, and the
			// LF of a CRLF pair — one event. Read whole, a parser that looks
			// for the LF first cuts the line at it, leaving "data: a\r" and
			// a CR that is not a boundary — ZERO events, and the client's
			// stream stalls with a payload that never dispatches.
			name:  "cr_boundary_then_crlf",
			input: "data: a\r\r\n",
		},
		{
			// The CR is genuinely the blank separator, with a real frame
			// either side of it and a trailing LF-terminated done line: the
			// event count must be 2 whether the trailing bytes are buffered.
			name:  "cr_boundary_before_lf_frames",
			input: "data: a\r\rdata: [DONE]\n\n",
		},
		{
			// A CRLF pair split across the buffer's edge, with a further
			// CRLF-terminated event after it: the paired LF belongs to its
			// CR and must not be re-read as a blank boundary.
			name:  "split_crlf_then_more_events",
			input: "data: a\r\n\r\ndata: b\r\n\r\ndata: [DONE]\r\n\r\n",
		},
		{
			// A lone CR as the LAST byte of the stream, with no LF to
			// disambiguate it: one event, dispatched before the read that
			// would have proved anything.
			name:  "trailing_lone_cr",
			input: "data: a\r\rdata: [DONE]\r\r",
		},
		{
			name:  "uniform_lf",
			input: "data: a\n\ndata: b\n\ndata: [DONE]\n\n",
		},
		{
			name:  "uniform_crlf",
			input: "data: a\r\n\r\ndata: b\r\n\r\ndata: [DONE]\r\n\r\n",
		},
		{
			name:  "uniform_cr",
			input: "data: a\r\rdata: b\r\rdata: [DONE]\r\r",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			want := relayWhole(t, tc.input)
			if want.bytes != tc.input {
				t.Fatalf("relayed bytes = %q, want exactly %q", want.bytes, tc.input)
			}
			// Every chunk size is checked, not just the two extremes: the
			// defect needs a CR and a later LF in the SAME buffered window,
			// so it hides at large chunks and vanishes at small ones, and a
			// pair of endpoints can straddle the boundary between them.
			for _, chunk := range []int{1, 2, 3, 5, 7, 8, 13, 64, 4096} {
				got := relayChunked(t, tc.input, chunk)
				if got.bytes != want.bytes {
					t.Errorf("chunk=%d: relayed bytes = %q, want %q", chunk, got.bytes, want.bytes)
				}
				if got.events != want.events {
					t.Errorf("chunk=%d: events = %d, want %d (flush count follows)", chunk, got.events, want.events)
				}
				if got.terminal != want.terminal {
					t.Errorf("chunk=%d: terminal = %v, want %v", chunk, got.terminal, want.terminal)
				}
				if strings.Join(got.payloads, "|") != strings.Join(want.payloads, "|") {
					t.Errorf("chunk=%d: observed payloads = %q, want %q", chunk, got.payloads, want.payloads)
				}
			}
		})
	}
}

// TestCopySSELineEndsAtTheEarliestTerminator pins the rule the invariance above
// is an instance of: a line ends at the EARLIEST terminator in the window,
// whichever kind it is. The relayed bytes are the same under every splitting
// here, so the assertions are on the framing.
func TestCopySSELineEndsAtTheEarliestTerminator(t *testing.T) {
	for _, tc := range []struct {
		name         string
		input        string
		wantPayloads []string
		wantEvents   int
	}{
		// CR first: the line ends at the CR, so the LF-terminated rest is a
		// second data line and a following blank line dispatches one event.
		{"cr before lf splits the line", "data: one\rdata: two\n\n", []string{"one", "two"}, 1},
		// LF first: the line ends at the LF, so the CR is content of a later
		// line and that CR is its own terminator.
		{"lf before cr splits the line", "data: one\ndata: two\r\r", []string{"one", "two"}, 1},
		// CR then CRLF: the first CR ends the data line, the second is a
		// blank boundary, the third pairs with the LF. Two events.
		{"cr boundary then crlf", "data: a\r\r\n", []string{"a"}, 1},
		// LF then CRLF: the LF ends the data line and the CRLF is a single
		// blank line, so the frame dispatches once — its LF is the CR's pair
		// rather than a boundary of its own.
		{"lf boundary then crlf", "data: a\n\r\n", []string{"a"}, 1},
		// A bare LF after a CRLF-terminated data line is a genuine blank
		// line — the earlier CRLF already consumed its own LF, so this one
		// dispatches the frame. Confusing the two is how a frame either
		// loses or invents a dispatch.
		{"crlf data then lone lf blank", "data: a\r\n\n", []string{"a"}, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := relayWhole(t, tc.input)
			if got.bytes != tc.input {
				t.Errorf("relayed bytes = %q, want exactly %q", got.bytes, tc.input)
			}
			if strings.Join(got.payloads, "|") != strings.Join(tc.wantPayloads, "|") {
				t.Errorf("observed payloads = %q, want %q", got.payloads, tc.wantPayloads)
			}
			if got.events != tc.wantEvents {
				t.Errorf("events = %d, want %d", got.events, tc.wantEvents)
			}
		})
	}
}

// TestCopySSELineEndsAtTheEarliestTerminatorHoldsForEveryChunkSize is the same
// rule under transport fragmentation, at the sizes a real relay meets rather
// than the two extremes. A terminator split from the byte before it, from the
// byte after it, and from both at once are the cases a single Read cannot
// produce, so they are the cases worth enumerating.
func TestCopySSELineEndsAtTheEarliestTerminatorHoldsForEveryChunkSize(t *testing.T) {
	for _, input := range []string{
		"data: one\rdata: two\n\n",
		"data: one\ndata: two\r\r",
		"data: a\r\r\n",
		"data: a\n\r\n",
		"data: a\r\n\r\n",
		"data: a\n\n",
		"data: a\r\r",
		": ping\r\n\r\ndata: [DONE]\r\n\r\n",
	} {
		t.Run(fmt.Sprintf("%q", input), func(t *testing.T) {
			want := relayWhole(t, input)
			for _, chunk := range []int{1, 2, 3, 4, 5, 6, 7, 8, 16, 4096} {
				got := relayChunked(t, input, chunk)
				if !got.sameFraming(want) {
					t.Errorf("chunk=%d: %s, want %s", chunk, got, want)
				}
			}
		})
	}
}
