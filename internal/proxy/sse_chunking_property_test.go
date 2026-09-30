package proxy

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"
)

// ---------------------------------------------------------------------------
// This file closes the GAP recorded under INV-SSE-03 in
// docs/design/behavioral-contract.md, which reads:
//
//   "internal/proxy/fuzz_test.go exists; the arbitrary-chunk-boundary
//    property is not yet asserted as a chunking-invariance property test."
//
// The existing suite proves the line grammar one fragmentation at a time.
// byteReader hands back exactly one byte per Read, which is the extreme case
// and a genuinely valuable test — but a single fragmentation proves the parser
// survives THAT split, not that it is invariant under the split. TCP makes no
// promise about where a read boundary falls: the same upstream can deliver the
// same bytes as one 8 KiB read or as forty 200-byte reads, and the client must
// receive identical bytes either way.
//
// So the property here is stated over the SPLIT rather than over any one
// split: for a fixed stream, run CopySSE under every chunking of that stream
// (and a deterministic spread of sizes above and below it) and require that
// output bytes, event count, terminal flag, byte count, error identity and the
// observe sequence are all identical.
//
// The cases are chosen for what a boundary-straddling parser gets wrong, one
// hazard per line. A CRLF split across two reads is the sharpest: the CR is a
// complete line ending in its own right, so a parser that decides eagerly
// emits a spurious blank line and inflates the event count by one. The
// terminator itself split from its content does the same thing one level up.
// A lone CR followed by a real LF of its own is the case that distinguishes
// "the LF belongs to the CRLF" from "the LF is a blank line" — the two differ
// only in whether the payload before them ended with CR, and getting it wrong
// either drops a byte or invents an event.
// ---------------------------------------------------------------------------

// chunkReader hands back exactly the next n bytes per Read, so a test can
// pick the fragmentation. Unlike byteReader it never over-reads: the returned
// slice is bounded to what remains, and the final Read reports io.EOF with
// whatever is left, matching a real short read.
type chunkReader struct {
	data  []byte
	chunk int
	pos   int
}

func (c *chunkReader) Read(p []byte) (int, error) {
	// bufio probes with an empty buffer; io.Reader forbids a non-nil error
	// there, and answering EOF would make the source look closed.
	if len(p) == 0 {
		return 0, nil
	}
	if c.pos >= len(c.data) {
		return 0, io.EOF
	}
	n := c.chunk
	if n > len(p) {
		n = len(p)
	}
	if c.pos+n > len(c.data) {
		n = len(c.data) - c.pos
	}
	copy(p, c.data[c.pos:c.pos+n])
	c.pos += n
	return n, nil
}

// observeRecord is one observation the relay made, captured so the sequence
// can be compared across fragmentations. The payload is COPIED because
// CopySSE's contract is that the observer may retain what it is given and the
// line buffer is reused.
type observeRecord struct {
	name    string
	payload string
}

// sseRunResult is everything observable about one CopySSE pass. Two passes
// over the same bytes with different fragmentations must agree on every field.
type sseRunResult struct {
	out      string
	bytes    int64
	events   int
	terminal bool
	errText  string
	observed []observeRecord
	flushes  int
}

// runFragmented runs CopySSE over input with the given read-chunk size and
// records every observable it produces.
func runFragmented(t *testing.T, input string, chunk int) sseRunResult {
	t.Helper()
	var buf bytes.Buffer
	flushes := 0
	var observed []observeRecord
	stats, err := CopySSE(
		&buf,
		&chunkReader{data: []byte(input), chunk: chunk},
		sseRewriter("public-name"),
		func() { flushes++ },
		nil,
		func(name, payload []byte) {
			observed = append(observed, observeRecord{
				name:    string(name),
				payload: string(append([]byte(nil), payload...)),
			})
		},
	)
	res := sseRunResult{
		out:      buf.String(),
		bytes:    stats.Bytes,
		events:   stats.Events,
		terminal: stats.Terminal,
		flushes:  flushes,
		observed: observed,
	}
	if err != nil {
		res.errText = err.Error()
	}
	return res
}

// sseChunkingCorpus is the stream matrix. Every entry is a valid SSE body; the
// point is not that each is correct but that each is FRAGMENTABLE at every
// offset without changing what the client receives.
var sseChunkingCorpus = []struct {
	name  string
	input string
}{
	{
		name:  "simple_lf",
		input: "data: one\ndata: two\n\ndata: three\n\n",
	},
	{
		// The CRLF case: every terminator is two bytes, so a naive parser
		// that cuts on CR alone emits a blank line per event boundary.
		name:  "all_crlf",
		input: "event: alpha\r\ndata: one\r\n\r\nevent: beta\r\ndata: two\r\n\r\n",
	},
	{
		// A lone CR is a complete terminator in its own right, so this
		// stream's boundaries are single bytes and every one of them is a
		// place a boundary-straddling read changes the answer.
		name:  "all_lone_cr",
		input: "event: alpha\rdata: one\r\revent: beta\rdata: two\r\r",
	},
	{
		// Mixed terminators in one stream: the grammar must not settle on
		// one style because the first bytes happened to be that style. This
		// is also the case that caught the readToLineEnd ordering bug — a
		// lone CR followed later by a real LF, in the same buffered window.
		// See TestCopySSELineEndsAtTheEarliestTerminator.
		name:  "mixed_terminators",
		input: "data: one\r\ndata: two\ndata: three\r\rdata: four\n\ndata: five\r\n\r\n",
	},
	{
		// Multi-line data: the EventSource grammar joins every data: line in
		// a frame with LF, and the relay must observe each line separately
		// while the client receives them all.
		name:  "multiline_data",
		input: "event: chunk\ndata: line one\ndata: line two\ndata: line three\n\ndata: solo\n\n",
	},
	{
		// A payload containing CR and LF INSIDE a data line is not a
		// terminator: only an unescaped terminator at a line start is. This
		// is the case a "split on any CR or LF byte" parser fails.
		name:  "embedded_newlines_in_json",
		input: "data: {\"a\":\"line1\\nline2\",\"b\":\"x\"}\ndata: tail\n\n",
	},
	{
		// A CR immediately followed by content with no LF: the CR ends the
		// line and the next byte starts a new one. This entry is the smallest
		// witness of the readToLineEnd ordering bug: while the whole stream
		// sits in one buffer, the trailing "\n\n" was enough to make the
		// parser take its LF branch and swallow the CR, and once the stream
		// was split small enough that the CR arrived without a buffered LF
		// the same bytes parsed correctly. Identical wire bytes, two answers.
		name:  "cr_then_content",
		input: "data: one\rdata: two\n\n",
	},
	{
		// An empty event (boundary with no data) between two frames.
		name:  "empty_event",
		input: "data: one\n\n\n\ndata: two\n\n",
	},
	{
		// The terminal marker, so chunking invariance is asserted on the
		// Terminal flag too and not only on the bytes.
		name:  "terminal_chat",
		input: "data: one\n\ndata: [DONE]\n\n",
	},
	{
		// The Responses terminal form, which is an event: line paired with a
		// data: line rather than a bare data: line.
		name:  "terminal_responses",
		input: "event: response.completed\ndata: {\"response\":{\"model\":\"m\"}}\n\n",
	},
	{
		// No trailing boundary at all: a final unterminated line is relayed
		// as-is, and that must not depend on where the last read ended.
		name:  "unterminated_tail",
		input: "data: one\n\ndata: trailing",
	},
	{
		// Comment and id/retry field lines, which are neither data nor event
		// and must not be mistaken for either.
		name:  "comment_and_field_lines",
		input: ": a comment\nid: 7\nretry: 100\ndata: one\n\n",
	},
	{
		// A field name that merely starts with "data" is not a data line.
		name:  "field_prefix_lookalike",
		input: "datax: not data\ndatabase: also not\ndata: real\n\n",
	},
}

// chunkSizes is the fragmentation schedule. Size 1 is the extreme case
// (already covered by byteReader elsewhere, included so the property is
// self-contained); the rest are deliberately UNEVEN, including values larger
// than the whole stream, because a uniform size k would let a parser that is
// only accidentally correct at one particular stride still pass every case.
var chunkSizes = []int{1, 2, 3, 5, 7, 8, 13, 64, 4096}

// TestCopySSEIsInvariantUnderChunking is the property: the client's received
// bytes, the event count, the terminal flag, the byte count, the error, the
// flush count and the observation sequence are functions of the STREAM, not of
// how the transport fragmented it.
//
// The baseline is the single-read pass (a chunk larger than the stream). Every
// other fragmentation must reproduce it exactly. A difference is not a
// tolerance to be widened — it is a parser bug, and the subtest names both
// the corpus entry and the offending chunk size so the failure is actionable.
func TestCopySSEIsInvariantUnderChunking(t *testing.T) {
	for _, tc := range sseChunkingCorpus {
		t.Run(tc.name, func(t *testing.T) {
			want := runFragmented(t, tc.input, len(tc.input)+16)
			for _, size := range chunkSizes {
				got := runFragmented(t, tc.input, size)
				if got.out != want.out {
					t.Errorf("chunk=%d: relayed bytes differ from the single-read baseline.\n want %q\n got  %q", size, want.out, got.out)
				}
				if got.bytes != want.bytes {
					t.Errorf("chunk=%d: stats.Bytes = %d, want %d", size, got.bytes, want.bytes)
				}
				if got.events != want.events {
					t.Errorf("chunk=%d: stats.Events = %d, want %d — a frame "+
						"boundary was derived from a read boundary", size, got.events, want.events)
				}
				if got.terminal != want.terminal {
					t.Errorf("chunk=%d: stats.Terminal = %v, want %v", size, got.terminal, want.terminal)
				}
				if got.flushes != want.flushes {
					t.Errorf("chunk=%d: flush count = %d, want %d", size, got.flushes, want.flushes)
				}
				if got.errText != want.errText {
					t.Errorf("chunk=%d: error = %q, want %q", size, got.errText, want.errText)
				}
				if len(got.observed) != len(want.observed) {
					t.Errorf("chunk=%d: observed %d payloads, want %d", size, len(got.observed), len(want.observed))
					continue
				}
				for i := range want.observed {
					if got.observed[i] != want.observed[i] {
						t.Errorf("chunk=%d: observation %d = %+v, want %+v", size, i, got.observed[i], want.observed[i])
					}
				}
			}
		})
	}
}

// TestCopySSELineEndsAtTheEarliestTerminator is the characterization test for
// the defect this file was written to find, isolated from the property that
// found it.
//
// The defect: readToLineEnd asked "does this buffered window contain an LF
// anywhere?" BEFORE "where is the first CR?". A window holding a CR at index 9
// and an LF at index 20 therefore ended the line at 20, and the CR became part
// of the line's data. It is invisible in the relayed BYTES — every byte still
// reached the client in order, so a byte-equality assertion passes — and in a
// stream with no LF at all the old order was right by accident. It became
// visible only as the difference between:
//
//	"data: one\rdata: two\n\n"  read whole  -> one data line, "one\rdata: two"
//	"data: one\rdata: two\n\n"  read short  -> two data lines, "one" then "two"
//
// Same wire bytes, two different frames, decided by where the transport split
// its reads. Everything downstream of framing inherits that: the event count,
// the flush cadence the client observes, and the payload the continuation
// accumulator reconstructs a truncated stream from.
//
// So this asserts the frame grammar at every split offset of a stream that
// contains BOTH terminator styles — the shape no existing test had, which is
// precisely why the defect survived: TestCopySSELineEndingsSurviveReadFragmentation
// uses all-CR and all-CRLF streams, and bufio fills a whole source in one Read,
// so both of its cases never put a CR in front of a buffered LF.
func TestCopySSELineEndsAtTheEarliestTerminator(t *testing.T) {
	cases := []struct {
		name string
		// input, the data payloads its lines must yield in order, and the
		// number of EVENTS. Those are separate counts: several data lines
		// inside one frame are one event, so a multi-line case below has two
		// payloads and one event. An earlier draft of this test assumed they
		// were the same number, which is not the EventSource rule.
		input    string
		payloads []string
		events   int
	}{
		{
			// The minimum witness: one lone CR, then content, then a real LF.
			// Both lines belong to the SAME frame — there is no blank line
			// between them — so this is two payloads and one event.
			name:     "lone_cr_then_lf_is_not_one_line",
			input:    "data: one\rdata: two\n\n",
			payloads: []string{"one", "two"},
			events:   1,
		},
		{
			// A CR, then content, then a blank CRLF line. The blank line
			// dispatches, so this is two payloads and one event.
			name:     "cr_then_content_then_crlf",
			input:    "data: one\rdata: two\r\n\r\n",
			payloads: []string{"one", "two"},
			events:   1,
		},
		{
			// A blank line made of a lone CR dispatches the first frame on
			// its own, so the trailing LF frame is a second event.
			name:     "blank_lone_cr_does_not_merge_following_frame",
			input:    "data: one\r\rdata: two\n\n",
			payloads: []string{"one", "two"},
			events:   2,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// The single-read pass is the ground truth: with the whole stream
			// buffered there is no split to depend on.
			want := runFragmented(t, tc.input, len(tc.input)+16)
			if len(want.observed) != len(tc.payloads) {
				t.Fatalf("single-read baseline observed %v, want %d payloads %q",
					want.observed, len(tc.payloads), tc.payloads)
			}
			for i, p := range tc.payloads {
				if want.observed[i].payload != p {
					t.Fatalf("single-read baseline payload %d = %q, want %q",
						i, want.observed[i].payload, p)
				}
			}
			if want.events != tc.events {
				t.Fatalf("single-read baseline reports %d events, want %d",
					want.events, tc.events)
			}

			// Then every split offset, which is where an ordering that asks
			// about the wrong terminator first gives a different answer.
			for split := 1; split < len(tc.input); split++ {
				var buf bytes.Buffer
				var observed []observeRecord
				stats, err := CopySSE(&buf, &splitReader{data: []byte(tc.input), split: split},
					sseRewriter("public-name"), nil, nil,
					func(name, payload []byte) {
						observed = append(observed, observeRecord{
							name:    string(name),
							payload: string(append([]byte(nil), payload...)),
						})
					})
				if err != nil {
					t.Fatalf("split=%d: CopySSE: %v", split, err)
				}
				if buf.String() != tc.input {
					t.Errorf("split=%d: relayed %q, want %q", split, buf.String(), tc.input)
				}
				if stats.Events != tc.events {
					t.Errorf("split=%d: stats.Events = %d, want %d — a terminator "+
						"was passed over because a later one of a different style "+
						"was already buffered", split, stats.Events, tc.events)
				}
				if len(observed) != len(tc.payloads) {
					t.Errorf("split=%d: observed %d payloads, want %d: %v",
						split, len(observed), len(tc.payloads), observed)
					continue
				}
				for i, p := range tc.payloads {
					if observed[i].payload != p {
						t.Errorf("split=%d: payload %d = %q, want %q", split, i, observed[i].payload, p)
					}
				}
			}
		})
	}
}

// TestCopySSELineEndsAtTheEarliestTerminatorHoldsForEveryChunkSize restates
// the same property over a whole-buffer pass, because the split-offset test
// above puts a read boundary between the CR and everything after it — the case
// where the old code took the CR branch and happened to be right. The failing
// case is the opposite: CR and LF in ONE buffer, which is what a real upstream
// connection produces.
func TestCopySSELineEndsAtTheEarliestTerminatorHoldsForEveryChunkSize(t *testing.T) {
	const input = "data: one\rdata: two\n\n"
	want := []string{"one", "two"}
	for _, size := range chunkSizes {
		var observed []observeRecord
		var buf bytes.Buffer
		stats, err := CopySSE(&buf, &chunkReader{data: []byte(input), chunk: size},
			sseRewriter("public-name"), nil, nil,
			func(name, payload []byte) {
				observed = append(observed, observeRecord{
					name:    string(name),
					payload: string(append([]byte(nil), payload...)),
				})
			})
		if err != nil {
			t.Fatalf("chunk=%d: CopySSE: %v", size, err)
		}
		if len(observed) != len(want) {
			t.Errorf("chunk=%d: observed %d payloads, want %d: %v", size, len(observed), len(want), observed)
			continue
		}
		for i, p := range want {
			if observed[i].payload != p {
				t.Errorf("chunk=%d: payload %d = %q, want %q", size, i, observed[i].payload, p)
			}
		}
		if stats.Events != 1 {
			t.Errorf("chunk=%d: stats.Events = %d, want 1 — two data lines inside "+
				"one frame are one event, and the CR between them must not "+
				"have closed the frame early", size, stats.Events)
		}
	}
}

// TestCopySSEChunkInvarianceCoversEverySplitOffset is the exhaustive half, and
// it is deliberately narrow where the property test above is broad.
//
// The property test samples a handful of chunk sizes. This one takes ONE
// adversarial stream — all-CRLF, the case whose blank-line accounting is
// decided by a single byte's arrival timing — and runs CopySSE with EVERY
// possible two-chunk split offset. For a stream of n bytes there are n-1
// interior offsets, so a defect that only appears when one specific byte
// lands alone at a read boundary cannot hide between two sampled sizes.
//
// This is the mutation that catches a parser rewritten to "wait for the next
// byte before deciding a CR was a terminator": that version passes every
// hand-written case and fails exactly one offset here.
func TestCopySSEChunkInvarianceCoversEverySplitOffset(t *testing.T) {
	const input = "event: alpha\r\ndata: one\r\n\r\nevent: beta\r\ndata: two\r\n\r\n"
	want := runFragmented(t, input, len(input)+16)

	for split := 1; split < len(input); split++ {
		// A reader that hands the stream over in exactly two Reads at the
		// given offset. Both halves are non-empty by construction.
		r := &splitReader{data: []byte(input), split: split}
		var buf bytes.Buffer
		stats, err := CopySSE(&buf, r, sseRewriter("public-name"), nil, nil, nil)
		gotErr := ""
		if err != nil {
			gotErr = err.Error()
		}
		if buf.String() != want.out {
			t.Fatalf("split=%d: relayed bytes differ.\n want %q\n got  %q", split, want.out, buf.String())
		}
		if stats.Events != want.events {
			t.Fatalf("split=%d: stats.Events = %d, want %d — the CR at the "+
				"split boundary was not carried across the read", split, stats.Events, want.events)
		}
		if stats.Terminal != want.terminal {
			t.Fatalf("split=%d: stats.Terminal = %v, want %v", split, stats.Terminal, want.terminal)
		}
		if gotErr != want.errText {
			t.Fatalf("split=%d: error = %q, want %q", split, gotErr, want.errText)
		}
	}
}

// splitReader delivers the stream in exactly two Reads, the second beginning
// at the split offset. It is the precise instrument for "exactly one boundary,
// exactly here", which a fixed chunk size cannot express.
//
// Three details are load-bearing, and each was a bug in an earlier version of
// this helper that a lucky stream hid:
//
//   - The first Read returns data[:split] and every later Read resumes AT
//     split. Serving the tail from offset 0 on the first call silently drops
//     the head, which showed up as a stream relayed one byte short.
//   - The served offset advances every call. A reader that re-copied the same
//     tail forever hands bufio the same bytes again, and CopySSE spins on a
//     stream whose framing depends on where a CR sat relative to a buffered LF.
//   - A zero-length destination returns (0, nil), NOT io.EOF. bufio probes
//     with an empty buffer to test for a closed stream, and io.Reader forbids a
//     non-nil error for len(p) == 0 outright.
//
// The first version was written for an all-CRLF stream, where the parser's LF
// branch consumed a duplicated tail before any of this could show. A helper
// that only works for one grammar is a test that only tests one grammar.
type splitReader struct {
	data  []byte
	split int
	pos   int
	reads int
}

func (r *splitReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if r.reads > 0 && r.pos >= len(r.data) {
		return 0, io.EOF
	}
	from := r.pos
	if r.reads == 0 {
		// The first read stops at the boundary and nothing further: the
		// whole point is that the tail arrives in a SEPARATE call, so the
		// parser has to reach its decision without it in the buffer.
		if r.split > len(r.data) {
			r.split = len(r.data)
		}
		from, r.pos = 0, r.split
	} else {
		r.pos = len(r.data)
	}
	r.reads++
	n := copy(p, r.data[from:r.pos])
	if n == 0 {
		return 0, io.EOF
	}
	return n, nil
}

// TestCopySSECRLFSplitAtEveryOffsetIsNotABlankLine isolates the single
// mechanism the whole pendingCRLF branch exists for, and asserts it at every
// offset of a minimal stream.
//
// The claim: for "data: x\r\n\r\n" the relay reports exactly ONE event and
// emits the bytes unchanged, no matter which byte of the CRLF pair is the read
// boundary. A parser that cuts the line at CR and then treats the arriving LF
// as a fresh blank line reports TWO events and flushes twice — the client
// still receives the right bytes, so only the event count and the flush
// cadence betray it. That is precisely why a bytes-only assertion would pass.
func TestCopySSECRLFSplitAtEveryOffsetIsNotABlankLine(t *testing.T) {
	const input = "data: x\r\n\r\n"
	for split := 1; split < len(input); split++ {
		var buf bytes.Buffer
		flushes := 0
		stats, err := CopySSE(&buf, &splitReader{data: []byte(input), split: split},
			sseRewriter("public-name"), func() { flushes++ }, nil, nil)
		if err != nil {
			t.Fatalf("split=%d: CopySSE: %v", split, err)
		}
		if buf.String() != input {
			t.Errorf("split=%d: relayed %q, want %q", split, buf.String(), input)
		}
		if stats.Events != 1 {
			t.Errorf("split=%d: stats.Events = %d, want 1 — the LF of a split "+
				"CRLF was counted as a second blank line", split, stats.Events)
		}
		if flushes != 1 {
			t.Errorf("split=%d: flushes = %d, want 1 — the client would see a "+
				"spurious event boundary", split, flushes)
		}
	}
}

// TestCopySSELoneCRFollowedByRealLFIsNotACRLF is the discriminating case for
// the CR ambiguity, stated directly rather than only through the property.
//
// The point is that a CR decides the END OF A LINE on its own, and an LF that
// immediately follows it is that same CRLF pair rather than a line of its own.
// The parser cannot see the next byte at the moment it cuts on the CR, so the
// relay has to carry the ambiguity forward (pendingCRLF) rather than resolve
// it eagerly. Resolving it eagerly by treating the arriving LF as a blank line
// invents an event; resolving it by assuming CRLF swallows a real blank line
// when the CR was a terminator to itself.
//
// Note there is no such thing as "a lone CR followed by an LF of its own": a
// line ending in CR and the very next byte being LF is precisely the CRLF
// case. An earlier draft of this test asserted one, and asserted two events
// for "data: x\r\n" — which has a single line and therefore no blank line and
// no event at all.
func TestCopySSELoneCRFollowedByRealLFIsNotACRLF(t *testing.T) {
	cases := []struct {
		name       string
		input      string
		wantEvents int
	}{
		{name: "crlf_is_one_terminator", input: "data: x\r\n\r\n", wantEvents: 1},
		{name: "crlf_without_a_blank_line_dispatches_nothing", input: "data: x\r\n", wantEvents: 0},
		{name: "lone_cr_only", input: "data: x\r", wantEvents: 0},
		{name: "cr_cr_is_a_blank_line", input: "data: x\r\r", wantEvents: 1},
		{name: "lone_cr_blank_then_lf_blank", input: "data: x\r\n\n", wantEvents: 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			want := runFragmented(t, tc.input, len(tc.input)+16)
			if want.events != tc.wantEvents {
				t.Fatalf("single-read baseline reports %d events for %q, want %d",
					want.events, tc.input, tc.wantEvents)
			}
			// The same must hold at every split offset, which is where an
			// eagerly-resolved CR breaks.
			for split := 1; split < len(tc.input); split++ {
				var buf bytes.Buffer
				stats, err := CopySSE(&buf, &splitReader{data: []byte(tc.input), split: split},
					sseRewriter("public-name"), nil, nil, nil)
				if err != nil {
					t.Fatalf("split=%d: CopySSE: %v", split, err)
				}
				if buf.String() != tc.input {
					t.Errorf("split=%d: relayed %q, want %q", split, buf.String(), tc.input)
				}
				if stats.Events != tc.wantEvents {
					t.Errorf("split=%d: stats.Events = %d, want %d", split, stats.Events, tc.wantEvents)
				}
			}
		})
	}
}

// TestCopySSEEventBoundaryCountIsNotADerivedFromReads pins the forbidden
// property from the contract directly: "a frame boundary derived from a read
// boundary".
//
// The stream below has three blank lines and therefore exactly three events,
// read one byte at a time so that every read is also a byte of content. Any
// implementation that treats a Read returning as a dispatch point inflates
// the count toward the byte count.
func TestCopySSEEventBoundaryCountIsNotADerivedFromReads(t *testing.T) {
	const input = "data: a\n\ndata: b\n\ndata: c\n\n"
	var buf bytes.Buffer
	flushes := 0
	stats, err := CopySSE(&buf, byteReader{r: strings.NewReader(input)},
		sseRewriter("public-name"), func() { flushes++ }, nil, nil)
	if err != nil {
		t.Fatalf("CopySSE: %v", err)
	}
	if buf.String() != input {
		t.Errorf("relayed %q, want %q", buf.String(), input)
	}
	if stats.Events != 3 {
		t.Errorf("stats.Events = %d, want 3", stats.Events)
	}
	if flushes != 3 {
		t.Errorf("flushes = %d, want 3", flushes)
	}
	if stats.Bytes != int64(len(input)) {
		t.Errorf("stats.Bytes = %d, want %d", stats.Bytes, len(input))
	}
}

// TestCopySSELimitBreachIsIndependentOfChunking asserts that the SIZE limits
// (INV-SSE-04) are measured on the reassembled line, not on whatever a single
// Read happened to deliver. An over-cap line must be rejected identically
// however it was fragmented, and the offending line must never be forwarded —
// the partial prefix is not relayed either, since a torn line is worse than a
// clean truncation.
func TestCopySSELimitBreachIsIndependentOfChunking(t *testing.T) {
	// A line just over the 1 MiB cap, followed by a well-formed event. The
	// relay must stop at the breach and emit neither the long line nor
	// anything after it.
	oversize := "data: " + strings.Repeat("x", MaxLineBytes) + "\n\n"
	want := runFragmented(t, oversize, len(oversize)+16)
	if !errors.Is(errorFromText(want.errText), ErrSSELineTooLong) && want.errText == "" {
		t.Fatalf("oversized line was not rejected; errText = %q", want.errText)
	}
	if want.out != "" {
		t.Errorf("the offending line reached the client: %d bytes", len(want.out))
	}
	for _, size := range []int{1, 7, 512, 1 << 16} {
		got := runFragmented(t, oversize, size)
		// The identity of the limit error is chunk-invariant; the byte count
		// inside the message is not, and asserting it would be asserting a
		// read boundary. The relay stops the moment the accumulated line
		// crosses the cap, so how far past the cap that happens depends on
		// how the line was fragmented. Only the sentinel is a promise.
		if errorFromText(got.errText) != errorFromText(want.errText) {
			t.Errorf("chunk=%d: error = %q, want the same limit error as the "+
				"single-read pass %q", size, got.errText, want.errText)
		}
		if got.out != want.out {
			t.Errorf("chunk=%d: relayed %d bytes, want %d", size, len(got.out), len(want.out))
		}
	}
}

// errorFromText recovers the sentinel from a recorded error string so the
// comparison above can use errors.Is semantics on a run that has already
// returned. It maps the two limit errors back from the stable prefix the
// production code wraps them with, and returns the SENTINEL itself — a fresh
// fmt.Errorf per call would compare unequal by pointer and make every
// chunking look like it produced a different error.
func errorFromText(text string) error {
	switch {
	case strings.Contains(text, ErrSSELineTooLong.Error()):
		return ErrSSELineTooLong
	case strings.Contains(text, ErrSSEEventTooLarge.Error()):
		return ErrSSEEventTooLarge
	default:
		return errors.New(text)
	}
}

// TestCopySSETrailingLFAfterLoneCRIsGrammarNotChunking pins the CR ambiguity
// and the decision the relay makes about it, so a future reader does not
// rediscover it as a bug and "fix" the parser in the other direction.
//
// A CR is a complete line ending on its own. When the byte after it turns out
// to be an LF, that LF is that same CRLF pair — part of the line already
// written, not a blank line of its own. The relay cannot know at the moment it
// sees the CR, and it must not WAIT to find out: a peer may pause indefinitely
// after a lone CR, and holding that line back would strand an event the client
// is entitled to see. So the line is written immediately and pendingCRLF
// carries the ambiguity forward.
//
// The consequence that looks wrong until it is named: a CR followed by LF is
// one terminator, so it cannot open an event boundary by itself. "data: a\r\n"
// is a single line and dispatches nothing; only a blank line AFTER it does.
// What the relay must never do is let the pairing depend on the read split —
// which is exactly what readToLineEnd did while it asked "is there an LF
// anywhere?" before "where is the first CR?", and why this case has to be
// asserted at every chunk size rather than once.
func TestCopySSETrailingLFAfterLoneCRIsGrammarNotChunking(t *testing.T) {
	cases := []struct {
		name       string
		input      string
		wantEvents int
	}{
		{name: "cr_cr_ends_with_blank_line", input: "data: a\r\r", wantEvents: 1},
		// The CR-LF pair is ONE blank line, so the trailing LF of the pair
		// does not open a second boundary. An earlier draft of this test
		// expected 0 here, recording the behavior readToLineEnd had while it
		// asked about the LF before the CR: the blank "\r\n" was read as the
		// CRLF of the PREVIOUS line, and the blank line that actually ended
		// the frame was never seen.
		{name: "crlf_blank_line_dispatches_once", input: "data: a\r\r\n", wantEvents: 1},
		{name: "crlf_without_a_blank_line_dispatches_nothing", input: "data: a\r\n", wantEvents: 0},
		{name: "single_cr_never_opens_a_boundary", input: "data: a\r", wantEvents: 0},
		{name: "unambiguous_blank_line_still_dispatches", input: "data: a\n\n", wantEvents: 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Every fragmentation must agree — including the ambiguous ones.
			// The grammar-level difference above is between STREAMS, not
			// between chunkings of one stream.
			for _, size := range []int{1, 2, 3, len(tc.input) + 16} {
				var buf bytes.Buffer
				stats, err := CopySSE(&buf, &chunkReader{data: []byte(tc.input), chunk: size},
					sseRewriter("public-name"), nil, nil, nil)
				if err != nil {
					t.Fatalf("chunk=%d: CopySSE: %v", size, err)
				}
				if stats.Events != tc.wantEvents {
					t.Errorf("chunk=%d: stats.Events = %d, want %d", size, stats.Events, tc.wantEvents)
				}
				if buf.String() != tc.input {
					t.Errorf("chunk=%d: relayed %q, want the input unchanged %q", size, buf.String(), tc.input)
				}
			}
		})
	}
}
