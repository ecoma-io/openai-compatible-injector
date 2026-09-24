package proxy

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"openai-compatible-injector/internal/inject"
)

// FuzzRewriteSSELine pins the per-line rewrite rule against arbitrary line
// bytes: it never panics; a line that is not a data line — or a data line
// whose payload never mentions "model" or "usage" — comes back
// byte-identical, terminator included; and when a valid-JSON payload is
// rewritten, the rebuilt line keeps its framing ("data:" prefix, line
// terminator) and the rewritten payload is still valid JSON. The rewriter
// under test is the handler's composed closure (model rewrite plus, under
// an active plan, the thinking-usage synthesis), so both transforms' line
// invariants are pinned in one pass; an inactive-plan variant asserts the
// whole pipeline stays byte-identical whatever the bytes.
func FuzzRewriteSSELine(f *testing.F) {
	seeds := []string{
		``,                                      // empty line
		` `,                                     // whitespace only
		"\n",                                    // blank event boundary
		"\r\n",                                  // CRLF event boundary
		"data:",                                 // data prefix, no payload
		"data: \n",                              // separator space, empty payload
		"data: [DONE]\n",                        // terminator passthrough
		"data: hello world\n",                   // non-JSON payload
		"data: {truncated\n",                    // truncated JSON payload
		"data: {\"model\": \"unterminated\n",    // unterminated string
		"data: {\"model\":\"upstream-name\"}\n", // the happy path, LF
		"data:{\"model\":\"upstream-name\"}\n",  // no separator space
		"data:   {\"model\":\"upstream-name\"}\n",                                                   // extra leading spaces
		"data: {\"model\":\"upstream-name\"}\r\n",                                                   // CRLF terminator
		"data: {\"model\":\"upstream-name\"}",                                                       // partial final line, no terminator
		"event: {\"model\":\"upstream-name\"}\n",                                                    // event line is not a data line
		": keep-alive {\"model\":\"x\"}\n",                                                          // comment line
		"retry: 100\n",                                                                              // other SSE field
		"DATA: {\"model\":\"x\"}\n",                                                                 // prefix matching is case-sensitive
		"data",                                                                                      // bare prefix with no colon
		"data: {\"temperature\":0.7}\n",                                                             // JSON without a model key
		"data: \"the \\\"model\\\" key\"\n",                                                         // model text inside a string value
		"data: {\"model\":\"x\",\"n\":1e400}\n",                                                     // float64-overflow number
		"data: {\"model\":\"x\",\"n\":-1.5e-300}\n",                                                 // negative numbers
		"data: {\"模型\":\"x\",\"mödél\":\"y\"}\n",                                                    // unicode keys
		"data: {\"usage\":{\"completion_tokens\":100}}\n",                                           // usage-only, the widened gate
		"data: {\"usage\":{\"output_tokens\":100,\"output_tokens_details\":{}}}\n",                  // responses shape, empty details
		"data: {\"usage\":{\"completion_tokens\":100,\"reasoning_tokens\":40}}\n",                   // reported reasoning wins
		"data: {\"response\":{\"usage\":{\"output_tokens\":1e2}}}\n",                                // responses descent scope
		"data: \"the \\\"usage\\\" key\"\n",                                                         // usage text inside a string value
		"data: {\"usage\":{\"completion_tokens\":\"many\"}}\n",                                      // non-numeric completion
		"data: {\"model\":\"x\",\"a\":" + strings.Repeat("[", 64) + strings.Repeat("]", 64) + "}\n", // deep nesting
		"data: {\"model\":\"" + strings.Repeat("x", 8192) + "\"}\n",                                 // huge string value
	}
	for _, s := range seeds {
		f.Add([]byte(s))
	}
	rawSeeds := [][]byte{
		[]byte("data: {\"model\":\"\xff\"}\n"),
		[]byte("data: {\"model\":\"a\xffb\"}\n"),
		[]byte("data\xff: {\"model\":\"x\"}\n"),
		[]byte("data: \xff\n"),
		[]byte("\r"),
		[]byte("data: {\"model\":\"x\"}\r"),
		[]byte("data: {\"usage\":{\"completion_tokens\":\xff}}\n"),
	}
	for _, b := range rawSeeds {
		f.Add(b)
	}

	const public = "public-name"
	active := inject.ThinkingPlan{Active: true, Share: 0.75}
	composed := func(p []byte) []byte {
		out := inject.RewriteChatModel(p, public)
		return inject.SynthesizeChatThinkingUsage(out, active)
	}
	f.Fuzz(func(t *testing.T, line []byte) {
		// Inactive plan: the composed rewriter degenerates to the model
		// rewrite, byte for byte, whatever the line.
		inactiveOut := rewriteSSELine(line, sseRewriter(public), nil)
		got := rewriteSSELine(line, composed, nil)

		content, term := splitSSELineTerminator(line)
		rest, isData := bytes.CutPrefix(content, sseDataPrefix)
		if !isData {
			if !bytes.Equal(got, line) || !bytes.Equal(inactiveOut, line) {
				t.Fatalf("non-data line mutated:\n in  %q\n out %q", line, got)
			}
			return
		}
		payload := rest
		if len(payload) > 0 && payload[0] == ' ' {
			payload = payload[1:]
		}
		if !bytes.Contains(payload, sseModelKey) && !bytes.Contains(payload, sseUsageKey) {
			if !bytes.Equal(got, line) || !bytes.Equal(inactiveOut, line) {
				t.Fatalf("data line without a \"model\" or \"usage\" key mutated:\n in  %q\n out %q", line, got)
			}
			return
		}
		if !json.Valid(payload) {
			if !bytes.Equal(got, line) || !bytes.Equal(inactiveOut, line) {
				t.Fatalf("data line with an invalid-JSON payload mutated:\n in  %q\n out %q", line, got)
			}
			return
		}

		// A candidate payload: untouched, or rebuilt without losing the
		// framing and without breaking the JSON. The inactive variant's own
		// output obeys the same framing rule and stays valid JSON.
		for name, out := range map[string][]byte{"active": got, "inactive": inactiveOut} {
			checkCandidate(t, name, line, out, term)
		}
	})
}

// checkCandidate asserts one rewritten candidate line: either byte-identical
// to the input, or rebuilt with the original "data:" framing, the original
// line terminator, and a still-valid JSON payload.
func checkCandidate(t *testing.T, name string, line, got, term []byte) {
	t.Helper()
	if bytes.Equal(got, line) {
		return
	}
	outContent, outTerm := splitSSELineTerminator(got)
	if !bytes.Equal(outTerm, term) {
		t.Fatalf("%s: line terminator changed by the rewrite:\n in  %q\n out %q", name, line, got)
	}
	outRest, ok := bytes.CutPrefix(outContent, sseDataPrefix)
	if !ok {
		t.Fatalf("%s: data: prefix lost by the rewrite:\n in  %q\n out %q", name, line, got)
	}
	outPayload := outRest
	if len(outPayload) > 0 && outPayload[0] == ' ' {
		outPayload = outPayload[1:]
	}
	if !json.Valid(outPayload) {
		t.Fatalf("%s: rewrite broke payload JSON validity:\n in  %q\n out %q", name, line, got)
	}
}

// FuzzStripSSELine pins the strip rewriter's line invariants against
// arbitrary line bytes: it never panics; a line that is not a data line, or
// that mentions neither a "model"/"usage" key nor any strip first-segment
// key, comes back byte-identical (nil stripKeys included); and any rebuilt
// line keeps its framing and a valid-JSON payload. The rewriter under test
// is the composed handler closure with the strip last, so model-rewrite and
// strip share one line pass.
func FuzzStripSSELine(f *testing.F) {
	seeds := []string{
		``,                                   // empty line
		"\n",                                 // blank event boundary
		"data:",                              // data prefix, no payload
		"data: [DONE]\n",                     // terminator passthrough
		"data: hello world\n",                // non-JSON payload
		"data: {truncated\n",                 // truncated JSON payload
		"data: {\"service_tier\":\"std\"}\n", // the strip-only gate
		"data: {\"provider\":\"kilo\"}\n",    // the strip-only gate, another key
		"data: {\"service_tier\":\"std\",\"choices\":[]}\n",         // strip + no model key
		"data: {\"model\":\"x\",\"provider\":\"kilo\"}\n",           // model and strip together
		"data: {\"a.b\":{\"c\":1},\"service_tier\":\"s\"}\n",        // quoted-dot key
		"data: {\"response\":{\"service_tier\":\"std\"}}\n",         // responses descent with strip
		"data: \"the \\\"service_tier\\\" key\"\n",                  // strip key inside a string value
		"data: {\"content\":\"the service_tier said\"}\n",           // unquoted text decoy
		"data: {\"model\":\"" + strings.Repeat("x", 8192) + "\"}\n", // huge string value
	}
	for _, s := range seeds {
		f.Add([]byte(s))
	}
	rawSeeds := [][]byte{
		[]byte("data: {\"service_tier\":\xff}\n"),
		[]byte("data: {\"provider\":\"a\xffb\"}\n"),
		[]byte("data\xff: {\"model\":\"x\"}\n"),
		[]byte("data: \xff\n"),
		[]byte("data: {\"provider\":\"x\"}\r"),
	}
	for _, b := range rawSeeds {
		f.Add(b)
	}

	const public = "public-name"
	stripKeys := []byte(`"service_tier"`)
	composed := func(p []byte) []byte {
		out := inject.RewriteChatModel(p, public)
		return inject.StripChatFields(out, [][]string{{"service_tier"}})
	}
	f.Fuzz(func(t *testing.T, line []byte) {
		got := rewriteSSELine(line, composed, [][]byte{stripKeys})

		content, term := splitSSELineTerminator(line)
		rest, isData := bytes.CutPrefix(content, sseDataPrefix)
		if !isData {
			if !bytes.Equal(got, line) {
				t.Fatalf("non-data line mutated:\n in  %q\n out %q", line, got)
			}
			return
		}
		payload := rest
		if len(payload) > 0 && payload[0] == ' ' {
			payload = payload[1:]
		}
		if !bytes.Contains(payload, sseModelKey) && !bytes.Contains(payload, sseUsageKey) && !bytes.Contains(payload, stripKeys) {
			if !bytes.Equal(got, line) {
				t.Fatalf("data line without a \"model\", \"usage\", or strip key mutated:\n in  %q\n out %q", line, got)
			}
			return
		}
		if !json.Valid(payload) {
			if !bytes.Equal(got, line) {
				t.Fatalf("data line with an invalid-JSON payload mutated:\n in  %q\n out %q", line, got)
			}
			return
		}
		checkCandidate(t, "strip", line, got, term)
	})
}
