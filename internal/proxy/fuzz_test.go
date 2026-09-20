package proxy

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

// FuzzRewriteSSELine pins the per-line rewrite rule against arbitrary line
// bytes: it never panics; a line that is not a data line — or a data line
// whose payload either never mentions "model" or is not valid JSON — comes
// back byte-identical, terminator included; and when a valid-JSON payload is
// rewritten, the rebuilt line keeps its framing ("data:" prefix, line
// terminator) and the rewritten payload is still valid JSON.
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
		"data:   {\"model\":\"upstream-name\"}\n",   // extra leading spaces
		"data: {\"model\":\"upstream-name\"}\r\n",   // CRLF terminator
		"data: {\"model\":\"upstream-name\"}",       // partial final line, no terminator
		"event: {\"model\":\"upstream-name\"}\n",    // event line is not a data line
		": keep-alive {\"model\":\"x\"}\n",          // comment line
		"retry: 100\n",                              // other SSE field
		"DATA: {\"model\":\"x\"}\n",                 // prefix matching is case-sensitive
		"data",                                      // bare prefix with no colon
		"data: {\"temperature\":0.7}\n",             // JSON without a model key
		"data: \"the \\\"model\\\" key\"\n",         // model text inside a string value
		"data: {\"model\":\"x\",\"n\":1e400}\n",     // float64-overflow number
		"data: {\"model\":\"x\",\"n\":-1.5e-300}\n", // negative numbers
		"data: {\"模型\":\"x\",\"mödél\":\"y\"}\n",    // unicode keys
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
	}
	for _, b := range rawSeeds {
		f.Add(b)
	}

	const public = "public-name"
	f.Fuzz(func(t *testing.T, line []byte) {
		got := rewriteSSELine(line, public)

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
		if !bytes.Contains(payload, sseModelKey) {
			if !bytes.Equal(got, line) {
				t.Fatalf("data line without a \"model\" key mutated:\n in  %q\n out %q", line, got)
			}
			return
		}
		if !json.Valid(payload) {
			if !bytes.Equal(got, line) {
				t.Fatalf("data line with an invalid-JSON payload mutated:\n in  %q\n out %q", line, got)
			}
			return
		}

		// A candidate payload: untouched, or rebuilt without losing the
		// framing and without breaking the JSON.
		if bytes.Equal(got, line) {
			return
		}
		outContent, outTerm := splitSSELineTerminator(got)
		if !bytes.Equal(outTerm, term) {
			t.Fatalf("line terminator changed by the rewrite:\n in  %q\n out %q", line, got)
		}
		outRest, ok := bytes.CutPrefix(outContent, sseDataPrefix)
		if !ok {
			t.Fatalf("data: prefix lost by the rewrite:\n in  %q\n out %q", line, got)
		}
		outPayload := outRest
		if len(outPayload) > 0 && outPayload[0] == ' ' {
			outPayload = outPayload[1:]
		}
		if !json.Valid(outPayload) {
			t.Fatalf("rewrite broke payload JSON validity:\n in  %q\n out %q", line, got)
		}
	})
}
