package inject

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

// FuzzProbe pins the Probe contract against arbitrary bytes: it never
// panics; on error it reports no routeable fields; and on success the model
// is exactly the top-level "model" string value (empty when that key is
// absent, not a string, or the document is not an object), and the stream
// flag is true exactly when a top-level "stream" carries the literal true
// and false for the literal false — a stream value of any other shape must
// have produced the error, not a success.
func FuzzProbe(f *testing.F) {
	seeds := []string{
		``,                                  // empty body
		`   `,                               // whitespace only
		`{}`,                                // empty object
		`{"model":"gpt-5","stream":true}`,   // the happy path
		`{"model":"gpt-5","stream":false}`,  // stream false
		`{"model":"gpt-5","stream": true }`, // whitespace around the value
		`{"model":""}`,                      // empty model string
		`{"stream":true}`,                   // stream without model
		`{"model":"gpt-5","stream":null}`,   // null stream: error
		`{"model":"gpt-5","stream":"true"}`, // string stream: error
		`{"model":"gpt-5","stream":1}`,      // number stream: error
		`{"model":"gpt-5","stream":-0.5}`,   // negative number stream: error
		`{"model":"gpt-5","stream":[]}`,     // array stream: error
		`{"model":"gpt-5","stream":{}}`,     // object stream: error
		`{"model":123}`,                     // non-string model
		`{"model":null}`,                    // null model
		`{"model":-1}`,                      // negative-number model
		`[{"model":"x"}]`,                   // non-object document
		`"plain string"`,                    // string document
		`42`,                                // number document
		`{"n":1e400,"model":"gpt-5"}`,       // float64-overflow number
		`{"model":"gpt-5","n":-1.5e-300}`,   // negative exponent
		`{"model":`,                         // truncated JSON
		`not json`,                          // garbage
		`{"model":"say \"hi\" \\ done"}`,    // escaped quotes
		`{"mödél":"x","模型":"y","model":"gpt-5"}`,                                          // unicode keys
		`{"model":"a","model":"gpt-5","stream":true,"stream":false}`,                      // duplicate keys
		`{"model":"gpt-5","a":` + strings.Repeat("[", 64) + strings.Repeat("]", 64) + `}`, // deep nesting
		`{"model":"` + strings.Repeat("a", 8192) + `"}`,                                   // huge string value
		"{\n\t\"model\"\t:\n\"gpt-5\",\n\"stream\"\r:\rtrue\r\n}\r\n",                     // whitespace-heavy framing
	}
	for _, s := range seeds {
		f.Add([]byte(s))
	}
	// Byte-level oddities an interpreted string literal carries awkwardly:
	// invalid UTF-8, NUL bytes, and a UTF-8 BOM, which is not valid JSON.
	rawSeeds := [][]byte{
		[]byte("\xff"),
		[]byte("\xff\xfe\x00"),
		[]byte("{\"model\":\"\xff\"}"),
		[]byte("{\"model\":\"a\xffb\",\"stream\":true}"),
		[]byte("{\"mod\xffel\":\"x\"}"),
		[]byte("\xef\xbb\xbf{\"model\":\"gpt-5\"}"),
	}
	for _, b := range rawSeeds {
		f.Add(b)
	}

	f.Fuzz(func(t *testing.T, body []byte) {
		model, stream, err := Probe(body)
		if err != nil {
			if model != "" || stream {
				t.Fatalf("Probe(%q) errored (%v) but still reported model=%q stream=%v", body, err, model, stream)
			}
			return
		}
		if !json.Valid(body) {
			t.Fatalf("Probe reported success for invalid JSON: %q", body)
		}
		var fields map[string]json.RawMessage
		if uerr := json.Unmarshal(body, &fields); uerr != nil {
			// Valid JSON the field map cannot represent: a non-object
			// document carries no routeable fields at all.
			if model != "" || stream {
				t.Fatalf("Probe reported model=%q stream=%v for a document with no field map: %v", model, stream, uerr)
			}
			return
		}

		wantModel := ""
		if raw, ok := fields["model"]; ok {
			var name string
			if json.Unmarshal(raw, &name) == nil {
				wantModel = name
			}
		}
		if model != wantModel {
			t.Fatalf("model = %q, want %q (the top-level \"model\" string value)", model, wantModel)
		}

		raw, ok := fields["stream"]
		if !ok {
			if stream {
				t.Fatalf("stream = true with no top-level \"stream\" key")
			}
			return
		}
		var compacted bytes.Buffer
		if cerr := json.Compact(&compacted, raw); cerr != nil {
			t.Fatalf("compacting a stream value of valid JSON failed: %v", cerr)
		}
		switch compacted.String() {
		case "true":
			if !stream {
				t.Fatalf("top-level \"stream\":true but stream flag = false: %q", body)
			}
		case "false":
			if stream {
				t.Fatalf("top-level \"stream\":false but stream flag = true: %q", body)
			}
		default:
			t.Fatalf("Probe reported success while \"stream\" is %s, not a bool: %q", compacted.String(), body)
		}
	})
}

// FuzzRewriteModel pins RewriteModel's two load-bearing invariants against
// arbitrary bytes: the validity gate is byte-exact — invalid input comes
// back byte-identical — and the rewrite never turns valid JSON into invalid
// JSON. Anything else (which spans are found, what the replacement looks
// like) is the unit table's business; here the gate and validity are the
// contract, because a violated gate corrupts a client's payload in flight.
func FuzzRewriteModel(f *testing.F) {
	seeds := []string{
		``,                               // empty body
		`   `,                            // whitespace only
		`[DONE]`,                         // SSE terminator, not JSON
		`{"model":"gpt-5"}`,              // the happy path
		`{ "model" : "gpt-5" }`,          // whitespace around the key
		`{"response":{"model":"gpt-5"}}`, // response.model scope
		`{"model":"gpt-5","response":{"model":"gpt-5"}}`, // both scopes
		`{"response":{"wrapper":{"model":"deep"}}}`,      // out of scope
		`[{"model":"x"}]`,                                     // array document, out of scope
		`{"model":123}`,                                       // non-string value
		`{"model":null}`,                                      // null value
		`{"model":-1}`,                                        // negative-number value
		`{"model":["x"]}`,                                     // array value
		`{"model":"gpt-5","n":1e400}`,                         // float64-overflow number in scope
		`{"model":"gpt-5","n":-1.5e-300,"m":-0}`,              // negative numbers
		`{"model":"say \"hi\" \u0041"}`,                       // escaped quotes
		`{"a":"the \"model\" key","model":"x"}`,               // quoted text mentioning the key
		`{"mödél":"x","模型":"gpt-5"}`,                          // unicode keys
		`{"mod\u0065l":"x"}`,                                  // escaped key decodes to "model"
		`{"model":"a","model":"gpt-5"}`,                       // duplicate keys
		`{"response":{"model":"x"},"response":{"model":"y"}}`, // duplicate response objects
		`{"model":`,                                           // truncated JSON
		`not json`,                                            // garbage
		`"model"`,                                             // bare string document
		`{"a":` + strings.Repeat("[", 64) + strings.Repeat("]", 64) + `,"model":"gpt-5"}`, // deep nesting
		`{"model":"` + strings.Repeat("x", 8192) + `"}`,                                   // huge string value
		"{\n \"model\" : \"gpt-5\",\n \"stream\": true\n}\n",                              // whitespace-heavy framing
	}
	for _, s := range seeds {
		f.Add([]byte(s))
	}
	rawSeeds := [][]byte{
		[]byte("\xff"),
		[]byte("{\"model\":\xff}"),
		[]byte("{\"model\":\"a\xffb\"}"),
		[]byte("data: {\"model\":\"x\"}"), // an SSE line fed whole, not just the payload
		[]byte("\xef\xbb\xbf{\"model\":\"gpt-5\"}"),
	}
	for _, b := range rawSeeds {
		f.Add(b)
	}

	const public = "public-name"
	f.Fuzz(func(t *testing.T, body []byte) {
		got := RewriteModel(body, public)
		if !json.Valid(body) {
			if !bytes.Equal(got, body) {
				t.Fatalf("invalid input was not returned unchanged:\n in  %q\n out %q", body, got)
			}
			return
		}
		if !json.Valid(got) {
			t.Fatalf("rewrite broke JSON validity:\n in  %q\n out %q", body, got)
		}
		// No in-scope key can exist when the exact key bytes are absent, so
		// the output must be the input, byte for byte.
		if !bytes.Contains(body, []byte(`"model"`)) && !bytes.Equal(got, body) {
			t.Fatalf("no \"model\" key present, input mutated:\n in  %q\n out %q", body, got)
		}
	})
}
