package proxy

import (
	"strconv"
	"strings"
	"testing"
)

// The dialect is a substitution, not a rewrite: for every surface that is
// not the Messages route the bytes must be the constants this package has
// always produced, and for the Messages route they must be the Anthropic
// shape — {"type":"error","error":{"type","message"}} — with no param, no
// code and no request id anywhere.

func TestDialectForOpenAISurfacesIsTheExistingConstants(t *testing.T) {
	for _, api := range []string{apiChat, apiResponses, "", "unknown"} {
		env := dialectFor(api)
		want := map[string]string{
			"badMethod":   env.badMethod,
			"authMissing": env.authMissing,
			"authInvalid": env.authInvalid,
			"invalidReq":  env.invalidReq,
			"missingMod":  env.missingMod,
			"tooLarge":    env.tooLarge,
			"atCapacity":  env.atCapacity,
			"upInvalid":   env.upInvalid,
			"upUnreach":   env.upUnreach,
		}
		fixed := map[string]string{
			"badMethod":   envelopeBadMethod,
			"authMissing": envelopeAuthMissing,
			"authInvalid": envelopeAuthInvalid,
			"invalidReq":  envelopeInvalidReq,
			"missingMod":  envelopeMissingMod,
			"tooLarge":    envelopeTooLarge,
			"atCapacity":  envelopeAtCapacity,
			"upInvalid":   envelopeUpInvalid,
			"upUnreach":   envelopeUpUnreach,
		}
		for name, got := range want {
			if got != fixed[name] {
				t.Errorf("dialectFor(%q).%s = %s, want the existing constant %s", api, name, got, fixed[name])
			}
		}
		if env.modelNotFound == nil || env.upstreamErr == nil {
			t.Fatalf("dialectFor(%q) left a builder nil", api)
		}
	}
}

func TestAnthropicDialectExactBytes(t *testing.T) {
	env := dialectFor(apiMessages)
	want := map[string]string{
		"badMethod":   `{"type":"error","error":{"type":"invalid_request_error","message":"method not allowed"}}`,
		"authMissing": `{"type":"error","error":{"type":"authentication_error","message":"you must provide an API key in the Authorization header (Bearer <key>) or the x-api-key header"}}`,
		"authInvalid": `{"type":"error","error":{"type":"authentication_error","message":"invalid API key"}}`,
		"invalidReq":  `{"type":"error","error":{"type":"invalid_request_error","message":"invalid JSON in request body"}}`,
		"missingMod":  `{"type":"error","error":{"type":"invalid_request_error","message":"you must provide a model parameter"}}`,
		"tooLarge":    `{"type":"error","error":{"type":"request_too_large","message":"request body too large"}}`,
		"atCapacity":  `{"type":"error","error":{"type":"api_error","message":"server is out of buffering capacity"}}`,
		"upInvalid":   `{"type":"error","error":{"type":"api_error","message":"upstream returned an invalid response"}}`,
		"upUnreach":   `{"type":"error","error":{"type":"api_error","message":"upstream request failed"}}`,
	}
	got := map[string]string{
		"badMethod":   env.badMethod,
		"authMissing": env.authMissing,
		"authInvalid": env.authInvalid,
		"invalidReq":  env.invalidReq,
		"missingMod":  env.missingMod,
		"tooLarge":    env.tooLarge,
		"atCapacity":  env.atCapacity,
		"upInvalid":   env.upInvalid,
		"upUnreach":   env.upUnreach,
	}
	for name, w := range want {
		if got[name] != w {
			t.Errorf("%s:\n got %s\nwant %s", name, got[name], w)
		}
	}

	// Every Anthropic body is the error envelope shape and carries nothing
	// an Anthropic error object does not define: no "param", no "code", no
	// "request_id". This is the whole dialect in one assertion.
	for name, b := range got {
		if !strings.HasPrefix(b, `{"type":"error","error":{`) || !strings.HasSuffix(b, "}}") {
			t.Errorf("%s is not an Anthropic error envelope: %s", name, b)
		}
		for _, forbidden := range []string{`"param"`, `"code"`, `"request_id"`} {
			if strings.Contains(b, forbidden) {
				t.Errorf("%s carries %s — an Anthropic error object has type and message only: %s", name, forbidden, b)
			}
		}
	}
}

// The 404 interpolates the model name byte-exact, with no HTML escaping —
// both dialects. This is the property the OpenAI arm is already pinned for;
// it must hold under the Anthropic wrapper too.
func TestAnthropicModelNotFoundPreservesNameBytes(t *testing.T) {
	const model = `<script>&model`
	got, err := dialectFor(apiMessages).modelNotFound(model)
	if err != nil {
		t.Fatalf("modelNotFound: %v", err)
	}
	want := `{"type":"error","error":{"type":"not_found_error","message":"The model '` + model +
		`' does not exist or you do not have access to it."}}`
	if string(got) != want {
		t.Fatalf("model_not_found:\n got %s\nwant %s", got, want)
	}
	if strings.Contains(string(got), `\u003c`) || strings.Contains(string(got), `\u0026`) {
		t.Fatalf("model name was HTML-escaped: %s", got)
	}

	// The OpenAI arm is unchanged by the dialect existing at all.
	oa, err := dialectFor(apiChat).modelNotFound(model)
	if err != nil {
		t.Fatalf("openai modelNotFound: %v", err)
	}
	wantOA := `{"error":{"message":"The model '` + model +
		`' does not exist or you do not have access to it.","type":"invalid_request_error","param":null,"code":"model_not_found"}}`
	if string(oa) != wantOA {
		t.Fatalf("openai model_not_found:\n got %s\nwant %s", oa, wantOA)
	}
}

// The upstream status picks the Anthropic type from a closed table, and the
// message carries the status number only — the same static text the OpenAI
// arm writes, never provider bytes.
func TestAnthropicUpstreamErrStatusTypes(t *testing.T) {
	cases := []struct {
		status int
		typ    string
	}{
		{400, "invalid_request_error"},
		{401, "authentication_error"},
		{403, "permission_error"},
		{404, "not_found_error"},
		{413, "request_too_large"},
		{422, "invalid_request_error"},
		{429, "rate_limit_error"},
		{500, "api_error"},
		{502, "api_error"},
		{503, "api_error"},
	}
	for _, tc := range cases {
		got, err := dialectFor(apiMessages).upstreamErr(upstreamErrorEvidence{status: tc.status})
		if err != nil {
			t.Fatalf("status %d: %v", tc.status, err)
		}
		want := `{"type":"error","error":{"type":"` + tc.typ + `","message":"upstream provider returned HTTP ` +
			strconv.Itoa(tc.status) + `"}}`
		if string(got) != want {
			t.Errorf("status %d:\n got %s\nwant %s", tc.status, got, want)
		}
	}
}
