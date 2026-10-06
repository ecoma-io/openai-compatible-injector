package proxy

import (
	"net/http"
	"slices"
	"strings"
	"testing"
)

// The client credential may arrive as either spelling the served dialects
// use. The rules this pins, in the order they matter:
//
//   - either header alone authenticates;
//   - when both appear the bearer wins, so a client that also sends x-api-key
//     keeps authenticating with the credential it always used;
//   - a malformed bearer does not short-circuit the request — the x-api-key
//     is still consulted;
//   - both candidates are gated by the same validBearerToken, so an oversize
//     or ill-formed x-api-key is refused exactly like an oversize bearer.
//
// Header literals use the canonical MIME spelling because http.Header.Get
// canonicalizes its argument: a map keyed "X-API-Key" would never be found by
// a lookup for it. net/http canonicalizes incoming header names the same way,
// so the production path is the same shape as these fixtures.

func TestClientToken(t *testing.T) {
	const bearer = "bearer-abcDEF123-_~+/"
	const xkey = "xkey-abcDEF123-_~+/"
	oversize := strings.Repeat("a", maxBearerTokenBytes+1)

	cases := []struct {
		name    string
		header  http.Header
		wantTok string
		wantOK  bool
	}{
		{
			name:    "bearer only",
			header:  http.Header{"Authorization": {"Bearer " + bearer}},
			wantTok: bearer,
			wantOK:  true,
		},
		{
			name:    "x-api-key only",
			header:  http.Header{"X-Api-Key": {xkey}},
			wantTok: xkey,
			wantOK:  true,
		},
		{
			name: "both — the bearer wins",
			header: http.Header{
				"Authorization": {"Bearer " + bearer},
				"X-Api-Key":     {xkey},
			},
			wantTok: bearer,
			wantOK:  true,
		},
		{
			name: "malformed bearer falls through to a valid x-api-key",
			header: http.Header{
				"Authorization": {"Basic " + bearer},
				"X-Api-Key":     {xkey},
			},
			wantTok: xkey,
			wantOK:  true,
		},
		{
			name: "empty bearer falls through to a valid x-api-key",
			header: http.Header{
				"Authorization": {"Bearer "},
				"X-Api-Key":     {xkey},
			},
			wantTok: xkey,
			wantOK:  true,
		},
		{
			name:    "neither header",
			header:  http.Header{},
			wantTok: "",
			wantOK:  false,
		},
		{
			name:    "both headers empty",
			header:  http.Header{"Authorization": {""}, "X-Api-Key": {""}},
			wantTok: "",
			wantOK:  false,
		},
		{
			name:    "oversize x-api-key rejected",
			header:  http.Header{"X-Api-Key": {oversize}},
			wantTok: "",
			wantOK:  false,
		},
		{
			name:    "ill-formed x-api-key rejected",
			header:  http.Header{"X-Api-Key": {"has a space"}},
			wantTok: "",
			wantOK:  false,
		},
		{
			name:    "scheme is case-insensitive",
			header:  http.Header{"Authorization": {"bEaReR " + bearer}},
			wantTok: bearer,
			wantOK:  true,
		},
		{
			name:    "outer spaces on the bearer token are tolerated",
			header:  http.Header{"Authorization": {"Bearer   " + bearer + "  "}},
			wantTok: bearer,
			wantOK:  true,
		},
		{
			name:    "outer spaces on the x-api-key are tolerated",
			header:  http.Header{"X-Api-Key": {"  " + xkey + "  "}},
			wantTok: xkey,
			wantOK:  true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tok, ok := clientToken(tc.header)
			if ok != tc.wantOK {
				t.Fatalf("clientToken ok = %v, want %v", ok, tc.wantOK)
			}
			if tok != tc.wantTok {
				t.Fatalf("clientToken token = %q, want %q", tok, tc.wantTok)
			}
		})
	}
}

// Neither credential spelling is on the forward allow-list, so neither can
// reach an upstream no matter which one the client used to authenticate.
// This is the property that keeps a client credential out of provider
// traffic; it is asserted here because clientToken is the only code that
// reads either header.
func TestClientTokenHeadersNeverForwarded(t *testing.T) {
	for _, name := range []string{"Authorization", "X-Api-Key"} {
		if slices.Contains(forwardHeaderNames, name) {
			t.Fatalf("forwardHeaderNames lists %q — client credentials must never be forwarded", name)
		}
	}
}
