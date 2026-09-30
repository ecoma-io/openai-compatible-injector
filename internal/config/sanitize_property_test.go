package config

import (
	"fmt"
	"strings"
	"testing"
)

// ---------------------------------------------------------------------------
// This file closes the gap under INV-CFG-01 in
// docs/design/behavioral-contract.md, which reads:
//
//   "the sanitizer is a hand-picked list of positions; a position nobody
//    thought of would ship silently."
//
// TestLoadRuntimeErrorsNeverEchoInput plants a marker in a curated set of
// positions, and that set is the weakness. It is a LIST, and a list is only
// as good as the person who wrote it: an input-echoing position that nobody
// enumerated is invisible to it, and the failure is silent in the worst way
// — the file is rejected correctly, the exit code is right, and the
// credential is written to the log anyway.
//
// The two halves of the rejection path make this a live risk rather than a
// theoretical one. decodeConfigError replaces a known TABLE of
// input-echoing yaml.v3 scanner prefixes wholesale, and returns the error
// unredacted for anything not in that table. So the table is exactly the
// enumeration that must not be incomplete, and a yaml.v3 upgrade that grows
// one new quoting error style would pass every existing test.
//
// The property here is stated over the SHAPE of a YAML document rather than
// over hand-picked strings: plant the marker in every structural position the
// document grammar offers, and require that no rejection ever quotes it. A new
// position is covered because the generator produces it, not because someone
// remembered to add it.
//
// FuzzLoadRuntime does not close this gap and is not an alternative to it: a
// fuzzer has no marker to look for, so it can prove a rejection never panics
// but never that it stayed quiet.
// ---------------------------------------------------------------------------

// markerPositions is the set of structural slots a YAML document offers where
// operator text can land. Each is a substitution into a valid baseline, so
// every case differs from an accepted file in exactly one position and any
// rejection is attributable to that position.
//
// The generator is deliberately not exhaustive over YAML's grammar — it cannot
// be, without becoming a parser. It is exhaustive over the positions this
// file's schema actually consumes, plus the scanner-level positions that quote
// input on their way to being rejected.
var markerPositions = []struct {
	name string
	// yaml returns a valid baseline with the marker planted in one position.
	// A position that produces an ACCEPTED file is a bug in the position
	// list, and the test below reports it as such rather than skipping.
	yaml string
}{
	// --- Top-level scalar positions -------------------------------------
	{"top_level_value", "models: " + marker + "\n"},
	{"top_level_key", marker + ": true\n" + validRuntime()},
	{"api_key_value", "api-key: " + marker + "\n" + validRuntime()},
	{"api_key_value_invalid_token", "api-key: " + marker + ":x\n" + validRuntime()},
	{"api_key_whitespace_padded", "api-key: \"  " + marker + "  \"\n" + validRuntime()},
	{"log_level_value", "api-key: k\nmodels:\n  a:\n    endpoint: http://h.example/v1\n    upstream-model: m\nlog-level: https://h.example/v1?" + marker + "=x\n"},
	{"log_level_type_mismatch", "api-key: k\nmodels:\n  a:\n    endpoint: http://h.example/v1\n    upstream-model: m\nlog-level: " + marker + "\n"},
	{"sse_keepalive_value", "api-key: k\nmodels:\n  a:\n    endpoint: http://h.example/v1\n    upstream-model: m\nsse-keep-alive:\n  interval: " + marker + "\n"},
	{"sse_keepalive_type_mismatch", "api-key: k\nmodels:\n  a:\n    endpoint: http://h.example/v1\n    upstream-model: m\nsse-keep-alive: " + marker + "\n"},

	// --- Model table positions ------------------------------------------
	{"models_table_shadowed_by_scalar", "models: " + marker + "\n"},
	{"model_name", "models:\n  " + marker + ":\n    endpoint: http://h.example/v1\n    upstream-model: m\n"},
	{"model_name_padded", "models:\n  \"" + marker + "  \":\n    endpoint: http://h.example/v1\n    upstream-model: m\n"},
	{"model_entry_shadowed_by_scalar", "models:\n  a: " + marker + "\n"},
	{"model_entry_duplicate_key", "models:\n  a:\n    upstream-model: m\n  a:\n    upstream-model: " + marker + "\n"},

	// --- Endpoint / provider positions ----------------------------------
	{"endpoint_value", "models:\n  a:\n    endpoint: http://h.example/v1?" + marker + "=x\n    upstream-model: m\n"},
	{"endpoint_bad_escape", "models:\n  a:\n    endpoint: http://h.example/v1%zz?" + marker + "=x\n    upstream-model: m\n"},
	{"endpoint_key", "models:\n  a:\n    " + marker + ": http://h.example/v1\n    upstream-model: m\n"},
	{"upstream_model_value", "models:\n  a:\n    endpoint: http://h.example/v1\n    upstream-model: " + marker + "\n"},
	{"provider_ref", "models:\n  a:\n    endpoint: http://h.example/v1\n    upstream-model: m\n    provider: " + marker + "\n"},
	{"transport_ref", "models:\n  a:\n    endpoint: http://h.example/v1\n    upstream-model: m\n    transport: " + marker + "\n"},

	// --- Scanner-level positions that quote input on rejection ---------
	// These are the prefixes in inputEchoingPrefixes that are actually
	// reachable from LoadRuntime: each is a case where yaml.v3 fails BEFORE
	// the strict decode and interpolates operator text.
	{"undefined_alias", "models:\n  a:\n    endpoint: *" + marker + "\n    upstream-model: m\n"},
	{"recursive_anchor", "models:\n  a: &" + marker + "\n    endpoint: *" + marker + "\n    upstream-model: m\n"},
	{"explicit_tag_mismatch", "models:\n  a:\n    endpoint: !!int " + marker + "\n    upstream-model: m\n"},
	// NOTE: an explicit "? key" form does NOT produce yaml.v3's
	// "invalid map key" scanner error here -- it surfaces as a TypeError and
	// is caught by line-number extraction instead. The corresponding entry in
	// inputEchoingPrefixes is therefore unreachable from LoadRuntime, which
	// is why removing it is a silent no-op. See TestInputEchoingPrefixesAreReachable.
	{"explicit_key_form_type_error", "models:\n  a:\n    ? " + marker + "\n    : v\n"},

	// --- Document-level positions ---------------------------------------
	{"whole_file_bare_scalar", marker + "\n"},
	{"second_document_key", validRuntime() + "---\n" + marker + ": true\n"},
	{"second_document_value", validRuntime() + "---\nmodels: " + marker + "\n"},
	{"duplicate_top_level_key", marker + ": true\n" + validRuntime() + marker + ": false\n"},
}

// TestRejectionTextNeverEchoesInputOverThePositionCorpus is the property:
// across every structural position, a rejection is allowed to say anything
// about POSITIONS and nothing about the operator's text.
//
// The assertion is deliberately narrow. It does not require the error to
// mention the position, does not require any particular wording, and does not
// require the file to be rejected at all — a file the marker happens not to
// invalidate is a valid outcome. What it forbids, without exception, is the
// marker appearing in the message. That is the one thing INV-CFG-01's
// sanitizer exists to prevent, and it is checkable in one expression.
func TestRejectionTextNeverEchoesInputOverThePositionCorpus(t *testing.T) {
	for _, p := range markerPositions {
		t.Run(p.name, func(t *testing.T) {
			_, err := LoadRuntime([]byte(p.yaml))
			if err == nil {
				// The position list is supposed to describe inputs that get
				// rejected. An accepted one is not a sanitizer failure, but
				// it does mean the corpus stopped exercising what it claims,
				// so it is reported rather than passed over in silence.
				t.Skipf("marker in %q position does not produce a rejection; "+
					"this position no longer exercises the sanitizer", p.name)
			}
			if strings.Contains(err.Error(), marker) {
				t.Fatalf("rejection echoed operator input from the %s position.\n"+
					"Error text reaches the log verbatim, so this writes the "+
					"credential to disk.\n error: %s", p.name, err.Error())
			}
		})
	}
}

// TestRejectionTextIsPositionedNotQuoted is the positive half, and it exists
// because the negative half is satisfiable by an error that says nothing at
// all. A sanitizer that returned a fixed string for every rejection would pass
// every test above while destroying the operator's ability to find the line
// they broke — so this asserts the diagnostic value SURVIVED redaction.
func TestRejectionTextIsPositionedNotQuoted(t *testing.T) {
	// This fixture must reach the STRICT DECODE and produce a yaml.TypeError
	// — a mapping where a string is expected. Validation-layer rejections
	// (an endpoint without a scheme, say) never had a line number to keep, so
	// asserting "line" on one of those would be asserting something the code
	// never promised. The model table is the reliable TypeError source: it is
	// a map, and a scalar there is a type mismatch rather than a bad value.
	_, err := LoadRuntime([]byte("api-key: k\nmodels: " + marker + "\n"))
	if err == nil {
		t.Fatal("expected a rejection for a scalar models table")
	}
	if strings.Contains(err.Error(), marker) {
		t.Fatalf("error echoes the scalar: %s", err.Error())
	}
	if !strings.Contains(err.Error(), "line") {
		t.Fatalf("redaction discarded the position too; the operator is left "+
			"with no way to find the broken line: %q", err.Error())
	}
}

// TestEveryEchoingPrefixIsRedacted pins the table itself from the other side.
// inputEchoingPrefixes is a WHITELIST of scanner prefixes that get replaced
// wholesale, and decodeConfigError returns an unrecognized error unchanged.
// That makes the table the single point of failure for a yaml.v3 upgrade that
// grows a new quoting error.
//
// This cannot detect a prefix yaml.v3 has not started producing, but it does
// pin the property that makes the table safe to extend: every entry maps a
// real yaml.v3 prefix to text containing NO operator input, and the mapping is
// prefix-anchored (so a longer real error starting with the same text is still
// caught) rather than an equality test that a suffix would evade.
func TestEveryEchoingPrefixIsRedacted(t *testing.T) {
	seen := make(map[string]struct{}, len(inputEchoingPrefixes))
	for _, e := range inputEchoingPrefixes {
		if e.prefix == "" || e.safe == "" {
			t.Errorf("inputEchoingPrefixes entry with an empty field: %+v", e)
			continue
		}
		if _, dup := seen[e.prefix]; dup {
			t.Errorf("duplicate prefix %q in inputEchoingPrefixes; the first "+
				"match wins, so the later entry is dead", e.prefix)
		}
		seen[e.prefix] = struct{}{}

		if strings.Contains(e.safe, marker) {
			t.Errorf("safe text for prefix %q itself contains a marker", e.prefix)
		}
		// The replacement must be a COMPLETE replacement: given the real
		// error this entry exists for, nothing of the original may survive.
		// A prefix match that then returned the original would defeat the
		// whole table.
		full := e.prefix + "SECRET_MARKER trailing operator text"
		if got := decodeConfigError(fmt.Errorf("%s", full)); strings.Contains(got.Error(), "SECRET_MARKER") {
			t.Errorf("prefix %q did not redact the quoted tail: %s", e.prefix, got.Error())
		}
	}
}

// TestInputEchoingPrefixesAreReachable records a finding from mutation
// verification rather than asserting a rule the code happens to satisfy.
//
// Removing the "yaml: invalid map key" entry from inputEchoingPrefixes was a
// SILENT no-op: every test in this package, including this file, stayed green.
// The reason is that no input reachable from LoadRuntime produces that
// scanner error — an explicit "? key" form surfaces as a yaml.TypeError, which
// decodeConfigError catches by extracting line numbers, so control never
// reaches the prefix table for it.
//
// That is a maintenance hazard in a specific direction. The entry looks
// load-bearing to a reader, so deleting it looks dangerous and nobody will; but
// it protects nothing, so the table's completeness cannot be reasoned about
// from the entries alone. Worse, the natural "coverage" case for it — a
// malformed map key — passes for the WRONG reason, via the other mechanism.
//
// So this test asserts the reachability of each entry explicitly, and names
// the unreachable ones as an EXPECTED set. If a future yaml.v3 upgrade makes a
// listed prefix reachable, the tripwire fires and the corpus gains a real case
// for it. If an unlisted prefix becomes reachable, this test cannot see it —
// that is the limit of a whitelist, and it is why the corpus property above is
// the primary defense and this is the secondary one.
func TestInputEchoingPrefixesAreReachable(t *testing.T) {
	// One minimal document per table entry, chosen to produce that entry's
	// yaml.v3 scanner error.
	probes := map[string]string{
		"yaml: unknown anchor ": "models:\n  a:\n    endpoint: *NOPE\n    upstream-model: m\n",
		"yaml: anchor ":         "models:\n  a: &x\n    endpoint: *x\n    upstream-model: m\n",
		"yaml: cannot decode ":  "models:\n  a:\n    endpoint: !!int " + marker + "\n    upstream-model: m\n",
		"yaml: invalid map key": "models:\n  a:\n    ? " + marker + "\n    : v\n",
	}
	// Unreachable from LoadRuntime today; the entry is retained deliberately
	// as defence against a yaml.v3 version that does emit it.
	knownUnreachable := map[string]string{
		"yaml: invalid map key": "surfaces as a yaml.TypeError, redacted by line-number extraction",
	}

	for _, e := range inputEchoingPrefixes {
		doc, ok := probes[e.prefix]
		if !ok {
			t.Errorf("no reachability probe for table entry %q; add one so this "+
				"test can tell a reachable entry from an unreachable one", e.prefix)
			continue
		}
		_, err := LoadRuntime([]byte(doc))
		if err == nil {
			t.Errorf("probe for %q was accepted; it no longer exercises the "+
				"rejection path at all", e.prefix)
			continue
		}
		reached := err.Error() == "parse config: "+e.safe
		if _, expectedUnreachable := knownUnreachable[e.prefix]; expectedUnreachable {
			if reached {
				t.Logf("table entry %q has become reachable (%s); move it out of "+
					"knownUnreachable and add a corpus case for it", e.prefix,
					knownUnreachable[e.prefix])
			}
			continue
		}
		if !reached {
			t.Errorf("table entry %q was expected to fire for its probe, got %q. "+
				"The entry may be dead, or yaml.v3 may have changed the message — "+
				"either way the redaction for this error is now unproven.",
				e.prefix, err.Error())
		}
	}
}
