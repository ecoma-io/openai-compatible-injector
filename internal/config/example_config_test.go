package config

import (
	"os"
	"strings"
	"testing"
)

// TestExampleConfigLoads pins the shipped template as a real runtime file:
// config.example.yaml is the document operators copy and edit, so it must
// load. This is not a formatting or substring check — it runs the same
// strict decode and validation every deployment runs, so a field renamed in
// the schema without the example following it, a block that outlives its
// builder, or a recovery policy that stops validating all fail here rather
// than in the operator's first edit.
func TestExampleConfigLoads(t *testing.T) {
	// The test's working directory is this package, so the template sits two
	// levels up at the repository root.
	data, err := os.ReadFile("../../config.example.yaml")
	if err != nil {
		t.Fatalf("read the shipped example config: %v", err)
	}
	snap, err := LoadRuntime(data)
	if err != nil {
		t.Fatalf("the shipped example config is not a valid runtime file: %v", err)
	}
	// A template with no loadable model would pass LoadRuntime's emptiness
	// check only by accident; pin that it demonstrates the mapping it
	// documents, recovery policy included.
	m, ok := snap.Model("gpt-reviewer")
	if !ok {
		t.Fatal("the example no longer defines its documented gpt-reviewer model")
	}
	// The documented auth block is part of the demonstration: the example's
	// opencode provider must keep carrying a loadable two-key credential the
	// loader actually binds to the model's first candidate — the example and
	// the auth schema are locked together, so a schema change without the
	// example following fails here.
	if len(m.Chain) == 0 || m.Chain[0].Cred == nil {
		t.Fatal("the example no longer binds its documented auth block to gpt-reviewer's first candidate")
	}
	cred := m.Chain[0].Cred
	if cred.Spec.Header != "Authorization" || cred.Spec.Prefix != "Bearer " {
		t.Fatalf("example auth surface = %q/%q, want the documented Authorization/Bearer",
			cred.Spec.Header, cred.Spec.Prefix)
	}
	if len(cred.Spec.Keys) < 2 {
		t.Fatalf("example auth carries %d keys, want at least the two the rotation comment documents", len(cred.Spec.Keys))
	}
	// The template must never ship anything that looks like live credential
	// material: every documented key value stays an obvious placeholder.
	for _, k := range cred.Spec.Keys {
		if !strings.HasPrefix(k.Value, "sk-example-") {
			t.Fatalf("example key %s carries non-placeholder material %q", k.ID, redactExampleValue(k.Value))
		}
	}
}

// redactExampleValue keeps a failure message from printing whatever a future
// edit put in the template — the test names the violation, not the value.
func redactExampleValue(string) string { return "(value redacted)" }
