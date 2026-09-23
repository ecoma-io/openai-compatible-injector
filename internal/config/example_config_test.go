package config

import (
	"os"
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
	if _, ok := snap.Model("gpt-reviewer"); !ok {
		t.Fatal("the example no longer defines its documented gpt-reviewer model")
	}
}
