package config

import (
	"os"
	"strings"
	"testing"

	"openai-compatible-injector/internal/recovery"
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
			// The failure names the violation, never the value it found.
			t.Fatalf("example key %s carries non-placeholder material", k.ID)
		}
	}
	// The shipped template must not turn post-commitment stream recovery on.
	// It is the one setting that changes output a client has already
	// received, so the template documents it and leaves it off; an operator
	// opts in deliberately, per deployment or per model.
	for i, cand := range m.Chain {
		if cand.Recovery.Stream.Enabled {
			t.Fatalf("the example enables stream recovery on candidate %d", i+1)
		}
	}
}

// TestPreStreamRecoveryConfigStillLoads is the backward-compatibility pin, in
// the form the risk actually takes. A deployment upgrading into this feature
// runs a file written before the block existed: it states no `stream` key at
// all, and — the second case, which is the one a reviewer is likely to miss —
// it may state a `recovery` block whose members predate the addition. Both
// must load under the strict decoder and resolve to the DISABLED policy, so a
// truncated stream keeps behaving exactly as it did.
//
// The bodies are frozen literals rather than a read of the shipped template:
// the template is expected to gain the new block as documentation, and a test
// that read it could not tell "an old file still loads" from "the new file
// loads".
func TestPreStreamRecoveryConfigStillLoads(t *testing.T) {
	const oldNoRecovery = "api-key: unit-test-key\n" +
		"transports:\n  t1:\n    type: direct\n" +
		"providers:\n  pa:\n    base-url: https://a.example/v1\n    transport: t1\n" +
		"models:\n  m:\n    provider: pa\n    upstream-model: up-a\n"

	const oldWithRecovery = oldNoRecovery +
		"recovery:\n" +
		"  matrix:\n    default: terminal\n" +
		"  retries:\n    max-retries: 2\n" +
		"  fallback:\n    enabled: true\n    max-candidates: 2\n" +
		"  budget:\n    request:\n      max-exchanges: 32\n" +
		"    candidate:\n      max-exchanges: 16\n" +
		"  retry-after:\n    enabled: true\n    mode: max\n    max-delay: 5s\n"

	for _, tc := range []struct {
		name string
		data string
	}{
		{"a file with no recovery block", oldNoRecovery},
		{"a file whose recovery block predates the stream member", oldWithRecovery},
	} {
		t.Run(tc.name, func(t *testing.T) {
			snap, err := LoadRuntime([]byte(tc.data))
			if err != nil {
				t.Fatalf("a pre-existing config stopped loading: %v", err)
			}
			m, ok := snap.Model("m")
			if !ok {
				t.Fatal("model not found")
			}
			st := m.Recovery.Stream
			if st.Enabled || st.MaxRecoveries != 0 {
				t.Fatalf("a pre-existing config resolved to an enabled stream policy: %+v", st)
			}
			// The documented bounds are still materialized, so an operator who
			// later writes only `enabled: true` inherits a sane window rather
			// than a zero — but nothing reads them until they do.
			if st.MaxElapsed != recovery.DefaultStreamMaxElapsed || st.MaxPartialBytes != recovery.DefaultStreamMaxPartialBytes {
				t.Fatalf("unstated stream bounds = %+v, want the documented defaults", st)
			}
		})
	}
}
