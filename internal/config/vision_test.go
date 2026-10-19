package config

import (
	"strings"
	"testing"
)

// visionModelBody builds a model entry over the chainRuntime harness with
// the given candidate bodies (each "- ... candidates" YAML lines).
func visionModelBody(cands string) string {
	return "    providers:\n" + cands
}

// TestLoadRuntimeVisionPerCandidate pins the declaration shape: vision rides
// the chain candidate, resolves per candidate, and the aggregate flag
// reports whether ANY candidate declared it. Absent (the default) means
// false — the reading every pre-key file keeps.
func TestLoadRuntimeVisionPerCandidate(t *testing.T) {
	cases := []struct {
		name    string
		cands   string
		want    []bool
		anyFlag bool
	}{
		{
			name: "absent on both",
			cands: "      - provider: pa\n        upstream-model: up-a\n" +
				"      - provider: pb\n        upstream-model: up-b\n",
			want:    []bool{false, false},
			anyFlag: false,
		},
		{
			name: "first declared true",
			cands: "      - provider: pa\n        upstream-model: up-a\n        vision: true\n" +
				"      - provider: pb\n        upstream-model: up-b\n",
			want:    []bool{true, false},
			anyFlag: true,
		},
		{
			name: "second declared true",
			cands: "      - provider: pa\n        upstream-model: up-a\n" +
				"      - provider: pb\n        upstream-model: up-b\n        vision: true\n",
			want:    []bool{false, true},
			anyFlag: true,
		},
		{
			name: "explicit false",
			cands: "      - provider: pa\n        upstream-model: up-a\n        vision: false\n" +
				"      - provider: pb\n        upstream-model: up-b\n",
			want:    []bool{false, false},
			anyFlag: false,
		},
	}
	for _, tc := range cases {
		snap, err := LoadRuntime([]byte(chainRuntime(visionModelBody(tc.cands), "")))
		if err != nil {
			t.Fatalf("%s: LoadRuntime: %v", tc.name, err)
		}
		m, ok := snap.Model("m")
		if !ok {
			t.Fatalf("%s: model missing", tc.name)
		}
		for i, want := range tc.want {
			if m.Chain[i].Vision != want {
				t.Errorf("%s: chain[%d].Vision = %v, want %v (chain: %s)",
					tc.name, i, m.Chain[i].Vision, want, tc.cands)
			}
		}
		if m.HasVisionCandidate != tc.anyFlag {
			t.Errorf("%s: HasVisionCandidate = %v, want %v", tc.name, m.HasVisionCandidate, tc.anyFlag)
		}
	}
}

// TestLoadRuntimeVisionModelLevelFallsBackToEveryCandidate pins the
// model-level spelling on a chain: it is the fallback every candidate
// without its own declaration inherits, and a candidate that declares its
// own wins.
func TestLoadRuntimeVisionModelLevelFallsBackToEveryCandidate(t *testing.T) {
	// Model-level true + candidate-level false on the second: true, false.
	body := "    vision: true\n" +
		"    providers:\n" +
		"      - provider: pa\n        upstream-model: up-a\n" +
		"      - provider: pb\n        upstream-model: up-b\n        vision: false\n"
	snap, err := LoadRuntime([]byte(chainRuntime(body, "")))
	if err != nil {
		t.Fatalf("LoadRuntime: %v", err)
	}
	m, _ := snap.Model("m")
	if !m.Chain[0].Vision || m.Chain[1].Vision {
		t.Errorf("chain vision = [%v, %v], want [true, false]", m.Chain[0].Vision, m.Chain[1].Vision)
	}
	if !m.HasVisionCandidate {
		t.Errorf("HasVisionCandidate = false, want true")
	}

	// Model-level false + candidate-level true: false, true.
	body = "    vision: false\n" +
		"    providers:\n" +
		"      - provider: pa\n        upstream-model: up-a\n" +
		"      - provider: pb\n        upstream-model: up-b\n        vision: true\n"
	snap, err = LoadRuntime([]byte(chainRuntime(body, "")))
	if err != nil {
		t.Fatalf("LoadRuntime: %v", err)
	}
	m, _ = snap.Model("m")
	if m.Chain[0].Vision || !m.Chain[1].Vision {
		t.Errorf("chain vision = [%v, %v], want [false, true]", m.Chain[0].Vision, m.Chain[1].Vision)
	}
	if !m.HasVisionCandidate {
		t.Errorf("HasVisionCandidate = false, want true")
	}
}

// TestLoadRuntimeVisionLegacyForm pins the provider/endpoint forms: the
// model-level spelling is the only one there, and it applies to the one
// candidate the form builds.
func TestLoadRuntimeVisionLegacyForm(t *testing.T) {
	snap, err := LoadRuntime([]byte(chainRuntime(
		"    provider: pa\n    upstream-model: up-a\n    vision: true\n", "")))
	if err != nil {
		t.Fatalf("provider form with vision rejected: %v", err)
	}
	m, _ := snap.Model("m")
	if len(m.Chain) != 1 || !m.Chain[0].Vision || !m.HasVisionCandidate {
		t.Fatalf("provider form: chain = %+v anyVision = %v, want one vision candidate", m.Chain, m.HasVisionCandidate)
	}

	snap, err = LoadRuntime([]byte(
		"api-key: unit-test-key\nmodels:\n  m:\n    endpoint: https://inline.example/v1\n    upstream-model: up-i\n    vision: true\n"))
	if err != nil {
		t.Fatalf("inline form with vision rejected: %v", err)
	}
	m, _ = snap.Model("m")
	if len(m.Chain) != 1 || !m.Chain[0].Vision || !m.HasVisionCandidate {
		t.Fatalf("inline form: chain = %+v anyVision = %v, want one vision candidate", m.Chain, m.HasVisionCandidate)
	}

	// The legacy form without vision stays text-only — the pre-key reading.
	snap, err = LoadRuntime([]byte(chainRuntime(
		"    provider: pa\n    upstream-model: up-a\n", "")))
	if err != nil {
		t.Fatalf("provider form rejected: %v", err)
	}
	m, _ = snap.Model("m")
	if m.Chain[0].Vision || m.HasVisionCandidate {
		t.Errorf("no-declaration form: vision = %v / %v, want false / false", m.Chain[0].Vision, m.HasVisionCandidate)
	}
}

// TestLoadRuntimeVisionUnknownValueRejected pins the invalid-value
// rejection: a vision value that is not a boolean rejects the whole file
// rather than silently reading as false — a silently-read typo would route
// image requests to a model that cannot read them.
func TestLoadRuntimeVisionUnknownValueRejected(t *testing.T) {
	cases := []struct {
		name      string
		modelBody string
	}{
		{"candidate non-bool", "    providers:\n      - provider: pa\n        upstream-model: up-a\n        vision: yes-please\n"},
		{"model non-bool", "    vision: [true]\n    provider: pa\n    upstream-model: up-a\n"},
	}
	for _, tc := range cases {
		_, err := LoadRuntime([]byte(chainRuntime(tc.modelBody, "")))
		if err == nil {
			t.Errorf("%s: accepted", tc.name)
		} else if strings.Contains(err.Error(), "yes-please") || strings.Contains(err.Error(), "[true]") {
			// A scalar that reached error text would violate the no-echo rule.
			t.Errorf("%s: rejection echoes operator input: %v", tc.name, err)
		}
	}
}

// TestLoadRuntimeVisionNoVisionBackwardCompat mirrors the sibling test for
// pre-stream-recovery config: a file with no vision key anywhere loads, and
// every model in it stays text-only exactly as before the key existed.
func TestLoadRuntimeVisionNoVisionBackwardCompat(t *testing.T) {
	body := "    injection-prompt: stay honest\n" +
		"    providers:\n" +
		"      - provider: pa\n        upstream-model: up-a\n" +
		"      - provider: pb\n        upstream-model: up-b\n"
	snap, err := LoadRuntime([]byte(chainRuntime(body, "")))
	if err != nil {
		t.Fatalf("pre-vision file rejected: %v", err)
	}
	m, _ := snap.Model("m")
	if m.HasVisionCandidate {
		t.Errorf("HasVisionCandidate = true, want false for a pre-vision file")
	}
	for i, cand := range m.Chain {
		if cand.Vision {
			t.Errorf("chain[%d].Vision = true, want false", i)
		}
	}
}
