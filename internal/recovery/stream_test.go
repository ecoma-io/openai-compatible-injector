package recovery

import (
	"testing"
	"time"
)

// streamPolicy builds a policy that differs from the default only in its
// stream block, so a test can ask one question at a time.
func streamPolicy(s StreamPolicy) Policy {
	p := Default()
	p.Stream = s
	return p
}

// TestDefaultStreamRecoveryIsOff: the compatibility story is one assertion.
// A file that never mentions the block resolves to exactly this, and this
// recovers nothing — so a truncated stream stays what it has always been.
func TestDefaultStreamRecoveryIsOff(t *testing.T) {
	st := Default().Stream
	if st.Enabled {
		t.Fatal("the default policy has stream recovery enabled")
	}
	// A disabled policy carries no reach. This is not a formality: a base
	// stating 1 here would make every inherited `enabled: false` a bound the
	// policy cannot use, which Validate rejects.
	if st.MaxRecoveries != 0 {
		t.Fatalf("the default stream policy states %d recoveries while disabled", st.MaxRecoveries)
	}
	// The bounds are still the documented ones, so a layer that states only
	// `enabled: true` inherits a sensible window rather than a zero.
	if st.MaxElapsed != DefaultStreamMaxElapsed {
		t.Fatalf("default stream window = %v, want %v", st.MaxElapsed, DefaultStreamMaxElapsed)
	}
	if st.MaxPartialBytes != DefaultStreamMaxPartialBytes {
		t.Fatalf("default stream partial bound = %d, want %d", st.MaxPartialBytes, DefaultStreamMaxPartialBytes)
	}
	if err := Default().Validate(); err != nil {
		t.Fatalf("the default policy does not validate: %v", err)
	}
}

// TestStreamMergeEnabledAloneGetsTheDefaultReach: the one-line opt-in every
// operator writes must produce a policy that can act. The base carries 0
// because it is the disabled default, so the merge — not the base — supplies
// the reach.
func TestStreamMergeEnabledAloneGetsTheDefaultReach(t *testing.T) {
	got, err := Merge(Default(), Partial{Stream: &StreamPartial{Enabled: boolp(true)}})
	if err != nil {
		t.Fatalf("Merge: %v", err)
	}
	if !got.Stream.Enabled {
		t.Fatal("enabling the block produced a disabled policy")
	}
	if got.Stream.MaxRecoveries != DefaultMaxStreamRecoveries {
		t.Fatalf("recoveries = %d, want the default %d", got.Stream.MaxRecoveries, DefaultMaxStreamRecoveries)
	}
	// The bounds the layer did not state are inherited, not reset.
	if got.Stream.MaxElapsed != DefaultStreamMaxElapsed || got.Stream.MaxPartialBytes != DefaultStreamMaxPartialBytes {
		t.Fatalf("the layer's opt-in reset the bounds: %+v", got.Stream)
	}
}

// TestStreamMergeDisabledForcesZeroReach: switching recovery off IS zero
// recoveries, the same co-rule the walk bound follows when `fallback.enabled`
// goes false. Inheriting a parent's larger reach would leave the policy
// stating a bound it cannot use — a contradiction the operator never wrote,
// which Validate would then reject.
func TestStreamMergeDisabledForcesZeroReach(t *testing.T) {
	base, err := Merge(Default(), Partial{Stream: &StreamPartial{
		Enabled:       boolp(true),
		MaxRecoveries: intp(2),
	}})
	if err != nil {
		t.Fatalf("Merge: %v", err)
	}
	if base.Stream.MaxRecoveries != 2 {
		t.Fatalf("setup: recoveries = %d, want 2", base.Stream.MaxRecoveries)
	}

	got, err := Merge(base, Partial{Stream: &StreamPartial{Enabled: boolp(false)}})
	if err != nil {
		t.Fatalf("Merge: %v", err)
	}
	if got.Stream.Enabled {
		t.Fatal("disabling the block produced an enabled policy")
	}
	if got.Stream.MaxRecoveries != 0 {
		t.Fatalf("recoveries = %d after disabling, want 0", got.Stream.MaxRecoveries)
	}
}

// TestStreamMergeRestatingTheSwitchKeepsTheInheritedReach: the co-rule fills
// in a reach only when there is none to inherit. A model that turns recovery
// on beneath a global block that already configured a reach must inherit that
// reach — re-stating the switch is not a request to narrow it.
func TestStreamMergeRestatingTheSwitchKeepsTheInheritedReach(t *testing.T) {
	base, err := Merge(Default(), Partial{Stream: &StreamPartial{
		Enabled:       boolp(true),
		MaxRecoveries: intp(MaxStreamRecoveriesCap),
	}})
	if err != nil {
		t.Fatalf("Merge: %v", err)
	}
	got, err := Merge(base, Partial{Stream: &StreamPartial{
		Enabled:    boolp(true),
		MaxElapsed: durp(30 * time.Second),
	}})
	if err != nil {
		t.Fatalf("Merge: %v", err)
	}
	if got.Stream.MaxRecoveries != MaxStreamRecoveriesCap {
		t.Fatalf("re-stating the switch reset the reach to %d, want the inherited %d",
			got.Stream.MaxRecoveries, MaxStreamRecoveriesCap)
	}
	if got.Stream.MaxElapsed != 30*time.Second {
		t.Fatalf("window = %v, want the stated 30s", got.Stream.MaxElapsed)
	}
}

// TestStreamMergeStatedScalarsBeatTheCoRule: the co-rule only fills in what
// the layer left unstated. A layer that names both means both.
func TestStreamMergeStatedScalarsBeatTheCoRule(t *testing.T) {
	on, err := Merge(Default(), Partial{Stream: &StreamPartial{
		Enabled:       boolp(true),
		MaxRecoveries: intp(MaxStreamRecoveriesCap),
	}})
	if err != nil {
		t.Fatalf("Merge with stated recoveries: %v", err)
	}
	if on.Stream.MaxRecoveries != MaxStreamRecoveriesCap {
		t.Fatalf("stated recoveries = %d, want the stated %d", on.Stream.MaxRecoveries, MaxStreamRecoveriesCap)
	}

	// A layer that states `enabled: false` beside a positive reach is a
	// contradiction, and the merge says so rather than normalizing it away.
	if _, err := Merge(Default(), Partial{Stream: &StreamPartial{
		Enabled:       boolp(false),
		MaxRecoveries: intp(1),
	}}); err == nil {
		t.Fatal("a disabled policy stating a positive reach was accepted")
	}
}

// TestStreamMergeReplacesEveryScalar: each member of the block is its own
// pointer, so stating one leaves the others alone.
func TestStreamMergeReplacesEveryScalar(t *testing.T) {
	got, err := Merge(Default(), Partial{Stream: &StreamPartial{
		MaxElapsed: durp(3 * time.Second),
	}})
	if err != nil {
		t.Fatalf("Merge: %v", err)
	}
	if got.Stream.MaxElapsed != 3*time.Second {
		t.Fatalf("window = %v, want the stated 3s", got.Stream.MaxElapsed)
	}
	if got.Stream.Enabled || got.Stream.MaxRecoveries != 0 || got.Stream.MaxPartialBytes != DefaultStreamMaxPartialBytes {
		t.Fatalf("stating the window moved another member: %+v", got.Stream)
	}

	got, err = Merge(got, Partial{Stream: &StreamPartial{MaxPartialBytes: intp(64 << 10)}})
	if err != nil {
		t.Fatalf("Merge: %v", err)
	}
	if got.Stream.MaxPartialBytes != 64<<10 {
		t.Fatalf("partial bound = %d, want the stated %d", got.Stream.MaxPartialBytes, 64<<10)
	}
	if got.Stream.MaxElapsed != 3*time.Second {
		t.Fatalf("the second layer reset the window to %v", got.Stream.MaxElapsed)
	}
}

// TestStreamResolvesThroughTheLayerChain: the block rides the same layering as
// every other member — global states the intent, a model turns it off, and the
// candidate inherits the model's answer.
func TestStreamResolvesThroughTheLayerChain(t *testing.T) {
	global := Partial{Stream: &StreamPartial{Enabled: boolp(true)}}
	model := Partial{Stream: &StreamPartial{Enabled: boolp(false)}}

	pol, err := Resolve(Default(),
		Layer{Name: "global", Partial: global},
		Layer{Name: "model", Partial: model},
	)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if pol.Stream.Enabled || pol.Stream.MaxRecoveries != 0 {
		t.Fatalf("the model's opt-out did not win: %+v", pol.Stream)
	}
	if pol.Stream.MaxElapsed != DefaultStreamMaxElapsed {
		t.Fatalf("the model's opt-out reset the window to %v", pol.Stream.MaxElapsed)
	}
}

// TestStreamValidateRejectsOutOfRange: every bound is rejected past its cap
// rather than clamped — a value silently reduced to something else is a policy
// the operator did not write and cannot read back from the file.
func TestStreamValidateRejectsOutOfRange(t *testing.T) {
	cases := []struct {
		name string
		st   StreamPolicy
	}{
		{"recoveries above the cap", StreamPolicy{Enabled: true, MaxRecoveries: MaxStreamRecoveriesCap + 1, MaxElapsed: DefaultStreamMaxElapsed, MaxPartialBytes: DefaultStreamMaxPartialBytes}},
		{"negative recoveries", StreamPolicy{Enabled: false, MaxRecoveries: -1, MaxElapsed: DefaultStreamMaxElapsed, MaxPartialBytes: DefaultStreamMaxPartialBytes}},
		{"enabled with no reach", StreamPolicy{Enabled: true, MaxRecoveries: 0, MaxElapsed: DefaultStreamMaxElapsed, MaxPartialBytes: DefaultStreamMaxPartialBytes}},
		{"disabled with a reach", StreamPolicy{Enabled: false, MaxRecoveries: 1, MaxElapsed: DefaultStreamMaxElapsed, MaxPartialBytes: DefaultStreamMaxPartialBytes}},
		{"zero window", StreamPolicy{Enabled: true, MaxRecoveries: 1, MaxElapsed: 0, MaxPartialBytes: DefaultStreamMaxPartialBytes}},
		{"window above the cap", StreamPolicy{Enabled: true, MaxRecoveries: 1, MaxElapsed: MaxCandidateElapsedCap + time.Second, MaxPartialBytes: DefaultStreamMaxPartialBytes}},
		{"partial below the minimum", StreamPolicy{Enabled: true, MaxRecoveries: 1, MaxElapsed: DefaultStreamMaxElapsed, MaxPartialBytes: MinStreamPartialBytes - 1}},
		{"partial above the cap", StreamPolicy{Enabled: true, MaxRecoveries: 1, MaxElapsed: DefaultStreamMaxElapsed, MaxPartialBytes: MaxStreamPartialBytesCap + 1}},
	}
	for _, tc := range cases {
		if err := streamPolicy(tc.st).Validate(); err == nil {
			t.Errorf("%s: %+v was accepted", tc.name, tc.st)
		}
	}
}

// TestStreamValidateAcceptsItsBounds: the caps are inclusive, so a policy
// written exactly at one is a policy the operator can keep.
func TestStreamValidateAcceptsItsBounds(t *testing.T) {
	cases := []struct {
		name string
		st   StreamPolicy
	}{
		{"the default", Default().Stream},
		{"the minimum partial bound", StreamPolicy{Enabled: true, MaxRecoveries: 1, MaxElapsed: DefaultStreamMaxElapsed, MaxPartialBytes: MinStreamPartialBytes}},
		{"the maximum partial bound", StreamPolicy{Enabled: true, MaxRecoveries: 1, MaxElapsed: DefaultStreamMaxElapsed, MaxPartialBytes: MaxStreamPartialBytesCap}},
		{"the maximum reach", StreamPolicy{Enabled: true, MaxRecoveries: MaxStreamRecoveriesCap, MaxElapsed: MaxCandidateElapsedCap, MaxPartialBytes: DefaultStreamMaxPartialBytes}},
	}
	for _, tc := range cases {
		if err := streamPolicy(tc.st).Validate(); err != nil {
			t.Errorf("%s: %+v was rejected: %v", tc.name, tc.st, err)
		}
	}
}

// TestStreamBlockCannotReachTheWalk: the block is post-commitment policy and
// nothing else. Switching it on must leave every walk-facing member exactly
// where the default put it, or the feature would have silently reshaped the
// retry/fallback behavior the compatibility contract pins.
func TestStreamBlockCannotReachTheWalk(t *testing.T) {
	base := Default()
	on, err := Merge(base, Partial{Stream: &StreamPartial{Enabled: boolp(true)}})
	if err != nil {
		t.Fatalf("Merge: %v", err)
	}
	if on.Retry != base.Retry {
		t.Fatalf("the stream block moved the retry policy: %+v → %+v", base.Retry, on.Retry)
	}
	if on.Fallback != base.Fallback {
		t.Fatalf("the stream block moved the walk bound: %+v → %+v", base.Fallback, on.Fallback)
	}
	if on.Budget != base.Budget {
		t.Fatalf("the stream block moved the exchange envelope: %+v → %+v", base.Budget, on.Budget)
	}
	if on.RetryAfter != base.RetryAfter {
		t.Fatalf("the stream block moved the retry-after policy: %+v → %+v", base.RetryAfter, on.RetryAfter)
	}
	// And the matrix answers a pre-commitment failure the same way under both,
	// over the whole shape space the walk can observe.
	for _, obs := range []Observation{
		{Class: FailureHTTP, HTTPStatus: 429, StatusClass: StatusClass4xx},
		{Class: FailureHTTP, HTTPStatus: 503, StatusClass: StatusClass5xx},
		{Class: FailureHTTP, HTTPStatus: 404, StatusClass: StatusClass4xx},
		{Class: FailureTransport, TransportClass: TransportClassTimeout},
		{Class: FailureCaller, CallerCause: CallerCanceled},
	} {
		withAction, withID := on.Matrix.Match(obs)
		baseAction, baseID := base.Matrix.Match(obs)
		if withAction != baseAction || withID != baseID {
			t.Fatalf("the stream block moved the matrix for %+v: (%v, %q) → (%v, %q)",
				obs, baseAction, baseID, withAction, withID)
		}
	}
}

// TestStreamBlockChangesThePolicyHash: two policies that differ only in the
// stream block are two policies. The worst possible outcome is a deployment
// that recovers and one that truncates reporting the same policy_hash — the
// evidence would say a truncated stream ran under a policy that never
// truncates.
func TestStreamBlockChangesThePolicyHash(t *testing.T) {
	off := Default()
	offHash := off.Hash()

	on, err := Merge(off, Partial{Stream: &StreamPartial{Enabled: boolp(true)}})
	if err != nil {
		t.Fatalf("Merge: %v", err)
	}
	if on.Hash() == offHash {
		t.Fatal("enabling stream recovery left the policy hash unchanged")
	}

	// Both policies are valid and differ in exactly one member; nothing else
	// moved, so the hash difference is the stream block's own.
	if err := on.Validate(); err != nil {
		t.Fatalf("the enabled policy does not validate: %v", err)
	}
	wider, err := Merge(on, Partial{Stream: &StreamPartial{MaxRecoveries: intp(MaxStreamRecoveriesCap)}})
	if err != nil {
		t.Fatalf("Merge: %v", err)
	}
	if wider.Hash() == on.Hash() {
		t.Fatal("a wider recovery reach left the policy hash unchanged")
	}
}

// TestStreamHashTagIsVersioned pins the encoding's version. The four new
// members are hashed even when recovery is disabled, so a build that wrote
// them without bumping the tag would make a v2 policy and a v3 policy with the
// same walk-facing data compare equal — exactly what the tag exists to
// prevent. This test is the tripwire for the NEXT field added to Policy.
func TestStreamHashTagIsVersioned(t *testing.T) {
	if hashTag != "recovery-policy-v3" {
		t.Fatalf("hashTag = %q, want %q — a change to the canonical encoding must bump this tag",
			hashTag, "recovery-policy-v3")
	}
}
