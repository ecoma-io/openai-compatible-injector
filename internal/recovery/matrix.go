package recovery

import "fmt"

// Matrix is the failure → action table: an ordered rule list plus the
// action any unmatched failure takes.
//
// A matrix is built once, validated, and then read-only. Evaluation is
// first-match-wins over the ordered list, and the order is derived from the
// rules themselves — precedence key descending, identity ascending — so the
// decision for a given failure is a pure function of the policy, never of
// the order an operator happened to type the rules in, of YAML key order, or
// of Go map iteration.
type Matrix struct {
	rules []Rule
	def   Action
}

// NewMatrix validates, orders, and freezes a rule list.
//
// It rejects, in this order, a rule that can never match any failure, a rule
// identity that is missing, malformed, or repeated, a rule whose predicates
// contradict the layer it names, a caller rule that is not terminal, and —
// the one that keeps evaluation unambiguous — two rules of equal precedence
// that can both apply to the same failure. Rejecting is the fail-closed
// direction: an ambiguous policy has no defensible resolution, and silently
// preferring one of two equipollent rows would make the effective policy
// depend on something the operator cannot see.
//
// Errors name the POSITION of the offending rule (1-based, in the order
// given), never its identity: a rule ID is operator input, error text
// reaches logs verbatim, and a botched paste must not be able to smuggle a
// credential into a log line.
func NewMatrix(rules []Rule, def Action) (Matrix, error) {
	out := make([]Rule, len(rules))
	copy(out, rules)
	seen := make(map[string]int, len(out))
	for i := range out {
		if err := ValidateRuleID(out[i].ID); err != nil {
			return Matrix{}, fmt.Errorf("rule %d: %w", i+1, err)
		}
		if prev, dup := seen[out[i].ID]; dup {
			return Matrix{}, fmt.Errorf("rule %d repeats the identity of rule %d", i+1, prev+1)
		}
		seen[out[i].ID] = i
		if err := out[i].Match.validate(); err != nil {
			return Matrix{}, fmt.Errorf("rule %d: %w", i+1, err)
		}
		if err := validateRuleAction(out[i]); err != nil {
			return Matrix{}, fmt.Errorf("rule %d: %w", i+1, err)
		}
	}
	// Equal precedence plus overlapping predicates is the only ambiguity a
	// matrix may not contain; a differing precedence key is a strict order
	// and needs no further check.
	for i := range out {
		for j := i + 1; j < len(out); j++ {
			if out[i].Match.Specificity() != out[j].Match.Specificity() {
				continue
			}
			if out[i].Match.overlaps(out[j].Match) {
				return Matrix{}, fmt.Errorf("rules %d and %d have equal precedence and can both match the same failure", i+1, j+1)
			}
		}
	}
	sortRules(out)
	return Matrix{rules: out, def: def}, nil
}

// validateRuleAction enforces the positions where only one action is
// meaningful. A caller rule is the important one: a client's cancellation is
// terminal by construction, and a configuration that says otherwise is
// rejected rather than quietly ignored.
func validateRuleAction(r Rule) error {
	if r.Match.Class == FailureCaller || r.Match.CallerCause != "" {
		if r.Action != ActionTerminal {
			return errActionNotAllowed("a caller rule", "terminal")
		}
	}
	return nil
}

// validate rejects a predicate set that can never match any observation. A
// rule that cannot fire is a misunderstanding of the schema, not a policy,
// and leaving it in place would let an operator believe a recovery behavior
// is configured when it is not.
func (m Match) validate() error {
	kinds := 0
	if m.TransportCause != "" {
		kinds++
	}
	if m.ProtocolCause != "" {
		kinds++
	}
	if m.CallerCause != "" {
		kinds++
	}
	if kinds > 1 {
		return errActionNotAllowed("a rule constraining more than one cause kind", "a single cause kind")
	}
	if m.Status != 0 && m.StatusClass != StatusClassNone && StatusClassOf(m.Status) != m.StatusClass {
		return errActionNotAllowed("a rule whose status and status class", "consistent with each other")
	}
	if m.Status != 0 && (m.Status < 100 || m.Status > 599) {
		return errActionNotAllowed("a rule's status", "an HTTP status between 100 and 599")
	}
	// Every predicate but the class, the streaming flag and the two indexes
	// belongs to exactly one failure layer. A rule may only carry predicates
	// from one of them: there is no observation with a transport class and an
	// HTTP status. The rule holds whether or not the rule names a class — a
	// rule that names none is held to it by inference, so a predicate pair no
	// observation can ever satisfy is rejected here rather than loading as a
	// rule that silently never fires.
	http := m.Status != 0 || m.StatusClass != StatusClassNone ||
		m.ProviderErrorType != "" || m.ProviderErrorCode != ""
	transport := m.TransportClass != TransportClassNone || m.TransportCause != ""
	protocol := m.ProtocolCause != ""
	caller := m.CallerCause != ""
	layers := 0
	for _, named := range [...]bool{http, transport, protocol, caller} {
		if named {
			layers++
		}
	}
	if layers > 1 {
		return errActionNotAllowed("a rule mixing predicates from more than one failure layer", "predicates from a single layer")
	}
	if m.Class == FailureAny {
		return nil
	}
	if m.Class != FailureHTTP && http {
		return errActionNotAllowed("a rule's failure class and its HTTP predicates", "the same layer")
	}
	if m.Class != FailureTransport && transport {
		return errActionNotAllowed("a rule's failure class and its transport predicates", "the same layer")
	}
	if m.Class != FailureProtocol && protocol {
		return errActionNotAllowed("a rule's failure class and its protocol predicate", "the same layer")
	}
	if m.Class != FailureCaller && caller {
		return errActionNotAllowed("a rule's failure class and its caller predicate", "the same layer")
	}
	return nil
}

// Match returns the action the failing observation takes and the identity of
// the rule that decided it. An observation no rule claims takes the
// matrix's default action under the reserved "default" identity.
func (m Matrix) Match(o Observation) (Action, string) {
	for i := range m.rules {
		if m.rules[i].Match.matches(o) {
			return m.rules[i].Action, m.rules[i].ID
		}
	}
	return m.def, ReservedRuleID
}

// Default is the action unmatched failures take.
func (m Matrix) Default() Action { return m.def }

// Rules returns a copy of the ordered rule list, for inspection and tests.
func (m Matrix) Rules() []Rule {
	out := make([]Rule, len(m.rules))
	copy(out, m.rules)
	return out
}
