package recovery

import (
	"errors"
	"sort"
	"strconv"
	"strings"
)

// Match is an allow-listed set of typed predicates over one Observation. It
// is the ONLY way a policy can describe a failure, and the reason this
// package needs no expression language: every field below is a bounded,
// validated value — a status, a closed-set token, a bucket — never a
// pattern, a regular expression, or a body excerpt.
//
// The zero value constrains nothing and therefore matches every
// observation. Fields that would otherwise be ambiguous at zero (a retry
// index of 0 is a real, meaningful value) are pointers, so "unset" can never
// be confused with "set to the zero value".
type Match struct {
	// Class restricts the failure layer. FailureAny (the zero value) accepts
	// any layer.
	Class FailureClass
	// Status is an exact HTTP status (408, 429, 503); 0 accepts any.
	Status int
	// StatusClass restricts to a bucket (4xx, 5xx).
	StatusClass StatusClass
	// TransportClass and TransportCause restrict a transport failure; the
	// class is the coarse bucket (connection, timeout, ...), the cause the
	// fine token (tls, connection_refused, ...).
	TransportClass TransportClass
	TransportCause string
	// ProtocolCause restricts a protocol failure (invalid_response, ...).
	ProtocolCause string
	// CallerCause restricts a caller failure. Rules carrying one are
	// validated as terminal-only: the caller's own cancellation is a
	// hard-stop the matrix may describe but never weaken.
	CallerCause string
	// ProviderErrorType and ProviderErrorCode match the upstream error
	// object's own type/code members. They are the only provider-supplied
	// predicate inputs, and both have already passed the evidence parser's
	// printable-token gate.
	ProviderErrorType string
	ProviderErrorCode string
	// Streaming restricts to streamed (or buffered) client requests.
	Streaming *bool
	// CandidateIndex restricts to one chain position (1-based); 0 accepts
	// any.
	CandidateIndex int
	// RetryIndex restricts to one attempt number on the candidate (0 = the
	// initial attempt); nil accepts any.
	RetryIndex *int
}

// matches reports whether an observation satisfies every predicate the match
// states.
func (m Match) matches(o Observation) bool {
	if m.Class != FailureAny && m.Class != o.Class {
		return false
	}
	if m.Status != 0 && m.Status != o.HTTPStatus {
		return false
	}
	if m.StatusClass != StatusClassNone && m.StatusClass != o.StatusClass {
		return false
	}
	if m.TransportClass != TransportClassNone && m.TransportClass != o.TransportClass {
		return false
	}
	if m.TransportCause != "" && m.TransportCause != o.TransportCause {
		return false
	}
	if m.ProtocolCause != "" && m.ProtocolCause != o.ProtocolCause {
		return false
	}
	if m.CallerCause != "" && m.CallerCause != o.CallerCause {
		return false
	}
	if m.ProviderErrorType != "" && m.ProviderErrorType != o.ProviderErrorType {
		return false
	}
	if m.ProviderErrorCode != "" && m.ProviderErrorCode != o.ProviderErrorCode {
		return false
	}
	if m.Streaming != nil && *m.Streaming != o.Streaming {
		return false
	}
	if m.CandidateIndex != 0 && m.CandidateIndex != o.CandidateIndex {
		return false
	}
	if m.RetryIndex != nil && *m.RetryIndex != o.RetryIndex {
		return false
	}
	return true
}

// Specificity is the precedence key. It is a bit vector in which the most
// significant bit is the exact-status predicate and the least significant the
// failure-class predicate, ordered exactly as the documented precedence:
//
//	exact status > provider-error predicate > status class > failure cause > failure class
//
// Two rules of the SAME key can match the same observation, so they are
// ambiguous and are rejected when the matrix is built. Two rules of
// DIFFERENT keys are strictly ordered, so a more specific rule always wins —
// no configuration order, no Go map iteration, and no YAML key order can
// change which rule fires.
func (m Match) Specificity() int {
	k := 0
	if m.Status != 0 {
		k |= 1 << 4
	}
	if m.ProviderErrorType != "" || m.ProviderErrorCode != "" {
		k |= 1 << 3
	}
	if m.StatusClass != StatusClassNone {
		k |= 1 << 2
	}
	if m.TransportCause != "" || m.ProtocolCause != "" || m.CallerCause != "" {
		k |= 1 << 1
	}
	if m.Class != FailureAny || m.TransportClass != TransportClassNone {
		k |= 1
	}
	return k
}

// classSet is the set of failure classes this match can possibly apply to,
// as a bit vector over the FailureClass constants. It is what makes
// "an HTTP-only rule and a transport-only rule never conflict" derivable
// rather than assumed.
func (m Match) classSet() uint8 {
	if m.Class != FailureAny {
		return 1 << uint(m.Class)
	}
	var s uint8
	if m.Status != 0 || m.StatusClass != StatusClassNone {
		s |= 1 << uint(FailureHTTP)
	}
	if m.TransportClass != TransportClassNone || m.TransportCause != "" {
		s |= 1 << uint(FailureTransport)
	}
	if m.ProtocolCause != "" {
		s |= 1 << uint(FailureProtocol)
	}
	if m.CallerCause != "" {
		s |= 1 << uint(FailureCaller)
	}
	if s == 0 {
		// No predicate at all: every layer is in scope.
		return 0xff
	}
	return s
}

// overlaps reports whether the two matches can both apply to one and the
// same observation. It is deliberately conservative: when it cannot prove
// two rules are disjoint, it says they overlap, and the matrix rejects the
// pair. Rejecting a configuration that might be ambiguous is fail-closed;
// silently picking one of two equipollent rules is not.
func (m Match) overlaps(n Match) bool {
	if m.classSet()&n.classSet() == 0 {
		return false
	}
	if m.Status != 0 && n.Status != 0 && m.Status != n.Status {
		return false
	}
	if m.StatusClass != StatusClassNone && n.StatusClass != StatusClassNone && m.StatusClass != n.StatusClass {
		return false
	}
	// An exact status and a status class are compatible only when the status
	// really falls in the class.
	if m.Status != 0 && n.StatusClass != StatusClassNone && StatusClassOf(m.Status) != n.StatusClass {
		return false
	}
	if n.Status != 0 && m.StatusClass != StatusClassNone && StatusClassOf(n.Status) != m.StatusClass {
		return false
	}
	if m.TransportClass != TransportClassNone && n.TransportClass != TransportClassNone && m.TransportClass != n.TransportClass {
		return false
	}
	// A transport cause belongs to a class: a cause rule and a class rule
	// whose class that cause does not belong to cannot coincide.
	if m.TransportCause != "" && n.TransportClass != TransportClassNone && TransportClassOfCause(m.TransportCause) != n.TransportClass {
		return false
	}
	if n.TransportCause != "" && m.TransportClass != TransportClassNone && TransportClassOfCause(n.TransportCause) != m.TransportClass {
		return false
	}
	if m.ProviderErrorType != "" && n.ProviderErrorType != "" && m.ProviderErrorType != n.ProviderErrorType {
		return false
	}
	if m.ProviderErrorCode != "" && n.ProviderErrorCode != "" && m.ProviderErrorCode != n.ProviderErrorCode {
		return false
	}
	if m.Streaming != nil && n.Streaming != nil && *m.Streaming != *n.Streaming {
		return false
	}
	if m.CandidateIndex != 0 && n.CandidateIndex != 0 && m.CandidateIndex != n.CandidateIndex {
		return false
	}
	if m.RetryIndex != nil && n.RetryIndex != nil && *m.RetryIndex != *n.RetryIndex {
		return false
	}
	// The three cause kinds are mutually exclusive on an observation: a
	// transport failure carries no protocol cause, a protocol failure no
	// caller cause. Two rules that each constrain a kind the other leaves
	// free cannot both apply — the class set above already excludes the
	// cross-layer cases, and this covers a rule that constrains two kinds
	// against one that constrains a single, different one.
	if m.TransportCause != "" && n.TransportCause != "" && m.TransportCause != n.TransportCause {
		return false
	}
	if m.ProtocolCause != "" && n.ProtocolCause != "" && m.ProtocolCause != n.ProtocolCause {
		return false
	}
	if m.CallerCause != "" && n.CallerCause != "" && m.CallerCause != n.CallerCause {
		return false
	}
	return true
}

// Rule is one row of the matrix: a stable identity, a typed predicate set,
// and the action a matching failure takes.
//
// The identity is what makes partial overrides work: a child layer may
// restate a rule by ID (replacing it outright, wherever the parent put it)
// or introduce a new one, and the operator never has to repeat the parent's
// rule list to change one row.
type Rule struct {
	ID     string
	Match  Match
	Action Action
}

// maxRuleIDBytes bounds a rule identity; the charset is deliberately narrow
// because the ID rides on log evidence as a field value.
const maxRuleIDBytes = 64

// ReservedRuleID is the identity of the implicit catch-all. It names the
// policy's default action, which is carried on the Matrix itself rather than
// as a rule, so a rule may not claim the name.
const ReservedRuleID = "default"

// ValidateRuleID rejects an identity that is empty, over-long, or outside
// the safe charset. Error text is fixed: an ID is operator input.
func ValidateRuleID(id string) error {
	if id == "" {
		return errors.New("rule id must not be empty")
	}
	if len(id) > maxRuleIDBytes {
		return errors.New("rule id is too long")
	}
	if id == ReservedRuleID {
		return errors.New(`rule id "default" is reserved for the policy's default action`)
	}
	for i := 0; i < len(id); i++ {
		c := id[i]
		ok := (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '-' || c == '_' || c == '.'
		if !ok {
			return errors.New("rule id may contain only letters, digits, '-', '_' and '.'")
		}
	}
	return nil
}

// Canonical rule identities for the shorthand forms. Configuring a status,
// a bucket, or a cause through the shorthand generates exactly these IDs, so
// an override can replace any of them by name without restating the row.
func StatusRuleID(status int) string {
	return "http-" + strconv.Itoa(status)
}

// StatusClassRuleID is the canonical ID of a status-bucket rule.
func StatusClassRuleID(c StatusClass) string {
	return "http-class-" + c.String()
}

// TransportClassRuleID is the canonical ID of a transport-class rule.
func TransportClassRuleID(c TransportClass) string {
	return "transport-class-" + c.String()
}

// TransportCauseRuleID is the canonical ID of a transport-cause rule.
func TransportCauseRuleID(cause string) string {
	return "transport-cause-" + cause
}

// ProtocolRuleID is the canonical ID of a protocol-cause rule.
func ProtocolRuleID(cause string) string {
	return "protocol-" + cause
}

// CallerRuleID is the canonical ID of a caller-cause rule. The canonical
// cause token carries a prefix the ID drops, so "caller-canceled" and
// "caller-deadline" stay as short as the shorthand keys that generate them.
func CallerRuleID(cause string) string {
	switch cause {
	case CallerCanceled:
		return "caller-canceled"
	case CallerDeadline:
		return "caller-deadline"
	default:
		return "caller-" + strings.ReplaceAll(cause, "_", "-")
	}
}

// sortRules orders a rule list deterministically: by precedence key
// descending, then by identity ascending. The identity tiebreak makes the
// order total (IDs are unique once a matrix is built), so the evaluation
// order never depends on map iteration or on the order the operator wrote
// the rules in.
func sortRules(rules []Rule) {
	sort.Slice(rules, func(i, j int) bool {
		ki, kj := rules[i].Match.Specificity(), rules[j].Match.Specificity()
		if ki != kj {
			return ki > kj
		}
		return rules[i].ID < rules[j].ID
	})
}
