package recovery

import (
	"strings"
	"testing"
)

// legacyDisposition is the behavior the default matrix must reproduce,
// restated here as an independent oracle rather than as a copy of the
// matrix's own data: the pre-policy engine's classifyStatus, transcribed
// from the contract README documents. A change to the default matrix that
// alters any status's disposition fails this test, which is the point — the
// refactor moved the table from Go into data, and data is only equivalent if
// something outside it says so.
func legacyDisposition(status int) Action {
	if status < 400 || status > 599 {
		return ActionTerminal // an answer: committed, never decided
	}
	switch status {
	case 408, 425, 429:
		return ActionRetry
	case 401, 403, 404, 405, 409, 422:
		return ActionFallback
	case 501, 505:
		return ActionTerminal
	}
	if status >= 500 {
		return ActionRetry
	}
	return ActionTerminal
}

func TestDefaultMatrixReproducesLegacyStatusTable(t *testing.T) {
	pol := Default()
	for status := 400; status <= 599; status++ {
		o := Observation{Class: FailureHTTP, HTTPStatus: status, StatusClass: StatusClassOf(status)}
		got, id := pol.Matrix.Match(o)
		want := legacyDisposition(status)
		if got != want {
			t.Errorf("status %d: default matrix says %v, the pre-policy engine said %v", status, got, want)
		}
		if id == "" {
			t.Errorf("status %d: matched without a rule identity", status)
		}
	}
}

// TestDefaultMatrixStatusRulesAreNamed pins the identities an override has to
// address. They are the public contract of the matrix: renaming one silently
// breaks every file that overrides it.
func TestDefaultMatrixStatusRulesAreNamed(t *testing.T) {
	pol := Default()
	cases := []struct {
		status int
		id     string
	}{
		{429, "http-429"},
		{408, "http-408"},
		{425, "http-425"},
		{401, "http-401"},
		{403, "http-403"},
		{404, "http-404"},
		{405, "http-405"},
		{409, "http-409"},
		{422, "http-422"},
		{501, "http-501"},
		{505, "http-505"},
		{400, "http-class-4xx"},
		{520, "http-class-5xx"},
	}
	for _, c := range cases {
		_, id := pol.Matrix.Match(Observation{Class: FailureHTTP, HTTPStatus: c.status, StatusClass: StatusClassOf(c.status)})
		if id != c.id {
			t.Errorf("status %d decided by %q, want %q", c.status, id, c.id)
		}
	}
}

func TestDefaultMatrixTransportAndProtocolRows(t *testing.T) {
	pol := Default()
	cases := []struct {
		name string
		o    Observation
		want Action
		id   string
	}{
		{"connection refused", Observation{Class: FailureTransport, TransportClass: TransportClassConnection, TransportCause: CauseConnectionRefused}, ActionFallback, "transport-cause-connection_refused"},
		{"tls", Observation{Class: FailureTransport, TransportClass: TransportClassConnection, TransportCause: CauseTLS}, ActionFallback, "transport-cause-tls"},
		{"network timeout", Observation{Class: FailureTransport, TransportClass: TransportClassTimeout, TransportCause: CauseNetworkTimeout}, ActionFallback, "transport-cause-network_timeout"},
		{"proxy auth", Observation{Class: FailureTransport, TransportClass: TransportClassProxyAuth, TransportCause: CauseProxyAuth}, ActionFallback, "transport-cause-proxy_auth"},
		{"no eligible endpoint", Observation{Class: FailureTransport, TransportClass: TransportClassConnection, TransportCause: CauseNoEligibleEndpoint}, ActionFallback, "transport-cause-no_eligible_endpoint"},
		{"invalid body", Observation{Class: FailureProtocol, ProtocolCause: ProtocolInvalidResponse}, ActionRetry, "protocol-invalid_response"},
		{"oversized body", Observation{Class: FailureProtocol, ProtocolCause: ProtocolOversizedResponse}, ActionRetry, "protocol-oversized_response"},
		{"body read failed", Observation{Class: FailureProtocol, ProtocolCause: ProtocolBodyReadFailed}, ActionRetry, "protocol-body_read_failed"},
		{"body timeout", Observation{Class: FailureProtocol, ProtocolCause: ProtocolBodyTimeout}, ActionRetry, "protocol-body_timeout"},
		{"caller canceled", Observation{Class: FailureCaller, CallerCause: CallerCanceled}, ActionTerminal, "caller-canceled"},
		{"caller deadline", Observation{Class: FailureCaller, CallerCause: CallerDeadline}, ActionTerminal, "caller-deadline"},
	}
	for _, c := range cases {
		got, id := pol.Matrix.Match(c.o)
		if got != c.want || id != c.id {
			t.Errorf("%s: got (%v, %q), want (%v, %q)", c.name, got, id, c.want, c.id)
		}
	}
}

// TestDefaultMatrixClassRuleCoversUnlistedCause documents the fallback a
// transport failure takes when its cause is not in the closed set — a class
// rule, not a cause rule, so an unknown token still gets the layer's policy.
func TestDefaultMatrixClassRuleCoversUnlistedCause(t *testing.T) {
	pol := Default()
	o := Observation{Class: FailureTransport, TransportClass: TransportClassTimeout}
	got, id := pol.Matrix.Match(o)
	if got != ActionFallback || id != "transport-class-timeout" {
		t.Fatalf("a class-only transport failure got (%v, %q)", got, id)
	}
}

func TestMatrixUnmatchedTakesDefault(t *testing.T) {
	pol := Default()
	got, id := pol.Matrix.Match(Observation{Class: FailureHTTP, HTTPStatus: 418, StatusClass: StatusClass4xx})
	if id != "http-class-4xx" {
		t.Fatalf("418 matched %q", id)
	}
	if got != ActionTerminal {
		t.Fatalf("418 got %v", got)
	}
	// An observation that carries no class at all is not a failure this
	// package knows how to classify; it must land on the default, which is
	// the safe direction.
	got, id = pol.Matrix.Match(Observation{})
	if id != ReservedRuleID || got != ActionTerminal {
		t.Fatalf("an unclassifiable observation got (%v, %q), want the terminal default", got, id)
	}
}

func TestMatrixPrecedenceIsExactStatusOverClass(t *testing.T) {
	rules := []Rule{
		{ID: "http-class-5xx", Match: Match{StatusClass: StatusClass5xx}, Action: ActionRetry},
		{ID: "http-503", Match: Match{Status: 503}, Action: ActionTerminal},
	}
	m, err := NewMatrix(rules, ActionTerminal)
	if err != nil {
		t.Fatalf("NewMatrix: %v", err)
	}
	// Declaration order is irrelevant: the exact status wins regardless.
	if got, id := m.Match(Observation{Class: FailureHTTP, HTTPStatus: 503, StatusClass: StatusClass5xx}); got != ActionTerminal || id != "http-503" {
		t.Fatalf("503 got (%v, %q)", got, id)
	}
	if got, id := m.Match(Observation{Class: FailureHTTP, HTTPStatus: 500, StatusClass: StatusClass5xx}); got != ActionRetry || id != "http-class-5xx" {
		t.Fatalf("500 got (%v, %q)", got, id)
	}
}

func TestMatrixProviderErrorPredicateOutranksStatusClass(t *testing.T) {
	rules := []Rule{
		{ID: "http-class-4xx", Match: Match{StatusClass: StatusClass4xx}, Action: ActionTerminal},
		{ID: "quota", Match: Match{Status: 429, ProviderErrorCode: "insufficient_quota"}, Action: ActionFallback},
		{ID: "http-429", Match: Match{Status: 429}, Action: ActionRetry},
	}
	m, err := NewMatrix(rules, ActionTerminal)
	if err != nil {
		t.Fatalf("NewMatrix: %v", err)
	}
	quota := Observation{Class: FailureHTTP, HTTPStatus: 429, StatusClass: StatusClass4xx, ProviderErrorCode: "insufficient_quota"}
	if got, id := m.Match(quota); got != ActionFallback || id != "quota" {
		t.Fatalf("a provider-specific 429 got (%v, %q), want the quota rule", got, id)
	}
	plain := Observation{Class: FailureHTTP, HTTPStatus: 429, StatusClass: StatusClass4xx}
	if got, id := m.Match(plain); got != ActionRetry || id != "http-429" {
		t.Fatalf("a plain 429 got (%v, %q), want the general 429 rule", got, id)
	}
}

func TestNewMatrixRejectsEquallySpecificOverlap(t *testing.T) {
	cases := []struct {
		name  string
		rules []Rule
	}{
		{
			"same status twice",
			[]Rule{
				{ID: "a", Match: Match{Status: 429}, Action: ActionRetry},
				{ID: "b", Match: Match{Status: 429}, Action: ActionTerminal},
			},
		},
		{
			"same class twice",
			[]Rule{
				{ID: "a", Match: Match{StatusClass: StatusClass5xx}, Action: ActionRetry},
				{ID: "b", Match: Match{StatusClass: StatusClass5xx}, Action: ActionTerminal},
			},
		},
		{
			"provider error code and type covering the same class",
			[]Rule{
				{ID: "a", Match: Match{Status: 429, ProviderErrorCode: "x"}, Action: ActionRetry},
				{ID: "b", Match: Match{Status: 429, ProviderErrorType: "y"}, Action: ActionFallback},
			},
		},
		{
			"same transport cause twice",
			[]Rule{
				{ID: "a", Match: Match{Class: FailureTransport, TransportCause: CauseTLS}, Action: ActionFallback},
				{ID: "b", Match: Match{Class: FailureTransport, TransportCause: CauseTLS}, Action: ActionTerminal},
			},
		},
		{
			"class rule covered by another class predicate of the same rank",
			[]Rule{
				{ID: "a", Match: Match{Class: FailureTransport}, Action: ActionFallback},
				{ID: "b", Match: Match{TransportClass: TransportClassTimeout}, Action: ActionTerminal},
			},
		},
	}
	for _, c := range cases {
		if _, err := NewMatrix(c.rules, ActionTerminal); err == nil {
			t.Errorf("%s: equally specific overlapping rules were accepted", c.name)
		}
	}
}

func TestNewMatrixAcceptsDisjointSameRankRules(t *testing.T) {
	rules := []Rule{
		{ID: "a", Match: Match{Status: 429}, Action: ActionRetry},
		{ID: "b", Match: Match{Status: 503}, Action: ActionRetry},
		{ID: "c", Match: Match{StatusClass: StatusClass4xx}, Action: ActionTerminal},
		{ID: "d", Match: Match{StatusClass: StatusClass5xx}, Action: ActionRetry},
		{ID: "e", Match: Match{Class: FailureTransport, TransportCause: CauseTLS}, Action: ActionFallback},
		{ID: "f", Match: Match{Class: FailureTransport, TransportCause: CauseDial}, Action: ActionFallback},
		{ID: "g", Match: Match{Class: FailureTransport, TransportClass: TransportClassTimeout}, Action: ActionFallback},
		{ID: "h", Match: Match{Class: FailureProtocol, ProtocolCause: ProtocolBodyTimeout}, Action: ActionRetry},
	}
	if _, err := NewMatrix(rules, ActionTerminal); err != nil {
		t.Fatalf("disjoint same-rank rules were rejected: %v", err)
	}
}

func TestNewMatrixRejectsContradictions(t *testing.T) {
	cases := []struct {
		name string
		rule Rule
	}{
		{"status outside the HTTP range", Rule{ID: "a", Match: Match{Status: 99}, Action: ActionRetry}},
		{"status and contradictory class", Rule{ID: "a", Match: Match{Status: 429, StatusClass: StatusClass5xx}, Action: ActionRetry}},
		{"two cause kinds", Rule{ID: "a", Match: Match{TransportCause: CauseTLS, ProtocolCause: ProtocolBodyTimeout}, Action: ActionRetry}},
		{"http class with a transport cause", Rule{ID: "a", Match: Match{Class: FailureHTTP, TransportCause: CauseTLS}, Action: ActionRetry}},
		{"transport class with an HTTP status", Rule{ID: "a", Match: Match{Class: FailureTransport, Status: 429}, Action: ActionRetry}},
	}
	for _, c := range cases {
		if _, err := NewMatrix([]Rule{c.rule}, ActionTerminal); err == nil {
			t.Errorf("%s: an impossible rule was accepted", c.name)
		}
	}
}

func TestNewMatrixRejectsNonTerminalCallerRule(t *testing.T) {
	cases := []Rule{
		{ID: "a", Match: Match{Class: FailureCaller}, Action: ActionRetry},
		{ID: "a", Match: Match{CallerCause: CallerCanceled}, Action: ActionFallback},
	}
	for _, r := range cases {
		if _, err := NewMatrix([]Rule{r}, ActionTerminal); err == nil {
			t.Errorf("a non-terminal caller rule was accepted: %+v", r)
		}
	}
}

func TestNewMatrixRejectsDuplicateAndMalformedIDs(t *testing.T) {
	if _, err := NewMatrix([]Rule{
		{ID: "same", Match: Match{Status: 429}, Action: ActionRetry},
		{ID: "same", Match: Match{Status: 503}, Action: ActionRetry},
	}, ActionTerminal); err == nil {
		t.Error("a repeated rule identity was accepted")
	}
	for _, id := range []string{"", ReservedRuleID, strings.Repeat("x", maxRuleIDBytes+1), "has space", "has:colon"} {
		if err := ValidateRuleID(id); err == nil {
			t.Errorf("rule identity %q was accepted", id)
		}
	}
	for _, id := range []string{"http-429", "transport-cause-tls", "my.rule_1"} {
		if err := ValidateRuleID(id); err != nil {
			t.Errorf("rule identity %q was rejected: %v", id, err)
		}
	}
}

// TestMatrixOrderIsDeterministic pins the total order: precedence first,
// identity second. Without the identity tiebreak the order of two
// same-precedence rules would depend on their declaration order.
func TestMatrixOrderIsDeterministic(t *testing.T) {
	build := func(reversed bool) []string {
		rules := []Rule{
			{ID: "b", Match: Match{Status: 429}, Action: ActionRetry},
			{ID: "a", Match: Match{Status: 503}, Action: ActionRetry},
			{ID: "c", Match: Match{StatusClass: StatusClass5xx}, Action: ActionRetry},
		}
		if reversed {
			rules[0], rules[1] = rules[1], rules[0]
		}
		m, err := NewMatrix(rules, ActionTerminal)
		if err != nil {
			t.Fatalf("NewMatrix: %v", err)
		}
		ids := make([]string, 0, 3)
		for _, r := range m.Rules() {
			ids = append(ids, r.ID)
		}
		return ids
	}
	forward, backward := build(false), build(true)
	for i := range forward {
		if forward[i] != backward[i] {
			t.Fatalf("rule order depends on declaration order: %v vs %v", forward, backward)
		}
	}
	if forward[0] != "a" || forward[2] != "c" {
		t.Fatalf("unexpected order: %v", forward)
	}
}

// TestMatrixRulesAreCopied guards the immutability the resolver depends on: a
// caller that mutates the slice it passed in, or the slice it got back, must
// not be able to change a frozen matrix.
func TestMatrixRulesAreCopied(t *testing.T) {
	in := []Rule{{ID: "a", Match: Match{Status: 429}, Action: ActionRetry}}
	m, err := NewMatrix(in, ActionTerminal)
	if err != nil {
		t.Fatalf("NewMatrix: %v", err)
	}
	in[0].Action = ActionTerminal
	if got, _ := m.Match(Observation{Class: FailureHTTP, HTTPStatus: 429, StatusClass: StatusClass4xx}); got != ActionRetry {
		t.Fatalf("the matrix followed a mutation of the input slice: got %v", got)
	}
	out := m.Rules()
	out[0].Action = ActionTerminal
	if got, _ := m.Match(Observation{Class: FailureHTTP, HTTPStatus: 429, StatusClass: StatusClass4xx}); got != ActionRetry {
		t.Fatalf("the matrix followed a mutation of the returned slice: got %v", got)
	}
}

func TestReasonTokens(t *testing.T) {
	cases := []struct {
		o    Observation
		want string
	}{
		{Observation{Class: FailureHTTP, HTTPStatus: 408}, "http_408"},
		{Observation{Class: FailureHTTP, HTTPStatus: 425}, "http_425"},
		{Observation{Class: FailureHTTP, HTTPStatus: 429}, "http_429"},
		{Observation{Class: FailureHTTP, HTTPStatus: 503}, "http_5xx"},
		{Observation{Class: FailureHTTP, HTTPStatus: 400}, "http_400"},
		{Observation{Class: FailureHTTP, HTTPStatus: 401}, "http_401"},
		{Observation{Class: FailureTransport, TransportCause: CauseTLS}, "tls"},
		{Observation{Class: FailureProtocol, ProtocolCause: ProtocolInvalidResponse}, "upstream_invalid_response"},
		{Observation{Class: FailureProtocol, ProtocolCause: ProtocolOversizedResponse}, "upstream_invalid_response"},
		{Observation{Class: FailureProtocol, ProtocolCause: ProtocolBodyReadFailed}, "upstream_body_read_failed"},
		{Observation{Class: FailureProtocol, ProtocolCause: ProtocolBodyTimeout}, "upstream_body_timeout"},
		{Observation{Class: FailureCaller, CallerCause: CallerCanceled}, "caller_canceled"},
		{Observation{Class: FailureCaller, CallerCause: CallerDeadline}, "caller_deadline_exceeded"},
	}
	for _, c := range cases {
		if got := Reason(c.o); got != c.want {
			t.Errorf("Reason(%+v) = %q, want %q", c.o, got, c.want)
		}
	}
}

func TestStatusClassOfAndIsUpstreamError(t *testing.T) {
	cases := []struct {
		status int
		class  StatusClass
		err    bool
	}{
		{199, StatusClass1xx, false},
		{200, StatusClass2xx, false},
		{204, StatusClass2xx, false},
		{304, StatusClass3xx, false},
		{400, StatusClass4xx, true},
		{429, StatusClass4xx, true},
		{500, StatusClass5xx, true},
		{599, StatusClass5xx, true},
		{600, StatusClassNone, false},
		{0, StatusClassNone, false},
	}
	for _, c := range cases {
		if got := StatusClassOf(c.status); got != c.class {
			t.Errorf("StatusClassOf(%d) = %v, want %v", c.status, got, c.class)
		}
		if got := IsUpstreamError(c.status); got != c.err {
			t.Errorf("IsUpstreamError(%d) = %v, want %v", c.status, got, c.err)
		}
	}
}
