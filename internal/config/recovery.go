package config

import (
	"errors"
	"fmt"
	"sort"
	"strconv"
	"time"

	"openai-compatible-injector/internal/recovery"
)

// The runtime `recovery` block is the configuration surface of the recovery
// policy domain: the failure → action matrix, the retry mechanics, the
// candidate-walk bound, the exchange envelope, and the Retry-After policy.
//
// The block is accepted in four positions, and the same shape means something
// different in each: the top-level block is the GLOBAL layer (the only one
// that may state the request-scoped `budget.request` envelope), while a
// `providers` entry, a `models` entry, and a chained model's candidate entry
// each carry an OVERRIDE layer. Layers fold in a fixed order at load time —
// global, provider, model, candidate — and every candidate of every model
// freezes its own resolved policy onto the snapshot, so a reload can never
// reshape a request already in flight.
//
// Every rejection below is fixed text that names the position and never the
// value: a rule ID, a cause token, a duration, or a model/provider name can
// all carry a botched paste, and error text reaches logs verbatim.

// runtimeRecovery mirrors one `recovery` block. It is the shape of all four
// positions; the override positions reject `budget.request` explicitly
// (buildRecoveryOverride) rather than through a second struct, so the
// rejection can name the position instead of only a line number.
type runtimeRecovery struct {
	Matrix     *runtimeRecoveryMatrix     `yaml:"matrix"`
	Retries    *runtimeRecoveryRetries    `yaml:"retries"`
	Fallback   *runtimeRecoveryFallback   `yaml:"fallback"`
	Budget     *runtimeRecoveryBudget     `yaml:"budget"`
	RetryAfter *runtimeRecoveryRetryAfter `yaml:"retry-after"`
}

// runtimeRecoveryMatrix mirrors the `matrix` block: the shorthand forms that
// expand into canonical rules, the free-form rule list, and the catch-all
// action.
type runtimeRecoveryMatrix struct {
	HTTP      *runtimeRecoveryHTTP  `yaml:"http"`
	Transport map[string]string     `yaml:"transport"`
	Protocol  map[string]string     `yaml:"protocol"`
	Caller    map[string]string     `yaml:"caller"`
	Rules     []runtimeRecoveryRule `yaml:"rules"`
	Default   string                `yaml:"default"`
}

// runtimeRecoveryHTTP mirrors `matrix.http`: exact statuses and status
// buckets. Both key sets are operator-chosen tokens, so a malformed one is
// rejected by position, never echoed.
type runtimeRecoveryHTTP struct {
	Exact   map[string]string `yaml:"exact"`
	Classes map[string]string `yaml:"classes"`
}

// runtimeRecoveryRule mirrors one free-form `rules` entry. The identity is
// required here — it is what lets a later layer restate (replace) or add a
// row — and the action is required with it.
type runtimeRecoveryRule struct {
	ID     string               `yaml:"id"`
	When   *runtimeRecoveryWhen `yaml:"when"`
	Action string               `yaml:"action"`
}

// runtimeRecoveryWhen mirrors a rule's `when` predicate set. Every member is
// optional; an omitted `when` block constrains nothing. At most one of the
// transport/protocol/caller cause members may be stated — the domain rejects
// the rest, because no observation carries two cause kinds.
type runtimeRecoveryWhen struct {
	Status         *int                          `yaml:"status"`
	StatusClass    string                        `yaml:"status-class"`
	Failure        string                        `yaml:"failure"`
	TransportClass string                        `yaml:"transport-class"`
	TransportCause string                        `yaml:"transport-cause"`
	ProtocolCause  string                        `yaml:"protocol-cause"`
	CallerCause    string                        `yaml:"caller-cause"`
	ProviderError  *runtimeRecoveryProviderError `yaml:"provider-error"`
	Streaming      *bool                         `yaml:"streaming"`
	CandidateIndex *int                          `yaml:"candidate-index"`
	RetryIndex     *int                          `yaml:"retry-index"`
}

// runtimeRecoveryProviderError mirrors the rule predicate over the upstream
// error object's own type/code members. Both are matched against
// already-token-gated evidence values; neither is ever echoed.
type runtimeRecoveryProviderError struct {
	Code string `yaml:"code"`
	Type string `yaml:"type"`
}

// runtimeRecoveryRetries mirrors the `retries` block. Durations stay strings
// for the same reason as every other duration in this file: yaml.v3 decodes
// them as bare integer nanoseconds, not the spelling operators write.
type runtimeRecoveryRetries struct {
	MaxRetries  *int                    `yaml:"max-retries"`
	MaxElapsed  string                  `yaml:"max-elapsed"`
	Backoff     *runtimeRecoveryBackoff `yaml:"backoff"`
	OnExhausted string                  `yaml:"on-exhausted"`
}

// runtimeRecoveryBackoff mirrors the `retries.backoff` block.
type runtimeRecoveryBackoff struct {
	Initial string   `yaml:"initial"`
	Max     string   `yaml:"max"`
	Jitter  *float64 `yaml:"jitter"`
}

// runtimeRecoveryFallback mirrors the `fallback` block.
type runtimeRecoveryFallback struct {
	Enabled       *bool  `yaml:"enabled"`
	MaxCandidates *int   `yaml:"max-candidates"`
	OnExhausted   string `yaml:"on-exhausted"`
}

// runtimeRecoveryBudget mirrors the `budget` block. A candidate-scoped layer
// may only carry `candidate` — the request envelope is a property of the
// request, so an override stating one is rejected by position rather than
// silently ignored.
type runtimeRecoveryBudget struct {
	Request   *runtimeRecoveryEnvelope `yaml:"request"`
	Candidate *runtimeRecoveryEnvelope `yaml:"candidate"`
}

// runtimeRecoveryEnvelope mirrors one `budget` envelope.
type runtimeRecoveryEnvelope struct {
	MaxExchanges *int   `yaml:"max-exchanges"`
	MaxElapsed   string `yaml:"max-elapsed"`
}

// runtimeRecoveryRetryAfter mirrors the `retry-after` block.
type runtimeRecoveryRetryAfter struct {
	Enabled  *bool  `yaml:"enabled"`
	Mode     string `yaml:"mode"`
	MaxDelay string `yaml:"max-delay"`
}

// buildGlobalRecovery builds the global layer's partial from the top-level
// `recovery` block and the legacy `provider-fallback` block.
//
// The two forms are alternatives for the SAME field: stating both the
// fallback policy and its legacy spelling in one layer rejects the file
// rather than silently preferring one, because which of the two the service
// actually runs would otherwise be invisible in the file. Stating the legacy
// block beside a recovery block that does NOT carry a fallback is not a
// contradiction — the two are separate fields of the same layer, exactly as
// the per-model `retries` alias folds in beside a model recovery block that
// does not state `recovery.retries` — so the legacy value is folded into the
// layer instead of being dropped.
func buildGlobalRecovery(block *runtimeRecovery, legacy *runtimeProviderFallback) (recovery.Partial, error) {
	var p recovery.Partial
	if block != nil {
		built, err := buildRecoveryPartial(block)
		if err != nil {
			return recovery.Partial{}, err
		}
		if legacy != nil && block.Fallback != nil {
			return recovery.Partial{}, errors.New("recovery.fallback and the legacy provider-fallback block are mutually exclusive")
		}
		p = built
	}
	if legacy != nil && p.Fallback == nil {
		p.Fallback = buildLegacyFallback(legacy)
	}
	return p, nil
}

// buildRecoveryPartial translates one recovery block into the layer partial
// the resolver folds.
func buildRecoveryPartial(block *runtimeRecovery) (recovery.Partial, error) {
	var p recovery.Partial
	if block.Matrix != nil {
		m, err := buildRecoveryMatrix(block.Matrix)
		if err != nil {
			return recovery.Partial{}, err
		}
		p.Matrix = m
	}
	if block.Retries != nil {
		r, err := buildRecoveryRetries(block.Retries)
		if err != nil {
			return recovery.Partial{}, err
		}
		p.Retry = r
	}
	if block.Fallback != nil {
		f, err := buildRecoveryFallback(block.Fallback)
		if err != nil {
			return recovery.Partial{}, err
		}
		p.Fallback = f
	}
	if block.Budget != nil {
		b, err := buildRecoveryBudget(block.Budget)
		if err != nil {
			return recovery.Partial{}, err
		}
		p.Budget = b
	}
	if block.RetryAfter != nil {
		ra, err := buildRecoveryRetryAfter(block.RetryAfter)
		if err != nil {
			return recovery.Partial{}, err
		}
		p.RetryAfter = ra
	}
	return p, nil
}

// buildRecoveryOverride translates an override-position recovery block: the
// same shape as the global one minus the request-scoped envelope and the
// walk bound. Both rejections name the offending field and the position it
// belongs to, and never the value.
//
// `recovery.fallback` is rejected here — at every call-in — because the
// walk's reach is a property of the REQUEST's primary policy, not of any
// candidate: the engine's EnterCandidate gate reads only the primary
// policy's Fallback (engine.go canEnter), so a fallback block on a provider
// or candidate override would merge, validate, hash, and then do nothing.
// Accepting it would leave an operator believing a lower-level block steers
// chain reach it cannot. The global position owns the walk bound; the model
// position states it through buildModelRecovery, which applies the model's
// override to that model's own primary candidate and therefore genuinely
// steers that model's walk.
func buildRecoveryOverride(block *runtimeRecovery) (recovery.Partial, error) {
	if block.Budget != nil && block.Budget.Request != nil {
		return recovery.Partial{}, errors.New("recovery.budget.request is request-scoped and only valid in the top-level recovery block")
	}
	if block.Fallback != nil {
		return recovery.Partial{}, errors.New("recovery.fallback is request-scoped (the candidate walk's reach) and only valid in the top-level recovery block or a model entry")
	}
	return buildRecoveryPartial(block)
}

// buildRecoveryMatrix expands the shorthand forms and the free-form rule list
// into the layer's rule overrides plus its default action.
//
// Rules are emitted in a fixed order — http.exact by numeric status, then
// http.classes, transport, protocol, caller and rules each by sorted key (the
// free-form list in written order) — so a rejection's rule ordinal is
// deterministic and never depends on Go map iteration.
func buildRecoveryMatrix(rm *runtimeRecoveryMatrix) (*recovery.MatrixPartial, error) {
	out := &recovery.MatrixPartial{}
	if rm.HTTP != nil {
		for _, key := range sortedRecoveryKeys(rm.HTTP.Exact) {
			status, err := parseRecoveryStatusKey(key)
			if err != nil {
				return nil, fmt.Errorf("recovery.matrix.http.exact: %w", err)
			}
			action, err := recovery.ParseAction(rm.HTTP.Exact[key])
			if err != nil {
				return nil, fmt.Errorf("recovery.matrix.http.exact: %w", err)
			}
			out.Rules = append(out.Rules, recovery.Rule{
				ID:     recovery.StatusRuleID(status),
				Match:  recovery.Match{Status: status},
				Action: action,
			})
		}
		for _, key := range sortedRecoveryKeys(rm.HTTP.Classes) {
			class, err := recovery.ParseStatusClass(key)
			if err != nil {
				return nil, fmt.Errorf("recovery.matrix.http.classes: %w", err)
			}
			action, err := recovery.ParseAction(rm.HTTP.Classes[key])
			if err != nil {
				return nil, fmt.Errorf("recovery.matrix.http.classes: %w", err)
			}
			out.Rules = append(out.Rules, recovery.Rule{
				ID:     recovery.StatusClassRuleID(class),
				Match:  recovery.Match{StatusClass: class},
				Action: action,
			})
		}
	}
	for _, key := range sortedRecoveryKeys(rm.Transport) {
		action, err := recovery.ParseAction(rm.Transport[key])
		if err != nil {
			return nil, fmt.Errorf("recovery.matrix.transport: %w", err)
		}
		// A token that is both a class and a cause (proxy_connect,
		// proxy_auth) selects the CLASS: the coarse bucket is the wider
		// statement of intent, and the finer row stays reachable through an
		// explicit rule.
		if class, err := recovery.ParseTransportClass(key); err == nil {
			out.Rules = append(out.Rules, recovery.Rule{
				ID:     recovery.TransportClassRuleID(class),
				Match:  recovery.Match{Class: recovery.FailureTransport, TransportClass: class},
				Action: action,
			})
			continue
		}
		if !recovery.KnownTransportCause(key) {
			return nil, errors.New("recovery.matrix.transport: key must be a transport class (connection, timeout, proxy_connect, proxy_auth) or a transport cause token")
		}
		out.Rules = append(out.Rules, recovery.Rule{
			ID:     recovery.TransportCauseRuleID(key),
			Match:  recovery.Match{Class: recovery.FailureTransport, TransportCause: key},
			Action: action,
		})
	}
	for _, key := range sortedRecoveryKeys(rm.Protocol) {
		if !recovery.KnownProtocolCause(key) {
			return nil, errors.New("recovery.matrix.protocol: key must be a protocol cause token (invalid_response, oversized_response, body_read_failed, body_timeout)")
		}
		action, err := recovery.ParseAction(rm.Protocol[key])
		if err != nil {
			return nil, fmt.Errorf("recovery.matrix.protocol: %w", err)
		}
		out.Rules = append(out.Rules, recovery.Rule{
			ID:     recovery.ProtocolRuleID(key),
			Match:  recovery.Match{Class: recovery.FailureProtocol, ProtocolCause: key},
			Action: action,
		})
	}
	for _, key := range sortedRecoveryKeys(rm.Caller) {
		cause, err := canonicalCallerCause(key)
		if err != nil {
			return nil, fmt.Errorf("recovery.matrix.caller: %w", err)
		}
		action, err := recovery.ParseAction(rm.Caller[key])
		if err != nil {
			return nil, fmt.Errorf("recovery.matrix.caller: %w", err)
		}
		// A caller's own cancellation is a hard stop the matrix may describe
		// but never weaken; rejecting here names the offending key rather
		// than letting a generic rule error stand in for it.
		if action != recovery.ActionTerminal {
			return nil, errors.New("recovery.matrix.caller must be terminal")
		}
		out.Rules = append(out.Rules, recovery.Rule{
			ID:     recovery.CallerRuleID(cause),
			Match:  recovery.Match{Class: recovery.FailureCaller, CallerCause: cause},
			Action: action,
		})
	}
	for i, rr := range rm.Rules {
		rule, err := buildRecoveryRule(rr, i+1)
		if err != nil {
			return nil, err
		}
		out.Rules = append(out.Rules, rule)
	}
	if rm.Default != "" {
		action, err := recovery.ParseAction(rm.Default)
		if err != nil {
			return nil, fmt.Errorf("recovery.matrix.default: %w", err)
		}
		out.Default = &action
	}
	return out, nil
}

// buildRecoveryRule translates one free-form rule. The identity is validated
// by the domain's own rules (non-empty, bounded, safe charset, not the
// reserved catch-all name) and every message is fixed text.
func buildRecoveryRule(rr runtimeRecoveryRule, ordinal int) (recovery.Rule, error) {
	field := fmt.Sprintf("recovery.matrix.rules rule %d", ordinal)
	if err := recovery.ValidateRuleID(rr.ID); err != nil {
		return recovery.Rule{}, fmt.Errorf("%s: %w", field, err)
	}
	var match recovery.Match
	if rr.When != nil {
		m, err := buildRecoveryMatch(*rr.When, field)
		if err != nil {
			return recovery.Rule{}, err
		}
		match = m
	}
	action, err := recovery.ParseAction(rr.Action)
	if err != nil {
		return recovery.Rule{}, fmt.Errorf("%s.action: %w", field, err)
	}
	return recovery.Rule{ID: rr.ID, Match: match, Action: action}, nil
}

// buildRecoveryMatch translates one rule's predicate set. Every token is
// checked against the domain's closed vocabulary: a token that is not in it
// would produce a rule that can never fire, and a rule that cannot fire is a
// misunderstanding of the schema rather than a policy.
func buildRecoveryMatch(w runtimeRecoveryWhen, field string) (recovery.Match, error) {
	var m recovery.Match
	if w.Status != nil {
		if *w.Status < 100 || *w.Status > 599 {
			return recovery.Match{}, fmt.Errorf("%s.when.status: must be an HTTP status between 100 and 599", field)
		}
		m.Status = *w.Status
	}
	if w.StatusClass != "" {
		class, err := recovery.ParseStatusClass(w.StatusClass)
		if err != nil {
			return recovery.Match{}, fmt.Errorf("%s.when.status-class: %w", field, err)
		}
		m.StatusClass = class
	}
	if w.Failure != "" {
		class, err := recovery.ParseFailureClass(w.Failure)
		if err != nil {
			return recovery.Match{}, fmt.Errorf("%s.when.failure: %w", field, err)
		}
		m.Class = class
	}
	if w.TransportClass != "" {
		class, err := recovery.ParseTransportClass(w.TransportClass)
		if err != nil {
			return recovery.Match{}, fmt.Errorf("%s.when.transport-class: %w", field, err)
		}
		m.TransportClass = class
	}
	if w.TransportCause != "" {
		if !recovery.KnownTransportCause(w.TransportCause) {
			return recovery.Match{}, fmt.Errorf("%s.when.transport-cause: unknown transport cause token", field)
		}
		m.TransportCause = w.TransportCause
	}
	if w.ProtocolCause != "" {
		if !recovery.KnownProtocolCause(w.ProtocolCause) {
			return recovery.Match{}, fmt.Errorf("%s.when.protocol-cause: unknown protocol cause token", field)
		}
		m.ProtocolCause = w.ProtocolCause
	}
	if w.CallerCause != "" {
		cause, err := canonicalCallerCause(w.CallerCause)
		if err != nil {
			return recovery.Match{}, fmt.Errorf("%s.when.caller-cause: %w", field, err)
		}
		m.CallerCause = cause
	}
	if w.ProviderError != nil {
		m.ProviderErrorType = w.ProviderError.Type
		m.ProviderErrorCode = w.ProviderError.Code
	}
	if w.Streaming != nil {
		streaming := *w.Streaming
		m.Streaming = &streaming
	}
	if w.CandidateIndex != nil {
		if *w.CandidateIndex < 1 {
			return recovery.Match{}, fmt.Errorf("%s.when.candidate-index: must be at least 1", field)
		}
		m.CandidateIndex = *w.CandidateIndex
	}
	if w.RetryIndex != nil {
		if *w.RetryIndex < 0 {
			return recovery.Match{}, fmt.Errorf("%s.when.retry-index: must not be negative", field)
		}
		retryIndex := *w.RetryIndex
		m.RetryIndex = &retryIndex
	}
	return m, nil
}

// buildRecoveryRetries translates the `retries` block. Values are passed
// through untouched: the domain owns every range, cap, and cross-field
// contradiction, and validating them here as well would give two places to
// disagree about what is legal.
func buildRecoveryRetries(rr *runtimeRecoveryRetries) (*recovery.RetryPartial, error) {
	out := &recovery.RetryPartial{}
	if rr.MaxRetries != nil {
		n := *rr.MaxRetries
		out.MaxRetries = &n
	}
	if rr.MaxElapsed != "" {
		d, err := parseRecoveryDuration(rr.MaxElapsed, "recovery.retries.max-elapsed")
		if err != nil {
			return nil, err
		}
		out.MaxElapsed = &d
	}
	if rr.Backoff != nil {
		backoff := &recovery.BackoffPartial{}
		if rr.Backoff.Initial != "" {
			d, err := parseRecoveryDuration(rr.Backoff.Initial, "recovery.retries.backoff.initial")
			if err != nil {
				return nil, err
			}
			backoff.Initial = &d
		}
		if rr.Backoff.Max != "" {
			d, err := parseRecoveryDuration(rr.Backoff.Max, "recovery.retries.backoff.max")
			if err != nil {
				return nil, err
			}
			backoff.Max = &d
		}
		if rr.Backoff.Jitter != nil {
			j := *rr.Backoff.Jitter
			backoff.Jitter = &j
		}
		out.Backoff = backoff
	}
	if rr.OnExhausted != "" {
		action, err := recovery.ParseAction(rr.OnExhausted)
		if err != nil {
			return nil, fmt.Errorf("recovery.retries.on-exhausted: %w", err)
		}
		out.OnExhausted = &action
	}
	return out, nil
}

// buildRecoveryFallback translates the `fallback` block.
func buildRecoveryFallback(rf *runtimeRecoveryFallback) (*recovery.FallbackPartial, error) {
	out := &recovery.FallbackPartial{}
	if rf.Enabled != nil {
		enabled := *rf.Enabled
		out.Enabled = &enabled
	}
	if rf.MaxCandidates != nil {
		n := *rf.MaxCandidates
		out.MaxCandidates = &n
	}
	if rf.OnExhausted != "" {
		action, err := recovery.ParseAction(rf.OnExhausted)
		if err != nil {
			return nil, fmt.Errorf("recovery.fallback.on-exhausted: %w", err)
		}
		out.OnExhausted = &action
	}
	return out, nil
}

// buildRecoveryBudget translates the `budget` block. The request envelope is
// only reachable from the global position; buildRecoveryOverride rejects it
// everywhere else before this function is called.
func buildRecoveryBudget(rb *runtimeRecoveryBudget) (*recovery.BudgetPartial, error) {
	out := &recovery.BudgetPartial{}
	if rb.Request != nil {
		envelope, err := buildRecoveryEnvelope(rb.Request, "recovery.budget.request")
		if err != nil {
			return nil, err
		}
		out.Request = envelope
	}
	if rb.Candidate != nil {
		envelope, err := buildRecoveryEnvelope(rb.Candidate, "recovery.budget.candidate")
		if err != nil {
			return nil, err
		}
		out.Candidate = envelope
	}
	return out, nil
}

// buildRecoveryEnvelope translates one budget envelope.
func buildRecoveryEnvelope(re *runtimeRecoveryEnvelope, field string) (*recovery.EnvelopePartial, error) {
	out := &recovery.EnvelopePartial{}
	if re.MaxExchanges != nil {
		n := *re.MaxExchanges
		out.MaxExchanges = &n
	}
	if re.MaxElapsed != "" {
		d, err := parseRecoveryDuration(re.MaxElapsed, field+".max-elapsed")
		if err != nil {
			return nil, err
		}
		out.MaxElapsed = &d
	}
	return out, nil
}

// buildRecoveryRetryAfter translates the `retry-after` block.
func buildRecoveryRetryAfter(ra *runtimeRecoveryRetryAfter) (*recovery.RetryAfterPartial, error) {
	out := &recovery.RetryAfterPartial{}
	if ra.Enabled != nil {
		enabled := *ra.Enabled
		out.Enabled = &enabled
	}
	if ra.Mode != "" {
		mode, err := recovery.ParseRetryAfterMode(ra.Mode)
		if err != nil {
			return nil, fmt.Errorf("recovery.retry-after.mode: %w", err)
		}
		out.Mode = &mode
	}
	if ra.MaxDelay != "" {
		d, err := parseRecoveryDuration(ra.MaxDelay, "recovery.retry-after.max-delay")
		if err != nil {
			return nil, err
		}
		out.MaxDelay = &d
	}
	return out, nil
}

// buildModelRecovery builds a model entry's override layer from its
// `recovery` block and its legacy `retries` block. The two are alternatives
// for the same mechanics — stating both would leave the effective policy
// depending on which one the code happened to read last — so a file that does
// rejects. The returned partial is nil when the model states neither, so the
// layer is skipped rather than re-validated for nothing.
//
// Unlike the provider and candidate positions, a model entry MAY state
// `recovery.fallback`: the model layer resolves into the model's own primary
// candidate (Chain[0].Recovery, mirrored as Model.Recovery), and the engine's
// reach gate reads exactly that policy's Fallback — so a model-level block
// genuinely steers that model's walk. The request-scoped `budget.request`
// envelope stays rejected here like everywhere below the global layer.
// buildRecoveryOverride's fallback rejection is bypassed rather than
// duplicated, and its budget.request rejection still applies: build first for
// the validations that must hold for a model block, then re-check the one
// field that may differ from an override position.
func buildModelRecovery(rm runtimeModel) (*recovery.Partial, error) {
	if rm.Retries != nil && rm.Recovery != nil && rm.Recovery.Retries != nil {
		return nil, errors.New("recovery.retries and the legacy retries block are mutually exclusive")
	}
	if rm.Recovery == nil && rm.Retries == nil {
		return nil, nil
	}
	var p recovery.Partial
	if rm.Recovery != nil {
		if rm.Recovery.Budget != nil && rm.Recovery.Budget.Request != nil {
			return nil, errors.New("recovery.budget.request is request-scoped and only valid in the top-level recovery block")
		}
		block, err := buildRecoveryPartial(rm.Recovery)
		if err != nil {
			return nil, err
		}
		p = block
	}
	if rm.Retries != nil {
		retry, err := buildLegacyRetries(rm.Retries)
		if err != nil {
			return nil, err
		}
		p.Retry = retry
	}
	return &p, nil
}

// buildLegacyFallback normalizes the legacy `provider-fallback` block into
// the fallback layer. The defaults are the ones that block always documented
// — enabled, two candidates — and disabling the walk states a one-candidate
// reach explicitly, which is what the domain's validation requires of a
// disabled policy.
//
// A disabled block states the one-candidate reach whatever `max-attempts`
// says, because that is what the block always meant: the pre-policy-engine
// walk read the count only when the walk was enabled, so a file that carried
// both ran pinned, and an alias must not turn a working file into a startup
// failure over a number that never had an effect.
func buildLegacyFallback(rf *runtimeProviderFallback) *recovery.FallbackPartial {
	enabled := true
	if rf.Enabled != nil {
		enabled = *rf.Enabled
	}
	maxCandidates := defaultProviderFallbackAttempts
	if !enabled {
		maxCandidates = 1
	} else if rf.MaxAttempts != nil {
		maxCandidates = *rf.MaxAttempts
	}
	out := &recovery.FallbackPartial{Enabled: &enabled, MaxCandidates: &maxCandidates}
	return out
}

// buildLegacyRetries normalizes the legacy per-model `retries` block into the
// retry layer. Unstated fields stay nil so they inherit the defaults, exactly
// as the block always did. One legacy nicety is preserved rather than turned
// into a rejection: the old builder lifted an omitted backoff ceiling to a
// raised initial (the initial is the honest floor for a ceiling nobody set),
// so the normalized layer states the ceiling the legacy block produced.
func buildLegacyRetries(rr *runtimeRetries) (*recovery.RetryPartial, error) {
	out := &recovery.RetryPartial{}
	if rr.MaxRetries != nil {
		n := *rr.MaxRetries
		out.MaxRetries = &n
	}
	if rr.MaxElapsed != "" {
		d, err := parseRecoveryDuration(rr.MaxElapsed, "retries.max-elapsed")
		if err != nil {
			return nil, err
		}
		out.MaxElapsed = &d
	}
	if rr.Backoff != nil {
		backoff := &recovery.BackoffPartial{}
		if rr.Backoff.Initial != "" {
			d, err := parseRecoveryDuration(rr.Backoff.Initial, "retries.backoff.initial")
			if err != nil {
				return nil, err
			}
			backoff.Initial = &d
		}
		if rr.Backoff.Max != "" {
			d, err := parseRecoveryDuration(rr.Backoff.Max, "retries.backoff.max")
			if err != nil {
				return nil, err
			}
			backoff.Max = &d
		} else if backoff.Initial != nil && *backoff.Initial > recovery.DefaultBackoffMax {
			ceiling := *backoff.Initial
			backoff.Max = &ceiling
		}
		if rr.Backoff.Jitter != nil {
			j := *rr.Backoff.Jitter
			backoff.Jitter = &j
		}
		out.Backoff = backoff
	}
	return out, nil
}

// resolveCandidateRecovery folds one candidate's layers in the documented
// order and returns its effective policy.
//
// The order is the hierarchy the file expresses: the global block, then the
// provider the candidate names, then the model, then the candidate's own
// override — narrowest last, so an override always beats what it overrides.
// The base is the already-resolved global policy: resolving it once keeps a
// global contradiction reported without a model position attached to it.
//
// provider, model and candidate are nil when that position states nothing.
func resolveCandidateRecovery(global recovery.Policy, provider, model, candidate *recovery.Partial) (recovery.Policy, error) {
	layers := make([]recovery.Layer, 0, 3)
	if provider != nil {
		layers = append(layers, recovery.Layer{Name: "provider", Partial: *provider})
	}
	if model != nil {
		layers = append(layers, recovery.Layer{Name: "model", Partial: *model})
	}
	if candidate != nil {
		layers = append(layers, recovery.Layer{Name: "candidate", Partial: *candidate})
	}
	return recovery.Resolve(global, layers...)
}

// parseRecoveryStatusKey reads an exact-status shorthand key. Keys are
// three-digit statuses; a bucket ("4xx") belongs in `classes`, and anything
// else is rejected by position without echoing the key.
func parseRecoveryStatusKey(key string) (int, error) {
	if len(key) != 3 {
		return 0, errors.New("key must be a three-digit HTTP status")
	}
	status, err := strconv.Atoi(key)
	if err != nil {
		return 0, errors.New("key must be a three-digit HTTP status")
	}
	if status < 100 || status > 599 {
		return 0, errors.New("key must be an HTTP status between 100 and 599")
	}
	return status, nil
}

// canonicalCallerCause maps the short configuration spelling of a caller
// cause onto the canonical token the observation carries. Error text is
// fixed: the token is operator input.
func canonicalCallerCause(token string) (string, error) {
	switch token {
	case "canceled", recovery.CallerCanceled:
		return recovery.CallerCanceled, nil
	case "deadline", recovery.CallerDeadline:
		return recovery.CallerDeadline, nil
	default:
		return "", errors.New("caller cause must be one of canceled, deadline")
	}
}

// parseRecoveryDuration parses one operator-supplied duration. ParseDuration
// errors quote their input and error text reaches logs verbatim, so a parse
// failure is reported as fixed text naming the field; the value is never
// echoed.
func parseRecoveryDuration(raw, field string) (time.Duration, error) {
	d, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("%s must be a valid duration (e.g. 10s, 1m)", field)
	}
	return d, nil
}

// sortedRecoveryKeys returns a mapping's keys in sorted order, so shorthand
// expansion — and therefore every rule ordinal in a rejection — is
// deterministic rather than a function of map iteration.
func sortedRecoveryKeys(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
