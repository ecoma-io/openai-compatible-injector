package recovery

import "time"

// Default policy values. They are the behavior this proxy has had since
// provider retries and fallback landed, restated as data: the matrix below
// is the disposition table that used to live in Go, and the numbers are the
// ones the runtime config materialized for an absent retries block.
const (
	// DefaultMaxRetries is how many times one candidate is re-asked after
	// its initial attempt.
	DefaultMaxRetries = 1
	// DefaultRetryMaxElapsed bounds a candidate's retry window.
	DefaultRetryMaxElapsed = 10 * time.Second
	// DefaultBackoffInitial is the first retry's wait.
	DefaultBackoffInitial = 250 * time.Millisecond
	// DefaultBackoffMax is the ceiling the wait doubles up to.
	DefaultBackoffMax = 2 * time.Second
	// DefaultBackoffJitter spreads each wait by ±10%.
	DefaultBackoffJitter = 0.1
	// DefaultFallbackMaxCandidates is how many chain candidates a request
	// enters by default: the primary plus one.
	DefaultFallbackMaxCandidates = 2
	// DefaultRequestMaxExchanges is the default request-wide exchange
	// envelope. The default walk reaches at most 2 candidates × 2 attempts ×
	// an egress pool's 3 attempts = 12 exchanges, so the envelope never
	// binds a default deployment — it exists to bound what a configuration
	// can multiply into.
	DefaultRequestMaxExchanges = 32
	// DefaultRequestMaxElapsed bounds the request-wide recovery envelope. It
	// sits at the request cap on purpose: an unstated envelope is a backstop
	// against a runaway walk, never a tuning knob, so it must not bind a walk
	// the operator asked for. See DefaultCandidateMaxElapsed for why the
	// unstated defaults are this generous.
	DefaultRequestMaxElapsed = MaxRequestElapsedCap
	// DefaultCandidateMaxExchanges is the default per-candidate exchange
	// envelope.
	DefaultCandidateMaxExchanges = 16
	// DefaultCandidateMaxElapsed bounds one candidate's recovery envelope. It
	// sits at the candidate cap, and that is load-bearing rather than
	// generous: this envelope must be at least the widest retry window a
	// deployment can legitimately state, and the legacy retries block — which
	// still has to load — allowed a window of up to two minutes. An unstated
	// envelope that rejected a stated retry window would make a previously
	// valid file unloadable, so the default covers everything the vocabulary
	// allows and an operator who wants a tighter walk states one.
	DefaultCandidateMaxElapsed = MaxCandidateElapsedCap
	// DefaultRetryAfterMaxDelay is this policy's own ceiling on an
	// upstream's Retry-After directive. It sits above the default backoff
	// ceiling, so by default the backoff ceiling is what binds — exactly the
	// behavior that shipped before retry-after became configurable.
	DefaultRetryAfterMaxDelay = 5 * time.Second
)

// Default returns the built-in policy every configuration starts from: what
// a model with no recovery block at all, in a file with no recovery block at
// all, runs under.
//
// It is a value, freshly built per call, and it validates: a deployment that
// never writes a recovery block gets exactly this, and the config layer
// proves as much by validating it like any other policy.
func Default() Policy {
	rules := make([]Rule, 0, 32)
	add := func(id string, m Match, a Action) {
		rules = append(rules, Rule{ID: id, Match: m, Action: a})
	}

	// The exact statuses that carry behavior of their own. Everything else
	// in 4xx/5xx falls to the bucket rows below.
	for _, s := range []int{408, 425, 429} {
		add(StatusRuleID(s), Match{Status: s}, ActionRetry)
	}
	for _, s := range []int{401, 403, 404, 405, 409, 422} {
		add(StatusRuleID(s), Match{Status: s}, ActionFallback)
	}
	// The two terminal carve-outs inside 5xx: the provider cannot speak this
	// protocol, and re-asking changes nothing.
	for _, s := range []int{501, 505} {
		add(StatusRuleID(s), Match{Status: s}, ActionTerminal)
	}

	// The buckets. An unlisted 4xx is the request's own fault and would fail
	// identically anywhere; an unlisted 5xx — 520, 529, whatever a broken
	// peer emits — is a provider-side failure worth one bounded re-ask.
	add(StatusClassRuleID(StatusClass4xx), Match{StatusClass: StatusClass4xx}, ActionTerminal)
	add(StatusClassRuleID(StatusClass5xx), Match{StatusClass: StatusClass5xx}, ActionRetry)

	// A transport failure means the path did not deliver — the provider was
	// never asked. Moving to another candidate is the cheapest correct
	// answer.
	//
	// This rule is reached for every transport failure, and it is the ONLY
	// layer that replays: a pool never re-sends a request that may already
	// have reached its member (it stops and hands the failure up), so a
	// send-unknown failure arrives here instead. That is deliberate — the
	// walk is the replay authority, and the operator's policy is where the
	// decision belongs — but it means a chain whose candidates share an
	// upstream base-url does re-send, by this rule. Narrow the matrix if
	// that is not what a given deployment wants.
	for _, c := range []TransportClass{
		TransportClassConnection, TransportClassTimeout,
		TransportClassProxyConnect, TransportClassProxyAuth,
	} {
		add(TransportClassRuleID(c), Match{Class: FailureTransport, TransportClass: c}, ActionFallback)
	}
	for _, c := range []string{
		CauseConnectionRefused, CauseTLS, CauseDial,
		CauseNetworkTimeout, CauseDeadlineExceeded,
		CauseProxyConnect, CauseProxyTimeout, CauseProxyAuth,
		CauseNoEligibleEndpoint,
	} {
		add(TransportCauseRuleID(c), Match{Class: FailureTransport, TransportCause: c}, ActionFallback)
	}

	// A response that arrived but cannot be used is not an answer: no
	// candidate has spoken yet, so the same one gets one bounded re-ask and
	// the walk then falls back.
	for _, c := range []string{
		ProtocolInvalidResponse, ProtocolOversizedResponse,
		ProtocolBodyReadFailed, ProtocolBodyTimeout,
	} {
		add(ProtocolRuleID(c), Match{Class: FailureProtocol, ProtocolCause: c}, ActionRetry)
	}

	// The caller's own cancellation and deadline. These rows are declarative
	// — the engine hard-stops on the class before it consults the matrix —
	// and a configuration that makes either anything but terminal is
	// rejected. They exist so the vocabulary is complete and so no default
	// action can ever be reached by a caller failure.
	add(CallerRuleID(CallerCanceled), Match{Class: FailureCaller, CallerCause: CallerCanceled}, ActionTerminal)
	add(CallerRuleID(CallerDeadline), Match{Class: FailureCaller, CallerCause: CallerDeadline}, ActionTerminal)

	// Every key of the candidate's credential pool is cooling from earlier
	// upstream 429s, so the attempt was never dialed and no exchange was
	// consumed. The default is a retry whose wait is the cooldown's
	// remainder — it rides Observation.RetryAfter through the same
	// raise/cap machinery as an upstream Retry-After — and the candidate's
	// retry budget bounds how long the walk keeps re-asking before
	// on-exhausted takes over. This row does NOT decide rotation: which key
	// the next attempt carries is acquisition's job alone.
	add(CredentialCauseRuleID(CredentialCooldown), Match{Class: FailureCredential, CredentialCause: CredentialCooldown}, ActionRetry)

	m, err := NewMatrix(rules, ActionTerminal)
	if err != nil {
		// Unreachable: the rows above are the shipped data and the package's
		// own tests build this matrix. A panic here would be a build-time
		// defect, not a configuration error, so it must not look like one.
		panic("recovery: the default matrix is invalid: " + err.Error())
	}
	return Policy{
		Matrix: m,
		Retry: RetryPolicy{
			MaxRetries: DefaultMaxRetries,
			MaxElapsed: DefaultRetryMaxElapsed,
			Backoff: BackoffPolicy{
				Initial: DefaultBackoffInitial,
				Max:     DefaultBackoffMax,
				Jitter:  DefaultBackoffJitter,
			},
			OnExhausted: ActionFallback,
		},
		Fallback: FallbackPolicy{
			Enabled:       true,
			MaxCandidates: DefaultFallbackMaxCandidates,
			OnExhausted:   ActionTerminal,
		},
		Budget: BudgetPolicy{
			Request:   Envelope{MaxExchanges: DefaultRequestMaxExchanges, MaxElapsed: DefaultRequestMaxElapsed},
			Candidate: Envelope{MaxExchanges: DefaultCandidateMaxExchanges, MaxElapsed: DefaultCandidateMaxElapsed},
		},
		RetryAfter: RetryAfterPolicy{
			Enabled:  true,
			Mode:     RetryAfterMax,
			MaxDelay: DefaultRetryAfterMaxDelay,
		},
	}
}
