package inject

// BuildContinuationMessages is the continuation builder the Anthropic
// Messages route selects. It refuses, always.
//
// Post-commitment stream recovery is deliberately DEFERRED on this surface,
// and this function is where the deferral lives rather than in a comment
// beside a call site: a builder that returns a typed refusal cannot decay
// into an accidental hop that re-asks the upstream in the chat dialect and
// then fed those bytes into a relay already emitting Anthropic events.
//
// The reason is unsupported_shape and not something like no_op, because the
// refusal is not about the assembled request being identical to the last
// one — it is that this surface's continuation cannot be expressed without
// guessing. The Messages route serves a translated stream: the prefix the
// accumulator observed is the UPSTREAM's chat dialect, while the client
// holds an Anthropic event stream. Splicing that prefix into a chat
// continuation and re-translating the answer mid-stream would have to
// synthesise the block boundaries, stop_reason and terminal event the
// original translation never got to write — precisely the guessing
// BuildContinuationChat's own refusal exists to avoid, now with a second
// dialect layered on top.
//
// Refusing is the correct, expected outcome: a truncated Messages stream
// leaves the client with exactly the unterminated stream it would have had
// anyway, and the caller logs stream_recovery_failed with unsafe_reason
// unsupported_shape and dials nothing.
//
// The arguments are read for nothing at all — orig and prefix are accepted
// so the signature matches its two siblings (the route's buildCont selector
// is a single function value), and so a later implementation can take the
// usual inputs without a call-site change.
func BuildContinuationMessages(orig []byte, prefix string) ([]byte, error) {
	return nil, refuse(refusalUnsupportedShape)
}
