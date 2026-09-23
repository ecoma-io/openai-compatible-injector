// Package recovery owns the provider recovery policy: what a failed upstream
// attempt MEANS (the matrix), how often and how long the same candidate may be
// re-asked (retry), when the walk moves to the next candidate (fallback), and
// the absolute envelope of real outbound exchanges one request may spend
// (budget).
//
// The package is deliberately free of I/O and of HTTP. It evaluates facts the
// caller supplies — one Observation — against an immutable, already-resolved
// Policy and returns a Decision. It never sleeps, dials, writes a response,
// reads a pool, or logs. The proxy handler owns execution; the transport
// package owns the wire.
//
// The three layers that make a policy are separated on purpose:
//
//   - the MATRIX maps one failure to retry / fallback / terminal. It is
//     data-driven, normalized into stable rules, and evaluated under a
//     deterministic precedence order.
//   - the RETRY, FALLBACK, RETRY-AFTER and BUDGET policies size the actions
//     the matrix chose. They never change which action a failure maps to.
//   - the ENGINE combines the two with the request's live state (attempt
//     count, elapsed windows, exchange envelope, caller context) into the
//     decision the handler executes.
//
// Four invariants are code-owned and cannot be weakened by configuration,
// because a configuration that could weaken them would turn a client
// disconnect into upstream traffic, or a committed response into a retried
// one:
//
//   - a committed response is terminal: once the first client-visible byte
//     exists, the answer is final, whatever the matrix says about the failure
//     that follows it;
//   - a caller cancellation or an expired caller deadline is terminal, and is
//     decided from the request context rather than from any rule;
//   - the exchange envelope is absolute: no retry, fallback, or dial happens
//     once either the request or the candidate envelope is spent;
//   - a policy is immutable once resolved, and binds to the request's config
//     snapshot, so a reload can never reshape in-flight decisions.
package recovery
