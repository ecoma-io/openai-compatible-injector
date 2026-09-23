package recovery

import (
	"errors"
	"fmt"
)

// Action is what one failed attempt means for the walk. It is the whole
// vocabulary of the matrix: every rule, every filter default, and every
// on-exhausted clause resolves to exactly these three.
type Action int

const (
	// ActionTerminal ends the walk: the last received answer — or the
	// synthesized failure when no candidate ever answered — becomes the
	// client's response.
	//
	// It is the ZERO VALUE on purpose. An Action that was never set, or a
	// policy field that a build path forgot to fill, must mean "stop" and
	// never "ask the upstream again": the safe direction for a recovery
	// engine is the quiet one.
	ActionTerminal Action = iota
	// ActionFallback moves to the next provider candidate immediately —
	// there is no wait between candidates.
	ActionFallback
	// ActionRetry re-asks the SAME candidate after the policy's bounded
	// delay.
	ActionRetry
)

// String renders the action as the config-facing and log-facing token. The
// tokens are the closed disposition vocabulary the evidence events already
// carry: retry, fallback, terminal.
func (a Action) String() string {
	switch a {
	case ActionRetry:
		return "retry"
	case ActionFallback:
		return "fallback"
	default:
		return "terminal"
	}
}

// ParseAction reads an action token. Errors are fixed text: the input is
// operator-supplied and error text reaches logs verbatim, so it is never
// echoed.
func ParseAction(s string) (Action, error) {
	switch s {
	case "terminal":
		return ActionTerminal, nil
	case "fallback":
		return ActionFallback, nil
	case "retry":
		return ActionRetry, nil
	default:
		return ActionTerminal, errors.New("action must be one of retry, fallback, terminal")
	}
}

// errActionNotAllowed is the shared rejection for a position where only one
// action is meaningful — a caller rule that is not terminal, a fallback
// on-exhausted that is not terminal. It names the position, never the input.
func errActionNotAllowed(position, allowed string) error {
	return fmt.Errorf("%s must be %s", position, allowed)
}
