// The fault-injection harness and the execution fingerprint (S1 + S2 of
// docs/design/refactor-roadmap.md §2).
//
// A scripted upstream that can fail in a chosen way at a chosen point of a
// chosen attempt, and a fingerprint of what a request observably did. Between
// them they make the recovery matrix testable from a script instead of only
// from the outside, and they make a refactor diffable against "before" rather
// than argued from a diff.
//
// It is built on the EXPORTED seams — a refactor safety net that had to be
// white-box would be a safety net that dies with the first extraction:
//
//	NewHandler         internal/proxy/handler.go
//	transport.Doer     internal/transport/transport.go
//	transport.Resolver internal/transport/transport.go
//	transport.Executor internal/transport/transport.go
//	usage.Ingest       internal/usage/pipeline.go
//	credential.Registry -> Pool
//	config.LoadRuntime / NewStore
//
// It lives INSIDE package proxy rather than beside it so it can also reach
// the unexported clock seams — retryClock, retryJitterDraw, retryWait
// (internal/proxy/retryafter.go) — which are the only way to make a retry
// wait deterministic. A harness outside the package could not stub them, and
// a wall-clock wait is a flaky test, not a test.
//
// Everything here is _test.go. A safety net that shipped in the binary would
// be a safety net that could be reached in production.
package proxy

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"syscall"
	"time"

	"openai-compatible-injector/internal/transport"
)

// Phase names the wire phase a Step occupies. A consumed Step's phase is
// recorded, and the ExecutionFingerprint reads it to reconstruct the path.
type Phase string

const (
	// PhaseDial is a response that exists. Zero value.
	PhaseDial Phase = "dial"
	// PhaseDialFail is a failure before any connection.
	PhaseDialFail Phase = "dial_fail"
	// PhaseDialTimeout is a dial that blackholes — accepted nothing, sent
	// nothing, and hit a deadline.
	PhaseDialTimeout Phase = "dial_timeout"
	// PhaseBody is a response whose BODY misbehaves after the headers.
	PhaseBody Phase = "body"
)

// Step is one scripted dial outcome. It is a tagged union over the wire
// phases; the zero Step is a clean 200 with an empty JSON body, so a
// mis-scripted test fails loudly rather than passing by accident.
type Step struct {
	Phase Phase

	// Err is returned from Do in a dial phase. nil in a dial phase is
	// replaced by a default of the right SHAPE, because the SHAPE is what
	// transport.ClassifyAttempt (internal/transport/errors.go:303) reads —
	// never the message.
	Err error

	// Status / Header / Body describe the response. Status 0 means 200; an
	// empty Content-Type means application/json.
	Status int
	Header http.Header
	// CT is shorthand for the Content-Type header; it wins over Header's.
	CT   string
	Body string

	// Chunks scripts the body read-by-read when Phase == PhaseBody (or when
	// Chunks is set on any phase). nil means an in-memory body of Body.
	Chunks []Chunk

	// Gate, when non-nil, runs before the step is served. A non-nil return
	// becomes Do's error. This is how "cancel the client at this exact
	// point" is expressed with no timer and therefore no race.
	Gate func(req *http.Request) error

	// Handoff calls transport.HandoffStream on the response before
	// returning it, releasing the exchange window. A committed SSE stream
	// REQUIRES this in a real deployment (see internal/transport/transport.go:412),
	// and a harness that omits it silently tests a different code path.
	Handoff bool
}

// Chunk is one Read of a scripted body plus the terminal action ending it.
type Chunk struct {
	// Data is what this Read returns. Empty Data with a non-nil End is a
	// read returning (0, End) — the stall / reset-now shape.
	Data []byte
	// End is returned alongside Data. io.EOF = clean end; ReadReset() = the
	// peer reset mid-body; any other error is a body read failure.
	End error
	// Delay parks this Read. Only for tests that need a real timer to fire;
	// the FAULT itself is never timing-dependent.
	Delay time.Duration
	// After fires once, asynchronously, right after this chunk is returned.
	// It exists solely to produce a cancellation mid-body. The READ ITSELF
	// never depends on it having run.
	After func()
}

// ---------------------------------------------------------------------------
// Default error shapes — the ones the classifier actually reads.
// ---------------------------------------------------------------------------

// ErrTruncated is the "reset mid-body" signal: an error that is not io.EOF,
// so a reader can tell a cut from a clean end. Real RSTs arrive in this shape.
var ErrTruncated = errors.New("hro: connection reset by peer")

// ReadReset returns the exact shape a mid-body reset takes on the wire:
// *net.OpError{Op:"read"} wrapping ECONNRESET. transport.sendStateOf
// (errors.go:263-270) maps any op other than "dial"/"proxyconnect" to
// SendStateUnknown, so this is the shape that must NOT be replayed onto
// another egress member.
func ReadReset() error {
	return &net.OpError{Op: "read", Net: "tcp", Err: syscall.ECONNRESET}
}

// DialRefused returns the shape of a connection that was never established:
// *net.OpError{Op:"dial"} wrapping ECONNREFUSED. This is the ONE transport
// failure sendStateOf (errors.go:264-270) maps to SendStateNotSent, i.e. the
// only one the egress pool may replay. Paired with ReadReset it pins the
// asymmetry the whole fallback gate rests on.
func DialRefused() error {
	return &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}
}

type timeoutErr struct{}

func (timeoutErr) Error() string   { return "hro: i/o timeout" }
func (timeoutErr) Timeout() bool   { return true }
func (timeoutErr) Temporary() bool { return true }

// DialBlackhole returns a dial that times out. Same Op as DialRefused, so
// also SendStateNotSent, but Classify (errors.go:145-148) checks
// net.Error.Timeout() first, so it buckets as ClassTimeout with cause
// network_timeout — the shape the matrix keys on for
// `transport-class: timeout`.
func DialBlackhole() error {
	return &net.OpError{Op: "dial", Net: "tcp", Err: timeoutErr{}}
}

// ---------------------------------------------------------------------------
// Upstream: a scripted transport.Doer.
// ---------------------------------------------------------------------------

// Call is one consumed Step.
type Call struct {
	Phase Phase
	// Index is 1-based, within its own upstream.
	Index int
	// Upstream is the operator-chosen name of the upstream that served this
	// dial: the egress identity, as an operator would name it.
	Upstream string
	URL      string
	// SentBody is the exact body this upstream received — the post-injection,
	// post-rewrite request. Byte equality on it is how an injection or
	// model-rename regression is caught.
	SentBody string
	// Method and RequestHeader are the outbound request as sent. The header
	// carries the composed upstream credential, so a test reads it only
	// through a fingerprint field that records the configured KEY id.
	Method        string
	RequestHeader http.Header
	// Err is the error this dial returned, if any.
	Err error
}

// Upstream implements transport.Doer. Safe for concurrent use.
type Upstream struct {
	mu     sync.Mutex
	steps  []Step
	calls  []Call
	strict bool
	name   string
}

var _ transport.Doer = (*Upstream)(nil)

// New builds a scripted Doer. steps are consumed in order; the last one
// repeats once the script is exhausted, unless strict is set — in which case
// an extra dial returns ErrOverrun and the walk sees a transport failure,
// making "the walk made exactly N dials" an assertion rather than a hope.
func New(name string, strict bool, steps ...Step) *Upstream {
	return &Upstream{name: name, strict: strict, steps: steps}
}

// ErrOverrun is returned when a strict script is exhausted.
var ErrOverrun = errors.New("hro: scripted upstream overrun")

// Name is the operator-chosen label the fingerprint records.
func (u *Upstream) Name() string { return u.name }

func (u *Upstream) Do(req *http.Request) (*http.Response, error) {
	sent := ""
	if req.Body != nil {
		b, _ := io.ReadAll(req.Body)
		sent = string(b)
	}
	u.mu.Lock()
	i := len(u.calls)
	var step Step
	switch {
	case i < len(u.steps):
		step = u.steps[i]
	case u.strict:
		u.calls = append(u.calls, Call{Phase: "overrun", Index: i + 1, Upstream: u.name, URL: req.URL.String(),
			SentBody: sent, Method: req.Method, RequestHeader: req.Header.Clone(), Err: ErrOverrun})
		u.mu.Unlock()
		return nil, ErrOverrun
	default:
		step = u.steps[len(u.steps)-1]
	}
	u.calls = append(u.calls, Call{Phase: step.Phase, Index: i + 1, Upstream: u.name, URL: req.URL.String(),
		SentBody: sent, Method: req.Method, RequestHeader: req.Header.Clone()})
	u.mu.Unlock()

	if step.Gate != nil {
		if err := step.Gate(req); err != nil {
			return nil, err
		}
	}
	switch step.Phase {
	case PhaseDialFail:
		return nil, orDefault(step.Err, DialRefused())
	case PhaseDialTimeout:
		return nil, orDefault(step.Err, DialBlackhole())
	case PhaseBody:
		resp := buildResponse(req, step)
		resp.Body = &scriptedBody{steps: step.Chunks, fallback: step.Body}
		if step.Handoff {
			transport.HandoffStream(resp)
		}
		return resp, nil
	default:
		resp := buildResponse(req, step)
		if step.Chunks != nil {
			resp.Body = &scriptedBody{steps: step.Chunks, fallback: step.Body}
		}
		if step.Handoff {
			transport.HandoffStream(resp)
		}
		return resp, nil
	}
}

func orDefault(err, def error) error {
	if err == nil {
		return def
	}
	return err
}

func buildResponse(req *http.Request, step Step) *http.Response {
	h := http.Header{}
	for k, v := range step.Header {
		h[k] = append([]string(nil), v...)
	}
	ct := step.CT
	if ct == "" {
		ct = h.Get("Content-Type")
	}
	if ct == "" {
		ct = "application/json"
	}
	h.Set("Content-Type", ct)
	status := step.Status
	if status == 0 {
		status = http.StatusOK
	}
	return &http.Response{
		StatusCode: status,
		Header:     h,
		Body:       io.NopCloser(strings.NewReader(step.Body)),
		Request:    req,
	}
}

// Calls is the recorded dial log.
func (u *Upstream) Calls() []Call {
	u.mu.Lock()
	defer u.mu.Unlock()
	return append([]Call(nil), u.calls...)
}

// Dials is how many dials arrived.
func (u *Upstream) Dials() int {
	u.mu.Lock()
	defer u.mu.Unlock()
	return len(u.calls)
}

// ---------------------------------------------------------------------------
// scriptedBody
// ---------------------------------------------------------------------------

type scriptedBody struct {
	mu       sync.Mutex
	steps    []Chunk
	fallback string
	idx      int
	done     bool
}

func (b *scriptedBody) Read(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.done {
		return 0, io.EOF
	}
	var c Chunk
	switch {
	case b.idx < len(b.steps):
		c = b.steps[b.idx]
	case b.steps == nil:
		c = Chunk{Data: []byte(b.fallback), End: io.EOF}
	default:
		c = b.steps[len(b.steps)-1]
	}
	b.idx++
	if len(c.Data) == 0 && c.End == nil {
		if b.idx > len(b.steps) {
			b.done = true
			return 0, io.EOF
		}
		// A gap with neither data nor end: emit an empty successful read so
		// a caller looping on Read makes progress instead of spinning.
		return 0, nil
	}
	if c.Delay > 0 {
		time.Sleep(c.Delay)
	}
	if c.After != nil {
		go c.After()
	}
	if len(c.Data) == 0 {
		b.done = true
		return 0, c.End
	}
	n := copy(p, c.Data)
	if c.End != nil {
		b.done = true
		return n, c.End
	}
	return n, nil
}

func (b *scriptedBody) Close() error { return nil }

// ---------------------------------------------------------------------------
// Cancel helpers — the deterministic ClientCancel.
// ---------------------------------------------------------------------------

// CancelOnChunk returns an After hook that fires cancel once the body has
// served n chunks. The cancellation is triggered by OBSERVABLE PROGRESS,
// never by a timer, so it never races: the handler is always inside a Read
// when the cancel lands.
func CancelOnChunk(cancel context.CancelFunc, n int) func() {
	var mu sync.Mutex
	fired := false
	return func() {
		mu.Lock()
		defer mu.Unlock()
		if fired {
			return
		}
		if n > 0 {
			n--
			if n > 0 {
				return
			}
		}
		fired = true
		cancel()
	}
}

// CancelBefore is a Step.Gate that cancels the caller before the dial is
// served, producing a pre-dial caller cancellation.
func CancelBefore(cancel context.CancelFunc) func(*http.Request) error {
	return func(*http.Request) error {
		cancel()
		return context.Canceled
	}
}
