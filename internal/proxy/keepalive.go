package proxy

import (
	"bytes"
	"io"
	"sync"
	"time"
)

// ssePing is the keep-alive heartbeat: one SSE comment event. A line
// starting with ':' is a comment that every spec-compliant SSE parser
// ignores, so the heartbeat is invisible to clients while still counting
// as traffic for the intermediaries that would otherwise cut an idle
// stream (a Cloudflare-proxied hostname kills a silent HTTP/2 stream
// after ~125s).
var ssePing = []byte(": ping\n\n")

// pingWriter serializes SSE relay writes and keep-alive pings onto one
// client writer. The relay loop (CopySSE) and the heartbeat goroutine are
// two writers to a single ResponseWriter, so every byte and every flush
// goes through the same mutex. The writer also tracks where the stream
// sits — the time of the last byte written and whether the last relayed
// line completed an event — which is what decides whether a ping is legal
// right now: never inside a partial event, only at an event boundary (or
// before the first event), and only after the configured silence.
type pingWriter struct {
	// mu guards every field below and every client write or flush: the
	// relay and the heartbeat share this writer, and net/http allows only
	// the handler to write — through this serializer they behave as one.
	mu       sync.Mutex
	dst      io.Writer
	flush    func()
	interval time.Duration
	// last is the time of the last byte handed to dst, ping or relay
	// alike; any forwarded upstream byte resets the silence clock.
	last time.Time
	// boundary reports whether the stream sits at an event boundary — the
	// only legal injection point. True before the first byte: the start of
	// a stream precedes every event.
	boundary bool
	finished bool
	pings    int

	// stop/exited carry the heartbeat goroutine's lifecycle; they are set
	// once by start, before the goroutine exists, and read after
	// stopAndWait's channel operations — no race window.
	stop   chan struct{}
	exited chan struct{}
}

// newPingWriter builds the heartbeat writer for one streamed response.
// now seeds the silence clock: the caller constructs it right after the
// response headers are committed, which is when the heartbeat's window
// opens. interval comes from the request's snapshot and must be positive.
func newPingWriter(dst io.Writer, flush func(), interval time.Duration, now time.Time) *pingWriter {
	return &pingWriter{dst: dst, flush: flush, interval: interval, last: now, boundary: true}
}

// start spawns the heartbeat goroutine. One goroutine per streamed
// request; stopAndWait joins it, so nothing outlives the handler.
func (p *pingWriter) start() {
	p.stop = make(chan struct{})
	p.exited = make(chan struct{})
	go func() {
		defer close(p.exited)
		runKeepAlive(p, p.stop)
	}()
}

// stopAndWait marks the stream over — no ping may follow the terminal
// event — and joins the heartbeat goroutine, so the handler never returns
// with a writer still in flight.
func (p *pingWriter) stopAndWait() {
	p.mu.Lock()
	p.finished = true
	p.mu.Unlock()
	close(p.stop)
	<-p.exited
}

// Write relays one relay-produced buffer to the client. CopySSE writes
// exactly one line per call, terminator included, so the tail of the
// buffer tells whether the stream now sits at an event boundary.
func (p *pingWriter) Write(b []byte) (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	n, err := p.dst.Write(b)
	if n > 0 {
		p.last = time.Now()
		p.boundary = endsWithBlankLine(b[:n])
		// Stop eligibility as soon as the terminal marker reaches the
		// client, rather than waiting for the next upstream read to report
		// EOF. That closes the race where a ticker could otherwise acquire
		// the mutex after [DONE]/response.completed was forwarded but before
		// CopySSE returns, and append a forbidden trailing ping.
		if isTerminalSSELine(b[:n]) {
			p.finished = true
		}
	}
	return n, err
}

// Flush flushes the client writer under the same lock as writes, so a
// heartbeat ping and a relay flush never interleave mid-event.
func (p *pingWriter) Flush() {
	p.mu.Lock()
	p.flush()
	p.mu.Unlock()
}

// maybePing writes one keep-alive comment when the client has now been
// silent for the configured interval and the stream sits at a boundary.
// now is the tick's own time, not a fresh clock read, so mutex wait is
// never counted as silence. It reports whether the heartbeat should keep
// running: a failed ping write means the client is gone, and the relay
// will observe the same on its next write.
func (p *pingWriter) maybePing(now time.Time) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.finished || !p.boundary || now.Sub(p.last) < p.interval {
		return true
	}
	if _, err := p.dst.Write(ssePing); err != nil {
		return false
	}
	p.pings++
	p.last = now
	p.flush()
	return true
}

// pingCount reports how many keep-alive comments were written — access-log
// metadata only.
func (p *pingWriter) pingCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.pings
}

// runKeepAlive pumps the heartbeat until stop closes, the client goes
// away, or the stream finishes. The ticker period equals the interval, so
// during sustained silence one ping goes out per tick, and after any relay
// write the next tick's silence check fails until the interval has truly
// elapsed again.
func runKeepAlive(p *pingWriter, stop <-chan struct{}) {
	t := time.NewTicker(p.interval)
	defer t.Stop()
	for {
		select {
		case <-stop:
			return
		case now := <-t.C:
			if !p.maybePing(now) {
				return
			}
		}
	}
}

// endsWithBlankLine reports whether b ends with a complete, blank line —
// the event-boundary terminator. CopySSE writes one line per call, so the
// end of the buffer is the whole story: a data or event line leaves the
// stream mid-event (no ping may follow it until the completing blank
// line), a blank line dispatches the event and reopens the boundary.
func endsWithBlankLine(b []byte) bool {
	i := bytes.LastIndexByte(b, '\n')
	if i < 0 {
		return false
	}
	j := bytes.LastIndexByte(b[:i], '\n')
	last := b[j+1 : i]
	return len(last) == 0 || (len(last) == 1 && last[0] == '\r')
}

// isTerminalSSELine reports the two terminal markers this proxy serves.
// Chat's terminal payload is literally [DONE]. Responses identifies the
// terminal envelope with its event name; recognizing it at the event line
// (rather than waiting for its data line or EOF) makes the guarantee
// stronger: no comment can appear in the middle of, or after, the terminal
// event. The bytes remain untouched — this only disables the heartbeat.
func isTerminalSSELine(b []byte) bool {
	content, _ := splitSSELineTerminator(b)
	return bytes.Equal(content, []byte("data: [DONE]")) || bytes.Equal(content, []byte("event: response.completed"))
}
