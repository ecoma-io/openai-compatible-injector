package proxy

import (
	"errors"
	"io"
	"slices"

	"openai-compatible-injector/internal/memlimit"
)

// errBufferRefused reports that the process-wide buffering budget could not
// fund the next block of a buffer this request was filling. It is a LOCAL
// refusal: nothing was dialed for it, no upstream is at fault, and it is
// answered with the canonical 503 envelope at its two call sites — never
// through the recovery matrix, which decides what a FAILED ATTEMPT means and
// has no row for "this process declined to hold the bytes".
var errBufferRefused = errors.New("buffering capacity exhausted")

// Admission blocks. A buffer reserves as it grows instead of reserving its
// cap up front, because the cap is what a request MAY hold and almost no
// request holds it: reserving 64 MiB to read a 2 KiB prompt would let the
// budget admit a handful of concurrent requests and refuse everything else,
// which would convert a memory ceiling into a concurrency ceiling nobody
// configured. Reserving what is being read keeps the ceiling honest — an
// ordinary request costs one small block — while a request that really does
// grow towards the cap pays for it block by block, and the refusal lands
// exactly on the requests that were about to hold the memory.
const (
	// admissionBlockStart is the first block a buffer reserves: enough for
	// an ordinary request body or upstream answer many times over, and
	// small enough that the process-wide budget admits thousands of them.
	admissionBlockStart int64 = 64 << 10 // 64 KiB
	// admissionBlockMax caps how far a reservation steps up in one round,
	// which is also the most a buffer can over-reserve at end of input: a
	// grown buffer's last block is reserved but only partly used, so the
	// reservation carries at most this much slack. A maximum-size buffer
	// therefore costs about seventy reservations, none larger than this.
	admissionBlockMax int64 = 1 << 20 // 1 MiB
)

// admission tracks the process-wide bytes one buffer of one request is
// holding. Every reservation is given back exactly once by release, which is
// idempotent so that a path which releases its buffer the moment it discards
// it (a failed attempt) and the request-scoped defer that is the backstop
// can both call it without double-counting.
//
// It is not safe for concurrent use, and does not need to be: one buffer is
// read by one goroutine, and the release is always on that goroutine's
// unwind — a panic included.
type admission struct {
	budget *memlimit.Budget
	held   int64
}

// reserve asks the budget for n more bytes for this buffer, reporting
// whether it could be met. A false is the refusal the caller must act on:
// the bytes it already holds are still held, and release returns them.
func (a *admission) reserve(n int64) bool {
	if n <= 0 {
		return true
	}
	if !a.budget.Acquire(n) {
		return false
	}
	a.held += n
	return true
}

// release gives back everything this buffer holds. Calling it twice, or on a
// buffer that was never filled, is a no-op rather than a second release of
// the same bytes — the budget's own clamp would absorb that, but a caller
// should not have to rely on it to make a double release harmless.
func (a *admission) release() {
	if a.held == 0 {
		return
	}
	a.budget.Release(a.held)
	a.held = 0
}

// readAllAdmitted reads r to EOF, reserving process-wide budget as the
// buffer grows, and returns the bytes read. It is the admitted counterpart
// of io.ReadAll, and it differs from it in exactly one way: a reservation
// that cannot be met ends the read with errBufferRefused rather than
// continuing to grow.
//
// The source is expected to be bounded by its caller — the request body by
// http.MaxBytesReader, the buffered answer by io.LimitReader — so this
// reader can never be the thing that lets an unbounded peer pin memory. Its
// own bound is the budget.
//
// Error and byte-count semantics match io.ReadAll: a complete read returns
// the bytes and no error, and a read that fails returns no bytes and the
// error. EOF in the middle of a block is the ordinary end of input, not a
// failure.
//
// The reservation is an accounting bound on the traffic this process
// buffers, not a byte-exact map of the heap: the block that is reserved is
// the block that will be filled, but Go's own slice growth may round a
// block's allocation up, and the last block is reserved whole however little
// of it the input uses.
func readAllAdmitted(r io.Reader, a *admission) ([]byte, error) {
	var buf []byte
	block := admissionBlockStart
	for {
		if !a.reserve(block) {
			return nil, errBufferRefused
		}
		old := len(buf)
		buf = slices.Grow(buf, int(block))[:old+int(block)]
		n, err := io.ReadFull(r, buf[old:])
		buf = buf[:old+n]
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return buf, nil
			}
			return nil, err
		}
		if block < admissionBlockMax {
			block *= 2
			if block > admissionBlockMax {
				block = admissionBlockMax
			}
		}
	}
}
