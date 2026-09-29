package proxy

import "io"

// upstreamProgressReader records progress at the ONLY boundary that can prove
// it: a successful read from the upstream response body. It deliberately does
// not observe client writes, event dispatches, rewrites or the keep-alive
// ping. Those all happen after the peer has spoken (or without it), and using
// any of them to reset a maximum-silence bound would let a slow client or this
// proxy's own traffic conceal a dead upstream.
//
// `n > 0` is progress even when Read also returns an error. Go permits a reader
// to return its final bytes together with io.EOF; those bytes reached this
// proxy and must get the same treatment as any other received byte.
type upstreamProgressReader struct {
	io.Reader
	progress func()
}

func (r upstreamProgressReader) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	if n > 0 && r.progress != nil {
		r.progress()
	}
	return n, err
}
