package transport

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"net/url"
	"os"
	"syscall"
	"testing"
	"time"
)

// TestClassifyBucketsTypedErrors pins the classification seam the pool's
// fallback decision and the handler's error_class log field both read:
// typed proxy errors classify from their TYPE, never from message text.
func TestClassifyBucketsTypedErrors(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want Class
	}{
		{"nil", nil, ClassNone},
		{"canceled", context.Canceled, ClassCanceled},
		{"plain proxy auth", &ProxyAuthError{msg: "socks5: proxy authentication failed"}, ClassProxyAuth},
		{"auth through wrap", wrapped(&ProxyAuthError{msg: "x"}), ClassProxyAuth},
		{"plain proxy connect", &ProxyConnectError{msg: "socks5: general failure"}, ClassProxyConnect},
		{"connect through url.Error wrap", wrapped(&ProxyConnectError{msg: "proxy CONNECT replied 407"}), ClassProxyConnect},
		{"canceled through connect cause", &ProxyConnectError{msg: "socks5: dial proxy", cause: context.Canceled}, ClassCanceled},
		{"deadline exceeded", context.DeadlineExceeded, ClassTimeout},
		{"os deadline", os.ErrDeadlineExceeded, ClassTimeout},
		{"net timeout", &fakeNetError{timeout: true}, ClassTimeout},
		{"op error", &net.OpError{Op: "dial", Err: syscall.ECONNREFUSED}, ClassConnection},
		{"tls-ish generic", errors.New("x509: certificate signed by unknown authority"), ClassConnection},
	}
	for _, tc := range cases {
		if got := Classify(tc.err); got != tc.want {
			t.Errorf("%s: Classify = %v, want %v", tc.name, got, tc.want)
		}
	}
}

// TestClassifyCanceledBeatsTyped ensures a cancellation racing a proxy
// failure classifies as the caller's event: the canceled check comes first,
// and the typed wrapper's Unwrap chain makes the cause reachable.
func TestClassifyCanceledBeatsTyped(t *testing.T) {
	err := &ProxyConnectError{msg: "socks5: dial proxy", cause: &net.OpError{Op: "dial", Err: context.Canceled}}
	if got := Classify(err); got != ClassCanceled {
		t.Errorf("Classify = %v, want canceled", got)
	}
	if !errors.Is(err, context.Canceled) {
		t.Error("Unwrap chain lost the context.Canceled cause")
	}
}

// TestTypedErrorTextsUnchanged pins the log-safety contract: the typed
// errors render exactly the texts the errors carried before typing, so no
// existing log/pin shifts meaning.
func TestTypedErrorTextsUnchanged(t *testing.T) {
	if got := (&ProxyAuthError{msg: "socks5: proxy authentication failed"}).Error(); got != "socks5: proxy authentication failed" {
		t.Errorf("auth text = %q", got)
	}
	if got := (&ProxyConnectError{msg: "socks5: dial proxy", cause: context.DeadlineExceeded}).Error(); got != "socks5: dial proxy: context deadline exceeded" {
		t.Errorf("connect text = %q", got)
	}
	if got := (&ProxyConnectError{msg: "socks5: ttl expired"}).Error(); got != "socks5: ttl expired" {
		t.Errorf("causeless connect text = %q", got)
	}
}

func wrapped(err error) error {
	return &urlErrorShim{err: err}
}

// TestClassifyAttemptCauseTokens pins the context-aware classification the
// handler and pool both read: a LIVE context classifies the error on its own
// terms (canonical class + bounded cause), while a DONE context owns the
// failure whatever the error chain says — caller_canceled or
// caller_deadline_exceeded, always CallerTerminated. Deadline ownership is
// the point: a caller deadline and a transport timer both surface as
// context.DeadlineExceeded, and only the context can tell them apart.
func TestClassifyAttemptCauseTokens(t *testing.T) {
	live := context.Background()
	refused := &net.OpError{Op: "dial", Err: syscall.ECONNREFUSED}
	for _, tc := range []struct {
		name string
		ctx  context.Context
		err  error
		want Failure
	}{
		{"nil error", live, nil, Failure{Class: ClassNone}},
		{"refused", live, refused, Failure{Class: ClassConnection, Cause: CauseConnectionRefused}},
		{"refused through url.Error", live, &url.Error{Op: "Post", URL: "http://x", Err: refused}, Failure{Class: ClassConnection, Cause: CauseConnectionRefused}},
		{"plain dial failure", live, errors.New("dial tcp: i/o timeout"), Failure{Class: ClassConnection, Cause: CauseDial}},
		{"tls verification", live, &tls.CertificateVerificationError{Err: errors.New("x509: unknown authority")}, Failure{Class: ClassConnection, Cause: CauseTLS}},
		{"net timeout", live, &fakeNetError{timeout: true}, Failure{Class: ClassTimeout, Cause: CauseNetworkTimeout}},
		{"context deadline in chain", live, context.DeadlineExceeded, Failure{Class: ClassTimeout, Cause: CauseDeadlineExceeded}},
		{"os deadline in chain", live, os.ErrDeadlineExceeded, Failure{Class: ClassTimeout, Cause: CauseDeadlineExceeded}},
		{"proxy auth", live, &ProxyAuthError{msg: "socks5: proxy authentication failed"}, Failure{Class: ClassProxyAuth, Cause: "proxy_auth"}},
		{"proxy connect", live, &ProxyConnectError{msg: "socks5: general failure"}, Failure{Class: ClassProxyConnect, Cause: CauseProxyConnect}},
		{"proxy connect timeout", live, &ProxyConnectError{msg: "socks5: dial proxy", cause: &fakeNetError{timeout: true}}, Failure{Class: ClassProxyConnect, Cause: CauseProxyTimeout}},
		{"canceled shape, live context", live, context.Canceled, Failure{Class: ClassCanceled}},
		{"caller canceled", canceledCtx(t), errors.New("dial tcp: connection refused"), Failure{Class: ClassCanceled, Cause: CauseCallerCanceled, CallerTerminated: true}},
		{"caller deadline beats timeout shape", deadlineCtx(t), context.DeadlineExceeded, Failure{Class: ClassCanceled, Cause: CauseCallerDeadlineExceeded, CallerTerminated: true}},
		{"caller deadline beats refused", deadlineCtx(t), refused, Failure{Class: ClassCanceled, Cause: CauseCallerDeadlineExceeded, CallerTerminated: true}},
	} {
		got := ClassifyAttempt(tc.ctx, tc.err)
		if got != tc.want {
			t.Errorf("%s: ClassifyAttempt = %+v, want %+v", tc.name, got, tc.want)
		}
	}
}

// urlErrorShim mimics how net/http wraps transport errors (a wrapper with
// Unwrap), without importing net/url's concrete type here.
type urlErrorShim struct{ err error }

func (e *urlErrorShim) Error() string { return "shim: " + e.err.Error() }
func (e *urlErrorShim) Unwrap() error { return e.err }

// fakeNetError is a minimal net.Error for timeout classification.
type fakeNetError struct{ timeout bool }

func (e *fakeNetError) Error() string   { return "fake net error" }
func (e *fakeNetError) Timeout() bool   { return e.timeout }
func (e *fakeNetError) Temporary() bool { return false }

var _ net.Error = (*fakeNetError)(nil)

// TestProxyIdentityHelpers pin the log-facing rendering of endpoints:
// scheme+host only, userinfo stripped, direct stays direct.
func TestProxyIdentityHelpers(t *testing.T) {
	u, err := url.Parse("socks5://user:pass@gw.example:1080")
	if err != nil {
		t.Fatal(err)
	}
	c := Config{Kind: Proxy, ProxyURL: u}
	if got := c.target(); got != "socks5://gw.example:1080" {
		t.Errorf("target = %q", got)
	}
	if got := c.kindName(); got != "socks5" {
		t.Errorf("kindName = %q", got)
	}
	if got := (Config{}).target(); got != "direct" {
		t.Errorf("direct target = %q", got)
	}
}

// TestPoolIdentityIsContentStable pins the reload identity: byte-equal
// policies produce equal identities regardless of construction order or
// pointer, and any policy-affecting change (member endpoint, eligibility,
// strategy, fallback, health) changes it.
func TestPoolIdentityIsContentStable(t *testing.T) {
	u1, _ := url.Parse("socks5://gw.example:1080")
	u2, _ := url.Parse("socks5://gw.example:1080")
	mk := func(px *url.URL, w int, conc int) *Pool {
		return NewPool([]Member{
			{Endpoint: Config{Kind: Proxy, ProxyURL: px}, Streaming: true, Weight: w, MaxConcurrency: conc},
			{Endpoint: Config{}, Streaming: true, Weight: 1},
		}, RoundRobin, FallbackPolicy{Enabled: true, MaxAttempts: 3}, HealthPolicy{Enabled: true, FailureThreshold: 3, Cooldown: 30 * time.Second})
	}
	if mk(u1, 1, 0).Identity() != mk(u2, 1, 0).Identity() {
		t.Error("byte-equal pools disagree on identity")
	}
	if mk(u1, 1, 0).Identity() == mk(u1, 2, 0).Identity() {
		t.Error("weight change kept the identity")
	}
	if mk(u1, 1, 0).Identity() == mk(u1, 1, 5).Identity() {
		t.Error("concurrency change kept the identity")
	}
	other, _ := url.Parse("http://gw.example:1080")
	if mk(u1, 1, 0).Identity() == mk(other, 1, 0).Identity() {
		t.Error("endpoint change kept the identity")
	}
}

// TestKeyCoversPoolKind pins Config.key's pool form: identity-bearing,
// distinct from direct and proxy keys.
func TestKeyCoversPoolKind(t *testing.T) {
	p := NewPool(poolMembers(Config{}, Config{}), RoundRobin, FallbackPolicy{Enabled: true, MaxAttempts: 3}, HealthPolicy{})
	k := Config{Kind: EgressPool, Pool: p}.Key()
	if k == "direct" || len(k) <= len("pool ") {
		t.Errorf("pool key = %q", k)
	}
}
