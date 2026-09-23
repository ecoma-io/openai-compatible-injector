package transport

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"
)

// poolCfg builds a two-member pool config over direct endpoints for
// registry-level tests.
func poolCfg(strategy Strategy, maxAttempts int, conc int) Config {
	return Config{Kind: EgressPool, Pool: NewPool(
		[]Member{
			{Endpoint: Config{}, Streaming: true, Weight: 1, MaxConcurrency: conc},
			{Endpoint: Config{}, Streaming: true, Weight: 1},
		},
		strategy,
		FallbackPolicy{Enabled: true, MaxAttempts: maxAttempts},
		HealthPolicy{Enabled: true, FailureThreshold: 3, Cooldown: 30 * time.Second},
	)}
}

// TestRegistrySharesPoolDoerByIdentity pins the identity contract at the
// registry: two byte-equal pool configurations (distinct pointers,
// separately constructed) resolve to ONE doer with ONE shared state, so an
// unchanged reload keeps scheduler and health state warm.
func TestRegistrySharesPoolDoerByIdentity(t *testing.T) {
	r := NewRegistry()
	a := poolCfg(RoundRobin, 3, 0)
	b := poolCfg(RoundRobin, 3, 0) // equal content, fresh *Pool

	if r.Doer(a) != r.Doer(b) {
		t.Error("equal-content pools resolved to two doers")
	}
	pd := r.Doer(a).(*poolDoer)
	pd.st.begin() // simulate an in-flight request
	if st := r.pools[pd.pool.Identity()]; st != pd.st {
		t.Error("registry lost the shared pool state")
	}
	pd.st.end()
}

// TestRegistryChangedPolicyGetsFreshState pins the other half: a policy
// change (here the fallback cap) yields a new identity, a new doer, and a
// fresh state — old health never leaks into the new policy.
func TestRegistryChangedPolicyGetsFreshState(t *testing.T) {
	r := NewRegistry()
	a := poolCfg(RoundRobin, 3, 0)
	b := poolCfg(RoundRobin, 4, 0) // one knob differs

	first := r.Doer(a).(*poolDoer)
	r.Retain([]Config{b})
	second := r.Doer(b).(*poolDoer)
	if first == second {
		t.Fatal("changed pool policy kept the old doer")
	}
	if first.st == second.st {
		t.Error("changed pool policy kept the old state")
	}
	if _, ok := r.pools[first.pool.Identity()]; ok {
		t.Error("unleased old pool state survived Retain")
	}
}

// TestRegistryRetainDefersPoolTeardownToLease pins the lease protection:
// a pool absent from the Retain set but leased by in-flight work is not
// torn down until the lease releases — then the deferred teardown runs.
func TestRegistryRetainDefersPoolTeardownToLease(t *testing.T) {
	r := NewRegistry()
	cfg := poolCfg(RoundRobin, 3, 0)
	pd := r.Doer(cfg).(*poolDoer)

	pd.st.begin() // the request is executing
	r.Retain(nil) // a reload that dropped the pool

	if st, ok := r.pools[pd.pool.Identity()]; !ok || st != pd.st {
		t.Fatal("leased pool state was torn down under the request")
	}

	pd.st.end() // the request finishes; teardown fires

	if _, ok := r.pools[pd.pool.Identity()]; ok {
		t.Error("released pool state was never pruned")
	}
	if _, ok := r.doers[cfg.Key()]; ok {
		t.Error("released pool doer entry was never pruned")
	}
}

// TestRegistryUnleasedPoolTornDownImmediately pins the no-lease fast path:
// a dropped, idle pool is pruned by the same Retain that noticed it.
func TestRegistryUnleasedPoolTornDownImmediately(t *testing.T) {
	r := NewRegistry()
	cfg := poolCfg(RoundRobin, 3, 0)
	pd := r.Doer(cfg).(*poolDoer)
	_ = pd

	r.Retain(nil)
	if len(r.pools) != 0 {
		t.Errorf("pools = %d entries after dropping every transport", len(r.pools))
	}
	if len(r.doers) != 0 {
		t.Errorf("doers = %d entries after dropping every transport", len(r.doers))
	}
}

// TestRegistryPoolAndPlainShareMemberClient pins the connection-sharing
// property: a pool member and a standalone transport of the same endpoint
// resolve to the same client, so pool traffic warms the same pool a plain
// transport would use.
func TestRegistryPoolAndPlainShareMemberClient(t *testing.T) {
	r := NewRegistry()
	px, err := url.Parse("socks5://127.0.0.1:9")
	if err != nil {
		t.Fatal(err)
	}
	proxy := Config{Kind: Proxy, ProxyURL: px}
	pool := Config{Kind: EgressPool, Pool: NewPool(
		[]Member{{Endpoint: proxy, Streaming: true, Weight: 1}},
		RoundRobin, FallbackPolicy{Enabled: true, MaxAttempts: 2}, HealthPolicy{Enabled: true, FailureThreshold: 3, Cooldown: time.Second},
	)}

	pd := r.Doer(pool).(*poolDoer)
	if pd.st.members[0].client != r.Doer(proxy) {
		t.Error("pool member client is not the registry's shared client")
	}

	// The egress closure retains both: pool + member stay after Retain.
	r.Retain([]Config{pool, proxy})
	if r.Doer(proxy) != pd.st.members[0].client {
		t.Error("Retain on the closure evicted the shared member client")
	}
}

// blockingEndpoint is a member client whose Do blocks until released — the
// in-flight half of the reload race test.
type blockingEndpoint struct {
	block  chan struct{}
	called chan struct{}
	once   sync.Once
}

func newBlockingEndpoint() *blockingEndpoint {
	return &blockingEndpoint{block: make(chan struct{}), called: make(chan struct{})}
}

func (b *blockingEndpoint) Do(*http.Request) (*http.Response, error) {
	b.once.Do(func() { close(b.called) })
	<-b.block
	return nil, errors.New("context canceled")
}

func (b *blockingEndpoint) CloseIdleConnections() {}

// TestRegistryExecuteSurvivesReload pins the reload invariant end to end:
// an Execute in flight when the pool is dropped and evicted still runs to
// completion on its captured state, and the state is pruned only after the
// lease releases.
func TestRegistryExecuteSurvivesReload(t *testing.T) {
	r := NewRegistry()
	blocker := newBlockingEndpoint()
	// Seed the registry with the member client so the pool resolves to it.
	seed := Config{Kind: EgressPool, Pool: NewPool(
		[]Member{{Endpoint: Config{}, Streaming: true, Weight: 1}},
		RoundRobin, FallbackPolicy{Enabled: true, MaxAttempts: 1},
		HealthPolicy{Enabled: false},
	)}
	cfg := seed
	r.mu.Lock()
	r.doers["direct"] = blocker
	r.doers[cfg.Key()] = r.newPoolDoerLocked(cfg.Pool, cfg.Key())
	pd := r.doers[cfg.Key()].(*poolDoer)
	st := pd.st
	r.mu.Unlock()

	done := make(chan struct{})
	var respErr error
	go func() {
		defer close(done)
		_, _, respErr = pd.Execute(execReq(false, "{}"))
	}()
	<-blocker.called

	// The reload drops the pool mid-flight: the lease defers teardown.
	r.Retain(nil)
	select {
	case <-done:
		t.Fatal("Execute finished before unblocking")
	default:
	}

	close(blocker.block) // the endpoint answers (errors) at last
	<-done
	if respErr == nil {
		t.Error("expected the stubbed endpoint error")
	}
	if _, ok := r.pools[st.pool.Identity()]; ok {
		t.Error("pool state survived past its last lease")
	}
}

// ---- the 407 surfaces ----

// TestConnect407IsTypedProxyAuthError pins the HTTP CONNECT auth surface:
// a proxy answering 407 to the CONNECT is a typed proxy-auth transport
// error with static text — never a leaked proxy response and never a
// silent direct request.
func TestConnect407IsTypedProxyAuthError(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodConnect {
			w.WriteHeader(http.StatusProxyAuthRequired)
			return
		}
		w.WriteHeader(http.StatusForbidden)
	})}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { _ = srv.Close() })

	req, err := http.NewRequest(http.MethodPost, "https://origin.example/v1/chat/completions", strings.NewReader(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	_, err = NewRegistry().Doer(proxyCfg(t, "http://"+ln.Addr().String())).Do(req)
	if err == nil {
		t.Fatal("407 CONNECT tunneled; want a transport error")
	}
	if Classify(err) != ClassProxyAuth {
		t.Errorf("Classify = %v, want proxy_auth", Classify(err))
	}
	if !strings.Contains(err.Error(), "407") {
		t.Errorf("err = %v, want it to mention 407", err)
	}
}

// TestAbsoluteForm407IsAnAnswer pins the boundary on the other side: a 407
// answering an absolute-form plain-http request travels from the target's
// side of the wire and is an ordinary upstream response — statuses are
// answers, never transport errors.
func TestAbsoluteForm407IsAnAnswer(t *testing.T) {
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusProxyAuthRequired)
	}))
	defer origin.Close()

	req, err := http.NewRequest(http.MethodPost, origin.URL+"/v1/chat/completions", strings.NewReader(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := NewRegistry().Doer(Config{}).Do(req)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusProxyAuthRequired {
		t.Errorf("status = %d, want 407 relayed as an answer", resp.StatusCode)
	}
}

// TestCancellationThroughRealSocksDial pins the typed-wrap chain against a
// real dial: cancelling the context mid-handshake surfaces context.Canceled
// through the ProxyConnectError (so the pool never health-strikes it).
func TestCancellationThroughRealSocksDial(t *testing.T) {
	stub := newSocks5Stub(t, func(s *socks5Stub) { s.stall = true })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://localhost:1/v1/x", strings.NewReader(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()
	_, err = NewRegistry().Doer(stub.config(t, "socks5", "")).Do(req)
	if err == nil {
		t.Fatal("Do succeeded against a stalled proxy")
	}
	if Classify(err) != ClassCanceled {
		t.Errorf("Classify = %v, want canceled", Classify(err))
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("err = %v, want context.Canceled through the wrap chain", err)
	}
}
