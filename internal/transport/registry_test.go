package transport

import (
	"net/url"
	"sync"
	"testing"
)

// TestRegistrySharesOneDoerPerConfig pins the pooling contract: identical
// transport configs (by content, not pointer identity) resolve to the same
// long-lived doer, so a reload that keeps a transport also keeps its warm
// connection pool — the quiet direction is per-request pool churn.
func TestRegistrySharesOneDoerPerConfig(t *testing.T) {
	r := NewRegistry()
	a, _ := url.Parse("http://127.0.0.1:9")
	b, _ := url.Parse("http://127.0.0.1:9") // equal content, distinct pointer

	if got := r.Doer(Config{}); got != r.Doer(Config{}) {
		t.Error("direct config resolved to two doers")
	}
	if got := r.Doer(Config{Kind: Proxy, ProxyURL: a}); got != r.Doer(Config{Kind: Proxy, ProxyURL: b}) {
		t.Error("equal-content proxy configs resolved to two doers")
	}
	if r.Doer(Config{}) == r.Doer(Config{Kind: Proxy, ProxyURL: a}) {
		t.Error("direct and proxy configs share a doer")
	}
	c, _ := url.Parse("socks5://127.0.0.1:9")
	if r.Doer(Config{Kind: Proxy, ProxyURL: a}) == r.Doer(Config{Kind: Proxy, ProxyURL: c}) {
		t.Error("different proxy endpoints share a doer")
	}
}

// TestRegistryRetainEvictsUnreferencedTransports pins the reconciliation:
// after Retain, a config outside the active set resolves to a FRESH doer
// (the old pool's idle connections were closed), while a retained config
// keeps its doer. In-flight work on the evicted doer is untouched — only
// idle connections close.
func TestRegistryRetainEvictsUnreferencedTransports(t *testing.T) {
	r := NewRegistry()
	keep, _ := url.Parse("http://127.0.0.1:9")
	drop, _ := url.Parse("socks5://127.0.0.1:9")

	direct := r.Doer(Config{})
	kept := r.Doer(Config{Kind: Proxy, ProxyURL: keep})
	dropped := r.Doer(Config{Kind: Proxy, ProxyURL: drop})

	r.Retain([]Config{{}, {Kind: Proxy, ProxyURL: keep}})

	if r.Doer(Config{}) != direct {
		t.Error("retained direct doer was evicted")
	}
	if r.Doer(Config{Kind: Proxy, ProxyURL: keep}) != kept {
		t.Error("retained proxy doer was evicted")
	}
	if r.Doer(Config{Kind: Proxy, ProxyURL: drop}) == dropped {
		t.Error("unreferenced proxy doer survived Retain")
	}
}

// TestRegistryRetainEmptyEvictsEverything pins the degenerate case: a
// config whose models reference no proxy transports (or no models at all)
// must leave no proxy pool behind.
func TestRegistryRetainEmptyEvictsEverything(t *testing.T) {
	r := NewRegistry()
	p, _ := url.Parse("http://127.0.0.1:9")
	proxy := r.Doer(Config{Kind: Proxy, ProxyURL: p})

	r.Retain(nil)

	if r.Doer(Config{Kind: Proxy, ProxyURL: p}) == proxy {
		t.Error("proxy doer survived an empty retain set")
	}
}

// TestRegistryConcurrentResolution hammers Doer/Retain from parallel
// goroutines — the registry is read on every request and written on every
// reload, so it must be race-free by construction (-race is the oracle).
func TestRegistryConcurrentResolution(t *testing.T) {
	r := NewRegistry()
	urls := make([]*url.URL, 4)
	for i := range urls {
		u, err := url.Parse("socks5://127.0.0.1:" + itoa(1+i))
		if err != nil {
			t.Fatal(err)
		}
		urls[i] = u
	}
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				cfg := Config{}
				if j%3 == 0 {
					cfg = Config{Kind: Proxy, ProxyURL: urls[j%len(urls)]}
				}
				_ = r.Doer(cfg)
				if j%25 == 0 {
					r.Retain([]Config{cfg})
				}
			}
		}(i)
	}
	wg.Wait()
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b []byte
	for n > 0 {
		b = append([]byte{byte('0' + n%10)}, b...)
		n /= 10
	}
	return string(b)
}
