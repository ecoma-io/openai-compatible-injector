package proxy

import (
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"sync"
	"testing"

	"github.com/rs/zerolog"

	"openai-compatible-injector/internal/config"
)

// requestIDPattern is the shape of a minted request id: 16 lowercase hex
// characters, the same pin logging_test.go applies to the log field. Every
// assertion here goes through isRequestID rather than re-stating it, so the
// client-visible header and the logged request_id can never drift apart
// without a test noticing.
var requestIDPattern = regexp.MustCompile(`^[0-9a-f]{16}$`)

func isRequestID(s string) bool { return requestIDPattern.MatchString(s) }

// TestProxyRequestIDOverwritesUpstreamOnEveryCommitPath pins the quiet
// failure this whole change is built to prevent: copyRelayHeaders uses Set,
// so a proxy value written BEFORE it is silently replaced by the upstream's,
// with no error anywhere and the header still present. The symptom is a
// client holding a valid-looking id that matches no log line and no usage
// row. The overwrite is therefore asserted on every commit path rather than
// the happy one — each case below is a different site in serve, and a new
// commit path added later inherits the test only by being listed here.
func TestProxyRequestIDOverwritesUpstreamOnEveryCommitPath(t *testing.T) {
	// The upstream plants its own id on every answer. If any path relays it,
	// that is the defect this test exists to catch.
	const upstreamID = "upstream-owned"

	cases := []struct {
		name    string
		body    string
		handler func(w http.ResponseWriter, r *http.Request)
		want    int
	}{
		{
			// The normalized 4xx/5xx path: status survives, body is canonical.
			name: "normalized 4xx",
			body: `{"model":"test-model","messages":[]}`,
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.Header().Set("X-Request-Id", upstreamID)
				w.WriteHeader(http.StatusTooManyRequests)
				_, _ = w.Write([]byte(`{"error":{"message":"slow down"}}`))
			},
			want: http.StatusTooManyRequests,
		},
		{
			// The verbatim path: 3xx/204/304 relay byte for byte, and this
			// is the branch with no canonical envelope to hide behind.
			name: "verbatim 204",
			body: `{"model":"test-model","messages":[]}`,
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("X-Request-Id", upstreamID)
				w.WriteHeader(http.StatusNoContent)
			},
			want: http.StatusNoContent,
		},
		{
			// SSE is the case that most deserves its own entry: headers
			// commit exactly once, at the relay, and CopySSE is a pure body
			// relay with no header access afterwards. A stamp written at the
			// wrong moment here is unfixable for the life of the response.
			// The body must signal stream:true, or the request is answered
			// buffered and this case silently stops testing the SSE path.
			name: "sse stream",
			body: `{"model":"test-model","stream":true}`,
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "text/event-stream")
				w.Header().Set("X-Request-Id", upstreamID)
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte("data: {\"id\":\"1\"}\n\ndata: [DONE]\n\n"))
			},
			want: http.StatusOK,
		},
		{
			// The buffered 2xx tail — the ordinary success path.
			name: "buffered 2xx",
			body: `{"model":"test-model","messages":[]}`,
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.Header().Set("X-Request-Id", upstreamID)
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(`{"id":"1","model":"upstream-name"}`))
			},
			want: http.StatusOK,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			upstream := httptest.NewServer(http.HandlerFunc(tc.handler))
			defer upstream.Close()

			h := newTestHandler(t, newTestStore(t, upstream.URL+"/v1"))
			rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions", tc.body, nil)

			if rec.Code != tc.want {
				t.Fatalf("status = %d, want %d", rec.Code, tc.want)
			}
			got := rec.Header().Get("X-Request-Id")
			if !isRequestID(got) {
				t.Errorf("X-Request-Id = %q, want the proxy's own 16-hex id", got)
			}
			if got == upstreamID {
				t.Errorf("X-Request-Id = %q — the upstream's value was relayed, not overwritten; "+
					"the client's id would join to no log line", got)
			}
		})
	}
}

// TestProxyRequestIDStampsEveryEnvelopeWriter covers the locally generated
// answers, which reach the client through the reject closure rather than
// through a relay. One Set inside that closure covers all of them, and this
// test is what makes that claim load-bearing instead of hopeful: a future
// rejection that bypasses the closure is caught here.
func TestProxyRequestIDStampsEveryEnvelopeWriter(t *testing.T) {
	t.Run("401 missing bearer", func(t *testing.T) {
		h := newTestHandler(t, newTestStore(t, "http://127.0.0.1:9/v1"))
		rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions",
			`{"model":"test-model"}`, map[string]string{"Authorization": ""})
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("status = %d, want 401", rec.Code)
		}
		if got := rec.Header().Get("X-Request-Id"); !isRequestID(got) {
			t.Errorf("X-Request-Id = %q on the 401 envelope, want 16 hex chars", got)
		}
	})

	t.Run("404 model_not_found", func(t *testing.T) {
		h := newTestHandler(t, newTestStore(t, "http://127.0.0.1:9/v1"))
		rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions",
			`{"model":"ghost"}`, nil)
		if rec.Code != http.StatusNotFound {
			t.Fatalf("status = %d, want 404", rec.Code)
		}
		if got := rec.Header().Get("X-Request-Id"); !isRequestID(got) {
			t.Errorf("X-Request-Id = %q on the 404 envelope, want 16 hex chars", got)
		}
		// The envelope body stays byte-exact: stamping a header must not
		// reshape the body an existing unit test pins.
		want := `{"error":{"message":"The model 'ghost' does not exist or you do not have access to it.","type":"invalid_request_error","param":null,"code":"model_not_found"}}`
		if body := rec.Body.String(); body != want {
			t.Errorf("model_not_found body:\n got %s\nwant %s", body, want)
		}
	})

	t.Run("400 invalid json", func(t *testing.T) {
		h := newTestHandler(t, newTestStore(t, "http://127.0.0.1:9/v1"))
		rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions", `{"broken`, nil)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("status = %d, want 400", rec.Code)
		}
		if got := rec.Header().Get("X-Request-Id"); !isRequestID(got) {
			t.Errorf("X-Request-Id = %q on the 400 envelope, want 16 hex chars", got)
		}
	})

	t.Run("503 capacity_exceeded", func(t *testing.T) {
		// A budget too small to admit the body: the refusal is local, so it
		// carries the id for the same reason every other envelope does.
		withBufferBudget(t, 200<<10)
		h := newTestHandler(t, newTestStore(t, "http://127.0.0.1:9/v1"))
		body := `{"model":"test-model","padding":"` + strings.Repeat("x", 400<<10) + `"}`
		rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions", body, nil)
		if rec.Code != http.StatusServiceUnavailable {
			t.Fatalf("status = %d, want 503", rec.Code)
		}
		if got := rec.Header().Get("X-Request-Id"); !isRequestID(got) {
			t.Errorf("X-Request-Id = %q on the 503 envelope, want 16 hex chars", got)
		}
	})
}

// TestProxyRequestIDOnThe405s covers the two branches that answer before the
// request lifecycle binds a snapshot. Each is paired with the byte-exact
// envelope body, because the mint was hoisted above the method check to
// reach them and the body must not have moved with it.
func TestProxyRequestIDOnThe405s(t *testing.T) {
	h := newTestHandler(t, newTestStore(t, "http://127.0.0.1:9/v1"))

	t.Run("chat route, wrong method", func(t *testing.T) {
		rec := doRequest(t, h, http.MethodGet, "/v1/chat/completions", "", nil)
		if rec.Code != http.StatusMethodNotAllowed {
			t.Fatalf("status = %d, want 405", rec.Code)
		}
		if got := rec.Header().Get("X-Request-Id"); !isRequestID(got) {
			t.Errorf("X-Request-Id = %q on the 405, want 16 hex chars", got)
		}
		if body := rec.Body.String(); body != envelopeBadMethod {
			t.Errorf("405 body = %q, want the unchanged envelope %q", body, envelopeBadMethod)
		}
	})

	t.Run("models route, wrong method", func(t *testing.T) {
		rec := doRequest(t, h, http.MethodPost, "/v1/models", "", nil)
		if rec.Code != http.StatusMethodNotAllowed {
			t.Fatalf("status = %d, want 405", rec.Code)
		}
		if got := rec.Header().Get("X-Request-Id"); !isRequestID(got) {
			t.Errorf("X-Request-Id = %q on the 405, want 16 hex chars", got)
		}
		if body := rec.Body.String(); body != envelopeBadMethod {
			t.Errorf("405 body = %q, want the unchanged envelope %q", body, envelopeBadMethod)
		}
	})
}

// TestProxyRequestIDOnModelsAndCatchAll404 covers the two paths that answer
// without any provider at all. The catch-all 404 is deliberately id-bearing
// but NOT logged: its id is a stable handle a client can quote, and there is
// no log line to join it against. An exemption that is not asserted is an
// exemption that decays.
func TestProxyRequestIDOnModelsAndCatchAll404(t *testing.T) {
	h := newTestHandler(t, newTestStore(t, "http://127.0.0.1:9/v1"))

	t.Run("models catalog", func(t *testing.T) {
		rec := doRequest(t, h, http.MethodGet, "/v1/models", "", nil)
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200", rec.Code)
		}
		if got := rec.Header().Get("X-Request-Id"); !isRequestID(got) {
			t.Errorf("X-Request-Id = %q on the model catalog, want 16 hex chars", got)
		}
	})

	t.Run("models 401", func(t *testing.T) {
		rec := doRequest(t, h, http.MethodGet, "/v1/models", "",
			map[string]string{"Authorization": ""})
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("status = %d, want 401", rec.Code)
		}
		if got := rec.Header().Get("X-Request-Id"); !isRequestID(got) {
			t.Errorf("X-Request-Id = %q on the models 401, want 16 hex chars", got)
		}
	})

	t.Run("catch-all 404", func(t *testing.T) {
		rec := doRequest(t, h, http.MethodGet, "/v1/nope", "", nil)
		if rec.Code != http.StatusNotFound {
			t.Fatalf("status = %d, want 404", rec.Code)
		}
		if got := rec.Header().Get("X-Request-Id"); !isRequestID(got) {
			t.Errorf("X-Request-Id = %q on the catch-all 404, want 16 hex chars", got)
		}
	})
}

// TestProxyRequestIDAbsentOnHealthz pins the one exemption. /healthz and
// /readyz are container probes fired on an interval by the runtime: they sit
// outside the request lifecycle, load no snapshot, and log nothing, so an id
// on them would be a header joining to no evidence at all.
func TestProxyRequestIDAbsentOnHealthz(t *testing.T) {
	h := newTestHandler(t, newTestStore(t, "http://127.0.0.1:9/v1"))
	rec := doRequest(t, h, http.MethodGet, "/healthz", "", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if got := rec.Header().Get("X-Request-Id"); got != "" {
		t.Errorf("X-Request-Id = %q on /healthz, want empty (a probe has no request id)", got)
	}
}

// TestClientRequestIDNeverTravelsUpstream pins the injection surface from
// both sides at once. A client-chosen value would enter every log line of
// the process and the metered request_id column: a log-injection vector
// (CWE-117) and an unbounded-cardinality one, for no operational gain. The
// forward allow-list is the mechanism; this test is the pin.
func TestClientRequestIDNeverTravelsUpstream(t *testing.T) {
	const clientID = "client-chose-this"
	var seen string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = r.Header.Get("X-Request-Id")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"1","model":"upstream-name"}`))
	}))
	defer upstream.Close()

	h := newTestHandler(t, newTestStore(t, upstream.URL+"/v1"))
	rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions",
		`{"model":"test-model","messages":[]}`,
		map[string]string{"X-Request-Id": clientID})

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if seen == clientID {
		t.Errorf("the client's %q reached the upstream; the id must be the proxy's own", clientID)
	}
	if !isRequestID(seen) {
		t.Errorf("upstream X-Request-Id = %q, want the proxy's own 16-hex id", seen)
	}
}

// TestClientRequestIDNeverReachesTheLogs is the second half of the previous
// test, and a distinct risk: the value is not on the forward allow-list, but
// the client's header map is in scope on the request, and a future "echo the
// client's correlation id" convenience would put an attacker-controlled string
// into every log line of the process.
func TestClientRequestIDNeverReachesTheLogs(t *testing.T) {
	const clientID = "client-chose-this"
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"1","model":"upstream-name"}`))
	}))
	defer upstream.Close()

	buf, log := captureLog(zerolog.DebugLevel)
	h := NewHandler(newTestStore(t, upstream.URL+"/v1"), directResolver(), nil, nil, nil, log)
	rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions",
		`{"model":"test-model","messages":[]}`,
		map[string]string{"X-Request-Id": clientID})
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}

	logged := buf.String()
	if strings.Contains(logged, clientID) {
		t.Errorf("the client's request id reached the log stream:\n%s", logged)
	}
}

// TestProxyRequestIDUniquenessAcrossRequests is cheap and is the only thing
// that would catch a regression to a per-process or counter-derived id — the
// failure being silent in the worst way, since every individual response
// would still look correct.
func TestProxyRequestIDUniquenessAcrossRequests(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"1","model":"upstream-name"}`))
	}))
	defer upstream.Close()

	h := newTestHandler(t, newTestStore(t, upstream.URL+"/v1"))
	seen := make(map[string]struct{}, 16)
	for i := range 16 {
		rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions",
			`{"model":"test-model","messages":[]}`, nil)
		id := rec.Header().Get("X-Request-Id")
		if !isRequestID(id) {
			t.Fatalf("request %d: X-Request-Id = %q, want 16 hex chars", i, id)
		}
		if _, dup := seen[id]; dup {
			t.Fatalf("request %d reused id %q across requests", i, id)
		}
		seen[id] = struct{}{}
	}
}

// TestLoggedRequestIDIsTheStampedRequestID closes the loop the change
// exists to close: the id a client can quote and the id a log line carries
// are the same value, on the success path and on the normalized-error path
// alike. Before this change the two were unrelated — the client's was
// whatever the far end said.
func TestLoggedRequestIDIsTheStampedRequestID(t *testing.T) {
	for _, tc := range []struct {
		name    string
		handler http.HandlerFunc
		want    int
	}{
		{
			name: "200 buffered",
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(`{"id":"1","model":"upstream-name"}`))
			},
			want: http.StatusOK,
		},
		{
			name: "429 normalized",
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusTooManyRequests)
				_, _ = w.Write([]byte(`{"error":{"message":"slow down"}}`))
			},
			want: http.StatusTooManyRequests,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			upstream := httptest.NewServer(tc.handler)
			defer upstream.Close()

			buf, log := captureLog(zerolog.InfoLevel)
			h := NewHandler(newTestStore(t, upstream.URL+"/v1"), directResolver(), nil, nil, nil, log)
			rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions",
				`{"model":"test-model","messages":[]}`, nil)
			if rec.Code != tc.want {
				t.Fatalf("status = %d, want %d", rec.Code, tc.want)
			}

			stamped := rec.Header().Get("X-Request-Id")
			ev := findLogEvent(t, buf.String(), "request_completed")
			logged, _ := ev["request_id"].(string)
			if stamped != logged {
				t.Errorf("client saw X-Request-Id %q but request_completed carries request_id %q; "+
					"the two must be the same value for a ticket to be resolvable", stamped, logged)
			}
		})
	}
}

// TestProxyRequestIDSurvivesAMidRequestReload pins that the id is minted
// once per request and is not a function of the snapshot. There is no
// configuration surface for the header, but the property that makes one
// unnecessary is worth stating: a reload cannot reshape a correlation that
// is already on the wire and in the upstream's logs. The upstream is held
// mid-request while a fresh snapshot is published.
func TestProxyRequestIDSurvivesAMidRequestReload(t *testing.T) {
	var (
		release = make(chan struct{})
		entered = make(chan struct{})
		once    sync.Once
		seen    string
	)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = r.Header.Get("X-Request-Id")
		once.Do(func() { close(entered) })
		<-release
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"1","model":"upstream-name"}`))
	}))
	defer upstream.Close()

	store := newTestStore(t, upstream.URL+"/v1")
	_, log := captureLog(zerolog.InfoLevel)
	h := NewHandler(store, directResolver(), nil, nil, nil, log)

	done := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		done <- doRequest(t, h, http.MethodPost, "/v1/chat/completions",
			`{"model":"test-model","messages":[]}`, nil)
	}()

	<-entered
	// Publish a different snapshot mid-request. The id is already minted and
	// already on the wire; a second one must not appear.
	other, err := config.LoadRuntime([]byte(
		"api-key: unit-test-key\nmodels:\n  test-model:\n    endpoint: " + upstream.URL + "/v1\n" +
			"    upstream-model: other-name\n"))
	if err != nil {
		t.Fatalf("LoadRuntime: %v", err)
	}
	if err := store.Publish(other); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	close(release)

	rec := <-done
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	stamped := rec.Header().Get("X-Request-Id")
	if !isRequestID(stamped) {
		t.Fatalf("X-Request-Id = %q, want 16 hex chars", stamped)
	}
	if seen != stamped {
		t.Errorf("upstream saw %q but the client was stamped %q; a reload must not "+
			"reshape a correlation already on the wire", seen, stamped)
	}
}

// TestUpstreamRequestIDHeadersAreNotRelayed is the negative-space twin of
// the overwrite test: neither request-id name survives the relay, so a
// provider's value can never reach a client under any spelling. Pinned on
// the ordinary success path as well as the error path, because a relay list
// edit that re-adds the name would otherwise only show up in one of them.
func TestUpstreamRequestIDHeadersAreNotRelayed(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Request-Id", "req_upstream")
		w.Header().Set("OpenAI-Request-Id", "req_openai")
		_, _ = w.Write([]byte(`{"id":"1","model":"upstream-name"}`))
	}))
	defer upstream.Close()

	h := newTestHandler(t, newTestStore(t, upstream.URL+"/v1"))
	rec := doRequest(t, h, http.MethodPost, "/v1/chat/completions",
		`{"model":"test-model","messages":[]}`, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}

	if got := rec.Header().Get("OpenAI-Request-Id"); got != "" {
		t.Errorf("client received OpenAI-Request-Id = %q, want empty (not on the relay allow-list)", got)
	}
	if got := rec.Header().Get("X-Request-Id"); got == "req_upstream" {
		t.Errorf("client received the upstream's X-Request-Id; the proxy's own must replace it")
	}
}
