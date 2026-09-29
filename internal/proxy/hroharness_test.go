package proxy

// harness.go — binds the scripted upstream and the fingerprint to the REAL
// proxy handler, through its exported NewHandler seam only.
//
// Five independent observation channels, each a public contract of the
// service, so a fingerprint is never a re-derivation of handler state:
//
//  1. the client ResponseWriter — status, headers, bytes, commit point
//  2. transport.Doer              — one scripted dial per attempt
//  3. zerolog JSON lines          — the service's own evidence vocabulary
//  4. usage.Ingest                — the metered fact
//  5. the outbound request header — the CONFIGURED credential key id
//
// Channels 1-2 are authoritative for "what the client got"; 3-5 for "how it
// got there". A refactor that changes 1 is a behaviour change. One that
// changes only 3-5 is an observability change, and the fingerprint is
// deliberately able to see both.

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/rs/zerolog"
	"gopkg.in/yaml.v3"

	"openai-compatible-injector/internal/config"
	"openai-compatible-injector/internal/credential"
	"openai-compatible-injector/internal/transport"
	"openai-compatible-injector/internal/usage"
)

// ---------------------------------------------------------------------------
// usage.Ingest
// ---------------------------------------------------------------------------

// Meter is the usage.Ingest implementation. It keeps every event, in order.
type Meter struct {
	mu     sync.Mutex
	events []usage.Event
}

var _ usage.Ingest = (*Meter)(nil)

func (m *Meter) Record(ev usage.Event) {
	// Identity and time are dropped AT THE BOUNDARY so no downstream code
	// can reintroduce them. LatencyMS and ConfigGeneration are process state
	// (a function of how many reloads a test did), not request behaviour.
	ev.EventID = ""
	ev.OccurredAt = ev.OccurredAt.UTC()
	ev.RequestID = ""
	ev.LatencyMS = 0
	ev.ConfigGeneration = 0
	m.mu.Lock()
	m.events = append(m.events, ev)
	m.mu.Unlock()
}

func (m *Meter) Events() []usage.Event {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]usage.Event(nil), m.events...)
}

// ---------------------------------------------------------------------------
// log capture
// ---------------------------------------------------------------------------

// logFields is the ONLY set of log fields the fingerprint records. Every
// other field is identity-bearing (request_id), timing (duration_ms,
// elapsed_ms), cardinality-bearing (remote_addr, path), or free-form prose.
// This list IS the contract: adding to it can only make the fingerprint more
// sensitive, never flaky.
var logFields = []string{
	"status", "outcome", "public_model", "stream", "api",
	"provider_attempts", "candidate_attempts", "retries_total",
	"retry_attempts", "candidates_entered", "upstream_exchanges",
	"request_exchange_budget_remaining", "final_candidate", "final_provider",
	"provider_exhausted", "upstream_credential_id", "egress_attempts",
	"egress_kind", "egress_target", "egress_exhausted", "egress_attempt",
	"attempt", "error_class", "error_cause", "failure_origin", "send_state",
	"disposition", "policy_rule_id", "candidate_index", "candidate_attempt",
	"retry_index", "upstream_status", "error_shape", "bytes_in", "bytes_out",
	"code", "phase", "reason", "unsafe_reason", "terminal", "stream_limit",
	"provider", "upstream", "method", "retried_on_exhausted",
}

type logSink struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (s *logSink) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Write(p)
}

func (s *logSink) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.String()
}

// parseLogs reduces captured JSON lines to the semantic projection.
func parseLogs(s string) []LogEvent {
	var out []LogEvent
	for _, line := range strings.Split(s, "\n") {
		if line == "" {
			continue
		}
		var ev map[string]any
		if err := json.Unmarshal([]byte(line), &ev); err != nil {
			continue
		}
		slug, _ := ev["message"].(string)
		if slug == "" {
			continue
		}
		level, _ := ev["level"].(string)
		le := LogEvent{Slug: slug, Level: level}
		for _, k := range logFields {
			v, ok := ev[k]
			if !ok {
				continue
			}
			if le.Fields == nil {
				le.Fields = map[string]string{}
			}
			le.Fields[k] = renderValue(v)
		}
		out = append(out, le)
	}
	return out
}

func renderValue(v any) string {
	switch t := v.(type) {
	case string:
		return t
	case bool:
		if t {
			return "true"
		}
		return "false"
	case float64:
		if t == float64(int64(t)) {
			return itoa(int(t))
		}
		enc, _ := json.Marshal(t)
		return string(enc)
	default:
		enc, _ := json.Marshal(v)
		return string(enc)
	}
}

// ---------------------------------------------------------------------------
// ResponseWriter with a commit point
// ---------------------------------------------------------------------------

// Recorder wraps httptest.ResponseRecorder and records WHEN the handler
// committed: the first WriteHeader, then the first Write. It is a
// measurement of the writer's own transitions, not of handler state.
type Recorder struct {
	*httptest.ResponseRecorder
	mu     sync.Mutex
	commit string
}

func NewRecorder() *Recorder {
	return &Recorder{ResponseRecorder: httptest.NewRecorder(), commit: "none"}
}

func (r *Recorder) WriteHeader(code int) {
	r.mu.Lock()
	if r.commit == "none" {
		r.commit = "headers"
	}
	r.mu.Unlock()
	r.ResponseRecorder.WriteHeader(code)
}

func (r *Recorder) Write(p []byte) (int, error) {
	r.mu.Lock()
	switch r.commit {
	case "none":
		r.commit = "headers"
	case "headers":
		r.commit = "first_byte"
	}
	r.mu.Unlock()
	return r.ResponseRecorder.Write(p)
}

func (r *Recorder) Commit() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.commit
}

// ---------------------------------------------------------------------------
// Resolver
// ---------------------------------------------------------------------------

// ProviderResolver routes each transport config to its own scripted
// upstream, keyed by transport.Config.Key() (internal/transport/transport.go:235).
//
// It keys on CONTENT IDENTITY, not on Config.Kind. Keying on Kind — what the
// existing kindResolver at internal/proxy/provider_fallback_test.go:117
// does — silently MERGES two candidates that share a kind, so a two-candidate
// direct chain becomes one script. For a differential harness that is a
// correctness bug, not a shortcut.
type ProviderResolver struct {
	mu       sync.Mutex
	byKey    map[string]*Upstream
	fallback *Upstream
}

var _ transport.Resolver = (*ProviderResolver)(nil)

func NewProviderResolver(fallback *Upstream) *ProviderResolver {
	return &ProviderResolver{byKey: map[string]*Upstream{}, fallback: fallback}
}

func (r *ProviderResolver) Bind(c transport.Config, u *Upstream) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.byKey[c.Key()] = u
}

func (r *ProviderResolver) Doer(c transport.Config) transport.Doer {
	r.mu.Lock()
	u, ok := r.byKey[c.Key()]
	r.mu.Unlock()
	if ok {
		return u
	}
	if r.fallback != nil {
		return r.fallback
	}
	return transport.NewDirectClient()
}

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

// Options configure one run.
type Options struct {
	// YAML is the runtime config document, verbatim.
	YAML string
	// Model is the public model the request names; the harness reads its
	// chain off the parsed snapshot to bind routes per candidate.
	Model string
	// Route maps a TRANSPORT TABLE NAME onto the upstream serving it. The
	// harness resolves the name to the candidate's real transport.Config
	// from the snapshot, so the binding is by content identity, never by a
	// guess about Kind.
	Route map[string]*Upstream
	// Strict makes an over-run of any script an error, turning "the walk
	// made exactly N dials" into an assertion rather than a hope.
	Strict bool
	// Meter turns the usage seam on.
	Meter bool
	// LogLevel defaults to debug: the most sensitive, still deterministic.
	LogLevel zerolog.Level
}

// Result is everything one run observed.
type Result struct {
	Fingerprint Fingerprint
	// Body is the raw client-facing bytes, for byte-exact assertions the
	// fingerprint deliberately digests.
	Body string
	Log  string
	// Usage is the raw event list, with identity already stripped.
	Usage []usage.Event
	// Dials is the ordered dial log, every bound upstream merged.
	Dials []Call
}

// Run executes one scenario end to end and returns its fingerprint.
func Run(t *testing.T, opt Options, method, path, body string, headers map[string]string) Result {
	t.Helper()
	return RunCtx(t, opt, context.Background(), method, path, body, headers)
}

// RunCtx is Run with a caller-supplied context, for the ClientCancel
// scenarios. The cancel is fired by a Step hook, never by a timer.
func RunCtx(t *testing.T, opt Options, ctx context.Context, method, path, body string, headers map[string]string) Result {
	t.Helper()

	snap, err := config.LoadRuntime([]byte(opt.YAML))
	if err != nil {
		t.Fatalf("hro: LoadRuntime: %v", err)
	}
	store := config.NewStore(snap)

	model, ok := snap.Model(opt.Model)
	if !ok {
		t.Fatalf("hro: model %q not in snapshot", opt.Model)
	}

	// Bind each transport table name to the candidate that uses it, using
	// the chain's own flattened transport.Config.
	byName := map[string]transport.Config{}
	names := transportTableNames(opt.YAML)
	cfgs := snap.Transports()
	for _, name := range names {
		if c, ok := configForName(opt.YAML, name, cfgs); ok {
			byName[name] = c
		}
	}

	res := NewProviderResolver(nil)
	upstreams := map[string]*Upstream{}
	for _, c := range model.Chain {
		name := ""
		for n, cfg := range byName {
			if cfg.Key() == c.Transport.Key() {
				name = n
				break
			}
		}
		u, ok := opt.Route[name]
		if !ok {
			continue
		}
		if opt.Strict {
			u.strict = true
		}
		res.Bind(c.Transport, u)
		upstreams[name] = u
	}

	sink := &logSink{}
	level := opt.LogLevel
	if level == zerolog.NoLevel {
		level = zerolog.DebugLevel
	}
	log := zerolog.New(sink).Level(level)

	var meter usage.Ingest
	var m *Meter
	if opt.Meter {
		m = &Meter{}
		meter = m
	}

	var creds CredentialResolver
	if provs := snap.Credentials(); len(provs) > 0 {
		// Build the REAL pool from the snapshot's own validated provider, so
		// rotation, cooldown and the key-id mapping are the production ones.
		creds = fixedCreds{pool: credential.NewPool(provs[0].Spec)}
		knownKeyValues = map[string]string{}
		for _, k := range provs[0].Spec.Keys {
			knownKeyValues[k.Value] = k.ID
		}
	}

	h := NewHandler(store, res, creds, nil, meter, log)

	var req *http.Request
	if body != "" {
		req = httptest.NewRequest(method, path, strings.NewReader(body))
	} else {
		req = httptest.NewRequest(method, path, nil)
	}
	req = req.WithContext(ctx)
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	if _, ok := headers["Authorization"]; !ok {
		req.Header.Set("Authorization", "Bearer "+apiKeyOf(opt.YAML))
	}

	rec := NewRecorder()
	h.ServeHTTP(rec, req)

	logs := sink.String()
	fp := Fingerprint{
		Status:  rec.Code,
		Headers: NewHeader(rec.Header()),
		Body:    NewBody(rec.Body.Bytes()),
		Commit:  rec.Commit(),
		Stream:  strings.HasPrefix(rec.Header().Get("Content-Type"), "text/event-stream"),
		Logs:    parseLogs(logs),
	}

	// The dial log, merged in dial order across every bound upstream.
	var dials []Call
	ordered := make([]string, 0, len(upstreams))
	for n := range upstreams {
		ordered = append(ordered, n)
	}
	sort.Strings(ordered)
	for _, n := range ordered {
		dials = append(dials, upstreams[n].Calls()...)
	}

	fp.Attempts, fp.CredentialKeys = buildAttempts(model, dials)
	fp.EgressTargets = egressTargets(model, dials)
	fp.CandidatesEntered = distinctCandidates(fp.Attempts)
	fp.SameCandidateRetries = sameCandRetries(fp.Attempts)
	fp.Fallbacks = countChanges(fp.Attempts)
	fp.Exchanges = len(dials)

	// Walk shape and outcome come from the service's own published
	// evidence, which is a public contract — not from a re-derivation.
	if evs := findEvent(logs, "request_completed"); len(evs) == 1 {
		f := evs[0]
		fp.Outcome = f["outcome"]
		fp.FinalProvider = f["final_provider"]
		fp.ProviderExhausted = f["provider_exhausted"] == "true"
		fp.FinalCandidate = atoi(f["final_candidate"])
		if n, ok := f["candidates_entered"]; ok {
			fp.CandidatesEntered = atoi(n)
		}
		if n, ok := f["upstream_exchanges"]; ok {
			fp.Exchanges = atoi(n)
		}
		if n, ok := f["retry_attempts"]; ok {
			fp.SameCandidateRetries = atoi(n)
		}
	}
	// Per-attempt disposition, status and class come from the events that
	// already carry them. Deriving them in the harness would test the
	// harness's own model of the walk rather than the service's.
	applyAttemptEvidence(&fp, logs)

	fp.StreamEvents = observeStream(rec.Body.String(), fp.Stream)
	var usageEvents []usage.Event
	if m != nil {
		usageEvents = m.Events()
		for _, ev := range usageEvents {
			fp.Usage = append(fp.Usage, projectUsage(ev))
		}
	}
	return Result{Fingerprint: fp, Body: rec.Body.String(), Log: logs, Usage: usageEvents, Dials: dials}
}

// ---------------------------------------------------------------------------
// Credential resolver
// ---------------------------------------------------------------------------

// fixedCreds is a CredentialResolver (internal/proxy/credential.go:23)
// returning one pre-built pool, so a test asserts rotation and cooldown
// state without a config reload.
type fixedCreds struct{ pool *credential.Pool }

func (f fixedCreds) Pool(*credential.Provider) *credential.Pool { return f.pool }

// ---------------------------------------------------------------------------
// YAML introspection — only for what the harness needs to BIND ROUTES. The
// config itself is parsed by the service's own loader, never re-parsed here.
// ---------------------------------------------------------------------------

type yamlDoc struct {
	APIKey     string                     `yaml:"api-key"`
	Transports map[string]yamlTransport   `yaml:"transports"`
	Providers  map[string]yamlProviderRef `yaml:"providers"`
}

type yamlTransport struct {
	Type  string `yaml:"type"`
	Proxy string `yaml:"proxy"`
}

type yamlProviderRef struct {
	Transport string `yaml:"transport"`
}

func parseYAML(y string) yamlDoc {
	var d yamlDoc
	_ = yaml.Unmarshal([]byte(y), &d)
	return d
}

func apiKeyOf(y string) string { return parseYAML(y).APIKey }

func transportTableNames(y string) []string {
	d := parseYAML(y)
	out := make([]string, 0, len(d.Transports))
	for k := range d.Transports {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// configForName resolves a transport TABLE NAME onto the snapshot's real
// transport.Config, by matching Config.Key() against the key the YAML's own
// declaration implies. Exact for direct and proxy; a pool's Key() embeds a
// content hash, so a pool is matched only when it is the sole distinct
// config — a documented limitation, not a silent fallback.
func configForName(y, name string, cfgs []transport.Config) (transport.Config, bool) {
	tr, ok := parseYAML(y).Transports[name]
	if !ok {
		return transport.Config{}, false
	}
	want := ""
	switch tr.Type {
	case "", "direct":
		want = "direct"
	case "proxy":
		want = "proxy " + tr.Proxy
	default:
		// pool: fall back to the single distinct config when unambiguous.
		if len(cfgs) == 1 {
			return cfgs[0], true
		}
		return transport.Config{}, false
	}
	for _, c := range cfgs {
		if c.Key() == want {
			return c, true
		}
	}
	return transport.Config{}, false
}

// ---------------------------------------------------------------------------
// fingerprint assembly
// ---------------------------------------------------------------------------

// buildAttempts reconstructs the ordered attempt list from the dial log.
// Candidate attribution comes from the service's own
// provider_attempt_started events (applyAttemptEvidence); this pass supplies
// only what the dial log can see on its own: order, the sent body, the
// credential key id, and answer-vs-failure.
func buildAttempts(model config.Model, dials []Call) ([]Attempt, []string) {
	var out []Attempt
	var creds []string
	for _, c := range dials {
		a := Attempt{
			Index:         len(out) + 1,
			SentBody:      c.SentBody,
			Method:        c.Method,
			CredentialKey: credKeyOf(c.RequestHeader),
		}
		if a.CredentialKey != "" {
			creds = append(creds, a.CredentialKey)
		}
		if c.Err != nil {
			a.Result = "transport_failure"
		} else {
			a.Result = "answer"
		}
		out = append(out, a)
	}
	return out, creds
}

// egressTargets records the upstream NAME per dial, which is the
// operator-chosen egress identity the script assigned.
func egressTargets(model config.Model, dials []Call) []string {
	var out []string
	for _, c := range dials {
		out = append(out, c.Upstream)
	}
	return out
}

func distinctCandidates(as []Attempt) int {
	seen := map[int]bool{}
	for _, a := range as {
		seen[a.Candidate] = true
	}
	return len(seen)
}

func countChanges(as []Attempt) int {
	n := 0
	for i := 1; i < len(as); i++ {
		if as[i].Candidate != as[i-1].Candidate {
			n++
		}
	}
	return n
}

func sameCandRetries(as []Attempt) int {
	n := 0
	for i := 1; i < len(as); i++ {
		if as[i].Candidate == as[i-1].Candidate {
			n++
		}
	}
	return n
}

// applyAttemptEvidence fills the per-attempt facts the service already
// publishes: status, disposition, rule id, class/cause/send_state.
func applyAttemptEvidence(fp *Fingerprint, logs string) {
	type pending struct {
		idx int
		ev  map[string]string
	}
	var starts, failEvs, httpErrs []pending
	for _, line := range strings.Split(logs, "\n") {
		if line == "" {
			continue
		}
		var raw map[string]any
		if err := json.Unmarshal([]byte(line), &raw); err != nil {
			continue
		}
		slug, _ := raw["message"].(string)
		m := map[string]string{}
		for k, v := range raw {
			m[k] = renderValue(v)
		}
		switch slug {
		case "provider_attempt_started":
			starts = append(starts, pending{atoi(m["provider_attempt"]), m})
		case "egress_attempt_failed":
			failEvs = append(failEvs, pending{atoi(m["provider_attempt"]), m})
		case "upstream_http_error":
			httpErrs = append(httpErrs, pending{atoi(m["provider_attempt"]), m})
		}
	}
	byIdx := map[int]Attempt{}
	for _, a := range fp.Attempts {
		byIdx[a.Index] = a
	}
	// The log's provider_attempt counter indexes the WALK, which is the same
	// sequence as the merged dial log when every dial is one attempt. When
	// they diverge (a pooled attempt fanning out over members, or an attempt
	// refused before any dial) the counter is out of range for the dial log
	// and that attempt simply keeps what the dial log could see.
	for _, s := range starts {
		if a, ok := byIdx[s.idx]; ok {
			a.Candidate = atoi(s.ev["candidate_index"])
			a.CandidateName = s.ev["provider"]
			byIdx[s.idx] = a
		}
	}
	for _, f := range failEvs {
		if a, ok := byIdx[f.idx]; ok {
			a.ErrorClass = f.ev["error_class"]
			a.ErrorCause = f.ev["error_cause"]
			a.SendState = f.ev["send_state"]
			a.Result = "transport_failure"
			if k := f.ev["upstream_credential_id"]; k != "" {
				a.CredentialKey = k
			}
			byIdx[f.idx] = a
		}
	}
	for _, e := range httpErrs {
		if a, ok := byIdx[e.idx]; ok {
			a.Status = atoi(e.ev["upstream_status"])
			a.Result = "answer"
			byIdx[e.idx] = a
		}
	}
	for i := range fp.Attempts {
		if a, ok := byIdx[fp.Attempts[i].Index]; ok {
			fp.Attempts[i] = a
		}
	}
}

// credKeyOf recovers the CONFIGURED key id from the outbound header. The
// harness read the real values off the snapshot, so it maps value -> id.
// The value itself never reaches the fingerprint: only the id does.
var knownKeyValues = map[string]string{}

func credKeyOf(h http.Header) string {
	v := h.Get("Authorization")
	if v == "" {
		v = h.Get("X-Api-Key")
	}
	if v == "" {
		return ""
	}
	for val, id := range knownKeyValues {
		if strings.Contains(v, val) {
			return id
		}
	}
	return ""
}

// observeStream replays the RELAYED SSE bytes into the frame sequence the
// client actually received. Post-hoc parse of observed bytes, so it needs no
// seam inside the handler.
func observeStream(body string, isStream bool) []StreamEvent {
	if !isStream || body == "" {
		return nil
	}
	var out []StreamEvent
	var name string
	for _, raw := range strings.Split(body, "\n") {
		line := strings.TrimSuffix(raw, "\r")
		switch {
		case strings.HasPrefix(line, "event:"):
			name = strings.TrimSpace(strings.TrimPrefix(line, "event:"))
		case strings.HasPrefix(line, "data:"):
			payload := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
			if payload == "[DONE]" {
				out = append(out, StreamEvent{Name: "[DONE]", SHA: digest(payload), Len: len(payload), Terminal: true})
				name = ""
				continue
			}
			e := StreamEvent{Name: name, SHA: digest(payload), Len: len(payload)}
			if name == "response.completed" {
				e.Terminal = true
			}
			out = append(out, e)
			name = ""
		}
	}
	return out
}

func digest(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

func findEvent(logs, slug string) []map[string]string {
	var out []map[string]string
	for _, line := range strings.Split(logs, "\n") {
		if line == "" {
			continue
		}
		var ev map[string]any
		if err := json.Unmarshal([]byte(line), &ev); err != nil {
			continue
		}
		if m, _ := ev["message"].(string); m != slug {
			continue
		}
		m := map[string]string{}
		for k, v := range ev {
			m[k] = renderValue(v)
		}
		out = append(out, m)
	}
	return out
}

func atoi(s string) int {
	if s == "" {
		return 0
	}
	n, neg := 0, false
	for i, c := range s {
		if i == 0 && c == '-' {
			neg = true
			continue
		}
		if c < '0' || c > '9' {
			return 0
		}
		n = n*10 + int(c-'0')
	}
	if neg {
		return -n
	}
	return n
}
