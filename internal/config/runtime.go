package config

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"openai-compatible-injector/internal/transport"
)

// typeErrorLine matches the "line N:" prefix of each problem a yaml
// TypeError reports — the only part of that error that is safe to surface.
var typeErrorLine = regexp.MustCompile(`line (\d+)`)

// maxBearerTokenBytes bounds credentials held in a runtime snapshot. It is
// kept in sync with proxy authentication's fixed-size comparison buffer.
const maxBearerTokenBytes = 4 << 10

// inputEchoingPrefixes lists the yaml.v3 scanner-level failures whose
// messages interpolate operator text rather than positions: an undefined
// alias names its anchor, a recursive anchor names itself, an explicit tag
// mismatch quotes the scalar, an unhashable key quotes the key. Error text
// reaches logs verbatim, so each is replaced wholesale; everything else at
// this layer (syntax errors, control-character rejections) quotes only
// positions and passes through.
var inputEchoingPrefixes = []struct{ prefix, safe string }{
	{"yaml: unknown anchor ", "yaml: undefined anchor referenced (input redacted)"},
	{"yaml: anchor ", "yaml: recursive anchor rejected (input redacted)"},
	{"yaml: cannot decode ", "yaml: explicitly tagged value could not be decoded (input redacted)"},
	{"yaml: invalid map key", "yaml: invalid map key (input redacted)"},
}

// decodeConfigError rewrites a YAML decode failure into log-safe text.
// yaml.TypeError quotes the offending key or scalar value — and a botched
// paste into any YAML position can carry credentials — so the error is
// reduced to the line numbers that failed and a generic description. The
// handful of scanner-level errors that quote input instead of positions are
// replaced wholesale (see inputEchoingPrefixes).
func decodeConfigError(err error) error {
	msg := err.Error()
	for _, e := range inputEchoingPrefixes {
		if strings.HasPrefix(msg, e.prefix) {
			return errors.New(e.safe)
		}
	}
	var te *yaml.TypeError
	if !errors.As(err, &te) {
		return err
	}
	matches := typeErrorLine.FindAllStringSubmatch(te.Error(), -1)
	if len(matches) == 0 {
		return errors.New("yaml: unmarshal errors (input redacted)")
	}
	seen := make(map[int]struct{}, len(matches))
	lines := make([]int, 0, len(matches))
	for _, m := range matches {
		n, err := strconv.Atoi(m[1])
		if err != nil {
			continue
		}
		if _, dup := seen[n]; dup {
			continue
		}
		seen[n] = struct{}{}
		lines = append(lines, n)
	}
	if len(lines) == 0 {
		return errors.New("yaml: unmarshal errors (input redacted)")
	}
	sort.Ints(lines)
	parts := make([]string, len(lines))
	for i, n := range lines {
		parts[i] = strconv.Itoa(n)
	}
	return fmt.Errorf("yaml: unmarshal errors at line(s) %s (input redacted)", strings.Join(parts, ", "))
}

// runtime file schema. Strictness has two layers: top-level keys are
// validated against the raw YAML (only "models", "api-key", "log-level",
// "sse-keep-alive", "providers", and "transports" are legal — this is what
// keeps the bootstrap plane out of the runtime file: listen, config file,
// poll interval, ... any file that tries to define them fails validation,
// whatever their value's shape), and a KnownFields strict decode rejects
// unknown keys inside each model, provider, and transport entry.
//
// The models table is decoded with yaml.v3 directly, never through viper:
// viper's map normalization lowercases every key and flattens dotted names,
// which would both corrupt public model names (`MyModel` reaching a client
// as `mymodel`; `gpt-3.5-turbo` rejected as an unknown key) and silently
// accept case variants of entry keys. yaml.v3 preserves key bytes as
// written and matches entry fields case-sensitively.

type runtimeFile struct {
	Models map[string]runtimeModel `yaml:"models"`
	// APIKey mirrors the required top-level api-key — the bearer credential
	// clients must present on both /v1 routes. Absent, null, or
	// whitespace-only rejects the whole file, so the front door is closed by
	// default and a bad rewrite cannot silently reopen it on reload. The
	// value is credential material: it never reaches any log event or error
	// text.
	APIKey string `yaml:"api-key"`
	// LogLevel mirrors the optional top-level log-level key — the same
	// flat spelling the org's other Go services use, with no nested
	// section. Absent or null selects the default; the value itself is
	// validated by ParseLogLevel.
	LogLevel string `yaml:"log-level"`
	// SSEKeepAlive mirrors the optional top-level sse-keep-alive block.
	// The pointer distinguishes an absent or null block (defaults) from a
	// present one, which is validated even when it disables the feature.
	SSEKeepAlive *runtimeSSEKeepAlive `yaml:"sse-keep-alive"`
	// Providers mirrors the optional top-level providers table: named
	// upstream bases (base-url) each referencing a transport. Models point
	// at entries by name; the table is validated even when unreferenced.
	Providers map[string]runtimeProvider `yaml:"providers"`
	// Transports mirrors the optional top-level transports table: named
	// outbound paths (direct, or one configured proxy endpoint). Providers
	// reference entries by name; the table is validated even when
	// unreferenced.
	Transports map[string]runtimeTransport `yaml:"transports"`
}

// runtimeProvider mirrors one providers entry: where requests go
// (base-url) and how they get there (a named transport; omitted → direct).
type runtimeProvider struct {
	BaseURL   string `yaml:"base-url"`
	Transport string `yaml:"transport"`
}

// runtimeTransport mirrors one transports entry. Type is direct or proxy;
// proxy requires the proxy URL (http/https/socks5/socks5h, userinfo
// allowed as proxy authentication, explicit port required) and direct
// must not set one.
type runtimeTransport struct {
	Type  string `yaml:"type"`
	Proxy string `yaml:"proxy"`
}

// runtimeSSEKeepAlive mirrors the optional top-level sse-keep-alive block.
// Interval stays a string here: yaml.v3 decodes durations as bare integers
// (nanoseconds), which is not the spelling operators write, so the value
// parses through time.ParseDuration during validation instead.
type runtimeSSEKeepAlive struct {
	Enabled  *bool  `yaml:"enabled"`
	Interval string `yaml:"interval"`
}

type runtimeModel struct {
	Endpoint string `yaml:"endpoint"`
	// Provider names a providers entry whose base-url and transport this
	// model forwards through. It is mutually exclusive with endpoint:
	// exactly one of the two is required. endpoint stays the legacy inline
	// form — an implicit direct-transport provider.
	Provider        string                `yaml:"provider"`
	UpstreamModel   string                `yaml:"upstream-model"`
	InjectionPrompt string                `yaml:"injection-prompt"`
	ThinkingUsage   *runtimeThinkingUsage `yaml:"thinking-usage"`
}

// runtimeThinkingUsage mirrors the optional per-model thinking-usage block.
// The pointer distinguishes an absent or null block (feature off) from a
// present-but-empty one, which rejects on the missing mode.
type runtimeThinkingUsage struct {
	Mode     string   `yaml:"mode"`
	MinRatio *float64 `yaml:"min-ratio"`
	MaxRatio *float64 `yaml:"max-ratio"`
}

// LoadRuntime parses and validates runtime configuration bytes into an
// immutable Snapshot. Any error — parse failure, unknown key, invalid model
// entry — rejects the whole file; callers must keep serving the previous
// snapshot in that case.
func LoadRuntime(data []byte) (*Snapshot, error) {
	// Top-level keys are validated against the raw YAML first — a reject
	// with a fixed message independent of the key's text (which error text
	// must never echo). The strict struct decode below would also reject
	// these keys, but only after YAML-level type resolution; this pass keeps
	// the bootstrap-plane rule absolute regardless of a stray key's value
	// shape.
	var raw map[string]any
	if err := yaml.Unmarshal(data, &raw); err != nil {
		// Duplicate keys (uniqueKeys is on by default) and whole-file scalars
		// surface here as a yaml.TypeError that quotes the key or value —
		// the same sanitizer as every other decode failure, or the text
		// reaches the logs verbatim.
		return nil, fmt.Errorf("parse config: %w", decodeConfigError(err))
	}
	for k := range raw {
		if k == "models" || k == "api-key" || k == "log-level" || k == "sse-keep-alive" ||
			k == "providers" || k == "transports" {
			continue
		}
		// The key itself is not named: error text reaches logs verbatim, and
		// a pasted credential can land in a key position just as well as a
		// value position.
		return nil, errors.New("unknown top-level key (only models, api-key, log-level, sse-keep-alive, providers and transports are legal)")
	}
	var rf runtimeFile
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	// An empty document decodes as io.EOF; that is not a malformed file but
	// an empty models table, which the emptiness check below rejects with
	// the honest message.
	if err := dec.Decode(&rf); err != nil && !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("decode config: %w", decodeConfigError(err))
	}
	// yaml.v3's Decode consumes only the first document in a stream; a file
	// with more silently hides the rest — including any bootstrap-plane key
	// an operator (or a careless merge tool) appended after a `---`. That
	// would break the two-plane rule without any visible rejection, so a
	// multi-document file is a reject, full stop.
	var extra map[string]any
	switch err := dec.Decode(&extra); {
	case errors.Is(err, io.EOF):
		// Exactly one document: the only accepted shape.
	case err != nil:
		return nil, fmt.Errorf("decode config: %w", decodeConfigError(err))
	default:
		return nil, errors.New("config file must contain exactly one YAML document")
	}

	transports, err := buildTransports(rf.Transports)
	if err != nil {
		return nil, err
	}
	providers, err := buildProviders(rf.Providers, transports)
	if err != nil {
		return nil, err
	}

	models := make(map[string]Model, len(rf.Models))
	seen := make(map[string]struct{}, len(rf.Models))
	// Entries are validated in sorted-key order so the rejection's ordinal
	// ("model entry 3") is deterministic: the key text itself is never
	// named — error text reaches logs verbatim, and a pasted credential can
	// land in a key position just as well as a value position.
	names := make([]string, 0, len(rf.Models))
	for name := range rf.Models {
		names = append(names, name)
	}
	sort.Strings(names)
	for i, rawName := range names {
		name := strings.TrimSpace(rawName)
		ordinal := i + 1
		if name == "" {
			return nil, fmt.Errorf("model entry %d: name must not be empty", ordinal)
		}
		if _, dup := seen[name]; dup {
			// `"  a":` and `a:` are distinct YAML keys that trim to the same
			// model name; which one wins must not depend on map iteration
			// order, so an ambiguous file is a reject, not a coin toss.
			return nil, fmt.Errorf("model entry %d: name collides with another entry after trimming whitespace", ordinal)
		}
		seen[name] = struct{}{}
		m, err := buildModel(name, rf.Models[rawName], providers)
		if err != nil {
			return nil, fmt.Errorf("model entry %d: %w", ordinal, err)
		}
		models[name] = m
	}

	if len(models) == 0 {
		// An empty models table is a reject, not a valid state: a tool that
		// rewrites the file by truncate-then-write leaves it empty for a
		// window, and an empty file parses as valid YAML with zero models —
		// accepted, it would silently drop every model from the live service
		// until the next valid change. As a rejection it lands on the
		// last-known-good path instead.
		return nil, errors.New("models: at least one model is required")
	}

	// The client API key is required — fail-closed. A file without one never
	// becomes a snapshot: first boot refuses to start and a keyless rewrite
	// lands on the last-known-good path instead of reopening the front door.
	// A space-only value is the same as absent. Outer spaces are normalized, but
	// every other character must form an RFC 6750 bearer token, or clients could
	// not present it legally. The value is never named in the error: error text
	// reaches logs verbatim.
	key := strings.Trim(rf.APIKey, " ")
	if key == "" {
		return nil, errors.New("api-key is required")
	}
	if !validBearerToken(key) {
		return nil, errors.New("api-key must be a valid bearer token")
	}

	level, err := ParseLogLevel(rf.LogLevel)
	if err != nil {
		return nil, fmt.Errorf("decode config: %w", err)
	}

	keepAlive, err := buildSSEKeepAlive(rf.SSEKeepAlive)
	if err != nil {
		return nil, err
	}

	// The distinct outbound transports the models reference, in first-use
	// order over sorted model names — the set the registry retains on
	// publish. Config values compare by their proxy URL pointer, so models
	// sharing a provider share one entry; two providers with byte-identical
	// proxy URLs hold two entries, which the registry's content keying
	// merges back into one pool anyway.
	transportSeen := make(map[transport.Config]struct{}, len(models))
	transportSet := make([]transport.Config, 0, 1)
	for _, name := range names {
		tc := models[name].Transport
		if _, dup := transportSeen[tc]; !dup {
			transportSeen[tc] = struct{}{}
			transportSet = append(transportSet, tc)
		}
	}

	return &Snapshot{
		models:     models,
		apiKey:     key,
		logLevel:   level,
		keepAlive:  keepAlive,
		transports: transportSet,
	}, nil
}

// buildTransports validates the optional named-transports table. Entries
// are validated in sorted-key order so the rejection's ordinal is
// deterministic, and names follow the model-name discipline: trimmed,
// non-empty, unambiguous after trimming. Referenced from providers by the
// trimmed name; an unreferenced entry is validated like any other — dead
// config is not broken config. Messages never name the entry or echo its
// values: error text reaches logs verbatim, and a proxy URL carries
// credentials in its userinfo.
func buildTransports(rt map[string]runtimeTransport) (map[string]transport.Config, error) {
	out := make(map[string]transport.Config, len(rt))
	names := make([]string, 0, len(rt))
	for name := range rt {
		names = append(names, name)
	}
	sort.Strings(names)
	seen := make(map[string]struct{}, len(rt))
	for i, rawName := range names {
		name := strings.TrimSpace(rawName)
		ordinal := i + 1
		if name == "" {
			return nil, fmt.Errorf("transport entry %d: name must not be empty", ordinal)
		}
		if _, dup := seen[name]; dup {
			return nil, fmt.Errorf("transport entry %d: name collides with another entry after trimming whitespace", ordinal)
		}
		seen[name] = struct{}{}
		entry := rt[rawName]
		var cfg transport.Config
		switch entry.Type {
		case "":
			return nil, fmt.Errorf("transport entry %d: type is required (direct, proxy)", ordinal)
		case "direct":
			if strings.TrimSpace(entry.Proxy) != "" {
				return nil, fmt.Errorf("transport entry %d: type direct must not set a proxy URL", ordinal)
			}
			cfg = transport.Config{Kind: transport.Direct}
		case "proxy":
			u, err := parseProxyURL(entry.Proxy)
			if err != nil {
				return nil, fmt.Errorf("transport entry %d: %w", ordinal, err)
			}
			cfg = transport.Config{Kind: transport.Proxy, ProxyURL: u}
		default:
			return nil, fmt.Errorf("transport entry %d: type must be one of direct, proxy", ordinal)
		}
		out[name] = cfg
	}
	return out, nil
}

// providerEntry is one validated providers-table entry: the upstream base
// URL plus the outbound transport its requests execute through.
type providerEntry struct {
	endpoint  *url.URL
	transport transport.Config
}

// buildProviders validates the optional providers table against the
// validated transports table. Names follow the model-name discipline
// (sorted validation order, trimmed, non-empty, unambiguous); base-url is
// validated exactly like a model endpoint; a transport reference resolves
// by trimmed name and must exist — there is no fallback to direct for a
// name the file does not define, because silently rerouting egress is the
// quiet direction this schema exists to prevent.
func buildProviders(rp map[string]runtimeProvider, transports map[string]transport.Config) (map[string]providerEntry, error) {
	out := make(map[string]providerEntry, len(rp))
	names := make([]string, 0, len(rp))
	for name := range rp {
		names = append(names, name)
	}
	sort.Strings(names)
	seen := make(map[string]struct{}, len(rp))
	for i, rawName := range names {
		name := strings.TrimSpace(rawName)
		ordinal := i + 1
		if name == "" {
			return nil, fmt.Errorf("provider entry %d: name must not be empty", ordinal)
		}
		if _, dup := seen[name]; dup {
			return nil, fmt.Errorf("provider entry %d: name collides with another entry after trimming whitespace", ordinal)
		}
		seen[name] = struct{}{}
		entry := rp[rawName]
		if strings.TrimSpace(entry.BaseURL) == "" {
			return nil, fmt.Errorf("provider entry %d: base-url is required", ordinal)
		}
		u, err := parseOperatorURL("base-url", entry.BaseURL)
		if err != nil {
			return nil, fmt.Errorf("provider entry %d: %w", ordinal, err)
		}
		if err := validateEndpointURL(u, "base-url"); err != nil {
			return nil, fmt.Errorf("provider entry %d: %w", ordinal, err)
		}
		ref := strings.TrimSpace(entry.Transport)
		var tc transport.Config
		if ref != "" {
			cfg, ok := transports[ref]
			if !ok {
				return nil, fmt.Errorf("provider entry %d: transport reference is unknown", ordinal)
			}
			tc = cfg
		}
		out[name] = providerEntry{endpoint: u, transport: tc}
	}
	return out, nil
}

// parseProxyURL validates the proxy URL of a type: proxy transport. Unlike
// a provider endpoint, userinfo is legal — it is the proxy's
// authentication, read by the dialer and never logged — and the scheme set
// is the four proxy forms (http, https, socks5, socks5h). The URL must
// name a host and an explicit port (proxy ports are never implicit), and
// must carry nothing after the authority: a path, query, or fragment is
// either never sent or silently ignored by every proxy form, and silently
// ignored config is a reject. Messages never echo the input: the URL can
// carry credentials in any position.
func parseProxyURL(raw string) (*url.URL, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, errors.New("proxy URL is required for type proxy")
	}
	u, err := parseOperatorURL("proxy", raw)
	if err != nil {
		return nil, err
	}
	switch u.Scheme {
	case "http", "https", "socks5", "socks5h":
	default:
		return nil, errors.New("proxy scheme must be one of http, https, socks5, socks5h (input redacted)")
	}
	if u.Host == "" {
		return nil, errors.New("proxy URL must include a host (input redacted)")
	}
	if u.Port() == "" {
		return nil, errors.New("proxy URL must include an explicit port")
	}
	if u.Path != "" || u.RawQuery != "" || u.Fragment != "" {
		return nil, errors.New("proxy URL must be scheme://[user:pass@]host:port with nothing after the authority (input redacted)")
	}
	return u, nil
}

// defaultSSEKeepAliveInterval is the keep-alive silence threshold when the
// sse-keep-alive block omits interval. 15s keeps a silent stream well
// inside the ~125s window a Cloudflare-proxied hostname allows: measured
// 2026-09-22, a silent HTTP/2 stream was cut at 125.06s origin-side (client
// error at 125.39s), while a 15s comment ping held a 240s silent stream
// open through the same path.
const defaultSSEKeepAliveInterval = 15 * time.Second

// minSSEKeepAliveInterval is the smallest accepted interval. Below one
// second the heartbeat stops being a keep-alive and starts being traffic.
const minSSEKeepAliveInterval = time.Second

// buildSSEKeepAlive validates and normalizes the optional sse-keep-alive
// block. Absent or null means the defaults — on at 15s, because the
// deployment this proxy serves sits behind Cloudflare. Every message is
// fixed text: the interval position can carry a botched paste of anything,
// and time.ParseDuration errors quote their input, so the value is never
// echoed. A block that disables the feature is still validated — a bad
// interval in a disabled block is a config error like any other.
func buildSSEKeepAlive(rk *runtimeSSEKeepAlive) (SSEKeepAlive, error) {
	ka := SSEKeepAlive{Enabled: true, Interval: defaultSSEKeepAliveInterval}
	if rk == nil {
		return ka, nil
	}
	if rk.Enabled != nil {
		ka.Enabled = *rk.Enabled
	}
	if rk.Interval == "" {
		return ka, nil
	}
	d, err := time.ParseDuration(rk.Interval)
	if err != nil {
		return SSEKeepAlive{}, errors.New("sse-keep-alive: interval must be a valid duration (e.g. 15s, 1m)")
	}
	if d < minSSEKeepAliveInterval {
		return SSEKeepAlive{}, errors.New("sse-keep-alive: interval must be at least 1s")
	}
	ka.Interval = d
	return ka, nil
}

// validBearerToken reports whether token is an RFC 6750 b64token. Keeping
// this validation in the config plane ensures every accepted configured key
// can be presented legally in an Authorization: Bearer header.
func validBearerToken(token string) bool {
	if token == "" || len(token) > maxBearerTokenBytes {
		return false
	}
	padding := false
	hasTokenChar := false
	for i := range len(token) {
		c := token[i]
		if c == '=' {
			if !hasTokenChar {
				return false
			}
			padding = true
			continue
		}
		if padding || !isBearerTokenChar(c) {
			return false
		}
		hasTokenChar = true
	}
	return true
}

func isBearerTokenChar(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
		(c >= '0' && c <= '9') || c == '-' || c == '.' || c == '_' ||
		c == '~' || c == '+' || c == '/'
}

func buildModel(name string, rm runtimeModel, providers map[string]providerEntry) (Model, error) {
	// Where the request goes: a providers-table reference (whose entry
	// carries the base URL and the outbound transport) or the legacy inline
	// endpoint. Exactly one — an endpoint next to a provider reference is
	// an ambiguity about the upstream's identity, and neither is a model
	// with nowhere to go.
	providerRef := strings.TrimSpace(rm.Provider)
	var (
		endpoint     *url.URL
		providerName string
		outTransport transport.Config
	)
	switch {
	case providerRef != "" && strings.TrimSpace(rm.Endpoint) != "":
		return Model{}, errors.New("endpoint and provider are mutually exclusive")
	case providerRef != "":
		p, ok := providers[providerRef]
		if !ok {
			return Model{}, errors.New("provider reference is unknown")
		}
		endpoint, providerName, outTransport = p.endpoint, providerRef, p.transport
	case strings.TrimSpace(rm.Endpoint) != "":
		u, err := parseOperatorURL("endpoint", rm.Endpoint)
		if err != nil {
			return Model{}, err
		}
		if err := validateEndpointURL(u, "endpoint"); err != nil {
			return Model{}, err
		}
		endpoint = u
	default:
		return Model{}, errors.New("endpoint or provider is required")
	}
	if strings.TrimSpace(rm.UpstreamModel) == "" {
		return Model{}, errors.New("upstream-model is required")
	}
	tu, err := buildThinkingUsage(rm.ThinkingUsage)
	if err != nil {
		return Model{}, err
	}
	return Model{
		Public:          name,
		Provider:        providerName,
		Endpoint:        endpoint,
		UpstreamModel:   rm.UpstreamModel,
		InjectionPrompt: rm.InjectionPrompt,
		ThinkingUsage:   tu,
		Transport:       outTransport,
	}, nil
}

// parseOperatorURL parses an operator-supplied URL under the no-echo rule.
// url.Parse errors can quote the raw input, query string included; error
// text reaches logs verbatim, so the raw input must not appear. A
// *url.Error is unwrapped to its cause — position-free for most parse
// failures, but two stdlib causes quote raw bytes of the input (the
// offending escape sequence, the rejected host byte) and are swapped for
// static text, mirroring the proxy's sanitizeUpstreamError. If the text
// ever does contain the input's quoted form, it is dropped entirely — the
// sanitizer fails closed, never open. field names the config position the
// message is prefixed with (endpoint, base-url, proxy).
func parseOperatorURL(field, raw string) (*url.URL, error) {
	u, err := url.Parse(raw)
	if err == nil {
		return u, nil
	}
	var ue *url.Error
	switch {
	case errors.As(err, &ue):
		inner := ue.Err
		var ee url.EscapeError
		var he url.InvalidHostError
		switch {
		case errors.As(inner, &ee):
			inner = errors.New("invalid URL escape")
		case errors.As(inner, &he):
			inner = errors.New("invalid host")
		}
		return nil, fmt.Errorf("%s: %s", field, inner)
	case strings.Contains(err.Error(), strconv.Quote(raw)):
		return nil, fmt.Errorf("%s: invalid URL (input redacted)", field)
	default:
		return nil, fmt.Errorf("%s: %s", field, err)
	}
}

// validateEndpointURL enforces the provider-endpoint rules shared by model
// endpoints and provider base-urls: http(s) with a host, no userinfo, no
// fragment. field names the config position for the message.
func validateEndpointURL(u *url.URL, field string) error {
	if (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		// Scheme and host are operator input like any other; a botched paste
		// into the position can carry a credential into either.
		return fmt.Errorf("%s scheme and host must be http(s) with a host (input redacted)", field)
	}
	if u.User != nil {
		return fmt.Errorf("%s must not contain credentials", field)
	}
	if u.Fragment != "" {
		// A fragment is never sent to a server; accepting one would silently
		// ignore part of the configured endpoint.
		return fmt.Errorf("%s must not contain a fragment", field)
	}
	return nil
}

// defaultThinkingShare is the share of output tokens attributed to thinking
// when a thinking-usage block configures no ratio bounds (both absent). One
// bound set pins the share to it; both set leave the [Lo, Hi] range intact.
const defaultThinkingShare = 0.75

// buildThinkingUsage validates and normalizes the optional thinking-usage
// block. Every message is fixed text: value positions can carry a botched
// paste of anything, so neither the mode string nor the ratios are echoed.
// A block configured under mode off is validated like any other — a bad
// ratio in a disabled block is still a config error — but normalizes to the
// off mode, behaviorally identical to no block at all.
func buildThinkingUsage(rt *runtimeThinkingUsage) (ThinkingUsage, error) {
	if rt == nil {
		return ThinkingUsage{}, nil
	}
	var mode ThinkingMode
	switch rt.Mode {
	case "":
		return ThinkingUsage{}, errors.New("thinking-usage: mode is required (auto, always, off)")
	case "auto":
		mode = ThinkingAuto
	case "always":
		mode = ThinkingAlways
	case "off":
		mode = ThinkingOff
	default:
		return ThinkingUsage{}, errors.New("thinking-usage: mode must be one of auto, always, off")
	}
	lo, err := thinkingRatio(rt.MinRatio, "min-ratio")
	if err != nil {
		return ThinkingUsage{}, err
	}
	hi, err := thinkingRatio(rt.MaxRatio, "max-ratio")
	if err != nil {
		return ThinkingUsage{}, err
	}
	if lo != nil && hi != nil && *lo > *hi {
		return ThinkingUsage{}, errors.New("thinking-usage: min-ratio must not exceed max-ratio")
	}
	share := ThinkingUsage{Mode: mode, Lo: defaultThinkingShare, Hi: defaultThinkingShare}
	switch {
	case lo != nil && hi != nil:
		share.Lo, share.Hi = *lo, *hi
	case lo != nil:
		share.Lo, share.Hi = *lo, *lo
	case hi != nil:
		share.Lo, share.Hi = *hi, *hi
	}
	return share, nil
}

// thinkingRatio validates one optional ratio bound. Finiteness is explicit:
// YAML's .nan and .inf decode straight into float64 and NaN defeats every
// comparison, range and ordering alike.
func thinkingRatio(v *float64, name string) (*float64, error) {
	if v == nil {
		return nil, nil
	}
	if math.IsNaN(*v) || math.IsInf(*v, 0) || *v < 0 || *v > 1 {
		return nil, fmt.Errorf("thinking-usage: %s must be a number between 0 and 1", name)
	}
	return v, nil
}
