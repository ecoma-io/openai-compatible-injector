package config

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

// typeErrorLine matches the "line N:" prefix of each problem a yaml
// TypeError reports — the only part of that error that is safe to surface.
var typeErrorLine = regexp.MustCompile(`line (\d+)`)

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
// validated against the raw YAML (only "models" and "logging" are legal —
// this is what keeps the bootstrap plane out of the runtime file: listen,
// config file, poll interval, ... any file that tries to define them fails
// validation, whatever their value's shape), and a KnownFields strict decode
// rejects unknown keys inside each model entry and inside the logging
// section.
//
// The models table is decoded with yaml.v3 directly, never through viper:
// viper's map normalization lowercases every key and flattens dotted names,
// which would both corrupt public model names (`MyModel` reaching a client
// as `mymodel`; `gpt-3.5-turbo` rejected as an unknown key) and silently
// accept case variants of entry keys. yaml.v3 preserves key bytes as
// written and matches entry fields case-sensitively.

type runtimeFile struct {
	Models  map[string]runtimeModel `yaml:"models"`
	Logging runtimeLogging          `yaml:"logging"`
}

// runtimeLogging mirrors the optional logging section. Absent or null
// selects the default level; the level string itself is validated by
// ParseLogLevel.
type runtimeLogging struct {
	Level string `yaml:"level"`
}

type runtimeModel struct {
	Endpoint        string `yaml:"endpoint"`
	UpstreamModel   string `yaml:"upstream-model"`
	InjectionPrompt string `yaml:"injection-prompt"`
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
		if k == "models" || k == "logging" {
			continue
		}
		// The key itself is not named: error text reaches logs verbatim, and
		// a pasted credential can land in a key position just as well as a
		// value position.
		return nil, errors.New("unknown top-level key (only models and logging are legal)")
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
		m, err := buildModel(name, rf.Models[rawName])
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

	level, err := ParseLogLevel(rf.Logging.Level)
	if err != nil {
		return nil, fmt.Errorf("decode config: %w", err)
	}

	return &Snapshot{models: models, logLevel: level}, nil
}

func buildModel(name string, rm runtimeModel) (Model, error) {
	if strings.TrimSpace(rm.Endpoint) == "" {
		return Model{}, errors.New("endpoint is required")
	}
	u, err := url.Parse(rm.Endpoint)
	if err != nil {
		// url.Parse errors quote the raw input, query string included; error
		// text reaches logs verbatim, so the raw endpoint must not. A
		// *url.Error is unwrapped to its cause, which never carries the
		// input; a bare stdlib message (e.g. control-character rejection)
		// has no echo and passes through. If the text ever does contain the
		// input's quoted form, it is dropped entirely — the sanitizer fails
		// closed, never open.
		var ue *url.Error
		switch {
		case errors.As(err, &ue):
			return Model{}, fmt.Errorf("endpoint: %s", ue.Err)
		case strings.Contains(err.Error(), strconv.Quote(rm.Endpoint)):
			return Model{}, errors.New("endpoint: invalid URL (input redacted)")
		default:
			return Model{}, fmt.Errorf("endpoint: %s", err)
		}
	}
	if (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		// Scheme and host are operator input like any other; a botched paste
		// into the endpoint position can carry a credential into either.
		return Model{}, errors.New("endpoint scheme and host must be http(s) with a host (input redacted)")
	}
	if u.User != nil {
		return Model{}, errors.New("endpoint must not contain credentials")
	}
	if u.Fragment != "" {
		// A fragment is never sent to a server; accepting one would silently
		// ignore part of the configured endpoint.
		return Model{}, errors.New("endpoint must not contain a fragment")
	}
	if strings.TrimSpace(rm.UpstreamModel) == "" {
		return Model{}, errors.New("upstream-model is required")
	}
	return Model{
		Public:          name,
		Endpoint:        u,
		UpstreamModel:   rm.UpstreamModel,
		InjectionPrompt: rm.InjectionPrompt,
	}, nil
}
