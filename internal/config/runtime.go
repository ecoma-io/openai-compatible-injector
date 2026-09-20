package config

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strings"

	"gopkg.in/yaml.v3"
)

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
	// Top-level keys are validated against the raw YAML first: a strict
	// struct decode alone would report an unknown key without naming it, and
	// the bootstrap-plane rule is absolute — a runtime file must reject any
	// bootstrap key, regardless of its value's shape.
	var raw map[string]any
	if err := yaml.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	for k := range raw {
		if k != "models" && k != "logging" {
			return nil, fmt.Errorf("decode config: unknown top-level key %q", k)
		}
	}
	var rf runtimeFile
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	// An empty document decodes as io.EOF; that is not a malformed file but
	// an empty models table, which the emptiness check below rejects with
	// the honest message.
	if err := dec.Decode(&rf); err != nil && !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("decode config: %w", err)
	}

	models := make(map[string]Model, len(rf.Models))
	seen := make(map[string]struct{}, len(rf.Models))
	for name, rm := range rf.Models {
		name = strings.TrimSpace(name)
		if name == "" {
			return nil, errors.New("model name must not be empty")
		}
		if _, dup := seen[name]; dup {
			// `"  a":` and `a:` are distinct YAML keys that trim to the same
			// model name; which one wins must not depend on map iteration
			// order, so an ambiguous file is a reject, not a coin toss.
			return nil, fmt.Errorf("model %q: name collides with another entry after trimming whitespace", name)
		}
		seen[name] = struct{}{}
		m, err := buildModel(name, rm)
		if err != nil {
			return nil, fmt.Errorf("model %q: %w", name, err)
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
		return Model{}, fmt.Errorf("endpoint: %w", err)
	}
	if (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return Model{}, fmt.Errorf("endpoint %q must be an http(s) URL with a host", rm.Endpoint)
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
