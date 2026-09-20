package config

import (
	"bytes"
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/spf13/viper"
	"gopkg.in/yaml.v3"
)

// runtime file schema. Strictness has two layers: top-level keys are
// validated against the raw YAML (only "models" is legal — this is what
// keeps the bootstrap plane out of the runtime file: listen, config file,
// poll interval, ... any file that tries to define them fails validation,
// whatever their value's shape), and viper's UnmarshalExact rejects unknown
// keys inside each model entry.

type runtimeFile struct {
	Models map[string]runtimeModel `mapstructure:"models"`
}

type runtimeModel struct {
	Endpoint        string `mapstructure:"endpoint"`
	UpstreamModel   string `mapstructure:"upstream-model"`
	InjectionPrompt string `mapstructure:"injection-prompt"`
}

// LoadRuntime parses and validates runtime configuration bytes into an
// immutable Snapshot. Any error — parse failure, unknown key, invalid model
// entry — rejects the whole file; callers must keep serving the previous
// snapshot in that case.
func LoadRuntime(data []byte) (*Snapshot, error) {
	v := viper.New()
	v.SetConfigType("yaml")
	v.SetTypeByDefaultValue(true)
	if err := v.ReadConfig(bytes.NewReader(data)); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	// Top-level keys are validated against the raw YAML first: viper's
	// UnmarshalExact misses a key whose value is an empty map (an empty
	// `config:` or `listen:` block would slip through strict decode), and the
	// bootstrap-plane rule is absolute — a runtime file must reject any
	// bootstrap key, regardless of its value's shape.
	var raw map[string]any
	if err := yaml.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	for k := range raw {
		if k != "models" {
			return nil, fmt.Errorf("decode config: unknown top-level key %q", k)
		}
	}

	var rf runtimeFile
	if err := v.UnmarshalExact(&rf); err != nil {
		return nil, fmt.Errorf("decode config: %w", err)
	}

	models := make(map[string]Model, len(rf.Models))
	for name, rm := range rf.Models {
		name = strings.TrimSpace(name)
		if name == "" {
			return nil, errors.New("model name must not be empty")
		}
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

	return &Snapshot{models: models}, nil
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
