package config

import (
	"strings"
	"testing"
	"time"
)

func envOf(m map[string]string) func(string) string {
	return func(k string) string { return m[k] }
}

func TestLoadBootstrapDefaults(t *testing.T) {
	b, err := LoadBootstrap(envOf(nil))
	if err != nil {
		t.Fatalf("LoadBootstrap with empty env: %v", err)
	}
	if b.Listen != DefaultListen {
		t.Errorf("Listen = %q, want %q", b.Listen, DefaultListen)
	}
	if b.ConfigFile != DefaultConfigFile {
		t.Errorf("ConfigFile = %q, want %q", b.ConfigFile, DefaultConfigFile)
	}
	if b.PollInterval != DefaultPollInterval {
		t.Errorf("PollInterval = %v, want %v", b.PollInterval, DefaultPollInterval)
	}
	if b.ShutdownGrace != DefaultShutdownGrace {
		t.Errorf("ShutdownGrace = %v, want %v", b.ShutdownGrace, DefaultShutdownGrace)
	}
}

func TestLoadBootstrapOverrides(t *testing.T) {
	b, err := LoadBootstrap(envOf(map[string]string{
		"OAICR_LISTEN":               "127.0.0.1:9999",
		"OAICR_CONFIG_FILE":          "/tmp/cfg.yaml",
		"OAICR_CONFIG_POLL_INTERVAL": "250ms",
		"OAICR_SHUTDOWN_GRACE":       "5s",
		"OAICR_AUTH_DATABASE_URL":    "postgres://user:pass@127.0.0.1:5432/keys",
	}))
	if err != nil {
		t.Fatalf("LoadBootstrap: %v", err)
	}
	if b.Listen != "127.0.0.1:9999" || b.ConfigFile != "/tmp/cfg.yaml" {
		t.Errorf("got %+v", b)
	}
	if b.PollInterval != 250*time.Millisecond {
		t.Errorf("PollInterval = %v", b.PollInterval)
	}
	if b.ShutdownGrace != 5*time.Second {
		t.Errorf("ShutdownGrace = %v", b.ShutdownGrace)
	}
	if b.AuthDatabaseURL == "" {
		t.Error("AuthDatabaseURL empty; the env value must be carried through")
	}
}

// Auth mode is off by default: absent OAICR_AUTH_DATABASE_URL keeps static
// mode, where the runtime YAML api-key authenticates every client.
func TestLoadBootstrapDefaultsToStaticAuth(t *testing.T) {
	b, err := LoadBootstrap(envOf(map[string]string{"OAICR_AUTH_DATABASE_URL": ""}))
	if err != nil {
		t.Fatalf("LoadBootstrap: %v", err)
	}
	if b.AuthDatabaseURL != "" {
		t.Errorf("AuthDatabaseURL = non-empty; static mode must remain the default")
	}
}

func TestLoadBootstrapErrors(t *testing.T) {
	cases := []struct {
		name string
		env  map[string]string
		want string // substring of the joined error
	}{
		{"bad interval", map[string]string{"OAICR_CONFIG_POLL_INTERVAL": "soon"}, "OAICR_CONFIG_POLL_INTERVAL"},
		{"zero interval", map[string]string{"OAICR_CONFIG_POLL_INTERVAL": "0s"}, "positive"},
		{"negative interval", map[string]string{"OAICR_CONFIG_POLL_INTERVAL": "-1s"}, "positive"},
		{"bad grace", map[string]string{"OAICR_SHUTDOWN_GRACE": "abc"}, "OAICR_SHUTDOWN_GRACE"},
		{"negative grace", map[string]string{"OAICR_SHUTDOWN_GRACE": "-2s"}, "positive"},
		// Zero disables the graceful drain entirely — rejected, not honored.
		{"zero grace", map[string]string{"OAICR_SHUTDOWN_GRACE": "0s"}, "positive"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := LoadBootstrap(envOf(tc.env)); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want containing %q", err, tc.want)
			}
		})
	}

	// Multiple invalid values join into one error.
	_, err := LoadBootstrap(envOf(map[string]string{
		"OAICR_CONFIG_POLL_INTERVAL": "nope",
		"OAICR_SHUTDOWN_GRACE":       "also-nope",
	}))
	if err == nil {
		t.Fatal("expected joined error")
	}
	joined := err.Error()
	if !strings.Contains(joined, "OAICR_CONFIG_POLL_INTERVAL") || !strings.Contains(joined, "OAICR_SHUTDOWN_GRACE") {
		t.Errorf("joined error missing a field diagnosis: %s", joined)
	}
}

// TestLoadBootstrapIgnoresUnprefixedNames pins the namespace contract: the
// service consumes only OAICR_-prefixed variables. The quiet direction of a
// prefix rename is an operator still setting an unprefixed name and getting
// a silently honored override — here the unprefixed names must fall through
// to the defaults, so a stale deployment variable is inert, never half-read.
func TestLoadBootstrapIgnoresUnprefixedNames(t *testing.T) {
	b, err := LoadBootstrap(envOf(map[string]string{
		"LISTEN":               "127.0.0.1:1",
		"CONFIG_FILE":          "/elsewhere/config.yaml",
		"CONFIG_POLL_INTERVAL": "9s",
		"SHUTDOWN_GRACE":       "9s",
	}))
	if err != nil {
		t.Fatalf("LoadBootstrap: %v", err)
	}
	if b.Listen != DefaultListen {
		t.Errorf("Listen = %q, unprefixed name was honored", b.Listen)
	}
	if b.ConfigFile != DefaultConfigFile {
		t.Errorf("ConfigFile = %q, unprefixed name was honored", b.ConfigFile)
	}
	if b.PollInterval != DefaultPollInterval {
		t.Errorf("PollInterval = %v, unprefixed name was honored", b.PollInterval)
	}
	if b.ShutdownGrace != DefaultShutdownGrace {
		t.Errorf("ShutdownGrace = %v, unprefixed name was honored", b.ShutdownGrace)
	}
}

// TestLoadBootstrapErrorsDoNotEchoValues pins the credential rule on the
// bootstrap plane: env values are operator input too, and time.ParseDuration
// quotes its raw input — which would carry a botched paste into the fatal
// bootstrap_load_failed log. Only the value's length is reported.
func TestLoadBootstrapErrorsDoNotEchoValues(t *testing.T) {
	const marker = "SECRET_DURATION_PASTE"
	_, err := LoadBootstrap(envOf(map[string]string{
		"OAICR_CONFIG_POLL_INTERVAL": marker,
		"OAICR_SHUTDOWN_GRACE":       "https://h/v1?" + marker + "=x",
	}))
	if err == nil {
		t.Fatal("expected rejection")
	}
	if strings.Contains(err.Error(), marker) {
		t.Fatalf("bootstrap error echoes the raw value: %q", err.Error())
	}
	if strings.Contains(err.Error(), "time:") {
		t.Errorf("bootstrap error leaked the stdlib message (it quotes input): %q", err.Error())
	}
}
