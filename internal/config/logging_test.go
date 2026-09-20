package config

import (
	"strings"
	"testing"

	"github.com/rs/zerolog"
)

func TestParseLogLevel(t *testing.T) {
	cases := []struct {
		name  string
		in    string
		want  zerolog.Level
		valid bool
	}{
		{"empty selects default", "", DefaultLogLevel, true},
		{"debug", "debug", zerolog.DebugLevel, true},
		{"info", "info", zerolog.InfoLevel, true},
		{"warn", "warn", zerolog.WarnLevel, true},
		{"warning alias", "warning", zerolog.WarnLevel, true},
		{"error", "error", zerolog.ErrorLevel, true},
		{"case-insensitive", "DEBUG", zerolog.DebugLevel, true},
		{"mixed case warning", "Warning", zerolog.WarnLevel, true},
		{"surrounding whitespace", "  error \n", zerolog.ErrorLevel, true},
		{"trace is not offered", "trace", DefaultLogLevel, false},
		{"unknown word", "verbose", DefaultLogLevel, false},
		{"number", "3", DefaultLogLevel, false},
		{"empty-ish spaces only", "   ", DefaultLogLevel, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParseLogLevel(tc.in)
			if tc.valid && err != nil {
				t.Fatalf("ParseLogLevel(%q): unexpected error %v", tc.in, err)
			}
			if !tc.valid {
				if err == nil {
					t.Fatalf("ParseLogLevel(%q): expected rejection", tc.in)
				}
				if !strings.Contains(err.Error(), "logging.level") {
					t.Errorf("error %v does not name the offending key", err)
				}
				return
			}
			if got != tc.want {
				t.Errorf("ParseLogLevel(%q) = %v, want %v", tc.in, got, tc.want)
			}
		})
	}
}

func TestLoadRuntimeLoggingLevel(t *testing.T) {
	cases := []struct {
		name string
		yaml string
		want zerolog.Level
	}{
		{"absent section defaults to info", validRuntime(), zerolog.InfoLevel},
		{"null section defaults to info", "logging:\n" + validRuntime(), zerolog.InfoLevel},
		{"debug", "logging:\n  level: debug\n" + validRuntime(), zerolog.DebugLevel},
		{"warning alias", "logging:\n  level: warning\n" + validRuntime(), zerolog.WarnLevel},
		{"case-insensitive", "logging:\n  level: ERROR\n" + validRuntime(), zerolog.ErrorLevel},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := mustSnapshot(t, tc.yaml).LogLevel(); got != tc.want {
				t.Errorf("LogLevel() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestLoadRuntimeRejectsBadLogLevel(t *testing.T) {
	// A mistyped level must reject the whole file — landing on the
	// last-known-good path beats silently switching to a default.
	_, err := LoadRuntime([]byte("logging:\n  level: trace\n" + validRuntime()))
	if err == nil {
		t.Fatal("expected rejection of logging.level: trace")
	}
	if !strings.Contains(err.Error(), "logging.level") {
		t.Errorf("error %v does not name logging.level", err)
	}
}

func TestLoadRuntimeLoggingSectionStrictness(t *testing.T) {
	cases := []struct {
		name string
		yaml string
	}{
		{"unknown key inside logging", "logging:\n  format: json\n" + validRuntime()},
		{"uppercase level key", "logging:\n  LEVEL: debug\n" + validRuntime()},
		{"bootstrap key beside logging", "listen: :9000\nlogging:\n  level: debug\n" + validRuntime()},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := LoadRuntime([]byte(tc.yaml)); err == nil {
				t.Fatalf("expected strict rejection of %s", tc.name)
			}
		})
	}
}

func TestSnapshotLen(t *testing.T) {
	if got := mustSnapshot(t, validRuntime()).Len(); got != 1 {
		t.Errorf("Len() = %d, want 1", got)
	}
}
