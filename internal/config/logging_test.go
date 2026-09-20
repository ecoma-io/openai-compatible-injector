package config

import (
	"encoding/json"
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

// hookEvents decodes the captured lines into the objects whose message
// equals msg, failing on any non-JSON line.
func hookEvents(t *testing.T, buf *syncBuffer, msg string) []map[string]any {
	t.Helper()
	var found []map[string]any
	for _, line := range strings.Split(buf.String(), "\n") {
		if line == "" {
			continue
		}
		var ev map[string]any
		if err := json.Unmarshal([]byte(line), &ev); err != nil {
			t.Fatalf("log line is not JSON: %q (%v)", line, err)
		}
		if ev["message"] == msg {
			found = append(found, ev)
		}
	}
	return found
}

// TestLogLevelHookObservableAtEveryLevel pins the reload-ack contract: the
// log_level_applied event is emitted at the level it announces, so every
// transition yields a visible acknowledgement — an ack at any fixed
// severity would vanish on exactly the transitions an operator needs
// confirmed (anything->error drops an info/warn ack; warn->info drops a
// debug ack), and config_reloaded (info, before the swap) is suppressed
// while the old level is warn or above. The hook mutates the process-global
// zerolog level and is therefore never run in parallel.
func TestLogLevelHookObservableAtEveryLevel(t *testing.T) {
	previous := zerolog.GlobalLevel()
	defer zerolog.SetGlobalLevel(previous)

	levels := map[string]zerolog.Level{
		"debug": zerolog.DebugLevel,
		"info":  zerolog.InfoLevel,
		"warn":  zerolog.WarnLevel,
		"error": zerolog.ErrorLevel,
	}
	for fromName, from := range levels {
		for toName, to := range levels {
			t.Run(fromName+"_to_"+toName, func(t *testing.T) {
				zerolog.SetGlobalLevel(from)
				next := mustSnapshot(t, "logging:\n  level: "+toName+"\n"+validRuntime())
				if next.LogLevel() != to {
					t.Fatalf("snapshot level = %v, want %v", next.LogLevel(), to)
				}

				var buf syncBuffer
				// TraceLevel logger: in production the global level is the only
				// live control (the base logger stays maximally permissive), so
				// visibility of the ack here is decided exactly as it is in the
				// running process.
				LogLevelHook(zerolog.New(&buf).Level(zerolog.TraceLevel))(next)

				evs := hookEvents(t, &buf, "log_level_applied")
				if len(evs) != 1 {
					t.Fatalf("log_level_applied logged %d times, want exactly 1: %s", len(evs), buf.String())
				}
				if evs[0]["level"] != toName {
					t.Errorf("event level = %v, want %v (the level it announces)", evs[0]["level"], toName)
				}
				if evs[0]["log_level"] != toName {
					t.Errorf("log_level = %v, want %v", evs[0]["log_level"], toName)
				}
				if evs[0]["previous_level"] != fromName {
					t.Errorf("previous_level = %v, want %v", evs[0]["previous_level"], fromName)
				}
				if _, ok := evs[0]["generation"]; !ok {
					t.Error("generation missing")
				}
			})
		}
	}
}

// TestLogLevelHookWarningAliasAcknowledgesAsWarn pins the alias's ack: a
// file spelling `warning` publishes the warn level and the ack carries the
// canonical name at warn severity.
func TestLogLevelHookWarningAliasAcknowledgesAsWarn(t *testing.T) {
	previous := zerolog.GlobalLevel()
	defer zerolog.SetGlobalLevel(previous)
	zerolog.SetGlobalLevel(zerolog.InfoLevel)

	next := mustSnapshot(t, "logging:\n  level: warning\n"+validRuntime())
	if next.LogLevel() != zerolog.WarnLevel {
		t.Fatalf("snapshot level = %v, want warn", next.LogLevel())
	}

	var buf syncBuffer
	LogLevelHook(zerolog.New(&buf).Level(zerolog.TraceLevel))(next)

	evs := hookEvents(t, &buf, "log_level_applied")
	if len(evs) != 1 {
		t.Fatalf("log_level_applied logged %d times, want 1: %s", len(evs), buf.String())
	}
	if evs[0]["level"] != "warn" || evs[0]["log_level"] != "warn" {
		t.Errorf("ack = level %v / log_level %v, want warn/warn", evs[0]["level"], evs[0]["log_level"])
	}
	if evs[0]["previous_level"] != "info" {
		t.Errorf("previous_level = %v, want info", evs[0]["previous_level"])
	}
}
