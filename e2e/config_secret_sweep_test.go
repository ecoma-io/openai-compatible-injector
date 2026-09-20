package e2e_test

import (
	"fmt"
	"strings"
	"testing"
)

// Rejection-path secret sweep, black-box: when a runtime config file is
// rejected — at boot (fatal) or on reload (WARN) — the error text reaches
// stderr verbatim. An operator pasting a credential-bearing endpoint into
// the wrong YAML position must not have that credential echoed into the
// logs. Every input-echoing position is planted with a distinct marker and
// asserted absent from the process output.

const (
	secretLevelURL    = "SECRET_LEVEL_URL"
	secretTopKeyURL   = "SECRET_TOPKEY_URL"
	secretModelKey    = "SECRET_MODELKEY_URL"
	secretScalar      = "SECRET_SCALAR_VALUE"
	secretSecondDoc   = "SECRET_SECONDDOC_KEY"
	secretEscapePath  = "SECRET_ESCAPE_QUERY"
	secretDuplicate   = "SECRET_DUPKEY_URL"
	secretAnchorAlias = "SECRET_ANCHOR_NAME"
	secretModelName   = "SECRET_MODEL_NAME_SHOULD_NOT_LEAK"
	secretTokenQuery  = "SECRET_TOKEN_QUERY"
	secretSchemePaste = "secretschemepaste" // lowercase, no underscore: url.Parse lowercases schemes, so the echoed form must still match the marker
)

// allConfigSecrets is the full marker inventory the sweep asserts absent.
var allConfigSecrets = []string{
	secretLevelURL, secretTopKeyURL, secretModelKey, secretScalar,
	secretSecondDoc, secretEscapePath, secretDuplicate, secretAnchorAlias,
	secretModelName, secretTokenQuery, secretSchemePaste,
}

// rejectedCaseCount is the number of echo positions rejectedYAML renders.
const rejectedCaseCount = 12

// rejectedYAML renders one rejected runtime file per echo position. Every
// case must stay driven by the reload sweep below: a marker that is declared
// but never planted is coverage that can only ever pass.
func rejectedYAML(i int) string {
	switch i {
	case 0: // secret URL as the logging.level value
		return "models:\n  m:\n    endpoint: http://127.0.0.1:1/v1\n    upstream-model: up\nlogging:\n  level: https://gw.example/v1?" + secretLevelURL + "=x\n"
	case 1: // secret URL as a top-level key
		return "https://gw.example/v1?" + secretTopKeyURL + "=x: true\nmodels:\n  m:\n    endpoint: http://127.0.0.1:1/v1\n    upstream-model: up\n"
	case 2: // secret URL as a key inside a model entry (strict decode)
		return "models:\n  m:\n    https://gw.example/v1?" + secretModelKey + "=x: \"\"\n    upstream-model: up\n"
	case 3: // secret scalar where the models table belongs (type error)
		return "models: " + secretScalar + "\n"
	case 4: // secret URL in a rejected endpoint (invalid path escape + secret query)
		return "models:\n  m:\n    endpoint: http://gw.example/v1%zz?" + secretEscapePath + "=x\n    upstream-model: up\n"
	case 5: // a --- separated second document (must be a rejection, never decoded)
		return "models:\n  m:\n    endpoint: http://127.0.0.1:1/v1\n    upstream-model: up\n---\n" + secretSecondDoc + ": true\n"
	case 6: // duplicate top-level key (yaml.v3 uniqueKeys; the error quotes the key)
		return secretDuplicate + ": true\nmodels:\n  m:\n    endpoint: http://127.0.0.1:1/v1\n    upstream-model: up\n" + secretDuplicate + ": false\n"
	case 7: // undefined alias whose anchor name carries a secret
		return "models:\n  m:\n    endpoint: *" + secretAnchorAlias + "\n    upstream-model: up\n"
	case 8: // secret model name on a failing entry (the entry is named by ordinal, never by key)
		return "models:\n  " + secretModelName + ":\n    endpoint: ftp://h/v1?token=" + secretTokenQuery + "\n    upstream-model: up\n"
	case 9: // two keys that trim to the same secret model name (collision reject)
		return "models:\n  " + secretModelName + ":\n    endpoint: http://127.0.0.1:1/v1\n    upstream-model: up\n  \" " + secretModelName + "\":\n    endpoint: http://127.0.0.1:1/v1\n    upstream-model: up\n"
	case 10: // paste in the endpoint's scheme position (scheme/host never echoed)
		return "models:\n  m:\n    endpoint: \"" + secretSchemePaste + ":x\"\n    upstream-model: up\n"
	case 11: // secret model name where a model entry value belongs (type error)
		return "models:\n  m: " + secretModelName + "\n"
	default:
		panic("no such rejected yaml case")
	}
}

// TestBootRejectionNeverEchoesSecrets: an invalid runtime file at boot fails
// the process with a fatal config_load_failed — whose error text must not
// carry the operator's pasted credential back into the logs. Every echo
// position is booted, not just one.
func TestBootRejectionNeverEchoesSecrets(t *testing.T) {
	for i := 0; i < rejectedCaseCount; i++ {
		code, stderr := startSubprocessExpectExit(t, startOpts{
			yaml: rejectedYAML(i),
		})
		if code != 1 {
			t.Fatalf("case %d: exit code = %d, want 1 for invalid initial config", i, code)
		}
		if !strings.Contains(stderr, "config_load_failed") {
			t.Fatalf("case %d: config_load_failed event missing from boot failure:\n%s", i, stderr)
		}
		for _, secret := range allConfigSecrets {
			if strings.Contains(stderr, secret) {
				t.Errorf("case %d: boot failure output echoes %q — rejection-path leak", i, secret)
			}
		}
	}
}

// TestReloadRejectionNeverEchoesSecrets: on a live process, each echo
// position is rejected with a WARN on the healthy->failing transition; the
// file is then healed with fresh valid content so the next case produces its
// own transition WARN (a persistently failing file downgrades to debug).
func TestReloadRejectionNeverEchoesSecrets(t *testing.T) {
	p := startSubprocess(t, startOpts{
		yaml:     "models:\n  m:\n    endpoint: http://127.0.0.1:1/v1\n    upstream-model: up\nlogging:\n  level: info\n",
		logLevel: "", // the YAML's logging section governs
	})

	reloadedEvents := func() int {
		return len(eventsWithMessage(parseLogEvents(t, p.stderr.String()), "config_reloaded"))
	}

	for i := 0; i < rejectedCaseCount; i++ {
		rewriteConfig(t, p.cfgPath, rejectedYAML(i))
		waitForEventCount(t, p, "config_reload_rejected", i+1)

		// Heal with fresh valid content (must differ from the rejected bytes
		// and from every earlier heal, so the hash changes and the reload
		// event re-fires for the next round).
		heal := fmt.Sprintf("models:\n  m:\n    endpoint: http://127.0.0.1:1/v1\n    upstream-model: up-heal-%d\nlogging:\n  level: info\n", i)
		rewriteConfig(t, p.cfgPath, heal)
		waitForEventCount(t, p, "config_reloaded", i+1)
	}

	if got := reloadedEvents(); got != rejectedCaseCount {
		t.Errorf("config_reloaded events = %d, want %d (every heal applied)", got, rejectedCaseCount)
	}

	for _, secret := range allConfigSecrets {
		if strings.Contains(p.stderr.String(), secret) {
			t.Errorf("reload rejection output echoes %q — rejection-path leak", secret)
		}
	}
}
