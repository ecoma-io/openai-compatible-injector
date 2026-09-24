package credential

import (
	"errors"
	"strings"
	"testing"
)

func TestValidHeader(t *testing.T) {
	good := []string{"Authorization", "X-API-Key", "x-api-key", "X.Api-Key~1", "A"}
	for _, h := range good {
		if err := ValidHeader(h); err != nil {
			t.Errorf("ValidHeader(%q) = %v, want nil", h, err)
		}
	}
	bad := map[string]string{
		"":                                    "empty",
		"Bearer key":                          "space is not a token character",
		"X-Key\n":                             "newline injection",
		"X-Key\r":                             "CR injection",
		"X-Key(":                              "paren is not tchar",
		"X Key":                               "space",
		strings.Repeat("h", MaxHeaderBytes+1): "over bound",
	}
	for h, why := range bad {
		if err := ValidHeader(h); err == nil {
			t.Errorf("ValidHeader(%q) = nil, want error (%s)", h, why)
		}
	}
}

func TestValidHeaderRejectsWithoutEchoingInput(t *testing.T) {
	secret := "super-secret-header-value\nEVIL"
	err := ValidHeader(secret)
	if err == nil {
		t.Fatal("ValidHeader = nil, want error")
	}
	if strings.Contains(err.Error(), "super-secret") || strings.Contains(err.Error(), "EVIL") {
		t.Fatalf("validation error echoed the input: %q", err.Error())
	}
}

func TestValidPrefix(t *testing.T) {
	for _, p := range []string{"", "Bearer ", "Token", "api-key="} {
		if err := ValidPrefix(p); err != nil {
			t.Errorf("ValidPrefix(%q) = %v, want nil", p, err)
		}
	}
	for _, p := range []string{"Bad\n", "Bad\r", "Bad\x00", strings.Repeat("p", MaxPrefixBytes+1)} {
		if err := ValidPrefix(p); err == nil {
			t.Errorf("ValidPrefix(%q) = nil, want error", p)
		}
	}
}

func TestValidID(t *testing.T) {
	good := []string{"kilo-1", "key_2", "a", "A.b:c-d_e9", strings.Repeat("i", MaxIDBytes)}
	for _, id := range good {
		if err := ValidID(id); err != nil {
			t.Errorf("ValidID(%q) = %v, want nil", id, err)
		}
	}
	bad := map[string]string{
		"":                                "empty",
		"key 1":                           "space",
		"key\n1":                          "newline injection",
		"key/1":                           "slash",
		"kéy":                             "non-ASCII",
		strings.Repeat("i", MaxIDBytes+1): "over bound",
	}
	for id, why := range bad {
		if err := ValidID(id); err == nil {
			t.Errorf("ValidID(%q) = nil, want error (%s)", id, why)
		}
	}
}

func TestValidIDRejectsWithoutEchoingInput(t *testing.T) {
	evil := "id\n\"EVIL\""
	err := ValidID(evil)
	if err == nil {
		t.Fatal("ValidID = nil, want error")
	}
	if strings.Contains(err.Error(), "EVIL") {
		t.Fatalf("id validation echoed the input: %q", err.Error())
	}
}

func TestValidValue(t *testing.T) {
	if err := ValidValue("sk-some-secret"); err != nil {
		t.Errorf("ValidValue = %v, want nil", err)
	}
	for _, v := range []string{"", "bad\nvalue", "bad\x00", strings.Repeat("v", MaxValueBytes+1)} {
		if err := ValidValue(v); err == nil {
			t.Errorf("ValidValue(%q…) = nil, want error", v)
		}
	}
	// The bound is named; the value never is.
	err := ValidValue(strings.Repeat("v", MaxValueBytes+1))
	if err == nil || !strings.Contains(err.Error(), "at most") {
		t.Fatalf("over-bound value error = %v, want a bound-only message", err)
	}
	if err != nil && strings.Contains(err.Error(), "vvv") {
		t.Fatalf("value error echoed the value: %q", err.Error())
	}
}

func TestParseStrategy(t *testing.T) {
	s, err := ParseStrategy("round_robin")
	if err != nil || s != StrategyRoundRobin {
		t.Fatalf("ParseStrategy(round_robin) = %q, %v", s, err)
	}
	if _, err := ParseStrategy("least_connections"); err == nil {
		t.Fatal("ParseStrategy(least_connections) = nil error, want rejection")
	}
	if _, err := ParseStrategy(""); err == nil {
		t.Fatal("ParseStrategy(\"\") = nil error, want rejection")
	}
}

func TestValidateSpec(t *testing.T) {
	ok := Spec{
		Header:   "Authorization",
		Prefix:   "Bearer ",
		Strategy: StrategyRoundRobin,
		Keys:     []Key{{ID: "k1", Value: "v1"}, {ID: "k2", Value: "v2"}},
	}
	if err := ValidateSpec(ok); err != nil {
		t.Fatalf("ValidateSpec(valid) = %v", err)
	}

	emptyKeys := ok
	emptyKeys.Keys = nil
	if err := ValidateSpec(emptyKeys); !errors.Is(err, ErrInvalidCredentialConfig) {
		t.Errorf("ValidateSpec(no keys) = %v, want ErrInvalidCredentialConfig", err)
	}

	badStrategy := ok
	badStrategy.Strategy = "priority"
	if err := ValidateSpec(badStrategy); err == nil {
		t.Error("ValidateSpec(bad strategy) = nil, want error")
	}

	badHeader := ok
	badHeader.Header = "not a header"
	if err := ValidateSpec(badHeader); err == nil {
		t.Error("ValidateSpec(bad header) = nil, want error")
	}

	dup := Spec{
		Header:   "Authorization",
		Strategy: StrategyRoundRobin,
		Keys:     []Key{{ID: "k1", Value: "v1"}, {ID: "k1", Value: "v2"}},
	}
	if err := ValidateSpec(dup); err == nil {
		t.Error("ValidateSpec(duplicate ids) = nil, want error")
	}

	tooMany := ok
	tooMany.Keys = make([]Key, 0, MaxKeys+1)
	for i := 0; i <= MaxKeys; i++ {
		tooMany.Keys = append(tooMany.Keys, Key{ID: "k" + strings.Repeat("x", 10) + string(rune('a'+i%26)) + itoa(i), Value: "v"})
	}
	if err := ValidateSpec(tooMany); err == nil {
		t.Error("ValidateSpec(over key cap) = nil, want error")
	}
}

func TestSpecContentKey(t *testing.T) {
	base := Spec{
		Header:   "Authorization",
		Prefix:   "Bearer ",
		Strategy: StrategyRoundRobin,
		Keys:     []Key{{ID: "k1", Value: "s1"}, {ID: "k2", Value: "s2"}},
	}
	same := Spec{
		Header:   "Authorization",
		Prefix:   "Bearer ",
		Strategy: StrategyRoundRobin,
		Keys:     []Key{{ID: "k1", Value: "s1"}, {ID: "k2", Value: "s2"}},
	}
	if base.ContentKey() != same.ContentKey() {
		t.Fatal("identical specs produced different content keys")
	}

	mutations := map[string]func(*Spec){
		"header":      func(s *Spec) { s.Header = "X-API-Key" },
		"prefix":      func(s *Spec) { s.Prefix = "" },
		"key value":   func(s *Spec) { s.Keys[1].Value = "changed" },
		"key id":      func(s *Spec) { s.Keys[1].ID = "changed" },
		"key order":   func(s *Spec) { s.Keys[0], s.Keys[1] = s.Keys[1], s.Keys[0] },
		"key removed": func(s *Spec) { s.Keys = s.Keys[:1] },
	}
	for name, mutate := range mutations {
		m := cloneSpec(base) // Spec copies share the Keys backing array
		mutate(&m)
		if m.ContentKey() == base.ContentKey() {
			t.Errorf("mutation %q did not change the content key", name)
		}
	}
}

func cloneSpec(s Spec) Spec {
	keys := make([]Key, len(s.Keys))
	copy(keys, s.Keys)
	s.Keys = keys
	return s
}

// itoa avoids importing strconv for one test helper.
func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b [20]byte
	pos := len(b)
	for i > 0 {
		pos--
		b[pos] = byte('0' + i%10)
		i /= 10
	}
	return string(b[pos:])
}
