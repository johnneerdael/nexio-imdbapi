package auth

import (
	"strings"
	"testing"
)

func TestGenerateAPIKeyAndValidate(t *testing.T) {
	t.Parallel()

	key, stored, err := GenerateAPIKey("super-secret-pepper", strings.NewReader(strings.Repeat("a", 64)))
	if err != nil {
		t.Fatalf("GenerateAPIKey() error = %v", err)
	}

	if key == "" {
		t.Fatal("expected generated plaintext key")
	}
	if stored.Prefix == "" {
		t.Fatal("expected stored prefix")
	}
	if stored.Hash == "" {
		t.Fatal("expected stored hash")
	}
	if !strings.HasPrefix(key, stored.Prefix+".") {
		t.Fatalf("expected key to start with prefix %q, got %q", stored.Prefix, key)
	}
	if !ValidateAPIKey(key, "super-secret-pepper", stored) {
		t.Fatal("expected generated key to validate")
	}
}

func TestValidateAPIKeyRejectsWrongKeyAndMalformedInput(t *testing.T) {
	t.Parallel()

	key, stored, err := GenerateAPIKey("pepper", strings.NewReader(strings.Repeat("b", 64)))
	if err != nil {
		t.Fatalf("GenerateAPIKey() error = %v", err)
	}

	if ValidateAPIKey(key, "wrong-pepper", stored) {
		t.Fatal("expected validation to fail with wrong pepper")
	}
	if ValidateAPIKey("not-a-valid-key", "pepper", stored) {
		t.Fatal("expected malformed key to fail validation")
	}
	if ValidateAPIKey(stored.Prefix+".different-secret", "pepper", stored) {
		t.Fatal("expected different secret to fail validation")
	}
}
