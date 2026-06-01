package riversecretbox

import (
	"bytes"
	"errors"
	"testing"

	"riverqueue.com/riverpro/riverencrypt"
)

func TestEncryptorRoundTripAndKeyRotation(t *testing.T) {
	var oldKey, newKey [32]byte
	oldKey[0] = 1
	newKey[0] = 2

	oldEnc := NewEncryptor(oldKey)
	cipher := oldEnc.Encrypt([]byte("secret"))

	rotated := NewEncryptor(newKey, oldKey)
	plain, err := rotated.Decrypt(cipher)
	if err != nil {
		t.Fatalf("Decrypt with rotated keys: %v", err)
	}
	if !bytes.Equal(plain, []byte("secret")) {
		t.Fatalf("unexpected plaintext: %q", plain)
	}

	wrong := NewEncryptor(newKey)
	if _, err := wrong.Decrypt(cipher); !errors.Is(err, riverencrypt.ErrNoKeyDecrypted) {
		t.Fatalf("expected ErrNoKeyDecrypted, got %v", err)
	}
}
