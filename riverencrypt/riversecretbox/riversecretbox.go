package riversecretbox

import (
	"crypto/rand"
	"errors"
	"fmt"
	"io"

	"golang.org/x/crypto/nacl/secretbox"
	"riverqueue.com/riverpro/riverencrypt"
)

const keySize = 32

type Encryptor struct{ keys [][keySize]byte }

func NewEncryptor(keys ...[keySize]byte) *Encryptor {
	copied := make([][keySize]byte, len(keys))
	copy(copied, keys)
	return &Encryptor{keys: copied}
}
func (e *Encryptor) Encrypt(plain []byte) []byte {
	if e == nil || len(e.keys) == 0 {
		panic("riversecretbox: at least one key is required")
	}
	var nonce [24]byte
	if _, err := io.ReadFull(rand.Reader, nonce[:]); err != nil {
		panic(fmt.Sprintf("riversecretbox: nonce: %v", err))
	}
	out := make([]byte, 0, 24+len(plain)+secretbox.Overhead)
	out = append(out, nonce[:]...)
	return secretbox.Seal(out, plain, &nonce, &e.keys[0])
}
func (e *Encryptor) Decrypt(cipher []byte) ([]byte, error) {
	if e == nil || len(e.keys) == 0 {
		return nil, errors.New("riversecretbox: no keys configured")
	}
	if len(cipher) < 24+secretbox.Overhead {
		return nil, riverencrypt.ErrNoKeyDecrypted
	}
	var nonce [24]byte
	copy(nonce[:], cipher[:24])
	body := cipher[24:]
	for i := range e.keys {
		if out, ok := secretbox.Open(nil, body, &nonce, &e.keys[i]); ok {
			return out, nil
		}
	}
	return nil, riverencrypt.ErrNoKeyDecrypted
}
