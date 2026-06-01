package riverencrypt

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/riverqueue/river"
	"github.com/riverqueue/river/rivertype"
)

var ErrNoKeyDecrypted = errors.New("no key successfully decrypted ciphertext")

type Encryptor interface {
	Decrypt(cipher []byte) ([]byte, error)
	Encrypt(plain []byte) []byte
}
type EncryptHookConfig struct {
	DecryptOnly     bool
	Encryptor       Encryptor
	JobKindsExclude []string
	JobKindsInclude []string
}
type EncryptHook struct {
	river.HookDefaults
	config  EncryptHookConfig
	include map[string]struct{}
	exclude map[string]struct{}
}

func NewEncryptHook(encryptor Encryptor) *EncryptHook {
	return NewEncryptHookConfig(&EncryptHookConfig{Encryptor: encryptor})
}
func NewEncryptHookConfig(config *EncryptHookConfig) *EncryptHook {
	if config == nil {
		config = &EncryptHookConfig{}
	}
	h := &EncryptHook{config: *config, include: map[string]struct{}{}, exclude: map[string]struct{}{}}
	for _, k := range config.JobKindsInclude {
		h.include[k] = struct{}{}
	}
	for _, k := range config.JobKindsExclude {
		h.exclude[k] = struct{}{}
	}
	return h
}
func (h *EncryptHook) applies(kind string) bool {
	if h == nil {
		return false
	}
	if len(h.include) > 0 {
		_, ok := h.include[kind]
		return ok
	}
	if len(h.exclude) > 0 {
		_, ok := h.exclude[kind]
		return !ok
	}
	return true
}
func (h *EncryptHook) InsertBegin(ctx context.Context, params *rivertype.JobInsertParams) error {
	_ = ctx
	if h == nil || h.config.Encryptor == nil || params == nil || h.config.DecryptOnly || !h.applies(params.Kind) {
		return nil
	}
	if len(params.EncodedArgs) == 0 || isEncrypted(params.EncodedArgs) {
		return nil
	}
	params.EncodedArgs = wrapCiphertext(h.config.Encryptor.Encrypt(params.EncodedArgs))
	return nil
}
func (h *EncryptHook) WorkBegin(ctx context.Context, job *rivertype.JobRow) error {
	_ = ctx
	if h == nil || h.config.Encryptor == nil || job == nil || !h.applies(job.Kind) {
		return nil
	}
	plain, err := DecryptArgs(h.config.Encryptor, job.EncodedArgs)
	if err != nil {
		return err
	}
	job.EncodedArgs = plain
	return nil
}
func isEncrypted(data []byte) bool {
	return bytes.Contains(data, []byte(`"river_encrypted_args"`)) || bytes.Contains(data, []byte(`"ciphertext"`))
}
func wrapCiphertext(cipher []byte) []byte {
	payload := map[string]string{"river_encrypted_args": "v1", "ciphertext": base64.StdEncoding.EncodeToString(cipher)}
	b, _ := json.Marshal(payload)
	return b
}
func DecryptArgs(encryptor Encryptor, encoded []byte) ([]byte, error) {
	if encryptor == nil || !isEncrypted(encoded) {
		return encoded, nil
	}
	var wrapper struct {
		Version    string `json:"river_encrypted_args"`
		Ciphertext string `json:"ciphertext"`
	}
	if err := json.Unmarshal(encoded, &wrapper); err != nil {
		return nil, err
	}
	if wrapper.Ciphertext == "" {
		return nil, fmt.Errorf("riverencrypt: missing ciphertext")
	}
	cipher, err := base64.StdEncoding.DecodeString(wrapper.Ciphertext)
	if err != nil {
		return nil, err
	}
	return encryptor.Decrypt(cipher)
}
