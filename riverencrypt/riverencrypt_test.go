package riverencrypt

import (
	"context"
	"testing"

	"github.com/riverqueue/river/rivertype"
)

type fakeEncryptor struct{}

func (fakeEncryptor) Encrypt(plain []byte) []byte { return append([]byte("cipher:"), plain...) }
func (fakeEncryptor) Decrypt(cipher []byte) ([]byte, error) {
	return append([]byte("plain:"), cipher...), nil
}

func TestEncryptHookIncludeExcludeAndDecryptOnly(t *testing.T) {
	hook := NewEncryptHookConfig(&EncryptHookConfig{Encryptor: fakeEncryptor{}, JobKindsInclude: []string{"included"}})
	included := &rivertype.JobInsertParams{Kind: "included", EncodedArgs: []byte(`{"a":1}`)}
	if err := hook.InsertBegin(context.Background(), included); err != nil {
		t.Fatalf("InsertBegin included: %v", err)
	}
	if string(included.EncodedArgs) == `{"a":1}` {
		t.Fatal("expected included job args to be encrypted")
	}
	excluded := &rivertype.JobInsertParams{Kind: "other", EncodedArgs: []byte(`{"a":1}`)}
	if err := hook.InsertBegin(context.Background(), excluded); err != nil {
		t.Fatalf("InsertBegin excluded: %v", err)
	}
	if string(excluded.EncodedArgs) != `{"a":1}` {
		t.Fatal("expected excluded job args to remain plaintext")
	}

	decryptOnly := NewEncryptHookConfig(&EncryptHookConfig{DecryptOnly: true, Encryptor: fakeEncryptor{}})
	params := &rivertype.JobInsertParams{Kind: "included", EncodedArgs: []byte(`{"a":1}`)}
	if err := decryptOnly.InsertBegin(context.Background(), params); err != nil {
		t.Fatalf("InsertBegin decrypt only: %v", err)
	}
	if string(params.EncodedArgs) != `{"a":1}` {
		t.Fatal("decrypt-only hook should not encrypt on insert")
	}
}
