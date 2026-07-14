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

func TestDecryptArgsEdgeCases(t *testing.T) {
	plain := []byte(`{"plain":true}`)
	got, err := DecryptArgs(fakeEncryptor{}, plain)
	if err != nil {
		t.Fatalf("DecryptArgs plaintext: %v", err)
	}
	if string(got) != string(plain) {
		t.Fatalf("plaintext should pass through unchanged, got %s", got)
	}

	got, err = DecryptArgs(nil, wrapCiphertext([]byte("cipher")))
	if err != nil {
		t.Fatalf("DecryptArgs nil encryptor: %v", err)
	}
	if string(got) != string(wrapCiphertext([]byte("cipher"))) {
		t.Fatal("nil encryptor should leave encrypted wrapper unchanged")
	}

	if _, err := DecryptArgs(fakeEncryptor{}, []byte(`{"river_encrypted_args":"v1"}`)); err == nil {
		t.Fatal("expected missing ciphertext error")
	}
	if _, err := DecryptArgs(fakeEncryptor{}, []byte(`{"river_encrypted_args":"v1","ciphertext":"not-base64!!!"}`)); err == nil {
		t.Fatal("expected malformed base64 error")
	}
}

func TestEncryptHookWorkBeginDecryptsAndPropagatesErrors(t *testing.T) {
	hook := NewEncryptHook(fakeEncryptor{})
	job := &rivertype.JobRow{Kind: "decrypt-me", EncodedArgs: wrapCiphertext([]byte("ciphertext"))}
	if err := hook.WorkBegin(context.Background(), job); err != nil {
		t.Fatalf("WorkBegin decrypt: %v", err)
	}
	if string(job.EncodedArgs) != "plain:ciphertext" {
		t.Fatalf("unexpected decrypted args: %s", job.EncodedArgs)
	}

	badJob := &rivertype.JobRow{Kind: "decrypt-me", EncodedArgs: []byte(`{"river_encrypted_args":"v1"}`)}
	if err := hook.WorkBegin(context.Background(), badJob); err == nil {
		t.Fatal("expected WorkBegin to propagate decrypt error")
	}

	excluded := NewEncryptHookConfig(&EncryptHookConfig{Encryptor: fakeEncryptor{}, JobKindsExclude: []string{"excluded"}})
	excludedJob := &rivertype.JobRow{Kind: "excluded", EncodedArgs: wrapCiphertext([]byte("cipher"))}
	if err := excluded.WorkBegin(context.Background(), excludedJob); err != nil {
		t.Fatalf("excluded WorkBegin: %v", err)
	}
	if string(excludedJob.EncodedArgs) != string(wrapCiphertext([]byte("cipher"))) {
		t.Fatal("excluded job should not be decrypted")
	}
}
