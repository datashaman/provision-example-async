package rabbit

import (
	"os"
	"path/filepath"
	"testing"
)

func TestReadURLReadsOneTrimmedCredential(t *testing.T) {
	path := filepath.Join(t.TempDir(), "rabbitmq-url")
	if err := os.WriteFile(path, []byte("amqp://user:secret@example.invalid/vhost\n"), 0600); err != nil {
		t.Fatal(err)
	}
	got, err := readURL(path)
	if err != nil {
		t.Fatal(err)
	}
	if got != "amqp://user:secret@example.invalid/vhost" {
		t.Fatalf("URL = %q", got)
	}
}

func TestReadURLRejectsEmptyCredential(t *testing.T) {
	path := filepath.Join(t.TempDir(), "rabbitmq-url")
	if err := os.WriteFile(path, []byte("\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := readURL(path); err == nil {
		t.Fatal("expected an error")
	}
}
