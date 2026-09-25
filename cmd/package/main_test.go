package main

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestPackIsReproducible(t *testing.T) {
	dir := t.TempDir()
	input := filepath.Join(dir, "input")
	if err := os.WriteFile(input, []byte("example executable"), 0755); err != nil {
		t.Fatal(err)
	}
	first := filepath.Join(dir, "first.tar.gz")
	second := filepath.Join(dir, "second.tar.gz")
	if err := pack(input, "provision-example-async-task", first); err != nil {
		t.Fatal(err)
	}
	if err := pack(input, "provision-example-async-task", second); err != nil {
		t.Fatal(err)
	}
	firstData, _ := os.ReadFile(first)
	secondData, _ := os.ReadFile(second)
	if !bytes.Equal(firstData, secondData) {
		t.Fatal("identical inputs produced different archives")
	}
	info, err := os.Stat(first)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0644 {
		t.Fatalf("archive mode = %o; want 644", info.Mode().Perm())
	}
}

func TestPackRejectsNestedExecutableName(t *testing.T) {
	dir := t.TempDir()
	input := filepath.Join(dir, "input")
	if err := os.WriteFile(input, []byte("example executable"), 0755); err != nil {
		t.Fatal(err)
	}
	if err := pack(input, "nested/example", filepath.Join(dir, "output.tar.gz")); err == nil {
		t.Fatal("expected nested executable name to fail")
	}
}
