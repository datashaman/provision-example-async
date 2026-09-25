package main

import (
	"bytes"
	"context"
	"strings"
	"testing"
)

func TestRunRejectsIncompleteConfigurationBeforeConnecting(t *testing.T) {
	var stderr bytes.Buffer
	err := run(context.Background(), []string{
		"--queue", "jobs",
		"--application-revision", "application-revision-a",
		"--artifact-digest", "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
	}, &stderr)
	if err == nil || !strings.Contains(err.Error(), "gate file") {
		t.Fatalf("error = %v", err)
	}
}

func TestRunRequiresEvidenceAndLedgerPathsBeforeConnecting(t *testing.T) {
	var stderr bytes.Buffer
	err := run(context.Background(), []string{
		"--queue", "jobs",
		"--application-revision", "application-revision-a",
		"--artifact-digest", "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		"--gate-file", "/tmp/gate", "--state-file", "/tmp/state",
	}, &stderr)
	if err == nil || !strings.Contains(err.Error(), "evidence file") {
		t.Fatalf("error = %v", err)
	}
}
