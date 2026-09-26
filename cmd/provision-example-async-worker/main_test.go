package main

import (
	"bytes"
	"context"
	"strings"
	"testing"
)

func TestRunRejectsIncompleteConfigurationBeforeConnecting(t *testing.T) {
	var stderr bytes.Buffer
	var stdout bytes.Buffer
	err := run(context.Background(), []string{
		"--queue", "jobs",
		"--application-revision", "application-revision-a",
		"--artifact-digest", "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
	}, &stdout, &stderr)
	if err == nil || !strings.Contains(err.Error(), "gate file") {
		t.Fatalf("error = %v", err)
	}
}

func TestRunRequiresEvidenceAndLedgerPathsBeforeConnecting(t *testing.T) {
	var stderr bytes.Buffer
	var stdout bytes.Buffer
	err := run(context.Background(), []string{
		"--queue", "jobs",
		"--application-revision", "application-revision-a",
		"--artifact-digest", "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		"--gate-file", "/tmp/gate", "--state-file", "/tmp/state",
	}, &stdout, &stderr)
	if err == nil || !strings.Contains(err.Error(), "evidence file") {
		t.Fatalf("error = %v", err)
	}
}

func TestRunPrintsVersionWithoutRuntimeConfiguration(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if err := run(context.Background(), []string{"--version"}, &stdout, &stderr); err != nil {
		t.Fatal(err)
	}
	if stdout.String() != "0.2.0\n" || stderr.Len() != 0 {
		t.Fatalf("version output = stdout %q, stderr %q", stdout.String(), stderr.String())
	}
}
