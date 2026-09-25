package main

import (
	"bytes"
	"context"
	"strings"
	"testing"
)

func TestRunRejectsIncompleteInvocationBeforeConnecting(t *testing.T) {
	var stdout, stderr bytes.Buffer
	err := run(context.Background(), []string{
		"--queue", "jobs",
		"--application-revision", "application-revision-a",
		"--artifact-digest", "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	}, &stdout, &stderr)
	if err == nil || !strings.Contains(err.Error(), "invocation ID is required") {
		t.Fatalf("error = %v", err)
	}
	if stdout.Len() != 0 {
		t.Fatalf("unexpected output: %s", stdout.Bytes())
	}
}

func TestRunRejectsUnsupportedBehaviorBeforeConnecting(t *testing.T) {
	var stdout, stderr bytes.Buffer
	err := run(context.Background(), []string{
		"--queue", "jobs",
		"--application-revision", "application-revision-a",
		"--artifact-digest", "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		"--invocation", "invocation-a",
		"--behavior", "mystery",
	}, &stdout, &stderr)
	if err == nil || !strings.Contains(err.Error(), "unsupported behavior") {
		t.Fatalf("error = %v", err)
	}
}
