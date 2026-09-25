package worker

import (
	"bufio"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/datashaman/provision-example-async/internal/example"
)

const (
	testApplicationRevision example.ApplicationRevision = "application-revision-a"
	testTaskArtifact        example.ArtifactDigest      = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	testWorkerArtifact      example.ArtifactDigest      = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
)

func TestProcessorRecordsOneEffectForRepeatedDelivery(t *testing.T) {
	dir := t.TempDir()
	recorder, err := NewRecorder(filepath.Join(dir, "evidence.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	defer recorder.Close()
	processor, err := NewProcessor(filepath.Join(dir, "ledger"), filepath.Join(dir, "holds"), recorder, testApplicationRevision, testWorkerArtifact)
	if err != nil {
		t.Fatal(err)
	}
	message, _ := example.NewMessage(testApplicationRevision, testTaskArtifact, "invocation-a", 1, example.BehaviorProcess)

	first := processor.Process(context.Background(), message)
	second := processor.Process(context.Background(), message)
	if first.Disposition != Acknowledge || first.Duplicate {
		t.Fatalf("first result = %#v", first)
	}
	if second.Disposition != Acknowledge || !second.Duplicate {
		t.Fatalf("second result = %#v", second)
	}
	entries, err := os.ReadDir(filepath.Join(dir, "ledger", "processed"))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("processed effects = %d; want 1", len(entries))
	}
}

func TestProcessorHoldCompletesAfterExplicitRelease(t *testing.T) {
	dir := t.TempDir()
	recorder, _ := NewRecorder(filepath.Join(dir, "evidence.jsonl"))
	defer recorder.Close()
	processor, _ := NewProcessor(filepath.Join(dir, "ledger"), filepath.Join(dir, "holds"), recorder, testApplicationRevision, testWorkerArtifact)
	message, _ := example.NewMessage(testApplicationRevision, testTaskArtifact, "invocation-a", 1, example.BehaviorHold)
	result := make(chan Result, 1)
	go func() { result <- processor.Process(context.Background(), message) }()

	select {
	case got := <-result:
		t.Fatalf("hold returned before release: %#v", got)
	case <-time.After(100 * time.Millisecond):
	}
	if err := processor.Release(message.ID); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-result:
		if got.Disposition != Acknowledge {
			t.Fatalf("released hold = %#v", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("hold did not complete after release")
	}
}

func TestProcessorHoldIsSafelyRequeuedOnCancellation(t *testing.T) {
	dir := t.TempDir()
	recorder, _ := NewRecorder(filepath.Join(dir, "evidence.jsonl"))
	defer recorder.Close()
	processor, _ := NewProcessor(filepath.Join(dir, "ledger"), filepath.Join(dir, "holds"), recorder, testApplicationRevision, testWorkerArtifact)
	message, _ := example.NewMessage(testApplicationRevision, testTaskArtifact, "invocation-a", 1, example.BehaviorHold)
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan Result, 1)
	go func() { result <- processor.Process(ctx, message) }()
	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case got := <-result:
		if got.Disposition != Requeue {
			t.Fatalf("cancelled hold = %#v; want requeue", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("cancelled hold did not return")
	}
}

func TestProcessorFailsOnceThenProcessesWithSameIdentity(t *testing.T) {
	dir := t.TempDir()
	recorder, _ := NewRecorder(filepath.Join(dir, "evidence.jsonl"))
	defer recorder.Close()
	processor, _ := NewProcessor(filepath.Join(dir, "ledger"), filepath.Join(dir, "holds"), recorder, testApplicationRevision, testWorkerArtifact)
	message, _ := example.NewMessage(testApplicationRevision, testTaskArtifact, "invocation-a", 1, example.BehaviorFailOnce)

	first := processor.Process(context.Background(), message)
	second := processor.Process(context.Background(), message)
	if first.Disposition != Requeue || second.Disposition != Acknowledge {
		t.Fatalf("results = %#v then %#v", first, second)
	}
	if first.MessageID != second.MessageID || first.MessageID != message.ID {
		t.Fatalf("identity changed across retry: %#v then %#v", first, second)
	}
}

func TestProcessorRejectsMalformedPayloadWithoutRequeue(t *testing.T) {
	dir := t.TempDir()
	recorder, _ := NewRecorder(filepath.Join(dir, "evidence.jsonl"))
	defer recorder.Close()
	processor, _ := NewProcessor(filepath.Join(dir, "ledger"), filepath.Join(dir, "holds"), recorder, testApplicationRevision, testWorkerArtifact)
	result := processor.ProcessPayload(context.Background(), []byte("not json"), "broker-id")
	if result.Disposition != Reject {
		t.Fatalf("result = %#v; want reject", result)
	}

	data, err := os.ReadFile(filepath.Join(dir, "evidence.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	var event Event
	if err := json.Unmarshal(data[:len(data)-1], &event); err != nil {
		t.Fatal(err)
	}
	if event.Event != "invalid_message" || event.MessageID != "broker-id" {
		t.Fatalf("event = %#v", event)
	}
}

func TestCandidateProcessesCrossRevisionRedeliveryWithStableIdentity(t *testing.T) {
	dir := t.TempDir()
	evidencePath := filepath.Join(dir, "evidence.jsonl")
	recorder, _ := NewRecorder(evidencePath)
	defer recorder.Close()
	candidateRevision := example.ApplicationRevision("application-revision-b")
	candidateArtifact := example.ArtifactDigest("sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc")
	processor, _ := NewProcessor(filepath.Join(dir, "ledger"), filepath.Join(dir, "holds"), recorder, candidateRevision, candidateArtifact)
	message, _ := example.NewMessage(testApplicationRevision, testTaskArtifact, "handoff-invocation", 1, example.BehaviorProcess)

	first := processor.Process(context.Background(), message)
	redelivery := processor.Process(context.Background(), message)
	if first.Err != nil || first.Disposition != Acknowledge || first.MessageID != message.ID {
		t.Fatalf("first candidate result = %#v; want acknowledgement for %s", first, message.ID)
	}
	if redelivery.Err != nil || redelivery.Disposition != Acknowledge || !redelivery.Duplicate || redelivery.MessageID != message.ID {
		t.Fatalf("candidate redelivery result = %#v; want duplicate acknowledgement for %s", redelivery, message.ID)
	}
	entries, err := os.ReadDir(filepath.Join(dir, "ledger", "processed"))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("cross-revision redelivery produced %d effects; want one", len(entries))
	}

	file, err := os.Open(evidencePath)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	found := map[string]bool{}
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var event Event
		if err := json.Unmarshal(scanner.Bytes(), &event); err != nil {
			t.Fatal(err)
		}
		if event.Event != "processed" && event.Event != "duplicate_ignored" {
			continue
		}
		if event.MessageID != message.ID || event.ProducerApplicationRevision != testApplicationRevision || event.TaskArtifactDigest != testTaskArtifact || event.WorkerApplicationRevision != candidateRevision || event.WorkerArtifactDigest != candidateArtifact {
			t.Fatalf("cross-revision evidence = %#v", event)
		}
		found[event.Event] = true
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	if !found["processed"] || !found["duplicate_ignored"] {
		t.Fatalf("cross-revision evidence events = %#v", found)
	}
}
