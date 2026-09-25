package worker

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/datashaman/provision-example-async/internal/example"
)

func TestProcessorRecordsOneEffectForRepeatedDelivery(t *testing.T) {
	dir := t.TempDir()
	recorder, err := NewRecorder(filepath.Join(dir, "evidence.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	defer recorder.Close()
	processor, err := NewProcessor(filepath.Join(dir, "ledger"), filepath.Join(dir, "holds"), recorder, "worker-revision-a")
	if err != nil {
		t.Fatal(err)
	}
	message, _ := example.NewMessage("task-revision-a", "invocation-a", 1, example.BehaviorProcess)

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
	processor, _ := NewProcessor(filepath.Join(dir, "ledger"), filepath.Join(dir, "holds"), recorder, "worker-revision-a")
	message, _ := example.NewMessage("task-revision-a", "invocation-a", 1, example.BehaviorHold)
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
	processor, _ := NewProcessor(filepath.Join(dir, "ledger"), filepath.Join(dir, "holds"), recorder, "worker-revision-a")
	message, _ := example.NewMessage("task-revision-a", "invocation-a", 1, example.BehaviorHold)
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
	processor, _ := NewProcessor(filepath.Join(dir, "ledger"), filepath.Join(dir, "holds"), recorder, "worker-revision-a")
	message, _ := example.NewMessage("task-revision-a", "invocation-a", 1, example.BehaviorFailOnce)

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
	processor, _ := NewProcessor(filepath.Join(dir, "ledger"), filepath.Join(dir, "holds"), recorder, "worker-revision-a")
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
