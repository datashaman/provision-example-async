package worker

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/datashaman/provision-example-async/internal/example"
)

type fakeConsumer struct {
	mu         sync.Mutex
	starts     int
	cancels    int
	deliveries chan Delivery
}

func (f *fakeConsumer) Start(string) (<-chan Delivery, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.starts++
	return f.deliveries, nil
}

func (f *fakeConsumer) Cancel(string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.cancels++
	return nil
}

func (f *fakeConsumer) counts() (int, int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.starts, f.cancels
}

func TestRuntimeStartsGatedThenConsumesAndAcknowledges(t *testing.T) {
	dir := t.TempDir()
	gatePath := filepath.Join(dir, "gate")
	statePath := filepath.Join(dir, "state.json")
	recorder, _ := NewRecorder(filepath.Join(dir, "evidence.jsonl"))
	defer recorder.Close()
	processor, _ := NewProcessor(filepath.Join(dir, "ledger"), filepath.Join(dir, "holds"), recorder, testApplicationRevision, testWorkerArtifact)
	consumer := &fakeConsumer{deliveries: make(chan Delivery, 1)}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- Run(ctx, RuntimeConfig{ApplicationRevision: testApplicationRevision, WorkerArtifactDigest: testWorkerArtifact, Queue: "jobs", GateFile: gatePath, StateFile: statePath, PollInterval: 10 * time.Millisecond}, consumer, processor, recorder)
	}()

	waitFor(t, func() bool {
		state, err := readState(statePath)
		return err == nil && state.Gated && state.Connected && !state.Consuming
	})
	if starts, _ := consumer.counts(); starts != 0 {
		t.Fatalf("consumer started while gated: %d", starts)
	}
	if err := os.WriteFile(gatePath, []byte("open\n"), 0640); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		state, err := readState(statePath)
		return err == nil && !state.Gated && state.Consuming
	})

	message, _ := example.NewMessage(testApplicationRevision, testTaskArtifact, "invocation-a", 1, example.BehaviorProcess)
	payload, _ := message.Marshal()
	acked := make(chan struct{}, 1)
	consumer.deliveries <- Delivery{
		Body:      payload,
		MessageID: message.ID,
		Ack:       func() error { acked <- struct{}{}; return nil },
		Nack:      func(bool) error { t.Error("unexpected nack"); return nil },
	}
	select {
	case <-acked:
	case <-time.After(2 * time.Second):
		t.Fatal("message was not acknowledged")
	}

	if err := os.WriteFile(gatePath, []byte("closed\n"), 0640); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		_, cancels := consumer.counts()
		return cancels == 1
	})
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("runtime did not stop")
	}
	state, err := readState(statePath)
	if err != nil {
		t.Fatal(err)
	}
	if state.Connected || state.Consuming || !state.Gated {
		t.Fatalf("stopped runtime state = %#v", state)
	}
}

func TestRuntimeRequeuesInflightDeliveryOnShutdown(t *testing.T) {
	dir := t.TempDir()
	gatePath := filepath.Join(dir, "gate")
	if err := os.WriteFile(gatePath, []byte("open\n"), 0640); err != nil {
		t.Fatal(err)
	}
	recorder, _ := NewRecorder(filepath.Join(dir, "evidence.jsonl"))
	defer recorder.Close()
	processor, _ := NewProcessor(filepath.Join(dir, "ledger"), filepath.Join(dir, "holds"), recorder, testApplicationRevision, testWorkerArtifact)
	consumer := &fakeConsumer{deliveries: make(chan Delivery, 1)}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- Run(ctx, RuntimeConfig{ApplicationRevision: testApplicationRevision, WorkerArtifactDigest: testWorkerArtifact, Queue: "jobs", GateFile: gatePath, StateFile: filepath.Join(dir, "state.json"), PollInterval: 10 * time.Millisecond}, consumer, processor, recorder)
	}()
	waitFor(t, func() bool { starts, _ := consumer.counts(); return starts == 1 })

	message, _ := example.NewMessage(testApplicationRevision, testTaskArtifact, "invocation-a", 1, example.BehaviorHold)
	payload, _ := message.Marshal()
	requeued := make(chan bool, 1)
	consumer.deliveries <- Delivery{
		Body:      payload,
		MessageID: message.ID,
		Ack:       func() error { t.Error("unexpected ack"); return nil },
		Nack:      func(requeue bool) error { requeued <- requeue; return nil },
	}
	waitFor(t, func() bool {
		state, err := readState(filepath.Join(dir, "state.json"))
		return err == nil && state.InFlightMessageID == message.ID
	})
	cancel()
	select {
	case got := <-requeued:
		if !got {
			t.Fatal("held message was rejected instead of requeued")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("held message was not released for redelivery")
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestRuntimeRequeuesBufferedDeliveryAfterGateClosesWithoutProcessing(t *testing.T) {
	dir := t.TempDir()
	gatePath := filepath.Join(dir, "gate")
	statePath := filepath.Join(dir, "state.json")
	if err := os.WriteFile(gatePath, []byte("open\n"), 0640); err != nil {
		t.Fatal(err)
	}
	recorder, _ := NewRecorder(filepath.Join(dir, "evidence.jsonl"))
	defer recorder.Close()
	ledgerDir := filepath.Join(dir, "ledger")
	processor, _ := NewProcessor(ledgerDir, filepath.Join(dir, "holds"), recorder, testApplicationRevision, testWorkerArtifact)
	consumer := &fakeConsumer{deliveries: make(chan Delivery, 1)}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- Run(ctx, RuntimeConfig{ApplicationRevision: testApplicationRevision, WorkerArtifactDigest: testWorkerArtifact, Queue: "jobs", GateFile: gatePath, StateFile: statePath, PollInterval: time.Hour}, consumer, processor, recorder)
	}()
	waitFor(t, func() bool { starts, _ := consumer.counts(); return starts == 1 })

	if err := os.WriteFile(gatePath, []byte("closed\n"), 0640); err != nil {
		t.Fatal(err)
	}

	message, _ := example.NewMessage(testApplicationRevision, testTaskArtifact, "invocation-after-gate", 1, example.BehaviorProcess)
	payload, _ := message.Marshal()
	requeued := make(chan bool, 1)
	consumer.deliveries <- Delivery{
		Body:      payload,
		MessageID: message.ID,
		Ack:       func() error { t.Error("post-gate delivery was acknowledged"); return nil },
		Nack:      func(requeue bool) error { requeued <- requeue; return nil },
	}
	select {
	case got := <-requeued:
		if !got {
			t.Fatal("post-gate delivery was rejected rather than requeued")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("post-gate delivery was not requeued")
	}
	state, err := readState(statePath)
	if err != nil {
		t.Fatal(err)
	}
	_, cancels := consumer.counts()
	if !state.Gated || state.Consuming || cancels != 1 {
		t.Fatalf("gate race state = %#v, cancels = %d", state, cancels)
	}
	entries, err := os.ReadDir(filepath.Join(ledgerDir, "processed"))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("post-gate delivery produced %d effects; want none", len(entries))
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

type failingRecorder struct {
	failEvent string
}

func (r *failingRecorder) Record(event Event) error {
	if event.Event == r.failEvent {
		return errors.New("evidence storage unavailable")
	}
	return nil
}

func TestRuntimeDoesNotSettleDeliveryWhenDispositionEvidenceFails(t *testing.T) {
	dir := t.TempDir()
	gatePath := filepath.Join(dir, "gate")
	if err := os.WriteFile(gatePath, []byte("open\n"), 0640); err != nil {
		t.Fatal(err)
	}
	recorder := &failingRecorder{failEvent: "acknowledgement_decided"}
	processor, err := NewProcessor(filepath.Join(dir, "ledger"), filepath.Join(dir, "holds"), recorder, testApplicationRevision, testWorkerArtifact)
	if err != nil {
		t.Fatal(err)
	}
	consumer := &fakeConsumer{deliveries: make(chan Delivery, 1)}
	done := make(chan error, 1)
	go func() {
		done <- Run(context.Background(), RuntimeConfig{ApplicationRevision: testApplicationRevision, WorkerArtifactDigest: testWorkerArtifact, Queue: "jobs", GateFile: gatePath, StateFile: filepath.Join(dir, "state.json"), PollInterval: 10 * time.Millisecond}, consumer, processor, recorder)
	}()
	waitFor(t, func() bool { starts, _ := consumer.counts(); return starts == 1 })

	message, _ := example.NewMessage(testApplicationRevision, testTaskArtifact, "invocation-evidence-failure", 1, example.BehaviorProcess)
	payload, _ := message.Marshal()
	settled := make(chan string, 1)
	consumer.deliveries <- Delivery{
		Body:      payload,
		MessageID: message.ID,
		Ack:       func() error { settled <- "ack"; return nil },
		Nack:      func(bool) error { settled <- "nack"; return nil },
	}
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "evidence storage unavailable") {
			t.Fatalf("error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("runtime did not fail when evidence recording failed")
	}
	select {
	case operation := <-settled:
		t.Fatalf("delivery was settled with %s despite evidence failure", operation)
	default:
	}
}

func readState(path string) (State, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return State{}, err
	}
	var state State
	err = json.Unmarshal(data, &state)
	return state, err
}

func waitFor(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition was not met before timeout")
}
