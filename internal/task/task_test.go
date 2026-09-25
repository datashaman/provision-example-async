package task

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/datashaman/provision-example-async/internal/example"
)

const (
	testApplicationRevision example.ApplicationRevision = "application-revision-a"
	testTaskArtifact        example.ArtifactDigest      = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
)

type recordingPublisher struct {
	messages  []example.Message
	confirmed []bool
	errAt     int
}

func (p *recordingPublisher) Publish(_ context.Context, queue string, message example.Message) (bool, error) {
	if queue == "" {
		return false, errors.New("queue is empty")
	}
	p.messages = append(p.messages, message)
	index := len(p.messages) - 1
	if p.errAt > 0 && len(p.messages) == p.errAt {
		return false, errors.New("publish failed")
	}
	if index >= len(p.confirmed) {
		return true, nil
	}
	return p.confirmed[index], nil
}

func TestRunPublishesStableMessagesAndReportsConfirmations(t *testing.T) {
	publisher := &recordingPublisher{}
	var output bytes.Buffer
	config := Config{Queue: "jobs", ApplicationRevision: testApplicationRevision, TaskArtifactDigest: testTaskArtifact, InvocationID: "invocation-a", Count: 2, Behavior: example.BehaviorProcess}

	if err := Run(context.Background(), config, publisher, &output); err != nil {
		t.Fatal(err)
	}
	if len(publisher.messages) != 2 {
		t.Fatalf("published %d messages; want 2", len(publisher.messages))
	}
	first, _ := example.NewMessage(testApplicationRevision, testTaskArtifact, "invocation-a", 1, example.BehaviorProcess)
	second, _ := example.NewMessage(testApplicationRevision, testTaskArtifact, "invocation-a", 2, example.BehaviorProcess)
	if publisher.messages[0].ID != first.ID || publisher.messages[1].ID != second.ID {
		t.Fatalf("unexpected stable IDs: %#v", publisher.messages)
	}

	decoder := json.NewDecoder(&output)
	for sequence, wantID := range []example.MessageID{first.ID, second.ID} {
		var event Confirmation
		if err := decoder.Decode(&event); err != nil {
			t.Fatal(err)
		}
		if event.Event != "message_confirmed" || event.MessageID != wantID || event.ApplicationRevision != testApplicationRevision || event.TaskArtifactDigest != testTaskArtifact || event.Sequence != sequence+1 {
			t.Fatalf("unexpected confirmation: %#v", event)
		}
	}
	var summary Summary
	if err := decoder.Decode(&summary); err != nil {
		t.Fatal(err)
	}
	if summary.Event != "publish_complete" || summary.Confirmed != 2 {
		t.Fatalf("unexpected summary: %#v", summary)
	}
}

func TestRunDoesNotClaimAnUnconfirmedMessage(t *testing.T) {
	publisher := &recordingPublisher{confirmed: []bool{false}}
	var output bytes.Buffer
	err := Run(context.Background(), Config{Queue: "jobs", ApplicationRevision: testApplicationRevision, TaskArtifactDigest: testTaskArtifact, InvocationID: "invocation-a", Count: 1, Behavior: example.BehaviorProcess}, publisher, &output)
	if err == nil {
		t.Fatal("expected unconfirmed publish to fail")
	}
	if output.Len() != 0 {
		t.Fatalf("output claimed acceptance: %s", output.Bytes())
	}
}

func TestRunStopsAtFirstPublishFailure(t *testing.T) {
	publisher := &recordingPublisher{errAt: 2}
	var output bytes.Buffer
	err := Run(context.Background(), Config{Queue: "jobs", ApplicationRevision: testApplicationRevision, TaskArtifactDigest: testTaskArtifact, InvocationID: "invocation-a", Count: 3, Behavior: example.BehaviorProcess}, publisher, &output)
	if err == nil {
		t.Fatal("expected publish failure")
	}
	if len(publisher.messages) != 2 {
		t.Fatalf("published %d messages; want 2", len(publisher.messages))
	}
}
