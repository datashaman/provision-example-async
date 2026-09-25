package example

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"regexp"
)

const MessageSchemaVersion = "provision.dev/example-async-message/v1alpha1"

type Behavior string

type ApplicationRevision string

type ArtifactDigest string

type InvocationID string

type MessageID string

var sha256DigestPattern = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)

const (
	BehaviorProcess  Behavior = "process"
	BehaviorHold     Behavior = "hold"
	BehaviorFailOnce Behavior = "fail-once"
	BehaviorReject   Behavior = "reject"
)

type Message struct {
	SchemaVersion       string              `json:"schemaVersion"`
	ID                  MessageID           `json:"messageId"`
	ApplicationRevision ApplicationRevision `json:"applicationRevision"`
	TaskArtifactDigest  ArtifactDigest      `json:"taskArtifactDigest"`
	InvocationID        InvocationID        `json:"invocationId"`
	Sequence            int                 `json:"sequence"`
	Behavior            Behavior            `json:"behavior"`
}

type BehaviorSemantics struct {
	Hold                  bool
	FailuresBeforeSuccess int
	Reject                bool
}

func NewMessage(applicationRevision ApplicationRevision, taskArtifactDigest ArtifactDigest, invocationID InvocationID, sequence int, behavior Behavior) (Message, error) {
	message := Message{
		SchemaVersion:       MessageSchemaVersion,
		ApplicationRevision: applicationRevision,
		TaskArtifactDigest:  taskArtifactDigest,
		InvocationID:        invocationID,
		Sequence:            sequence,
		Behavior:            behavior,
	}
	if err := applicationRevision.Validate(); err != nil {
		return Message{}, err
	}
	if err := taskArtifactDigest.Validate("task artifact"); err != nil {
		return Message{}, err
	}
	if err := invocationID.Validate(); err != nil {
		return Message{}, err
	}
	if sequence < 1 {
		return Message{}, fmt.Errorf("sequence must be positive")
	}
	if _, err := behavior.Semantics(); err != nil {
		return Message{}, err
	}
	digest := sha256.Sum256([]byte(fmt.Sprintf("provision-example-async\x00%s\x00%s\x00%s\x00%d", applicationRevision, taskArtifactDigest, invocationID, sequence)))
	message.ID = MessageID("msg-" + hex.EncodeToString(digest[:]))
	return message, nil
}

func (r ApplicationRevision) Validate() error {
	if r == "" {
		return fmt.Errorf("application revision is required")
	}
	return nil
}

func (d ArtifactDigest) Validate(label string) error {
	if !sha256DigestPattern.MatchString(string(d)) {
		return fmt.Errorf("%s digest must be a lowercase sha256 digest", label)
	}
	return nil
}

func (i InvocationID) Validate() error {
	if i == "" {
		return fmt.Errorf("invocation ID is required")
	}
	return nil
}

func (b Behavior) Semantics() (BehaviorSemantics, error) {
	switch b {
	case BehaviorProcess:
		return BehaviorSemantics{}, nil
	case BehaviorHold:
		return BehaviorSemantics{Hold: true}, nil
	case BehaviorFailOnce:
		return BehaviorSemantics{FailuresBeforeSuccess: 1}, nil
	case BehaviorReject:
		return BehaviorSemantics{Reject: true}, nil
	default:
		return BehaviorSemantics{}, fmt.Errorf("unsupported behavior %q", b)
	}
}

func (m Message) Validate() error {
	want, err := NewMessage(m.ApplicationRevision, m.TaskArtifactDigest, m.InvocationID, m.Sequence, m.Behavior)
	if err != nil {
		return err
	}
	if m.SchemaVersion != MessageSchemaVersion {
		return fmt.Errorf("unsupported message schema %q", m.SchemaVersion)
	}
	if m.ID != want.ID {
		return fmt.Errorf("message identity does not match its stable inputs")
	}
	return nil
}

func (m Message) Marshal() ([]byte, error) {
	if err := m.Validate(); err != nil {
		return nil, err
	}
	return json.Marshal(m)
}

func ParseMessage(data []byte) (Message, error) {
	var message Message
	if err := json.Unmarshal(data, &message); err != nil {
		return Message{}, fmt.Errorf("decode message: %w", err)
	}
	if err := message.Validate(); err != nil {
		return Message{}, err
	}
	return message, nil
}
