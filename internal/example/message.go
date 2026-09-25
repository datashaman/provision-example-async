package example

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
)

const MessageSchemaVersion = "provision.dev/example-async-message/v1alpha1"

type Behavior string

const (
	BehaviorProcess  Behavior = "process"
	BehaviorHold     Behavior = "hold"
	BehaviorFailOnce Behavior = "fail-once"
	BehaviorReject   Behavior = "reject"
)

type Message struct {
	SchemaVersion string   `json:"schemaVersion"`
	ID            string   `json:"messageId"`
	Revision      string   `json:"revision"`
	InvocationID  string   `json:"invocationId"`
	Sequence      int      `json:"sequence"`
	Behavior      Behavior `json:"behavior"`
}

func NewMessage(revision, invocationID string, sequence int, behavior Behavior) (Message, error) {
	message := Message{
		SchemaVersion: MessageSchemaVersion,
		Revision:      revision,
		InvocationID:  invocationID,
		Sequence:      sequence,
		Behavior:      behavior,
	}
	if revision == "" {
		return Message{}, fmt.Errorf("revision is required")
	}
	if invocationID == "" {
		return Message{}, fmt.Errorf("invocation ID is required")
	}
	if sequence < 1 {
		return Message{}, fmt.Errorf("sequence must be positive")
	}
	if !behavior.Valid() {
		return Message{}, fmt.Errorf("unsupported behavior %q", behavior)
	}
	digest := sha256.Sum256([]byte(fmt.Sprintf("provision-example-async\x00%s\x00%s\x00%d", revision, invocationID, sequence)))
	message.ID = "msg-" + hex.EncodeToString(digest[:])
	return message, nil
}

func (b Behavior) Valid() bool {
	switch b {
	case BehaviorProcess, BehaviorHold, BehaviorFailOnce, BehaviorReject:
		return true
	default:
		return false
	}
}

func (m Message) Validate() error {
	want, err := NewMessage(m.Revision, m.InvocationID, m.Sequence, m.Behavior)
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
