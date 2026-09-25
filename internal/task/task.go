package task

import (
	"context"
	"encoding/json"
	"fmt"
	"io"

	"github.com/datashaman/provision-example-async/internal/example"
)

const EvidenceSchemaVersion = "provision.dev/example-async-task-evidence/v1alpha1"

type Config struct {
	Queue        string
	Revision     string
	InvocationID string
	Count        int
	Behavior     example.Behavior
}

type Publisher interface {
	Publish(context.Context, string, example.Message) (bool, error)
}

type Confirmation struct {
	SchemaVersion string `json:"schemaVersion"`
	Event         string `json:"event"`
	MessageID     string `json:"messageId"`
	Revision      string `json:"revision"`
	InvocationID  string `json:"invocationId"`
	Sequence      int    `json:"sequence"`
}

type Summary struct {
	SchemaVersion string `json:"schemaVersion"`
	Event         string `json:"event"`
	Revision      string `json:"revision"`
	InvocationID  string `json:"invocationId"`
	Confirmed     int    `json:"confirmed"`
}

func (c Config) Validate() error {
	if c.Queue == "" {
		return fmt.Errorf("queue is required")
	}
	if c.Count < 1 || c.Count > 10000 {
		return fmt.Errorf("count must be between 1 and 10000")
	}
	_, err := example.NewMessage(c.Revision, c.InvocationID, 1, c.Behavior)
	return err
}

func Run(ctx context.Context, config Config, publisher Publisher, output io.Writer) error {
	if err := config.Validate(); err != nil {
		return err
	}
	encoder := json.NewEncoder(output)
	for sequence := 1; sequence <= config.Count; sequence++ {
		message, err := example.NewMessage(config.Revision, config.InvocationID, sequence, config.Behavior)
		if err != nil {
			return err
		}
		confirmed, err := publisher.Publish(ctx, config.Queue, message)
		if err != nil {
			return fmt.Errorf("publish message %s: %w", message.ID, err)
		}
		if !confirmed {
			return fmt.Errorf("broker did not confirm message %s", message.ID)
		}
		if err := encoder.Encode(Confirmation{
			SchemaVersion: EvidenceSchemaVersion,
			Event:         "message_confirmed",
			MessageID:     message.ID,
			Revision:      message.Revision,
			InvocationID:  message.InvocationID,
			Sequence:      message.Sequence,
		}); err != nil {
			return fmt.Errorf("write confirmation evidence: %w", err)
		}
	}
	if err := encoder.Encode(Summary{
		SchemaVersion: EvidenceSchemaVersion,
		Event:         "publish_complete",
		Revision:      config.Revision,
		InvocationID:  config.InvocationID,
		Confirmed:     config.Count,
	}); err != nil {
		return fmt.Errorf("write summary evidence: %w", err)
	}
	return nil
}
