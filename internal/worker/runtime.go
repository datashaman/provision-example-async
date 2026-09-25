package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const StateSchemaVersion = "provision.dev/example-async-worker-state/v1alpha1"

type RuntimeConfig struct {
	Revision     string
	Queue        string
	GateFile     string
	StateFile    string
	PollInterval time.Duration
}

type Delivery struct {
	Body      []byte
	MessageID string
	Ack       func() error
	Nack      func(requeue bool) error
}

type Consumer interface {
	Start(tag string) (<-chan Delivery, error)
	Cancel(tag string) error
}

type State struct {
	SchemaVersion     string `json:"schemaVersion"`
	Revision          string `json:"revision"`
	Queue             string `json:"queue"`
	Connected         bool   `json:"connected"`
	Gated             bool   `json:"gated"`
	Consuming         bool   `json:"consuming"`
	InFlightMessageID string `json:"inFlightMessageId,omitempty"`
}

type completedDelivery struct {
	delivery Delivery
	result   Result
}

func (c RuntimeConfig) Validate() error {
	if c.Revision == "" || c.Queue == "" || c.GateFile == "" || c.StateFile == "" {
		return fmt.Errorf("revision, queue, gate file, and state file are required")
	}
	if c.PollInterval <= 0 {
		return fmt.Errorf("poll interval must be positive")
	}
	return nil
}

func Run(ctx context.Context, config RuntimeConfig, consumer Consumer, processor *Processor, recorder *Recorder) error {
	if err := config.Validate(); err != nil {
		return err
	}
	if consumer == nil || processor == nil || recorder == nil {
		return fmt.Errorf("consumer, processor, and recorder are required")
	}
	state := State{SchemaVersion: StateSchemaVersion, Revision: config.Revision, Queue: config.Queue, Connected: true, Gated: true}
	writeState := func() error { return writeStateFile(config.StateFile, state) }
	if err := writeState(); err != nil {
		return err
	}
	defer func() {
		state.Connected = false
		state.Consuming = false
		state.Gated = true
		_ = writeState()
	}()
	_ = recorder.Record(Event{Event: "connected_gated", WorkerRevision: config.Revision})

	tag := "provision-example-async-" + consumerTag(config.Revision)
	ticker := time.NewTicker(config.PollInterval)
	defer ticker.Stop()
	processCtx, cancelProcessing := context.WithCancel(ctx)
	defer cancelProcessing()
	completed := make(chan completedDelivery, 1)
	var deliveries <-chan Delivery
	var current *Delivery

	reconcileGate := func() error {
		open, err := gateOpen(config.GateFile)
		if err != nil {
			return err
		}
		state.Gated = !open
		if !open && state.Consuming {
			if err := consumer.Cancel(tag); err != nil {
				return fmt.Errorf("fence consumer intake: %w", err)
			}
			state.Consuming = false
			_ = recorder.Record(Event{Event: "intake_gated", WorkerRevision: config.Revision})
		}
		if open && !state.Consuming && current == nil && deliveries == nil {
			started, err := consumer.Start(tag)
			if err != nil {
				return fmt.Errorf("start consumer: %w", err)
			}
			deliveries = started
			state.Consuming = true
			_ = recorder.Record(Event{Event: "intake_opened", WorkerRevision: config.Revision})
		}
		return writeState()
	}
	if err := reconcileGate(); err != nil {
		return err
	}

	for {
		select {
		case <-ctx.Done():
			if state.Consuming {
				if err := consumer.Cancel(tag); err != nil {
					return fmt.Errorf("cancel consumer during shutdown: %w", err)
				}
				state.Consuming = false
				deliveries = nil
			}
			cancelProcessing()
			if current != nil {
				finished := <-completed
				if err := finishDelivery(finished, recorder, config.Revision); err != nil {
					return err
				}
				current = nil
				state.InFlightMessageID = ""
			}
			state.Gated = true
			return writeState()
		case <-ticker.C:
			if err := reconcileGate(); err != nil {
				return err
			}
		case delivery, ok := <-deliveries:
			if !ok {
				deliveries = nil
				if state.Consuming {
					return fmt.Errorf("consumer delivery channel closed while intake was open")
				}
				continue
			}
			if current != nil {
				_ = delivery.Nack(true)
				return fmt.Errorf("consumer delivered more than one in-flight message")
			}
			current = &delivery
			state.InFlightMessageID = delivery.MessageID
			if err := writeState(); err != nil {
				_ = delivery.Nack(true)
				return err
			}
			go func(delivery Delivery) {
				completed <- completedDelivery{delivery: delivery, result: processor.ProcessPayload(processCtx, delivery.Body, delivery.MessageID)}
			}(delivery)
		case finished := <-completed:
			if current == nil {
				return fmt.Errorf("received a processing result without an in-flight delivery")
			}
			if err := finishDelivery(finished, recorder, config.Revision); err != nil {
				return err
			}
			current = nil
			state.InFlightMessageID = ""
			if err := writeState(); err != nil {
				return err
			}
		}
	}
}

func finishDelivery(finished completedDelivery, recorder *Recorder, revision string) error {
	event := Event{MessageID: finished.result.MessageID, WorkerRevision: revision}
	switch finished.result.Disposition {
	case Acknowledge:
		if err := finished.delivery.Ack(); err != nil {
			return fmt.Errorf("acknowledge %s: %w", finished.result.MessageID, err)
		}
		event.Event = "acknowledged"
	case Requeue:
		if err := finished.delivery.Nack(true); err != nil {
			return fmt.Errorf("requeue %s: %w", finished.result.MessageID, err)
		}
		event.Event = "requeued"
	case Reject:
		if err := finished.delivery.Nack(false); err != nil {
			return fmt.Errorf("reject %s: %w", finished.result.MessageID, err)
		}
		event.Event = "rejected"
	default:
		return fmt.Errorf("unknown delivery disposition %d", finished.result.Disposition)
	}
	return recorder.Record(event)
}

func gateOpen(path string) (bool, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("read gate file: %w", err)
	}
	return strings.TrimSpace(string(data)) == "open", nil
}

func writeStateFile(path string, state State) error {
	if err := os.MkdirAll(filepath.Dir(path), 0750); err != nil {
		return err
	}
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}
	return writeAtomic(path, append(data, '\n'), 0640)
}

func consumerTag(revision string) string {
	var builder strings.Builder
	for _, char := range revision {
		if char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z' || char >= '0' && char <= '9' || char == '-' || char == '_' {
			builder.WriteRune(char)
		} else {
			builder.WriteByte('-')
		}
	}
	result := builder.String()
	if len(result) > 80 {
		result = result[:80]
	}
	return result
}
