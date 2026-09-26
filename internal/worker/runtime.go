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

	"github.com/datashaman/provision-example-async/internal/example"
)

const StateSchemaVersion = "provision.dev/example-async-worker-state/v1alpha1"

type RuntimeConfig struct {
	ApplicationRevision  example.ApplicationRevision
	WorkerArtifactDigest example.ArtifactDigest
	Queue                string
	GateFile             string
	StateFile            string
	PollInterval         time.Duration
}

type Delivery struct {
	Body      []byte
	MessageID example.MessageID
	Ack       func() error
	Nack      func(requeue bool) error
}

type Consumer interface {
	Start(tag string) (<-chan Delivery, error)
	Cancel(tag string) error
}

type State struct {
	SchemaVersion        string                      `json:"schemaVersion"`
	ApplicationRevision  example.ApplicationRevision `json:"applicationRevision"`
	WorkerArtifactDigest example.ArtifactDigest      `json:"workerArtifactDigest"`
	Queue                string                      `json:"queue"`
	Connected            bool                        `json:"connected"`
	Gated                bool                        `json:"gated"`
	Consuming            bool                        `json:"consuming"`
	InFlightMessageID    example.MessageID           `json:"inFlightMessageId,omitempty"`
}

type completedDelivery struct {
	delivery Delivery
	result   Result
}

func (c RuntimeConfig) Validate() error {
	if err := c.ApplicationRevision.Validate(); err != nil {
		return err
	}
	if err := c.WorkerArtifactDigest.Validate("worker artifact"); err != nil {
		return err
	}
	if c.Queue == "" || c.GateFile == "" || c.StateFile == "" {
		return fmt.Errorf("queue, gate file, and state file are required")
	}
	if c.PollInterval <= 0 {
		return fmt.Errorf("poll interval must be positive")
	}
	return nil
}

func Run(ctx context.Context, config RuntimeConfig, consumer Consumer, processor *Processor, recorder EvidenceRecorder) error {
	if err := config.Validate(); err != nil {
		return err
	}
	if consumer == nil || processor == nil || recorder == nil {
		return fmt.Errorf("consumer, processor, and recorder are required")
	}
	state := State{
		SchemaVersion:        StateSchemaVersion,
		ApplicationRevision:  config.ApplicationRevision,
		WorkerArtifactDigest: config.WorkerArtifactDigest,
		Queue:                config.Queue,
		Connected:            true,
		Gated:                true,
	}
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
	if err := recorder.Record(runtimeEvent(config, "connected_gated", "")); err != nil {
		return fmt.Errorf("record connected gated evidence: %w", err)
	}

	tag := "provision-example-async-" + consumerTag(string(config.WorkerArtifactDigest))
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
			if err := recorder.Record(runtimeEvent(config, "intake_gated", "")); err != nil {
				return fmt.Errorf("record intake gated evidence: %w", err)
			}
		}
		if open && !state.Consuming && current == nil && deliveries == nil {
			started, err := consumer.Start(tag)
			if err != nil {
				return fmt.Errorf("start consumer: %w", err)
			}
			deliveries = started
			state.Consuming = true
			if err := recorder.Record(runtimeEvent(config, "intake_opened", "")); err != nil {
				return fmt.Errorf("record intake opened evidence: %w", err)
			}
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
				if err := finishDelivery(finished, recorder, config); err != nil {
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
			if err := reconcileGate(); err != nil {
				return err
			}
			if state.Gated {
				if err := settleWithoutProcessing(delivery, recorder, config, "post_gate_delivery_requeue_decided"); err != nil {
					return err
				}
				continue
			}
			if current != nil {
				if err := settleWithoutProcessing(delivery, recorder, config, "prefetch_violation_requeue_decided"); err != nil {
					return err
				}
				return fmt.Errorf("consumer delivered more than one in-flight message")
			}
			current = &delivery
			state.InFlightMessageID = delivery.MessageID
			if err := writeState(); err != nil {
				if recordErr := recorder.Record(runtimeEvent(config, "state_failure_requeue_decided", delivery.MessageID)); recordErr != nil {
					return errors.Join(err, fmt.Errorf("record state failure disposition: %w", recordErr))
				}
				if nackErr := delivery.Nack(true); nackErr != nil {
					return errors.Join(err, fmt.Errorf("requeue after state failure: %w", nackErr))
				}
				return err
			}
			go func(delivery Delivery) {
				completed <- completedDelivery{delivery: delivery, result: processor.ProcessPayload(processCtx, delivery.Body, string(delivery.MessageID))}
			}(delivery)
		case finished := <-completed:
			if current == nil {
				return fmt.Errorf("received a processing result without an in-flight delivery")
			}
			if err := finishDelivery(finished, recorder, config); err != nil {
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

func finishDelivery(finished completedDelivery, recorder EvidenceRecorder, config RuntimeConfig) error {
	if finished.result.Err != nil {
		return fmt.Errorf("process message %s: %w", finished.result.MessageID, finished.result.Err)
	}
	event := runtimeEvent(config, "", finished.result.MessageID)
	switch finished.result.Disposition {
	case Acknowledge:
		event.Event = "acknowledgement_decided"
	case Requeue:
		event.Event = "requeue_decided"
	case Reject:
		event.Event = "rejection_decided"
	default:
		return fmt.Errorf("unknown delivery disposition %d", finished.result.Disposition)
	}
	if err := recorder.Record(event); err != nil {
		return fmt.Errorf("record delivery disposition evidence: %w", err)
	}
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
	}
	if err := recorder.Record(event); err != nil {
		return fmt.Errorf("record settled delivery evidence: %w", err)
	}
	return nil
}

func settleWithoutProcessing(delivery Delivery, recorder EvidenceRecorder, config RuntimeConfig, eventName string) error {
	if err := recorder.Record(runtimeEvent(config, eventName, delivery.MessageID)); err != nil {
		return fmt.Errorf("record %s evidence: %w", eventName, err)
	}
	if err := delivery.Nack(true); err != nil {
		return fmt.Errorf("requeue message %s without processing: %w", delivery.MessageID, err)
	}
	return nil
}

func runtimeEvent(config RuntimeConfig, event string, messageID example.MessageID) Event {
	return Event{
		Event:                     event,
		MessageID:                 messageID,
		WorkerApplicationRevision: config.ApplicationRevision,
		WorkerArtifactDigest:      config.WorkerArtifactDigest,
	}
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

func consumerTag(artifactDigest string) string {
	var builder strings.Builder
	for _, char := range artifactDigest {
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
