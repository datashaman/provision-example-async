package worker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/datashaman/provision-example-async/internal/example"
)

const EvidenceSchemaVersion = "provision.dev/example-async-worker-evidence/v1alpha1"

type Disposition int

const (
	Acknowledge Disposition = iota + 1
	Requeue
	Reject
)

type Result struct {
	MessageID   example.MessageID
	Disposition Disposition
	Duplicate   bool
	Err         error
}

type Event struct {
	SchemaVersion        string                      `json:"schemaVersion"`
	Event                string                      `json:"event"`
	MessageID            example.MessageID           `json:"messageId,omitempty"`
	ApplicationRevision  example.ApplicationRevision `json:"applicationRevision"`
	WorkerArtifactDigest example.ArtifactDigest      `json:"workerArtifactDigest"`
	TaskArtifactDigest   example.ArtifactDigest      `json:"taskArtifactDigest,omitempty"`
	InvocationID         example.InvocationID        `json:"invocationId,omitempty"`
	Sequence             int                         `json:"sequence,omitempty"`
	Attempt              int                         `json:"attempt,omitempty"`
}

type EvidenceRecorder interface {
	Record(Event) error
}

type Recorder struct {
	mu   sync.Mutex
	file *os.File
}

func NewRecorder(path string) (*Recorder, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0750); err != nil {
		return nil, err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0640)
	if err != nil {
		return nil, err
	}
	return &Recorder{file: file}, nil
}

func (r *Recorder) Record(event Event) error {
	event.SchemaVersion = EvidenceSchemaVersion
	r.mu.Lock()
	defer r.mu.Unlock()
	data, err := json.Marshal(event)
	if err != nil {
		return err
	}
	data = append(data, '\n')
	if _, err := r.file.Write(data); err != nil {
		return err
	}
	return r.file.Sync()
}

func (r *Recorder) Close() error {
	return r.file.Close()
}

type Processor struct {
	ledgerDir            string
	holdDir              string
	recorder             EvidenceRecorder
	applicationRevision  example.ApplicationRevision
	workerArtifactDigest example.ArtifactDigest
}

func NewProcessor(ledgerDir, holdDir string, recorder EvidenceRecorder, applicationRevision example.ApplicationRevision, workerArtifactDigest example.ArtifactDigest) (*Processor, error) {
	if ledgerDir == "" || holdDir == "" || recorder == nil {
		return nil, fmt.Errorf("ledger directory, hold directory, and recorder are required")
	}
	if err := applicationRevision.Validate(); err != nil {
		return nil, err
	}
	if err := workerArtifactDigest.Validate("worker artifact"); err != nil {
		return nil, err
	}
	for _, path := range []string{filepath.Join(ledgerDir, "processed"), filepath.Join(ledgerDir, "attempts"), holdDir} {
		if err := os.MkdirAll(path, 0750); err != nil {
			return nil, err
		}
	}
	return &Processor{
		ledgerDir:            ledgerDir,
		holdDir:              holdDir,
		recorder:             recorder,
		applicationRevision:  applicationRevision,
		workerArtifactDigest: workerArtifactDigest,
	}, nil
}

func (p *Processor) ProcessPayload(ctx context.Context, payload []byte, brokerMessageID string) Result {
	message, err := example.ParseMessage(payload)
	brokerID := example.MessageID(brokerMessageID)
	if err != nil || (brokerMessageID != "" && brokerID != message.ID) {
		result := Result{MessageID: brokerID, Disposition: Reject}
		if recordErr := p.record(Event{Event: "invalid_message", MessageID: brokerID}); recordErr != nil {
			result.Disposition = 0
			result.Err = fmt.Errorf("record invalid message evidence: %w", recordErr)
		}
		return result
	}
	return p.Process(ctx, message)
}

func (p *Processor) Process(ctx context.Context, message example.Message) Result {
	if err := message.Validate(); err != nil {
		result := Result{MessageID: message.ID, Disposition: Reject}
		if recordErr := p.record(Event{Event: "invalid_message", MessageID: message.ID}); recordErr != nil {
			result.Disposition = 0
			result.Err = fmt.Errorf("record invalid message evidence: %w", recordErr)
		}
		return result
	}
	if message.ApplicationRevision != p.applicationRevision {
		result := Result{MessageID: message.ID, Disposition: Reject}
		if err := p.recordMessage("application_revision_mismatch", message, 0); err != nil {
			result.Disposition = 0
			result.Err = err
		}
		return result
	}

	processedPath := filepath.Join(p.ledgerDir, "processed", fileKey(string(message.ID))+".json")
	if _, err := os.Stat(processedPath); err == nil {
		if err := p.recordMessage("duplicate_ignored", message, 0); err != nil {
			return Result{MessageID: message.ID, Err: err}
		}
		return Result{MessageID: message.ID, Disposition: Acknowledge, Duplicate: true}
	} else if !errors.Is(err, os.ErrNotExist) {
		return p.recordedDisposition(message, "processing_failed", 0, Requeue)
	}

	attempt, err := p.nextAttempt(message.ID)
	if err != nil {
		return p.recordedDisposition(message, "processing_failed", 0, Requeue)
	}
	if err := p.recordMessage("received", message, attempt); err != nil {
		return Result{MessageID: message.ID, Err: err}
	}
	semantics, err := message.Behavior.Semantics()
	if err != nil {
		return p.recordedDisposition(message, "invalid_message", attempt, Reject)
	}
	if semantics.Hold {
		if err := p.recordMessage("held", message, attempt); err != nil {
			return Result{MessageID: message.ID, Err: err}
		}
		if !p.waitForRelease(ctx, message.ID) {
			return p.recordedDisposition(message, "released_for_redelivery", attempt, Requeue)
		}
		if err := p.recordMessage("hold_released", message, attempt); err != nil {
			return Result{MessageID: message.ID, Err: err}
		}
	}
	if attempt <= semantics.FailuresBeforeSuccess {
		return p.recordedDisposition(message, "deliberate_failure", attempt, Requeue)
	}
	if semantics.Reject {
		return p.recordedDisposition(message, "deliberate_rejection", attempt, Reject)
	}
	if err := writeExclusiveJSON(processedPath, message); err != nil {
		if errors.Is(err, os.ErrExist) {
			result := p.recordedDisposition(message, "duplicate_ignored", attempt, Acknowledge)
			result.Duplicate = result.Err == nil
			return result
		}
		return p.recordedDisposition(message, "processing_failed", attempt, Requeue)
	}
	return p.recordedDisposition(message, "processed", attempt, Acknowledge)
}

func (p *Processor) Release(messageID example.MessageID) error {
	path := filepath.Join(p.holdDir, fileKey(string(messageID))+".release")
	return os.WriteFile(path, []byte(string(messageID)+"\n"), 0640)
}

func (p *Processor) waitForRelease(ctx context.Context, messageID example.MessageID) bool {
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	path := filepath.Join(p.holdDir, fileKey(string(messageID))+".release")
	for {
		if data, err := os.ReadFile(path); err == nil && string(data) == string(messageID)+"\n" {
			_ = os.Remove(path)
			return true
		}
		select {
		case <-ctx.Done():
			return false
		case <-ticker.C:
		}
	}
}

func (p *Processor) nextAttempt(messageID example.MessageID) (int, error) {
	path := filepath.Join(p.ledgerDir, "attempts", fileKey(string(messageID))+".txt")
	attempt := 1
	if data, err := os.ReadFile(path); err == nil {
		previous, parseErr := strconv.Atoi(string(data))
		if parseErr != nil {
			return 0, parseErr
		}
		attempt = previous + 1
	} else if !errors.Is(err, os.ErrNotExist) {
		return 0, err
	}
	if err := writeAtomic(path, []byte(strconv.Itoa(attempt)), 0640); err != nil {
		return 0, err
	}
	return attempt, nil
}

func (p *Processor) recordedDisposition(message example.Message, event string, attempt int, disposition Disposition) Result {
	if err := p.recordMessage(event, message, attempt); err != nil {
		return Result{MessageID: message.ID, Err: err}
	}
	return Result{MessageID: message.ID, Disposition: disposition}
}

func (p *Processor) recordMessage(name string, message example.Message, attempt int) error {
	return p.record(Event{
		Event:              name,
		MessageID:          message.ID,
		TaskArtifactDigest: message.TaskArtifactDigest,
		InvocationID:       message.InvocationID,
		Sequence:           message.Sequence,
		Attempt:            attempt,
	})
}

func (p *Processor) record(event Event) error {
	event.ApplicationRevision = p.applicationRevision
	event.WorkerArtifactDigest = p.workerArtifactDigest
	if err := p.recorder.Record(event); err != nil {
		return fmt.Errorf("record %s evidence: %w", event.Event, err)
	}
	return nil
}

func writeExclusiveJSON(path string, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0640)
	if err != nil {
		return err
	}
	if _, err := file.Write(append(data, '\n')); err != nil {
		file.Close()
		return err
	}
	return file.Close()
}

func writeAtomic(path string, data []byte, mode os.FileMode) error {
	temp, err := os.CreateTemp(filepath.Dir(path), ".tmp-*")
	if err != nil {
		return err
	}
	name := temp.Name()
	defer os.Remove(name)
	if err := temp.Chmod(mode); err != nil {
		temp.Close()
		return err
	}
	if _, err := temp.Write(data); err != nil {
		temp.Close()
		return err
	}
	if err := temp.Close(); err != nil {
		return err
	}
	return os.Rename(name, path)
}

func fileKey(value string) string {
	digest := sha256.Sum256([]byte(value))
	return hex.EncodeToString(digest[:])
}
