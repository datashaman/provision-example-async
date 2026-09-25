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
	MessageID   string
	Disposition Disposition
	Duplicate   bool
}

type Event struct {
	SchemaVersion  string `json:"schemaVersion"`
	Event          string `json:"event"`
	MessageID      string `json:"messageId,omitempty"`
	WorkerRevision string `json:"workerRevision"`
	TaskRevision   string `json:"taskRevision,omitempty"`
	InvocationID   string `json:"invocationId,omitempty"`
	Sequence       int    `json:"sequence,omitempty"`
	Attempt        int    `json:"attempt,omitempty"`
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
	ledgerDir      string
	holdDir        string
	recorder       *Recorder
	workerRevision string
}

func NewProcessor(ledgerDir, holdDir string, recorder *Recorder, workerRevision string) (*Processor, error) {
	if ledgerDir == "" || holdDir == "" || recorder == nil || workerRevision == "" {
		return nil, fmt.Errorf("ledger directory, hold directory, recorder, and worker revision are required")
	}
	for _, path := range []string{filepath.Join(ledgerDir, "processed"), filepath.Join(ledgerDir, "attempts"), holdDir} {
		if err := os.MkdirAll(path, 0750); err != nil {
			return nil, err
		}
	}
	return &Processor{ledgerDir: ledgerDir, holdDir: holdDir, recorder: recorder, workerRevision: workerRevision}, nil
}

func (p *Processor) ProcessPayload(ctx context.Context, payload []byte, brokerMessageID string) Result {
	message, err := example.ParseMessage(payload)
	if err != nil || (brokerMessageID != "" && brokerMessageID != message.ID) {
		_ = p.recorder.Record(Event{Event: "invalid_message", MessageID: brokerMessageID, WorkerRevision: p.workerRevision})
		return Result{MessageID: brokerMessageID, Disposition: Reject}
	}
	return p.Process(ctx, message)
}

func (p *Processor) Process(ctx context.Context, message example.Message) Result {
	if err := message.Validate(); err != nil {
		_ = p.recorder.Record(Event{Event: "invalid_message", MessageID: message.ID, WorkerRevision: p.workerRevision})
		return Result{MessageID: message.ID, Disposition: Reject}
	}
	event := func(name string, attempt int) {
		_ = p.recorder.Record(Event{
			Event:          name,
			MessageID:      message.ID,
			WorkerRevision: p.workerRevision,
			TaskRevision:   message.Revision,
			InvocationID:   message.InvocationID,
			Sequence:       message.Sequence,
			Attempt:        attempt,
		})
	}
	processedPath := filepath.Join(p.ledgerDir, "processed", fileKey(message.ID)+".json")
	if _, err := os.Stat(processedPath); err == nil {
		event("duplicate_ignored", 0)
		return Result{MessageID: message.ID, Disposition: Acknowledge, Duplicate: true}
	} else if !errors.Is(err, os.ErrNotExist) {
		event("processing_failed", 0)
		return Result{MessageID: message.ID, Disposition: Requeue}
	}

	attempt, err := p.nextAttempt(message.ID)
	if err != nil {
		event("processing_failed", 0)
		return Result{MessageID: message.ID, Disposition: Requeue}
	}
	event("received", attempt)
	switch message.Behavior {
	case example.BehaviorHold:
		event("held", attempt)
		if !p.waitForRelease(ctx, message.ID) {
			event("released_for_redelivery", attempt)
			return Result{MessageID: message.ID, Disposition: Requeue}
		}
		event("hold_released", attempt)
	case example.BehaviorFailOnce:
		if attempt == 1 {
			event("deliberate_failure", attempt)
			return Result{MessageID: message.ID, Disposition: Requeue}
		}
	case example.BehaviorReject:
		event("deliberate_rejection", attempt)
		return Result{MessageID: message.ID, Disposition: Reject}
	}
	if err := writeExclusiveJSON(processedPath, message); err != nil {
		if errors.Is(err, os.ErrExist) {
			event("duplicate_ignored", attempt)
			return Result{MessageID: message.ID, Disposition: Acknowledge, Duplicate: true}
		}
		event("processing_failed", attempt)
		return Result{MessageID: message.ID, Disposition: Requeue}
	}
	event("processed", attempt)
	return Result{MessageID: message.ID, Disposition: Acknowledge}
}

func (p *Processor) Release(messageID string) error {
	path := filepath.Join(p.holdDir, fileKey(messageID)+".release")
	return os.WriteFile(path, []byte(messageID+"\n"), 0640)
}

func (p *Processor) waitForRelease(ctx context.Context, messageID string) bool {
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	path := filepath.Join(p.holdDir, fileKey(messageID)+".release")
	for {
		if data, err := os.ReadFile(path); err == nil && string(data) == messageID+"\n" {
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

func (p *Processor) nextAttempt(messageID string) (int, error) {
	path := filepath.Join(p.ledgerDir, "attempts", fileKey(messageID)+".txt")
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
