// Package state persists versioned conversation checkpoints independently
// from context-window assembly and model-provider payloads.
package state

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"goharness/pkg/schema"
)

var (
	ErrNilContext = errors.New("checkpoint context cannot be nil")
	ErrConflict   = errors.New("checkpoint version conflict")
)

const CurrentCheckpointSchemaVersion = 1

// GraphStateCheckpoint is the portable representation of a typed graph
// State. Data is owned by the checkpoint and is always copied at API
// boundaries, so callers cannot mutate a persisted continuation in place.
type GraphStateCheckpoint struct {
	CodecID string `json:"codec_id"`
	Version int    `json:"version"`
	Data    []byte `json:"data"`
}

func (c GraphStateCheckpoint) Clone() GraphStateCheckpoint {
	c.Data = append([]byte(nil), c.Data...)
	return c
}

func (c GraphStateCheckpoint) Validate() error {
	if strings.TrimSpace(c.CodecID) == "" {
		return errors.New("graph state checkpoint codec ID cannot be empty")
	}
	if c.Version <= 0 {
		return fmt.Errorf("graph state checkpoint version must be positive, got %d", c.Version)
	}
	return nil
}

// CursorFrame is a clean, portable Graph execution frame. It contains no
// functions, contexts, clients or credentials.
type CursorFrame struct {
	Graph     string `json:"graph"`
	Node      string `json:"node"`
	Step      int    `json:"step"`
	Iteration int    `json:"iteration,omitempty"`
}

type CursorStack []CursorFrame

func (s CursorStack) Clone() CursorStack {
	return append(CursorStack(nil), s...)
}

func (s CursorStack) Validate() error {
	for index, frame := range s {
		if strings.TrimSpace(frame.Graph) == "" {
			return fmt.Errorf("cursor frame %d graph cannot be empty", index)
		}
		if strings.TrimSpace(frame.Node) == "" {
			return fmt.Errorf("cursor frame %d node cannot be empty", index)
		}
		if frame.Step < 0 || frame.Iteration < 0 {
			return fmt.Errorf("cursor frame %d counters cannot be negative", index)
		}
	}
	return nil
}

// Checkpoint is the durable state of one conversation. Version zero means the
// conversation has not been persisted; each successful Save increments it.
type Checkpoint struct {
	SchemaVersion  int              `json:"schema_version"`
	ConversationID string           `json:"conversation_id"`
	Version        uint64           `json:"version"`
	Messages       []schema.Message `json:"messages,omitempty"`
	Values         map[string]any   `json:"values,omitempty"`
	CreatedAt      time.Time        `json:"created_at"`
	UpdatedAt      time.Time        `json:"updated_at"`
}

func (c Checkpoint) Clone() Checkpoint {
	c.Messages = cloneMessages(c.Messages)
	c.Values = cloneMap(c.Values)
	return c
}

func (c Checkpoint) Validate() error {
	if strings.TrimSpace(c.ConversationID) == "" {
		return errors.New("checkpoint conversation ID cannot be empty")
	}
	if c.SchemaVersion != 0 && c.SchemaVersion != CurrentCheckpointSchemaVersion {
		return fmt.Errorf("unsupported checkpoint schema version %d", c.SchemaVersion)
	}
	for index, message := range c.Messages {
		if err := message.Validate(); err != nil {
			return fmt.Errorf("checkpoint message %d: %w", index, err)
		}
	}
	if err := validateJSON("checkpoint messages", c.Messages); err != nil {
		return err
	}
	if err := validateJSON("checkpoint values", c.Values); err != nil {
		return err
	}
	return nil
}

// JSON returns a validated, portable representation suitable for transport to
// another process or device.
func (c Checkpoint) JSON() ([]byte, error) {
	prepared := prepareCheckpoint(c)
	if err := prepared.Validate(); err != nil {
		return nil, err
	}
	return json.Marshal(prepared)
}

// DecodeCheckpoint validates a checkpoint received from an external system.
func DecodeCheckpoint(data []byte) (Checkpoint, error) {
	var checkpoint Checkpoint
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&checkpoint); err != nil {
		return Checkpoint{}, fmt.Errorf("decode checkpoint JSON: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		if err == nil {
			err = errors.New("multiple JSON values")
		}
		return Checkpoint{}, fmt.Errorf("decode checkpoint JSON: %w", err)
	}
	checkpoint = prepareCheckpoint(checkpoint)
	if err := checkpoint.Validate(); err != nil {
		return Checkpoint{}, err
	}
	return checkpoint.Clone(), nil
}

// ConflictError reports optimistic-lock details for a rejected Save.
type ConflictError struct {
	ConversationID string
	Expected       uint64
	Actual         uint64
}

func (e *ConflictError) Error() string {
	return fmt.Sprintf("checkpoint %q version conflict: expected %d, actual %d", e.ConversationID, e.Expected, e.Actual)
}

func (e *ConflictError) Unwrap() error { return ErrConflict }

// Store is the persistence boundary used by Agent conversation lifecycles.
type Store interface {
	Load(ctx context.Context, conversationID string) (Checkpoint, bool, error)
	Save(ctx context.Context, checkpoint Checkpoint) (Checkpoint, error)
	Delete(ctx context.Context, conversationID string) error
}

func validateContextAndID(ctx context.Context, conversationID string) error {
	if ctx == nil {
		return ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if strings.TrimSpace(conversationID) == "" {
		return errors.New("checkpoint conversation ID cannot be empty")
	}
	return nil
}

func cloneMap(input map[string]any) map[string]any {
	if input == nil {
		return map[string]any{}
	}
	if data, err := json.Marshal(input); err == nil {
		var cloned map[string]any
		if err := json.Unmarshal(data, &cloned); err == nil {
			return cloned
		}
	}
	cloned := make(map[string]any, len(input))
	for key, value := range input {
		cloned[key] = cloneValue(value)
	}
	return cloned
}

func cloneMessages(messages []schema.Message) []schema.Message {
	if messages == nil {
		return nil
	}
	if data, err := json.Marshal(messages); err == nil {
		var cloned []schema.Message
		if err := json.Unmarshal(data, &cloned); err == nil {
			return cloned
		}
	}
	return schema.CloneMessages(messages)
}

func cloneValue(value any) any {
	switch current := value.(type) {
	case map[string]any:
		return cloneMap(current)
	case []any:
		cloned := make([]any, len(current))
		for index := range current {
			cloned[index] = cloneValue(current[index])
		}
		return cloned
	case []string:
		return append([]string(nil), current...)
	case []int:
		return append([]int(nil), current...)
	default:
		return value
	}
}

func validateJSON(label string, value any) error {
	if _, err := json.Marshal(value); err != nil {
		return fmt.Errorf("%s is not JSON serializable: %w", label, err)
	}
	return nil
}

func prepareCheckpoint(checkpoint Checkpoint) Checkpoint {
	prepared := checkpoint.Clone()
	if prepared.SchemaVersion == 0 {
		prepared.SchemaVersion = CurrentCheckpointSchemaVersion
	}
	return prepared
}
