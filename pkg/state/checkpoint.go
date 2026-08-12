// Package state persists versioned conversation checkpoints independently
// from context-window assembly and model-provider payloads.
package state

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"goharness/pkg/schema"
)

var (
	ErrNilContext = errors.New("checkpoint context cannot be nil")
	ErrConflict   = errors.New("checkpoint version conflict")
)

const CurrentCheckpointSchemaVersion = 1
const CurrentEngineCheckpointSchemaVersion = 1

type CheckpointStatus string

const (
	CheckpointStatusReady       CheckpointStatus = "ready"
	CheckpointStatusInterrupted CheckpointStatus = "interrupted"
)

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

// Continuation is a provider-neutral cursor captured only at framework safe
// points. It intentionally contains no context, goroutine, callback, client,
// model implementation, credential or API key.
type Continuation struct {
	AgentKind     string                `json:"agent_kind"`
	Point         string                `json:"point"`
	Input         []schema.Message      `json:"input,omitempty"`
	Messages      []schema.Message      `json:"messages"`
	Response      []schema.Message      `json:"response,omitempty"`
	LLMCalls      int                   `json:"llm_calls"`
	Cursor        map[string]any        `json:"cursor,omitempty"`
	CursorStack   CursorStack           `json:"cursor_stack,omitempty"`
	Reason        string                `json:"reason,omitempty"`
	Metadata      map[string]any        `json:"metadata,omitempty"`
	Engine        *EngineCheckpoint     `json:"engine,omitempty"`
	GraphState    *GraphStateCheckpoint `json:"graph_state,omitempty"`
	InterruptedAt time.Time             `json:"interrupted_at"`
}

// EngineCheckpoint is the durable, provider-neutral execution control state
// that cannot be safely inferred from business messages alone. It contains no
// runtime objects, closures, contexts, clients or credentials.
type EngineCheckpoint struct {
	SchemaVersion int         `json:"schema_version"`
	AgentKind     string      `json:"agent_kind"`
	Point         string      `json:"point"`
	LLMCalls      int         `json:"llm_calls"`
	Round         int         `json:"round,omitempty"`
	LastAgent     string      `json:"last_agent,omitempty"`
	CursorStack   CursorStack `json:"cursor_stack,omitempty"`
}

func (c EngineCheckpoint) Clone() EngineCheckpoint {
	c.CursorStack = c.CursorStack.Clone()
	return c
}

func (c EngineCheckpoint) Validate() error {
	if c.SchemaVersion != 0 && c.SchemaVersion != CurrentEngineCheckpointSchemaVersion {
		return fmt.Errorf("unsupported engine checkpoint schema version %d", c.SchemaVersion)
	}
	if strings.TrimSpace(c.AgentKind) == "" || strings.TrimSpace(c.Point) == "" {
		return errors.New("engine checkpoint requires agent kind and point")
	}
	if c.LLMCalls < 0 || c.Round < 0 {
		return errors.New("engine checkpoint counters cannot be negative")
	}
	return c.CursorStack.Validate()
}

func (c EngineCheckpoint) MarshalJSON() ([]byte, error) {
	if c.SchemaVersion == 0 {
		c.SchemaVersion = CurrentEngineCheckpointSchemaVersion
	}
	if err := c.Validate(); err != nil {
		return nil, err
	}
	type plain EngineCheckpoint
	return json.Marshal(plain(c))
}

func (c *EngineCheckpoint) UnmarshalJSON(data []byte) error {
	type plain EngineCheckpoint
	var decoded plain
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	value := EngineCheckpoint(decoded)
	if value.SchemaVersion == 0 {
		value.SchemaVersion = CurrentEngineCheckpointSchemaVersion
	}
	if err := value.Validate(); err != nil {
		return err
	}
	*c = value
	return nil
}

func (c Continuation) Clone() Continuation {
	c.Input = cloneMessages(c.Input)
	c.Messages = cloneMessages(c.Messages)
	c.Response = cloneMessages(c.Response)
	c.Metadata = cloneMap(c.Metadata)
	c.Cursor = cloneMap(c.Cursor)
	c.CursorStack = c.CursorStack.Clone()
	if c.Engine != nil {
		engine := c.Engine.Clone()
		c.Engine = &engine
	}
	if c.GraphState != nil {
		graphState := c.GraphState.Clone()
		c.GraphState = &graphState
	}
	return c
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
	Status         CheckpointStatus `json:"status"`
	Messages       []schema.Message `json:"messages,omitempty"`
	Values         map[string]any   `json:"values,omitempty"`
	Continuation   *Continuation    `json:"continuation,omitempty"`
	CreatedAt      time.Time        `json:"created_at"`
	UpdatedAt      time.Time        `json:"updated_at"`
}

func (c Checkpoint) Clone() Checkpoint {
	c.Messages = cloneMessages(c.Messages)
	c.Values = cloneMap(c.Values)
	if c.Continuation != nil {
		continuation := c.Continuation.Clone()
		c.Continuation = &continuation
	}
	return c
}

func (c Checkpoint) Validate() error {
	if strings.TrimSpace(c.ConversationID) == "" {
		return errors.New("checkpoint conversation ID cannot be empty")
	}
	if c.SchemaVersion != 0 && c.SchemaVersion != CurrentCheckpointSchemaVersion {
		return fmt.Errorf("unsupported checkpoint schema version %d", c.SchemaVersion)
	}
	if c.Status != "" && c.Status != CheckpointStatusReady && c.Status != CheckpointStatusInterrupted {
		return fmt.Errorf("unsupported checkpoint status %q", c.Status)
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
	if c.Continuation != nil {
		if strings.TrimSpace(c.Continuation.AgentKind) == "" || strings.TrimSpace(c.Continuation.Point) == "" {
			return errors.New("checkpoint continuation requires agent kind and point")
		}
		if c.Continuation.LLMCalls < 0 {
			return errors.New("checkpoint continuation LLM calls cannot be negative")
		}
		for _, group := range []struct {
			name     string
			messages []schema.Message
		}{
			{name: "input", messages: c.Continuation.Input},
			{name: "messages", messages: c.Continuation.Messages},
			{name: "response", messages: c.Continuation.Response},
		} {
			for index, message := range group.messages {
				if err := message.Validate(); err != nil {
					return fmt.Errorf("checkpoint continuation %s message %d: %w", group.name, index, err)
				}
			}
			if err := validateJSON("checkpoint continuation "+group.name, group.messages); err != nil {
				return err
			}
		}
		if err := validateJSON("checkpoint continuation metadata", c.Continuation.Metadata); err != nil {
			return err
		}
		if err := validateJSON("checkpoint continuation cursor", c.Continuation.Cursor); err != nil {
			return err
		}
		if err := c.Continuation.CursorStack.Validate(); err != nil {
			return err
		}
		if err := validateJSON("checkpoint continuation cursor stack", c.Continuation.CursorStack); err != nil {
			return err
		}
		if c.Continuation.Engine != nil {
			if err := c.Continuation.Engine.Validate(); err != nil {
				return err
			}
			if err := validateJSON("checkpoint continuation engine", c.Continuation.Engine); err != nil {
				return err
			}
		}
		if c.Continuation.GraphState != nil {
			if err := c.Continuation.GraphState.Validate(); err != nil {
				return fmt.Errorf("checkpoint continuation graph state: %w", err)
			}
		}
	}
	if c.Status == CheckpointStatusInterrupted && c.Continuation == nil {
		return errors.New("interrupted checkpoint requires a continuation")
	}
	if c.Status == CheckpointStatusReady && c.Continuation != nil {
		return errors.New("ready checkpoint cannot contain a continuation")
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
	if err := json.Unmarshal(data, &checkpoint); err != nil {
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
	if prepared.Status == "" {
		if prepared.Continuation != nil {
			prepared.Status = CheckpointStatusInterrupted
		} else {
			prepared.Status = CheckpointStatusReady
		}
	}
	return prepared
}
