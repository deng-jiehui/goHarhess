package graph

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"goharness/pkg/control"
	"goharness/pkg/runnable"
	statepkg "goharness/pkg/state"
)

const (
	CurrentContinuationSchemaVersion = 1
	DefaultCheckpointTimeout         = 5 * time.Second
)

var (
	ErrInterrupted           = errors.New("graph execution interrupted")
	ErrCheckpointRequired    = errors.New("graph interruption requires a checkpoint")
	ErrCheckpointNotFound    = errors.New("graph checkpoint not found")
	ErrInvalidContinuation   = errors.New("invalid graph continuation")
	ErrGraphRevisionMismatch = errors.New("graph revision mismatch")
	ErrGraphNotResumable     = errors.New("graph is not resumable")
)

type InterruptPoint string

const (
	InterruptPointBeforeNode  InterruptPoint = "before_node"
	InterruptPointDuringNode  InterruptPoint = "during_node"
	InterruptPointAfterCommit InterruptPoint = "after_commit"
)

// Continuation is the portable Graph execution state captured at one node
// boundary. It intentionally contains no edge values or runtime objects.
type Continuation struct {
	SchemaVersion int                           `json:"schema_version"`
	GraphName     string                        `json:"graph_name"`
	GraphRevision string                        `json:"graph_revision"`
	ResumeNode    string                        `json:"resume_node"`
	Step          int                           `json:"step"`
	Visits        map[string]int                `json:"visits,omitempty"`
	State         statepkg.GraphStateCheckpoint `json:"state"`
	CursorStack   statepkg.CursorStack          `json:"cursor_stack,omitempty"`
	Point         InterruptPoint                `json:"point"`
	Reason        string                        `json:"reason,omitempty"`
	Metadata      map[string]any                `json:"metadata,omitempty"`
	CreatedAt     time.Time                     `json:"created_at"`
}

func (c Continuation) Clone() Continuation {
	c.Visits = cloneVisits(c.Visits)
	c.State = c.State.Clone()
	c.CursorStack = c.CursorStack.Clone()
	c.Metadata = clonePortableMetadata(c.Metadata)
	return c
}

func (c Continuation) Validate() error {
	if c.SchemaVersion != CurrentContinuationSchemaVersion {
		return fmt.Errorf("%w: unsupported schema version %d", ErrInvalidContinuation, c.SchemaVersion)
	}
	if strings.TrimSpace(c.GraphName) == "" || strings.TrimSpace(c.GraphRevision) == "" {
		return fmt.Errorf("%w: graph name and revision are required", ErrInvalidContinuation)
	}
	if strings.TrimSpace(c.ResumeNode) == "" || c.ResumeNode == Start || c.ResumeNode == End {
		return fmt.Errorf("%w: invalid resume node %q", ErrInvalidContinuation, c.ResumeNode)
	}
	if c.Step < 0 {
		return fmt.Errorf("%w: step cannot be negative", ErrInvalidContinuation)
	}
	for node, visits := range c.Visits {
		if strings.TrimSpace(node) == "" || visits < 0 {
			return fmt.Errorf("%w: invalid visit count for %q", ErrInvalidContinuation, node)
		}
	}
	switch c.Point {
	case InterruptPointBeforeNode, InterruptPointDuringNode, InterruptPointAfterCommit:
	default:
		return fmt.Errorf("%w: unsupported interrupt point %q", ErrInvalidContinuation, c.Point)
	}
	if err := c.State.Validate(); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidContinuation, err)
	}
	if err := c.CursorStack.Validate(); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidContinuation, err)
	}
	if _, err := json.Marshal(c.Metadata); err != nil {
		return fmt.Errorf("%w: metadata is not JSON serializable: %v", ErrInvalidContinuation, err)
	}
	return nil
}

func (c Continuation) JSON() ([]byte, error) {
	copy := c.Clone()
	if err := copy.Validate(); err != nil {
		return nil, err
	}
	return json.Marshal(copy)
}

func DecodeContinuation(data []byte) (Continuation, error) {
	var continuation Continuation
	if err := json.Unmarshal(append([]byte(nil), data...), &continuation); err != nil {
		return Continuation{}, fmt.Errorf("%w: decode JSON: %v", ErrInvalidContinuation, err)
	}
	if err := continuation.Validate(); err != nil {
		return Continuation{}, err
	}
	return continuation.Clone(), nil
}

// InterruptedError is returned only after the continuation has been saved.
type InterruptedError struct {
	Point        InterruptPoint
	Reason       string
	Continuation Continuation
}

func (e *InterruptedError) Error() string {
	if e == nil {
		return ErrInterrupted.Error()
	}
	return fmt.Sprintf("%v at %s: %s", ErrInterrupted, e.Point, e.Reason)
}

func (*InterruptedError) Unwrap() error { return ErrInterrupted }

func (e *InterruptedError) PortableContinuation() Continuation {
	if e == nil {
		return Continuation{}
	}
	return e.Continuation.Clone()
}

type checkpointConfig struct {
	Store   control.CheckpointStore
	Key     string
	Timeout time.Duration
}

const checkpointOptionKey = "graph.checkpoint"

type checkpointOption struct{ config checkpointConfig }

func (o checkpointOption) Apply(set *runnable.OptionSet) error {
	if set == nil {
		return errors.New("runnable option set is nil")
	}
	if isNilCheckpointStore(o.config.Store) {
		return errors.New("graph checkpoint store cannot be nil")
	}
	o.config.Key = strings.TrimSpace(o.config.Key)
	if o.config.Key == "" {
		return errors.New("graph checkpoint key cannot be empty")
	}
	if o.config.Timeout <= 0 {
		o.config.Timeout = DefaultCheckpointTimeout
	}
	if set.Values == nil {
		set.Values = map[string]any{}
	}
	set.Values[checkpointOptionKey] = o.config
	return nil
}

func (checkpointOption) OptionScope() runnable.OptionScope { return runnable.OptionScopeGlobal }

func WithCheckpoint(store control.CheckpointStore, key string) runnable.Option {
	return checkpointOption{config: checkpointConfig{Store: store, Key: key, Timeout: DefaultCheckpointTimeout}}
}

// RequestInterrupt is a convenience alias for the shared control protocol.
func RequestInterrupt(reason string, metadata map[string]any) error {
	return control.RequestInterrupt(reason, metadata)
}

func checkpointFromPlan(plan runnable.OptionPlan) (checkpointConfig, bool, error) {
	value, exists := plan.Set().Values[checkpointOptionKey]
	if !exists {
		return checkpointConfig{}, false, nil
	}
	config, ok := value.(checkpointConfig)
	if !ok || isNilCheckpointStore(config.Store) || strings.TrimSpace(config.Key) == "" {
		return checkpointConfig{}, false, errors.New("graph checkpoint option is invalid")
	}
	if config.Timeout <= 0 {
		config.Timeout = DefaultCheckpointTimeout
	}
	return config, true, nil
}

func isNilCheckpointStore(store control.CheckpointStore) bool {
	if store == nil {
		return true
	}
	value := reflect.ValueOf(store)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

func cloneVisits(source map[string]int) map[string]int {
	if source == nil {
		return nil
	}
	copy := make(map[string]int, len(source))
	for key, value := range source {
		copy[key] = value
	}
	return copy
}

func clonePortableMetadata(source map[string]any) map[string]any {
	if source == nil {
		return nil
	}
	data, err := json.Marshal(source)
	if err != nil {
		copy := make(map[string]any, len(source))
		for key, value := range source {
			copy[key] = value
		}
		return copy
	}
	var copy map[string]any
	if err := json.Unmarshal(data, &copy); err != nil {
		return nil
	}
	return copy
}
