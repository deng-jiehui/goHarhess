// Package callback provides synchronous lifecycle observations for Agents,
// model calls and tools. Handlers are observational: their failures never
// replace a business result or error.
package callback

import (
	"context"
	"fmt"
	"time"
)

// EventType identifies one stable lifecycle hook.
type EventType string

const (
	EventAgentStart EventType = "agent.start"
	EventAgentChunk EventType = "agent.chunk"
	EventAgentEnd   EventType = "agent.end"
	EventAgentError EventType = "agent.error"

	EventLLMStart EventType = "llm.start"
	EventLLMChunk EventType = "llm.chunk"
	EventLLMEnd   EventType = "llm.end"
	EventLLMError EventType = "llm.error"

	EventRetrieverStart EventType = "retriever.start"
	EventRetrieverEnd   EventType = "retriever.end"
	EventRetrieverError EventType = "retriever.error"

	EventToolStart EventType = "tool.start"
	EventToolEnd   EventType = "tool.end"
	EventToolError EventType = "tool.error"

	EventGraphStart EventType = "graph.start"
	// EventGraphNode is the legacy unified node event. New graph sinks emit the
	// phase-specific EventGraphNode* values below.
	EventGraphNode       EventType = "graph.node"
	EventGraphNodeStart  EventType = "graph.node.start"
	EventGraphNodeOutput EventType = "graph.node.output"
	EventGraphNodeEnd    EventType = "graph.node.end"
	EventGraphEnd        EventType = "graph.end"
	EventGraphError      EventType = "graph.error"
)

// GraphNodePhase identifies the lifecycle phase of a graph node event.
type GraphNodePhase string

const (
	GraphNodePhaseStart  GraphNodePhase = "node_start"
	GraphNodePhaseOutput GraphNodePhase = "node_output"
	GraphNodePhaseEnd    GraphNodePhase = "node_end"
)

// Stable Metadata keys for graph node identity and lifecycle phase.
const (
	MetadataGraphNodeID    = "graph.node_id"
	MetadataGraphStep      = "graph.step"
	MetadataGraphNodePhase = "graph.node_phase"
)

// Component is the kind of operation that produced an event.
type Component string

const (
	ComponentAgent     Component = "agent"
	ComponentLLM       Component = "llm"
	ComponentRetriever Component = "retriever"
	ComponentTool      Component = "tool"
	ComponentGraph     Component = "graph"
)

// Status is the provider-neutral state of a lifecycle event.
type Status string

const (
	StatusRunning Status = "running"
	StatusOK      Status = "ok"
	StatusError   Status = "error"
)

// Trace identifies a root Agent run and the current nested operation.
type Trace struct {
	TraceID      string `json:"trace_id"`
	RunID        string `json:"run_id"`
	SpanID       string `json:"span_id"`
	ParentSpanID string `json:"parent_span_id,omitempty"`
}

// Metrics contains safe, content-free measurements useful for monitoring.
// Custom is available for provider- or application-specific measurements.
type Metrics struct {
	InputMessages    int            `json:"input_messages,omitempty"`
	OutputMessages   int            `json:"output_messages,omitempty"`
	Chunks           int            `json:"chunks,omitempty"`
	ToolCalls        int            `json:"tool_calls,omitempty"`
	InputBytes       int64          `json:"input_bytes,omitempty"`
	OutputBytes      int64          `json:"output_bytes,omitempty"`
	PromptTokens     int            `json:"prompt_tokens,omitempty"`
	CompletionTokens int            `json:"completion_tokens,omitempty"`
	TotalTokens      int            `json:"total_tokens,omitempty"`
	ReasoningTokens  int            `json:"reasoning_tokens,omitempty"`
	Custom           map[string]any `json:"custom,omitempty"`
}

// Event is the immutable snapshot delivered to a Handler. Metadata contains
// context and RunOption metadata; prompts and credentials are never added by
// the framework.
type Event struct {
	Type      EventType      `json:"type"`
	Sequence  uint64         `json:"sequence"`
	Component Component      `json:"component"`
	Name      string         `json:"name,omitempty"`
	Trace     Trace          `json:"trace"`
	Timestamp time.Time      `json:"timestamp"`
	StartedAt time.Time      `json:"started_at"`
	Duration  time.Duration  `json:"duration,omitempty"`
	Status    Status         `json:"status"`
	Error     error          `json:"-"`
	ErrorText string         `json:"error,omitempty"`
	Metrics   Metrics        `json:"metrics,omitempty"`
	Metadata  map[string]any `json:"metadata,omitempty"`
}

// Operation is a timed span shared by the start and terminal events.
type Operation struct {
	Component Component
	Name      string
	Trace     Trace
	StartedAt time.Time
	Metadata  map[string]any
}

// StartEvent creates the first event for an operation.
func StartEvent(eventType EventType, operation Operation, metrics Metrics) Event {
	return Event{
		Type:      eventType,
		Component: operation.Component,
		Name:      operation.Name,
		Trace:     operation.Trace,
		Timestamp: operation.StartedAt,
		StartedAt: operation.StartedAt,
		Status:    StatusRunning,
		Metrics:   cloneMetrics(metrics),
		Metadata:  CloneMetadata(operation.Metadata),
	}
}

// ProgressEvent creates a point-in-time event within an operation.
func ProgressEvent(eventType EventType, operation Operation, metrics Metrics) Event {
	return Event{
		Type:      eventType,
		Component: operation.Component,
		Name:      operation.Name,
		Trace:     operation.Trace,
		Timestamp: time.Now(),
		StartedAt: operation.StartedAt,
		Status:    StatusRunning,
		Metrics:   cloneMetrics(metrics),
		Metadata:  CloneMetadata(operation.Metadata),
	}
}

// EndEvent creates a successful terminal event.
func EndEvent(eventType EventType, operation Operation, metrics Metrics) Event {
	now := time.Now()
	return Event{
		Type:      eventType,
		Component: operation.Component,
		Name:      operation.Name,
		Trace:     operation.Trace,
		Timestamp: now,
		StartedAt: operation.StartedAt,
		Duration:  now.Sub(operation.StartedAt),
		Status:    StatusOK,
		Metrics:   cloneMetrics(metrics),
		Metadata:  CloneMetadata(operation.Metadata),
	}
}

// ErrorEvent creates a failed terminal event without changing the business
// error observed by the caller.
func ErrorEvent(eventType EventType, operation Operation, callErr error, metrics Metrics) Event {
	event := EndEvent(eventType, operation, metrics)
	event.Status = StatusError
	event.Error = callErr
	if callErr != nil {
		event.ErrorText = callErr.Error()
	}
	return event
}

// Clone returns a callback-safe copy of an Event.
func (e Event) Clone() Event {
	e.Metadata = CloneMetadata(e.Metadata)
	e.Metrics = cloneMetrics(e.Metrics)
	return e
}

func cloneMetrics(metrics Metrics) Metrics {
	if metrics.Custom != nil {
		metrics.Custom = CloneMetadata(metrics.Custom)
	}
	return metrics
}

// PanicError reports a recovered callback panic.
type PanicError struct {
	Callback string
	Value    any
}

// TimeoutError reports a Handler that returned after Config.Timeout's
// cooperative deadline.
type TimeoutError struct {
	Callback string
	Timeout  time.Duration
}

func (e *TimeoutError) Error() string {
	return fmt.Sprintf("callback %q exceeded execution timeout %s", e.Callback, e.Timeout)
}

func (e *TimeoutError) Unwrap() error { return context.DeadlineExceeded }

func (e *PanicError) Error() string {
	return fmt.Sprintf("callback %q panicked: %v", e.Callback, e.Value)
}

// CallbackError identifies one isolated Handler failure. It is returned from
// SyncDispatcher.Dispatch and never returned from an Agent, LLM or tool
// invocation because framework business paths ignore callback errors.
type CallbackError struct {
	Callback string
	Event    Event
	Err      error
}

func (e CallbackError) Error() string {
	return fmt.Sprintf("callback %q handling %s: %v", e.Callback, e.Event.Type, e.Err)
}

func (e CallbackError) Unwrap() error { return e.Err }
