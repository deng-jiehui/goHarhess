package callback

import (
	"context"
	"fmt"
	"time"
)

// Phase identifies a lifecycle boundary.
type Phase string

const (
	PhaseStart  Phase = "start"
	PhaseOutput Phase = "output"
	PhaseEnd    Phase = "end"
	PhaseError  Phase = "error"
)

// Trace identifies a root run and one nested operation.
type Trace struct {
	TraceID      string `json:"trace_id"`
	RunID        string `json:"run_id"`
	SpanID       string `json:"span_id"`
	ParentSpanID string `json:"parent_span_id,omitempty"`
}

// Invocation is payload-free lifecycle and tracing metadata.
type Invocation struct {
	Component string         `json:"component"`
	Operation string         `json:"operation"`
	Phase     Phase          `json:"phase"`
	Trace     Trace          `json:"trace"`
	StartedAt time.Time      `json:"started_at"`
	Timestamp time.Time      `json:"timestamp"`
	Duration  time.Duration  `json:"duration,omitempty"`
	Metadata  map[string]any `json:"metadata,omitempty"`
}

func (i Invocation) Clone() Invocation {
	i.Metadata = CloneMetadata(i.Metadata)
	return i
}

// WithPhase returns a terminal or progress snapshot of the same operation.
func (i Invocation) WithPhase(phase Phase) Invocation {
	now := time.Now()
	i.Phase = phase
	i.Timestamp = now
	if phase == PhaseEnd || phase == PhaseError {
		i.Duration = now.Sub(i.StartedAt)
	}
	return i.Clone()
}

type PanicError struct {
	Handler string
	Value   any
}

func (e *PanicError) Error() string {
	return fmt.Sprintf("callback %q panicked: %v", e.Handler, e.Value)
}

type TimeoutError struct {
	Handler string
	Timeout time.Duration
}

func (e *TimeoutError) Error() string {
	return fmt.Sprintf("callback %q exceeded execution timeout %s", e.Handler, e.Timeout)
}
func (e *TimeoutError) Unwrap() error { return context.DeadlineExceeded }

type CallbackError struct {
	Handler    string
	Invocation Invocation
	Err        error
}

func (e CallbackError) Error() string {
	return fmt.Sprintf("callback %q handling %s/%s/%s: %v", e.Handler, e.Invocation.Component, e.Invocation.Operation, e.Invocation.Phase, e.Err)
}
func (e CallbackError) Unwrap() error { return e.Err }
