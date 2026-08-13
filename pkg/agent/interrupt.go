package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"

	"goharness/pkg/callback"
	"goharness/pkg/graph"
	"goharness/pkg/runnable"
	"goharness/pkg/schema"
	"goharness/pkg/state"
)

var (
	ErrInterrupted    = errors.New("agent execution interrupted")
	ErrResumeRequired = errors.New("conversation has an interrupted continuation; call agent.Resume")
)

type AgentKind string

const (
	AgentKindBasic     AgentKind = "basic"
	AgentKindReact     AgentKind = "react"
	AgentKindRouting   AgentKind = "routing"
	AgentKindGroupChat AgentKind = "group_chat"
)

type BreakpointPoint string

const (
	// BreakpointBeforeLLM is safe because all prior tool results have already
	// been appended to the provider-neutral working conversation.
	BreakpointBeforeLLM BreakpointPoint = "before_llm"
	// BreakpointBeforeRound is the GroupChat equivalent between complete rounds.
	BreakpointBeforeRound BreakpointPoint = "before_round"
	// BreakpointBeforeCommit catches signals raised during the final model stream
	// while the complete response is available but not yet committed.
	BreakpointBeforeCommit BreakpointPoint = "before_commit"
)

// Breakpoint is an immutable observation passed to a synchronous Breakpointer.
// It contains no runtime objects or credentials.
type Breakpoint struct {
	ConversationID string
	AgentName      string
	AgentKind      AgentKind
	Point          BreakpointPoint
	Messages       []schema.Message
	Response       []schema.Message
	LLMCalls       int
	Cursor         map[string]any
	Metadata       map[string]any
}

type InterruptDecision struct {
	Interrupt bool
	Reason    string
	Metadata  map[string]any
}

type Breakpointer interface {
	CheckBreakpoint(context.Context, Breakpoint) (InterruptDecision, error)
}

type BreakpointFunc func(context.Context, Breakpoint) (InterruptDecision, error)

func (f BreakpointFunc) CheckBreakpoint(ctx context.Context, point Breakpoint) (InterruptDecision, error) {
	if f == nil {
		return InterruptDecision{}, nil
	}
	return f(ctx, point)
}

// InterruptSignal can be requested by another goroutine. The next safe point
// consumes the request and persists a continuation.
type InterruptSignal struct {
	mu        sync.Mutex
	requested bool
	reason    string
	metadata  map[string]any
}

func NewInterruptSignal() *InterruptSignal { return &InterruptSignal{} }

func (s *InterruptSignal) Interrupt(reason string, metadata map[string]any) {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.requested = true
	s.reason = strings.TrimSpace(reason)
	s.metadata = callback.CloneMetadata(metadata)
	s.mu.Unlock()
}

func (s *InterruptSignal) CheckBreakpoint(context.Context, Breakpoint) (InterruptDecision, error) {
	if s == nil {
		return InterruptDecision{}, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.requested {
		return InterruptDecision{}, nil
	}
	decision := InterruptDecision{Interrupt: true, Reason: s.reason, Metadata: callback.CloneMetadata(s.metadata)}
	s.requested = false
	s.reason = ""
	s.metadata = nil
	return decision, nil
}

// InterruptedError carries the exact persisted checkpoint required by Resume.
type InterruptedError struct {
	Point      BreakpointPoint
	Reason     string
	Checkpoint state.Checkpoint
}

func (e *InterruptedError) Error() string {
	return fmt.Sprintf("%v at %s: %s", ErrInterrupted, e.Point, e.Reason)
}

func (e *InterruptedError) Unwrap() error { return ErrInterrupted }

func (e *InterruptedError) PortableCheckpoint() state.Checkpoint {
	if e == nil {
		return state.Checkpoint{}
	}
	return e.Checkpoint.Clone()
}

func (b *Base) checkBreakpoint(
	ctx context.Context,
	session *conversationSession,
	opts RunOptions,
	kind AgentKind,
	point BreakpointPoint,
	messages []schema.Message,
	response []schema.Message,
	llmCalls int,
	cursor map[string]any,
	engine *state.EngineCheckpoint,
) error {
	if opts.interruptBypass {
		return nil
	}
	breakpointers := make([]Breakpointer, 0, len(b.breakpointers)+len(opts.Breakpointers)+1)
	breakpointers = append(breakpointers, b.breakpointers...)
	breakpointers = append(breakpointers, opts.Breakpointers...)
	if opts.InterruptSignal != nil {
		breakpointers = append(breakpointers, opts.InterruptSignal)
	}
	if len(breakpointers) == 0 {
		return nil
	}
	conversationID := ""
	if session != nil {
		conversationID = session.checkpoint.ConversationID
	}
	breakpoint := Breakpoint{
		ConversationID: conversationID,
		AgentName:      b.name,
		AgentKind:      kind,
		Point:          point,
		Messages:       portableMessages(messages),
		Response:       portableMessages(response),
		LLMCalls:       llmCalls,
		Cursor:         callback.CloneMetadata(cursor),
		Metadata:       callback.CloneMetadata(opts.Metadata),
	}
	for index, breakpointer := range breakpointers {
		if breakpointer == nil {
			return fmt.Errorf("breakpointer at index %d cannot be nil", index)
		}
		decision, err := callBreakpointer(ctx, breakpointer, breakpoint)
		if err != nil {
			return fmt.Errorf("check breakpoint %s with breakpointer %d: %w", point, index, err)
		}
		if !decision.Interrupt {
			continue
		}
		if session == nil {
			return errors.New("interrupt requires WithConversationID and a checkpoint store")
		}
		reason := strings.TrimSpace(decision.Reason)
		if reason == "" {
			reason = "user breakpoint"
		}
		persistedCursor := callback.CloneMetadata(cursor)
		if engine != nil {
			// GroupChat's last agent is typed EngineCheckpoint state, not an
			// untyped continuation cursor field.
			delete(persistedCursor, "last_agent")
		}
		var engineState *state.EngineCheckpoint
		if engine != nil {
			checkpoint := state.EngineCheckpoint{
				SchemaVersion: state.CurrentEngineCheckpointSchemaVersion,
				AgentKind:     string(kind),
				Point:         string(point),
				LLMCalls:      llmCalls,
				Round:         engine.Round,
				LastAgent:     engine.LastAgent,
				CursorStack:   state.CursorStackFromContext(ctx),
			}
			engineState = &checkpoint
		}
		var graphState *state.GraphStateCheckpoint
		if checkpoint, ok, snapshotErr := graph.StateCheckpointFromContext(ctx); snapshotErr != nil {
			return fmt.Errorf("snapshot Graph State at breakpoint %s: %w", point, snapshotErr)
		} else if ok {
			checkpointCopy := checkpoint.Clone()
			graphState = &checkpointCopy
		} else if checkpoint, ok := state.GraphStateCheckpointFromContext(ctx); ok {
			checkpointCopy := checkpoint.Clone()
			graphState = &checkpointCopy
		} else if opts.graphState != nil {
			checkpointCopy := opts.graphState.Clone()
			graphState = &checkpointCopy
		}
		saved, err := session.interrupt(ctx, state.Continuation{
			AgentKind:   string(kind),
			Point:       string(point),
			Input:       schema.CloneMessages(session.input),
			Messages:    session.portableWorkingMessages(messages),
			Response:    schema.CloneMessages(response),
			LLMCalls:    llmCalls,
			Cursor:      persistedCursor,
			CursorStack: state.CursorStackFromContext(ctx),
			Engine:      engineState,
			Reason:      reason,
			Metadata:    callback.CloneMetadata(decision.Metadata),
			GraphState:  graphState,
		})
		if err != nil {
			return err
		}
		return &InterruptedError{Point: point, Reason: reason, Checkpoint: saved}
	}
	return nil
}

func portableMessages(messages []schema.Message) []schema.Message {
	data, err := json.Marshal(messages)
	if err == nil {
		var cloned []schema.Message
		if err := json.Unmarshal(data, &cloned); err == nil {
			return cloned
		}
	}
	return schema.CloneMessages(messages)
}

func callBreakpointer(ctx context.Context, value Breakpointer, breakpoint Breakpoint) (decision InterruptDecision, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("breakpointer panic: %v", recovered)
		}
	}()
	return value.CheckBreakpoint(ctx, breakpoint)
}

func continuationFor(opts RunOptions, kind AgentKind, point BreakpointPoint) (state.Continuation, bool, error) {
	if opts.continuation == nil {
		return state.Continuation{}, false, nil
	}
	continuation := opts.continuation.Clone()
	if continuation.AgentKind != string(kind) {
		return state.Continuation{}, false, fmt.Errorf(
			"checkpoint continuation requires agent kind %q, got %q",
			continuation.AgentKind,
			kind,
		)
	}
	if continuation.Point != string(point) {
		return state.Continuation{}, false, fmt.Errorf(
			"checkpoint continuation point %q is not supported by agent kind %q",
			continuation.Point,
			kind,
		)
	}
	return continuation, true, nil
}

func resumeAtCommit(opts *RunOptions, kind AgentKind) ([]schema.Message, bool, error) {
	if opts == nil || opts.continuation == nil || opts.continuation.Point != string(BreakpointBeforeCommit) {
		return nil, false, nil
	}
	continuation := opts.continuation.Clone()
	if continuation.AgentKind != string(kind) {
		return nil, false, fmt.Errorf(
			"checkpoint continuation requires agent kind %q, got %q",
			continuation.AgentKind,
			kind,
		)
	}
	opts.interruptBypass = true
	return schema.CloneMessages(continuation.Response), true, nil
}

func completedResumeEvents(messages []schema.Message) runnable.Stream[schema.MessageEvent] {
	out := make(chan runnable.Item[schema.MessageEvent], 2)
	if len(messages) > 0 {
		out <- runnable.Item[schema.MessageEvent]{Value: schema.MessageEvent{Kind: schema.MessageEventSnapshot, Messages: schema.CloneMessages(messages)}}
	}
	out <- runnable.Item[schema.MessageEvent]{Value: schema.MessageEvent{Kind: schema.MessageEventDone}}
	close(out)
	return out
}

// Resume continues a portable interrupted checkpoint with the supplied Agent.
// The Agent may use a different model instance, endpoint or API key.
func Resume(ctx context.Context, value Agent, conversationID string, opts ...RunOption) ([]schema.Message, error) {
	if value == nil {
		return nil, errors.New("resume agent cannot be nil")
	}
	options := append([]RunOption(nil), opts...)
	options = append(options, WithConversationID(conversationID), withResume())
	return value.Invoke(ctx, nil, runOptionsAsRunnable(options)...)
}

func ResumeStream(ctx context.Context, value Agent, conversationID string, opts ...RunOption) (runnable.Stream[schema.MessageEvent], error) {
	if value == nil {
		return nil, errors.New("resume agent cannot be nil")
	}
	options := append([]RunOption(nil), opts...)
	options = append(options, WithConversationID(conversationID), withResume())
	return value.Stream(ctx, nil, runOptionsAsRunnable(options)...)
}

func withResume() RunOption {
	return func(opts *RunOptions) { opts.resume = true }
}

func withInterruptBypass() RunOption {
	return func(opts *RunOptions) { opts.interruptBypass = true }
}
