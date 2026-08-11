package agent

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"goharness/pkg/callback"
	"goharness/pkg/schema"
	"goharness/pkg/state"
)

type conversationSession struct {
	store        state.Store
	writeTimeout time.Duration
	checkpoint   state.Checkpoint
	input        []schema.Message
	values       map[string]any
}

func (s *conversationSession) bind(ctx context.Context) context.Context {
	if s == nil {
		return ctx
	}
	checkpoint := s.checkpoint.Clone()
	checkpoint.Values = callback.CloneMetadata(s.values)
	ctx = state.WithCheckpoint(ctx, checkpoint)
	if checkpoint.Continuation != nil {
		ctx = state.WithCursorStack(ctx, checkpoint.Continuation.CursorStack)
		if checkpoint.Continuation.GraphState != nil {
			ctx = state.WithGraphStateCheckpoint(ctx, *checkpoint.Continuation.GraphState)
		}
	}
	return ctx
}

func bindRunContext(ctx context.Context, session *conversationSession, opts *RunOptions) context.Context {
	ctx = session.bind(ctx)
	if opts == nil {
		return ctx
	}
	if checkpoint, ok := state.GraphStateCheckpointFromContext(ctx); ok {
		copy := checkpoint.Clone()
		opts.graphState = &copy
		return state.IgnoreGraphStateCheckpoint(ctx)
	}
	return ctx
}

func (b *Base) loadConversation(
	ctx context.Context,
	incoming []schema.Message,
	opts *RunOptions,
) ([]schema.Message, *conversationSession, error) {
	if opts == nil || opts.CheckpointBypass || strings.TrimSpace(opts.ConversationID) == "" {
		return schema.CloneMessages(incoming), nil, nil
	}
	if ctx == nil {
		return nil, nil, errors.New("conversation load context cannot be nil")
	}
	if b.checkpointStore == nil {
		return nil, nil, errors.New("conversation ID requires an Agent checkpoint store")
	}
	conversationID := strings.TrimSpace(opts.ConversationID)
	checkpoint, found, err := b.checkpointStore.Load(ctx, conversationID)
	if err != nil {
		return nil, nil, fmt.Errorf("load conversation checkpoint %q: %w", conversationID, err)
	}
	if !found {
		if opts.resume {
			return nil, nil, fmt.Errorf("resume conversation checkpoint %q: not found", conversationID)
		}
		checkpoint = state.Checkpoint{ConversationID: conversationID, Values: map[string]any{}}
	}
	if opts.resume {
		if checkpoint.Status != state.CheckpointStatusInterrupted || checkpoint.Continuation == nil {
			return nil, nil, fmt.Errorf("resume conversation checkpoint %q: no interrupted continuation", conversationID)
		}
		continuation := checkpoint.Continuation.Clone()
		opts.continuation = &continuation
	} else if checkpoint.Status == state.CheckpointStatusInterrupted {
		return nil, nil, fmt.Errorf("conversation checkpoint %q: %w", conversationID, ErrResumeRequired)
	}
	values := checkpoint.Clone().Values
	for key, value := range opts.StateValues {
		values[key] = value
	}
	opts.Metadata["conversation_id"] = conversationID
	opts.Metadata["checkpoint_version"] = checkpoint.Version
	assembled := append(schema.CloneMessages(checkpoint.Messages), schema.CloneMessages(incoming)...)
	input := schema.CloneMessages(incoming)
	if opts.resume {
		assembled = schema.CloneMessages(checkpoint.Continuation.Messages)
		input = schema.CloneMessages(checkpoint.Continuation.Input)
	}
	return assembled, &conversationSession{
		store:        b.checkpointStore,
		writeTimeout: b.checkpointTimeout,
		checkpoint:   checkpoint.Clone(),
		input:        input,
		values:       callback.CloneMetadata(values),
	}, nil
}

func (s *conversationSession) commit(ctx context.Context, output []schema.Message) error {
	if s == nil || s.store == nil {
		return nil
	}
	checkpoint := s.checkpoint.Clone()
	checkpoint.Messages = append(checkpoint.Messages, schema.CloneMessages(s.input)...)
	checkpoint.Messages = append(checkpoint.Messages, withoutSystemMessages(output)...)
	checkpoint.Values = callback.CloneMetadata(s.values)
	checkpoint.Status = state.CheckpointStatusReady
	checkpoint.Continuation = nil
	writeCtx, cancel := s.writeContext(ctx)
	defer cancel()
	saved, err := s.store.Save(writeCtx, checkpoint)
	if err != nil {
		return fmt.Errorf("save conversation checkpoint %q: %w", checkpoint.ConversationID, err)
	}
	s.checkpoint = saved.Clone()
	return nil
}

func (s *conversationSession) interrupt(ctx context.Context, continuation state.Continuation) (state.Checkpoint, error) {
	if s == nil || s.store == nil {
		return state.Checkpoint{}, errors.New("interrupt checkpoint session is not configured")
	}
	continuation.InterruptedAt = time.Now().UTC()
	checkpoint := s.checkpoint.Clone()
	checkpoint.Status = state.CheckpointStatusInterrupted
	checkpoint.Continuation = &continuation
	checkpoint.Values = callback.CloneMetadata(s.values)
	writeCtx, cancel := s.writeContext(ctx)
	defer cancel()
	saved, err := s.store.Save(writeCtx, checkpoint)
	if err != nil {
		return state.Checkpoint{}, fmt.Errorf("save interrupted checkpoint %q: %w", checkpoint.ConversationID, err)
	}
	s.checkpoint = saved.Clone()
	return saved.Clone(), nil
}

func (s *conversationSession) writeContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithTimeout(context.WithoutCancel(ctx), s.writeTimeout)
}

func withoutSystemMessages(messages []schema.Message) []schema.Message {
	filtered := make([]schema.Message, 0, len(messages))
	for _, message := range messages {
		if message.Role != schema.RoleSystem {
			filtered = append(filtered, schema.CloneMessages([]schema.Message{message})[0])
		}
	}
	return filtered
}

func systemMessages(messages []schema.Message) []schema.Message {
	systems := make([]schema.Message, 0)
	for _, message := range messages {
		if message.Role == schema.RoleSystem {
			systems = append(systems, schema.CloneMessages([]schema.Message{message})[0])
		}
	}
	return systems
}

func (s *conversationSession) portableWorkingMessages(working []schema.Message) []schema.Message {
	if s == nil {
		return withoutSystemMessages(working)
	}
	callerSystems := systemMessages(s.checkpoint.Messages)
	callerSystems = append(callerSystems, systemMessages(s.input)...)
	return append(callerSystems, withoutSystemMessages(working)...)
}
