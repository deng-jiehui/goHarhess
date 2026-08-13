package agent

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"goharness/internal/agentrecipe"
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
	return state.WithCheckpoint(ctx, checkpoint)
}

func bindRunContext(ctx context.Context, session *conversationSession) context.Context {
	return session.bind(ctx)
}

func conversationInput(session *conversationSession, fallback []schema.Message) []schema.Message {
	if session != nil {
		return schema.CloneMessages(session.input)
	}
	return schema.CloneMessages(fallback)
}

func withRecipeCompletion(ctx context.Context, session *conversationSession) context.Context {
	return agentrecipe.WithCompletionSink(ctx, func(input []schema.Message) {
		if session != nil {
			session.input = schema.CloneMessages(input)
		}
	})
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
		checkpoint = state.Checkpoint{ConversationID: conversationID, Values: map[string]any{}}
	}
	values := checkpoint.Clone().Values
	for key, value := range opts.StateValues {
		values[key] = value
	}
	opts.Metadata["conversation_id"] = conversationID
	opts.Metadata["checkpoint_version"] = checkpoint.Version
	assembled := append(schema.CloneMessages(checkpoint.Messages), schema.CloneMessages(incoming)...)
	input := schema.CloneMessages(incoming)
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
	writeCtx, cancel := s.writeContext(ctx)
	defer cancel()
	saved, err := s.store.Save(writeCtx, checkpoint)
	if err != nil {
		return fmt.Errorf("save conversation checkpoint %q: %w", checkpoint.ConversationID, err)
	}
	s.checkpoint = saved.Clone()
	return nil
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
