package agent

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/goharness/goharness/internal/agentrecipe"
	"github.com/goharness/goharness/pkg/callback"
	"github.com/goharness/goharness/pkg/control"
	"github.com/goharness/goharness/pkg/graph"
	"github.com/goharness/goharness/pkg/schema"
	"github.com/goharness/goharness/pkg/state"
)

type conversationSession struct {
	checkpoint state.Checkpoint
	input      []schema.Message
	values     map[string]any
}

func (s *conversationSession) bind(ctx context.Context) context.Context {
	if s == nil {
		return ctx
	}
	checkpoint := s.checkpoint.Clone()
	checkpoint.Values = callback.CloneMetadata(s.values)
	ctx = state.WithCheckpoint(ctx, checkpoint)
	// 会话标识属于运行追踪信息，经 ctx 元数据下发给回调与工具；
	// 与持久化 State 分离，不参与业务 values。
	return callback.WithMetadata(ctx, map[string]any{
		"conversation_id":    checkpoint.ConversationID,
		"checkpoint_version": checkpoint.Version,
	})
}

func bindRunContext(ctx context.Context, session *conversationSession) context.Context {
	return session.bind(ctx)
}

func conversationInput(session *conversationSession, fallback []schema.Message) []schema.Message {
	if session != nil {
		// session.input 归本次运行独有且按不可变约定只读，复制切片即可。
		return append([]schema.Message(nil), session.input...)
	}
	return schema.CloneMessages(fallback)
}

// recipeRunConfig 将完整会话与模型工作消息分开，恢复时由持久化 State 提供。
// 会话侧数据在 loadConversation 完成隔离，这里只装配新切片、共享不可变元素。
func recipeRunConfig(session *conversationSession, messages []schema.Message, maxLLMCalls int) agentrecipe.RunConfig {
	cfg := agentrecipe.RunConfig{Input: conversationInput(session, messages), MaxLLMCalls: maxLLMCalls}
	if session != nil {
		cfg.History = make([]schema.Message, 0, len(session.checkpoint.Messages)+len(session.input))
		cfg.History = append(cfg.History, session.checkpoint.Messages...)
		cfg.History = append(cfg.History, session.input...)
	} else {
		cfg.History = schema.CloneMessages(messages)
	}
	return cfg
}

func (b *base) loadConversation(
	ctx context.Context,
	incoming []schema.Message,
	opts *RunOptions,
) ([]schema.Message, *conversationSession, error) {
	if opts == nil || opts.CheckpointBypass {
		return schema.CloneMessages(incoming), nil, nil
	}
	if ctx == nil {
		return nil, nil, errors.New("conversation load context cannot be nil")
	}
	store := b.checkpointStore
	conversationID := strings.TrimSpace(opts.ConversationID)
	if opts.checkpointSet {
		opts.checkpointKey = strings.TrimSpace(opts.checkpointKey)
		if opts.checkpointKey == "" {
			return nil, nil, errors.New("checkpoint ID cannot be empty")
		}
		if conversationID != "" && conversationID != opts.checkpointKey {
			return nil, nil, errors.New("checkpoint ID and conversation ID must match")
		}
		conversationID = opts.checkpointKey
		store = opts.checkpointStore
	}
	if conversationID == "" {
		return schema.CloneMessages(incoming), nil, nil
	}
	if store == nil || (reflect.ValueOf(store).Kind() == reflect.Pointer && reflect.ValueOf(store).IsNil()) {
		return nil, nil, errors.New("conversation ID requires an Agent checkpoint store")
	}
	loadCtx := ctx
	if _, interrupted := control.AsInterrupt(context.Cause(ctx)); interrupted {
		loadCtx = context.WithoutCancel(ctx)
	}
	loadCtx, cancel := context.WithTimeout(loadCtx, b.checkpointTimeout)
	defer cancel()
	checkpoint, found, err := store.Load(loadCtx, conversationID)
	if err != nil {
		return nil, nil, fmt.Errorf("load conversation checkpoint %q: %w", conversationID, err)
	}
	if !found {
		checkpoint = state.Checkpoint{ConversationID: conversationID, Values: map[string]any{}}
	}
	if !opts.resume && checkpoint.Unfinished() {
		if !opts.abandonUnfinished {
			return nil, nil, fmt.Errorf("conversation %q: %w", conversationID, state.ErrUnfinishedRun)
		}
		if checkpoint, err = state.AbandonRun(loadCtx, store, conversationID); err != nil {
			return nil, nil, fmt.Errorf("abandon unfinished run of conversation %q: %w", conversationID, err)
		}
	}
	opts.ConversationID = conversationID
	// 检查点与持久化是配方图的入口配置，经转发通道交给图自己解析。
	opts.forwarded = append(opts.forwarded, graph.WithCheckpointTimeout(store, conversationID, b.checkpointTimeout))
	durability := b.checkpointDurability
	if opts.durability != "" {
		durability = opts.durability
	}
	if durability != "" {
		opts.forwarded = append(opts.forwarded, graph.WithDurability(durability))
	}
	// checkpoint 由 Store.Load 返回的独立副本，已归本次运行所有；
	// 只需要隔离 Values 供运行期修改，不再整份克隆。
	values := callback.CloneMetadata(checkpoint.Values)
	if !opts.resume {
		for key, value := range opts.StateValues {
			values[key] = value
		}
	}
	// incoming 是调用方消息，进入运行边界克隆一次；checkpoint.Messages
	// 已归运行所有，元素按不可变约定共享，只装配新切片。
	input := schema.CloneMessages(incoming)
	assembled := make([]schema.Message, 0, len(checkpoint.Messages)+len(input))
	assembled = append(assembled, checkpoint.Messages...)
	assembled = append(assembled, input...)
	return assembled, &conversationSession{
		checkpoint: checkpoint,
		input:      input,
		values:     values,
	}, nil
}
