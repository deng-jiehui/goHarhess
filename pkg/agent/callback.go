package agent

import (
	"context"
	"errors"

	"goharness/pkg/callback"
	"goharness/pkg/contextengine"
	"goharness/pkg/control"
	"goharness/pkg/llm"
	"goharness/pkg/runnable"
	"goharness/pkg/schema"
)

// runLifecycle owns only Agent message-stream integrity and conversation
// commit. Agent execution lifecycle is the lifecycle of its compiled Graph.
type runLifecycle struct {
	ctx     context.Context
	session *conversationSession
}

func (b *Base) beginRun(ctx context.Context, _ []schema.Message, opts RunOptions) (context.Context, *runLifecycle, error) {
	if ctx == nil {
		return nil, nil, errors.New("agent run context cannot be nil")
	}
	ctx, _ = callback.EnterScope(ctx)
	ctx = callback.WithMetadata(ctx, opts.Metadata)
	return ctx, &runLifecycle{ctx: ctx}, nil
}

func (*runLifecycle) fail(error) {}

func (l *runLifecycle) wrap(stream runnable.Stream[schema.MessageEvent], bufferSize int) runnable.Stream[schema.MessageEvent] {
	if stream == nil {
		err := errors.New("agent returned a nil stream")
		out := make(chan runnable.Item[schema.MessageEvent], 1)
		out <- runnable.Item[schema.MessageEvent]{Err: err}
		close(out)
		return out
	}
	if bufferSize < 1 {
		bufferSize = 1
	}
	out := make(chan runnable.Item[schema.MessageEvent], bufferSize)
	go func() {
		defer close(out)
		var finalMessages []schema.Message
		sawDone := false
		for item := range stream {
			if item.Err != nil {
				if isPersistedInterruption(item.Err) || interruptCauseRequested(l.ctx) {
					out <- item
				} else {
					emitMessageItem(l.ctx, out, item)
				}
				return
			}
			if item.Value.Kind == schema.MessageEventDone {
				sawDone = true
				continue
			}
			if item.Value.Kind == schema.MessageEventSnapshot {
				finalMessages = schema.CloneMessages(item.Value.Messages)
			}
			callback.EmitStream(l.ctx, item.Value.Clone())
			if !emitMessageItem(l.ctx, out, item) {
				if _, interrupted := control.AsInterrupt(context.Cause(l.ctx)); interrupted {
					continue
				}
				return
			}
		}
		if err := l.ctx.Err(); err != nil {
			return
		}
		if !sawDone {
			emitMessageItem(l.ctx, out, runnable.Item[schema.MessageEvent]{Err: errors.New("agent stream closed without done event")})
			return
		}
		if err := l.session.commit(l.ctx, finalMessages); err != nil {
			emitMessageItem(l.ctx, out, runnable.Item[schema.MessageEvent]{Err: err})
			return
		}
		emitMessageItem(l.ctx, out, runnable.Item[schema.MessageEvent]{Value: schema.MessageEvent{Kind: schema.MessageEventDone}})
	}()
	return out
}

func (b *Base) beginLLMCall(
	ctx context.Context,
	request llm.Request,
	opts RunOptions,
	contextStats contextengine.Stats,
) (context.Context, callback.Invocation) {
	info := llm.DescribeModel(b.model)
	name := info.Name
	if name == "" {
		name = info.ModelType
	}
	capabilities := b.model.Capabilities()
	metadata := map[string]any{
		"llm.provider":                 info.Provider,
		"llm.model_type":               info.ModelType,
		"llm.endpoint":                 info.Endpoint,
		"llm.streaming":                true,
		"llm.delta_stream":             opts.DeltaStream,
		"llm.function_count":           len(request.Options.Functions),
		"llm.capabilities.streaming":   capabilities.Streaming,
		"llm.capabilities.tool_calls":  capabilities.ToolCalls,
		"llm.capabilities.vision":      capabilities.Vision,
		"llm.capabilities.json_schema": capabilities.JSONSchema,
		"llm.capabilities.reasoning":   capabilities.Reasoning,
		"context.budget_tokens":        contextStats.BudgetTokens,
		"context.input_tokens":         contextStats.InputTokens,
		"context.output_tokens":        contextStats.OutputTokens,
		"context.stable_prefix_tokens": contextStats.StablePrefixTokens,
		"context.dropped_turns":        contextStats.DroppedTurns,
		"context.dropped_messages":     contextStats.DroppedMessages,
		"context.truncated_messages":   contextStats.TruncatedMessages,
		"context.role_repairs":         contextStats.RoleRepairs,
		"context.prompt_cache_aligned": contextStats.PromptCacheAligned,
	}
	if len(info.Attributes) > 0 {
		metadata["llm.attributes"] = info.Attributes
	}
	return llm.BeginCallback(ctx, name, metadata, request)
}

func observeLLMStream(
	ctx context.Context,
	stream runnable.Stream[llm.Event],
	invocation callback.Invocation,
	deltas bool,
) runnable.Stream[llm.Event] {
	out := make(chan runnable.Item[llm.Event])
	go func() {
		defer close(out)
		accumulator := llm.NewDeltaAccumulator(deltas)
		response := llm.Response{}
		for {
			select {
			case <-ctx.Done():
				llm.ErrorCallback(ctx, invocation, ctx.Err())
				return
			case item, ok := <-stream:
				if !ok {
					err := errors.New("llm stream closed without done event")
					llm.ErrorCallback(ctx, invocation, err)
					emitLLMEvent(ctx, out, runnable.Item[llm.Event]{Err: err})
					return
				}
				if item.Err != nil {
					llm.ErrorCallback(ctx, invocation, item.Err)
					emitLLMEvent(ctx, out, item)
					return
				}
				event := item.Value
				if event.Usage != nil {
					response.Usage = event.Clone().Usage
				}
				if event.FinishReason != "" {
					response.FinishReason = event.FinishReason
				}
				if event.Kind == llm.EventDelta && len(event.Messages) > 0 {
					messages, err := accumulator.Apply(event)
					if err != nil {
						llm.ErrorCallback(ctx, invocation, err)
						emitLLMEvent(ctx, out, runnable.Item[llm.Event]{Err: err})
						return
					}
					response.Messages = messages
					callback.EmitStream(ctx, response.Clone())
				}
				if event.Kind == llm.EventDone {
					llm.EndCallback(ctx, invocation, response)
					emitLLMEvent(ctx, out, item)
					return
				}
				if !emitLLMEvent(ctx, out, item) {
					llm.ErrorCallback(ctx, invocation, ctx.Err())
					return
				}
			}
		}
	}()
	return out
}

func emitLLMEvent(ctx context.Context, out chan<- runnable.Item[llm.Event], event runnable.Item[llm.Event]) bool {
	select {
	case <-ctx.Done():
		return false
	case out <- event:
		return true
	}
}
