package agent

import (
	"context"
	"errors"
	"sync/atomic"

	"goharness/pkg/callback"
	"goharness/pkg/contextengine"
	"goharness/pkg/llm"
	"goharness/pkg/runnable"
	"goharness/pkg/schema"
)

type runLifecycle struct {
	base       *Base
	ctx        context.Context
	dispatcher *callback.SyncDispatcher
	operation  callback.Operation
	terminal   atomic.Bool
	metrics    callback.Metrics
	session    *conversationSession
	opts       RunOptions
	initial    []schema.Message
}

func (b *Base) beginRun(ctx context.Context, messages []schema.Message, opts RunOptions) (context.Context, *runLifecycle, error) {
	if ctx == nil {
		return nil, nil, errors.New("agent run context cannot be nil")
	}
	ctx, scope := callback.EnterScope(ctx)
	dispatcher, exists := callback.DispatcherFromContext(ctx)
	if !exists {
		callbacks := make([]callback.Handler, 0, len(b.callbacks)+len(opts.Callbacks))
		callbacks = append(callbacks, b.callbacks...)
		callbacks = append(callbacks, opts.Callbacks...)
		var err error
		dispatcher, err = callback.New(callbacks, b.callbackConfig)
		if err != nil {
			return nil, nil, err
		}
		ctx = callback.WithDispatcher(ctx, dispatcher)
	} else if len(b.callbacks) > 0 || len(opts.Callbacks) > 0 {
		callbacks := make([]callback.Handler, 0, len(b.callbacks)+len(opts.Callbacks))
		callbacks = append(callbacks, b.callbacks...)
		callbacks = append(callbacks, opts.Callbacks...)
		var err error
		dispatcher, err = callback.NewChild(dispatcher, callbacks)
		if err != nil {
			return nil, nil, err
		}
		ctx = callback.WithDispatcher(ctx, dispatcher)
	}
	ctx = callback.WithMetadata(ctx, opts.Metadata)
	metadata := map[string]any{
		"agent.description":        b.description,
		"agent.language":           opts.Lang,
		"agent.delta_stream":       opts.DeltaStream,
		"agent.max_llm_calls":      opts.MaxLLMCalls,
		"agent.max_parallel_tools": opts.MaxParallelTools,
		"agent.tool_timeout_ms":    opts.ToolCallTimeout.Milliseconds(),
		"agent.stream_buffer_size": opts.StreamBufferSize,
		"callback.scope_depth":     scope.Depth,
		"callback.scope_root":      scope.Root,
	}
	ctx, operation := callback.Begin(ctx, callback.ComponentAgent, b.name, metadata)
	agentCtx := ctx
	ctx = callback.BindGraph(ctx, recipeName(b, string(opts.agentKind))+".graph")
	lifecycle := &runLifecycle{
		base:       b,
		ctx:        agentCtx,
		dispatcher: dispatcher,
		operation:  operation,
		opts:       opts,
		initial:    schema.CloneMessages(messages),
		metrics: callback.Metrics{
			InputMessages: len(messages),
			InputBytes:    messageBytes(messages),
		},
	}
	_ = dispatcher.Dispatch(agentCtx, callback.StartEvent(callback.EventAgentStart, operation, lifecycle.metrics))
	return ctx, lifecycle, nil
}

func (l *runLifecycle) fail(err error) {
	if l == nil || !l.terminal.CompareAndSwap(false, true) {
		return
	}
	_ = l.dispatcher.Dispatch(l.ctx, callback.ErrorEvent(callback.EventAgentError, l.operation, err, l.metrics))
}

func (l *runLifecycle) finish() {
	if l == nil || !l.terminal.CompareAndSwap(false, true) {
		return
	}
	_ = l.dispatcher.Dispatch(l.ctx, callback.EndEvent(callback.EventAgentEnd, l.operation, l.metrics))
}

func (l *runLifecycle) wrap(stream runnable.Stream[schema.MessageEvent], bufferSize int) runnable.Stream[schema.MessageEvent] {
	if stream == nil {
		err := errors.New("agent returned a nil stream")
		l.fail(err)
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
		chunkIndex := 0
		var finalMessages []schema.Message
		sawDone := false
		for item := range stream {
			if item.Err != nil {
				l.fail(item.Err)
				emitMessageItem(l.ctx, out, item)
				return
			}
			// The lifecycle owns the terminal event. Upstream completion only
			// starts the breakpoint/checkpoint commit phase below.
			if item.Value.Kind == schema.MessageEventDone {
				sawDone = true
				continue
			}
			chunkIndex++
			chunkMetrics := callback.Metrics{
				Chunks:         1,
				OutputMessages: len(item.Value.Messages),
				OutputBytes:    messageBytes(item.Value.Messages),
			}
			l.metrics.Chunks++
			l.metrics.OutputMessages += len(item.Value.Messages)
			l.metrics.OutputBytes += chunkMetrics.OutputBytes
			if item.Value.Kind == schema.MessageEventSnapshot && len(item.Value.Messages) > 0 {
				finalMessages = schema.CloneMessages(item.Value.Messages)
			}
			event := callback.ProgressEvent(callback.EventAgentChunk, l.operation, chunkMetrics)
			event.Metadata["agent.chunk_index"] = chunkIndex
			_ = l.dispatcher.Dispatch(l.ctx, event)
			if !emitMessageItem(l.ctx, out, item) {
				l.fail(l.ctx.Err())
				return
			}
		}
		if err := l.ctx.Err(); err != nil {
			l.fail(err)
			return
		}
		if !sawDone {
			err := errors.New("agent stream closed without done event")
			l.fail(err)
			emitMessageItem(l.ctx, out, runnable.Item[schema.MessageEvent]{Err: err})
			return
		}
		working := appendMessages(l.initial, finalMessages)
		if err := l.base.checkBreakpoint(
			l.ctx, l.session, l.opts, l.opts.agentKind, BreakpointBeforeCommit,
			working, finalMessages, 0, nil, nil,
		); err != nil {
			l.fail(err)
			emitMessageItem(l.ctx, out, runnable.Item[schema.MessageEvent]{Err: err})
			return
		}
		if err := l.session.commit(l.ctx, finalMessages); err != nil {
			l.fail(err)
			emitMessageItem(l.ctx, out, runnable.Item[schema.MessageEvent]{Err: err})
			return
		}
		l.finish()
		emitMessageItem(l.ctx, out, runnable.Item[schema.MessageEvent]{Value: schema.MessageEvent{Kind: schema.MessageEventDone}})
	}()
	return out
}

func (b *Base) beginLLMCall(
	ctx context.Context,
	messages []schema.Message,
	functions []schema.FunctionDefinition,
	opts RunOptions,
	contextStats contextengine.Stats,
) (context.Context, callback.Operation, *callback.SyncDispatcher, callback.Metrics) {
	dispatcher, _ := callback.DispatcherFromContext(ctx)
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
		"llm.function_count":           len(functions),
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
	llmCtx, operation := callback.Begin(ctx, callback.ComponentLLM, name, metadata)
	metrics := callback.Metrics{
		InputMessages: len(messages),
		InputBytes:    messageBytes(messages),
		ToolCalls:     len(functions),
	}
	if dispatcher != nil {
		_ = dispatcher.Dispatch(llmCtx, callback.StartEvent(callback.EventLLMStart, operation, metrics))
	}
	return llmCtx, operation, dispatcher, metrics
}

func observeLLMStream(
	ctx context.Context,
	stream runnable.Stream[llm.Event],
	dispatcher *callback.SyncDispatcher,
	operation callback.Operation,
	metrics callback.Metrics,
) runnable.Stream[llm.Event] {
	out := make(chan runnable.Item[llm.Event])
	go func() {
		defer close(out)
		chunkIndex := 0
		for {
			select {
			case <-ctx.Done():
				if dispatcher != nil {
					_ = dispatcher.Dispatch(ctx, callback.ErrorEvent(callback.EventLLMError, operation, ctx.Err(), metrics))
				}
				return
			case item, ok := <-stream:
				if !ok {
					if dispatcher != nil {
						_ = dispatcher.Dispatch(ctx, callback.EndEvent(callback.EventLLMEnd, operation, metrics))
					}
					return
				}
				if item.Err != nil {
					if dispatcher != nil {
						_ = dispatcher.Dispatch(ctx, callback.ErrorEvent(callback.EventLLMError, operation, item.Err, metrics))
					}
					emitLLMEvent(ctx, out, item)
					return
				}
				eventValue := item.Value
				if eventValue.Kind == llm.EventDone {
					if dispatcher != nil {
						_ = dispatcher.Dispatch(ctx, callback.EndEvent(callback.EventLLMEnd, operation, metrics))
					}
					emitLLMEvent(ctx, out, item)
					return
				}
				if eventValue.Kind == llm.EventUsage {
					applyUsageMetrics(&metrics, eventValue.Usage)
					if !emitLLMEvent(ctx, out, item) {
						if dispatcher != nil {
							_ = dispatcher.Dispatch(ctx, callback.ErrorEvent(callback.EventLLMError, operation, ctx.Err(), metrics))
						}
						return
					}
					continue
				}
				chunkIndex++
				chunkMetrics := callback.Metrics{
					Chunks:         1,
					OutputMessages: len(eventValue.Messages),
					OutputBytes:    messageBytes(eventValue.Messages),
				}
				metrics.Chunks++
				metrics.OutputMessages += len(eventValue.Messages)
				metrics.OutputBytes += chunkMetrics.OutputBytes
				if eventValue.Usage != nil {
					applyUsageMetrics(&metrics, eventValue.Usage)
					chunkMetrics.PromptTokens = eventValue.Usage.PromptTokens
					chunkMetrics.CompletionTokens = eventValue.Usage.CompletionTokens
					chunkMetrics.TotalTokens = eventValue.Usage.TotalTokens
					chunkMetrics.ReasoningTokens = eventValue.Usage.ReasoningTokens
				}
				if dispatcher != nil {
					event := callback.ProgressEvent(callback.EventLLMChunk, operation, chunkMetrics)
					event.Metadata["llm.chunk_index"] = chunkIndex
					_ = dispatcher.Dispatch(ctx, event)
				}
				if !emitLLMEvent(ctx, out, item) {
					if dispatcher != nil {
						_ = dispatcher.Dispatch(ctx, callback.ErrorEvent(callback.EventLLMError, operation, ctx.Err(), metrics))
					}
					return
				}
			}
		}
	}()
	return out
}

func applyUsageMetrics(metrics *callback.Metrics, usage *llm.Usage) {
	if metrics == nil || usage == nil {
		return
	}
	metrics.PromptTokens = usage.PromptTokens
	metrics.CompletionTokens = usage.CompletionTokens
	metrics.TotalTokens = usage.TotalTokens
	metrics.ReasoningTokens = usage.ReasoningTokens
}

func emitLLMEvent(ctx context.Context, out chan<- runnable.Item[llm.Event], event runnable.Item[llm.Event]) bool {
	select {
	case <-ctx.Done():
		return false
	case out <- event:
		return true
	}
}

func messageBytes(messages []schema.Message) int64 {
	var total int64
	for _, message := range messages {
		total += int64(len(message.Text()))
		total += int64(len(message.ReasoningContent.String()))
		if message.FunctionCall != nil {
			total += int64(len(message.FunctionCall.Name) + len(message.FunctionCall.Arguments))
		}
	}
	return total
}
