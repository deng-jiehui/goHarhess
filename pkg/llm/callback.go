package llm

import (
	"context"

	"github.com/goharness/goharness/pkg/callback"
	"github.com/goharness/goharness/pkg/observe"
)

// Callback observes ChatModel lifecycle boundaries.
type Callback interface {
	callback.Handler
	OnChatModelStart(context.Context, Request) error
	OnChatModelEnd(context.Context, Response) error
	OnChatModelError(context.Context, error) error
}

// CallbackFuncs adapts typed functions to Callback.
type CallbackFuncs struct {
	CallbackName string
	Start        func(context.Context, Request) error
	End          func(context.Context, Response) error
	Error        func(context.Context, error) error
}

func (f CallbackFuncs) Name() string { return f.CallbackName }
func (f CallbackFuncs) OnChatModelStart(ctx context.Context, request Request) error {
	if f.Start == nil {
		return nil
	}
	return f.Start(ctx, request.Clone())
}
func (f CallbackFuncs) OnChatModelEnd(ctx context.Context, response Response) error {
	if f.End == nil {
		return nil
	}
	return f.End(ctx, response.Clone())
}
func (f CallbackFuncs) OnChatModelError(ctx context.Context, err error) error {
	if f.Error == nil {
		return nil
	}
	return f.Error(ctx, err)
}

// BeginCallback starts one ChatModel child operation.
func BeginCallback(ctx context.Context, name string, metadata map[string]any, request Request) (context.Context, callback.Invocation) {
	modelCtx, invocation := callback.Begin(ctx, "chat_model", name, metadata)
	observe.EmitPayload(modelCtx, observe.EventOf(invocation.WithPhase(callback.PhaseStart), observe.KindLLM, nil), request)
	_ = callback.Dispatch[Callback](ctx, invocation.WithPhase(callback.PhaseStart), func(ctx context.Context, handler Callback) error {
		return handler.OnChatModelStart(ctx, request.Clone())
	})
	return modelCtx, invocation
}

// EndCallback completes one ChatModel child operation.
func EndCallback(ctx context.Context, invocation callback.Invocation, response Response) {
	end := invocation.WithPhase(callback.PhaseEnd)
	event := observe.EventOf(end, observe.KindLLM, map[string]any{"llm.finish_reason": response.FinishReason})
	event.Usage = response.Usage
	observe.EmitPayload(ctx, event, response)
	_ = callback.Dispatch[Callback](ctx, end, func(ctx context.Context, handler Callback) error {
		return handler.OnChatModelEnd(ctx, response.Clone())
	})
}

// ErrorCallback fails one ChatModel child operation.
func ErrorCallback(ctx context.Context, invocation callback.Invocation, err error) {
	failed := invocation.WithPhase(callback.PhaseError)
	event := observe.EventOf(failed, observe.KindLLM, nil)
	event.Err = err
	observe.Emit(ctx, event)
	_ = callback.Dispatch[Callback](ctx, failed, func(ctx context.Context, handler Callback) error {
		return handler.OnChatModelError(ctx, err)
	})
}
