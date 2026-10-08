package llm

import (
	"context"

	"github.com/goharness/goharness/pkg/callback"
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
	_ = callback.Dispatch[Callback](ctx, invocation.WithPhase(callback.PhaseStart), func(ctx context.Context, handler Callback) error {
		return handler.OnChatModelStart(ctx, request.Clone())
	})
	return modelCtx, invocation
}

// EndCallback completes one ChatModel child operation.
func EndCallback(ctx context.Context, invocation callback.Invocation, response Response) {
	_ = callback.Dispatch[Callback](ctx, invocation.WithPhase(callback.PhaseEnd), func(ctx context.Context, handler Callback) error {
		return handler.OnChatModelEnd(ctx, response.Clone())
	})
}

// ErrorCallback fails one ChatModel child operation.
func ErrorCallback(ctx context.Context, invocation callback.Invocation, err error) {
	_ = callback.Dispatch[Callback](ctx, invocation.WithPhase(callback.PhaseError), func(ctx context.Context, handler Callback) error {
		return handler.OnChatModelError(ctx, err)
	})
}
