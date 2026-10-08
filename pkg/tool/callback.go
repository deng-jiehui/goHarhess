package tool

import (
	"context"

	"github.com/goharness/goharness/pkg/callback"
	"github.com/goharness/goharness/pkg/observe"
	"github.com/goharness/goharness/pkg/schema"
)

type Callback interface {
	callback.Handler
	OnToolStart(context.Context, Spec, CallInput) error
	OnToolEnd(context.Context, Spec, CallInput, Result) error
	OnToolError(context.Context, Spec, CallInput, error) error
}

type CallbackFuncs struct {
	CallbackName string
	Start        func(context.Context, Spec, CallInput) error
	End          func(context.Context, Spec, CallInput, Result) error
	Error        func(context.Context, Spec, CallInput, error) error
}

func (f CallbackFuncs) Name() string { return f.CallbackName }
func (f CallbackFuncs) OnToolStart(ctx context.Context, spec Spec, input CallInput) error {
	if f.Start == nil {
		return nil
	}
	return f.Start(ctx, cloneSpecForCallback(spec), cloneCallInput(input))
}
func (f CallbackFuncs) OnToolEnd(ctx context.Context, spec Spec, input CallInput, result Result) error {
	if f.End == nil {
		return nil
	}
	return f.End(ctx, cloneSpecForCallback(spec), cloneCallInput(input), cloneResult(result))
}
func (f CallbackFuncs) OnToolError(ctx context.Context, spec Spec, input CallInput, err error) error {
	if f.Error == nil {
		return nil
	}
	return f.Error(ctx, cloneSpecForCallback(spec), cloneCallInput(input), err)
}

// BeginCallback starts one Tool child operation and notifies typed observers.
func BeginCallback(ctx context.Context, spec Spec, input CallInput) (context.Context, callback.Invocation) {
	metadata := callback.CloneMetadata(input.Metadata)
	metadata["tool.read_only"] = spec.IsReadOnly
	metadata["tool.file_access"] = spec.FileAccess
	toolCtx, invocation := callback.Begin(ctx, "tool", spec.Name, metadata)
	observe.EmitPayload(toolCtx, observe.EventOf(invocation.WithPhase(callback.PhaseStart), observe.KindTool, nil), input)
	_ = callback.Dispatch[Callback](toolCtx, invocation.WithPhase(callback.PhaseStart), func(ctx context.Context, handler Callback) error {
		return handler.OnToolStart(ctx, cloneSpecForCallback(spec), cloneCallInput(input))
	})
	return toolCtx, invocation
}

// EndCallback notifies typed Tool observers of the terminal result.
func EndCallback(ctx context.Context, invocation callback.Invocation, spec Spec, input CallInput, result Result, callErr error) {
	if callErr != nil {
		failed := invocation.WithPhase(callback.PhaseError)
		event := observe.EventOf(failed, observe.KindTool, nil)
		event.Err = callErr
		observe.Emit(ctx, event)
		_ = callback.Dispatch[Callback](ctx, failed, func(ctx context.Context, handler Callback) error {
			return handler.OnToolError(ctx, cloneSpecForCallback(spec), cloneCallInput(input), callErr)
		})
		return
	}
	end := invocation.WithPhase(callback.PhaseEnd)
	observe.EmitPayload(ctx, observe.EventOf(end, observe.KindTool, nil), result)
	_ = callback.Dispatch[Callback](ctx, end, func(ctx context.Context, handler Callback) error {
		return handler.OnToolEnd(ctx, cloneSpecForCallback(spec), cloneCallInput(input), cloneResult(result))
	})
}

func cloneSpecForCallback(spec Spec) Spec {
	spec.Parameters = callback.CloneMetadata(spec.Parameters)
	return spec
}

func cloneCallInput(input CallInput) CallInput {
	input.Args = callback.CloneMetadata(input.Args)
	input.Metadata = callback.CloneMetadata(input.Metadata)
	input.Messages = schema.CloneMessages(input.Messages)
	input.Files = append([]string(nil), input.Files...)
	return input
}

func cloneResult(result Result) Result {
	result.ContentBlocks = cloneResultBlocks(result.ContentBlocks)
	result.Data = callback.CloneValue(result.Data)
	return result
}
