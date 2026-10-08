package llm

import (
	"context"

	"github.com/goharness/goharness/pkg/observe"
)

// 本文件是 llm 生命周期的观测发射点：Begin/End/Error 各自产生一条
// observe.Event（kind=llm），start/end 的原始 Request/Response 经
// CapturePayload 脱敏后才进入 Payload。

// BeginCallback starts one ChatModel child operation.
func BeginCallback(ctx context.Context, name string, metadata map[string]any, request Request) (context.Context, observe.Invocation) {
	if !observe.Active(ctx) {
		return ctx, observe.Invocation{}
	}
	modelCtx, invocation := observe.Begin(ctx, "chat_model", name, metadata)
	observe.EmitPayload(modelCtx, observe.EventOf(invocation.WithPhase(observe.PhaseStart), observe.KindLLM, nil), request)
	return modelCtx, invocation
}

// EndCallback completes one ChatModel child operation.
func EndCallback(ctx context.Context, invocation observe.Invocation, response Response) {
	if invocation.Component == "" {
		return
	}
	end := invocation.WithPhase(observe.PhaseEnd)
	event := observe.EventOf(end, observe.KindLLM, map[string]any{"llm.finish_reason": response.FinishReason})
	event.Usage = response.Usage
	observe.EmitPayload(ctx, event, response)
}

// ErrorCallback fails one ChatModel child operation.
func ErrorCallback(ctx context.Context, invocation observe.Invocation, err error) {
	if invocation.Component == "" {
		return
	}
	failed := invocation.WithPhase(observe.PhaseError)
	event := observe.EventOf(failed, observe.KindLLM, nil)
	event.Err = err
	observe.Emit(ctx, event)
}
