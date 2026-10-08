package tool

import (
	"context"

	"github.com/goharness/goharness/pkg/observe"
	"github.com/goharness/goharness/pkg/schema"
)

// 本文件是 tool 生命周期的观测发射点：Begin/End 各产生一条
// observe.Event（kind=tool），CallInput/Result 经 CapturePayload 脱敏后
// 才进入 Payload。

// BeginCallback starts one Tool child operation and notifies observers.
func BeginCallback(ctx context.Context, spec Spec, input CallInput) (context.Context, observe.Invocation) {
	metadata := observe.CloneMetadata(input.Metadata)
	metadata["tool.read_only"] = spec.IsReadOnly
	metadata["tool.file_access"] = spec.FileAccess
	toolCtx, invocation := observe.Begin(ctx, "tool", spec.Name, metadata)
	observe.EmitPayload(toolCtx, observe.EventOf(invocation.WithPhase(observe.PhaseStart), observe.KindTool, nil), input)
	return toolCtx, invocation
}

// EndCallback notifies observers of the terminal result.
func EndCallback(ctx context.Context, invocation observe.Invocation, spec Spec, input CallInput, result Result, callErr error) {
	if callErr != nil {
		failed := invocation.WithPhase(observe.PhaseError)
		event := observe.EventOf(failed, observe.KindTool, nil)
		event.Err = callErr
		observe.Emit(ctx, event)
		return
	}
	end := invocation.WithPhase(observe.PhaseEnd)
	observe.EmitPayload(ctx, observe.EventOf(end, observe.KindTool, nil), result)
}

// cloneCallInput 制备执行前输入快照：放弃告警在后台路径上引用它，
// 工具/中间件超时后仍可能修改原始 Args 与 Metadata。
func cloneCallInput(input CallInput) CallInput {
	input.Args = observe.CloneMetadata(input.Args)
	input.Metadata = observe.CloneMetadata(input.Metadata)
	input.Messages = schema.CloneMessages(input.Messages)
	input.Files = append([]string(nil), input.Files...)
	return input
}
