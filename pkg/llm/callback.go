package llm

import (
	"context"
	"sync"

	"github.com/goharness/goharness/pkg/observe"
)

// modelCallback 只持有一次模型调用的观测身份，节点与 Reader 共用同一个
// 收尾入口；它不包装模型，也不参与模型调用或图节点描述。
type modelCallback struct {
	ctx        context.Context
	invocation observe.Invocation
	once       sync.Once
}

func newModelCallback(ctx context.Context, invocation observe.Invocation) *modelCallback {
	if invocation.Component == "" {
		return nil
	}
	return &modelCallback{ctx: ctx, invocation: invocation}
}

func beginModelCallback(ctx context.Context, name string, metadata map[string]any, request Request) (context.Context, *modelCallback) {
	modelCtx, invocation := BeginCallback(ctx, name, metadata, request)
	return modelCtx, newModelCallback(modelCtx, invocation)
}

// finish 将全部终止出口归到已有 callback，重复收尾不会重复累计指标。
func (c *modelCallback) finish(response Response, err error) {
	if c == nil {
		return
	}
	c.once.Do(func() {
		if err != nil {
			ErrorCallback(c.ctx, c.invocation, err)
			return
		}
		EndCallback(c.ctx, c.invocation, response)
	})
}

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
