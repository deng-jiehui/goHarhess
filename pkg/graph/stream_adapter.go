package graph

import (
	"context"

	"github.com/goharness/goharness/pkg/observe"
	"github.com/goharness/goharness/pkg/stream"
)

// streamLifecycleAdapter 把 run/node 观测事件适配为公共流 Lifecycle，
// 替代此前 publish() 内联的翻译与计时簿记：耗时直接取事件的 Duration。
// 经 observe.With 注册一次即在整棵运行 ctx 上生效；嵌套图事件携带
// 各自的 namespace 到达同一适配器，语义与旧路径一致。
type streamLifecycleAdapter struct{}

type streamAdapterKey struct{}

func (streamLifecycleAdapter) OnEvent(ctx context.Context, event observe.Event) {
	var scope stream.Scope
	switch event.Kind {
	case observe.KindRun:
		scope = stream.ScopeGraph
	case observe.KindNode:
		scope = stream.ScopeNode
	default:
		return
	}
	var phase stream.Phase
	switch event.Phase {
	case observe.PhaseStart:
		phase = stream.PhaseStart
	case observe.PhaseEnd:
		// 挂起收尾的 end 事件不算闭合：观测侧 span 已带 suspended 收尾，
		// 公共流的挂起通告由 finish() 按未闭合生命周期统一产出
		// PhaseInterrupted（附带完整待答记录），这里跳过避免误报正常结束。
		if event.Attrs["graph.outcome"] == "suspended" {
			return
		}
		phase = stream.PhaseEnd
	case observe.PhaseError:
		phase = stream.PhaseError
	default:
		return
	}
	value := stream.Lifecycle{Scope: scope, Phase: phase, Name: event.Name, Duration: event.Duration}
	if step, ok := event.Attrs["graph.step"].(int); ok {
		value.Step = step
	}
	if event.Err != nil {
		value.Error = event.Err.Error()
	}
	if scope == stream.ScopeGraph {
		// 图级事件可能经由某个节点 ctx 上抛，清掉节点归属避免误标。
		ctx = stream.WithNode(ctx, "")
	}
	_ = stream.PublishLifecycle(ctx, value)
}

// enableStreamLifecycle 为本次运行注册一次适配器；嵌套图复用根注册的
// 实例，不叠加第二份（否则子图事件会被两次翻译进公共流）。
func enableStreamLifecycle(ctx context.Context) context.Context {
	if !stream.Enabled(ctx) {
		return ctx
	}
	if registered, _ := ctx.Value(streamAdapterKey{}).(bool); registered {
		return ctx
	}
	ctx = observe.With(ctx, streamLifecycleAdapter{})
	return context.WithValue(ctx, streamAdapterKey{}, true)
}
