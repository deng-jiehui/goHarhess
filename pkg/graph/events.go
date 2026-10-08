package graph

import (
	"context"

	"github.com/goharness/goharness/pkg/stream"
)

// lifecycleEvent/lifecycleEventType 定义在 types.go。控制面事件只流向
// 本次运行的生命周期汇聚器（lifecycleSink），由它翻译成统一观测事件；
// 外部观测一律走 pkg/observe。

// lifecycleSinkKey 承载本次运行的生命周期汇聚器。嵌套图在 bindCallbacks
// 挂自己的实例覆盖父级值，天然实现父子隔离——子图事件只进自己的汇聚器。
type lifecycleSinkKey struct{}

func lifecycleFromContext(ctx context.Context) *lifecycleSink {
	if ctx == nil {
		return nil
	}
	sink, _ := ctx.Value(lifecycleSinkKey{}).(*lifecycleSink)
	return sink
}

func enterGraphExecution(ctx context.Context, name string) (context.Context, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	ctx = stream.WithNamespace(ctx, name)
	ctx = enableStreamLifecycle(ctx)
	return ctx, nil
}

func publish(ctx context.Context, event lifecycleEvent) {
	sink := lifecycleFromContext(ctx)
	if sink == nil {
		return
	}
	// 观测不能破坏图执行。
	defer func() { _ = recover() }()
	sink.Publish(ctx, event)
}

func bindEventContext(ctx context.Context, event lifecycleEvent) (bound context.Context) {
	if ctx == nil {
		ctx = context.Background()
	}
	ctx = stream.WithNode(ctx, event.NodeID)
	bound = ctx
	sink := lifecycleFromContext(ctx)
	if sink == nil {
		return bound
	}
	defer func() {
		if recover() != nil || bound == nil {
			bound = ctx
		}
	}()
	bound = sink.BindEventContext(ctx, event)
	return bound
}
