package llm

import "context"

// 流式重试守卫：模型一旦产出增量就不允许安全重试。这是 llm 内部机制，
// 不属于通用观测 API——observe 事件流已覆盖面向用户的生命周期观测。
// 增量路径上的信号是无类型回调，不做反射匹配。
type streamProducedContextKey struct{}

// withStreamProduced 注册产出回调：首个模型增量到达时 fn 被调用。
func withStreamProduced(ctx context.Context, fn func()) context.Context {
	existing, _ := ctx.Value(streamProducedContextKey{}).([]func())
	values := append(append(make([]func(), 0, len(existing)+1), existing...), fn)
	return context.WithValue(ctx, streamProducedContextKey{}, values)
}

// notifyStreamProduced 触发全部已注册回调；回调 panic 被隔离。
func notifyStreamProduced(ctx context.Context) {
	if ctx == nil {
		return
	}
	values, _ := ctx.Value(streamProducedContextKey{}).([]func())
	for _, fn := range values {
		if fn != nil {
			func() { defer func() { _ = recover() }(); fn() }()
		}
	}
}
