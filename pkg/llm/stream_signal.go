package llm

import (
	"context"
	"reflect"
)

// 流式重试守卫：模型一旦产出增量就不允许安全重试。这是 llm 内部机制，
// 不属于通用观测 API——observe 事件流已覆盖面向用户的生命周期观测。
type streamSignalsContextKey struct{}

type streamSignal struct {
	typ reflect.Type
	fn  func()
}

// withStreamSignal 注册类型化产出信号：notifyStream[T] 触发时调用 fn。
func withStreamSignal[T any](ctx context.Context, fn func()) context.Context {
	existing, _ := ctx.Value(streamSignalsContextKey{}).([]streamSignal)
	values := append(append([]streamSignal(nil), existing...), streamSignal{reflect.TypeFor[T](), fn})
	return context.WithValue(ctx, streamSignalsContextKey{}, values)
}

// notifyStream 触发 T 类型的全部已注册信号；信号 panic 被隔离。
func notifyStream[T any](ctx context.Context) {
	if ctx == nil {
		return
	}
	values, _ := ctx.Value(streamSignalsContextKey{}).([]streamSignal)
	for _, v := range values {
		if v.typ == reflect.TypeFor[T]() && v.fn != nil {
			func() { defer func() { _ = recover() }(); v.fn() }()
		}
	}
}
