package callback

import (
	"context"

	"github.com/goharness/goharness/pkg/observe"
)

// Trace、Invocation、Begin 与元数据机制已迁入 pkg/observe；本文件保留
// 转发薄壳直到各领域 Callback 通道删除。
type Scope = observe.Scope

func EnterScope(ctx context.Context) (context.Context, Scope) { return observe.EnterScope(ctx) }

func ScopeFromContext(ctx context.Context) (Scope, bool) { return observe.ScopeFromContext(ctx) }

func WithTraceID(ctx context.Context, traceID string) context.Context {
	return observe.WithTraceID(ctx, traceID)
}

func TraceFromContext(ctx context.Context) (Trace, bool) { return observe.TraceFromContext(ctx) }

func WithMetadata(ctx context.Context, metadata map[string]any) context.Context {
	return observe.WithMetadata(ctx, metadata)
}

// WithoutMetadata 删除指定的元数据键，返回新的派生上下文；删除不存在的键
// 为无操作。父调用与其他分支的上下文不受影响。
func WithoutMetadata(ctx context.Context, keys ...string) context.Context {
	return observe.WithoutMetadata(ctx, keys...)
}

func MetadataFromContext(ctx context.Context) map[string]any {
	return observe.MetadataFromContext(ctx)
}

func Begin(ctx context.Context, component, name string, metadata map[string]any) (context.Context, Invocation) {
	return observe.Begin(ctx, component, name, metadata)
}

func InvocationFromContext(ctx context.Context) (Invocation, bool) {
	return observe.InvocationFromContext(ctx)
}

func CloneMetadata(metadata map[string]any) map[string]any { return observe.CloneMetadata(metadata) }

func CloneValue(value any) any { return observe.CloneValue(value) }

func BindInvocation(ctx context.Context, invocation Invocation) context.Context {
	return observe.BindInvocation(ctx, invocation)
}

// withInvocation overlays an operation snapshot for a single dispatch call.
func withInvocation(ctx context.Context, invocation Invocation) context.Context {
	return observe.BindInvocation(ctx, invocation)
}

type managerContextKey struct{}

// WithManager exposes one synchronous Manager to nested framework components.
func WithManager(ctx context.Context, manager *Manager) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, managerContextKey{}, manager)
}

// ManagerFromContext resolves the callback Manager for the current run.
func ManagerFromContext(ctx context.Context) (*Manager, bool) {
	if ctx == nil {
		return nil, false
	}
	manager, ok := ctx.Value(managerContextKey{}).(*Manager)
	return manager, ok && manager != nil
}
