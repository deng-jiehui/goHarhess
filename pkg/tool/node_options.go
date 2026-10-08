package tool

import (
	"context"
	"fmt"
	"reflect"
	"time"

	"github.com/goharness/goharness/pkg/runnable"
)

// ExecutionLimits tighten one ToolsNode call without changing the immutable
// node configuration. Limits can only narrow the node's own bounds; values
// wider than the node configuration are ignored.
type ExecutionLimits struct {
	MaxConcurrency int
	Timeout        time.Duration
}

// OptionRelay 标记节点把收到的选项继续透传给每次工具调用，由工具自行
// 解析归属类型；定向到本节点的选项不因声明类型受限。
func (*ToolsNode) OptionRelay() bool { return true }

// OptionTypes 声明 ToolsNode 接收 ExecutionLimits 类型的配置选项。
func (*ToolsNode) OptionTypes() []reflect.Type {
	return []reflect.Type{runnable.OptionType[ExecutionLimits]()}
}

// WithMaxConcurrency tightens one ToolsNode invocation's batch parallelism.
func WithMaxConcurrency(max int) runnable.Option {
	return runnable.WrapOption(func(limits *ExecutionLimits) {
		if max < 0 {
			panic("tool node max concurrency cannot be negative")
		}
		limits.MaxConcurrency = max
	})
}

// WithTimeout tightens one ToolsNode invocation's per-call timeout.
func WithTimeout(timeout time.Duration) runnable.Option {
	return runnable.WrapOption(func(limits *ExecutionLimits) {
		if timeout < 0 {
			panic("tool node timeout cannot be negative")
		}
		limits.Timeout = timeout
	})
}

// resolveExecutionLimits 合并节点选项与父调用注入的 ctx 限制；
// 二者都只能收紧节点自身上限。
func (n *ToolsNode) resolveExecutionLimits(ctx context.Context, options []runnable.Option) (ExecutionLimits, []runnable.Option, error) {
	limits, remaining, err := runnable.ResolveComponentOptions(ExecutionLimits{}, options...)
	if err != nil {
		return ExecutionLimits{}, nil, fmt.Errorf("resolve tools node options: %w", err)
	}
	contextLimits := executionLimitsFromContext(ctx)
	if contextLimits.MaxConcurrency > 0 && (limits.MaxConcurrency == 0 || contextLimits.MaxConcurrency < limits.MaxConcurrency) {
		limits.MaxConcurrency = contextLimits.MaxConcurrency
	}
	if contextLimits.Timeout > 0 && (limits.Timeout == 0 || contextLimits.Timeout < limits.Timeout) {
		limits.Timeout = contextLimits.Timeout
	}
	return limits, remaining, nil
}

type executionLimitsContextKey struct{}

// WithExecutionLimits binds per-run limits for a ToolsNode reached through a
// compiled Graph, where invocation options cannot target one node by ID.
func WithExecutionLimits(ctx context.Context, limits ExecutionLimits) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, executionLimitsContextKey{}, limits)
}

func executionLimitsFromContext(ctx context.Context) ExecutionLimits {
	if ctx == nil {
		return ExecutionLimits{}
	}
	limits, _ := ctx.Value(executionLimitsContextKey{}).(ExecutionLimits)
	return limits
}
