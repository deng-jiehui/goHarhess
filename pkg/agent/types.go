package agent

import (
	"context"

	"github.com/goharness/goharness/pkg/runnable"
	"github.com/goharness/goharness/pkg/schema"
)

// Agent 是公开 Agent 契约：一次调用与一次消息流。Collect/Transform 由
// runnable 适配器按需提供；具体实现仍实现 runnable.Named，需要诊断命名时
// 通过类型断言获取。
type Agent interface {
	Invoke(context.Context, []schema.Message, ...runnable.Option) ([]schema.Message, error)
	Stream(context.Context, []schema.Message, ...runnable.Option) (*runnable.StreamReader[[]schema.Message], error)
}

// agentName/agentDescription 尽力返回 Agent 的诊断命名与描述：内置实现都
// 带 runnable.Named，自定义最小实现可能只有 Invoke/Stream，断言失败给空串。
func agentName(value Agent) string {
	if n, ok := value.(runnable.Named); ok {
		return n.Name()
	}
	return ""
}

func agentDescription(value Agent) string {
	if n, ok := value.(runnable.Named); ok {
		return n.Description()
	}
	return ""
}
