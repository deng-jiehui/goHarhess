package agent

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	"github.com/goharness/goharness/pkg/control"
	"github.com/goharness/goharness/pkg/graph"
	"github.com/goharness/goharness/pkg/runnable"
	"github.com/goharness/goharness/pkg/schema"
	"github.com/goharness/goharness/pkg/state"
)

// AgentNode mounts an Agent as a public Graph component. Child business
// failures are returned as data so the parent recipe can choose fail-fast or
// observation semantics in its PostHandler; control failures (interrupts,
// unsupported interrupts, missing checkpoints and parent cancellation) are
// returned as errors so the parent Graph fails or suspends instead.
type AgentNode struct {
	agent Agent
}

func NewAgentNode(value Agent) (*AgentNode, error) {
	if isNilAgent(value) {
		return nil, errors.New("agent node requires an agent")
	}
	return &AgentNode{agent: value}, nil
}

func isNilAgent(value Agent) bool {
	if value == nil {
		return true
	}
	rv := reflect.ValueOf(value)
	switch rv.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return rv.IsNil()
	default:
		return false
	}
}

func (n *AgentNode) Name() string {
	if n == nil || isNilAgent(n.agent) {
		return ""
	}
	return n.agent.Name()
}

func (n *AgentNode) Description() string {
	if n == nil || isNilAgent(n.agent) {
		return ""
	}
	return n.agent.Description()
}

func (*AgentNode) NodeKind() runnable.NodeKind { return runnable.NodeKindAgent }

// OptionRelay 标记 AgentNode 内部还有可继续消费路径段的组件：
// 定向到该节点的剩余路径原样传入，由子 Agent 的配方图入口继续分发。
// AgentNode 不声明任何配置类型：父级按类型广播的组件选项（如 llm.Options）
// 只到达父 Agent 自己拥有的组件，不会自动进入子 Agent；要配置子 Agent
// 内部组件必须显式使用 ForNode/ForPath 定向。
func (*AgentNode) OptionRelay() bool { return true }

func (n *AgentNode) Invoke(ctx context.Context, messages []schema.Message, invocationOptions ...runnable.Option) (runnable.Outcome[[]schema.Message], error) {
	if n == nil || isNilAgent(n.agent) {
		return runnable.Outcome[[]schema.Message]{}, errors.New("agent node is not configured")
	}
	if ctx == nil {
		return runnable.Outcome[[]schema.Message]{}, errors.New("agent node context cannot be nil")
	}
	runOptions, _ := resolvedRunOptionsFromContext(ctx)
	// 子 Agent 总是跳过自己的检查点读写；DeltaStream 继承父级运行设置。
	// 分发到本节点的组件选项（广播或定向）原样传给子 Agent 入口。
	options := []runnable.Option{WithCheckpointBypass()}
	if runOptions.DeltaStream {
		options = append(options, WithDeltaStream(true))
	}
	options = append(options, invocationOptions...)
	reader, err := n.agent.Stream(ctx, schema.CloneMessages(messages), options...)
	if err != nil {
		return n.outcome(ctx, nil, err)
	}
	defer reader.Close()
	output, err := reader.Collect(ctx)
	return n.outcome(ctx, output, err)
}

// outcome 区分子 Agent 的业务失败与控制类失败：前者作为数据交给父级配方，
// 后者必须穿透，避免挂起请求被转成模型可见的观察结果而让父级"成功"结束。
func (n *AgentNode) outcome(ctx context.Context, output []schema.Message, err error) (runnable.Outcome[[]schema.Message], error) {
	if err != nil && isControlError(ctx, err) {
		return runnable.Outcome[[]schema.Message]{}, fmt.Errorf("sub-agent %q: %w", n.agent.Name(), err)
	}
	return runnable.Outcome[[]schema.Message]{Value: schema.CloneMessages(output), Err: err}, nil
}

func isControlError(ctx context.Context, err error) bool {
	if ctx.Err() != nil {
		return true
	}
	if _, interrupted := control.AsInterrupt(err); interrupted {
		return true
	}
	if _, suspended := control.AsSuspension(err); suspended {
		return true
	}
	// 子结果/子进度的保存失败与恢复校验失败都属于框架错误而非业务失败：
	// 不能作为子 Agent 的可恢复失败交给主 Agent 决策——否则 Supervisor
	// 会把"无法安全恢复"当成普通观察，以成功结束并掩盖事实。
	var checkpoint *graph.CheckpointError
	if errors.As(err, &checkpoint) {
		return true
	}
	return errors.Is(err, graph.ErrInterrupted) ||
		errors.Is(err, graph.ErrInterruptUnsupported) ||
		errors.Is(err, graph.ErrInterruptMismatch) ||
		errors.Is(err, graph.ErrResumeIncompatible) ||
		errors.Is(err, graph.ErrGraphRevisionMismatch) ||
		errors.Is(err, graph.ErrGraphNotResumable) ||
		errors.Is(err, graph.ErrRunAbandoned) ||
		errors.Is(err, state.ErrInvalidCheckpoint) ||
		errors.Is(err, graph.ErrCheckpointRequired) ||
		errors.Is(err, graph.ErrCheckpointNotFound)
}

func (n *AgentNode) Stream(ctx context.Context, input []schema.Message, options ...runnable.Option) (*runnable.StreamReader[runnable.Outcome[[]schema.Message]], error) {
	return runnable.StreamFromInvoke(ctx, input, n.Invoke, options...)
}

func (n *AgentNode) Collect(ctx context.Context, input *runnable.StreamReader[[]schema.Message], options ...runnable.Option) (runnable.Outcome[[]schema.Message], error) {
	return runnable.CollectFromInvoke(ctx, input, n.Invoke, options...)
}

func (n *AgentNode) Transform(ctx context.Context, input *runnable.StreamReader[[]schema.Message], options ...runnable.Option) (*runnable.StreamReader[runnable.Outcome[[]schema.Message]], error) {
	return runnable.TransformFromInvoke(ctx, input, n.Invoke, options...)
}

var _ runnable.NamedRunnable[[]schema.Message, runnable.Outcome[[]schema.Message]] = (*AgentNode)(nil)
var _ runnable.NodeDescriptor = (*AgentNode)(nil)
