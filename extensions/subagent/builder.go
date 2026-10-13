package subagent

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/goharness/goharness/pkg/agent"
	"github.com/goharness/goharness/pkg/control"
	"github.com/goharness/goharness/pkg/runnable"
	"github.com/goharness/goharness/pkg/schema"
	"github.com/goharness/goharness/pkg/tool"
)

// ContextCodec 决定子 Agent 的独立上下文如何参与下一次任务：Input 把此前
// 导出的上下文与新任务合成本次输入，Export 在任务成功完成后产生新的上下文。
// 默认实现保留完整的会话消息；自建图若不以消息历史为上下文，提供自己的编码。
// 父 Agent 的模型输入不自动包含子 Agent 的上下文。
type ContextCodec interface {
	Input(saved json.RawMessage, task string) ([]schema.Message, error)
	Export(input, output []schema.Message) (json.RawMessage, error)
}

type messageCodec struct{}

func (messageCodec) Input(saved json.RawMessage, task string) ([]schema.Message, error) {
	var history []schema.Message
	if len(saved) > 0 {
		if err := json.Unmarshal(saved, &history); err != nil {
			return nil, fmt.Errorf("decode saved sub-agent context: %w", err)
		}
	}
	return append(history, schema.UserMessage(task)), nil
}

func (messageCodec) Export(input, output []schema.Message) (json.RawMessage, error) {
	all := make([]schema.Message, 0, len(input)+len(output))
	all = append(append(all, input...), output...)
	return json.Marshal(all)
}

// savedConfig 是随子 Agent 记录保存的最终配置：只含组件引用与合并后的
// 指令，不含模型实例、工具实现或凭据。
type savedConfig struct {
	Instructions string   `json:"instructions,omitempty"`
	Model        string   `json:"model"`
	Tools        []string `json:"tools"`
	Role         string   `json:"role,omitempty"`
}

func savedConfigOf(spec ResolvedSpec) savedConfig {
	tools := spec.Tools
	if tools == nil {
		tools = []string{}
	}
	return savedConfig{Instructions: spec.Instructions, Model: spec.Model, Tools: tools, Role: spec.Role}
}

// specFromSaved 按记录中保存的最终配置还原解析结果：登记时保存的组件引用
// 必须在当前注册表中仍然存在，否则拒绝重建而不是换实现继续。
func (m *Manager) specFromSaved(saved savedConfig, builderID, builderVersion string) (ResolvedSpec, error) {
	if builderID != m.cfg.BuilderID || builderVersion != m.cfg.BuilderVersion {
		return ResolvedSpec{}, fmt.Errorf("sub-agent was built by %q/%q, current builder is %q/%q: %w",
			builderID, builderVersion, m.cfg.BuilderID, m.cfg.BuilderVersion, ErrAgentCorrupt)
	}
	if _, ok := m.cfg.Models[saved.Model]; !ok {
		return ResolvedSpec{}, fmt.Errorf("saved sub-agent uses model %q which is no longer registered: %w", saved.Model, ErrAgentCorrupt)
	}
	for _, name := range saved.Tools {
		if _, ok := m.tools[name]; !ok {
			return ResolvedSpec{}, fmt.Errorf("saved sub-agent uses tool %q which is no longer registered: %w", name, ErrAgentCorrupt)
		}
	}
	return ResolvedSpec{
		Instructions: saved.Instructions, Model: saved.Model, Tools: saved.Tools, Role: saved.Role,
		BuilderID: builderID, BuilderVersion: builderVersion,
	}, nil
}

// build 构建（或复用）一份已解析配置对应的执行器。构建结果必须明确声明
// 恢复能力：登记记录之前确认，不能等任务挂起后才发现无法重建。
func (m *Manager) build(ctx context.Context, spec ResolvedSpec) (Runner, error) {
	key, err := json.Marshal(savedConfigOf(spec))
	if err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if runner, ok := m.runners[string(key)]; ok {
		return runner, nil
	}
	builder := m.cfg.Builder
	if builder == nil {
		builder = m.defaultBuild
	}
	runner, err := builder(ctx, spec)
	if err != nil {
		return nil, err
	}
	if runner == nil {
		return nil, fmt.Errorf("sub-agent builder %q returned a nil runner", m.cfg.BuilderID)
	}
	declared, ok := runner.(Recoverable)
	if !ok {
		return nil, fmt.Errorf("%w: builder %q returned a runner that does not declare recovery support; return a graph.CompiledGraph, forward Recoverable() from your wrapper, or wrap it with subagent.WithRecovery", ErrNotRecoverable, m.cfg.BuilderID)
	}
	if err := declared.Recoverable(); err != nil {
		return nil, fmt.Errorf("%w: builder %q: %v", ErrNotRecoverable, m.cfg.BuilderID, err)
	}
	m.runners[string(key)] = runner
	return runner, nil
}

// defaultBuild 用 agent.New 构建：有工具时是 ReAct 配方，否则是单模型配方。
func (m *Manager) defaultBuild(_ context.Context, spec ResolvedSpec) (Runner, error) {
	tools := make([]tool.Tool, 0, len(spec.Tools))
	for _, name := range spec.Tools {
		tools = append(tools, m.tools[name])
	}
	description := spec.Role
	if description == "" {
		description = "sub-agent"
	}
	built, err := agent.New(m.cfg.Models[spec.Model],
		agent.WithName("subagent"),
		agent.WithDescription(description),
		agent.WithSystemMessage(spec.Instructions),
		agent.WithTools(tools...),
	)
	if err != nil {
		return nil, err
	}
	return FromAgent(built), nil
}

// freezeInput 计算并序列化登记任务输入：输入必须在重放间逐字节一致，
// 登记后 record.Context 的更新不再影响本次调用的子执行输入。
func (m *Manager) freezeInput(saved json.RawMessage, task string) (json.RawMessage, error) {
	messages, err := m.cfg.Codec.Input(saved, task)
	if err != nil {
		return nil, err
	}
	return json.Marshal(messages)
}

// run 执行一次任务：input 是登记时冻结的任务输入，运行后导出新的上下文
// 并返回最终回复。挂起、取消等控制信号原样穿透给图引擎。返回的新上下文
// 只在任务成功完成时使用。配置 MaxLLMCalls 时按次收紧执行器的模型预算，
// 超限（输出停在未配对的工具调用上）返回模型可修正的错误，不静默截断。
func (m *Manager) run(ctx context.Context, runner Runner, input []schema.Message) (result string, exported json.RawMessage, err error) {
	opts := []runnable.Option(nil)
	if m.cfg.MaxLLMCalls > 0 {
		opts = append(opts, agent.WithRunMaxLLMCalls(m.cfg.MaxLLMCalls))
	}
	output, err := runner.Invoke(ctx, schema.CloneMessages(input), opts...)
	if err != nil {
		return "", nil, err
	}
	if m.cfg.MaxLLMCalls > 0 && endsWithPendingCalls(output) {
		return "", nil, configErrorf("sub-agent task stopped: reached the model-call limit of %d; retry with a smaller task or fewer steps", m.cfg.MaxLLMCalls)
	}
	exported, err = m.cfg.Codec.Export(input, output)
	if err != nil {
		return "", nil, fmt.Errorf("export sub-agent context: %w", err)
	}
	return finalText(output), exported, nil
}

// endsWithPendingCalls 判断输出是否停在未配对的工具调用上：ReAct 配方中
// 模型调用预算耗尽时循环直接收尾，最后一条助手消息仍携带工具调用。
func endsWithPendingCalls(output []schema.Message) bool {
	if len(output) == 0 {
		return false
	}
	last := output[len(output)-1]
	return last.Role == schema.RoleAssistant && len(last.FunctionToolCalls()) > 0
}

// finalText 取最后一条带文本的助手消息作为任务的最终回复。
func finalText(messages []schema.Message) string {
	for i := len(messages) - 1; i >= 0; i-- {
		if messages[i].Role != schema.RoleAssistant {
			continue
		}
		if text := messages[i].JoinedText(""); strings.TrimSpace(text) != "" {
			return text
		}
	}
	return ""
}

// FromAgent 把预置 Agent 适配为子 Agent 执行器：每次调用附带检查点旁路，
// 子进度由父运行的命名空间子记录统一保存，不另开会话。自建 Builder 使用
// 预置配方时用它包装；使用者自建的图直接按 Runner 契约返回即可。
func FromAgent(a agent.Agent) Runner { return bypassRunner{a} }

type bypassRunner struct{ agent.Agent }

// Recoverable 声明预置配方可由父运行检查点恢复：内置配方都是 Graph 配方。
func (bypassRunner) Recoverable() error { return nil }

// Name/Description 满足 Runner 的命名契约：内置 Agent 都实现
// runnable.Named，自定义最小实现没有命名时给空串。
func (b bypassRunner) Name() string {
	if n, ok := b.Agent.(runnable.Named); ok {
		return n.Name()
	}
	return ""
}

func (b bypassRunner) Description() string {
	if n, ok := b.Agent.(runnable.Named); ok {
		return n.Description()
	}
	return ""
}

// Recoverable 是执行器对"可由父运行检查点恢复"的声明：返回 nil 表示可以，
// 返回错误说明原因。graph.CompiledGraph 已实现它；包装图的执行器要么嵌入
// 具体的图类型使方法被提升，要么显式转发，要么用 WithRecovery 声明。
// 构建结果没有声明时，创建任务会在登记前被拒绝。
type Recoverable interface{ Recoverable() error }

// ErrNotRecoverable 表示构建结果没有声明恢复能力或声明不可恢复。它是接入
// 错误而不是模型可修正的配置错误，不会被转换成工具结果。
var ErrNotRecoverable = control.Sentinel("sub-agent runner cannot be resumed from the parent checkpoint")

type recoveryRunner struct {
	Runner
	check func() error
}

func (r recoveryRunner) Recoverable() error {
	if r.check == nil {
		return nil
	}
	return r.check()
}

// WithRecovery 为任意执行器声明恢复能力：check 为 nil 表示无条件可恢复，
// 否则由它判断（例如转发被包装图的 Recoverable）。适用于加了日志、监控等
// 外层包装、无法让方法自动提升的场景。
func WithRecovery(r Runner, check func() error) Runner {
	return recoveryRunner{Runner: r, check: check}
}

func (b bypassRunner) Invoke(ctx context.Context, in []schema.Message, opts ...runnable.Option) ([]schema.Message, error) {
	return b.Agent.Invoke(ctx, in, append([]runnable.Option{agent.WithCheckpointBypass()}, opts...)...)
}

func (b bypassRunner) Stream(ctx context.Context, in []schema.Message, opts ...runnable.Option) (*runnable.StreamReader[[]schema.Message], error) {
	return b.Agent.Stream(ctx, in, append([]runnable.Option{agent.WithCheckpointBypass()}, opts...)...)
}

func (b bypassRunner) Collect(ctx context.Context, in *runnable.StreamReader[[]schema.Message], opts ...runnable.Option) ([]schema.Message, error) {
	return runnable.CollectFromInvoke(ctx, in, b.Invoke, opts...)
}

func (b bypassRunner) Transform(ctx context.Context, in *runnable.StreamReader[[]schema.Message], opts ...runnable.Option) (*runnable.StreamReader[[]schema.Message], error) {
	return runnable.TransformFromInvoke(ctx, in, b.Invoke, opts...)
}
