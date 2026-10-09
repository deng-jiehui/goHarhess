package subagent

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"github.com/goharness/goharness/pkg/agent"
	"github.com/goharness/goharness/pkg/control"
	"github.com/goharness/goharness/pkg/graph"
	"github.com/goharness/goharness/pkg/llm"
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

// taskInput 是登记进受管任务的冻结输入。
type taskInput struct {
	Task string `json:"task"`
}

// savedConfig 是随子 Agent 记录保存的最终配置：只含组件引用与合并后的
// 指令，不含模型实例、工具实现或凭据。
type savedConfig struct {
	Instructions string   `json:"instructions,omitempty"`
	Model        string   `json:"model"`
	Tools        []string `json:"tools"`
	Role         string   `json:"role,omitempty"`
}

// Manager 把使用者的注册范围、构建函数和模型工具接到图引擎上。它提供：
// Tools（模型可调用的管理工具）与 RunOptions（运行时必须随每次 Invoke 与
// Resume 传入的构建函数绑定——恢复受管任务之前就要完成，不能依赖再次
// 执行 spawn_agent 的工具处理函数才注册）。
type Manager struct {
	cfg   Config
	tools map[string]tool.Tool

	mu      sync.Mutex
	runners map[string]Runner
}

// New 校验配置并创建管理器。
func New(cfg Config) (*Manager, error) {
	normalized, err := cfg.normalized()
	if err != nil {
		return nil, err
	}
	m := &Manager{cfg: normalized, tools: make(map[string]tool.Tool, len(normalized.Tools)), runners: map[string]Runner{}}
	for _, t := range normalized.Tools {
		m.tools[t.Spec().Name] = t
	}
	if m.cfg.Codec == nil {
		m.cfg.Codec = messageCodec{}
	}
	return m, nil
}

// RunOptions 返回需要随根运行传入的选项：登记构建函数及其版本。开启持久化的
// 会话在 Invoke 与 Resume 时都要传入，缺失或版本不符会让恢复被明确拒绝。
func (m *Manager) RunOptions() []runnable.Option {
	return []runnable.Option{graph.WithManagedBuilder(m.cfg.BuilderID, m.cfg.BuilderVersion, m.managedBuilder)}
}

// Resolve 把模型的创建请求按注册范围解析为最终配置；错误是模型可修正的
// ConfigError。
func (m *Manager) Resolve(req Request) (ResolvedSpec, error) { return m.cfg.resolve(req) }

func (m *Manager) specFrom(spec graph.ManagedSpec) (ResolvedSpec, error) {
	var saved savedConfig
	if err := json.Unmarshal(spec.Config, &saved); err != nil {
		return ResolvedSpec{}, fmt.Errorf("%w: decode saved sub-agent config: %v", graph.ErrResumeIncompatible, err)
	}
	if _, ok := m.cfg.Models[saved.Model]; !ok {
		return ResolvedSpec{}, fmt.Errorf("%w: saved sub-agent uses model %q which is no longer registered", graph.ErrResumeIncompatible, saved.Model)
	}
	for _, name := range saved.Tools {
		if _, ok := m.tools[name]; !ok {
			return ResolvedSpec{}, fmt.Errorf("%w: saved sub-agent uses tool %q which is no longer registered", graph.ErrResumeIncompatible, name)
		}
	}
	return ResolvedSpec{
		Instructions: saved.Instructions, Model: saved.Model, Tools: saved.Tools, Role: saved.Role,
		BuilderID: spec.Builder, BuilderVersion: spec.Version,
	}, nil
}

// build 构建（或复用）一份已解析配置对应的执行器。
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
	// 构建结果必须明确声明恢复能力：在登记任务之前确认，不能等任务挂起后才
	// 发现无法重建。缺少声明按接入错误拒绝，不把"能力未知"当作通过。
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

func savedConfigOf(spec ResolvedSpec) savedConfig {
	tools := spec.Tools
	if tools == nil {
		tools = []string{}
	}
	return savedConfig{Instructions: spec.Instructions, Model: spec.Model, Tools: tools, Role: spec.Role}
}

// defaultBuild 用 agent.New 构建：有工具时是 ReAct 配方，否则是单模型配方。
// 注册的模型外包一层收件箱投递，使 send_message 的消息在下一次模型调用时并入。
func (m *Manager) defaultBuild(_ context.Context, spec ResolvedSpec) (Runner, error) {
	tools := make([]tool.Tool, 0, len(spec.Tools))
	for _, name := range spec.Tools {
		tools = append(tools, m.tools[name])
	}
	description := spec.Role
	if description == "" {
		description = "sub-agent"
	}
	built, err := agent.New(inboxModel{m.cfg.Models[spec.Model]},
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

// managedBuilder 是登记给图引擎的构建函数：按保存的最终配置重建执行器，
// 不再让模型选择组件，也不重新应用可能已经改变的默认值。
func (m *Manager) managedBuilder(ctx context.Context, spec graph.ManagedSpec) (graph.ManagedRunner, error) {
	resolved, err := m.specFrom(spec)
	if err != nil {
		return nil, err
	}
	runner, err := m.build(ctx, resolved)
	if err != nil {
		return nil, err
	}
	return func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		return m.run(ctx, runner, input)
	}, nil
}

// run 执行一次任务：把子 Agent 此前导出的上下文与新任务合成输入，运行后
// 导出新的上下文并返回最终回复。业务失败返回错误交给父模型处理；挂起、
// 取消等控制信号原样穿透给图引擎。
func (m *Manager) run(ctx context.Context, runner Runner, input json.RawMessage) (json.RawMessage, error) {
	var task taskInput
	if err := json.Unmarshal(input, &task); err != nil {
		return nil, fmt.Errorf("decode sub-agent task: %w", err)
	}
	saved, err := graph.ManagedAgentContext(ctx)
	if err != nil {
		return nil, err
	}
	messages, err := m.cfg.Codec.Input(saved, task.Task)
	if err != nil {
		return nil, err
	}
	output, err := runner.Invoke(ctx, schema.CloneMessages(messages))
	if err != nil {
		return nil, err
	}
	exported, err := m.cfg.Codec.Export(messages, output)
	if err != nil {
		return nil, fmt.Errorf("export sub-agent context: %w", err)
	}
	if err := graph.SetManagedAgentContext(ctx, exported); err != nil {
		return nil, err
	}
	return json.Marshal(finalText(output))
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
// 子进度由根检查点统一保存，不另开会话。自建 Builder 使用预置配方时用它
// 包装；使用者自建的图直接按 Runner 契约返回即可。
func FromAgent(a agent.Agent) Runner { return bypassRunner{a} }

type bypassRunner struct{ agent.Agent }

// Recoverable 声明预置配方可由根检查点恢复：内置配方都是 Graph 配方。
func (bypassRunner) Recoverable() error { return nil }

// Recoverable 是执行器对"可由根检查点恢复"的声明：返回 nil 表示可以，
// 返回错误说明原因。graph.CompiledGraph 已实现它；包装图的执行器要么嵌入
// 具体的图类型使方法被提升，要么显式转发，要么用 WithRecovery 声明。
// 构建结果没有声明时，创建任务会在登记前被拒绝。
type Recoverable interface{ Recoverable() error }

// ErrNotRecoverable 表示构建结果没有声明恢复能力或声明不可恢复。它是接入
// 错误而不是模型可修正的配置错误，不会被转换成工具结果。
var ErrNotRecoverable = control.Sentinel("sub-agent runner cannot be resumed from the root checkpoint")

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

// inboxModel 在每次模型调用前投递受管任务收件箱：未消费的消息作为用户消息
// 并入本次请求并确认。确认记在当前模型任务的待提交批次上，随该任务的提交
// 边界一起提升为已消费；审批挂起重放时批次原样重新投递，不丢消息。
// 非受管运行（没有收件箱）时原样透传。
type inboxModel struct{ llm.ChatModel }

func (m inboxModel) deliver(ctx context.Context, req llm.Request) (llm.Request, error) {
	pending, err := graph.ManagedInbox(ctx)
	if err != nil || len(pending) == 0 {
		return req, nil
	}
	req = req.Clone()
	for _, message := range pending {
		text := string(message.Payload)
		var s string
		if json.Unmarshal(message.Payload, &s) == nil {
			text = s
		}
		req.Messages = append(req.Messages, schema.UserMessage("Message from the parent agent: "+text))
	}
	if err := graph.AckManagedInbox(ctx, len(pending)); err != nil {
		return req, err
	}
	return req, nil
}

func (m inboxModel) Invoke(ctx context.Context, req llm.Request, opts ...runnable.Option) (llm.Response, error) {
	req, err := m.deliver(ctx, req)
	if err != nil {
		return llm.Response{}, err
	}
	return m.ChatModel.Invoke(ctx, req, opts...)
}

func (m inboxModel) Stream(ctx context.Context, req llm.Request, opts ...runnable.Option) (*runnable.StreamReader[llm.Response], error) {
	req, err := m.deliver(ctx, req)
	if err != nil {
		return nil, err
	}
	return m.ChatModel.Stream(ctx, req, opts...)
}

func (m inboxModel) Collect(ctx context.Context, in *runnable.StreamReader[llm.Request], opts ...runnable.Option) (llm.Response, error) {
	return runnable.CollectFromInvoke(ctx, in, m.Invoke, opts...)
}

func (m inboxModel) Transform(ctx context.Context, in *runnable.StreamReader[llm.Request], opts ...runnable.Option) (*runnable.StreamReader[llm.Response], error) {
	return runnable.TransformFromInvoke(ctx, in, m.Invoke, opts...)
}
