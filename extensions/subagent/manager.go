package subagent

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"unicode/utf8"

	"github.com/goharness/goharness/pkg/control"
	"github.com/goharness/goharness/pkg/schema"
	"github.com/goharness/goharness/pkg/tool"
)

// 模型可见的管理工具名称。子 Agent 在工具调用内同步运行：
// spawn_agent/run_subagent 直到任务完成才返回；挂起（如子 Agent 内审批）
// 经调用链上抛为父运行挂起，恢复时由图引擎从命名空间记录续跑。
const (
	ToolSpawn    = "spawn_agent"
	ToolRun      = "run_subagent"
	ToolFollowup = "followup_task"
	ToolList     = "list_agents"
	ToolGet      = "get_agent"
	ToolClose    = "close_agent"
)

// Manager 把使用者的注册范围、构建函数和模型工具接到图引擎上。Tools 放进
// 父 Agent 的 Tools 即可。子 Agent 运行产生的进度与挂起全部走嵌套子执行：
// 同一调用恢复重放时身份不变，已完成的子执行直接复用结果，挂起的从其
// 命名空间记录继续。
type Manager struct {
	cfg   Config
	tools map[string]tool.Tool

	mu      sync.Mutex
	runners map[string]Runner
	slots   chan struct{}
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
	if m.cfg.MaxConcurrent > 0 {
		m.slots = make(chan struct{}, m.cfg.MaxConcurrent)
	}
	return m, nil
}

// Resolve 把模型的创建请求按注册范围解析为最终配置；错误是模型可修正的
// ConfigError。
func (m *Manager) Resolve(req Request) (ResolvedSpec, error) { return m.cfg.resolve(req) }

// callID 派生本调用内稳定的子 Agent 标识：恢复重放同一工具调用时
// IdempotencyKey 不变，重建出的 ID 相同，使创建动作幂等。
func callID(prefix, key string) string {
	sum := sha256.Sum256([]byte(key))
	return prefix + hex.EncodeToString(sum[:8])
}

// acquireSlot 按 MaxConcurrent 领取并发空位；未配置时直接放行。
func (m *Manager) acquireSlot(ctx context.Context) error {
	if m.slots == nil {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case m.slots <- struct{}{}:
		return nil
	}
}

func (m *Manager) releaseSlot() {
	if m.slots != nil {
		<-m.slots
	}
}

// execute 同步运行一次子 Agent 任务：嵌套调用的挂起/取消/中断原样上交，
// 业务失败返回错误交给模型可见结果转换。input 为调用方冻结的任务输入。
func (m *Manager) execute(ctx context.Context, resolved ResolvedSpec, agentID string, input []schema.Message) (result string, exported json.RawMessage, err error) {
	if err := m.acquireSlot(ctx); err != nil {
		return "", nil, err
	}
	defer m.releaseSlot()
	runner, err := m.build(ctx, resolved)
	if err != nil {
		return "", nil, err
	}
	// 创建者作用域：子 Agent 内部再发起的子 Agent 归属该子 Agent。
	ctx = withCaller(ctx, agentID)
	result, exported, err = m.run(ctx, runner, input)
	if err != nil {
		return "", nil, err
	}
	return result, exported, nil
}

// modelError 把模型可修正的错误转成模型可见的错误结果；框架错误、挂起信号
// 与其他意外错误原样返回，由工具节点上交调度器处理。
func modelError(err error) (tool.Result, error) {
	switch {
	case IsConfigError(err) && !hasControlSignal(err),
		errors.Is(err, ErrAgentNotFound),
		errors.Is(err, ErrAgentBusy),
		errors.Is(err, ErrAgentClosed),
		errors.Is(err, ErrAgentCorrupt),
		errors.Is(err, ErrNoSession):
		return tool.ErrorResult(schema.NewTextBlock(friendly(err))), nil
	}
	return tool.Result{}, err
}

// hasControlSignal 判断错误链是否携带框架错误或中断、挂起控制信号——
// 合并错误中只要存在一个，整条错误就必须上交，不能被模型可修正的
// 配置反馈遮蔽。
func hasControlSignal(err error) bool {
	if control.AsFramework(err) {
		return true
	}
	if _, ok := control.AsInterrupt(err); ok {
		return true
	}
	if _, ok := control.AsSuspension(err); ok {
		return true
	}
	return false
}

func friendly(err error) string {
	switch {
	case IsConfigError(err):
		return configMessage(err)
	case errors.Is(err, ErrAgentNotFound):
		return fmt.Sprintf("%v. Only sub-agents created by the current caller in this conversation are visible; use list_agents to see them.", err)
	case errors.Is(err, ErrAgentBusy):
		return fmt.Sprintf("%v. Wait for its current task before assigning another one.", err)
	case errors.Is(err, ErrAgentClosed):
		return fmt.Sprintf("%v. Its saved results stay readable with get_agent.", err)
	case errors.Is(err, ErrNoSession):
		return fmt.Sprintf("%v. Session sub-agents need a run with checkpoint enabled; run_subagent works without one.", err)
	}
	return err.Error()
}

// configMessage 取出配置错误声明的模型可见文本，不含外层包装的内容。
func configMessage(err error) string {
	var config *ConfigError
	if errors.As(err, &config) {
		return config.Message
	}
	return err.Error()
}

// buildFailure 按错误类别处理创建前的构建失败：模型可修正的配置错误作为
// 错误结果返回；框架错误以及挂起、取消等控制信号原样上交；其余构建失败
// 对模型只给出泛化提示，完整诊断随错误链保留。
func buildFailure(ctx context.Context, err error) (tool.Result, error) {
	if hasControlSignal(err) || ctx.Err() != nil ||
		errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return tool.Result{}, err
	}
	visible := "the sub-agent could not be created with this configuration"
	var config *ConfigError
	if errors.As(err, &config) {
		visible = config.Message
	}
	return tool.Result{}, errors.Join(
		tool.ModelVisible(errors.New(visible)),
		fmt.Errorf("build sub-agent: %w", err),
	)
}

func truncate(text string, limit int) string {
	if utf8.RuneCountInString(text) <= limit {
		return text
	}
	return string([]rune(text)[:limit]) + "…"
}

// describe 生成子 Agent 的职责说明：优先用模型给的名称，其次角色，最后任务摘要。
func describe(name, role, task string) string {
	switch {
	case name != "":
		return truncate(name, 80)
	case role != "":
		return role
	}
	return truncate(task, 80)
}
