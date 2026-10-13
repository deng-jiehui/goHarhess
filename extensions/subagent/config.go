// Package subagent 让模型在运行中创建并管理子 Agent：子 Agent 是在工具
// 调用里同步运行的子图。使用者注册可选的模型、工具和角色模板并规定默认值；
// 模型通过 spawn_agent 等工具提出任务与选择，本包解析并校验为最终配置，
// 交给图引擎按嵌套子执行运行、检查点与恢复。
//
// 本包只依赖 graph、agent、state、tool 的公开 API：工具调用内的嵌套运行
// 自动按调用身份成为子执行并写入命名空间记录（父级检查点键 + 调用键），
// 子 Agent 内的审批挂起、恢复、已完成结果复用由图引擎的嵌套执行语义保证。
// 会话子 Agent 的跨轮记忆保存在调用方提供的 state.Store 中，键位于会话
// 命名空间 "<会话ID>/agents/<子AgentID>" 下，与父运行记录相互独立。
package subagent

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/goharness/goharness/pkg/llm"
	"github.com/goharness/goharness/pkg/runnable"
	"github.com/goharness/goharness/pkg/schema"
	"github.com/goharness/goharness/pkg/state"
	"github.com/goharness/goharness/pkg/tool"
)

const (
	// ParentModel 作为 Config.DefaultModel 时表示"默认使用父模型"：创建时
	// 解析为 Config.ParentModel 登记的具体名称并保存，恢复沿用该引用。
	ParentModel = "@parent"

	// DefaultBuilderID 与 DefaultBuilderVersion 是未显式指定时的构建函数身份。
	DefaultBuilderID      = "subagent"
	DefaultBuilderVersion = "v1"
)

// Runner 是子 Agent 的执行形态：输入输出都是消息序列，与 agent.Agent
// 一致，使用者自建的图也可以按该契约接入。
type Runner = runnable.NamedRunnable[[]schema.Message, []schema.Message]

// ResolvedSpec 是模型请求解析后的最终配置：每一项都已通过使用者注册范围
// 的校验。Model、Tools 是组件引用，模型实例、工具实现和凭据由构建函数
// 关联的资源提供，不进入可持久化的任务描述。
//
// 它只描述可缓存的图定义，不含任务文本：任务通过运行输入传给 Runner，
// 创建、追加任务与恢复重建使用同一份构建参数，相同配置的任务复用同一个
// 构建结果。
type ResolvedSpec struct {
	Instructions   string
	Model          string
	Tools          []string
	Role           string
	BuilderID      string
	BuilderVersion string
}

// Builder 按解析后的配置构建子 Agent 的执行器。默认实现使用 agent.New；
// 使用者可以替换它，返回自己的 Agent 图。相同配置的构建结果会被复用，
// 只读图定义与每次运行的 State 分开。
type Builder func(context.Context, ResolvedSpec) (Runner, error)

// Role 是使用者提供的角色模板：通用执行指令与可选的模型、工具默认值。
type Role struct {
	Description  string
	Instructions string
	// Model 是该角色的默认模型名称，空表示沿用使用者默认。
	Model string
	// Tools 是该角色的默认工具集合：nil 沿用使用者默认，非 nil（含空）覆盖它。
	Tools []string
}

// Config 声明使用者允许的范围与默认值。模型和工具名称是对这里注册组件
// 的引用——框架不根据模型输出猜测 Provider、创建工具实现或读取凭据。
type Config struct {
	// Models 是允许选择的模型；DefaultModel 为空且只有一个模型时使用它。
	Models map[string]llm.ChatModel
	// DefaultModel 是省略模型时的默认名称，可取 ParentModel。
	DefaultModel string
	// ParentModel 是父 Agent 所用模型在 Models 中的名称，供 ParentModel 解析。
	ParentModel string

	// Tools 是允许子 Agent 使用的工具（以 Spec().Name 为名）；DefaultTools
	// 是省略 tools 参数时的默认集合，nil 表示无工具。
	Tools        []tool.Tool
	DefaultTools []string

	Roles map[string]Role
	// Instructions 是所有子 Agent 共用的通用执行指令。
	Instructions string

	// Store 保存会话子 Agent 的跨轮记录；为空时 spawn_agent 等会话工具
	// 返回明确错误，run_subagent 一次性运行不受影响。
	Store state.Store

	// BuilderID 与 BuilderVersion 标识构建函数：登记在每个子 Agent 的记录里，
	// 恢复或追加任务时必须与注册项一致，不一致即拒绝而不是换实现继续。
	BuilderID      string
	BuilderVersion string
	// Builder 为空时使用默认的 agent.New 构建。
	Builder Builder
	// Codec 为空时使用"会话消息"上下文编码：下一次任务沿用此前的全部消息。
	// 自建图若不以消息历史为上下文，需要提供自己的编码。
	Codec ContextCodec
	// MaxTaskRunes 限制 task 与 instructions 的字符数，0 表示不限制。
	MaxTaskRunes int
	// MaxConcurrent 限制同一管理器下同时运行的子 Agent 数；0 表示不限制。
	// 超限的调用等待空位，不再与图批任务共享并发额度。
	MaxConcurrent int
	// TaskTimeout 是单次任务的时限（按次计，不含等待空位）；0 表示不限。
	// 超时返回模型可修正的错误结果；子 Agent 仍受工具运行时超时约束，
	// 更长的任务应整次放到后台，见对接说明。
	TaskTimeout time.Duration
	// MaxLLMCalls 是单次任务的模型调用上限（按次计）；0 表示沿用执行器
	// 自身默认。超限返回模型可修正的错误结果而非静默截断。
	// 只收紧不放宽：不能提高执行器构建时声明的上限。
	MaxLLMCalls int
	// EmitEvents 允许子 Agent 的运行事件透传到父运行的公共事件流；
	// 默认不透传，只影响展示，不影响观测、检查点与中断传播。
	EmitEvents bool
}

// ConfigError 是模型可修正的配置错误：名称不存在、超出范围或组合不兼容。
// 工具层把它作为模型可见的错误结果返回，不静默换成另一个组件。
type ConfigError struct{ Message string }

func (e *ConfigError) Error() string { return e.Message }

func configErrorf(format string, args ...any) error {
	return &ConfigError{Message: fmt.Sprintf(format, args...)}
}

// IsConfigError 判断错误链中是否有模型可修正的配置错误。
func IsConfigError(err error) bool {
	var target *ConfigError
	return errors.As(err, &target)
}

// Request 是模型提交的创建请求：未解析的原始选择。
type Request struct {
	Task         string
	Instructions string
	Model        string
	// Tools 为 nil 表示省略（使用默认集合）；非 nil 的空切片表示明确的空集合。
	Tools []string
	Role  string
}

// normalized 返回补全默认标识后的配置副本并完成校验。
func (c Config) normalized() (Config, error) {
	if c.BuilderID == "" {
		c.BuilderID = DefaultBuilderID
	}
	if c.BuilderVersion == "" {
		c.BuilderVersion = DefaultBuilderVersion
	}
	if c.MaxConcurrent < 0 {
		return c, errors.New("subagent MaxConcurrent cannot be negative")
	}
	if c.TaskTimeout < 0 {
		return c, errors.New("subagent TaskTimeout cannot be negative")
	}
	if c.MaxLLMCalls < 0 {
		return c, errors.New("subagent MaxLLMCalls cannot be negative")
	}
	if len(c.Models) == 0 {
		return c, errors.New("subagent config requires at least one model")
	}
	for name, model := range c.Models {
		if strings.TrimSpace(name) == "" || model == nil {
			return c, fmt.Errorf("subagent model %q must have a non-empty name and a model instance", name)
		}
		if name == ParentModel {
			return c, fmt.Errorf("subagent model name %q is reserved", ParentModel)
		}
	}
	if c.DefaultModel == "" && len(c.Models) == 1 {
		for name := range c.Models {
			c.DefaultModel = name
		}
	}
	if c.DefaultModel == ParentModel {
		if _, ok := c.Models[c.ParentModel]; !ok {
			return c, fmt.Errorf("subagent default model %q requires ParentModel to name a registered model, got %q", ParentModel, c.ParentModel)
		}
	} else if c.DefaultModel != "" {
		if _, ok := c.Models[c.DefaultModel]; !ok {
			return c, fmt.Errorf("subagent default model %q is not registered", c.DefaultModel)
		}
	}
	seen := make(map[string]bool, len(c.Tools))
	for _, t := range c.Tools {
		if t == nil {
			return c, errors.New("subagent tool cannot be nil")
		}
		name := t.Spec().Name
		if err := t.Spec().Validate(); err != nil {
			return c, err
		}
		if seen[name] {
			return c, fmt.Errorf("subagent tool %q is registered twice", name)
		}
		seen[name] = true
	}
	if err := c.checkTools("default tools", c.DefaultTools, seen); err != nil {
		return c, err
	}
	for name, role := range c.Roles {
		if strings.TrimSpace(name) == "" {
			return c, errors.New("subagent role name cannot be empty")
		}
		if role.Model != "" {
			if _, ok := c.Models[role.Model]; !ok {
				return c, fmt.Errorf("subagent role %q model %q is not registered", name, role.Model)
			}
		}
		if err := c.checkTools(fmt.Sprintf("role %q tools", name), role.Tools, seen); err != nil {
			return c, err
		}
	}
	if c.Builder != nil && c.BuilderID == DefaultBuilderID && c.BuilderVersion == DefaultBuilderVersion {
		return c, errors.New("a custom Builder requires its own BuilderID or BuilderVersion so that saved agents are never rebuilt by a different implementation")
	}
	return c, nil
}

func (c Config) checkTools(label string, names []string, registered map[string]bool) error {
	for _, name := range names {
		if !registered[name] {
			return fmt.Errorf("subagent %s: tool %q is not registered", label, name)
		}
	}
	return nil
}

// resolve 按固定优先级解析创建请求：使用者的默认配置 → 所选角色模板的
// 默认配置 → 模型明确提供的可选项。允许使用的范围始终由使用者确定。
func (c Config) resolve(req Request) (ResolvedSpec, error) {
	if strings.TrimSpace(req.Task) == "" {
		return ResolvedSpec{}, configErrorf("task is required and cannot be empty")
	}
	if c.MaxTaskRunes > 0 {
		if n := len([]rune(req.Task)); n > c.MaxTaskRunes {
			return ResolvedSpec{}, configErrorf("task is too long: %d characters, the limit is %d", n, c.MaxTaskRunes)
		}
		if n := len([]rune(req.Instructions)); n > c.MaxTaskRunes {
			return ResolvedSpec{}, configErrorf("instructions are too long: %d characters, the limit is %d", n, c.MaxTaskRunes)
		}
	}
	out := ResolvedSpec{Role: req.Role, BuilderID: c.BuilderID, BuilderVersion: c.BuilderVersion}

	var role Role
	if req.Role != "" {
		var ok bool
		if role, ok = c.Roles[req.Role]; !ok {
			return ResolvedSpec{}, configErrorf("unknown role %q; available roles: %s", req.Role, listNames(c.roleNames()))
		}
	}

	modelName := c.DefaultModel
	if role.Model != "" {
		modelName = role.Model
	}
	if req.Model != "" {
		modelName = req.Model
	}
	if modelName == ParentModel {
		modelName = c.ParentModel
	}
	if modelName == "" {
		return ResolvedSpec{}, configErrorf("no model selected and no default model is configured; available models: %s", listNames(c.modelNames()))
	}
	if _, ok := c.Models[modelName]; !ok {
		return ResolvedSpec{}, configErrorf("unknown model %q; available models: %s", modelName, listNames(c.modelNames()))
	}
	out.Model = modelName

	tools := c.DefaultTools
	if role.Tools != nil {
		tools = role.Tools
	}
	if req.Tools != nil {
		tools = req.Tools
	}
	registered := make(map[string]bool, len(c.Tools))
	for _, t := range c.Tools {
		registered[t.Spec().Name] = true
	}
	out.Tools = make([]string, 0, len(tools))
	picked := map[string]bool{}
	for _, name := range tools {
		if !registered[name] {
			return ResolvedSpec{}, configErrorf("unknown tool %q; available tools: %s", name, listNames(c.toolNames()))
		}
		if !picked[name] {
			picked[name] = true
			out.Tools = append(out.Tools, name)
		}
	}

	var parts []string
	for _, text := range []string{c.Instructions, role.Instructions, req.Instructions} {
		if text = strings.TrimSpace(text); text != "" {
			parts = append(parts, text)
		}
	}
	out.Instructions = strings.Join(parts, "\n\n")
	return out, nil
}

func (c Config) roleNames() []string {
	names := make([]string, 0, len(c.Roles))
	for name := range c.Roles {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func (c Config) modelNames() []string {
	names := make([]string, 0, len(c.Models))
	for name := range c.Models {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func (c Config) toolNames() []string {
	names := make([]string, 0, len(c.Tools))
	for _, t := range c.Tools {
		names = append(names, t.Spec().Name)
	}
	sort.Strings(names)
	return names
}

func listNames(names []string) string {
	if len(names) == 0 {
		return "(none)"
	}
	return strings.Join(names, ", ")
}
