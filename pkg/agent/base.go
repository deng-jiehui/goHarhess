package agent

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"time"

	"github.com/goharness/goharness/internal/agentrecipe"
	"github.com/goharness/goharness/pkg/contextengine"
	"github.com/goharness/goharness/pkg/graph"
	"github.com/goharness/goharness/pkg/llm"
	"github.com/goharness/goharness/pkg/runnable"
	"github.com/goharness/goharness/pkg/schema"
	"github.com/goharness/goharness/pkg/state"
	"github.com/goharness/goharness/pkg/tool"
)

var ErrMissingModel = errors.New("agent requires an llm model")

// baseConfig 是内置 Agent 实现的共享配置。公开入口只有 Option：
// agent.New(model, opts...) 把位置参数写入 Model 后逐项应用 Option，
// 「显式配置为 0」与「未配置」由 Option 的调用与否天然区分。
type baseConfig struct {
	Name          string
	Description   string
	SystemMessage string
	Model         llm.ChatModel
	Tools         []tool.Tool
	// BeforeToolBatch runs synchronously after a complete tool-call batch is
	// parsed and before any tool starts.
	BeforeToolBatch []tool.BeforeBatchHook
	// Middlewares run in order inside the fixed system runtime.
	Middlewares []tool.Middleware
	// ContextBuilder assembles model context. The zero value uses the standard
	// contextengine builder.
	ContextBuilder runnable.Runnable[contextengine.Request, contextengine.Result]
	// CheckpointStore enables durable conversations when a run supplies a conversation ID.
	CheckpointStore   state.Store
	CheckpointTimeout time.Duration
	// CheckpointDurability 是检查点持久化模式的默认值，零值使用 graph.DurabilityAsync；
	// 单次运行可用 WithDurability 覆盖。
	CheckpointDurability graph.Durability
	// Numeric execution limits can also be set with Agent initialization Options.
	MaxDepth       int
	Timeout        time.Duration
	MaxConcurrency int
	// ToolRetries 是只读工具的自动重试次数，ToolRetryBackoff 是重试间隔。
	ToolRetries         int
	ToolRetryBackoff    time.Duration
	MaxTokens           int
	MaxLLMCalls         int
	ExtraGenerateConfig llm.GenerateConfig
	// Prompt 与 TextReAct 只作用于 ReAct 配方：自定义提示模板，
	// 或退回文本协议解析而不发送原生 Functions。
	Prompt    string
	TextReAct bool
}

// base provides common model, tool and extension behavior to concrete Agents.
type base struct {
	name                 string
	description          string
	systemMessage        string
	modelNode            *llm.ChatModelNode
	tools                map[string]schema.FunctionDefinition
	toolOrder            []string
	toolNode             *tool.ToolsNode
	contextBuilder       runnable.Runnable[contextengine.Request, contextengine.Result]
	checkpointStore      state.Store
	checkpointTimeout    time.Duration
	checkpointDurability graph.Durability
	limits               agentLimits
	extraGenerateConfig  llm.GenerateConfig
}

// newBase validates shared configuration and registers all configured tools.
// cfg 必须是经 resolveBaseConfig 解析过的配置，本函数不再重复解析。
func newBase(cfg baseConfig) (*base, error) {
	var err error
	if cfg.ContextBuilder == nil {
		cfg.ContextBuilder, err = contextengine.NewContextBuilder(contextengine.Config{})
		if err != nil {
			return nil, fmt.Errorf("configure agent %q context builder: %w", cfg.Name, err)
		}
	}
	var modelNode *llm.ChatModelNode
	if cfg.Model != nil {
		// 重试/限流等模型策略由调用方自行组合装饰器，Agent 不再代为包装。
		modelNode, err = llm.NewChatModelNode(cfg.Model)
		if err != nil {
			return nil, fmt.Errorf("configure agent %q model node: %w", cfg.Name, err)
		}
	}
	var toolsNode *tool.ToolsNode
	if len(cfg.Tools) > 0 {
		toolsNode, err = tool.NewToolsNode(tool.ToolNodeConfig{
			Name:           "tools",
			Tools:          cfg.Tools,
			BeforeBatch:    cfg.BeforeToolBatch,
			Middlewares:    cfg.Middlewares,
			MaxConcurrency: cfg.MaxConcurrency,
			MaxDepth:       cfg.MaxDepth,
			Timeout:        cfg.Timeout,
			MaxRetries:     cfg.ToolRetries,
			// 此处传递的是别名合并与默认值填充后的最终配置，标记为已提供，
			// 显式零间隔不会被工具节点再次填回默认值。
			RetryBackoff:    cfg.ToolRetryBackoff,
			RetryBackoffSet: true,
		})
		if err != nil {
			return nil, fmt.Errorf("configure agent %q tools node: %w", cfg.Name, err)
		}
	}
	b := &base{
		name:                 cfg.Name,
		description:          cfg.Description,
		systemMessage:        cfg.SystemMessage,
		modelNode:            modelNode,
		tools:                map[string]schema.FunctionDefinition{},
		toolNode:             toolsNode,
		contextBuilder:       cfg.ContextBuilder,
		checkpointStore:      cfg.CheckpointStore,
		checkpointTimeout:    cfg.CheckpointTimeout,
		checkpointDurability: cfg.CheckpointDurability,
		limits:               limitsFromConfig(cfg),
		extraGenerateConfig:  cfg.ExtraGenerateConfig,
	}
	if toolsNode != nil {
		for _, definition := range toolsNode.Definitions() {
			b.toolOrder = append(b.toolOrder, definition.Name)
			b.tools[definition.Name] = definition
		}
	}
	return b, nil
}

func (b *base) Name() string {
	return b.name
}

func (b *base) Description() string {
	return b.description
}

func (*base) NodeKind() runnable.NodeKind { return runnable.NodeKindAgent }

// OptionTypes 声明 Agent 入口接受的配置类型：自身 RunOptions、模型节点与
// 工具节点声明的类型（含下游模型扩展配置），以及 Context Builder 声明的
// 类型。声明用于按类型广播与 AgentNode 的转发委托。
func (b *base) OptionTypes() []reflect.Type {
	types := []reflect.Type{runnable.OptionType[RunOptions]()}
	if b.modelNode != nil {
		types = append(types, b.modelNode.OptionTypes()...)
	}
	if b.toolNode != nil {
		types = append(types, b.toolNode.OptionTypes()...)
	}
	if provider, ok := b.contextBuilder.(optionTypesProvider); ok && provider != nil {
		types = append(types, provider.OptionTypes()...)
	}
	seen := make(map[reflect.Type]bool, len(types))
	out := types[:0]
	for _, item := range types {
		if item != nil && !seen[item] {
			seen[item] = true
			out = append(out, item)
		}
	}
	return out
}

// runPrelude 汇总各 Agent 共用的运行前置：解析并下发运行选项、装载会话、
// 绑定运行上下文、预处理消息并开启生命周期。返回可直接驱动配方的上下文
// 与冻结后的运行选项；session 已回填进 opts。
func (b *base) runPrelude(ctx context.Context, messages []schema.Message, kind AgentKind, options []runnable.Option) (runCtx context.Context, prepared []schema.Message, opts RunOptions, lifecycle *runLifecycle, err error) {
	opts, err = resolveRunOptions(b.limits, options)
	if err != nil {
		return nil, nil, RunOptions{}, nil, err
	}
	if err = b.dispatchRunOptions(&opts); err != nil {
		return nil, nil, RunOptions{}, nil, err
	}
	opts.agentKind = kind
	loaded, session, err := b.loadConversation(ctx, messages, &opts)
	if err != nil {
		return nil, nil, RunOptions{}, nil, err
	}
	ctx = bindRunContext(ctx, session)
	prepared = b.prepareMessages(loaded, &opts)
	runCtx, lifecycle, err = b.beginRun(ctx, prepared, opts)
	if err != nil {
		return nil, nil, RunOptions{}, nil, err
	}
	opts.session = session
	runCtx = withResolvedRunOptions(runCtx, opts)
	runCtx = withToolExecutionLimits(runCtx, opts)
	return runCtx, prepared, opts, lifecycle, nil
}

// prepareMessages 是子类预处理输入的扩展点。输入已在会话装配边界隔离，
// 内部按 Message 不可变约定传递，默认实现不再重复深拷贝。
func (b *base) prepareMessages(messages []schema.Message, _ *RunOptions) []schema.Message {
	return messages
}

// withSystemMessage 注入系统消息：需要修改首条时只复制该消息结构，
// 元素级深拷贝交给消费方边界，不克隆整个历史。
func (b *base) withSystemMessage(messages []schema.Message) []schema.Message {
	if b.systemMessage == "" {
		return messages
	}
	if len(messages) == 0 || messages[0].Role != schema.RoleSystem {
		prepared := make([]schema.Message, 0, len(messages)+1)
		prepared = append(prepared, schema.SystemMessage(b.systemMessage))
		return append(prepared, messages...)
	}
	prepared := append([]schema.Message(nil), messages...)
	head := prepared[0]
	head.ContentBlocks = append([]schema.ContentBlock{schema.NewTextBlock(b.systemMessage + "\n\n")}, head.ContentBlocks...)
	prepared[0] = head
	return prepared
}

// withAgentName 逐条命名：setMessageAgentName 对未命名消息自行克隆，
// 已命名消息原样通过，不预克隆整个列表。
func (b *base) withAgentName(messages []schema.Message) []schema.Message {
	if b.name == "" {
		return messages
	}
	named := make([]schema.Message, len(messages))
	for i := range named {
		named[i] = setMessageAgentName(messages[i], b.name)
	}
	return named
}

func (b *base) toolDefinitions() []schema.FunctionDefinition {
	definitions := make([]schema.FunctionDefinition, 0, len(b.toolOrder))
	for _, name := range b.toolOrder {
		definitions = append(definitions, b.tools[name])
	}
	return definitions
}

func (b *base) prepareLLMRequest(ctx context.Context, messages []schema.Message, functions []schema.FunctionDefinition, opts RunOptions) (llm.Request, error) {
	if b.modelNode == nil {
		return llm.Request{}, ErrMissingModel
	}
	messages = b.withSystemMessage(messages)
	// Builder 只收到按其声明类型分流的选项；其余组件配置由配方图入口分发。
	// 预留输出预算使用裁剪前解析出的有效模型配置，与模型实际收到的参数一致。
	assembled, err := b.contextBuilder.Invoke(ctx, contextengine.Request{
		// ContextBuilder 是可替换扩展点，输入按边界克隆；默认引擎内部
		// 还会为归一化/裁剪再建自有副本。
		Input:                schema.CloneMessages(messages),
		MaxContextTokens:     opts.ContextWindowTokens,
		ReservedOutputTokens: effectiveMaxTokens(opts),
	}, opts.builderOptions...)
	if err != nil {
		return llm.Request{}, fmt.Errorf("assemble LLM context: %w", err)
	}
	// assembled.Messages 归本次请求所有；模型边界由 ChatModelNode 的
	// request.Clone() 隔离，此处不再重复深拷贝。
	return llm.Request{Messages: assembled.Messages, Functions: functions}, nil
}

// modelOptions 把 Agent 级运行配置翻译为模型调用的类型化选项。
// 顺序即优先级：构造时 GenerateConfig → 本次运行的派生参数 → 调用方
// 直接给出的组件选项在 forwarded 尾部，同名键后者覆盖。
// 未配置模型的 Agent（如纯成员编排的 GroupChat）不派生模型选项：
// 配方内没有模型节点可消费，广播只会在入口产生无接收者错误。
func (b *base) modelOptions(opts RunOptions) []runnable.Option {
	if b.modelNode == nil {
		return nil
	}
	derived := make([]runnable.Option, 0, 3)
	if len(b.extraGenerateConfig) > 0 {
		derived = append(derived, llm.WithGenerateConfig(b.extraGenerateConfig))
	}
	cfg := llm.GenerateConfig{}
	if opts.Seed != nil {
		cfg["seed"] = *opts.Seed
	}
	if opts.MaxTokens > 0 {
		cfg["max_tokens"] = opts.MaxTokens
	}
	if len(cfg) > 0 {
		derived = append(derived, llm.WithGenerateConfig(cfg))
	}
	if opts.DeltaStream {
		derived = append(derived, llm.WithDeltaStream(true))
	}
	return derived
}

// dispatchRunOptions 分流未消费选项并挂接 Agent 级派生配置：
// Context Builder 声明的类型直达 Builder；其余连同模型选项一起进入配方图
// 入口统一分发。llm.Options 在边界解析一次并冻结转发，解析结果回填为本次
// 运行的有效模型配置，供上下文预算与增量协议判断复用。
func (b *base) dispatchRunOptions(opts *RunOptions) error {
	forwarded, builderOptions, err := splitComponentOptions(b.contextBuilder, opts.forwarded)
	if err != nil {
		return err
	}
	opts.builderOptions = builderOptions
	combined := append(b.modelOptions(*opts), forwarded...)
	resolved, combined, err := llm.PrepareOptions(combined)
	if err != nil {
		return err
	}
	// 定向到配方模型节点的 llm.Options 同样进入有效配置：在此冻结一次，
	// 冻结 apply 并入有效配置，转发给模型节点的是冻结 spec——节点应用
	// 快照而不是重放设置函数，定向参数只执行一次。
	specs, err := runnable.FlattenOptions(combined)
	if err != nil {
		return err
	}
	targeted, rebuilt, firstIndex := extractModelTargetedSpecs(specs)
	if len(targeted) > 0 {
		frozen, _, ferr := runnable.FreezeSpecs(targeted)
		if ferr != nil {
			return ferr
		}
		if resolved, _, err = runnable.ResolveComponentOptions(resolved, runnable.OptionsFromSpecs(frozen)...); err != nil {
			return err
		}
		for index := range frozen {
			frozen[index].Path = []string{agentrecipe.ModelNodeID}
		}
		combined = runnable.OptionsFromSpecs(slices.Insert(rebuilt, firstIndex, frozen...))
	}
	opts.effectiveModel = resolved
	// 调用方的 agent.WithDeltaStream 写进 RunOptions，无模型 Agent（如纯
	// 编排 GroupChat）没有 llm.Options 规格可解析，resolved 为零值——不
	// 覆盖调用方设置；模型选项显式开启时仍会生效。
	opts.DeltaStream = opts.DeltaStream || resolved.DeltaStream
	opts.forwarded = combined
	return nil
}

// extractModelTargetedSpecs 把定向到本 Agent 配方模型节点（llm）的
// llm.Options spec 从列表中剥离：matched 去掉路径可直接冻结；rebuilt 是
// 剔除后的列表；firstIndex 是首个命中位置，冻结结果插回该处保持相对顺序。
func extractModelTargetedSpecs(specs []runnable.OptionSpec) (matched, rebuilt []runnable.OptionSpec, firstIndex int) {
	modelType := runnable.OptionType[llm.Options]()
	firstIndex = len(specs)
	for _, spec := range specs {
		if len(spec.Path) == 1 && spec.Path[0] == agentrecipe.ModelNodeID && spec.ConfigType == modelType {
			if len(matched) == 0 {
				firstIndex = len(rebuilt)
			}
			spec.Path = nil
			matched = append(matched, spec)
			continue
		}
		rebuilt = append(rebuilt, spec)
	}
	return matched, rebuilt, firstIndex
}

// effectiveMaxTokens 返回本次运行的有效 max_tokens：优先取调用方经模型
// 选项显式给出的值，否则回落到 Agent 运行配置。
func effectiveMaxTokens(opts RunOptions) int {
	raw, ok := opts.effectiveModel.ExtraGenerateConfig["max_tokens"]
	if !ok {
		return opts.MaxTokens
	}
	switch v := raw.(type) {
	case int:
		if v > 0 {
			return v
		}
	case int64:
		if v > 0 {
			return int(v)
		}
	case float64:
		if v > 0 {
			return int(v)
		}
	}
	return opts.MaxTokens
}
