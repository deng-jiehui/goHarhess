package agent

import (
	"context"
	"reflect"
	"slices"
	"time"

	"github.com/goharness/goharness/pkg/contextengine"
	"github.com/goharness/goharness/pkg/graph"
	"github.com/goharness/goharness/pkg/llm"
	"github.com/goharness/goharness/pkg/runnable"
	"github.com/goharness/goharness/pkg/state"
	"github.com/goharness/goharness/pkg/tool"
)

type resolvedRunOptionsKey struct{}

func withResolvedRunOptions(ctx context.Context, opts RunOptions) context.Context {
	return context.WithValue(ctx, resolvedRunOptionsKey{}, opts)
}

func withToolExecutionLimits(ctx context.Context, options RunOptions) context.Context {
	return tool.WithExecutionLimits(ctx, tool.ExecutionLimits{
		MaxConcurrency: options.MaxParallelTools,
		Timeout:        options.ToolCallTimeout,
	})
}

func resolvedRunOptionsFromContext(ctx context.Context) (RunOptions, bool) {
	opts, ok := ctx.Value(resolvedRunOptionsKey{}).(RunOptions)
	return opts, ok
}

const (
	DefaultMaxParallelTools = 8
)

// RunOptions 是 Agent 单次运行的类型化配置。公开 WithXXX 函数返回归属于该
// 类型的 runnable.Option；forwarded 与 builderOptions 是入口解析后分流的
// 未消费选项——前者交给内部配方图统一分发，后者直达 Context Builder。
type RunOptions struct {
	Seed                *int64
	MaxLLMCalls         int
	MaxParallelTools    int
	ToolCallTimeout     time.Duration
	MaxTokens           int
	DeltaStream         bool
	ConversationID      string
	ContextWindowTokens int
	StateValues         map[string]any
	CheckpointBypass    bool

	forwarded      []runnable.Option
	builderOptions []runnable.Option
	effectiveModel llm.Options

	checkpointStore   state.Store
	checkpointKey     string
	checkpointSet     bool
	abandonUnfinished bool
	durability        graph.Durability
	resume            bool
	agentKind         AgentKind
	session           *conversationSession
}

func defaultRunOptions(limits agentLimits) RunOptions {
	return RunOptions{
		MaxLLMCalls:      limits.maxLLMCalls,
		MaxParallelTools: limits.maxConcurrency,
		ToolCallTimeout:  limits.timeout,
		MaxTokens:        limits.maxTokens,
		StateValues:      map[string]any{},
	}
}

// resolveRunOptions 把调用入口的 Runnable Option 按类型归属解析为
// RunOptions：设置函数在此执行一次并附带 panic 保护；未被 Agent 消费的
// 选项进入 remaining，等待分发给配方图或 Context Builder。
func resolveRunOptions(limits agentLimits, options []runnable.Option) (RunOptions, error) {
	resolved, remaining, err := runnable.ResolveComponentOptions(defaultRunOptions(limits), options...)
	if err != nil {
		return RunOptions{}, err
	}
	// WithResume 等选项的 apply 函数已向 forwarded 追加图级配置，保留它们，
	// 再补上未被 Agent 消费的剩余选项交给配方图分发。
	resolved.forwarded = append(resolved.forwarded, remaining...)
	if resolved.MaxLLMCalls <= 0 || resolved.MaxLLMCalls > limits.maxLLMCalls {
		resolved.MaxLLMCalls = limits.maxLLMCalls
	}
	if resolved.MaxParallelTools <= 0 || resolved.MaxParallelTools > limits.maxConcurrency {
		resolved.MaxParallelTools = limits.maxConcurrency
	}
	if resolved.ToolCallTimeout <= 0 || resolved.ToolCallTimeout > limits.timeout {
		resolved.ToolCallTimeout = limits.timeout
	}
	if resolved.ContextWindowTokens < 0 {
		resolved.ContextWindowTokens = 0
	}
	if resolved.StateValues == nil {
		resolved.StateValues = map[string]any{}
	}
	return resolved, nil
}

// optionTypesProvider 与 graph 包内的可选接口一致：组件声明接受的配置类型。
type optionTypesProvider interface {
	OptionTypes() []reflect.Type
}

// splitComponentOptions 把未消费选项按 Context Builder 声明的类型分流：
// 定向选项与模型/工具/图配置留在 forwarded 交给配方图入口分发；Builder
// 声明的无目标类型改由 prepareLLMRequest 直达 Builder，避免在图内出现
// 无接收者错误。
func splitComponentOptions(builder runnable.Runnable[contextengine.Request, contextengine.Result], options []runnable.Option) (forwarded, builderOptions []runnable.Option, err error) {
	specs, err := runnable.FlattenOptions(options)
	if err != nil {
		return nil, nil, err
	}
	var builderTypes []reflect.Type
	if provider, ok := builder.(optionTypesProvider); ok && provider != nil {
		builderTypes = provider.OptionTypes()
	}
	var toBuilder, rest []runnable.OptionSpec
	for _, spec := range specs {
		if len(spec.Path) == 0 && spec.ConfigType != nil && slices.Contains(builderTypes, spec.ConfigType) {
			toBuilder = append(toBuilder, spec)
			continue
		}
		rest = append(rest, spec)
	}
	return runnable.OptionsFromSpecs(rest), runnable.OptionsFromSpecs(toBuilder), nil
}

func WithSeed(seed int64) runnable.Option {
	return runnable.WrapOption(func(opts *RunOptions) { opts.Seed = &seed })
}

// WithRunMaxLLMCalls tightens the Agent-wide model-call limit for one run.
// The Agent-wide default is configured with WithMaxLLMCalls.
func WithRunMaxLLMCalls(max int) runnable.Option {
	return runnable.WrapOption(func(opts *RunOptions) { opts.MaxLLMCalls = max })
}

func WithMaxParallelTools(max int) runnable.Option {
	return runnable.WrapOption(func(opts *RunOptions) { opts.MaxParallelTools = max })
}

func WithToolCallTimeout(timeout time.Duration) runnable.Option {
	return runnable.WrapOption(func(opts *RunOptions) { opts.ToolCallTimeout = timeout })
}

func WithDeltaStream(enabled bool) runnable.Option {
	return runnable.WrapOption(func(opts *RunOptions) { opts.DeltaStream = enabled })
}

// WithConversationID loads and saves a versioned conversation checkpoint.
func WithConversationID(value string) runnable.Option {
	return runnable.WrapOption(func(opts *RunOptions) { opts.ConversationID = value })
}

// WithAbandonUnfinished 让新输入先放弃会话中未完成的运行（失败或等待人工输入），
// 会话内容回退到该运行开始前，再开始本轮。对 Resume 无效。
func WithAbandonUnfinished() runnable.Option {
	return runnable.WrapOption(func(opts *RunOptions) { opts.abandonUnfinished = true })
}

// WithDurability 覆盖本次运行的检查点持久化模式（async 或 sync）。
func WithDurability(value graph.Durability) runnable.Option {
	return runnable.WrapOption(func(opts *RunOptions) { opts.durability = value })
}

// WithCheckpoint 为本次运行指定统一存储与会话 ID。
func WithCheckpoint(store state.Store, key string) runnable.Option {
	return runnable.WrapOption(func(opts *RunOptions) {
		opts.checkpointSet = true
		opts.checkpointStore = store
		opts.checkpointKey = key
	})
}

// WithContextWindowTokens overrides the configured context window for one run.
func WithContextWindowTokens(value int) runnable.Option {
	return runnable.WrapOption(func(opts *RunOptions) { opts.ContextWindowTokens = value })
}

// WithStateValue updates one durable value on the next successful checkpoint.
func WithStateValue(key string, value any) runnable.Option {
	return runnable.WrapOption(func(opts *RunOptions) { opts.StateValues[key] = value })
}

// WithStateValues updates durable values on the next successful checkpoint.
func WithStateValues(values map[string]any) runnable.Option {
	return runnable.WrapOption(func(opts *RunOptions) {
		for key, value := range values {
			opts.StateValues[key] = value
		}
	})
}

// WithCheckpointBypass prevents nested Agents from loading or saving state.
func WithCheckpointBypass() runnable.Option {
	return runnable.WrapOption(func(opts *RunOptions) { opts.CheckpointBypass = true })
}
