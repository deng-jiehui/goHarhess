package agent

import (
	"fmt"
	"time"

	"github.com/goharness/goharness/pkg/contextengine"
	"github.com/goharness/goharness/pkg/graph"
	"github.com/goharness/goharness/pkg/llm"
	"github.com/goharness/goharness/pkg/runnable"
	"github.com/goharness/goharness/pkg/state"
	"github.com/goharness/goharness/pkg/tool"
)

const (
	DefaultMaxDepth          = 16
	DefaultTimeout           = 30 * time.Second
	DefaultMaxConcurrency    = 8
	DefaultMaxRetries        = 0
	DefaultRetryBackoff      = 100 * time.Millisecond
	DefaultMaxTokens         = 4096
	DefaultMaxLLMCalls       = 20
	DefaultCheckpointTimeout = 5 * time.Second

	hardMaxDepth             = 32
	hardMaxTimeout           = 5 * time.Minute
	hardMaxConcurrency       = 64
	hardMaxRetries           = 10
	hardMaxRetryBackoff      = 30 * time.Second
	hardMaxTokens            = 1_000_000
	hardMaxLLMCalls          = 100
	hardMaxCheckpointTimeout = time.Minute
)

// Option configures one Agent-wide numeric or behavioral setting at initialization.
type Option func(*baseConfig)

func WithName(value string) Option {
	return func(config *baseConfig) { config.Name = value }
}

func WithDescription(value string) Option {
	return func(config *baseConfig) { config.Description = value }
}

func WithSystemMessage(value string) Option {
	return func(config *baseConfig) { config.SystemMessage = value }
}

func WithTools(values ...tool.Tool) Option {
	return func(config *baseConfig) { config.Tools = append([]tool.Tool(nil), values...) }
}

func WithTool(value tool.Tool) Option {
	return func(config *baseConfig) { config.Tools = append(config.Tools, value) }
}

// WithBeforeToolBatch replaces the synchronous whole-batch admission hooks.
// Hooks run after the model stream completes and before any tool starts.
func WithBeforeToolBatch(values ...tool.BeforeBatchHook) Option {
	return func(config *baseConfig) {
		config.BeforeToolBatch = append([]tool.BeforeBatchHook(nil), values...)
	}
}

func WithMiddlewares(values ...tool.Middleware) Option {
	return func(config *baseConfig) { config.Middlewares = append([]tool.Middleware(nil), values...) }
}

func WithMiddleware(value tool.Middleware) Option {
	return func(config *baseConfig) { config.Middlewares = append(config.Middlewares, value) }
}

// WithContextBuilder replaces the default context assembly Runnable.
func WithContextBuilder(value runnable.Runnable[contextengine.Request, contextengine.Result]) Option {
	return func(config *baseConfig) { config.ContextBuilder = value }
}

// WithCheckpointStore 为指定会话 ID 的运行配置统一检查点存储。
func WithCheckpointStore(value state.Store) Option {
	return func(config *baseConfig) { config.CheckpointStore = value }
}

// WithCheckpointTimeout bounds one durable checkpoint write. Writes detach
// from request cancellation so a completed turn can still be persisted.
func WithCheckpointTimeout(value time.Duration) Option {
	return func(config *baseConfig) { config.CheckpointTimeout = value }
}

// WithCheckpointDurability 设置检查点持久化模式的默认值（async 或 sync）。
func WithCheckpointDurability(value graph.Durability) Option {
	return func(config *baseConfig) { config.CheckpointDurability = value }
}

func WithExtraGenerateConfig(value llm.GenerateConfig) Option {
	return func(config *baseConfig) {
		config.ExtraGenerateConfig = llm.MergeGenerateConfig(value, nil)
	}
}

// WithMaxDepth sets the maximum nested tool-call depth.
func WithMaxDepth(value int) Option {
	return func(config *baseConfig) { config.MaxDepth = value }
}

// WithTimeout sets the maximum duration of one tool invocation, including retries.
func WithTimeout(value time.Duration) Option {
	return func(config *baseConfig) { config.Timeout = value }
}

// WithMaxConcurrency sets the maximum parallel tool calls per batch.
func WithMaxConcurrency(value int) Option {
	return func(config *baseConfig) { config.MaxConcurrency = value }
}

// WithToolRetries sets retries for failed read-only tools.
func WithToolRetries(value int) Option {
	return func(config *baseConfig) { config.ToolRetries = value }
}

// WithToolRetryBackoff sets the delay between automatic tool retries.
func WithToolRetryBackoff(value time.Duration) Option {
	return func(config *baseConfig) { config.ToolRetryBackoff = value }
}

// WithToolWorkers sets the instance-level cap on concurrent tool executions
// shared by all in-flight runs of this Agent; 0 keeps the default of 128,
// the hard ceiling is tool.MaxRuntimeWorkers.
func WithToolWorkers(value int) Option {
	return func(config *baseConfig) { config.ToolWorkers = value }
}

// WithMaxTokens sets the model-facing max_tokens generation limit.
func WithMaxTokens(value int) Option {
	return func(config *baseConfig) { config.MaxTokens = value }
}

// WithMaxLLMCalls sets the maximum model calls in one Agent run.
func WithMaxLLMCalls(value int) Option {
	return func(config *baseConfig) { config.MaxLLMCalls = value }
}

// WithPrompt 替换 ReAct 配方的提示模板；仅在存在工具时生效。
func WithPrompt(value string) Option {
	return func(config *baseConfig) { config.Prompt = value }
}

// WithTextReAct 开启文本 ReAct 兼容模式：请求不携带原生 Functions，
// 改为提示内列出工具文本协议并解析模型正文中的 Action/Action Input。
func WithTextReAct(value bool) Option {
	return func(config *baseConfig) { config.TextReAct = value }
}

type agentLimits struct {
	maxDepth       int
	timeout        time.Duration
	maxConcurrency int
	maxTokens      int
	maxLLMCalls    int
}

// resolveBaseConfig 是唯一的配置解析入口：默认值先于 Option 填充并
// 且只执行一次，WithToolRetryBackoff(0) 等显式零值由调用与否天然区分。
func resolveBaseConfig(config baseConfig, opts ...Option) (baseConfig, error) {
	applyConfigDefaults(&config)
	for _, opt := range opts {
		if opt != nil {
			opt(&config)
		}
	}
	if err := validateBaseConfig(config); err != nil {
		return baseConfig{}, err
	}
	return config, nil
}

func applyConfigDefaults(config *baseConfig) {
	if config.MaxDepth == 0 {
		config.MaxDepth = DefaultMaxDepth
	}
	if config.Timeout == 0 {
		config.Timeout = DefaultTimeout
	}
	if config.MaxConcurrency == 0 {
		config.MaxConcurrency = DefaultMaxConcurrency
	}
	if config.MaxTokens == 0 {
		config.MaxTokens = DefaultMaxTokens
	}
	if config.MaxLLMCalls == 0 {
		config.MaxLLMCalls = DefaultMaxLLMCalls
	}
	if config.CheckpointTimeout == 0 {
		config.CheckpointTimeout = DefaultCheckpointTimeout
	}
	// 默认值先于 Option 填充：WithToolRetryBackoff(0) 等显式零值由
	// 调用与否天然区分，不再依赖存在性标记。该函数只允许被调用一次
	// （resolveBaseConfig 内部），重复执行会覆盖显式零值。
	config.ToolRetryBackoff = DefaultRetryBackoff
}

func validateBaseConfig(config baseConfig) error {
	checks := []struct {
		name  string
		value int
		min   int
		max   int
	}{
		{name: "max depth", value: config.MaxDepth, min: 1, max: hardMaxDepth},
		{name: "max concurrency", value: config.MaxConcurrency, min: 1, max: hardMaxConcurrency},
		{name: "tool retries", value: config.ToolRetries, min: 0, max: hardMaxRetries},
		{name: "tool workers", value: config.ToolWorkers, min: 0, max: tool.MaxRuntimeWorkers},
		{name: "max tokens", value: config.MaxTokens, min: 1, max: hardMaxTokens},
		{name: "max LLM calls", value: config.MaxLLMCalls, min: 1, max: hardMaxLLMCalls},
	}
	for _, check := range checks {
		if check.value < check.min || check.value > check.max {
			return fmt.Errorf("agent %s must be between %d and %d: %d", check.name, check.min, check.max, check.value)
		}
	}
	if config.Timeout <= 0 || config.Timeout > hardMaxTimeout {
		return fmt.Errorf("agent timeout must be greater than zero and at most %s: %s", hardMaxTimeout, config.Timeout)
	}
	if config.ToolRetryBackoff < 0 || config.ToolRetryBackoff > hardMaxRetryBackoff {
		return fmt.Errorf("agent tool retry backoff must be between zero and %s: %s", hardMaxRetryBackoff, config.ToolRetryBackoff)
	}
	if config.CheckpointTimeout <= 0 || config.CheckpointTimeout > hardMaxCheckpointTimeout {
		return fmt.Errorf("agent checkpoint timeout must be greater than zero and at most %s: %s", hardMaxCheckpointTimeout, config.CheckpointTimeout)
	}
	if err := validateDurability(config.CheckpointDurability); err != nil {
		return err
	}
	return nil
}

// validateDurability 校验持久化模式；零值表示沿用默认的 async。
func validateDurability(value graph.Durability) error {
	switch value {
	case "", graph.DurabilityAsync, graph.DurabilitySync:
		return nil
	default:
		return fmt.Errorf("unsupported agent checkpoint durability %q", value)
	}
}

func limitsFromConfig(config baseConfig) agentLimits {
	return agentLimits{
		maxDepth:       config.MaxDepth,
		timeout:        config.Timeout,
		maxConcurrency: config.MaxConcurrency,
		maxTokens:      config.MaxTokens,
		maxLLMCalls:    config.MaxLLMCalls,
	}
}
