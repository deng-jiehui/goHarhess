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
type Option func(*BaseConfig)

func WithName(value string) Option {
	return func(config *BaseConfig) { config.Name = value }
}

func WithDescription(value string) Option {
	return func(config *BaseConfig) { config.Description = value }
}

func WithSystemMessage(value string) Option {
	return func(config *BaseConfig) { config.SystemMessage = value }
}

func WithModel(value llm.ChatModel) Option {
	return func(config *BaseConfig) { config.Model = value }
}

func WithTools(values ...tool.Tool) Option {
	return func(config *BaseConfig) { config.Tools = append([]tool.Tool(nil), values...) }
}

func WithTool(value tool.Tool) Option {
	return func(config *BaseConfig) { config.Tools = append(config.Tools, value) }
}

// WithBeforeToolBatch replaces the synchronous whole-batch admission hooks.
// Hooks run after the model stream completes and before any tool starts.
func WithBeforeToolBatch(values ...tool.BeforeBatchHook) Option {
	return func(config *BaseConfig) {
		config.BeforeToolBatch = append([]tool.BeforeBatchHook(nil), values...)
	}
}

func WithMiddlewares(values ...tool.Middleware) Option {
	return func(config *BaseConfig) { config.Middlewares = append([]tool.Middleware(nil), values...) }
}

func WithMiddleware(value tool.Middleware) Option {
	return func(config *BaseConfig) { config.Middlewares = append(config.Middlewares, value) }
}

// WithContextBuilder replaces the default context assembly Runnable.
func WithContextBuilder(value runnable.Runnable[contextengine.Request, contextengine.Result]) Option {
	return func(config *BaseConfig) { config.ContextBuilder = value }
}

// WithCheckpointStore 为指定会话 ID 的运行配置统一检查点存储。
func WithCheckpointStore(value state.Store) Option {
	return func(config *BaseConfig) { config.CheckpointStore = value }
}

// WithCheckpointTimeout bounds one durable checkpoint write. Writes detach
// from request cancellation so a completed turn can still be persisted.
func WithCheckpointTimeout(value time.Duration) Option {
	return func(config *BaseConfig) { config.CheckpointTimeout = value }
}

// WithCheckpointDurability 设置检查点持久化模式的默认值（async 或 sync）。
func WithCheckpointDurability(value graph.Durability) Option {
	return func(config *BaseConfig) { config.CheckpointDurability = value }
}

func WithExtraGenerateConfig(value llm.GenerateConfig) Option {
	return func(config *BaseConfig) {
		config.ExtraGenerateConfig = llm.MergeGenerateConfig(value, nil)
	}
}

// WithMaxDepth sets the maximum nested tool-call depth.
func WithMaxDepth(value int) Option {
	return func(config *BaseConfig) { config.MaxDepth = value }
}

// WithTimeout sets the maximum duration of one tool invocation, including retries.
func WithTimeout(value time.Duration) Option {
	return func(config *BaseConfig) { config.Timeout = value }
}

// WithMaxConcurrency sets the maximum parallel tool calls per batch.
func WithMaxConcurrency(value int) Option {
	return func(config *BaseConfig) { config.MaxConcurrency = value }
}

// WithMaxRetries sets retries for failed read-only tools.
// Deprecated: 该选项只作用于工具重试，请改用 WithToolRetries。
func WithMaxRetries(value int) Option {
	return func(config *BaseConfig) { config.MaxRetries = value; config.legacyRetriesSet = true }
}

// WithRetryBackoff sets the delay between automatic retries.
// Deprecated: 该选项只作用于工具重试，请改用 WithToolRetryBackoff。
func WithRetryBackoff(value time.Duration) Option {
	return func(config *BaseConfig) { config.RetryBackoff = value; config.legacyBackoffSet = true }
}

// WithToolRetries sets retries for failed read-only tools.
func WithToolRetries(value int) Option {
	return func(config *BaseConfig) { config.ToolRetries = value; config.toolRetriesSet = true }
}

// WithToolRetryBackoff sets the delay between automatic tool retries.
func WithToolRetryBackoff(value time.Duration) Option {
	return func(config *BaseConfig) { config.ToolRetryBackoff = value; config.toolRetryBackoffSet = true }
}

// WithModelRetry wraps the configured model with llm.NewRetryingModel.
// MaxAttempts must be at least 2 to enable retrying; retryable errors and
// stream semantics follow the llm decorator.
func WithModelRetry(value llm.RetryConfig) Option {
	return func(config *BaseConfig) { config.ModelRetry = value }
}

// WithModelRateLimit wraps the configured model with llm.NewRateLimitedModel.
// The limit applies per Agent instance; wrap the underlying model manually to
// share one quota across instances.
func WithModelRateLimit(value llm.RateLimitConfig) Option {
	return func(config *BaseConfig) { config.ModelRateLimit = value }
}

// WithMaxTokens sets the model-facing max_tokens generation limit.
func WithMaxTokens(value int) Option {
	return func(config *BaseConfig) { config.MaxTokens = value }
}

// WithMaxLLMCalls sets the maximum model calls in one Agent run.
func WithMaxLLMCalls(value int) Option {
	return func(config *BaseConfig) { config.MaxLLMCalls = value }
}

type agentLimits struct {
	maxDepth       int
	timeout        time.Duration
	maxConcurrency int
	maxTokens      int
	maxLLMCalls    int
}

func resolveBaseConfig(config BaseConfig, opts ...Option) (BaseConfig, error) {
	applyConfigDefaults(&config)
	for _, opt := range opts {
		if opt != nil {
			opt(&config)
		}
	}
	if err := resolveRetryAliases(&config); err != nil {
		return BaseConfig{}, err
	}
	if err := validateBaseConfig(config); err != nil {
		return BaseConfig{}, err
	}
	return config, nil
}

// resolveRetryAliases 合并废弃字段与规范字段。存在性规则对两侧一致：
// 对应 With* 选项被调用过或字段值非零都算「已提供」；两边提供不同值
// （含显式零值与非零值）在初始化时拒绝，单侧提供时采用该值。退避默认值
// 只在两侧都未提供时填充，显式零值保留为「无重试间隔」。
func resolveRetryAliases(config *BaseConfig) error {
	newRetries := config.toolRetriesSet || config.ToolRetries != 0
	oldRetries := config.legacyRetriesSet || config.MaxRetries != 0
	switch {
	case newRetries && oldRetries && config.ToolRetries != config.MaxRetries:
		return fmt.Errorf("agent tool retries configured twice with different values: ToolRetries=%d, MaxRetries=%d", config.ToolRetries, config.MaxRetries)
	case !newRetries && oldRetries:
		config.ToolRetries = config.MaxRetries
	}
	newBackoff := config.toolRetryBackoffSet || config.ToolRetryBackoff != 0
	oldBackoff := config.legacyBackoffSet || config.RetryBackoff != 0
	switch {
	case newBackoff && oldBackoff && config.ToolRetryBackoff != config.RetryBackoff:
		return fmt.Errorf("agent tool retry backoff configured twice with different values: ToolRetryBackoff=%s, RetryBackoff=%s", config.ToolRetryBackoff, config.RetryBackoff)
	case !newBackoff && oldBackoff:
		config.ToolRetryBackoff = config.RetryBackoff
	}
	if !newBackoff && !oldBackoff {
		config.ToolRetryBackoff = DefaultRetryBackoff
	}
	return nil
}

func applyConfigDefaults(config *BaseConfig) {
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
}

func validateBaseConfig(config BaseConfig) error {
	checks := []struct {
		name  string
		value int
		min   int
		max   int
	}{
		{name: "max depth", value: config.MaxDepth, min: 1, max: hardMaxDepth},
		{name: "max concurrency", value: config.MaxConcurrency, min: 1, max: hardMaxConcurrency},
		{name: "tool retries", value: config.ToolRetries, min: 0, max: hardMaxRetries},
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
	if err := validateModelPolicy(config.ModelRetry, config.ModelRateLimit); err != nil {
		return err
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

func limitsFromConfig(config BaseConfig) agentLimits {
	return agentLimits{
		maxDepth:       config.MaxDepth,
		timeout:        config.Timeout,
		maxConcurrency: config.MaxConcurrency,
		maxTokens:      config.MaxTokens,
		maxLLMCalls:    config.MaxLLMCalls,
	}
}
