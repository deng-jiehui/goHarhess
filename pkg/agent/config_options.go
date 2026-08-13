package agent

import (
	"fmt"
	"time"

	"goharness/pkg/callback"
	"goharness/pkg/contextengine"
	"goharness/pkg/llm"
	"goharness/pkg/runnable"
	"goharness/pkg/state"
	"goharness/pkg/tool"
)

const (
	DefaultMaxDepth          = 16
	DefaultTimeout           = 30 * time.Second
	DefaultMaxConcurrency    = 8
	DefaultMaxRetries        = 0
	DefaultRetryBackoff      = 100 * time.Millisecond
	DefaultMaxTokens         = 4096
	DefaultMaxLLMCalls       = 20
	DefaultStreamBufferSize  = 16
	DefaultCheckpointTimeout = 5 * time.Second

	hardMaxDepth             = 32
	hardMaxTimeout           = 5 * time.Minute
	hardMaxConcurrency       = 64
	hardMaxRetries           = 10
	hardMaxRetryBackoff      = 30 * time.Second
	hardMaxTokens            = 1_000_000
	hardMaxLLMCalls          = 100
	hardMaxStreamBuffer      = 256
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

func WithModel(value llm.Model) Option {
	return func(config *BaseConfig) { config.Model = value }
}

func WithTools(values ...tool.BaseTool) Option {
	return func(config *BaseConfig) { config.Tools = append([]tool.BaseTool(nil), values...) }
}

func WithTool(value tool.BaseTool) Option {
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

// WithCallbacks replaces the Agent-wide synchronous lifecycle Handlers.
func WithCallbacks(values ...callback.Handler) Option {
	return func(config *BaseConfig) { config.Callbacks = append([]callback.Handler(nil), values...) }
}

// WithCallback appends one Agent-wide synchronous lifecycle Handler.
func WithCallback(value callback.Handler) Option {
	return func(config *BaseConfig) { config.Callbacks = append(config.Callbacks, value) }
}

// WithCallbackErrorSinks replaces Agent-wide synchronous callback error sinks.
func WithCallbackErrorSinks(values ...callback.ErrorSink) Option {
	return func(config *BaseConfig) {
		config.CallbackErrorSinks = append([]callback.ErrorSink(nil), values...)
	}
}

// WithCallbackErrorSink appends one Agent-wide callback error sink.
func WithCallbackErrorSink(value callback.ErrorSink) Option {
	return func(config *BaseConfig) { config.CallbackErrorSinks = append(config.CallbackErrorSinks, value) }
}

// WithCallbackConfig configures synchronous Handler deadlines and Chunk filtering.
func WithCallbackConfig(value callback.Config) Option {
	return func(config *BaseConfig) { config.CallbackConfig = value }
}

// WithContextEngine replaces the default context assembler.
func WithContextEngine(value runnable.Runnable[contextengine.Request, contextengine.Result]) Option {
	return func(config *BaseConfig) {
		config.ContextEngine = value
		if config.ContextBuilder == nil {
			config.ContextBuilder = value
		}
	}
}

// WithContextBuilder replaces the default context assembly Runnable with a
// plain function/Lambda. The function receives the complete provider-neutral
// request and returns the messages to send to the model.
func WithContextBuilder(value contextengine.ContextBuilderFunc) Option {
	return func(config *BaseConfig) { config.ContextBuilder = value }
}

// WithContextBuilderNode replaces the context node with any typed Runnable.
// Use this when the custom builder needs invocation options or richer state.
func WithContextBuilderNode(value runnable.Runnable[contextengine.Request, contextengine.Result]) Option {
	return func(config *BaseConfig) { config.ContextBuilder = value }
}

// WithCheckpointStore enables durable conversation state for runs with an ID.
func WithCheckpointStore(value state.Store) Option {
	return func(config *BaseConfig) { config.CheckpointStore = value }
}

// WithCheckpointTimeout bounds one durable checkpoint write. Writes detach
// from request cancellation so a completed turn can still be persisted.
func WithCheckpointTimeout(value time.Duration) Option {
	return func(config *BaseConfig) { config.CheckpointTimeout = value }
}

// WithBreakpointers replaces the Agent-wide durable breakpoint policies.
func WithBreakpointers(values ...Breakpointer) Option {
	return func(config *BaseConfig) { config.Breakpointers = append([]Breakpointer(nil), values...) }
}

// WithBreakpointer appends one Agent-wide durable breakpoint policy.
func WithBreakpointer(value Breakpointer) Option {
	return func(config *BaseConfig) { config.Breakpointers = append(config.Breakpointers, value) }
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
func WithMaxRetries(value int) Option {
	return func(config *BaseConfig) { config.MaxRetries = value }
}

// WithRetryBackoff sets the delay between automatic retries.
func WithRetryBackoff(value time.Duration) Option {
	return func(config *BaseConfig) { config.RetryBackoff = value }
}

// WithMaxTokens sets the model-facing max_tokens generation limit.
func WithMaxTokens(value int) Option {
	return func(config *BaseConfig) { config.MaxTokens = value }
}

// WithStreamBufferSize sets bounded asynchronous buffering for model and Agent streams.
func WithStreamBufferSize(value int) Option {
	return func(config *BaseConfig) { config.StreamBufferSize = value }
}

// WithMaxLLMCalls sets the maximum model calls in one Agent run.
func WithMaxLLMCalls(value int) Option {
	return func(config *BaseConfig) { config.MaxLLMCalls = value }
}

type agentLimits struct {
	maxDepth         int
	timeout          time.Duration
	maxConcurrency   int
	maxRetries       int
	retryBackoff     time.Duration
	maxTokens        int
	maxLLMCalls      int
	streamBufferSize int
}

func resolveBaseConfig(config BaseConfig, opts ...Option) (BaseConfig, error) {
	applyConfigDefaults(&config)
	for _, opt := range opts {
		if opt != nil {
			opt(&config)
		}
	}
	if err := validateBaseConfig(config); err != nil {
		return BaseConfig{}, err
	}
	return config, nil
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
	if config.RetryBackoff == 0 {
		config.RetryBackoff = DefaultRetryBackoff
	}
	if config.MaxTokens == 0 {
		config.MaxTokens = DefaultMaxTokens
	}
	if config.MaxLLMCalls == 0 {
		config.MaxLLMCalls = DefaultMaxLLMCalls
	}
	if config.StreamBufferSize == 0 {
		config.StreamBufferSize = DefaultStreamBufferSize
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
		{name: "max retries", value: config.MaxRetries, min: 0, max: hardMaxRetries},
		{name: "max tokens", value: config.MaxTokens, min: 1, max: hardMaxTokens},
		{name: "max LLM calls", value: config.MaxLLMCalls, min: 1, max: hardMaxLLMCalls},
		{name: "stream buffer size", value: config.StreamBufferSize, min: 1, max: hardMaxStreamBuffer},
	}
	for _, check := range checks {
		if check.value < check.min || check.value > check.max {
			return fmt.Errorf("agent %s must be between %d and %d: %d", check.name, check.min, check.max, check.value)
		}
	}
	if config.Timeout <= 0 || config.Timeout > hardMaxTimeout {
		return fmt.Errorf("agent timeout must be greater than zero and at most %s: %s", hardMaxTimeout, config.Timeout)
	}
	if config.RetryBackoff < 0 || config.RetryBackoff > hardMaxRetryBackoff {
		return fmt.Errorf("agent retry backoff must be between zero and %s: %s", hardMaxRetryBackoff, config.RetryBackoff)
	}
	if config.CheckpointTimeout <= 0 || config.CheckpointTimeout > hardMaxCheckpointTimeout {
		return fmt.Errorf("agent checkpoint timeout must be greater than zero and at most %s: %s", hardMaxCheckpointTimeout, config.CheckpointTimeout)
	}
	return nil
}

func limitsFromConfig(config BaseConfig) agentLimits {
	return agentLimits{
		maxDepth:         config.MaxDepth,
		timeout:          config.Timeout,
		maxConcurrency:   config.MaxConcurrency,
		maxRetries:       config.MaxRetries,
		retryBackoff:     config.RetryBackoff,
		maxTokens:        config.MaxTokens,
		maxLLMCalls:      config.MaxLLMCalls,
		streamBufferSize: config.StreamBufferSize,
	}
}
