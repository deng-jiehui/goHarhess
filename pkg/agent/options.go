package agent

import (
	"context"
	"errors"
	"time"

	"goharness/pkg/control"
	"goharness/pkg/graph"
	"goharness/pkg/runnable"
	"goharness/pkg/tool"
)

type resolvedRunOptionsKey struct{}

func withResolvedRunOptions(ctx context.Context, opts RunOptions) context.Context {
	return context.WithValue(ctx, resolvedRunOptionsKey{}, opts)
}

func graphNodeOptions(options RunOptions) []runnable.Option {
	values := append([]runnable.Option(nil), options.GraphOptions...)
	values = append(values, options.NodeOptions...)
	return values
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

type RunOptions struct {
	Lang                string
	Seed                *int64
	MaxLLMCalls         int
	MaxParallelTools    int
	ToolCallTimeout     time.Duration
	MaxTokens           int
	DeltaStream         bool
	Knowledge           string
	Metadata            map[string]any
	GlobalOptions       []runnable.Option
	NodeOptions         []runnable.Option
	ConversationID      string
	ContextWindowTokens int
	StateValues         map[string]any
	CheckpointBypass    bool
	GraphOptions        []runnable.Option
	resume              bool
	agentKind           AgentKind
	session             *conversationSession
}

type RunOption func(*RunOptions)

const runOptionsValueKey = "agent.run_options"

func (option RunOption) Apply(values *runnable.OptionSet) error {
	if option == nil {
		return errors.New("agent run option is nil")
	}
	if values == nil {
		return errors.New("runnable option set is nil")
	}
	if values.Values == nil {
		values.Values = map[string]any{}
	}
	options, ok := values.Values[runOptionsValueKey].([]RunOption)
	if _, exists := values.Values[runOptionsValueKey]; exists && !ok {
		return errors.New("agent run option set has an invalid value")
	}
	values.Values[runOptionsValueKey] = append(options, option)
	return nil
}

func defaultRunOptions(limits agentLimits) RunOptions {
	return RunOptions{
		MaxLLMCalls:      limits.maxLLMCalls,
		MaxParallelTools: limits.maxConcurrency,
		ToolCallTimeout:  limits.timeout,
		MaxTokens:        limits.maxTokens,
		Metadata:         map[string]any{},
		StateValues:      map[string]any{},
	}
}

func resolveRunOptions(limits agentLimits, opts ...RunOption) RunOptions {
	ro := defaultRunOptions(limits)
	for _, opt := range opts {
		if opt != nil {
			opt(&ro)
		}
	}
	if ro.MaxLLMCalls <= 0 || ro.MaxLLMCalls > limits.maxLLMCalls {
		ro.MaxLLMCalls = limits.maxLLMCalls
	}
	if ro.MaxParallelTools <= 0 || ro.MaxParallelTools > limits.maxConcurrency {
		ro.MaxParallelTools = limits.maxConcurrency
	}
	if ro.ToolCallTimeout <= 0 || ro.ToolCallTimeout > limits.timeout {
		ro.ToolCallTimeout = limits.timeout
	}
	if ro.ContextWindowTokens < 0 {
		ro.ContextWindowTokens = 0
	}
	if ro.Metadata == nil {
		ro.Metadata = map[string]any{}
	}
	if ro.StateValues == nil {
		ro.StateValues = map[string]any{}
	}
	return ro
}

func WithLang(lang string) RunOption {
	return func(opts *RunOptions) {
		opts.Lang = lang
	}
}

func WithSeed(seed int64) RunOption {
	return func(opts *RunOptions) {
		opts.Seed = &seed
	}
}

// WithRunMaxLLMCalls tightens the Agent-wide model-call limit for one run.
// The Agent-wide default is configured with WithMaxLLMCalls.
func WithRunMaxLLMCalls(max int) RunOption {
	return func(opts *RunOptions) {
		opts.MaxLLMCalls = max
	}
}

func WithMaxParallelTools(max int) RunOption {
	return func(opts *RunOptions) {
		opts.MaxParallelTools = max
	}
}

func WithToolCallTimeout(timeout time.Duration) RunOption {
	return func(opts *RunOptions) {
		opts.ToolCallTimeout = timeout
	}
}

func WithDeltaStream(enabled bool) RunOption {
	return func(opts *RunOptions) {
		opts.DeltaStream = enabled
	}
}

func WithKnowledge(knowledge string) RunOption {
	return func(opts *RunOptions) {
		opts.Knowledge = knowledge
	}
}

func WithMetadata(key string, value any) RunOption {
	return func(opts *RunOptions) {
		opts.Metadata[key] = value
	}
}

// WithConversationID loads and saves a versioned conversation checkpoint.
func WithConversationID(value string) RunOption {
	return func(opts *RunOptions) { opts.ConversationID = value }
}

// WithCheckpoint enables Graph-owned execution interruption and recovery.
// Its key is independent from WithConversationID and is owned by the caller.
func WithCheckpoint(store control.CheckpointStore, key string) RunOption {
	return func(opts *RunOptions) {
		opts.GraphOptions = append(opts.GraphOptions, graph.WithCheckpoint(store, key))
	}
}

// WithContextWindowTokens overrides the configured context window for one run.
func WithContextWindowTokens(value int) RunOption {
	return func(opts *RunOptions) { opts.ContextWindowTokens = value }
}

// WithStateValue updates one durable value on the next successful checkpoint.
func WithStateValue(key string, value any) RunOption {
	return func(opts *RunOptions) { opts.StateValues[key] = value }
}

// WithStateValues updates durable values on the next successful checkpoint.
func WithStateValues(values map[string]any) RunOption {
	return func(opts *RunOptions) {
		for key, value := range values {
			opts.StateValues[key] = value
		}
	}
}

// WithCheckpointBypass prevents nested Agents from loading or saving state.
func WithCheckpointBypass() RunOption {
	return func(opts *RunOptions) { opts.CheckpointBypass = true }
}
