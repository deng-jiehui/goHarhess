package agent

import (
	"context"
	"errors"
	"fmt"
	"time"

	"goharness/internal/toolruntime"
	"goharness/pkg/callback"
	"goharness/pkg/contextengine"
	"goharness/pkg/llm"
	"goharness/pkg/runnable"
	"goharness/pkg/schema"
	"goharness/pkg/state"
	"goharness/pkg/tool"
)

var ErrMissingModel = errors.New("agent requires an llm model")

// BaseConfig contains behavior shared by all built-in Agent implementations.
type BaseConfig struct {
	Name          string
	Description   string
	SystemMessage string
	Model         llm.Model
	Tools         []tool.BaseTool
	// BeforeToolBatch runs synchronously after a complete tool-call batch is
	// parsed and before any tool starts.
	BeforeToolBatch []tool.BeforeBatchHook
	// Middlewares run in order inside the fixed system runtime.
	Middlewares []tool.Middleware
	// Callbacks observe Agent, LLM, retriever and tool lifecycles synchronously.
	Callbacks      []callback.Handler
	CallbackConfig callback.Config
	// CallbackErrorSinks receive isolated Handler failures synchronously.
	CallbackErrorSinks []callback.ErrorSink
	// ContextEngine governs role ordering, prompt-cache prefixes and token budgets.
	ContextEngine runnable.Runnable[contextengine.Request, contextengine.Result]
	// ContextBuilder is the standard Runnable node used to assemble model
	// context. When set, it takes precedence over ContextEngine.
	ContextBuilder runnable.Runnable[contextengine.Request, contextengine.Result]
	// CheckpointStore enables durable conversations when a run supplies a conversation ID.
	CheckpointStore   state.Store
	CheckpointTimeout time.Duration
	// Numeric execution limits can also be set with Agent initialization Options.
	MaxDepth            int
	Timeout             time.Duration
	MaxConcurrency      int
	MaxRetries          int
	RetryBackoff        time.Duration
	MaxTokens           int
	MaxLLMCalls         int
	StreamBufferSize    int
	ExtraGenerateConfig llm.GenerateConfig
}

// Base provides common model, tool and extension behavior to concrete Agents.
type Base struct {
	name                string
	description         string
	systemMessage       string
	model               llm.Model
	tools               map[string]*toolruntime.Binding
	toolOrder           []string
	toolRuntime         *toolruntime.Runtime
	beforeToolBatch     []tool.BeforeBatchHook
	callbacks           []callback.Handler
	callbackConfig      callback.Config
	callbackErrorSinks  []callback.ErrorSink
	contextEngine       runnable.Runnable[contextengine.Request, contextengine.Result]
	contextBuilder      runnable.Runnable[contextengine.Request, contextengine.Result]
	checkpointStore     state.Store
	checkpointTimeout   time.Duration
	limits              agentLimits
	extraGenerateConfig llm.GenerateConfig
}

// NewBase validates shared configuration and registers all configured tools.
func NewBase(cfg BaseConfig, opts ...Option) (*Base, error) {
	resolved, err := resolveBaseConfig(cfg, opts...)
	if err != nil {
		return nil, fmt.Errorf("configure agent %q: %w", cfg.Name, err)
	}
	cfg = resolved
	if _, err := callback.New(cfg.Callbacks, cfg.CallbackConfig, cfg.CallbackErrorSinks...); err != nil {
		return nil, fmt.Errorf("configure agent %q callbacks: %w", cfg.Name, err)
	}
	if cfg.ContextBuilder == nil {
		cfg.ContextBuilder = cfg.ContextEngine
	}
	if cfg.ContextBuilder == nil {
		cfg.ContextBuilder = contextengine.NewDefaultContextBuilder()
	}
	runtime, err := toolruntime.New(cfg.Middlewares, toolruntime.Config{
		MaxDepth:       cfg.MaxDepth,
		Timeout:        cfg.Timeout,
		MaxConcurrency: cfg.MaxConcurrency,
		MaxRetries:     cfg.MaxRetries,
		RetryBackoff:   cfg.RetryBackoff,
	})
	if err != nil {
		return nil, fmt.Errorf("configure agent %q tool extensions: %w", cfg.Name, err)
	}
	b := &Base{
		name:                cfg.Name,
		description:         cfg.Description,
		systemMessage:       cfg.SystemMessage,
		model:               cfg.Model,
		tools:               map[string]*toolruntime.Binding{},
		toolRuntime:         runtime,
		beforeToolBatch:     append([]tool.BeforeBatchHook(nil), cfg.BeforeToolBatch...),
		callbacks:           append([]callback.Handler(nil), cfg.Callbacks...),
		callbackConfig:      cfg.CallbackConfig,
		callbackErrorSinks:  append([]callback.ErrorSink(nil), cfg.CallbackErrorSinks...),
		contextEngine:       cfg.ContextEngine,
		contextBuilder:      cfg.ContextBuilder,
		checkpointStore:     cfg.CheckpointStore,
		checkpointTimeout:   cfg.CheckpointTimeout,
		limits:              limitsFromConfig(cfg),
		extraGenerateConfig: cfg.ExtraGenerateConfig,
	}
	for _, t := range cfg.Tools {
		if err := b.AddTool(t); err != nil {
			return nil, err
		}
	}
	return b, nil
}

func (b *Base) Name() string {
	return b.name
}

func (b *Base) Description() string {
	return b.description
}

// AddTool validates a tool and registers its policy with the internal runtime.
func (b *Base) AddTool(t tool.BaseTool) error {
	if t == nil {
		return errors.New("tool cannot be nil")
	}
	binding, err := b.toolRuntime.Register(t)
	if err != nil {
		return fmt.Errorf("register tool in system runtime: %w", err)
	}
	spec := binding.Spec()
	name := spec.Name
	if _, exists := b.tools[name]; !exists {
		b.toolOrder = append(b.toolOrder, name)
	}
	b.tools[name] = binding
	return nil
}

func (b *Base) prepareMessages(messages []schema.Message, opts *RunOptions) []schema.Message {
	prepared := schema.CloneMessages(messages)
	if opts.Lang == "" {
		if schema.HasChinese(prepared) {
			opts.Lang = "zh"
		} else {
			opts.Lang = "en"
		}
	}
	return prepared
}

func (b *Base) withSystemMessage(messages []schema.Message) []schema.Message {
	prepared := schema.CloneMessages(messages)
	if b.systemMessage == "" {
		return prepared
	}
	if len(prepared) == 0 || prepared[0].Role != schema.RoleSystem {
		return append([]schema.Message{schema.SystemMessage(b.systemMessage)}, prepared...)
	}
	prepared[0].Content = prepared[0].Content.PrependText(b.systemMessage + "\n\n")
	return prepared
}

func (b *Base) withAgentName(messages []schema.Message) []schema.Message {
	named := schema.CloneMessages(messages)
	if b.name == "" {
		return named
	}
	for i := range named {
		if named[i].Name == "" {
			named[i].Name = b.name
		}
	}
	return named
}

func (b *Base) toolDefinitions() []schema.FunctionDefinition {
	definitions := make([]schema.FunctionDefinition, 0, len(b.toolOrder))
	for _, name := range b.toolOrder {
		definitions = append(definitions, b.tools[name].Spec().Definition())
	}
	return definitions
}

func (b *Base) callLLM(ctx context.Context, messages []schema.Message, functions []schema.FunctionDefinition, opts RunOptions) (runnable.Stream[llm.Event], error) {
	if b.model == nil {
		return nil, ErrMissingModel
	}
	messages = b.withSystemMessage(messages)
	globalOptions := append([]runnable.Option(nil), opts.GlobalOptions...)
	nodeOptions := append([]runnable.Option(nil), opts.NodeOptions...)
	globalPlan, err := runnable.NewOptionPlan(globalOptions...)
	if err != nil {
		return nil, fmt.Errorf("resolve context builder options: %w", err)
	}
	builder := b.contextBuilder
	if builder == nil {
		builder = b.contextEngine
	}
	builderCtx := runnable.WithOptionPlan(ctx, globalPlan)
	assembled, err := builder.Invoke(builderCtx, contextengine.Request{
		Input:                schema.CloneMessages(messages),
		MaxContextTokens:     opts.ContextWindowTokens,
		ReservedOutputTokens: opts.MaxTokens,
	}, globalPlan.ForwardOptionsFor(runnable.OptionScopeGlobal)...)
	if err != nil {
		return nil, fmt.Errorf("assemble LLM context: %w", err)
	}
	messages = assembled.Messages
	llmCtx, operation, dispatcher, metrics := b.beginLLMCall(ctx, messages, functions, opts, assembled.Stats)
	nodePlan, err := runnable.NewOptionPlan(nodeOptions...)
	if err != nil {
		return nil, fmt.Errorf("resolve model options: %w", err)
	}
	llmCtx = runnable.WithOptionPlan(llmCtx, nodePlan)
	cfg := llm.MergeGenerateConfig(b.extraGenerateConfig, nil)
	if opts.Seed != nil {
		cfg["seed"] = *opts.Seed
	}
	if opts.MaxTokens > 0 {
		cfg["max_tokens"] = opts.MaxTokens
	}
	chatOptions := llm.ChatOptions{
		Functions:           functions,
		DeltaStream:         opts.DeltaStream,
		ExtraGenerateConfig: cfg,
	}
	stream, err := b.model.Stream(llmCtx, llm.Request{Messages: schema.CloneMessages(messages), Options: chatOptions}, nodePlan.ForwardOptionsFor(runnable.OptionScopeNode)...)
	if err != nil {
		if dispatcher != nil {
			_ = dispatcher.Dispatch(llmCtx, callback.ErrorEvent(callback.EventLLMError, operation, err, metrics))
		}
		return nil, err
	}
	if stream == nil {
		err := errors.New("llm model returned a nil stream")
		if dispatcher != nil {
			_ = dispatcher.Dispatch(llmCtx, callback.ErrorEvent(callback.EventLLMError, operation, err, metrics))
		}
		return nil, err
	}
	observed := observeLLMStream(llmCtx, stream, dispatcher, operation, metrics)
	return runnable.Buffer(llmCtx, observed, opts.StreamBufferSize), nil
}

func (b *Base) callTool(
	ctx context.Context,
	executor *toolruntime.Scope,
	toolName string,
	rawArgs string,
	messages []schema.Message,
	opts RunOptions,
) (schema.Content, error) {
	binding, ok := b.tools[toolName]
	if !ok {
		return schema.TextContent(fmt.Sprintf("Tool %s does not exist.", toolName)), nil
	}
	args, err := tool.ParseArgsStrict(rawArgs)
	if err != nil {
		return schema.TextContent(""), fmt.Errorf("parse arguments for tool %q: %w", toolName, err)
	}

	input := tool.CallInput{
		RawArgs:  rawArgs,
		Args:     args,
		Messages: schema.CloneMessages(messages),
		Metadata: callback.CloneMetadata(opts.Metadata),
	}
	spec := binding.Spec()
	if spec.FileAccess {
		input.Files = schema.ExtractFiles(messages, true)
	}

	result, err := executor.Execute(ctx, binding, input, opts.NodeOptions...)
	if err != nil {
		return schema.TextContent(""), err
	}
	content, err := result.ToContent()
	if err != nil {
		return schema.TextContent(""), err
	}
	if result.IsError {
		content = schema.TextContent(fmt.Sprintf("Tool %q reported an error: %s", toolName, content.String()))
	}
	return content, nil
}
