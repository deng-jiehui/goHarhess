package agent

import (
	"context"
	"errors"
	"fmt"
	"time"

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
	// Numeric execution limits can also be set with Agent initialization Options.
	MaxDepth            int
	Timeout             time.Duration
	MaxConcurrency      int
	MaxRetries          int
	RetryBackoff        time.Duration
	MaxTokens           int
	MaxLLMCalls         int
	ExtraGenerateConfig llm.GenerateConfig
}

// base provides common model, tool and extension behavior to concrete Agents.
type base struct {
	name                string
	description         string
	systemMessage       string
	modelNode           *llm.ChatModelNode
	tools               map[string]schema.FunctionDefinition
	toolOrder           []string
	toolNode            *tool.ToolsNode
	contextBuilder      runnable.Runnable[contextengine.Request, contextengine.Result]
	checkpointStore     state.Store
	checkpointTimeout   time.Duration
	limits              agentLimits
	extraGenerateConfig llm.GenerateConfig
}

// newBase validates shared configuration and registers all configured tools.
func newBase(cfg BaseConfig, opts ...Option) (*base, error) {
	resolved, err := resolveBaseConfig(cfg, opts...)
	if err != nil {
		return nil, fmt.Errorf("configure agent %q: %w", cfg.Name, err)
	}
	cfg = resolved
	if cfg.ContextBuilder == nil {
		cfg.ContextBuilder, err = contextengine.NewContextBuilder(contextengine.Config{})
		if err != nil {
			return nil, fmt.Errorf("configure agent %q context builder: %w", cfg.Name, err)
		}
	}
	var modelNode *llm.ChatModelNode
	if cfg.Model != nil {
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
			MaxRetries:     cfg.MaxRetries,
			RetryBackoff:   cfg.RetryBackoff,
		})
		if err != nil {
			return nil, fmt.Errorf("configure agent %q tools node: %w", cfg.Name, err)
		}
	}
	b := &base{
		name:                cfg.Name,
		description:         cfg.Description,
		systemMessage:       cfg.SystemMessage,
		modelNode:           modelNode,
		tools:               map[string]schema.FunctionDefinition{},
		toolNode:            toolsNode,
		contextBuilder:      cfg.ContextBuilder,
		checkpointStore:     cfg.CheckpointStore,
		checkpointTimeout:   cfg.CheckpointTimeout,
		limits:              limitsFromConfig(cfg),
		extraGenerateConfig: cfg.ExtraGenerateConfig,
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

func (b *base) prepareMessages(messages []schema.Message, opts *RunOptions) []schema.Message {
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

func (b *base) withSystemMessage(messages []schema.Message) []schema.Message {
	prepared := schema.CloneMessages(messages)
	if b.systemMessage == "" {
		return prepared
	}
	if len(prepared) == 0 || prepared[0].Role != schema.RoleSystem {
		return append([]schema.Message{schema.SystemMessage(b.systemMessage)}, prepared...)
	}
	prepared[0].ContentBlocks = append([]schema.ContentBlock{schema.NewTextBlock(b.systemMessage + "\n\n")}, prepared[0].ContentBlocks...)
	return prepared
}

func (b *base) withAgentName(messages []schema.Message) []schema.Message {
	named := schema.CloneMessages(messages)
	if b.name == "" {
		return named
	}
	for i := range named {
		named[i] = setMessageAgentName(named[i], b.name)
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
	globalOptions := append([]runnable.Option(nil), opts.GlobalOptions...)
	globalPlan, err := runnable.NewOptionPlan(globalOptions...)
	if err != nil {
		return llm.Request{}, fmt.Errorf("resolve context builder options: %w", err)
	}
	builder := b.contextBuilder
	builderCtx := runnable.WithOptionPlan(ctx, globalPlan)
	assembled, err := builder.Invoke(builderCtx, contextengine.Request{
		Input:                schema.CloneMessages(messages),
		MaxContextTokens:     opts.ContextWindowTokens,
		ReservedOutputTokens: opts.MaxTokens,
	}, globalPlan.ForwardOptionsFor(runnable.OptionScopeGlobal)...)
	if err != nil {
		return llm.Request{}, fmt.Errorf("assemble LLM context: %w", err)
	}
	messages = assembled.Messages
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
	return llm.Request{Messages: schema.CloneMessages(messages), Options: chatOptions}, nil
}
