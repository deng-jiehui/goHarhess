package agent

import (
	"context"
	"errors"
	"fmt"

	"goharness/pkg/llm"
	"goharness/pkg/schema"
	"goharness/pkg/tool"
)

var ErrMissingModel = errors.New("agent requires an llm model")

type BaseConfig struct {
	Name                string
	Description         string
	SystemMessage       string
	Model               llm.ChatModel
	Tools               []tool.Tool
	ExtraGenerateConfig llm.GenerateConfig
}

type Base struct {
	name                string
	description         string
	systemMessage       string
	model               llm.ChatModel
	tools               map[string]tool.Tool
	toolOrder           []string
	extraGenerateConfig llm.GenerateConfig
}

func NewBase(cfg BaseConfig) (*Base, error) {
	b := &Base{
		name:                cfg.Name,
		description:         cfg.Description,
		systemMessage:       cfg.SystemMessage,
		model:               cfg.Model,
		tools:               map[string]tool.Tool{},
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

func (b *Base) AddTool(t tool.Tool) error {
	if t == nil {
		return errors.New("tool cannot be nil")
	}
	name := t.Spec().Name
	if name == "" {
		return errors.New("tool name cannot be empty")
	}
	if _, exists := b.tools[name]; !exists {
		b.toolOrder = append(b.toolOrder, name)
	}
	b.tools[name] = t
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

func (b *Base) callLLM(ctx context.Context, messages []schema.Message, functions []schema.FunctionDefinition, opts RunOptions) (llm.Stream, error) {
	if b.model == nil {
		return nil, ErrMissingModel
	}
	cfg := llm.MergeGenerateConfig(b.extraGenerateConfig, nil)
	if opts.Lang != "" {
		cfg["lang"] = opts.Lang
	}
	if opts.Seed != nil {
		cfg["seed"] = *opts.Seed
	}
	return b.model.Chat(ctx, schema.CloneMessages(messages), llm.ChatOptions{
		Functions:           functions,
		Stream:              true,
		ExtraGenerateConfig: cfg,
	})
}

func (b *Base) callTool(ctx context.Context, toolName string, rawArgs string, messages []schema.Message, opts RunOptions) (schema.Content, error) {
	t, ok := b.tools[toolName]
	if !ok {
		return schema.TextContent(fmt.Sprintf("Tool %s does not exist.", toolName)), nil
	}

	input := tool.CallInput{
		RawArgs:  rawArgs,
		Args:     tool.ParseArgs(rawArgs),
		Messages: schema.CloneMessages(messages),
		Metadata: opts.Metadata,
	}
	if t.Spec().FileAccess {
		input.Files = schema.ExtractFiles(messages, true)
	}

	result, err := t.Call(ctx, input)
	if err != nil {
		return schema.TextContent(""), err
	}
	return result.ToContent()
}
