package agent

import (
	"context"

	"goharness/pkg/schema"
)

type KnowledgeProvider interface {
	Knowledge(ctx context.Context, messages []schema.Message, lang string) (string, error)
}

type AssistantConfig struct {
	BaseConfig
	KnowledgeProvider KnowledgeProvider
}

type Assistant struct {
	*FnCallAgent
	knowledgeProvider KnowledgeProvider
}

func NewAssistant(cfg AssistantConfig) (*Assistant, error) {
	fn, err := NewFnCallAgent(cfg.BaseConfig)
	if err != nil {
		return nil, err
	}
	return &Assistant{FnCallAgent: fn, knowledgeProvider: cfg.KnowledgeProvider}, nil
}

func (a *Assistant) Run(ctx context.Context, messages []schema.Message, opts ...RunOption) (Stream, error) {
	runOpts := resolveRunOptions(opts...)
	prepared := a.prepareMessages(messages, &runOpts)

	knowledge := runOpts.Knowledge
	if knowledge == "" && a.knowledgeProvider != nil {
		var err error
		knowledge, err = a.knowledgeProvider.Knowledge(ctx, prepared, runOpts.Lang)
		if err != nil {
			return nil, err
		}
	}
	if knowledge != "" {
		prepared = prependKnowledge(prepared, runOpts.Lang, knowledge)
	}
	return a.runPrepared(ctx, prepared, runOpts)
}

func prependKnowledge(messages []schema.Message, lang string, knowledge string) []schema.Message {
	prepared := schema.CloneMessages(messages)
	title := "# Knowledge Base"
	if lang == "zh" {
		title = "# 知识库"
	}
	prompt := title + "\n\n" + knowledge

	if len(prepared) > 0 && prepared[0].Role == schema.RoleSystem {
		prepared[0].Content = prepared[0].Content.AppendText("\n\n" + prompt)
		return prepared
	}
	return append([]schema.Message{schema.SystemMessage(prompt)}, prepared...)
}
