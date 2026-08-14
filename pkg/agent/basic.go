package agent

import (
	"context"
	"errors"

	"goharness/internal/agentrecipe"
	"goharness/pkg/llm"
	"goharness/pkg/runnable"
	"goharness/pkg/schema"
)

type BasicAgent struct {
	*base
	recipe *agentrecipe.Recipe
}

func NewBasicAgent(cfg BaseConfig, opts ...Option) (*BasicAgent, error) {
	base, err := newBase(cfg, opts...)
	if err != nil {
		return nil, err
	}
	if len(base.toolOrder) > 0 {
		return nil, errors.New("BasicAgent does not execute tools; use agent.New or ReactAgent")
	}
	a := &BasicAgent{base: base}
	recipeName := string(AgentKindBasic)
	recipe, err := agentrecipe.NewToolRecipe(recipeName, agentrecipe.Hooks{
		Model: a.modelNode,
		PrepareModel: func(ctx context.Context, messages []schema.Message) (llm.Request, error) {
			runOpts, _ := resolvedRunOptionsFromContext(ctx)
			return a.prepareLLMRequest(ctx, messages, nil, runOpts)
		},
		NameMessages: a.withAgentName,
	}, false)
	if err != nil {
		return nil, err
	}
	a.recipe = recipe
	return a, nil
}

func (a *BasicAgent) stream(ctx context.Context, messages []schema.Message, opts ...RunOption) (*runnable.StreamReader[[]schema.Message], error) {
	runOpts := resolveRunOptions(a.limits, opts...)
	runOpts.agentKind = AgentKindBasic
	loaded, session, err := a.loadConversation(ctx, messages, &runOpts)
	if err != nil {
		return nil, err
	}
	ctx = bindRunContext(ctx, session)
	prepared := a.prepareMessages(loaded, &runOpts)
	runCtx, lifecycle, err := a.beginRun(ctx, prepared, runOpts)
	if err != nil {
		return nil, err
	}
	lifecycle.session = session
	runOpts.session = session
	runCtx = withResolvedRunOptions(runCtx, runOpts)
	runCtx = a.bindModelStream(runCtx)
	runCtx = withToolExecutionLimits(runCtx, runOpts)
	runCtx = withRecipeCompletion(runCtx, session)
	runCtx = agentrecipe.WithRunConfig(runCtx, agentrecipe.RunConfig{
		MaxLLMCalls: 1, Input: conversationInput(session, messages),
	})
	var dataStream *runnable.StreamReader[[]schema.Message]
	if runOpts.resume {
		dataStream, err = a.recipe.ResumeStream(runCtx, graphNodeOptions(runOpts)...)
	} else {
		dataStream, err = a.recipe.Stream(runCtx, prepared, graphNodeOptions(runOpts)...)
	}
	if err != nil {
		lifecycle.fail(err)
		return nil, err
	}
	return lifecycle.wrap(dataStream)
}
