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
	*Base
	recipe *agentrecipe.Recipe
}

func NewBasicAgent(cfg BaseConfig, opts ...Option) (*BasicAgent, error) {
	base, err := NewBase(cfg, opts...)
	if err != nil {
		return nil, err
	}
	if len(base.toolOrder) > 0 {
		return nil, errors.New("BasicAgent does not execute tools; use agent.New or ReactAgent")
	}
	a := &BasicAgent{Base: base}
	recipeName := string(AgentKindBasic)
	recipe, err := agentrecipe.NewToolRecipe(recipeName, agentrecipe.Hooks{
		Name: recipeName,
		CallModel: func(ctx context.Context, messages []schema.Message) (runnable.Stream[llm.Event], error) {
			runOpts, _ := resolvedRunOptionsFromContext(ctx)
			return a.callLLM(ctx, messages, nil, runOpts)
		},
		NameMessages: a.withAgentName,
	}, false)
	if err != nil {
		return nil, err
	}
	a.recipe = recipe
	return a, nil
}

func (a *BasicAgent) stream(ctx context.Context, messages []schema.Message, opts ...RunOption) (runnable.Stream[schema.MessageEvent], error) {
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
	runCtx = withRecipeCompletion(runCtx, session)
	runCtx = agentrecipe.WithRunConfig(runCtx, agentrecipe.RunConfig{
		MaxLLMCalls: 1, DeltaStream: runOpts.DeltaStream, Input: conversationInput(session, messages),
	})
	var dataStream runnable.Stream[schema.MessageEvent]
	if runOpts.resume {
		dataStream, err = a.recipe.ResumeStream(runCtx, runOpts.GraphOptions...)
	} else {
		dataStream, err = a.recipe.Stream(runCtx, prepared, runOpts.GraphOptions...)
	}
	if err != nil {
		lifecycle.fail(err)
		return nil, err
	}
	dataStream, err = prefetchMessageEvents(runCtx, dataStream)
	if err != nil {
		lifecycle.fail(err)
		return nil, err
	}

	return lifecycle.wrap(dataStream, runOpts.StreamBufferSize), nil
}
