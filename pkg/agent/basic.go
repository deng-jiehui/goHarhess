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
		CheckBreakpoint: func(ctx context.Context, messages, response []schema.Message, calls int) error {
			runOpts, _ := resolvedRunOptionsFromContext(ctx)
			return a.checkBreakpoint(ctx, runOpts.session, runOpts, AgentKindBasic, BreakpointBeforeLLM, messages, response, calls, nil, nil)
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
	commitResponse, resumeCommit, err := resumeAtCommit(&runOpts, AgentKindBasic)
	if err != nil {
		return nil, err
	}
	ctx = bindRunContext(ctx, session, &runOpts)
	prepared := a.prepareMessages(loaded, &runOpts)
	runCtx, lifecycle, err := a.beginRun(ctx, prepared, runOpts)
	if err != nil {
		return nil, err
	}
	lifecycle.session = session
	runOpts.session = session
	if resumeCommit {
		return lifecycle.wrap(completedResumeEvents(commitResponse), runOpts.StreamBufferSize), nil
	}
	if _, _, err := continuationFor(runOpts, AgentKindBasic, BreakpointBeforeLLM); err != nil {
		lifecycle.fail(err)
		return nil, err
	}
	runCtx = withResolvedRunOptions(runCtx, runOpts)
	runCtx = agentrecipe.WithRunConfig(runCtx, agentrecipe.RunConfig{MaxLLMCalls: 1, DeltaStream: runOpts.DeltaStream})
	dataStream, err := a.recipe.Stream(runCtx, prepared)
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
