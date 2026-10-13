package agent

import (
	"context"
	"errors"

	"github.com/goharness/goharness/internal/agentrecipe"
	"github.com/goharness/goharness/pkg/llm"
	"github.com/goharness/goharness/pkg/runnable"
	"github.com/goharness/goharness/pkg/schema"
)

type BasicAgent struct {
	*base
	recipe *agentrecipe.Recipe
}

// newBasicAgent 构造无工具的最小 Agent；cfg 须已解析。
func newBasicAgent(cfg baseConfig) (*BasicAgent, error) {
	base, err := newBase(cfg)
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
	}, false, cfg.DefinitionVersion)
	if err != nil {
		return nil, err
	}
	a.recipe = recipe
	return a, nil
}

func (a *BasicAgent) stream(ctx context.Context, messages []schema.Message, options ...runnable.Option) (*runnable.StreamReader[[]schema.Message], error) {
	runCtx, prepared, runOpts, lifecycle, err := a.runPrelude(ctx, messages, AgentKindBasic, options)
	if err != nil {
		return nil, err
	}
	recipeConfig := recipeRunConfig(runOpts.session, messages, 1)
	var dataStream *runnable.StreamReader[[]schema.Message]
	if runOpts.resume {
		dataStream, err = a.recipe.ResumeStream(runCtx, recipeConfig, runOpts.forwarded...)
	} else {
		dataStream, err = a.recipe.Stream(runCtx, prepared, recipeConfig, runOpts.forwarded...)
	}
	if err != nil {
		lifecycle.fail(err)
		return nil, err
	}
	return lifecycle.wrap(dataStream)
}
