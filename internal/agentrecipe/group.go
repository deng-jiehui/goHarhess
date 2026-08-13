package agentrecipe

import (
	"context"
	"errors"
	"fmt"

	"goharness/pkg/callback"
	"goharness/pkg/graph"
	"goharness/pkg/llm"
	"goharness/pkg/runnable"
	"goharness/pkg/schema"
)

type GroupRunConfig struct {
	Input     []schema.Message
	Round     int
	LastAgent string
	Response  []schema.Message
}

type groupRunState struct {
	round       int
	lastAgent   string
	response    []schema.Message
	continueRun bool
	input       []schema.Message
}

type groupRunContextKey struct{}

func WithGroupRunConfig(ctx context.Context, cfg GroupRunConfig) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, groupRunContextKey{}, &groupRunState{
		round: cfg.Round, lastAgent: cfg.LastAgent, response: schema.CloneMessages(cfg.Response), input: schema.CloneMessages(cfg.Input),
	})
}

func hydrateGroupRun(ctx context.Context, state *RecipeState) error {
	run, err := groupStateFromContext(ctx)
	if err != nil {
		return err
	}
	run.input = schema.CloneMessages(state.Input)
	run.round = state.Round
	run.lastAgent = state.LastAgent
	run.response = schema.CloneMessages(state.Response)
	run.continueRun = state.ContinueRun
	return nil
}

func commitGroupRun(ctx context.Context, state *RecipeState, output []schema.Message) error {
	run, err := groupStateFromContext(ctx)
	if err != nil {
		return err
	}
	state.Messages = schema.CloneMessages(output)
	state.Response = schema.CloneMessages(run.response)
	state.Round = run.round
	state.LastAgent = run.lastAgent
	state.ContinueRun = run.continueRun
	return nil
}

func groupStateFromContext(ctx context.Context) (*groupRunState, error) {
	if ctx == nil {
		return nil, errors.New("group recipe context is nil")
	}
	state, ok := ctx.Value(groupRunContextKey{}).(*groupRunState)
	if !ok || state == nil {
		return nil, errors.New("group recipe run config is missing")
	}
	if state.round < 0 {
		return nil, errors.New("group recipe round cannot be negative")
	}
	return state, nil
}

type GroupHooks struct {
	MaxRounds int
	Dispatch  func(context.Context, []schema.Message, int, string) (string, runnable.Stream[schema.MessageEvent], error)
}

func NewGroupRecipe(name string, hooks GroupHooks) (*Recipe, error) {
	if hooks.MaxRounds < 1 {
		return nil, errors.New("group recipe max rounds must be positive")
	}
	if hooks.Dispatch == nil {
		return nil, errors.New("group recipe dispatch hook is required")
	}
	g, err := newRecipeGraph(name)
	if err != nil {
		return nil, err
	}
	dispatch := runnable.Func[[]schema.Message, []schema.Message](func(ctx context.Context, messages []schema.Message, _ ...runnable.Option) ([]schema.Message, error) {
		run, err := groupStateFromContext(ctx)
		if err != nil {
			return nil, err
		}
		if run.round >= hooks.MaxRounds {
			run.continueRun = false
			return schema.CloneMessages(messages), nil
		}
		selected, stream, err := hooks.Dispatch(ctx, messages, run.round, run.lastAgent)
		if err != nil {
			return nil, err
		}
		if stream == nil {
			return nil, fmt.Errorf("group member %q returned a nil stream", selected)
		}
		var roundOutput []schema.Message
		accumulator := llm.NewDeltaAccumulator(true)
		sawDone := false
		for item := range stream {
			if item.Err != nil {
				return nil, fmt.Errorf("group member %q: %w", selected, item.Err)
			}
			event := item.Value
			if event.Kind == schema.MessageEventDone {
				sawDone = true
				break
			}
			if len(event.Messages) == 0 {
				continue
			}
			if event.Kind == schema.MessageEventDelta {
				roundOutput, err = accumulator.Apply(llm.Event{Kind: llm.EventDelta, Messages: event.Messages})
				if err != nil {
					return nil, err
				}
			} else {
				roundOutput = schema.CloneMessages(event.Messages)
			}
			roundOutput = withSpeaker(roundOutput, selected)
			callback.EmitStream(ctx, schema.MessageEvent{Kind: schema.MessageEventSnapshot, Messages: appendMessages(run.response, roundOutput)})
		}
		if !sawDone {
			return nil, fmt.Errorf("group member %q stream closed without done event", selected)
		}
		if len(roundOutput) == 0 {
			run.continueRun = false
			return schema.CloneMessages(messages), nil
		}
		run.response = append(run.response, schema.CloneMessages(roundOutput)...)
		updated := appendMessages(messages, roundOutput)
		run.round++
		run.lastAgent = selected
		run.continueRun = run.round < hooks.MaxRounds
		return updated, nil
	})
	if err := g.AddNode("dispatch", dispatch,
		graph.WithPreHandler(func(ctx context.Context, state *RecipeState) ([]schema.Message, error) {
			if err := hydrateGroupRun(ctx, state); err != nil {
				return nil, err
			}
			return schema.CloneMessages(state.Messages), nil
		}),
		graph.WithPostHandler(commitGroupRun),
	); err != nil {
		return nil, err
	}
	finish := runnable.Func[[]schema.Message, []schema.Message](func(ctx context.Context, _ []schema.Message, _ ...runnable.Option) ([]schema.Message, error) {
		run, err := groupStateFromContext(ctx)
		if err != nil {
			return nil, err
		}
		return schema.CloneMessages(run.response), nil
	})
	if err := g.AddNode("finish", finish, graph.WithPreHandler(func(ctx context.Context, state *RecipeState) ([]schema.Message, error) {
		if err := hydrateGroupRun(ctx, state); err != nil {
			return nil, err
		}
		notifyCompletion(ctx, state)
		return schema.CloneMessages(state.Messages), nil
	})); err != nil {
		return nil, err
	}
	if err := g.AddEdge(graph.Start, "dispatch"); err != nil {
		return nil, err
	}
	if err := g.AddBranch("dispatch", "dispatch", "next_round", func(_ context.Context, state *RecipeState) (bool, error) {
		return state.ContinueRun, nil
	}); err != nil {
		return nil, err
	}
	if err := g.AddEdge("dispatch", "finish"); err != nil {
		return nil, err
	}
	if err := g.AddEdge("finish", graph.End); err != nil {
		return nil, err
	}
	if err := g.SetMaxSteps(hooks.MaxRounds + 2); err != nil {
		return nil, err
	}
	compiled, err := g.Compile()
	if err != nil {
		return nil, err
	}
	return &Recipe{graph: compiled}, nil
}

func withSpeaker(messages []schema.Message, speaker string) []schema.Message {
	result := schema.CloneMessages(messages)
	for i := range result {
		if result[i].Name == "" {
			result[i].Name = speaker
		}
	}
	return result
}
