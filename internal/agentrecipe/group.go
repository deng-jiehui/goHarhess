package agentrecipe

import (
	"context"
	"errors"
	"fmt"

	"goharness/pkg/graph"
	"goharness/pkg/llm"
	"goharness/pkg/runnable"
	"goharness/pkg/schema"
)

type GroupRunConfig struct {
	Round     int
	LastAgent string
	Response  []schema.Message
}

type groupRunState struct {
	round       int
	lastAgent   string
	response    []schema.Message
	continueRun bool
}

type groupRunContextKey struct{}

func WithGroupRunConfig(ctx context.Context, cfg GroupRunConfig) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, groupRunContextKey{}, &groupRunState{
		round: cfg.Round, lastAgent: cfg.LastAgent, response: schema.CloneMessages(cfg.Response),
	})
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
	MaxRounds       int
	CheckBreakpoint func(context.Context, []schema.Message, []schema.Message, int, string) error
	Dispatch        func(context.Context, []schema.Message, int, string) (string, runnable.Stream[schema.MessageEvent], error)
}

func NewGroupRecipe(name string, hooks GroupHooks) (*Recipe, error) {
	if hooks.MaxRounds < 1 {
		return nil, errors.New("group recipe max rounds must be positive")
	}
	if hooks.Dispatch == nil {
		return nil, errors.New("group recipe dispatch hook is required")
	}
	g := newRecipeGraph(name)
	dispatch := runnable.Func[RecipeState, RecipeState](func(ctx context.Context, state RecipeState, _ ...runnable.Option) (RecipeState, error) {
		run, err := groupStateFromContext(ctx)
		if err != nil {
			return RecipeState{}, err
		}
		if run.round >= hooks.MaxRounds {
			run.continueRun = false
			return state, nil
		}
		if hooks.CheckBreakpoint != nil {
			if err := hooks.CheckBreakpoint(ctx, state.Messages, schema.CloneMessages(run.response), run.round, run.lastAgent); err != nil {
				return RecipeState{}, err
			}
		}
		selected, stream, err := hooks.Dispatch(ctx, state.Messages, run.round, run.lastAgent)
		if err != nil {
			return RecipeState{}, err
		}
		if stream == nil {
			return RecipeState{}, fmt.Errorf("group member %q returned a nil stream", selected)
		}
		var roundOutput []schema.Message
		accumulator := llm.NewDeltaAccumulator(true)
		sawDone := false
		for item := range stream {
			if item.Err != nil {
				return RecipeState{}, fmt.Errorf("group member %q: %w", selected, item.Err)
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
					return RecipeState{}, err
				}
			} else {
				roundOutput = schema.CloneMessages(event.Messages)
			}
			roundOutput = withSpeaker(roundOutput, selected)
			graph.EmitData(ctx, schema.MessageEvent{Kind: schema.MessageEventSnapshot, Messages: appendMessages(run.response, roundOutput)})
		}
		if !sawDone {
			return RecipeState{}, fmt.Errorf("group member %q stream closed without done event", selected)
		}
		if len(roundOutput) == 0 {
			run.continueRun = false
			return state, nil
		}
		run.response = append(run.response, schema.CloneMessages(roundOutput)...)
		state.Messages = append(state.Messages, schema.CloneMessages(roundOutput)...)
		run.round++
		run.lastAgent = selected
		run.continueRun = run.round < hooks.MaxRounds
		return state, nil
	})
	if err := graph.AddNode(g, "dispatch", dispatch); err != nil {
		return nil, err
	}
	finish := runnable.Func[RecipeState, []schema.Message](func(ctx context.Context, _ RecipeState, _ ...runnable.Option) ([]schema.Message, error) {
		run, err := groupStateFromContext(ctx)
		if err != nil {
			return nil, err
		}
		return schema.CloneMessages(run.response), nil
	})
	if err := graph.AddNode(g, "finish", finish); err != nil {
		return nil, err
	}
	if err := g.AddEdge(graph.Start, "dispatch"); err != nil {
		return nil, err
	}
	if err := graph.AddConditionalEdge(g, "dispatch", "dispatch", "next_round", func(ctx context.Context, _ RecipeState) (bool, error) {
		run, err := groupStateFromContext(ctx)
		if err != nil {
			return false, err
		}
		return run.continueRun, nil
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
	return &Recipe{graph: g}, nil
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
