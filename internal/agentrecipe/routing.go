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

type RoutingRunConfig struct {
	MaxSteps      int
	ReturnControl bool
	Steps         int
	Response      []schema.Message
}

type routingRunState struct {
	maxSteps      int
	returnControl bool
	steps         int
	response      []schema.Message
	pending       []Call
	dispatchInput []schema.Message
	continueRun   bool
}

type routingRunContextKey struct{}

func WithRoutingRunConfig(ctx context.Context, cfg RoutingRunConfig) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, routingRunContextKey{}, &routingRunState{
		maxSteps: cfg.MaxSteps, returnControl: cfg.ReturnControl, steps: cfg.Steps,
		response: schema.CloneMessages(cfg.Response),
	})
}

func routingStateFromContext(ctx context.Context) (*routingRunState, error) {
	if ctx == nil {
		return nil, errors.New("routing recipe context is nil")
	}
	value, ok := ctx.Value(routingRunContextKey{}).(*routingRunState)
	if !ok || value == nil {
		return nil, errors.New("routing recipe run config is missing")
	}
	if value.maxSteps < 1 || value.steps < 0 {
		return nil, errors.New("routing recipe has invalid step limits")
	}
	return value, nil
}

type RoutedResult struct {
	Call     Call
	Messages []schema.Message
	Err      error
}

type RoutingHooks struct {
	MaxSteps        int
	ReturnControl   bool
	CallModel       func(context.Context, []schema.Message) (runnable.Stream[llm.Event], error)
	CheckBreakpoint func(context.Context, []schema.Message, []schema.Message, int) error
	DetectCalls     func([]schema.Message) []Call
	Dispatch        func(context.Context, []Call, []schema.Message) ([]RoutedResult, error)
	Observation     func(RoutedResult) schema.Message
	NameMessages    func([]schema.Message) []schema.Message
}

func NewRoutingRecipe(name string, hooks RoutingHooks) (*Recipe, error) {
	if hooks.MaxSteps < 1 {
		return nil, errors.New("routing recipe max steps must be positive")
	}
	if hooks.CallModel == nil || hooks.Dispatch == nil || hooks.DetectCalls == nil {
		return nil, errors.New("routing recipe requires model, call detection, and dispatch hooks")
	}
	g := newRecipeGraph(name)
	decide := runnable.Func[RecipeState, RecipeState](func(ctx context.Context, state RecipeState, _ ...runnable.Option) (RecipeState, error) {
		run, err := routingStateFromContext(ctx)
		if err != nil {
			return RecipeState{}, err
		}
		if run.steps >= run.maxSteps {
			run.continueRun = false
			return state, nil
		}
		if hooks.CheckBreakpoint != nil {
			if err := hooks.CheckBreakpoint(ctx, state.Messages, schema.CloneMessages(run.response), run.steps); err != nil {
				return RecipeState{}, err
			}
		}
		stream, err := hooks.CallModel(ctx, state.Messages)
		if err != nil {
			return RecipeState{}, err
		}
		if stream == nil {
			return RecipeState{}, errors.New("routing recipe model returned a nil stream")
		}
		accumulator := llm.NewDeltaAccumulator(true)
		var output []schema.Message
		for {
			select {
			case <-ctx.Done():
				return RecipeState{}, ctx.Err()
			case item, open := <-stream:
				if !open {
					goto streamDone
				}
				if item.Err != nil {
					return RecipeState{}, item.Err
				}
				if item.Value.Kind != llm.EventDelta || len(item.Value.Messages) == 0 {
					continue
				}
				output, err = accumulator.Apply(item.Value)
				if err != nil {
					return RecipeState{}, err
				}
				messages := appendMessages(run.response, output)
				if hooks.NameMessages != nil {
					messages = hooks.NameMessages(messages)
				}
				if !graph.EmitData(ctx, schema.MessageEvent{Kind: schema.MessageEventSnapshot, Messages: messages}) {
					return RecipeState{}, ctx.Err()
				}
			}
		}
	streamDone:
		run.steps++
		run.dispatchInput = schema.CloneMessages(state.Messages)
		state.Messages = append(state.Messages, schema.CloneMessages(output)...)
		calls := hooks.DetectCalls(output)
		if !run.returnControl && len(calls) > 1 {
			calls = calls[:1]
		}
		run.pending = append([]Call(nil), calls...)
		if len(calls) > 0 {
			return state, nil
		}
		run.response = append(run.response, schema.CloneMessages(output)...)
		run.continueRun = false
		return state, nil
	})
	if err := graph.AddNode(g, "decide", decide); err != nil {
		return nil, err
	}
	dispatch := runnable.Func[RecipeState, RecipeState](func(ctx context.Context, state RecipeState, _ ...runnable.Option) (RecipeState, error) {
		run, err := routingStateFromContext(ctx)
		if err != nil {
			return RecipeState{}, err
		}
		calls := append([]Call(nil), run.pending...)
		if len(calls) == 0 {
			run.continueRun = false
			return state, nil
		}
		dispatchInput := schema.CloneMessages(run.dispatchInput)
		results, err := hooks.Dispatch(ctx, calls, dispatchInput)
		if err != nil {
			return RecipeState{}, err
		}
		if len(results) != len(calls) {
			return RecipeState{}, fmt.Errorf("routing dispatch returned %d results for %d calls", len(results), len(calls))
		}
		run.dispatchInput = nil
		for _, result := range results {
			if result.Err != nil && !run.returnControl {
				return RecipeState{}, fmt.Errorf("routed agent %q: %w", result.Call.Name, result.Err)
			}
		}
		run.pending = nil
		if !run.returnControl {
			var output []schema.Message
			for _, result := range results {
				output = append(output, schema.CloneMessages(result.Messages)...)
			}
			state.Messages = append(state.Messages, schema.CloneMessages(output)...)
			run.response = schema.CloneMessages(output)
			run.continueRun = false
			messages := output
			if hooks.NameMessages != nil {
				messages = hooks.NameMessages(messages)
			}
			if !graph.EmitData(ctx, schema.MessageEvent{Kind: schema.MessageEventSnapshot, Messages: messages}) {
				return RecipeState{}, ctx.Err()
			}
			return state, nil
		}
		observations := make([]schema.Message, 0, len(results))
		for _, result := range results {
			if hooks.Observation == nil {
				return RecipeState{}, errors.New("routing recipe observation hook is missing")
			}
			observations = append(observations, hooks.Observation(result))
		}
		state.Messages = append(state.Messages, schema.CloneMessages(observations)...)
		run.response = append(run.response, schema.CloneMessages(observations)...)
		run.continueRun = run.steps < run.maxSteps
		messages := run.response
		if hooks.NameMessages != nil {
			messages = hooks.NameMessages(messages)
		}
		if !graph.EmitData(ctx, schema.MessageEvent{Kind: schema.MessageEventSnapshot, Messages: messages}) {
			return RecipeState{}, ctx.Err()
		}
		return state, nil
	})
	if err := graph.AddNode(g, "dispatch", dispatch); err != nil {
		return nil, err
	}
	finish := runnable.Func[RecipeState, []schema.Message](func(ctx context.Context, _ RecipeState, _ ...runnable.Option) ([]schema.Message, error) {
		run, err := routingStateFromContext(ctx)
		if err != nil {
			return nil, err
		}
		response := schema.CloneMessages(run.response)
		if hooks.NameMessages != nil {
			response = hooks.NameMessages(response)
		}
		return response, nil
	})
	if err := graph.AddNode(g, "finish", finish); err != nil {
		return nil, err
	}
	if err := g.AddEdge(graph.Start, "decide"); err != nil {
		return nil, err
	}
	if err := graph.AddConditionalEdge(g, "decide", "dispatch", "sub_agents", func(ctx context.Context, _ RecipeState) (bool, error) {
		run, err := routingStateFromContext(ctx)
		if err != nil {
			return false, err
		}
		return len(run.pending) > 0, nil
	}); err != nil {
		return nil, err
	}
	if err := g.AddEdge("decide", "finish"); err != nil {
		return nil, err
	}
	if err := graph.AddConditionalEdge(g, "dispatch", "decide", "continue", func(ctx context.Context, _ RecipeState) (bool, error) {
		run, err := routingStateFromContext(ctx)
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
	if err := g.SetMaxSteps(hooks.MaxSteps*2 + 2); err != nil {
		return nil, err
	}
	return &Recipe{graph: g}, nil
}
