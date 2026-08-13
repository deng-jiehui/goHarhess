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

type RoutingRunConfig struct {
	MaxSteps      int
	ReturnControl bool
	Input         []schema.Message
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
	input         []schema.Message
}

type routingRunContextKey struct{}

func WithRoutingRunConfig(ctx context.Context, cfg RoutingRunConfig) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, routingRunContextKey{}, &routingRunState{
		maxSteps: cfg.MaxSteps, returnControl: cfg.ReturnControl, steps: cfg.Steps,
		response: schema.CloneMessages(cfg.Response), input: schema.CloneMessages(cfg.Input),
	})
}

func hydrateRoutingRun(ctx context.Context, state *RecipeState) error {
	run, err := routingStateFromContext(ctx)
	if err != nil {
		return err
	}
	run.input = schema.CloneMessages(state.Input)
	run.steps = state.RoutingSteps
	run.response = schema.CloneMessages(state.Response)
	run.pending = cloneCalls(state.Pending)
	run.dispatchInput = schema.CloneMessages(state.DispatchInput)
	run.continueRun = state.ContinueRun
	return nil
}

func commitRoutingRun(ctx context.Context, state *RecipeState, output []schema.Message) error {
	run, err := routingStateFromContext(ctx)
	if err != nil {
		return err
	}
	state.Messages = schema.CloneMessages(output)
	state.Response = schema.CloneMessages(run.response)
	state.RoutingSteps = run.steps
	state.Pending = cloneCalls(run.pending)
	state.DispatchInput = schema.CloneMessages(run.dispatchInput)
	state.ContinueRun = run.continueRun
	return nil
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
	MaxSteps      int
	ReturnControl bool
	CallModel     func(context.Context, []schema.Message) (runnable.Stream[llm.Event], error)
	DetectCalls   func([]schema.Message) []Call
	Dispatch      func(context.Context, []Call, []schema.Message) ([]RoutedResult, error)
	Observation   func(RoutedResult) schema.Message
	NameMessages  func([]schema.Message) []schema.Message
}

func NewRoutingRecipe(name string, hooks RoutingHooks) (*Recipe, error) {
	if hooks.MaxSteps < 1 {
		return nil, errors.New("routing recipe max steps must be positive")
	}
	if hooks.CallModel == nil || hooks.Dispatch == nil || hooks.DetectCalls == nil {
		return nil, errors.New("routing recipe requires model, call detection, and dispatch hooks")
	}
	g, err := newRecipeGraph(name)
	if err != nil {
		return nil, err
	}
	decide := runnable.Func[[]schema.Message, []schema.Message](func(ctx context.Context, messages []schema.Message, _ ...runnable.Option) ([]schema.Message, error) {
		run, err := routingStateFromContext(ctx)
		if err != nil {
			return nil, err
		}
		if run.steps >= run.maxSteps {
			run.continueRun = false
			return schema.CloneMessages(messages), nil
		}
		stream, err := hooks.CallModel(ctx, messages)
		if err != nil {
			return nil, err
		}
		if stream == nil {
			return nil, errors.New("routing recipe model returned a nil stream")
		}
		accumulator := llm.NewDeltaAccumulator(true)
		var output []schema.Message
		sawDone := false
		for {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case item, open := <-stream:
				if !open {
					goto streamClosed
				}
				if item.Err != nil {
					return nil, item.Err
				}
				if item.Value.Kind == llm.EventDone {
					sawDone = true
					goto streamClosed
				}
				if item.Value.Kind != llm.EventDelta || len(item.Value.Messages) == 0 {
					continue
				}
				output, err = accumulator.Apply(item.Value)
				if err != nil {
					return nil, err
				}
				messages := appendMessages(run.response, output)
				if hooks.NameMessages != nil {
					messages = hooks.NameMessages(messages)
				}
				callback.EmitStream(ctx, schema.MessageEvent{Kind: schema.MessageEventSnapshot, Messages: messages})
			}
		}
	streamClosed:
		if !sawDone {
			return nil, errors.New("routing model stream closed without done event")
		}
		run.steps++
		run.dispatchInput = schema.CloneMessages(messages)
		updated := appendMessages(messages, output)
		calls := hooks.DetectCalls(output)
		if !run.returnControl && len(calls) > 1 {
			calls = calls[:1]
		}
		run.pending = append([]Call(nil), calls...)
		if len(calls) > 0 {
			return updated, nil
		}
		run.response = append(run.response, schema.CloneMessages(output)...)
		run.continueRun = false
		return updated, nil
	})
	if err := g.AddNode("decide", decide,
		graph.WithPreHandler(func(ctx context.Context, state *RecipeState) ([]schema.Message, error) {
			if err := hydrateRoutingRun(ctx, state); err != nil {
				return nil, err
			}
			return schema.CloneMessages(state.Messages), nil
		}),
		graph.WithPostHandler(commitRoutingRun),
	); err != nil {
		return nil, err
	}
	dispatch := runnable.Func[[]schema.Message, []schema.Message](func(ctx context.Context, messages []schema.Message, _ ...runnable.Option) ([]schema.Message, error) {
		run, err := routingStateFromContext(ctx)
		if err != nil {
			return nil, err
		}
		calls := append([]Call(nil), run.pending...)
		if len(calls) == 0 {
			run.continueRun = false
			return schema.CloneMessages(messages), nil
		}
		dispatchInput := schema.CloneMessages(run.dispatchInput)
		results, err := hooks.Dispatch(ctx, calls, dispatchInput)
		if err != nil {
			return nil, err
		}
		if len(results) != len(calls) {
			return nil, fmt.Errorf("routing dispatch returned %d results for %d calls", len(results), len(calls))
		}
		run.dispatchInput = nil
		for _, result := range results {
			if result.Err != nil && !run.returnControl {
				return nil, fmt.Errorf("routed agent %q: %w", result.Call.Name, result.Err)
			}
		}
		run.pending = nil
		if !run.returnControl {
			var output []schema.Message
			for _, result := range results {
				output = append(output, schema.CloneMessages(result.Messages)...)
			}
			updated := appendMessages(messages, output)
			run.response = schema.CloneMessages(output)
			run.continueRun = false
			messages := output
			if hooks.NameMessages != nil {
				messages = hooks.NameMessages(messages)
			}
			callback.EmitStream(ctx, schema.MessageEvent{Kind: schema.MessageEventSnapshot, Messages: messages})
			return updated, nil
		}
		observations := make([]schema.Message, 0, len(results))
		for _, result := range results {
			if hooks.Observation == nil {
				return nil, errors.New("routing recipe observation hook is missing")
			}
			observations = append(observations, hooks.Observation(result))
		}
		updated := appendMessages(messages, observations)
		run.response = append(run.response, schema.CloneMessages(observations)...)
		run.continueRun = run.steps < run.maxSteps
		emitted := run.response
		if hooks.NameMessages != nil {
			emitted = hooks.NameMessages(emitted)
		}
		callback.EmitStream(ctx, schema.MessageEvent{Kind: schema.MessageEventSnapshot, Messages: emitted})
		return updated, nil
	})
	if err := g.AddNode("dispatch", dispatch,
		graph.WithPreHandler(func(ctx context.Context, state *RecipeState) ([]schema.Message, error) {
			if err := hydrateRoutingRun(ctx, state); err != nil {
				return nil, err
			}
			return schema.CloneMessages(state.Messages), nil
		}),
		graph.WithPostHandler(commitRoutingRun),
	); err != nil {
		return nil, err
	}
	finish := runnable.Func[[]schema.Message, []schema.Message](func(ctx context.Context, _ []schema.Message, _ ...runnable.Option) ([]schema.Message, error) {
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
	if err := g.AddNode("finish", finish, graph.WithPreHandler(func(ctx context.Context, state *RecipeState) ([]schema.Message, error) {
		if err := hydrateRoutingRun(ctx, state); err != nil {
			return nil, err
		}
		notifyCompletion(ctx, state)
		return schema.CloneMessages(state.Messages), nil
	})); err != nil {
		return nil, err
	}
	if err := g.AddEdge(graph.Start, "decide"); err != nil {
		return nil, err
	}
	if err := g.AddBranch("decide", "dispatch", "sub_agents", func(_ context.Context, state *RecipeState) (bool, error) {
		return len(state.Pending) > 0, nil
	}); err != nil {
		return nil, err
	}
	if err := g.AddEdge("decide", "finish"); err != nil {
		return nil, err
	}
	if err := g.AddBranch("dispatch", "decide", "continue", func(_ context.Context, state *RecipeState) (bool, error) {
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
	if err := g.SetMaxSteps(hooks.MaxSteps*2 + 2); err != nil {
		return nil, err
	}
	compiled, err := g.Compile()
	if err != nil {
		return nil, err
	}
	return &Recipe{graph: compiled}, nil
}
