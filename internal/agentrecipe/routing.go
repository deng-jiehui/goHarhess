package agentrecipe

import (
	"context"
	"errors"
	"fmt"
	"strings"

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

func syncRoutingState(state *RecipeState, run *routingRunState) {
	state.Response = schema.CloneMessages(run.response)
	state.RoutingSteps = run.steps
	state.Pending = cloneCalls(run.pending)
	state.DispatchInput = schema.CloneMessages(run.dispatchInput)
	state.ContinueRun = run.continueRun
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

type RoutedAgent struct {
	Name string
	Node runnable.Runnable[[]schema.Message, runnable.Outcome[[]schema.Message]]
}

type RoutingHooks struct {
	MaxSteps     int
	Model        runnable.Runnable[llm.Request, llm.Response]
	PrepareModel func(context.Context, []schema.Message) (llm.Request, error)
	Agents       []RoutedAgent
	DetectCalls  func([]schema.Message) []Call
	Observation  func(RoutedResult) schema.Message
	NameMessages func([]schema.Message) []schema.Message
}

func NewRoutingRecipe(name string, hooks RoutingHooks) (*Recipe, error) {
	if hooks.MaxSteps < 1 {
		return nil, errors.New("routing recipe max steps must be positive")
	}
	if hooks.Model == nil || hooks.PrepareModel == nil || hooks.DetectCalls == nil {
		return nil, errors.New("routing recipe requires a model node, model input handler, and call detector")
	}
	agents := make(map[string]RoutedAgent, len(hooks.Agents))
	for index, candidate := range hooks.Agents {
		candidate.Name = strings.TrimSpace(candidate.Name)
		if candidate.Name == "" || candidate.Node == nil {
			return nil, fmt.Errorf("routing agent %d is not configured", index)
		}
		if _, exists := agents[candidate.Name]; exists {
			return nil, fmt.Errorf("duplicate routing agent %q", candidate.Name)
		}
		agents[candidate.Name] = candidate
	}

	g, err := newRecipeGraph(name)
	if err != nil {
		return nil, err
	}
	if err := g.AddNode("decide", hooks.Model,
		graph.WithPreHandler(func(ctx context.Context, state *RecipeState) (llm.Request, error) {
			if err := hydrateRoutingRun(ctx, state); err != nil {
				return llm.Request{}, err
			}
			return hooks.PrepareModel(ctx, schema.CloneMessages(state.Messages))
		}),
		graph.WithPostHandler(func(ctx context.Context, state *RecipeState, response llm.Response) error {
			run, err := routingStateFromContext(ctx)
			if err != nil {
				return err
			}
			output := schema.CloneMessages(response.Messages)
			run.steps++
			run.dispatchInput = schema.CloneMessages(state.Messages)
			state.Messages = appendMessages(state.Messages, output)
			calls := hooks.DetectCalls(output)
			if !run.returnControl && len(calls) > 1 {
				calls = calls[:1]
			}
			run.pending = cloneCalls(calls)
			if len(calls) == 0 {
				run.response = append(run.response, output...)
				run.dispatchInput = nil
				run.continueRun = false
			} else {
				run.continueRun = true
			}
			syncRoutingState(state, run)
			return nil
		}),
	); err != nil {
		return nil, err
	}

	postAgent := func(ctx context.Context, state *RecipeState, outcome runnable.Outcome[[]schema.Message]) error {
		run, err := routingStateFromContext(ctx)
		if err != nil {
			return err
		}
		if len(run.pending) == 0 {
			return errors.New("routing agent completed without a pending call")
		}
		call := run.pending[0]
		result := RoutedResult{Call: call, Messages: withSpeaker(outcome.Value, call.Name), Err: outcome.Err}
		if result.Err != nil && !run.returnControl {
			return fmt.Errorf("routed agent %q: %w", call.Name, result.Err)
		}
		run.pending = cloneCalls(run.pending[1:])
		if !run.returnControl {
			output := schema.CloneMessages(result.Messages)
			state.Messages = appendMessages(state.Messages, output)
			run.response = output
			run.pending = nil
			run.dispatchInput = nil
			run.continueRun = false
			emitRoutingSnapshot(ctx, hooks, run.response)
			syncRoutingState(state, run)
			return nil
		}
		if hooks.Observation == nil {
			return errors.New("routing recipe observation hook is missing")
		}
		observation := hooks.Observation(result)
		state.Messages = appendMessages(state.Messages, []schema.Message{observation})
		run.response = append(run.response, observation)
		if len(run.pending) == 0 {
			run.dispatchInput = nil
		}
		run.continueRun = len(run.pending) > 0 || run.steps < run.maxSteps
		emitRoutingSnapshot(ctx, hooks, run.response)
		syncRoutingState(state, run)
		return nil
	}

	for _, candidate := range hooks.Agents {
		nodeID := routedAgentNodeID(candidate.Name)
		if err := g.AddNode(nodeID, candidate.Node,
			graph.WithPreHandler(func(ctx context.Context, state *RecipeState) ([]schema.Message, error) {
				if err := hydrateRoutingRun(ctx, state); err != nil {
					return nil, err
				}
				return schema.CloneMessages(state.DispatchInput), nil
			}),
			graph.WithPostHandler(postAgent),
		); err != nil {
			return nil, err
		}
	}

	unroutable := runnable.Func[[]schema.Message, runnable.Outcome[[]schema.Message]](func(ctx context.Context, _ []schema.Message, _ ...runnable.Option) (runnable.Outcome[[]schema.Message], error) {
		run, err := routingStateFromContext(ctx)
		if err != nil {
			return runnable.Outcome[[]schema.Message]{}, err
		}
		if len(run.pending) == 0 {
			return runnable.Outcome[[]schema.Message]{}, errors.New("routing call is missing")
		}
		name := run.pending[0].Name
		return runnable.Outcome[[]schema.Message]{Err: fmt.Errorf("routing model selected unregistered sub-agent %q", name)}, nil
	})
	if err := g.AddNode("unroutable", unroutable,
		graph.WithPreHandler(func(ctx context.Context, state *RecipeState) ([]schema.Message, error) {
			if err := hydrateRoutingRun(ctx, state); err != nil {
				return nil, err
			}
			return schema.CloneMessages(state.DispatchInput), nil
		}),
		graph.WithPostHandler(postAgent),
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

	if err := g.AddBranch(graph.Start, "finish", "step_limit", func(ctx context.Context, state *RecipeState) (bool, error) {
		if err := hydrateRoutingRun(ctx, state); err != nil {
			return false, err
		}
		run, err := routingStateFromContext(ctx)
		return err == nil && run.steps >= run.maxSteps, err
	}); err != nil {
		return nil, err
	}
	if err := g.AddEdge(graph.Start, "decide"); err != nil {
		return nil, err
	}
	if err := addRoutingEdges(g, "decide", hooks.Agents); err != nil {
		return nil, err
	}
	for _, candidate := range hooks.Agents {
		if err := addRoutingEdges(g, routedAgentNodeID(candidate.Name), hooks.Agents); err != nil {
			return nil, err
		}
	}
	if err := addRoutingEdges(g, "unroutable", hooks.Agents); err != nil {
		return nil, err
	}
	if err := g.AddEdge("finish", graph.End); err != nil {
		return nil, err
	}
	if err := g.SetMaxSteps(hooks.MaxSteps*(len(hooks.Agents)+2) + 2); err != nil {
		return nil, err
	}
	compiled, err := g.Compile()
	if err != nil {
		return nil, err
	}
	return &Recipe{graph: compiled}, nil
}

func addRoutingEdges(g *graph.Graph[[]schema.Message, []schema.Message], from string, agents []RoutedAgent) error {
	for _, candidate := range agents {
		name := candidate.Name
		if err := g.AddBranch(from, routedAgentNodeID(name), "route:"+name, func(_ context.Context, state *RecipeState) (bool, error) {
			return len(state.Pending) > 0 && state.Pending[0].Name == name, nil
		}); err != nil {
			return err
		}
	}
	if err := g.AddBranch(from, "unroutable", "unregistered", func(_ context.Context, state *RecipeState) (bool, error) {
		return len(state.Pending) > 0, nil
	}); err != nil {
		return err
	}
	if from != "decide" {
		if err := g.AddBranch(from, "decide", "continue", func(_ context.Context, state *RecipeState) (bool, error) {
			return state.ContinueRun, nil
		}); err != nil {
			return err
		}
	}
	return g.AddEdge(from, "finish")
}

func emitRoutingSnapshot(ctx context.Context, hooks RoutingHooks, response []schema.Message) {
	emitted := schema.CloneMessages(response)
	if hooks.NameMessages != nil {
		emitted = hooks.NameMessages(emitted)
	}
	callback.EmitStream(ctx, schema.MessageEvent{Kind: schema.MessageEventSnapshot, Messages: emitted})
}

func routedAgentNodeID(name string) string { return "agent:" + name }
