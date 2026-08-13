// Package agentrecipe contains strongly typed Graph recipes used by Agents.
package agentrecipe

import (
	"context"
	"encoding/json"
	"errors"

	"goharness/pkg/graph"
	"goharness/pkg/llm"
	"goharness/pkg/runnable"
	"goharness/pkg/schema"
)

type Call struct {
	Name       string
	Arguments  string
	FunctionID string
	Thought    string
	Structured bool
}

type ToolResult struct {
	Call    Call
	Content schema.Content
	Err     error
}

type ToolBatch interface {
	Resolve([]Call, []schema.Message) ([]ToolResult, error)
}

type RunConfig struct {
	MaxLLMCalls int
	DeltaStream bool
	Input       []schema.Message
	LLMCalls    int
	Response    []schema.Message
}

// RecipeState is the run-exclusive State used by built-in Agent graphs.
type RecipeState struct {
	Input         []schema.Message `json:"input,omitempty"`
	Messages      []schema.Message `json:"messages,omitempty"`
	Response      []schema.Message `json:"response,omitempty"`
	LLMCalls      int              `json:"llm_calls,omitempty"`
	Calls         []Call           `json:"calls,omitempty"`
	HasCalls      bool             `json:"has_calls,omitempty"`
	RoutingSteps  int              `json:"routing_steps,omitempty"`
	Pending       []Call           `json:"pending,omitempty"`
	DispatchInput []schema.Message `json:"dispatch_input,omitempty"`
	ContinueRun   bool             `json:"continue_run,omitempty"`
	Round         int              `json:"round,omitempty"`
	LastAgent     string           `json:"last_agent,omitempty"`
}

func newRecipeGraph(name string) (*graph.Graph[[]schema.Message, []schema.Message], error) {
	g := graph.NewGraph[[]schema.Message, []schema.Message](name)
	if err := g.SetStateFactory(func(ctx context.Context, input []schema.Message) (*RecipeState, error) {
		state := &RecipeState{Input: schema.CloneMessages(input), Messages: schema.CloneMessages(input)}
		seedRecipeState(ctx, state)
		return state, nil
	}); err != nil {
		return nil, err
	}
	codec, err := graph.NewStateCodec[*RecipeState](
		"agentrecipe:"+name,
		1,
		func(_ context.Context, state *RecipeState) ([]byte, error) { return json.Marshal(state) },
		func(_ context.Context, data []byte) (*RecipeState, error) {
			var state RecipeState
			if err := json.Unmarshal(data, &state); err != nil {
				return nil, err
			}
			return &state, nil
		},
	)
	if err != nil {
		return nil, err
	}
	if err := g.SetStateCodec(codec); err != nil {
		return nil, err
	}
	return g, nil
}

type toolRunState struct {
	maxLLMCalls int
	deltaStream bool
	llmCalls    int
	response    []schema.Message
	calls       []Call
	hasCalls    bool
	input       []schema.Message
}

type Hooks struct {
	Name              string
	CallModel         func(context.Context, []schema.Message) (runnable.Stream[llm.Event], error)
	NewToolBatch      func(context.Context) ToolBatch
	DetectCalls       func([]schema.Message) []Call
	RehydrateLLMCalls func([]schema.Message) int
	Observation       func(ToolResult) schema.Message
	NameMessages      func([]schema.Message) []schema.Message
}

type runContextKey struct{}

func WithRunConfig(ctx context.Context, cfg RunConfig) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, runContextKey{}, &toolRunState{
		maxLLMCalls: cfg.MaxLLMCalls,
		deltaStream: cfg.DeltaStream,
		llmCalls:    cfg.LLMCalls,
		response:    schema.CloneMessages(cfg.Response),
		input:       schema.CloneMessages(cfg.Input),
	})
}

type completionSinkKey struct{}

// WithCompletionSink installs an ephemeral callback used by the Agent layer
// to recover the original caller input for conversation commit. The function
// itself is never part of RecipeState or a Graph checkpoint.
func WithCompletionSink(ctx context.Context, sink func([]schema.Message)) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, completionSinkKey{}, sink)
}

func notifyCompletion(ctx context.Context, state *RecipeState) {
	if ctx == nil || state == nil {
		return
	}
	if sink, ok := ctx.Value(completionSinkKey{}).(func([]schema.Message)); ok && sink != nil {
		sink(schema.CloneMessages(state.Input))
	}
}

func seedRecipeState(ctx context.Context, state *RecipeState) {
	if state == nil {
		return
	}
	if run, ok := ctx.Value(runContextKey{}).(*toolRunState); ok && run != nil {
		if run.input != nil {
			state.Input = schema.CloneMessages(run.input)
		}
		state.Response = schema.CloneMessages(run.response)
		state.LLMCalls = run.llmCalls
		state.Calls = cloneCalls(run.calls)
		state.HasCalls = run.hasCalls
	}
	if run, ok := ctx.Value(routingRunContextKey{}).(*routingRunState); ok && run != nil {
		if run.input != nil {
			state.Input = schema.CloneMessages(run.input)
		}
		state.Response = schema.CloneMessages(run.response)
		state.RoutingSteps = run.steps
		state.Pending = cloneCalls(run.pending)
		state.DispatchInput = schema.CloneMessages(run.dispatchInput)
		state.ContinueRun = run.continueRun
	}
	if run, ok := ctx.Value(groupRunContextKey{}).(*groupRunState); ok && run != nil {
		if run.input != nil {
			state.Input = schema.CloneMessages(run.input)
		}
		state.Response = schema.CloneMessages(run.response)
		state.Round = run.round
		state.LastAgent = run.lastAgent
		state.ContinueRun = run.continueRun
	}
}

func hydrateToolRun(ctx context.Context, state *RecipeState) error {
	run, err := configFromContext(ctx)
	if err != nil {
		return err
	}
	run.input = schema.CloneMessages(state.Input)
	run.response = schema.CloneMessages(state.Response)
	run.llmCalls = state.LLMCalls
	run.calls = cloneCalls(state.Calls)
	run.hasCalls = state.HasCalls
	return nil
}

func commitToolRun(ctx context.Context, state *RecipeState, output []schema.Message) error {
	run, err := configFromContext(ctx)
	if err != nil {
		return err
	}
	state.Messages = schema.CloneMessages(output)
	state.Response = schema.CloneMessages(run.response)
	state.LLMCalls = run.llmCalls
	state.Calls = cloneCalls(run.calls)
	state.HasCalls = run.hasCalls
	return nil
}

func cloneCalls(calls []Call) []Call {
	return append([]Call(nil), calls...)
}

func configFromContext(ctx context.Context) (*toolRunState, error) {
	if ctx == nil {
		return nil, errors.New("agent recipe context is nil")
	}
	cfg, ok := ctx.Value(runContextKey{}).(*toolRunState)
	if !ok || cfg == nil {
		return nil, errors.New("agent recipe run config is missing")
	}
	if cfg.maxLLMCalls < 1 {
		return nil, errors.New("agent recipe max LLM calls must be positive")
	}
	return cfg, nil
}

type Recipe struct {
	graph *graph.Graph[[]schema.Message, []schema.Message]
}

func NewToolRecipe(name string, hooks Hooks, tools bool) (*Recipe, error) {
	if hooks.CallModel == nil {
		return nil, errors.New("agent recipe requires model hook")
	}
	g, err := newRecipeGraph(name)
	if err != nil {
		return nil, err
	}
	llmNode := runnable.Func[[]schema.Message, []schema.Message](func(ctx context.Context, messages []schema.Message, _ ...runnable.Option) ([]schema.Message, error) {
		cfg, err := configFromContext(ctx)
		if err != nil {
			return nil, err
		}
		if cfg.llmCalls == 0 && len(cfg.response) > 0 && hooks.RehydrateLLMCalls != nil {
			cfg.llmCalls = hooks.RehydrateLLMCalls(cfg.response)
		}
		if cfg.llmCalls >= cfg.maxLLMCalls {
			cfg.hasCalls = false
			return schema.CloneMessages(messages), nil
		}
		stream, err := hooks.CallModel(ctx, messages)
		if err != nil {
			return nil, err
		}
		if stream == nil {
			return nil, errors.New("agent recipe model returned a nil stream")
		}
		accumulator := llm.NewDeltaAccumulator(cfg.deltaStream)
		var output []schema.Message
		sawDone := false
		for item := range stream {
			if item.Err != nil {
				return nil, item.Err
			}
			if item.Value.Kind == llm.EventDone {
				sawDone = true
				break
			}
			if item.Value.Kind != llm.EventDelta || len(item.Value.Messages) == 0 {
				continue
			}
			merged, err := accumulator.Apply(item.Value)
			if err != nil {
				return nil, err
			}
			output = merged
			kind := schema.MessageEventSnapshot
			messages := appendMessages(cfg.response, output)
			if cfg.deltaStream {
				kind = schema.MessageEventDelta
				messages = schema.CloneMessages(item.Value.Messages)
			}
			if hooks.NameMessages != nil {
				messages = hooks.NameMessages(messages)
			}
			if !graph.EmitData(ctx, schema.MessageEvent{Kind: kind, Messages: messages}) {
				return nil, ctx.Err()
			}
		}
		if !sawDone {
			return nil, errors.New("agent recipe model stream closed without done event")
		}
		cfg.llmCalls++
		cfg.response = append(cfg.response, schema.CloneMessages(output)...)
		updated := appendMessages(messages, output)
		calls := []Call{}
		if hooks.DetectCalls != nil {
			calls = hooks.DetectCalls(output)
		}
		cfg.calls = append([]Call(nil), calls...)
		cfg.hasCalls = tools && len(calls) > 0 && cfg.llmCalls < cfg.maxLLMCalls
		return updated, nil
	})
	if err := g.AddNode("llm", llmNode,
		graph.WithPreHandler(func(ctx context.Context, state *RecipeState) ([]schema.Message, error) {
			if err := hydrateToolRun(ctx, state); err != nil {
				return nil, err
			}
			return schema.CloneMessages(state.Messages), nil
		}),
		graph.WithPostHandler(commitToolRun),
	); err != nil {
		return nil, err
	}
	if err := g.AddEdge(graph.Start, "llm"); err != nil {
		return nil, err
	}

	if tools {
		toolNode := runnable.Func[[]schema.Message, []schema.Message](func(ctx context.Context, messages []schema.Message, _ ...runnable.Option) ([]schema.Message, error) {
			cfg, err := configFromContext(ctx)
			if err != nil {
				return nil, err
			}
			calls := append([]Call(nil), cfg.calls...)
			if len(calls) == 0 {
				cfg.hasCalls = false
				return schema.CloneMessages(messages), nil
			}
			var batch ToolBatch
			if hooks.NewToolBatch != nil {
				batch = hooks.NewToolBatch(ctx)
			}
			if batch == nil {
				return nil, errors.New("agent recipe tool batch is unavailable")
			}
			results, err := batch.Resolve(calls, messages)
			if err != nil {
				return nil, err
			}
			observations := make([]schema.Message, 0, len(results))
			for _, result := range results {
				if hooks.Observation == nil {
					return nil, errors.New("agent recipe observation hook is missing")
				}
				observations = append(observations, hooks.Observation(result))
			}
			updated := appendMessages(messages, observations)
			cfg.response = append(cfg.response, schema.CloneMessages(observations)...)
			cfg.hasCalls = false
			cfg.calls = nil
			emitted := cfg.response
			if hooks.NameMessages != nil {
				emitted = hooks.NameMessages(emitted)
			}
			graph.EmitData(ctx, schema.MessageEvent{Kind: schema.MessageEventSnapshot, Messages: emitted})
			return updated, nil
		})
		if err := g.AddNode("tools", toolNode,
			graph.WithPreHandler(func(ctx context.Context, state *RecipeState) ([]schema.Message, error) {
				if err := hydrateToolRun(ctx, state); err != nil {
					return nil, err
				}
				return schema.CloneMessages(state.Messages), nil
			}),
			graph.WithPostHandler(commitToolRun),
		); err != nil {
			return nil, err
		}
		if err := g.AddBranch("llm", "tools", "tool_calls", func(_ context.Context, state *RecipeState) (bool, error) {
			return state.HasCalls, nil
		}); err != nil {
			return nil, err
		}
		if err := g.AddEdge("llm", "finish"); err != nil {
			return nil, err
		}
		if err := g.AddEdge("tools", "llm"); err != nil {
			return nil, err
		}
	} else if err := g.AddEdge("llm", "finish"); err != nil {
		return nil, err
	}

	finish := runnable.Func[[]schema.Message, []schema.Message](func(ctx context.Context, _ []schema.Message, _ ...runnable.Option) ([]schema.Message, error) {
		cfg, err := configFromContext(ctx)
		if err != nil {
			return nil, err
		}
		response := schema.CloneMessages(cfg.response)
		if hooks.NameMessages != nil {
			response = hooks.NameMessages(response)
		}
		return response, nil
	})
	if err := g.AddNode("finish", finish, graph.WithPreHandler(func(ctx context.Context, state *RecipeState) ([]schema.Message, error) {
		if err := hydrateToolRun(ctx, state); err != nil {
			return nil, err
		}
		notifyCompletion(ctx, state)
		return schema.CloneMessages(state.Messages), nil
	})); err != nil {
		return nil, err
	}
	if err := g.AddEdge("finish", graph.End); err != nil {
		return nil, err
	}
	if tools {
		if err := g.SetMaxSteps(201); err != nil {
			return nil, err
		}
	}
	return &Recipe{graph: g}, nil
}

func (r *Recipe) Invoke(ctx context.Context, messages []schema.Message, options ...runnable.Option) ([]schema.Message, error) {
	if r == nil || r.graph == nil {
		return nil, errors.New("agent recipe is not configured")
	}
	ctx = withSession(ctx)
	return r.graph.Invoke(ctx, messages, options...)
}

func (r *Recipe) Stream(ctx context.Context, messages []schema.Message, options ...runnable.Option) (runnable.Stream[schema.MessageEvent], error) {
	if r == nil || r.graph == nil {
		return nil, errors.New("agent recipe is not configured")
	}
	ctx = withSession(ctx)
	return graph.StreamMessages(ctx, r.graph, messages, options...)
}

func (r *Recipe) Resume(ctx context.Context, options ...runnable.Option) ([]schema.Message, error) {
	if r == nil || r.graph == nil {
		return nil, errors.New("agent recipe is not configured")
	}
	ctx = withSession(ctx)
	return r.graph.Resume(ctx, options...)
}

func (r *Recipe) ResumeStream(ctx context.Context, options ...runnable.Option) (runnable.Stream[schema.MessageEvent], error) {
	if r == nil || r.graph == nil {
		return nil, errors.New("agent recipe is not configured")
	}
	ctx = withSession(ctx)
	return graph.ResumeMessages(ctx, r.graph, options...)
}

func withSession(ctx context.Context) context.Context {
	if ctx == nil {
		return nil
	}
	if current, ok := ctx.Value(runContextKey{}).(*toolRunState); ok && current != nil {
		cloned := *current
		cloned.response = schema.CloneMessages(current.response)
		cloned.calls = append([]Call(nil), current.calls...)
		cloned.input = schema.CloneMessages(current.input)
		ctx = context.WithValue(ctx, runContextKey{}, &cloned)
	}
	if current, ok := ctx.Value(groupRunContextKey{}).(*groupRunState); ok && current != nil {
		cloned := *current
		cloned.response = schema.CloneMessages(current.response)
		cloned.input = schema.CloneMessages(current.input)
		ctx = context.WithValue(ctx, groupRunContextKey{}, &cloned)
	}
	if current, ok := ctx.Value(routingRunContextKey{}).(*routingRunState); ok && current != nil {
		cloned := *current
		cloned.response = schema.CloneMessages(current.response)
		cloned.pending = append([]Call(nil), current.pending...)
		cloned.dispatchInput = schema.CloneMessages(current.dispatchInput)
		cloned.input = schema.CloneMessages(current.input)
		ctx = context.WithValue(ctx, routingRunContextKey{}, &cloned)
	}
	return ctx
}

func appendMessages(left, right []schema.Message) []schema.Message {
	combined := make([]schema.Message, 0, len(left)+len(right))
	combined = append(combined, schema.CloneMessages(left)...)
	combined = append(combined, schema.CloneMessages(right)...)
	return combined
}
