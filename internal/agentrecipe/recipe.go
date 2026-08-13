// Package agentrecipe contains strongly typed Graph recipes used by Agents.
package agentrecipe

import (
	"context"
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
	LLMCalls    int
	Response    []schema.Message
}

// RecipeState is the run-exclusive State used by built-in Agent graphs.
type RecipeState struct {
	Messages []schema.Message
}

func newRecipeGraph(name string) (*graph.Graph[[]schema.Message, []schema.Message], error) {
	g := graph.NewGraph[[]schema.Message, []schema.Message](name)
	if err := g.SetStateFactory(func(_ context.Context, input []schema.Message) (*RecipeState, error) {
		return &RecipeState{Messages: schema.CloneMessages(input)}, nil
	}); err != nil {
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
	batch       ToolBatch
}

type Hooks struct {
	Name              string
	CallModel         func(context.Context, []schema.Message) (runnable.Stream[llm.Event], error)
	CheckBreakpoint   func(context.Context, []schema.Message, []schema.Message, int) error
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
	})
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
		if hooks.CheckBreakpoint != nil {
			if err := hooks.CheckBreakpoint(ctx, messages, schema.CloneMessages(cfg.response), cfg.llmCalls); err != nil {
				return nil, err
			}
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
		graph.WithPreHandler(func(_ context.Context, state *RecipeState) ([]schema.Message, error) {
			return schema.CloneMessages(state.Messages), nil
		}),
		graph.WithPostHandler(func(_ context.Context, state *RecipeState, output []schema.Message) error {
			state.Messages = schema.CloneMessages(output)
			return nil
		}),
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
			batch := cfg.batch
			if batch == nil && hooks.NewToolBatch != nil {
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
			cfg.batch = nil
			emitted := cfg.response
			if hooks.NameMessages != nil {
				emitted = hooks.NameMessages(emitted)
			}
			graph.EmitData(ctx, schema.MessageEvent{Kind: schema.MessageEventSnapshot, Messages: emitted})
			return updated, nil
		})
		if err := g.AddNode("tools", toolNode,
			graph.WithPreHandler(func(_ context.Context, state *RecipeState) ([]schema.Message, error) {
				return schema.CloneMessages(state.Messages), nil
			}),
			graph.WithPostHandler(func(_ context.Context, state *RecipeState, output []schema.Message) error {
				state.Messages = schema.CloneMessages(output)
				return nil
			}),
		); err != nil {
			return nil, err
		}
		if err := g.AddBranch("llm", "tools", "tool_calls", func(ctx context.Context, _ *RecipeState) (bool, error) {
			cfg, err := configFromContext(ctx)
			if err != nil {
				return false, err
			}
			return cfg.hasCalls, nil
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
	if err := g.AddNode("finish", finish, graph.WithPreHandler(func(_ context.Context, state *RecipeState) ([]schema.Message, error) {
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

func withSession(ctx context.Context) context.Context {
	if ctx == nil {
		return nil
	}
	if current, ok := ctx.Value(runContextKey{}).(*toolRunState); ok && current != nil {
		cloned := *current
		cloned.response = schema.CloneMessages(current.response)
		cloned.calls = append([]Call(nil), current.calls...)
		cloned.batch = nil
		ctx = context.WithValue(ctx, runContextKey{}, &cloned)
	}
	if current, ok := ctx.Value(groupRunContextKey{}).(*groupRunState); ok && current != nil {
		cloned := *current
		cloned.response = schema.CloneMessages(current.response)
		ctx = context.WithValue(ctx, groupRunContextKey{}, &cloned)
	}
	if current, ok := ctx.Value(routingRunContextKey{}).(*routingRunState); ok && current != nil {
		cloned := *current
		cloned.response = schema.CloneMessages(current.response)
		cloned.pending = append([]Call(nil), current.pending...)
		cloned.dispatchInput = schema.CloneMessages(current.dispatchInput)
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
