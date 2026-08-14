// Package agentrecipe contains strongly typed Graph recipes used by Agents.
package agentrecipe

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"goharness/pkg/callback"
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
	Call          Call
	ContentBlocks []schema.ContentBlock
	Err           error
}

type RunConfig struct {
	MaxLLMCalls int
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
	SelectedAgent string           `json:"selected_agent,omitempty"`
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
	llmCalls    int
	response    []schema.Message
	calls       []Call
	hasCalls    bool
	input       []schema.Message
}

type Hooks struct {
	Model             runnable.Runnable[llm.Request, llm.Response]
	Tools             runnable.Runnable[[]schema.Message, []schema.Message]
	PrepareModel      func(context.Context, []schema.Message) (llm.Request, error)
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
		state.SelectedAgent = run.selectedAgent
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
	graph *graph.CompiledGraph[[]schema.Message, []schema.Message]
}

func (r *Recipe) Snapshot() graph.Snapshot {
	if r == nil || r.graph == nil {
		return graph.Snapshot{}
	}
	return r.graph.Snapshot()
}

func NewToolRecipe(name string, hooks Hooks, tools bool) (*Recipe, error) {
	if hooks.Model == nil {
		return nil, errors.New("agent recipe requires a model node")
	}
	if hooks.PrepareModel == nil {
		return nil, errors.New("agent recipe requires a model input handler")
	}
	if tools && hooks.Tools == nil {
		return nil, errors.New("agent recipe requires a tools node")
	}
	g, err := newRecipeGraph(name)
	if err != nil {
		return nil, err
	}
	if err := g.AddNode("llm", hooks.Model,
		graph.WithPreHandler(func(ctx context.Context, state *RecipeState) (llm.Request, error) {
			if err := hydrateToolRun(ctx, state); err != nil {
				return llm.Request{}, err
			}
			cfg, err := configFromContext(ctx)
			if err != nil {
				return llm.Request{}, err
			}
			if cfg.llmCalls == 0 && len(cfg.response) > 0 && hooks.RehydrateLLMCalls != nil {
				cfg.llmCalls = hooks.RehydrateLLMCalls(cfg.response)
				state.LLMCalls = cfg.llmCalls
			}
			return hooks.PrepareModel(ctx, schema.CloneMessages(state.Messages))
		}),
		graph.WithPostHandler(func(ctx context.Context, state *RecipeState, response llm.Response) error {
			cfg, err := configFromContext(ctx)
			if err != nil {
				return err
			}
			output := schema.CloneMessages(response.Messages)
			cfg.llmCalls++
			cfg.response = append(cfg.response, schema.CloneMessages(output)...)
			updated := appendMessages(state.Messages, output)
			calls := []Call{}
			if hooks.DetectCalls != nil {
				calls = hooks.DetectCalls(output)
			}
			cfg.calls = append([]Call(nil), calls...)
			cfg.hasCalls = tools && len(calls) > 0 && cfg.llmCalls < cfg.maxLLMCalls
			state.Messages = updated
			state.Response = schema.CloneMessages(cfg.response)
			state.LLMCalls = cfg.llmCalls
			state.Calls = cloneCalls(cfg.calls)
			state.HasCalls = cfg.hasCalls
			return nil
		}),
	); err != nil {
		return nil, err
	}
	if err := g.AddBranch(graph.Start, "finish", "llm_limit", func(ctx context.Context, state *RecipeState) (bool, error) {
		if err := hydrateToolRun(ctx, state); err != nil {
			return false, err
		}
		cfg, err := configFromContext(ctx)
		if err != nil {
			return false, err
		}
		if cfg.llmCalls == 0 && len(cfg.response) > 0 && hooks.RehydrateLLMCalls != nil {
			cfg.llmCalls = hooks.RehydrateLLMCalls(cfg.response)
			state.LLMCalls = cfg.llmCalls
		}
		return cfg.llmCalls >= cfg.maxLLMCalls, nil
	}); err != nil {
		return nil, err
	}
	if err := g.AddEdge(graph.Start, "llm"); err != nil {
		return nil, err
	}

	if tools {
		if err := g.AddNode("tools", hooks.Tools,
			graph.WithPreHandler(func(ctx context.Context, state *RecipeState) ([]schema.Message, error) {
				if err := hydrateToolRun(ctx, state); err != nil {
					return nil, err
				}
				cfg, err := configFromContext(ctx)
				if err != nil {
					return nil, err
				}
				return toolNodeInput(ctx, state.Messages, cfg.calls), nil
			}),
			graph.WithPostHandler(func(ctx context.Context, state *RecipeState, results []schema.Message) error {
				cfg, err := configFromContext(ctx)
				if err != nil {
					return err
				}
				calls := cloneCalls(cfg.calls)
				if len(results) != len(calls) {
					return fmt.Errorf("tools node returned %d results for %d calls", len(results), len(calls))
				}
				observations := make([]schema.Message, 0, len(results))
				for index, message := range results {
					if hooks.Observation == nil {
						return errors.New("agent recipe observation hook is missing")
					}
					result := ToolResult{Call: calls[index]}
					if typed := message.FunctionToolResults(); len(typed) == 1 {
						result.ContentBlocks = append([]schema.ContentBlock(nil), typed[0].ContentBlocks...)
						if typed[0].IsError {
							result.Err = errors.New(joinTextBlocks(typed[0].ContentBlocks))
						}
					} else if failed, _ := message.Extra["tool_error"].(bool); failed {
						result.Err = errors.New(message.JoinedText(""))
					}
					observations = append(observations, hooks.Observation(result))
				}
				updated := appendMessages(state.Messages, observations)
				cfg.response = append(cfg.response, schema.CloneMessages(observations)...)
				cfg.hasCalls = false
				cfg.calls = nil
				state.Messages = updated
				state.Response = schema.CloneMessages(cfg.response)
				state.Calls = nil
				state.HasCalls = false
				emitted := cfg.response
				if hooks.NameMessages != nil {
					emitted = hooks.NameMessages(emitted)
				}
				callback.EmitStream(ctx, schema.MessageEvent{Kind: schema.MessageEventSnapshot, Messages: emitted})
				return nil
			}),
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
	compiled, err := g.Compile()
	if err != nil {
		return nil, err
	}
	return &Recipe{graph: compiled}, nil
}

func joinTextBlocks(blocks []schema.ContentBlock) string {
	parts := make([]string, 0)
	for _, block := range blocks {
		if block.Type == schema.ContentBlockTypeText && block.Text != nil {
			parts = append(parts, block.Text.Text)
		}
	}
	return strings.Join(parts, "")
}

func toolNodeInput(ctx context.Context, messages []schema.Message, calls []Call) []schema.Message {
	prepared := schema.CloneMessages(messages)
	if len(calls) == 0 {
		return prepared
	}
	separateTail := len(prepared) >= len(calls)
	if separateTail {
		start := len(prepared) - len(calls)
		for index, call := range calls {
			blocks := prepared[start+index].FunctionToolCalls()
			if !call.Structured || len(blocks) != 1 || blocks[0].Name != call.Name {
				separateTail = false
				break
			}
		}
	}
	structuredTail := !separateTail && len(prepared) > 0
	if structuredTail {
		last := prepared[len(prepared)-1]
		blocks := last.FunctionToolCalls()
		structuredTail = len(blocks) == len(calls)
		for index, call := range calls {
			if !call.Structured || !structuredTail || blocks[index].Name != call.Name {
				structuredTail = false
				break
			}
		}
	}
	metadata := callback.MetadataFromContext(ctx)
	if separateTail {
		start := len(prepared) - len(calls)
		for index, call := range calls {
			prepared[start+index].Extra = callback.CloneMetadata(prepared[start+index].Extra)
			for key, value := range metadata {
				prepared[start+index].Extra[key] = value
			}
			prepared[start+index].Extra["function_id"] = call.FunctionID
			prepared[start+index].Extra["tool_call_id"] = call.FunctionID
		}
		return prepared
	}
	if structuredTail {
		last := prepared[len(prepared)-1]
		prepared = prepared[:len(prepared)-1]
		for _, call := range calls {
			message := schema.NewMessage(schema.RoleAssistant, schema.NewFunctionToolCallBlock(call.FunctionID, call.Name, call.Arguments))
			message.Extra = callback.CloneMetadata(last.Extra)
			for key, value := range metadata {
				message.Extra[key] = value
			}
			message.Extra["function_id"] = call.FunctionID
			message.Extra["tool_call_id"] = call.FunctionID
			prepared = append(prepared, message)
		}
		return prepared
	}
	for _, call := range calls {
		extra := callback.CloneMetadata(metadata)
		extra["function_id"] = call.FunctionID
		extra["tool_call_id"] = call.FunctionID
		message := schema.NewMessage(schema.RoleAssistant, schema.NewFunctionToolCallBlock(call.FunctionID, call.Name, call.Arguments))
		message.Extra = extra
		prepared = append(prepared, message)
	}
	return prepared
}

func (r *Recipe) Invoke(ctx context.Context, messages []schema.Message, options ...runnable.Option) ([]schema.Message, error) {
	if r == nil || r.graph == nil {
		return nil, errors.New("agent recipe is not configured")
	}
	ctx = withSession(ctx)
	return r.graph.Invoke(ctx, messages, options...)
}

func (r *Recipe) Stream(ctx context.Context, messages []schema.Message, options ...runnable.Option) (*runnable.StreamReader[[]schema.Message], error) {
	if r == nil || r.graph == nil {
		return nil, errors.New("agent recipe is not configured")
	}
	ctx = withSession(ctx)
	return r.graph.Stream(ctx, messages, options...)
}

func (r *Recipe) Resume(ctx context.Context, options ...runnable.Option) ([]schema.Message, error) {
	if r == nil || r.graph == nil {
		return nil, errors.New("agent recipe is not configured")
	}
	ctx = withSession(ctx)
	return r.graph.Resume(ctx, options...)
}

func (r *Recipe) ResumeStream(ctx context.Context, options ...runnable.Option) (*runnable.StreamReader[[]schema.Message], error) {
	if r == nil || r.graph == nil {
		return nil, errors.New("agent recipe is not configured")
	}
	ctx = withSession(ctx)
	return r.graph.ResumeStream(ctx, options...)
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
