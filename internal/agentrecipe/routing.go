package agentrecipe

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/goharness/goharness/pkg/graph"
	"github.com/goharness/goharness/pkg/llm"
	"github.com/goharness/goharness/pkg/runnable"
	"github.com/goharness/goharness/pkg/schema"
	"github.com/goharness/goharness/pkg/stream"
	"github.com/goharness/goharness/pkg/tool"
)

// RoutingState 是 Routing 配方的运行状态。
type RoutingState struct {
	baseState
	Steps         int              `json:"steps,omitempty"`
	Pending       []Call           `json:"pending,omitempty"`
	DispatchInput []schema.Message `json:"dispatch_input,omitempty"`
	ContinueRun   bool             `json:"continue_run,omitempty"`
}

type routingStateWire struct {
	baseStateWire
	Steps         int              `json:"steps,omitempty"`
	Pending       []Call           `json:"pending,omitempty"`
	DispatchInput []schema.Message `json:"dispatch_input,omitempty"`
	ContinueRun   bool             `json:"continue_run,omitempty"`
}

func (s *RoutingState) MarshalJSON() ([]byte, error) {
	return json.Marshal(routingStateWire{
		baseStateWire: s.baseState.wire(),
		Steps:         s.Steps,
		Pending:       s.Pending,
		DispatchInput: s.DispatchInput,
		ContinueRun:   s.ContinueRun,
	})
}

func (s *RoutingState) UnmarshalJSON(data []byte) error {
	var wire routingStateWire
	if err := json.Unmarshal(data, &wire); err != nil {
		return err
	}
	s.baseState = wire.baseStateWire.toState()
	s.Steps, s.Pending, s.DispatchInput, s.ContinueRun = wire.Steps, wire.Pending, wire.DispatchInput, wire.ContinueRun
	return nil
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
	MaxSteps int
	// ReturnControl 为 true 时是 Supervisor 模式：子 Agent 结果作为观察交还给决策模型。
	ReturnControl bool
	Model         runnable.Runnable[llm.Request, llm.Response]
	PrepareModel  func(context.Context, []schema.Message) (llm.Request, error)
	Agents        []RoutedAgent
	DetectCalls   func([]schema.Message) []Call
	Observation   func(RoutedResult) schema.Message
	NameMessages  func([]schema.Message) []schema.Message
}

func NewRoutingRecipe(name string, hooks RoutingHooks) (*Recipe, error) {
	if hooks.MaxSteps < 1 {
		return nil, errors.New("routing recipe max steps must be positive")
	}
	if hooks.Model == nil || hooks.PrepareModel == nil || hooks.DetectCalls == nil {
		return nil, errors.New("routing recipe requires a model node, model input handler, and call detector")
	}
	if hooks.ReturnControl && hooks.Observation == nil {
		return nil, errors.New("routing recipe observation hook is missing")
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

	r := &Recipe{}
	g, err := newRecipeGraph(name, func(ctx context.Context, input []schema.Message) (*RoutingState, error) {
		return &RoutingState{baseState: newBaseState(input, r.config(ctx))}, nil
	})
	if err != nil {
		return nil, err
	}
	if err := g.AddNode("decide", hooks.Model,
		graph.WithPreHandler(func(ctx context.Context, state *RoutingState) (llm.Request, error) {
			return hooks.PrepareModel(ctx, state.Messages)
		}),
		graph.WithPostHandler(func(_ context.Context, state *RoutingState, response llm.Response) error {
			output := response.Messages
			state.Steps++
			// DispatchInput 是派发前的只读视图，共享不可变元素即可。
			state.DispatchInput = append([]schema.Message(nil), state.Messages...)
			state.Messages = append(state.Messages, output...)
			calls := hooks.DetectCalls(output)
			if !hooks.ReturnControl && len(calls) > 1 {
				calls = calls[:1]
			}
			state.Pending = cloneCalls(calls)
			if len(calls) == 0 {
				state.Response = append(state.Response, output...)
				state.DispatchInput = nil
				state.ContinueRun = false
			} else {
				state.ContinueRun = true
			}
			return nil
		}),
	); err != nil {
		return nil, err
	}

	postAgent := func(ctx context.Context, state *RoutingState, outcome runnable.Outcome[[]schema.Message]) error {
		if len(state.Pending) == 0 {
			return errors.New("routing agent completed without a pending call")
		}
		call := state.Pending[0]
		result := RoutedResult{Call: call, Messages: withSpeaker(outcome.Value, call.Name), Err: outcome.Err}
		if result.Err != nil && !hooks.ReturnControl {
			return fmt.Errorf("routed agent %q: %w", call.Name, result.Err)
		}
		state.Pending = cloneCalls(state.Pending[1:])
		if !hooks.ReturnControl {
			// result.Messages 已由 withSpeaker 克隆并标记来源，直接接管。
			state.Messages = append(state.Messages, result.Messages...)
			state.Response = result.Messages
			state.Pending = nil
			state.DispatchInput = nil
			state.ContinueRun = false
			return emitRoutingSnapshot(ctx, hooks, state.Response)
		}
		observation := hooks.Observation(result)
		state.Messages = append(state.Messages, observation)
		state.Response = append(state.Response, observation)
		if len(state.Pending) == 0 {
			state.DispatchInput = nil
		}
		state.ContinueRun = len(state.Pending) > 0 || state.Steps < hooks.MaxSteps
		return emitRoutingSnapshot(ctx, hooks, state.Response)
	}
	dispatchInput := func(_ context.Context, state *RoutingState) ([]schema.Message, error) {
		return schema.CloneMessages(state.DispatchInput), nil
	}
	for _, candidate := range hooks.Agents {
		if err := g.AddNode(routedAgentNodeID(candidate.Name), candidate.Node,
			graph.WithPreHandler(dispatchInput),
			graph.WithPostHandler(postAgent),
		); err != nil {
			return nil, err
		}
	}

	unroutable := runnable.Func[Call, runnable.Outcome[[]schema.Message]](func(_ context.Context, call Call, _ ...runnable.Option) (runnable.Outcome[[]schema.Message], error) {
		// 未注册的子代理名是模型可自纠的反馈，标记为模型可见。
		return runnable.Outcome[[]schema.Message]{Err: tool.ModelVisible(fmt.Errorf("routing model selected unregistered sub-agent %q", call.Name))}, nil
	})
	if err := g.AddNode("unroutable", unroutable,
		graph.WithPreHandler(func(_ context.Context, state *RoutingState) (Call, error) {
			if len(state.Pending) == 0 {
				return Call{}, errors.New("routing call is missing")
			}
			return state.Pending[0], nil
		}),
		graph.WithPostHandler(postAgent),
	); err != nil {
		return nil, err
	}

	if err := addFinishNode(g, func(ctx context.Context, state *RoutingState) []schema.Message {
		response := state.Response
		if hooks.NameMessages != nil {
			response = hooks.NameMessages(response)
		}
		state.Response = response
		return response
	}); err != nil {
		return nil, err
	}

	if err := graph.AddBranch(g, graph.Start, "finish", "step_limit", func(_ context.Context, state *RoutingState) (bool, error) {
		return state.Steps >= hooks.MaxSteps, nil
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
	if err := g.SetMaxSteps(hooks.MaxSteps*(len(hooks.Agents)+2) + 2); err != nil {
		return nil, err
	}
	compiled, err := g.Compile()
	if err != nil {
		return nil, err
	}
	r.graph = compiled
	return r, nil
}

func addRoutingEdges(g *graph.Graph[[]schema.Message, []schema.Message], from string, agents []RoutedAgent) error {
	for _, candidate := range agents {
		name := candidate.Name
		if err := graph.AddBranch(g, from, routedAgentNodeID(name), "route:"+name, func(_ context.Context, state *RoutingState) (bool, error) {
			return len(state.Pending) > 0 && state.Pending[0].Name == name, nil
		}); err != nil {
			return err
		}
	}
	if err := graph.AddBranch(g, from, "unroutable", "unregistered", func(_ context.Context, state *RoutingState) (bool, error) {
		return len(state.Pending) > 0, nil
	}); err != nil {
		return err
	}
	if from != "decide" {
		if err := graph.AddBranch(g, from, "decide", "continue", func(_ context.Context, state *RoutingState) (bool, error) {
			return state.ContinueRun, nil
		}); err != nil {
			return err
		}
	}
	return g.AddEdge(from, "finish")
}

func emitRoutingSnapshot(ctx context.Context, hooks RoutingHooks, response []schema.Message) error {
	stream.EnsureMessageIDs(response)
	// NameMessages 返回新视图；发布层（Snapshot/PublishSnapshots）各自
	// 在订阅边界克隆，这里不预克隆。
	emitted := response
	if hooks.NameMessages != nil {
		emitted = hooks.NameMessages(emitted)
	}
	return stream.PublishSnapshots(ctx, emitted)
}

func routedAgentNodeID(name string) string { return "agent:" + name }
