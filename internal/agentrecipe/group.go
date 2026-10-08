package agentrecipe

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/goharness/goharness/internal/streamcompat"
	"github.com/goharness/goharness/pkg/graph"
	"github.com/goharness/goharness/pkg/runnable"
	"github.com/goharness/goharness/pkg/schema"
	"github.com/goharness/goharness/pkg/stream"
)

// GroupState 是 GroupChat 配方的运行状态。
type GroupState struct {
	baseState
	Round         int    `json:"round,omitempty"`
	LastAgent     string `json:"last_agent,omitempty"`
	SelectedAgent string `json:"selected_agent,omitempty"`
	ContinueRun   bool   `json:"continue_run,omitempty"`
}

type groupStateWire struct {
	baseStateWire
	Round         int    `json:"round,omitempty"`
	LastAgent     string `json:"last_agent,omitempty"`
	SelectedAgent string `json:"selected_agent,omitempty"`
	ContinueRun   bool   `json:"continue_run,omitempty"`
}

func (s *GroupState) MarshalJSON() ([]byte, error) {
	return json.Marshal(groupStateWire{
		baseStateWire: s.baseState.wire(),
		Round:         s.Round,
		LastAgent:     s.LastAgent,
		SelectedAgent: s.SelectedAgent,
		ContinueRun:   s.ContinueRun,
	})
}

func (s *GroupState) UnmarshalJSON(data []byte) error {
	var wire groupStateWire
	if err := json.Unmarshal(data, &wire); err != nil {
		return err
	}
	s.baseState = wire.baseStateWire.toState()
	s.Round, s.LastAgent, s.SelectedAgent, s.ContinueRun = wire.Round, wire.LastAgent, wire.SelectedAgent, wire.ContinueRun
	return nil
}

// groupSelection 是 select 节点的输入，由 PreHandler 从 State 构造。
type groupSelection struct {
	Messages  []schema.Message
	Round     int
	LastAgent string
}

type GroupMember struct {
	Name string
	Node runnable.Runnable[[]schema.Message, runnable.Outcome[[]schema.Message]]
}

type GroupHooks struct {
	MaxRounds int
	Members   []GroupMember
	Select    func(context.Context, []schema.Message, int, string) (string, error)
}

func NewGroupRecipe(name string, hooks GroupHooks) (*Recipe, error) {
	if hooks.MaxRounds < 1 {
		return nil, errors.New("group recipe max rounds must be positive")
	}
	if hooks.Select == nil {
		return nil, errors.New("group recipe selector is required")
	}
	seen := make(map[string]struct{}, len(hooks.Members))
	for index, member := range hooks.Members {
		member.Name = strings.TrimSpace(member.Name)
		if member.Name == "" || member.Node == nil {
			return nil, fmt.Errorf("group member %d is not configured", index)
		}
		if _, exists := seen[member.Name]; exists {
			return nil, fmt.Errorf("duplicate group member %q", member.Name)
		}
		seen[member.Name] = struct{}{}
	}

	r := &Recipe{}
	g, err := newRecipeGraph(name, func(ctx context.Context, input []schema.Message) (*GroupState, error) {
		return &GroupState{baseState: newBaseState(input, r.config(ctx))}, nil
	})
	if err != nil {
		return nil, err
	}
	selectNode := runnable.Func[groupSelection, string](func(ctx context.Context, selection groupSelection, _ ...runnable.Option) (string, error) {
		if selection.Round >= hooks.MaxRounds {
			return "", nil
		}
		return hooks.Select(ctx, schema.CloneMessages(selection.Messages), selection.Round, selection.LastAgent)
	})
	if err := g.AddNode("select", selectNode,
		graph.WithPreHandler(func(_ context.Context, state *GroupState) (groupSelection, error) {
			// 选择视图在图内只读传递；Select 是外部钩子，调用前才克隆。
			return groupSelection{Messages: state.Messages, Round: state.Round, LastAgent: state.LastAgent}, nil
		}),
		graph.WithPostHandler(func(_ context.Context, state *GroupState, selected string) error {
			state.SelectedAgent = selected
			state.ContinueRun = selected != ""
			return nil
		}),
	); err != nil {
		return nil, err
	}

	for _, member := range hooks.Members {
		memberName := member.Name
		if err := g.AddNode(groupMemberNodeID(memberName), member.Node,
			graph.WithPreHandler(func(_ context.Context, state *GroupState) ([]schema.Message, error) {
				return schema.CloneMessages(state.Messages), nil
			}),
			graph.WithPostHandler(func(ctx context.Context, state *GroupState, outcome runnable.Outcome[[]schema.Message]) error {
				if outcome.Err != nil {
					return outcome.Err
				}
				roundOutput := withSpeaker(outcome.Value, memberName)
				stream.EnsureMessageIDs(roundOutput)
				streamcompat.Snapshot(ctx, concatMessages(state.Response, roundOutput))
				if err := stream.PublishSnapshots(ctx, roundOutput); err != nil {
					return err
				}
				state.SelectedAgent = ""
				if len(roundOutput) == 0 {
					state.ContinueRun = false
					return nil
				}
				state.Response = append(state.Response, roundOutput...)
				state.Messages = append(state.Messages, roundOutput...)
				state.Round++
				state.LastAgent = memberName
				state.ContinueRun = state.Round < hooks.MaxRounds
				return nil
			}),
		); err != nil {
			return nil, err
		}
	}

	if err := addFinishNode(g, func(ctx context.Context, state *GroupState) []schema.Message {
		return state.Response
	}); err != nil {
		return nil, err
	}

	if err := graph.AddBranch(g, graph.Start, "finish", "round_limit", func(_ context.Context, state *GroupState) (bool, error) {
		return state.Round >= hooks.MaxRounds, nil
	}); err != nil {
		return nil, err
	}
	if err := g.AddEdge(graph.Start, "select"); err != nil {
		return nil, err
	}
	for _, member := range hooks.Members {
		memberName := member.Name
		if err := graph.AddBranch(g, "select", groupMemberNodeID(memberName), "select:"+memberName, func(_ context.Context, state *GroupState) (bool, error) {
			return state.SelectedAgent == memberName, nil
		}); err != nil {
			return nil, err
		}
	}
	if err := g.AddEdge("select", "finish"); err != nil {
		return nil, err
	}
	for _, member := range hooks.Members {
		from := groupMemberNodeID(member.Name)
		if err := graph.AddBranch(g, from, "select", "next_round", func(_ context.Context, state *GroupState) (bool, error) {
			return state.ContinueRun, nil
		}); err != nil {
			return nil, err
		}
		if err := g.AddEdge(from, "finish"); err != nil {
			return nil, err
		}
	}
	if err := g.SetMaxSteps(hooks.MaxRounds*2 + 2); err != nil {
		return nil, err
	}
	compiled, err := g.Compile()
	if err != nil {
		return nil, err
	}
	r.graph = compiled
	return r, nil
}

func groupMemberNodeID(name string) string { return "agent:" + name }

// withSpeaker 为子 Agent 输出标记来源：只克隆需要写入 Extra 的消息，
// 已标记的消息原样共享（不可变约定），不预克隆整个输出。
func withSpeaker(messages []schema.Message, speaker string) []schema.Message {
	result := make([]schema.Message, len(messages))
	for i, message := range messages {
		if _, exists := message.Extra["agent_name"]; exists {
			result[i] = message
			continue
		}
		m := schema.CloneMessages([]schema.Message{message})[0]
		if m.Extra == nil {
			m.Extra = map[string]any{}
		}
		m.Extra["agent_name"] = speaker
		result[i] = m
	}
	return result
}
