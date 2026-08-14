package agentrecipe

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"goharness/pkg/callback"
	"goharness/pkg/graph"
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
	round         int
	lastAgent     string
	selectedAgent string
	response      []schema.Message
	continueRun   bool
	input         []schema.Message
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
	run.selectedAgent = state.SelectedAgent
	run.response = schema.CloneMessages(state.Response)
	run.continueRun = state.ContinueRun
	return nil
}

func syncGroupState(state *RecipeState, run *groupRunState) {
	state.Response = schema.CloneMessages(run.response)
	state.Round = run.round
	state.LastAgent = run.lastAgent
	state.SelectedAgent = run.selectedAgent
	state.ContinueRun = run.continueRun
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

	g, err := newRecipeGraph(name)
	if err != nil {
		return nil, err
	}
	selectNode := runnable.Func[[]schema.Message, string](func(ctx context.Context, messages []schema.Message, _ ...runnable.Option) (string, error) {
		run, err := groupStateFromContext(ctx)
		if err != nil {
			return "", err
		}
		if run.round >= hooks.MaxRounds {
			return "", nil
		}
		return hooks.Select(ctx, schema.CloneMessages(messages), run.round, run.lastAgent)
	})
	if err := g.AddNode("select", selectNode,
		graph.WithPreHandler(func(ctx context.Context, state *RecipeState) ([]schema.Message, error) {
			if err := hydrateGroupRun(ctx, state); err != nil {
				return nil, err
			}
			return schema.CloneMessages(state.Messages), nil
		}),
		graph.WithPostHandler(func(ctx context.Context, state *RecipeState, selected string) error {
			run, err := groupStateFromContext(ctx)
			if err != nil {
				return err
			}
			run.selectedAgent = selected
			run.continueRun = selected != ""
			syncGroupState(state, run)
			return nil
		}),
	); err != nil {
		return nil, err
	}

	for _, member := range hooks.Members {
		memberName := member.Name
		if err := g.AddNode(groupMemberNodeID(memberName), member.Node,
			graph.WithPreHandler(func(ctx context.Context, state *RecipeState) ([]schema.Message, error) {
				if err := hydrateGroupRun(ctx, state); err != nil {
					return nil, err
				}
				return schema.CloneMessages(state.Messages), nil
			}),
			graph.WithPostHandler(func(ctx context.Context, state *RecipeState, outcome runnable.Outcome[[]schema.Message]) error {
				run, err := groupStateFromContext(ctx)
				if err != nil {
					return err
				}
				if outcome.Err != nil {
					return outcome.Err
				}
				roundOutput := withSpeaker(outcome.Value, memberName)
				callback.EmitStream(ctx, schema.MessageEvent{Kind: schema.MessageEventSnapshot, Messages: appendMessages(run.response, roundOutput)})
				if len(roundOutput) == 0 {
					run.selectedAgent = ""
					run.continueRun = false
					syncGroupState(state, run)
					return nil
				}
				run.response = append(run.response, schema.CloneMessages(roundOutput)...)
				state.Messages = appendMessages(state.Messages, roundOutput)
				run.round++
				run.lastAgent = memberName
				run.selectedAgent = ""
				run.continueRun = run.round < hooks.MaxRounds
				syncGroupState(state, run)
				return nil
			}),
		); err != nil {
			return nil, err
		}
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

	if err := g.AddBranch(graph.Start, "finish", "round_limit", func(ctx context.Context, state *RecipeState) (bool, error) {
		if err := hydrateGroupRun(ctx, state); err != nil {
			return false, err
		}
		run, err := groupStateFromContext(ctx)
		return err == nil && run.round >= hooks.MaxRounds, err
	}); err != nil {
		return nil, err
	}
	if err := g.AddEdge(graph.Start, "select"); err != nil {
		return nil, err
	}
	for _, member := range hooks.Members {
		memberName := member.Name
		if err := g.AddBranch("select", groupMemberNodeID(memberName), "select:"+memberName, func(_ context.Context, state *RecipeState) (bool, error) {
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
		if err := g.AddBranch(from, "select", "next_round", func(_ context.Context, state *RecipeState) (bool, error) {
			return state.ContinueRun, nil
		}); err != nil {
			return nil, err
		}
		if err := g.AddEdge(from, "finish"); err != nil {
			return nil, err
		}
	}
	if err := g.AddEdge("finish", graph.End); err != nil {
		return nil, err
	}
	if err := g.SetMaxSteps(hooks.MaxRounds*2 + 2); err != nil {
		return nil, err
	}
	compiled, err := g.Compile()
	if err != nil {
		return nil, err
	}
	return &Recipe{graph: compiled}, nil
}

func groupMemberNodeID(name string) string { return "agent:" + name }

func withSpeaker(messages []schema.Message, speaker string) []schema.Message {
	result := schema.CloneMessages(messages)
	for i := range result {
		if result[i].Extra == nil {
			result[i].Extra = map[string]any{}
		}
		if _, exists := result[i].Extra["agent_name"]; !exists {
			result[i].Extra["agent_name"] = speaker
		}
	}
	return result
}
