package agent

import (
	"context"
	"errors"
	"fmt"
	"math/rand"

	"goharness/internal/agentrecipe"
	"goharness/pkg/graph"
	"goharness/pkg/runnable"
	"goharness/pkg/schema"
	"goharness/pkg/state"
)

type Hub struct {
	agents []Agent
}

func NewHub(agents []Agent) (*Hub, error) {
	if len(agents) == 0 {
		return nil, errors.New("hub requires at least one agent")
	}

	seen := map[string]struct{}{}
	for _, a := range agents {
		if a == nil {
			return nil, errors.New("hub agent cannot be nil")
		}
		name := a.Name()
		if name == "" {
			return nil, errors.New("hub agents must have names")
		}
		if _, ok := seen[name]; ok {
			return nil, fmt.Errorf("duplicate agent name %q", name)
		}
		seen[name] = struct{}{}
	}
	return &Hub{agents: append([]Agent(nil), agents...)}, nil
}

func (h *Hub) Agents() []Agent {
	return append([]Agent(nil), h.agents...)
}

func (h *Hub) AgentNames() []string {
	names := make([]string, 0, len(h.agents))
	for _, a := range h.agents {
		names = append(names, a.Name())
	}
	return names
}

type SelectionMethod string

const (
	SelectionRoundRobin       SelectionMethod = "round_robin"
	SelectionRandom           SelectionMethod = "random"
	DefaultGroupChatMaxRounds                 = 3
	MaxGroupChatRounds                        = 1000
)

type SelectionState struct {
	Round     int
	LastAgent string
	Messages  []schema.Message
}

type AgentSelector interface {
	Select(ctx context.Context, agents []Agent, state SelectionState) (Agent, error)
}

type AgentSelectorFunc func(ctx context.Context, agents []Agent, state SelectionState) (Agent, error)

func (f AgentSelectorFunc) Select(ctx context.Context, agents []Agent, state SelectionState) (Agent, error) {
	return f(ctx, agents, state)
}

type RoundRobinSelector struct{}

func (RoundRobinSelector) Select(ctx context.Context, agents []Agent, state SelectionState) (Agent, error) {
	if len(agents) == 0 {
		return nil, nil
	}
	for i, candidate := range agents {
		if candidate.Name() == state.LastAgent {
			return agents[(i+1)%len(agents)], nil
		}
	}
	return agents[0], nil
}

type RandomSelector struct{}

func (RandomSelector) Select(ctx context.Context, agents []Agent, state SelectionState) (Agent, error) {
	if len(agents) == 0 {
		return nil, nil
	}
	return agents[rand.Intn(len(agents))], nil
}

type GroupChatConfig struct {
	BaseConfig
	Agents          []Agent
	SelectionMethod SelectionMethod
	Selector        AgentSelector
	MaxRounds       int
}

type GroupChat struct {
	*Base
	hub       *Hub
	selector  AgentSelector
	maxRounds int
	recipe    *agentrecipe.Recipe
}

func NewGroupChat(cfg GroupChatConfig, opts ...Option) (*GroupChat, error) {
	base, err := NewBase(cfg.BaseConfig, opts...)
	if err != nil {
		return nil, err
	}
	hub, err := NewHub(cfg.Agents)
	if err != nil {
		return nil, err
	}
	selector := cfg.Selector
	if selector == nil {
		var err error
		selector, err = selectorForMethod(cfg.SelectionMethod)
		if err != nil {
			return nil, err
		}
	}
	maxRounds := cfg.MaxRounds
	if maxRounds <= 0 {
		maxRounds = DefaultGroupChatMaxRounds
	}
	if maxRounds > MaxGroupChatRounds {
		return nil, fmt.Errorf("group chat max rounds cannot exceed %d", MaxGroupChatRounds)
	}
	g := &GroupChat{Base: base, hub: hub, selector: selector, maxRounds: maxRounds}
	recipe, err := agentrecipe.NewGroupRecipe(string(AgentKindGroupChat), agentrecipe.GroupHooks{
		MaxRounds: maxRounds,
		CheckBreakpoint: func(ctx context.Context, messages, response []schema.Message, round int, lastAgent string) error {
			runOpts, _ := resolvedRunOptionsFromContext(ctx)
			return g.checkBreakpoint(ctx, runOpts.session, runOpts, AgentKindGroupChat, BreakpointBeforeRound, messages, response, round, map[string]any{"last_agent": lastAgent}, &state.EngineCheckpoint{Round: round, LastAgent: lastAgent})
		},
		Dispatch: func(ctx context.Context, messages []schema.Message, round int, lastAgent string) (string, runnable.Stream[schema.MessageEvent], error) {
			selected, err := g.selectAgent(ctx, SelectionState{Round: round, LastAgent: lastAgent, Messages: schema.CloneMessages(messages)})
			if err != nil || selected == nil {
				return "", nil, err
			}
			runOpts, _ := resolvedRunOptionsFromContext(ctx)
			childOpts := []RunOption{WithCheckpointBypass(), withInterruptBypass()}
			if runOpts.DeltaStream {
				childOpts = append(childOpts, WithDeltaStream(true))
			}
			childRunnableOpts := runOptionsAsRunnable(childOpts)
			childRunnableOpts = append(childRunnableOpts, runOpts.GlobalOptions...)
			childRunnableOpts = append(childRunnableOpts, runOpts.NodeOptions...)
			childCtx := graph.WithDataSinkIsolated(ctx, nil)
			childCtx = graph.WithEventSinkIsolated(childCtx, nil)
			stream, err := selected.Stream(childCtx, schema.CloneMessages(messages), childRunnableOpts...)
			if err != nil {
				return selected.Name(), nil, err
			}
			return selected.Name(), stream, nil
		},
	})
	if err != nil {
		return nil, err
	}
	g.recipe = recipe
	return g, nil
}

func (g *GroupChat) stream(ctx context.Context, messages []schema.Message, opts ...RunOption) (runnable.Stream[schema.MessageEvent], error) {
	runOpts := resolveRunOptions(g.limits, opts...)
	runOpts.agentKind = AgentKindGroupChat
	loaded, session, err := g.loadConversation(ctx, messages, &runOpts)
	if err != nil {
		return nil, err
	}
	commitResponse, resumeCommit, err := resumeAtCommit(&runOpts, AgentKindGroupChat)
	if err != nil {
		return nil, err
	}
	ctx = bindRunContext(ctx, session, &runOpts)
	prepared := g.prepareMessages(loaded, &runOpts)
	runCtx, lifecycle, err := g.beginRun(ctx, prepared, runOpts)
	if err != nil {
		return nil, err
	}
	lifecycle.session = session
	runOpts.session = session
	if resumeCommit {
		return lifecycle.wrap(completedResumeEvents(commitResponse), runOpts.StreamBufferSize), nil
	}
	continuation, resumed, err := continuationFor(runOpts, AgentKindGroupChat, BreakpointBeforeRound)
	if err != nil {
		lifecycle.fail(err)
		return nil, err
	}
	response := []schema.Message{}
	lastAgent := ""
	startRound := 0
	if resumed {
		response = schema.CloneMessages(continuation.Response)
		startRound = continuation.LLMCalls
		if continuation.Engine != nil {
			startRound = continuation.Engine.Round
			lastAgent = continuation.Engine.LastAgent
		}
	}
	runCtx = withResolvedRunOptions(runCtx, runOpts)
	runCtx = agentrecipe.WithGroupRunConfig(runCtx, agentrecipe.GroupRunConfig{Round: startRound, LastAgent: lastAgent, Response: response})
	stream, err := g.recipe.Stream(runCtx, prepared)
	if err != nil {
		lifecycle.fail(err)
		return nil, err
	}
	stream, err = prefetchMessageEvents(runCtx, stream)
	if err != nil {
		lifecycle.fail(err)
		return nil, err
	}
	return lifecycle.wrap(stream, runOpts.StreamBufferSize), nil
}

func (g *GroupChat) selectAgent(ctx context.Context, state SelectionState) (Agent, error) {
	agents := g.hub.Agents()
	selected, err := g.selector.Select(ctx, agents, state)
	if err != nil || selected == nil {
		return selected, err
	}
	for _, candidate := range agents {
		if candidate.Name() == selected.Name() {
			return candidate, nil
		}
	}
	return nil, fmt.Errorf("selector returned unregistered agent %q", selected.Name())
}

func selectorForMethod(method SelectionMethod) (AgentSelector, error) {
	switch method {
	case "", SelectionRoundRobin:
		return RoundRobinSelector{}, nil
	case SelectionRandom:
		return RandomSelector{}, nil
	default:
		return nil, fmt.Errorf("unsupported group chat selection method %q", method)
	}
}

func withSpeakerName(messages []schema.Message, speaker string) []schema.Message {
	named := schema.CloneMessages(messages)
	for i := range named {
		if named[i].Name == "" {
			named[i].Name = speaker
		}
	}
	return named
}
