package agent

import (
	"context"
	"errors"
	"fmt"
	"math/rand"

	"goharness/pkg/schema"
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
	SelectionRoundRobin SelectionMethod = "round_robin"
	SelectionRandom     SelectionMethod = "random"
)

type GroupChatConfig struct {
	BaseConfig
	Agents          []Agent
	SelectionMethod SelectionMethod
}

type GroupChat struct {
	*Base
	hub    *Hub
	method SelectionMethod
}

func NewGroupChat(cfg GroupChatConfig) (*GroupChat, error) {
	base, err := NewBase(cfg.BaseConfig)
	if err != nil {
		return nil, err
	}
	hub, err := NewHub(cfg.Agents)
	if err != nil {
		return nil, err
	}
	method := cfg.SelectionMethod
	if method == "" {
		method = SelectionRoundRobin
	}
	return &GroupChat{Base: base, hub: hub, method: method}, nil
}

func (g *GroupChat) Run(ctx context.Context, messages []schema.Message, opts ...RunOption) (Stream, error) {
	selected := g.selectAgent(messages)
	if selected == nil {
		out := make(chan Chunk)
		close(out)
		return out, nil
	}
	return selected.Run(ctx, messages, opts...)
}

func (g *GroupChat) selectAgent(messages []schema.Message) Agent {
	agents := g.hub.Agents()
	if len(agents) == 0 {
		return nil
	}
	if g.method == SelectionRandom {
		return agents[rand.Intn(len(agents))]
	}

	lastName := ""
	if len(messages) > 0 {
		lastName = messages[len(messages)-1].Name
	}
	for i, a := range agents {
		if a.Name() == lastName {
			return agents[(i+1)%len(agents)]
		}
	}
	return agents[0]
}
