package framework

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"

	"goharness/pkg/agent"
	"goharness/pkg/schema"
)

type App struct {
	name         string
	defaultAgent string

	mu     sync.RWMutex
	agents map[string]agent.Agent
}

func NewApp(name string, agents ...agent.Agent) (*App, error) {
	app := &App{
		name:   name,
		agents: map[string]agent.Agent{},
	}
	for _, a := range agents {
		if err := app.AddAgent(a); err != nil {
			return nil, err
		}
	}
	return app, nil
}

func (a *App) Name() string {
	return a.name
}

func (a *App) AddAgent(agent agent.Agent) error {
	if agent == nil {
		return errors.New("agent cannot be nil")
	}
	name := agent.Name()
	if name == "" {
		return errors.New("agent name cannot be empty")
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	if _, exists := a.agents[name]; exists {
		return fmt.Errorf("agent %q is already registered", name)
	}
	a.agents[name] = agent
	if a.defaultAgent == "" {
		a.defaultAgent = name
	}
	return nil
}

func (a *App) SetDefaultAgent(name string) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if _, ok := a.agents[name]; !ok {
		return fmt.Errorf("agent %q is not registered", name)
	}
	a.defaultAgent = name
	return nil
}

func (a *App) Agent(name string) (agent.Agent, bool) {
	a.mu.RLock()
	defer a.mu.RUnlock()

	got, ok := a.agents[name]
	return got, ok
}

func (a *App) AgentNames() []string {
	a.mu.RLock()
	defer a.mu.RUnlock()

	names := make([]string, 0, len(a.agents))
	for name := range a.agents {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func (a *App) Run(ctx context.Context, messages []schema.Message, opts ...agent.RunOption) (agent.Stream, error) {
	a.mu.RLock()
	defaultName := a.defaultAgent
	defaultAgent := a.agents[defaultName]
	a.mu.RUnlock()

	if defaultAgent == nil {
		return nil, errors.New("app has no default agent")
	}
	return defaultAgent.Run(ctx, messages, opts...)
}

func (a *App) RunAgent(ctx context.Context, name string, messages []schema.Message, opts ...agent.RunOption) (agent.Stream, error) {
	selected, ok := a.Agent(name)
	if !ok {
		return nil, fmt.Errorf("agent %q is not registered", name)
	}
	return selected.Run(ctx, messages, opts...)
}

func (a *App) RunSync(ctx context.Context, messages []schema.Message, opts ...agent.RunOption) ([]schema.Message, error) {
	stream, err := a.Run(ctx, messages, opts...)
	if err != nil {
		return nil, err
	}
	return agent.Collect(stream)
}

func (a *App) RunAgentSync(ctx context.Context, name string, messages []schema.Message, opts ...agent.RunOption) ([]schema.Message, error) {
	stream, err := a.RunAgent(ctx, name, messages, opts...)
	if err != nil {
		return nil, err
	}
	return agent.Collect(stream)
}
