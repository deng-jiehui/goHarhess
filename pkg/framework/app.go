package framework

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"

	"goharness/pkg/runnable"
	"goharness/pkg/schema"
)

type App struct {
	name         string
	defaultAgent string

	mu     sync.RWMutex
	agents map[string]runnable.NamedRunnable[[]schema.Message, []schema.Message]
}

func NewApp(name string, agents ...runnable.NamedRunnable[[]schema.Message, []schema.Message]) (*App, error) {
	app := &App{
		name:   name,
		agents: map[string]runnable.NamedRunnable[[]schema.Message, []schema.Message]{},
	}
	for _, a := range agents {
		if err := app.AddRunnable(a); err != nil {
			return nil, err
		}
	}
	return app, nil
}

func (a *App) Name() string {
	return a.name
}

func (a *App) Description() string {
	return "application runnable"
}

func (a *App) AddRunnable(value runnable.NamedRunnable[[]schema.Message, []schema.Message]) error {
	if value == nil {
		return errors.New("runnable cannot be nil")
	}
	name := value.Name()
	if name == "" {
		return errors.New("runnable name cannot be empty")
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	if _, exists := a.agents[name]; exists {
		return fmt.Errorf("agent %q is already registered", name)
	}
	a.agents[name] = value
	if a.defaultAgent == "" {
		a.defaultAgent = name
	}
	return nil
}

func (a *App) Add(value runnable.NamedRunnable[[]schema.Message, []schema.Message]) error {
	return a.AddRunnable(value)
}

// AddValue registers an unnamed message Runnable under an explicit app name.
// This keeps naming metadata at the registry boundary while the component
// itself only needs to implement Runnable.
func (a *App) AddValue(name, description string, value runnable.Runnable[[]schema.Message, []schema.Message]) error {
	if value == nil {
		return errors.New("runnable cannot be nil")
	}
	return a.AddRunnable(namedValue{name: name, description: description, value: value})
}

type namedValue struct {
	name        string
	description string
	value       runnable.Runnable[[]schema.Message, []schema.Message]
}

func (v namedValue) Name() string        { return v.name }
func (v namedValue) Description() string { return v.description }
func (v namedValue) Invoke(ctx context.Context, input []schema.Message, options ...runnable.Option) ([]schema.Message, error) {
	return v.value.Invoke(ctx, input, options...)
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

func (a *App) Runnable(name string) (runnable.NamedRunnable[[]schema.Message, []schema.Message], bool) {
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

func (a *App) Invoke(ctx context.Context, messages []schema.Message, options ...runnable.Option) ([]schema.Message, error) {
	a.mu.RLock()
	defaultName := a.defaultAgent
	defaultAgent := a.agents[defaultName]
	a.mu.RUnlock()

	if defaultAgent == nil {
		return nil, errors.New("app has no default runnable")
	}
	return defaultAgent.Invoke(ctx, messages, options...)
}

// Stream exposes a registered component's native stream. Components that only
// implement Runnable are lifted into a snapshot-plus-done stream so the App
// remains a uniform Streamer boundary.
func (a *App) Stream(ctx context.Context, messages []schema.Message, options ...runnable.Option) (runnable.Stream[schema.MessageEvent], error) {
	a.mu.RLock()
	defaultName := a.defaultAgent
	selected := a.agents[defaultName]
	a.mu.RUnlock()
	if selected == nil {
		return nil, errors.New("app has no default runnable")
	}
	if streamer, ok := selected.(runnable.Streamer[[]schema.Message, schema.MessageEvent]); ok {
		return streamer.Stream(ctx, messages, options...)
	}
	return streamValue(ctx, selected, messages, options...), nil
}

func (a *App) InvokeNamed(ctx context.Context, name string, messages []schema.Message, options ...runnable.Option) ([]schema.Message, error) {
	selected, ok := a.Runnable(name)
	if !ok {
		return nil, fmt.Errorf("runnable %q is not registered", name)
	}
	return selected.Invoke(ctx, messages, options...)
}

func streamValue(ctx context.Context, value runnable.Runnable[[]schema.Message, []schema.Message], messages []schema.Message, options ...runnable.Option) runnable.Stream[schema.MessageEvent] {
	out := make(chan runnable.Item[schema.MessageEvent], 2)
	go func() {
		defer close(out)
		output, err := value.Invoke(ctx, messages, options...)
		if err != nil {
			out <- runnable.Item[schema.MessageEvent]{Err: err}
			return
		}
		if len(output) > 0 {
			out <- runnable.Item[schema.MessageEvent]{Value: schema.MessageEvent{Kind: schema.MessageEventSnapshot, Messages: schema.CloneMessages(output)}}
		}
		out <- runnable.Item[schema.MessageEvent]{Value: schema.MessageEvent{Kind: schema.MessageEventDone}}
	}()
	return out
}

var _ runnable.NamedRunnable[[]schema.Message, []schema.Message] = (*App)(nil)
var _ runnable.Streamer[[]schema.Message, schema.MessageEvent] = (*App)(nil)
