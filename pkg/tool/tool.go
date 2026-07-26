package tool

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"goharness/pkg/schema"
)

type Config map[string]any

type Spec struct {
	Name        string
	Description string
	Parameters  map[string]any
	FileAccess  bool
}

func (s Spec) Definition() schema.FunctionDefinition {
	params := s.Parameters
	if params == nil {
		params = EmptyParameters()
	}
	return schema.FunctionDefinition{
		Name:        s.Name,
		Description: s.Description,
		Parameters:  params,
	}
}

func EmptyParameters() map[string]any {
	return map[string]any{
		"type":       "object",
		"properties": map[string]any{},
		"required":   []string{},
	}
}

type CallInput struct {
	RawArgs  string
	Args     map[string]any
	Messages []schema.Message
	Files    []string
	Metadata map[string]any
}

type Result struct {
	Content schema.Content
	Data    any
}

func TextResult(text string) Result {
	return Result{Content: schema.TextContent(text)}
}

func DataResult(data any) Result {
	return Result{Data: data}
}

func (r Result) ToContent() (schema.Content, error) {
	if !r.Content.IsZero() {
		return r.Content, nil
	}
	if r.Data == nil {
		return schema.TextContent(""), nil
	}
	if text, ok := r.Data.(string); ok {
		return schema.TextContent(text), nil
	}
	data, err := json.MarshalIndent(r.Data, "", "  ")
	if err != nil {
		return schema.TextContent(""), err
	}
	return schema.TextContent(string(data)), nil
}

type Tool interface {
	Spec() Spec
	Call(ctx context.Context, input CallInput) (Result, error)
}

type Handler func(ctx context.Context, input CallInput) (Result, error)

type FunctionTool struct {
	spec    Spec
	handler Handler
}

func NewFunctionTool(spec Spec, handler Handler) *FunctionTool {
	if spec.Parameters == nil {
		spec.Parameters = EmptyParameters()
	}
	return &FunctionTool{spec: spec, handler: handler}
}

func (t *FunctionTool) Spec() Spec {
	return t.spec
}

func (t *FunctionTool) Call(ctx context.Context, input CallInput) (Result, error) {
	if t.handler == nil {
		return Result{}, errors.New("tool handler is nil")
	}
	return t.handler(ctx, input)
}

type Factory func(Config) (Tool, error)

var registry = struct {
	sync.RWMutex
	factories map[string]Factory
}{
	factories: map[string]Factory{},
}

func Register(name string, factory Factory, allowOverwrite bool) error {
	if name == "" {
		return errors.New("tool name cannot be empty")
	}
	if factory == nil {
		return errors.New("tool factory cannot be nil")
	}

	registry.Lock()
	defer registry.Unlock()
	if _, exists := registry.factories[name]; exists && !allowOverwrite {
		return fmt.Errorf("tool %q is already registered", name)
	}
	registry.factories[name] = factory
	return nil
}

func MustRegister(name string, factory Factory, allowOverwrite bool) {
	if err := Register(name, factory, allowOverwrite); err != nil {
		panic(err)
	}
}

func New(name string, cfg Config) (Tool, error) {
	registry.RLock()
	factory := registry.factories[name]
	registry.RUnlock()
	if factory == nil {
		return nil, fmt.Errorf("tool %q is not registered", name)
	}
	return factory(cfg)
}

func RegisteredNames() []string {
	registry.RLock()
	defer registry.RUnlock()

	names := make([]string, 0, len(registry.factories))
	for name := range registry.factories {
		names = append(names, name)
	}
	return names
}

func ParseArgs(raw string) map[string]any {
	if raw == "" {
		return map[string]any{}
	}
	var args map[string]any
	if err := json.Unmarshal([]byte(raw), &args); err != nil {
		return map[string]any{}
	}
	return args
}
