package tool

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"

	"goharness/pkg/runnable"
	"goharness/pkg/schema"
)

// Config contains factory-specific tool configuration.
type Config map[string]any

// Spec describes a tool to models, authorization policies and the scheduler.
type Spec struct {
	Name        string
	Description string
	Parameters  map[string]any
	FileAccess  bool
	// IsReadOnly defaults to false so undeclared tools are treated as mutating.
	IsReadOnly bool
	// Concurrency's zero value serializes the tool with other undeclared tools.
	Concurrency ConcurrencyPolicy
}

// Validate checks required fields and concurrency settings.
func (s Spec) Validate() error {
	if s.Name == "" {
		return errors.New("tool name cannot be empty")
	}
	if err := s.Concurrency.Validate(); err != nil {
		return fmt.Errorf("tool %q concurrency: %w", s.Name, err)
	}
	return nil
}

// Definition converts the specification to the model-facing function schema.
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

// EmptyParameters returns a JSON schema for a function with no arguments.
func EmptyParameters() map[string]any {
	return map[string]any{
		"type":       "object",
		"properties": map[string]any{},
		"required":   []string{},
	}
}

// CallInput contains parsed arguments and non-identity invocation data.
// Authentication identity belongs in context.Context via WithSubject.
type CallInput struct {
	RawArgs string
	Args    map[string]any
	// HTTPClient is populated by the internal runtime for every managed call.
	// It shares the framework Transport unless the run context overrides it.
	HTTPClient *http.Client
	Messages   []schema.Message
	Files      []string
	Metadata   map[string]any
}

// Result contains either model-facing Content or structured Data.
type Result struct {
	Content schema.Content
	Data    any
	// IsError preserves a protocol-level tool result that is intentionally
	// visible to the next model turn. It is distinct from an execution error.
	IsError bool
}

// TextResult creates a text tool result.
func TextResult(text string) Result {
	return Result{Content: schema.TextContent(text)}
}

// DataResult creates a structured tool result serialized by ToContent.
func DataResult(data any) Result {
	return Result{Data: data}
}

// ErrorResult creates a model-visible tool error without converting it into a
// transport/runtime failure. This matches MCP's isError result semantics.
func ErrorResult(content schema.Content) Result {
	return Result{Content: content, IsError: true}
}

// ToContent converts structured result data to model-facing content.
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
		return schema.TextContent(""), fmt.Errorf("marshal tool result: %w", err)
	}
	return schema.TextContent(string(data)), nil
}

// BaseTool combines model-facing metadata with the canonical execution
// contract. Providers, MCP adapters and application tools all implement this
// small interface and can be placed in a ToolsNode.
type BaseTool interface {
	Spec() Spec
	runnable.Runnable[CallInput, Result]
}

// Tool is the default framework tool name. It is an alias so extensions can
// expose BaseTool without introducing a second incompatible tool hierarchy.
type Tool = BaseTool

// Handler implements a FunctionTool invocation.
type Handler func(ctx context.Context, input CallInput) (Result, error)

// FunctionTool adapts a Handler to Tool.
type FunctionTool struct {
	spec    Spec
	handler Handler
}

// NewFunctionTool creates a tool and fills an omitted parameter schema.
func NewFunctionTool(spec Spec, handler Handler) *FunctionTool {
	if spec.Parameters == nil {
		spec.Parameters = EmptyParameters()
	}
	return &FunctionTool{spec: spec, handler: handler}
}

// Spec returns the tool specification.
func (t *FunctionTool) Spec() Spec {
	return t.spec
}

func (t *FunctionTool) Invoke(ctx context.Context, input CallInput, _ ...runnable.Option) (Result, error) {
	if t.handler == nil {
		return Result{}, errors.New("tool handler is nil")
	}
	return t.handler(ctx, input)
}

var _ Tool = (*FunctionTool)(nil)

// Factory constructs a tool from registry configuration.
type Factory func(Config) (Tool, error)

var registry = struct {
	sync.RWMutex
	factories map[string]Factory
}{
	factories: map[string]Factory{},
}

// Register adds a tool factory to the process registry.
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

// MustRegister is Register with panic-on-error semantics for package initialization.
func MustRegister(name string, factory Factory, allowOverwrite bool) {
	if err := Register(name, factory, allowOverwrite); err != nil {
		panic(err)
	}
}

// New constructs and validates a registered tool.
func New(name string, cfg Config) (Tool, error) {
	registry.RLock()
	factory := registry.factories[name]
	registry.RUnlock()
	if factory == nil {
		return nil, fmt.Errorf("tool %q is not registered", name)
	}
	value, err := factory(cfg)
	if err != nil {
		return nil, fmt.Errorf("create tool %q: %w", name, err)
	}
	if value == nil {
		return nil, fmt.Errorf("tool %q factory returned nil", name)
	}
	if err := value.Spec().Validate(); err != nil {
		return nil, err
	}
	return value, nil
}

// RegisteredNames returns all currently registered tool factory names.
func RegisteredNames() []string {
	registry.RLock()
	defer registry.RUnlock()

	names := make([]string, 0, len(registry.factories))
	for name := range registry.factories {
		names = append(names, name)
	}
	return names
}

// ParseArgs decodes a JSON object and returns an empty map for invalid input.
// Use this only for tolerant stream assembly; finalized calls should validate JSON first.
func ParseArgs(raw string) map[string]any {
	args, err := ParseArgsStrict(raw)
	if err != nil {
		return map[string]any{}
	}
	return args
}

// ParseArgsStrict decodes finalized tool arguments as a JSON object.
func ParseArgsStrict(raw string) (map[string]any, error) {
	if strings.TrimSpace(raw) == "" {
		return map[string]any{}, nil
	}
	var args map[string]any
	if err := json.Unmarshal([]byte(raw), &args); err != nil {
		return nil, fmt.Errorf("decode tool arguments: %w", err)
	}
	if args == nil {
		return nil, errors.New("tool arguments must be a JSON object")
	}
	return args, nil
}
