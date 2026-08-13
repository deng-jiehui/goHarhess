package llm

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"

	"goharness/pkg/runnable"
	"goharness/pkg/schema"
)

type GenerateConfig map[string]any

type Config struct {
	Model       string
	ModelType   string
	ModelServer string
	APIKey      string
	// HTTPClient can be shared by all models that use the same provider.
	// A nil value makes a provider create its package default client.
	HTTPClient     *http.Client
	GenerateConfig GenerateConfig
	Raw            map[string]any
}

type ChatOptions struct {
	Functions           []schema.FunctionDefinition
	DeltaStream         bool
	ExtraGenerateConfig GenerateConfig
}

// Request is the provider-neutral model invocation envelope. Providers may
// still expose Chat internally, but framework composition only depends on
// Runnable[Request, Stream].
type Request struct {
	Messages []schema.Message
	Options  ChatOptions
}

func (r Request) Clone() Request {
	r.Messages = schema.CloneMessages(r.Messages)
	r.Options.Functions = cloneFunctionDefinitions(r.Options.Functions)
	r.Options.ExtraGenerateConfig = GenerateConfig(cloneAnyMap(r.Options.ExtraGenerateConfig))
	return r
}

// Response is the fully reduced result of one model invocation.
type Response struct {
	Messages     []schema.Message
	Usage        *Usage
	FinishReason string
}

func (r Response) Clone() Response {
	r.Messages = schema.CloneMessages(r.Messages)
	r.Usage = cloneUsage(r.Usage)
	return r
}

type EventKind string

const (
	EventDelta EventKind = "delta"
	EventUsage EventKind = "usage"
	EventDone  EventKind = "done"
)

// Event is the canonical model data-plane event. Errors are carried by
// runnable.Item.Err, never encoded as a competing Chunk kind.
type Event struct {
	Kind         EventKind
	Messages     []schema.Message
	Usage        *Usage
	FinishReason string
}

func (e Event) Clone() Event {
	e.Messages = schema.CloneMessages(e.Messages)
	e.Usage = cloneUsage(e.Usage)
	return e
}

type Model interface {
	runnable.Runnable[Request, Response]
	runnable.Streamer[Request, Event]
	Capabilities() Capabilities
	ModelInfo() ModelInfo
}

// ChatModel is the provider-neutral chat model contract used by supervisors
// and other orchestration components.
type ChatModel = Model

// Usage contains provider-neutral token accounting when a provider returns it.
// Providers may leave fields at zero when the upstream API does not report them.
type Usage struct {
	PromptTokens     int            `json:"prompt_tokens,omitempty"`
	CompletionTokens int            `json:"completion_tokens,omitempty"`
	TotalTokens      int            `json:"total_tokens,omitempty"`
	ReasoningTokens  int            `json:"reasoning_tokens,omitempty"`
	Raw              map[string]any `json:"raw,omitempty"`
}

// Capabilities describes optional behavior exposed by a Model.
type Capabilities struct {
	Streaming  bool
	ToolCalls  bool
	Vision     bool
	JSONSchema bool
	Reasoning  bool
}

// ModelInfo provides non-secret provider metadata for observability callbacks.
type ModelInfo struct {
	Name       string         `json:"name,omitempty"`
	Provider   string         `json:"provider,omitempty"`
	ModelType  string         `json:"model_type,omitempty"`
	Endpoint   string         `json:"endpoint,omitempty"`
	Attributes map[string]any `json:"attributes,omitempty"`
}

// DescribeModel returns callback-safe metadata without exposing credentials.
func DescribeModel(model Model) ModelInfo {
	if model == nil {
		return ModelInfo{}
	}
	info := model.ModelInfo()
	info.Attributes = cloneAnyMap(info.Attributes)
	return info
}

type Factory func(Config) (Model, error)

var registry = struct {
	sync.RWMutex
	factories map[string]Factory
}{
	factories: map[string]Factory{},
}

func Register(modelType string, factory Factory) error {
	if modelType == "" {
		return errors.New("model type cannot be empty")
	}
	if factory == nil {
		return errors.New("model factory cannot be nil")
	}

	registry.Lock()
	defer registry.Unlock()
	registry.factories[modelType] = factory
	return nil
}

func MustRegister(modelType string, factory Factory) {
	if err := Register(modelType, factory); err != nil {
		panic(err)
	}
}

func New(cfg Config) (Model, error) {
	modelType := cfg.ModelType
	if modelType == "" {
		return nil, errors.New("model type is required")
	}

	registry.RLock()
	factory := registry.factories[modelType]
	registry.RUnlock()
	if factory == nil {
		return nil, fmt.Errorf("llm model type %q is not registered", modelType)
	}
	return factory(cfg)
}

func RegisteredTypes() []string {
	registry.RLock()
	defer registry.RUnlock()

	types := make([]string, 0, len(registry.factories))
	for modelType := range registry.factories {
		types = append(types, modelType)
	}
	return types
}

func MergeGenerateConfig(base GenerateConfig, override GenerateConfig) GenerateConfig {
	merged := GenerateConfig{}
	for k, v := range base {
		merged[k] = v
	}
	for k, v := range override {
		merged[k] = v
	}
	return merged
}

func ReduceEvents(ctx context.Context, stream runnable.Stream[Event], deltas bool) (Response, error) {
	if ctx == nil {
		return Response{}, errors.New("model reduce context is nil")
	}
	if stream == nil {
		return Response{}, errors.New("model event stream is nil")
	}
	accumulator := NewDeltaAccumulator(deltas)
	response := Response{}
	for {
		select {
		case <-ctx.Done():
			return Response{}, ctx.Err()
		case item, ok := <-stream:
			if !ok {
				return Response{}, errors.New("model event stream closed without done event")
			}
			if item.Err != nil {
				return Response{}, item.Err
			}
			event := item.Value
			if event.Usage != nil {
				response.Usage = cloneUsage(event.Usage)
			}
			if event.FinishReason != "" {
				response.FinishReason = event.FinishReason
			}
			if event.Kind == EventDelta && len(event.Messages) > 0 {
				messages, err := accumulator.Apply(event)
				if err != nil {
					return Response{}, err
				}
				response.Messages = messages
			}
			if event.Kind == EventDone {
				return response.Clone(), nil
			}
		}
	}
}

func sendModelEvent(ctx context.Context, out chan<- runnable.Item[Event], item runnable.Item[Event]) bool {
	item.Value = item.Value.Clone()
	select {
	case <-ctx.Done():
		return false
	case out <- item:
		return true
	}
}

func cloneAnyMap(input map[string]any) map[string]any {
	if input == nil {
		return nil
	}
	cloned := make(map[string]any, len(input))
	for key, value := range input {
		cloned[key] = value
	}
	return cloned
}

func cloneFunctionDefinitions(input []schema.FunctionDefinition) []schema.FunctionDefinition {
	cloned := make([]schema.FunctionDefinition, len(input))
	for index, definition := range input {
		definition.Parameters = cloneAnyMap(definition.Parameters)
		cloned[index] = definition
	}
	return cloned
}

func cloneUsage(usage *Usage) *Usage {
	if usage == nil {
		return nil
	}
	cloned := *usage
	cloned.Raw = cloneAnyMap(usage.Raw)
	return &cloned
}
