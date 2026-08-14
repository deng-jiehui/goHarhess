package llm

import (
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

// Request is the provider-neutral model invocation envelope.
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

// ErrStreamTruncated reports a model Reader that ended before producing a
// complete Response.
var ErrStreamTruncated = errors.New("model response stream ended without a result")

func (r Response) Clone() Response {
	r.Messages = schema.CloneMessages(r.Messages)
	r.Usage = cloneUsage(r.Usage)
	return r
}

type ChatModel interface {
	runnable.Runnable[Request, Response]
	Capabilities() Capabilities
	ModelInfo() ModelInfo
}

type Usage = schema.TokenUsage

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
func DescribeModel(model ChatModel) ModelInfo {
	if model == nil {
		return ModelInfo{}
	}
	info := model.ModelInfo()
	info.Attributes = cloneAnyMap(info.Attributes)
	return info
}

type Factory func(Config) (ChatModel, error)

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

func New(cfg Config) (ChatModel, error) {
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
	cloned.Extra = cloneAnyMap(usage.Extra)
	return &cloned
}
