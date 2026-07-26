package llm

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"goharness/pkg/schema"
)

type GenerateConfig map[string]any

type Config struct {
	Model          string
	ModelType      string
	ModelServer    string
	APIKey         string
	GenerateConfig GenerateConfig
	Raw            map[string]any
}

type ChatOptions struct {
	Functions           []schema.FunctionDefinition
	Stream              bool
	DeltaStream         bool
	ExtraGenerateConfig GenerateConfig
}

type Chunk struct {
	Messages []schema.Message
	Err      error
}

type Stream <-chan Chunk

type ChatModel interface {
	Chat(ctx context.Context, messages []schema.Message, opts ChatOptions) (Stream, error)
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
