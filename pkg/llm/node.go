package llm

import (
	"context"
	"errors"

	"goharness/pkg/runnable"
)

// ChatModelNode turns a provider ChatModel into an observable Graph component.
// Prompt assembly and State updates remain Graph Pre/Post handler concerns.
type ChatModelNode struct {
	model       ChatModel
	name        string
	description string
}

func NewChatModelNode(model ChatModel) (*ChatModelNode, error) {
	if model == nil {
		return nil, errors.New("chat model node requires a model")
	}
	info := DescribeModel(model)
	name := info.Name
	if name == "" {
		name = info.ModelType
	}
	if name == "" {
		name = "chat-model"
	}
	description := info.Provider
	if description == "" {
		description = "provider-neutral chat model node"
	}
	return &ChatModelNode{model: model, name: name, description: description}, nil
}

func (n *ChatModelNode) Name() string {
	if n == nil {
		return ""
	}
	return n.name
}

func (n *ChatModelNode) Description() string {
	if n == nil {
		return ""
	}
	return n.description
}

func (*ChatModelNode) NodeKind() runnable.NodeKind { return runnable.NodeKindChatModel }

func (n *ChatModelNode) Invoke(ctx context.Context, request Request, options ...runnable.Option) (Response, error) {
	reader, err := n.Stream(ctx, request, options...)
	if err != nil {
		return Response{}, err
	}
	return CollectResponse(ctx, reader)
}

func (n *ChatModelNode) Stream(ctx context.Context, request Request, options ...runnable.Option) (*runnable.StreamReader[Response], error) {
	if n == nil || n.model == nil {
		return nil, errors.New("chat model node is not configured")
	}
	if ctx == nil {
		return nil, errors.New("chat model node context cannot be nil")
	}
	info := DescribeModel(n.model)
	capabilities := n.model.Capabilities()
	metadata := map[string]any{
		"llm.provider":                 info.Provider,
		"llm.model_type":               info.ModelType,
		"llm.endpoint":                 info.Endpoint,
		"llm.streaming":                true,
		"llm.function_count":           len(request.Options.Functions),
		"llm.capabilities.streaming":   capabilities.Streaming,
		"llm.capabilities.tool_calls":  capabilities.ToolCalls,
		"llm.capabilities.vision":      capabilities.Vision,
		"llm.capabilities.json_schema": capabilities.JSONSchema,
		"llm.capabilities.reasoning":   capabilities.Reasoning,
	}
	if len(info.Attributes) > 0 {
		metadata["llm.attributes"] = info.Attributes
	}
	modelCtx, invocation := BeginCallback(ctx, n.name, metadata, request)
	upstream, err := n.model.Stream(modelCtx, request.Clone(), options...)
	if err != nil {
		ErrorCallback(modelCtx, invocation, err)
		return nil, err
	}
	if upstream == nil {
		err := errors.New("chat model returned a nil stream reader")
		ErrorCallback(modelCtx, invocation, err)
		return nil, err
	}
	return ObserveReaderWithInvocation(modelCtx, invocation, upstream)
}

func (n *ChatModelNode) Collect(ctx context.Context, input *runnable.StreamReader[Request], options ...runnable.Option) (Response, error) {
	return runnable.CollectFromInvoke(ctx, input, n.Invoke, options...)
}

func (n *ChatModelNode) Transform(ctx context.Context, input *runnable.StreamReader[Request], options ...runnable.Option) (*runnable.StreamReader[Response], error) {
	return runnable.TransformFromInvoke(ctx, input, n.Invoke, options...)
}

func (n *ChatModelNode) Capabilities() Capabilities {
	if n == nil || n.model == nil {
		return Capabilities{}
	}
	return n.model.Capabilities()
}

func (n *ChatModelNode) ModelInfo() ModelInfo {
	if n == nil || n.model == nil {
		return ModelInfo{}
	}
	return DescribeModel(n.model)
}

var _ ChatModel = (*ChatModelNode)(nil)
var _ runnable.NodeDescriptor = (*ChatModelNode)(nil)
