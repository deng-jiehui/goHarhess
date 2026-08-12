package llm

import (
	"context"
	"sync"

	"goharness/pkg/runnable"
	"goharness/pkg/schema"
)

type Handler func(ctx context.Context, messages []schema.Message, opts ChatOptions) ([]schema.Message, error)

type ScriptedModel struct {
	mu        sync.Mutex
	responses [][]schema.Message
	handler   Handler
}

func NewScriptedModel(responses ...[]schema.Message) *ScriptedModel {
	return &ScriptedModel{responses: responses}
}

func NewHandlerModel(handler Handler) *ScriptedModel {
	return &ScriptedModel{handler: handler}
}

func (m *ScriptedModel) Invoke(ctx context.Context, request Request, _ ...runnable.Option) (Response, error) {
	messages, err := m.nextResponse(ctx, request.Messages, request.Options)
	if err != nil {
		return Response{}, err
	}
	return Response{Messages: messages}, nil
}

var _ Model = (*ScriptedModel)(nil)

func (m *ScriptedModel) Stream(ctx context.Context, request Request, _ ...runnable.Option) (runnable.Stream[Event], error) {
	response, err := m.Invoke(ctx, request)
	if err != nil {
		return nil, err
	}
	out := make(chan runnable.Item[Event], 2)
	out <- runnable.Item[Event]{Value: Event{Kind: EventDelta, Messages: response.Messages}}
	out <- runnable.Item[Event]{Value: Event{Kind: EventDone}}
	close(out)
	return out, nil
}

func (m *ScriptedModel) Capabilities() Capabilities {
	return Capabilities{Streaming: true, ToolCalls: true, Reasoning: true}
}

func (m *ScriptedModel) ModelInfo() ModelInfo {
	return ModelInfo{Name: "scripted", Provider: "test", ModelType: "scripted"}
}

func (m *ScriptedModel) nextResponse(ctx context.Context, messages []schema.Message, opts ChatOptions) ([]schema.Message, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.handler != nil {
		return m.handler(ctx, schema.CloneMessages(messages), opts)
	}
	if len(m.responses) > 0 {
		response := m.responses[0]
		m.responses = m.responses[1:]
		return schema.CloneMessages(response), nil
	}

	if len(messages) == 0 {
		return []schema.Message{schema.AssistantMessage("")}, nil
	}
	return []schema.Message{schema.AssistantMessage(messages[len(messages)-1].Text())}, nil
}
