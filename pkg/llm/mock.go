package llm

import (
	"context"
	"sync"

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

func (m *ScriptedModel) Chat(ctx context.Context, messages []schema.Message, opts ChatOptions) (Stream, error) {
	response, err := m.nextResponse(ctx, messages, opts)
	if err != nil {
		return nil, err
	}

	out := make(chan Chunk, 1)
	go func() {
		defer close(out)
		select {
		case <-ctx.Done():
			out <- Chunk{Err: ctx.Err()}
		case out <- Chunk{Messages: schema.CloneMessages(response)}:
		}
	}()
	return out, nil
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
