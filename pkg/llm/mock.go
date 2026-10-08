package llm

import (
	"context"
	"fmt"
	"sync"

	"github.com/goharness/goharness/pkg/callback"
	"github.com/goharness/goharness/pkg/runnable"
	"github.com/goharness/goharness/pkg/schema"
)

// Handler 是测试模型的响应回调：收到业务 Request 与本次调用解析后的
// llm.Options；其余调用选项不会进入测试处理器。
type Handler func(ctx context.Context, request Request, opts Options) ([]schema.Message, error)

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

func (m *ScriptedModel) Invoke(ctx context.Context, request Request, options ...runnable.Option) (Response, error) {
	resolved, remaining, err := resolveModelOptions(options)
	if err != nil {
		return Response{}, fmt.Errorf("resolve model options: %w", err)
	}
	if err := runnable.CheckNoRemainingOptions(remaining...); err != nil {
		return Response{}, fmt.Errorf("resolve model options: %w", err)
	}
	messages, err := m.nextResponse(ctx, request, resolved)
	if err != nil {
		return Response{}, err
	}
	return Response{Messages: messages}, nil
}

var _ ChatModel = (*ScriptedModel)(nil)

func (m *ScriptedModel) Stream(ctx context.Context, request Request, options ...runnable.Option) (*runnable.StreamReader[Response], error) {
	response, err := m.Invoke(ctx, request, options...)
	if err != nil {
		return nil, err
	}
	reader, writer := runnable.Pipe[Response](ctx)
	go func() {
		callback.EmitStream(ctx, response.Clone())
		if err := writer.Send(response.Clone()); err != nil {
			return
		}
		_ = writer.Close()
	}()
	return reader, nil
}

func (m *ScriptedModel) Collect(ctx context.Context, input *runnable.StreamReader[Request], options ...runnable.Option) (Response, error) {
	value, err := input.Collect(ctx)
	if err != nil {
		return Response{}, err
	}
	return m.Invoke(ctx, value, options...)
}
func (m *ScriptedModel) Transform(ctx context.Context, input *runnable.StreamReader[Request], options ...runnable.Option) (*runnable.StreamReader[Response], error) {
	value, err := input.Collect(ctx)
	if err != nil {
		return nil, err
	}
	return m.Stream(ctx, value, options...)
}

func (m *ScriptedModel) Capabilities() Capabilities {
	return Capabilities{Streaming: true, ToolCalls: true, Reasoning: true}
}

func (m *ScriptedModel) ModelInfo() ModelInfo {
	return ModelInfo{Name: "scripted", Provider: "test", ModelType: "scripted"}
}

func (m *ScriptedModel) nextResponse(ctx context.Context, request Request, opts Options) ([]schema.Message, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.handler != nil {
		return m.handler(ctx, request.Clone(), opts)
	}
	if len(m.responses) > 0 {
		response := m.responses[0]
		m.responses = m.responses[1:]
		return schema.CloneMessages(response), nil
	}

	if len(request.Messages) == 0 {
		return []schema.Message{schema.NewMessage(schema.RoleAssistant, schema.NewTextBlock(""))}, nil
	}
	return []schema.Message{schema.NewMessage(schema.RoleAssistant, schema.NewTextBlock(request.Messages[len(request.Messages)-1].JoinedText("")))}, nil
}
