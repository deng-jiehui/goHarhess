package decision

import (
	"context"
	"errors"
	"sync"

	"github.com/goharness/goharness/pkg/runnable"
)

// ErrScriptedExhausted 表示脚本化决策模型的预置响应已用尽。
var ErrScriptedExhausted = errors.New("decision: scripted responses exhausted")

// ScriptedModel 是按调用顺序返回预置 Response 的假决策模型；并发安全。
type ScriptedModel struct {
	mu        sync.Mutex
	responses []Response
	requests  []Request
}

// NewScriptedModel 创建脚本化决策模型：第 n 次 Invoke 返回第 n 个 Response，
// 用尽后返回 ErrScriptedExhausted。
func NewScriptedModel(responses ...Response) *ScriptedModel {
	return &ScriptedModel{responses: responses}
}

// Invoke 先校验 Request，再按顺序弹出预置响应。
func (m *ScriptedModel) Invoke(_ context.Context, request Request, _ ...runnable.Option) (Response, error) {
	if err := request.Validate(); err != nil {
		return Response{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.requests = append(m.requests, request)
	if len(m.responses) == 0 {
		return Response{}, ErrScriptedExhausted
	}
	response := m.responses[0]
	m.responses = m.responses[1:]
	return response, nil
}

// Requests 返回已收到请求的副本。
func (m *ScriptedModel) Requests() []Request {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]Request(nil), m.requests...)
}

func (m *ScriptedModel) Stream(ctx context.Context, input Request, options ...runnable.Option) (*runnable.StreamReader[Response], error) {
	return runnable.StreamFromInvoke(ctx, input, m.Invoke, options...)
}

func (m *ScriptedModel) Collect(ctx context.Context, input *runnable.StreamReader[Request], options ...runnable.Option) (Response, error) {
	return runnable.CollectFromInvoke(ctx, input, m.Invoke, options...)
}

func (m *ScriptedModel) Transform(ctx context.Context, input *runnable.StreamReader[Request], options ...runnable.Option) (*runnable.StreamReader[Response], error) {
	return runnable.TransformFromInvoke(ctx, input, m.Invoke, options...)
}

var _ Model = (*ScriptedModel)(nil)

// NewFuncModel 用回调函数构造决策模型；Invoke 先校验 Request 再调用 fn。
func NewFuncModel(fn func(context.Context, Request) (Response, error)) Model {
	return runnable.Func[Request, Response](func(ctx context.Context, request Request, _ ...runnable.Option) (Response, error) {
		if err := request.Validate(); err != nil {
			return Response{}, err
		}
		if fn == nil {
			return Response{}, errors.New("decision: nil model function")
		}
		return fn(ctx, request)
	})
}
