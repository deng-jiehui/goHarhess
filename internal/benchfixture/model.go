package benchfixture

import (
	"context"
	"fmt"

	"github.com/goharness/goharness/pkg/llm"
	"github.com/goharness/goharness/pkg/runnable"
	"github.com/goharness/goharness/pkg/schema"
	"github.com/goharness/goharness/pkg/stream"
)

// ModelStep 描述确定性模型的一次响应：Calls 非空时发工具调用块
// （每个名称一个调用），否则发 Text 最终答案。
type ModelStep struct {
	Calls []string
	Text  string
}

// StepModel 是确定性模型替身：按输入消息中已有的 assistant 工具调用
// 批次数选择脚本步骤——真实模型是无状态的，恢复运行时历史里已含先前
// 调用轮次，脚本索引由历史推导而不是实例游标，保证挂起/恢复、并发
// 子运行与跨进程重放都命中同一步骤。
type StepModel struct {
	steps []ModelStep
}

// NewStepModel 创建确定性模型：toolCalls 次同名工具调用后给最终答案。
func NewStepModel(toolCalls int, toolName, answer string) *StepModel {
	steps := make([]ModelStep, 0, toolCalls+1)
	for i := 0; i < toolCalls; i++ {
		steps = append(steps, ModelStep{Calls: []string{toolName}})
	}
	return &StepModel{steps: append(steps, ModelStep{Text: answer})}
}

// NewScriptedStepModel 创建按脚本响应的确定性模型；脚本耗尽后再调用
// 返回错误，防止基准静默跑超预期的轮次。
func NewScriptedStepModel(steps ...ModelStep) *StepModel {
	return &StepModel{steps: steps}
}

func (m *StepModel) next(ctx context.Context, request llm.Request) (llm.Response, error) {
	if ctx == nil {
		return llm.Response{}, fmt.Errorf("fixture model context cannot be nil")
	}
	if err := ctx.Err(); err != nil {
		return llm.Response{}, err
	}
	step := 0
	for _, message := range request.Messages {
		if message.Role == schema.RoleAssistant && len(message.FunctionToolCalls()) > 0 {
			step++
		}
	}
	if step >= len(m.steps) {
		return llm.Response{}, fmt.Errorf("fixture model script exhausted after %d steps", step)
	}
	current := m.steps[step]
	if len(current.Calls) > 0 {
		blocks := make([]schema.ContentBlock, 0, len(current.Calls))
		for i, name := range current.Calls {
			blocks = append(blocks, schema.NewFunctionToolCallBlock(fmt.Sprintf("fixture-call-%d-%d", step, i), name, `{"i":0}`))
		}
		return llm.Response{Messages: []schema.Message{schema.NewMessage(schema.RoleAssistant, blocks...)}}, nil
	}
	return llm.Response{Messages: []schema.Message{schema.AssistantMessage(current.Text)}}, nil
}

func (m *StepModel) Invoke(ctx context.Context, request llm.Request, _ ...runnable.Option) (llm.Response, error) {
	return m.next(ctx, request)
}

func (m *StepModel) Stream(ctx context.Context, request llm.Request, options ...runnable.Option) (*runnable.StreamReader[llm.Response], error) {
	response, err := m.Invoke(ctx, request, options...)
	if err != nil {
		return nil, err
	}
	return streamResponse(ctx, response), nil
}

// streamResponse 把一次完整响应写入输出管道：发送失败时以错误关闭，
// 成功时正常关闭，任何路径都不会留下未关闭的 writer。
func streamResponse(ctx context.Context, response llm.Response) *runnable.StreamReader[llm.Response] {
	reader, writer := runnable.Pipe[llm.Response](ctx)
	go func() {
		if err := writer.Send(response); err != nil {
			_ = writer.CloseWithError(err)
			return
		}
		_ = writer.Close()
	}()
	return reader
}

func (m *StepModel) Collect(ctx context.Context, input *runnable.StreamReader[llm.Request], options ...runnable.Option) (llm.Response, error) {
	value, err := input.Collect(ctx)
	if err != nil {
		return llm.Response{}, err
	}
	return m.Invoke(ctx, value, options...)
}

func (m *StepModel) Transform(ctx context.Context, input *runnable.StreamReader[llm.Request], options ...runnable.Option) (*runnable.StreamReader[llm.Response], error) {
	value, err := input.Collect(ctx)
	if err != nil {
		return nil, err
	}
	return m.Stream(ctx, value, options...)
}

func (m *StepModel) Capabilities() llm.Capabilities {
	return llm.Capabilities{Streaming: true, ToolCalls: true}
}

func (m *StepModel) ModelInfo() llm.ModelInfo {
	return llm.ModelInfo{Name: "fixture-step", Provider: "benchfixture", ModelType: "deterministic"}
}

var _ llm.ChatModel = (*StepModel)(nil)

// SegmentModel 把最终文本分 segments 个片段分段发送：Stream 与真实
// 适配器同构——经 stream.MessageWriter 逐片发布公共消息增量（增量而
// 非快照），结束时向业务响应管道发送一条完整 Response，两者共用同一
// 消息身份。
type SegmentModel struct {
	text     string
	segments int
}

// NewSegmentModel 创建分段模型：最终文本 text 分为 segments 个片段发布。
func NewSegmentModel(text string, segments int) *SegmentModel {
	if segments < 1 {
		segments = 1
	}
	return &SegmentModel{text: text, segments: segments}
}

// SegmentCount 返回该配置实际发布的文本片段数（空文本仍发一个空片段）。
func (m *SegmentModel) SegmentCount() int {
	if len(m.text) == 0 {
		return 1
	}
	chunk := (len(m.text) + m.segments - 1) / m.segments
	return (len(m.text) + chunk - 1) / chunk
}

func (m *SegmentModel) Invoke(ctx context.Context, _ llm.Request, _ ...runnable.Option) (llm.Response, error) {
	if err := ctx.Err(); err != nil {
		return llm.Response{}, err
	}
	return llm.Response{Messages: []schema.Message{schema.AssistantMessage(m.text)}}, nil
}

func (m *SegmentModel) Stream(ctx context.Context, request llm.Request, options ...runnable.Option) (*runnable.StreamReader[llm.Response], error) {
	if _, err := m.Invoke(ctx, request, options...); err != nil {
		return nil, err
	}
	reader, writer := runnable.Pipe[llm.Response](ctx)
	go func() {
		defer writer.Close()
		mw, err := stream.StartMessage(ctx, schema.RoleAssistant)
		if err != nil {
			_ = writer.CloseWithError(err)
			return
		}
		chunk := (len(m.text) + m.segments - 1) / m.segments
		for sent := 0; sent < len(m.text); sent += chunk {
			end := sent + chunk
			if end > len(m.text) {
				end = len(m.text)
			}
			if err := mw.AppendText(m.text[sent:end]); err != nil {
				_ = writer.CloseWithError(err)
				return
			}
		}
		if len(m.text) == 0 {
			if err := mw.AppendText(""); err != nil {
				_ = writer.CloseWithError(err)
				return
			}
		}
		message, err := mw.End()
		if err != nil {
			_ = writer.CloseWithError(err)
			return
		}
		if err := writer.Send(llm.Response{Messages: []schema.Message{message}}); err != nil {
			_ = writer.CloseWithError(err)
		}
	}()
	return reader, nil
}

func (m *SegmentModel) Collect(ctx context.Context, input *runnable.StreamReader[llm.Request], options ...runnable.Option) (llm.Response, error) {
	value, err := input.Collect(ctx)
	if err != nil {
		return llm.Response{}, err
	}
	return m.Invoke(ctx, value, options...)
}

func (m *SegmentModel) Transform(ctx context.Context, input *runnable.StreamReader[llm.Request], options ...runnable.Option) (*runnable.StreamReader[llm.Response], error) {
	value, err := input.Collect(ctx)
	if err != nil {
		return nil, err
	}
	return m.Stream(ctx, value, options...)
}

func (m *SegmentModel) Capabilities() llm.Capabilities {
	return llm.Capabilities{Streaming: true}
}

func (m *SegmentModel) ModelInfo() llm.ModelInfo {
	return llm.ModelInfo{Name: "fixture-segment", Provider: "benchfixture", ModelType: "deterministic"}
}

var _ llm.ChatModel = (*SegmentModel)(nil)

// TurnModel 按输入末尾消息决定响应：末尾为 Tool 结果时给最终答案，
// 否则发一次工具调用。每个用户请求固定"一次调用 + 一次回答"，
// 调用次数不随会话历史增长而放大——适合按请求计量的会话增长场景。
type TurnModel struct {
	toolName string
	answer   string
}

// NewTurnModel 创建按请求计量的确定性模型。
func NewTurnModel(toolName, answer string) *TurnModel {
	return &TurnModel{toolName: toolName, answer: answer}
}

func (m *TurnModel) next(ctx context.Context, request llm.Request) (llm.Response, error) {
	if ctx == nil {
		return llm.Response{}, fmt.Errorf("fixture model context cannot be nil")
	}
	if err := ctx.Err(); err != nil {
		return llm.Response{}, err
	}
	if n := len(request.Messages); n > 0 && request.Messages[n-1].Role == schema.RoleTool {
		return llm.Response{Messages: []schema.Message{schema.AssistantMessage(m.answer)}}, nil
	}
	step := 0
	for _, message := range request.Messages {
		if message.Role == schema.RoleAssistant && len(message.FunctionToolCalls()) > 0 {
			step++
		}
	}
	call := schema.NewFunctionToolCallBlock(fmt.Sprintf("fixture-turn-%d", step), m.toolName, `{"i":0}`)
	return llm.Response{Messages: []schema.Message{schema.NewMessage(schema.RoleAssistant, call)}}, nil
}

func (m *TurnModel) Invoke(ctx context.Context, request llm.Request, _ ...runnable.Option) (llm.Response, error) {
	return m.next(ctx, request)
}

func (m *TurnModel) Stream(ctx context.Context, request llm.Request, options ...runnable.Option) (*runnable.StreamReader[llm.Response], error) {
	response, err := m.Invoke(ctx, request, options...)
	if err != nil {
		return nil, err
	}
	return streamResponse(ctx, response), nil
}

func (m *TurnModel) Collect(ctx context.Context, input *runnable.StreamReader[llm.Request], options ...runnable.Option) (llm.Response, error) {
	value, err := input.Collect(ctx)
	if err != nil {
		return llm.Response{}, err
	}
	return m.Invoke(ctx, value, options...)
}

func (m *TurnModel) Transform(ctx context.Context, input *runnable.StreamReader[llm.Request], options ...runnable.Option) (*runnable.StreamReader[llm.Response], error) {
	value, err := input.Collect(ctx)
	if err != nil {
		return nil, err
	}
	return m.Stream(ctx, value, options...)
}

func (m *TurnModel) Capabilities() llm.Capabilities {
	return llm.Capabilities{Streaming: true, ToolCalls: true}
}

func (m *TurnModel) ModelInfo() llm.ModelInfo {
	return llm.ModelInfo{Name: "fixture-turn", Provider: "benchfixture", ModelType: "deterministic"}
}

var _ llm.ChatModel = (*TurnModel)(nil)
