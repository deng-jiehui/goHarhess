package subagent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/goharness/goharness/pkg/agent"
	"github.com/goharness/goharness/pkg/schema"
	"github.com/goharness/goharness/pkg/stream"
	"github.com/goharness/goharness/pkg/tool"
)

// AsToolOption 调整 AsTool 的行为。
type AsToolOption func(*asToolConfig)

type asToolConfig struct {
	parameters  map[string]any
	buildInput  func(args map[string]any) ([]schema.Message, error)
	history     Filter
	emitEvents  bool
	description string
}

// WithInputSchema 用自定义参数结构替换默认的 {"task": string}：build 把
// 解析后的参数转成专家的输入消息。返回错误作为模型可见的结果返回。
func WithInputSchema(parameters map[string]any, build func(args map[string]any) ([]schema.Message, error)) AsToolOption {
	return func(c *asToolConfig) {
		c.parameters = parameters
		c.buildInput = build
	}
}

// WithHistory 把父历史经过滤器过滤后附在任务之前传给专家；不设置时专家
// 只收到任务描述，看不到父的系统提示、历史消息与其他工具的输出。
func WithHistory(filter Filter) AsToolOption {
	return func(c *asToolConfig) { c.history = filter }
}

// WithEmitEvents 允许专家的运行事件透传到父运行的公共事件流；默认不透传，
// 只影响展示，不影响观测（observe）、检查点与中断传播。
func WithEmitEvents() AsToolOption {
	return func(c *asToolConfig) { c.emitEvents = true }
}

// AsTool 把一个固定的专家 Agent 包装成具名工具：模型调用时专家在工具调用
// 内同步运行，结果是专家最后一条回答的文本。嵌套运行按调用身份成为子执行，
// 审批挂起、已完成结果复用、检查点命名空间沿用现有语义。
// 需要模型按注册表动态选角色时用 Manager 的通用 run_subagent 工具。
func AsTool(a agent.Agent, name, description string, opts ...AsToolOption) tool.Tool {
	cfg := asToolConfig{
		description: description,
		parameters: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"task": map[string]any{"type": "string", "description": "The concrete task and the expected deliverable."},
			},
			"required":             []string{"task"},
			"additionalProperties": false,
		},
	}
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	spec := tool.Spec{
		Name:        name,
		Description: cfg.description,
		Parameters:  cfg.parameters,
		// WithHistory 需要会话历史视图；默认不声明，省逐调用拷贝。
		NeedsMessages: cfg.history != nil,
	}
	return tool.NewFunctionTool(spec, func(ctx context.Context, in tool.CallInput) (tool.Result, error) {
		input, err := cfg.toolInput(in)
		if err != nil {
			return tool.ErrorResult(schema.NewTextBlock(err.Error())), nil
		}
		// 嵌套执行要求输入 JSON 往返稳定：历史消息中的运行期 Extra
		// （如 checkpoint_version 的 uint64）经一次编解码统一为解码形态。
		raw, err := json.Marshal(input)
		if err != nil {
			return tool.Result{}, fmt.Errorf("marshal expert input: %w", err)
		}
		if err := json.Unmarshal(raw, &input); err != nil {
			return tool.Result{}, fmt.Errorf("normalize expert input: %w", err)
		}
		run := ctx
		if !cfg.emitEvents {
			run = stream.SuppressEvents(run)
		}
		output, err := a.Invoke(run, input, agent.WithCheckpointBypass())
		if err != nil {
			return tool.Result{}, err
		}
		return tool.TextResult(finalText(output)), nil
	})
}

// toolInput 组装专家输入：可选的过滤历史 + 任务消息（默认或自定义结构）。
func (c *asToolConfig) toolInput(in tool.CallInput) ([]schema.Message, error) {
	var history []schema.Message
	if c.history != nil && len(in.Messages) > 0 {
		history = c.history(in.Messages)
	}
	var task []schema.Message
	if c.buildInput != nil {
		built, err := c.buildInput(in.Args)
		if err != nil {
			return nil, err
		}
		task = built
	} else {
		value, _ := in.Args["task"].(string)
		if value == "" {
			return nil, errors.New("task is required and cannot be empty")
		}
		task = []schema.Message{schema.UserMessage(value)}
	}
	if len(task) == 0 {
		return nil, fmt.Errorf("%s input builder returned no messages", "as-tool")
	}
	return append(history, task...), nil
}
