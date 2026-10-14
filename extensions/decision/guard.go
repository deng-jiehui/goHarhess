package decision

import (
	"context"
	"errors"
	"fmt"

	"github.com/goharness/goharness/pkg/graph"
	"github.com/goharness/goharness/pkg/schema"
	"github.com/goharness/goharness/pkg/tool"
)

// GuardQuestionID 是工具筛查提问的固定问题 ID。
const GuardQuestionID = "allow"

// defaultGuardInstructions 是未配置 Instructions 时的筛查问题。
const defaultGuardInstructions = "Is this tool call consistent with the user's request and safe to execute without human approval?"

// GuardConfig 是 ToolGuard 的配置。
type GuardConfig struct {
	Model     Model
	ModelName string
	// State 构造发给决策模型的状态；默认 {"tool": 名称, "arguments": 原始参数}。
	// 默认状态包含工具的原始参数：会发给决策服务，也会进入人工审批中断的
	// 载荷与检查点；参数含敏感数据时用 State 回调只发必要字段（审批载荷里
	// 的参数仍是原始参数，需要时在应用侧脱敏展示）。
	State func(ctx context.Context, spec tool.Spec, input tool.CallInput) (any, error)
	// Instructions 是筛查问题文本；默认见 defaultGuardInstructions。
	Instructions string
	// MinPTrue 是放行阈值，必填且取值 (0,1]，否则返回配置错误。
	MinPTrue float64
	// Applies 决定工具是否筛查；默认 !spec.IsReadOnly。
	Applies func(spec tool.Spec) bool
}

// approvalPayload 是人工确认中断的载荷，可 JSON 序列化。
type approvalPayload struct {
	Tool      string  `json:"tool"`
	Arguments string  `json:"arguments"`
	PTrue     float64 `json:"ptrue"`
	Threshold float64 `json:"threshold"`
	Reason    string  `json:"reason"`
}

// ToolGuard 构造工具中间件：适用工具在调用前问决策模型一个 noul 问题；
// PTrue 达到 MinPTrue 直接放行，低于阈值或模型不可用则经 graph.Interrupt
// 请求人工确认。不适用工具直接放行且决策模型零调用。
//
// 已知限制：恢复时中间件随节点从头重放，决策模型会被再问一次
// （问题相同、代价便宜，不另做缓存）；回答越界或缺失按"模型不可用"处理。
func ToolGuard(cfg GuardConfig) (tool.Middleware, error) {
	if cfg.Model == nil {
		return nil, errors.New("decision: tool guard requires a model")
	}
	if cfg.MinPTrue <= 0 || cfg.MinPTrue > 1 {
		return nil, fmt.Errorf("decision: tool guard MinPTrue must be in (0,1], got %v", cfg.MinPTrue)
	}
	if cfg.State == nil {
		cfg.State = func(_ context.Context, spec tool.Spec, input tool.CallInput) (any, error) {
			return map[string]any{"tool": spec.Name, "arguments": input.RawArgs}, nil
		}
	}
	if cfg.Instructions == "" {
		cfg.Instructions = defaultGuardInstructions
	}
	if cfg.Applies == nil {
		cfg.Applies = func(spec tool.Spec) bool { return !spec.IsReadOnly }
	}
	return tool.MiddlewareFuncs{
		MiddlewareName: "decision.tool_guard",
		HandleFunc: func(ctx context.Context, spec tool.Spec, input tool.CallInput, next tool.Next) (tool.Result, error) {
			if !cfg.Applies(spec) {
				return next(ctx, input)
			}
			state, err := cfg.State(ctx, spec, input)
			if err != nil {
				return tool.Result{}, fmt.Errorf("decision: build guard state: %w", err)
			}
			request := Request{
				Model:     cfg.ModelName,
				State:     state,
				Questions: map[string]Question{GuardQuestionID: Noul(cfg.Instructions)},
			}
			response, err := cfg.Model.Invoke(ctx, request)
			if err != nil {
				// 取消/排空不转人工审批：原样上抛，保留 context.Cause。
				if ctx.Err() != nil {
					return tool.Result{}, err
				}
				// 保守策略：模型不可用也要人工确认。
				answer, err := askApproval(ctx, approvalPayload{
					Tool: spec.Name, Arguments: input.RawArgs,
					Threshold: cfg.MinPTrue, Reason: "decision model unavailable",
				})
				if err != nil {
					return tool.Result{}, err
				}
				return resolveApproval(ctx, spec, input, next, answer)
			}
			// 回答越界或缺失同样按模型不可用处理，走保守审批。
			if err := request.ValidateResponse(response); err != nil {
				answer, err := askApproval(ctx, approvalPayload{
					Tool: spec.Name, Arguments: input.RawArgs,
					Threshold: cfg.MinPTrue, Reason: "decision model returned an invalid answer",
				})
				if err != nil {
					return tool.Result{}, err
				}
				return resolveApproval(ctx, spec, input, next, answer)
			}
			ptrue := response.Answers[GuardQuestionID].PTrue
			if ptrue >= cfg.MinPTrue {
				return next(ctx, input)
			}
			answer, err := askApproval(ctx, approvalPayload{
				Tool: spec.Name, Arguments: input.RawArgs,
				PTrue: ptrue, Threshold: cfg.MinPTrue, Reason: "ptrue below threshold",
			})
			if err != nil {
				return tool.Result{}, err
			}
			return resolveApproval(ctx, spec, input, next, answer)
		},
	}, nil
}

func askApproval(ctx context.Context, payload approvalPayload) (any, error) {
	answer, err := graph.Interrupt(ctx, payload)
	if err != nil {
		return nil, err
	}
	return answer, nil
}

// resolveApproval 解释恢复输入：true 或 {"approved": true} 放行；
// 拒绝或无法识别都返回模型可见的 isError 结果，工具不执行。
func resolveApproval(ctx context.Context, spec tool.Spec, input tool.CallInput, next tool.Next, answer any) (tool.Result, error) {
	approved, ok := answer.(bool)
	if !ok {
		if m, isMap := answer.(map[string]any); isMap {
			approved, ok = m["approved"].(bool)
		}
	}
	if !ok {
		return tool.ErrorResult(schema.NewTextBlock(
			"tool call approval answer was not recognized; the tool was not executed")), nil
	}
	if !approved {
		return tool.ErrorResult(schema.NewTextBlock("tool call rejected by reviewer")), nil
	}
	return next(ctx, input)
}
