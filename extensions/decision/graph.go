package decision

import (
	"context"
	"errors"
)

// Node 把一次决策调用包装成可直接传给 graph.NewState[S](...).Node(name, fn)
// 的节点函数：build 从 State 构造 Request，Invoke 后由 apply 写回 State。
func Node[S any](model Model, build func(context.Context, *S) (Request, error), apply func(context.Context, *S, Response) error) func(context.Context, *S) error {
	return func(ctx context.Context, s *S) error {
		if model == nil {
			return errors.New("decision: nil model")
		}
		if build == nil || apply == nil {
			return errors.New("decision: node requires build and apply functions")
		}
		request, err := build(ctx, s)
		if err != nil {
			return err
		}
		response, err := model.Invoke(ctx, request)
		if err != nil {
			return err
		}
		if err := request.ValidateResponse(response); err != nil {
			return err
		}
		return apply(ctx, s, response)
	}
}

// Route 按 choice 回答给出路由目标：置信度不低于 minConfidence 且 targets
// 里有该选项时返回 targets[a.Choice]，否则返回 fallback。
// 返回值仍需调用方在 Branch 的 targets 里预先声明——未声明的目标会在
// 运行期被图路由按错误处理（现有图语义）。
func Route(a Answer, minConfidence float64, targets map[string]string, fallback string) string {
	if a.Confidence >= minConfidence {
		if target, ok := targets[a.Choice]; ok {
			return target
		}
	}
	return fallback
}

// Yes 按 noul 回答给出布尔决策：PTrue 不低于 threshold（边界相等算真）。
func Yes(a Answer, threshold float64) bool {
	return a.PTrue >= threshold
}
