package graph

import (
	"context"

	"github.com/goharness/goharness/pkg/runnable"
)

// stateDriven 标记输入直接取自运行时 State 的节点形态。AddNode 与并行分支
// 在未显式指定 Pre 时自动挂载恒等映射，使恢复、中断与边校验沿用既有规则。
type stateDriven interface {
	stateNodePre() *runtimePreHandler
	stateNodePost() *runtimePostHandler
}

// stateNode 是 StateNode 返回的组件：输入输出均为 *S，fn 在一个普通 Go
// 函数中完成读取、调用与写回。
type stateNode[S any] struct {
	*runnable.Adapter[*S, *S]
	pre  *runtimePreHandler
	post *runtimePostHandler
}

func (n *stateNode[S]) stateNodePre() *runtimePreHandler   { return n.pre }
func (n *stateNode[S]) stateNodePost() *runtimePostHandler { return n.post }

// StateNode 将一个直接读写运行时 State 的函数注册为图节点。
// 节点输入输出都是 *S：fn 就地修改 State，节点把同一 State 交给下一步，
// 不产生额外业务输出；State 节点链可零胶水直连，下游普通节点继续用自己的
// PreHandler 提取输入，图结束时由 finish 类节点声明输出。
// State 节点自动携带恒等 PreHandler（显式 WithPreHandler 可覆盖），因此天然
// 满足可恢复性与动态中断对 Pre 的要求；并行分支中未指定 Post 时默认为空操作。
func StateNode[S any](fn func(context.Context, *S) error) *stateNode[S] {
	return &stateNode[S]{
		Adapter: runnable.InvokableLambda(func(ctx context.Context, state *S) (*S, error) {
			if err := fn(ctx, state); err != nil {
				return nil, err
			}
			return state, nil
		}),
		pre:  newRuntimePreHandler(func(_ context.Context, state *S) (*S, error) { return state, nil }),
		post: newRuntimePostHandler(func(_ context.Context, _ *S, _ *S) error { return nil }),
	}
}
