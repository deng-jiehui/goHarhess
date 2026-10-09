package graph

import (
	"context"
	"errors"
	"strings"
)

// Builder 是 State 图的链式门面：节点函数直接读写 *S，路由一次求值，
// 建图错误不逐个返回而是累积到 Compile 统一报告。
// 需要高级能力（PreHandler、并行组、嵌套图、自定义分派输入等）时
// 经 Graph() 逃生舱直达既有 API。
type Builder[S any] struct {
	g    *Graph[S, *S]
	errs []error
	// pendingJoins 是 Parallel 声明的成员默认汇合接线：Compile 时只为
	// 没有声明任何出边形式（边、路由、扇出）的成员补 member→join 边，
	// 成员用 Branch 声明内部路由时不会被默认边抢占。
	pendingJoins []memberJoin
}

type memberJoin struct {
	member string
	join   string
}

// NewState 创建 State 图门面：图输入即 State 初值，运行结束输出最终 *S。
func NewState[S any](name string) *Builder[S] {
	g := NewGraph[S, *S](name)
	b := &Builder[S]{g: g}
	// 输入按值进入 State，各节点在同一份 *S 上就地修改。
	if err := SetStateFactory(g, func(_ context.Context, s S) (*S, error) {
		state := s
		return &state, nil
	}); err != nil {
		b.errs = append(b.errs, err)
	}
	return b
}

// Graph 返回底层图，供 AddNode/AddBranch/AddParallelNode 等高级 API 使用。
// 绕过门面登记的声明不经 Compile 聚合，仍以引擎自身校验为准。
func (b *Builder[S]) Graph() *Graph[S, *S] {
	return b.g
}

// Node 登记一个 State 节点：fn 直接读写 *S。
func (b *Builder[S]) Node(name string, fn func(context.Context, *S) error) *Builder[S] {
	if fn == nil {
		return b.record(errors.New("graph node function must be non-nil"))
	}
	return b.record(b.g.AddNode(name, StateNode(fn)))
}

// Start 声明入口节点，等价于 Edge(Start, name)。
func (b *Builder[S]) Start(name string) *Builder[S] {
	return b.record(b.g.AddEdge(Start, name))
}

// Edge 登记普通出边；to 为 End 时该节点输出即图输出。
func (b *Builder[S]) Edge(from, to string) *Builder[S] {
	return b.record(b.g.AddEdge(from, to))
}

// Branch 登记路由：router 每次决策只求值一次并返回 targets 之一。
// 目标必须预先声明，Compile 校验存在性与类型；返回值未声明时按路由错误处理。
func (b *Builder[S]) Branch(from string, router func(context.Context, *S) (string, error), targets ...string) *Builder[S] {
	if router == nil {
		return b.record(errors.New("graph route function must be non-nil"))
	}
	route := newRuntimeRoute(router)
	for _, target := range targets {
		route.targets[strings.TrimSpace(target)] = true
	}
	return b.record(b.g.addRuntimeRoute(from, route))
}

// Parallel 声明静态扇出：from 完成后 branches 并行执行，全部到达 join 后
// 由 merge 把各分支 State 副本写回主干 State。merge 只作用于本次扇出的
// 批内汇合，不影响图内其他步骤；成员节点的默认汇合边在 Compile 时补齐，
// 成员自行声明出边或 Branch 路由后不再补。
func (b *Builder[S]) Parallel(from string, branches []string, join string, merge func(context.Context, *S, []StepResult[*S]) error) *Builder[S] {
	b.record(b.g.AddFork(from, branches...))
	b.record(b.g.AddJoin(from, join))
	b.record(SetForkMerger(b.g, from, merge))
	for _, branch := range branches {
		b.pendingJoins = append(b.pendingJoins, memberJoin{member: branch, join: join})
	}
	return b
}

// Merge 登记每轮合并函数；使用 Send 而未用 Parallel 时需要它写回成员结果。
func (b *Builder[S]) Merge(merge func(context.Context, *S, []StepResult[*S]) error) *Builder[S] {
	return b.record(SetStepMerger(b.g, merge))
}

// Send 登记动态分派：handler 依据当前 State 生成任务清单（经 Send 构造），
// 成员完成后汇合到 join。from 不能再声明其他出边形式。
// 成员是 State 节点时，任务输入须为 *S（可传 handler 收到的 state 或新建值）。
func (b *Builder[S]) Send(from string, targets []string, join string, handler func(context.Context, *S) ([]DispatchTask, error)) *Builder[S] {
	if handler == nil {
		return b.record(errors.New("graph dispatch handler must be non-nil"))
	}
	return b.record(AddDispatch(b.g, from, targets, join, handler))
}

// MaxSteps 限制一次运行的最大步数；默认 DefaultMaxSteps。
func (b *Builder[S]) MaxSteps(limit int) *Builder[S] {
	return b.record(b.g.SetMaxSteps(limit))
}

// Compile 聚合累积的建图错误并完成编译；门面登记与引擎校验的问题一并返回。
// 选项为编译期绑定能力，如 Checkpointer(store) 绑定检查点存储，
// 运行时经 Thread("id") 启用。
func (b *Builder[S]) Compile(options ...CompileOption) (*CompiledGraph[S, *S], error) {
	if b == nil || b.g == nil {
		return nil, errors.New("graph builder is nil")
	}
	for _, pj := range b.pendingJoins {
		if !b.g.hasOutgoing(pj.member) {
			b.record(b.g.AddEdge(pj.member, pj.join))
		}
	}
	app, err := b.g.Compile(options...)
	return app, errors.Join(err, errors.Join(b.errs...))
}

func (b *Builder[S]) record(err error) *Builder[S] {
	if err != nil {
		b.errs = append(b.errs, err)
	}
	return b
}
