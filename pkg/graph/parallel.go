package graph

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/goharness/goharness/pkg/runnable"
)

// ParallelBranch describes one pure Runnable and its State adapters.
// NewParallelBranch 提供编译期检查签名、运行期直接调用的构造方式。
type ParallelBranch struct {
	Name    string
	Node    any
	Pre     any
	Post    any
	Options []runnable.Option
}

// NewParallelBranch builds one branch with compile-time checked State
// adapters. The node only needs to expose Invoke with matching input and
// output types; its relation to the branch handlers is checked by Compile.
func NewParallelBranch[S, I, O any](
	name string,
	node interface {
		Invoke(context.Context, I, ...runnable.Option) (O, error)
	},
	pre func(context.Context, *S) (I, error),
	post func(context.Context, *S, O) error,
	options ...runnable.Option,
) ParallelBranch {
	branch := ParallelBranch{Name: name, Node: node, Options: options}
	if pre != nil {
		branch.Pre = newRuntimePreHandler(pre)
	}
	if post != nil {
		branch.Post = newRuntimePostHandler(post)
	}
	return branch
}

type runtimeParallelBranch struct {
	name        string
	inputType   reflect.Type
	outputType  reflect.Type
	invoke      func(context.Context, any, []runnable.Option) (any, error)
	pre         *runtimePreHandler
	post        *runtimePostHandler
	options     []runnable.Option
	optionTypes []reflect.Type
	component   any
	nestedGraph bool
	compile     func(map[uintptr]bool) error
}

type runtimeParallelNode struct {
	// join 是框架分配的统一汇合节点 ID：分支末端声明替换默认的直接汇合边，
	// 组名的出边在编译期被解析为从该汇合出发。
	join     string
	branches []runtimeParallelBranch
	merger   runtimeStateMerger
}

// parallelMember 标记一个并行组的成员节点，保留稳定的「组名＋分支名」身份。
type parallelMember struct {
	group  string
	branch string
}

type runtimeStateMerger struct {
	stateType reflect.Type
	invoke    func(context.Context, any, []any) error
}

type ParallelOption interface {
	applyParallel(*runtimeParallelConfig) error
}

type runtimeParallelOption func(*runtimeParallelConfig) error

func (f runtimeParallelOption) applyParallel(config *runtimeParallelConfig) error {
	if f == nil {
		return errors.New("parallel option is nil")
	}
	return f(config)
}

type runtimeParallelConfig struct {
	merger runtimeStateMerger
}

// WithStateMerger applies branch State snapshots to the base State in
// declaration order. The signature func(context.Context, *S, []*S) error is
// checked by the Go compiler; whether *S matches the graph State is validated
// by Compile.
func WithStateMerger[S any](handler func(context.Context, *S, []*S) error) ParallelOption {
	return runtimeParallelOption(func(config *runtimeParallelConfig) error {
		if handler == nil {
			return errors.New("State merger must be a non-nil function")
		}
		if typeOf[*S]().Elem().Kind() != reflect.Struct {
			return errors.New("State merger must have signature func(context.Context, *State, []*State) error")
		}
		if config.merger.invoke != nil {
			return errors.New("parallel node has multiple State mergers")
		}
		config.merger = newRuntimeStateMerger(handler)
		return nil
	})
}

// parallelJoinName 分配组内统一汇合节点的稳定身份。后缀使用保留形式，
// 分支名中的分隔符已在 AddParallelNode 校验，不会与成员节点冲突。
func parallelJoinName(name string) string { return name + "/__join__" }

// parallelMemberName 是成员节点的公共身份：分支通过「组名＋分支名」定位，
// 普通边与恢复契约都复用同一份映射。
func parallelMemberName(group, branch string) string { return group + "/" + branch }

// AddParallelNode adds a Fan-Out/Fan-In node. State is copied only at this
// ownership boundary by the configured State Codec.
//
// 声明即展开：组名仍是公共入口与路由身份，成员节点以「组名/分支名」登记，
// 汇合点由框架分配为「组名/__join__」。默认拓扑是 入口扇出 → 各分支 → 汇合；
// 给分支节点连出更多普通边即可让该分支继续自己的路径，所有路径必须到达汇合。
// 编译期解析：以组名为目标的边，源在成员路径内时指向汇合，其余指向入口；
// 以组名为源的边等价于从汇合出发。
func (g *Graph[I, O]) AddParallelNode(name string, branches []ParallelBranch, options ...ParallelOption) error {
	if g == nil {
		return validationError("graph.nil", "graph", "graph cannot be nil")
	}
	name = strings.TrimSpace(name)
	if name == "" {
		return validationError("node.id.empty", "node.id", "node ID cannot be empty")
	}
	if name == Start || name == End {
		return validationError("node.id.reserved", fmt.Sprintf("nodes[%q].id", name), "ID is reserved")
	}
	if _, ok := g.routes[name]; ok {
		return validationError("route.edge.mix", fmt.Sprintf("nodes[%q].id", name), "routed node cannot also be a parallel group entry")
	}
	if len(branches) == 0 {
		return validationError("parallel.branches.empty", fmt.Sprintf("nodes[%q].branches", name), "parallel node requires at least one branch")
	}
	config := runtimeParallelConfig{}
	for index, option := range options {
		if option == nil {
			return validationError("parallel.options.nil", fmt.Sprintf("nodes[%q].options[%d]", name, index), "option cannot be nil")
		}
		if err := option.applyParallel(&config); err != nil {
			return validationError("parallel.options.invalid", fmt.Sprintf("nodes[%q].options[%d]", name, index), "%v", err)
		}
	}
	if config.merger.invoke == nil {
		return validationError("parallel.merger.nil", fmt.Sprintf("nodes[%q].merger", name), "parallel node requires a State merger")
	}
	owned := make([]runtimeParallelBranch, len(branches))
	seen := map[string]bool{}
	for index, branch := range branches {
		branchName := strings.TrimSpace(branch.Name)
		if branchName == "" {
			branchName = fmt.Sprintf("branch[%d]", index)
		}
		if strings.Contains(branchName, "/") || branchName == "__join__" || branchName == Start || branchName == End {
			return validationError("parallel.branch.name", fmt.Sprintf("nodes[%q].branches[%d]", name, index), "branch name %q is reserved or contains a separator", branchName)
		}
		if seen[branchName] {
			return validationError("parallel.branch.duplicate", fmt.Sprintf("nodes[%q].branches[%d]", name, index), "branch name %q is duplicated", branchName)
		}
		seen[branchName] = true
		invoker, inputType, outputType, _, component, err := bindRuntimeNode(branchName, branch.Node)
		if err != nil {
			return validationError("parallel.branch.invalid", fmt.Sprintf("nodes[%q].branches[%d]", name, index), "%v", err)
		}
		// State 驱动的分支默认使用恒等 Pre 与空 Post：分支在自己的 State 副本上就地修改。
		if driven, ok := branch.Node.(stateDriven); ok {
			if branch.Pre == nil {
				branch.Pre = driven.stateNodePre()
			}
			if branch.Post == nil {
				branch.Post = driven.stateNodePost()
			}
		}
		pre, err := bindOptionalRuntimePreHandler(branch.Pre)
		if err != nil {
			return validationError("parallel.branch.pre.invalid", fmt.Sprintf("nodes[%q].branches[%d].pre", name, index), "%v", err)
		}
		post, err := bindOptionalRuntimePostHandler(branch.Post)
		if err != nil {
			return validationError("parallel.branch.post.invalid", fmt.Sprintf("nodes[%q].branches[%d].post", name, index), "%v", err)
		}
		for optionIndex, option := range branch.Options {
			if option == nil {
				return validationError("parallel.branch.options.nil", fmt.Sprintf("nodes[%q].branches[%d].options[%d]", name, index, optionIndex), "option cannot be nil")
			}
		}
		if component != nil && reflect.ValueOf(branch.Node).Pointer() == reflect.ValueOf(g).Pointer() {
			return validationError("parallel.branch.graph.self", fmt.Sprintf("nodes[%q].branches[%d]", name, index), "graph cannot contain itself")
		}
		owned[index] = runtimeParallelBranch{
			name: branchName, inputType: inputType, outputType: outputType, invoke: invoker,
			pre: pre, post: post, options: append([]runnable.Option(nil), branch.Options...),
			optionTypes: declaredOptionTypes(branch.Node), component: branch.Node, nestedGraph: component != nil,
		}
		if component != nil {
			owned[index].compile = component.compileGraph
		}
	}
	compilers := make([]func(map[uintptr]bool) error, 0, len(owned))
	for _, branch := range owned {
		if branch.compile != nil {
			compilers = append(compilers, branch.compile)
		}
	}
	spec := &runtimeParallelNode{join: parallelJoinName(name), branches: owned, merger: config.merger}
	stateT := config.merger.stateType
	entry := runtimeNode{
		id: name, inputType: stateT, outputType: stateT,
		info:     NodeInfo{ID: name, Kind: runnable.NodeKindParallel, Label: name, InputType: typeName(stateT), OutputType: typeName(stateT)},
		parallel: spec,
		// 入口只做边界：输入由 State 重建，输出原样透传，扇出在调度层完成。
		pre:    &runtimePreHandler{stateType: stateT, inputType: stateT, invoke: func(_ context.Context, s any) (any, error) { return s, nil }},
		invoke: func(_ context.Context, in any, _ []runnable.Option) (any, error) { return in, nil },
		compile: func(stack map[uintptr]bool) error {
			for _, compile := range compilers {
				if err := compile(stack); err != nil {
					return err
				}
			}
			return nil
		},
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if _, exists := g.nodes[name]; exists {
		return validationError("node.id.duplicate", fmt.Sprintf("nodes[%q].id", name), "node ID is already registered")
	}
	for index, branch := range owned {
		memberID := parallelMemberName(name, branch.name)
		if _, exists := g.nodes[memberID]; exists {
			return validationError("node.id.duplicate", fmt.Sprintf("nodes[%q].branches[%d]", name, index), "member node ID %q is already registered", memberID)
		}
	}
	if _, exists := g.nodes[spec.join]; exists {
		return validationError("node.id.duplicate", fmt.Sprintf("nodes[%q].id", name), "join node ID %q is already registered", spec.join)
	}
	g.nodes[name] = entry
	g.nodeOrder = append(g.nodeOrder, name)
	for _, branch := range owned {
		memberID := parallelMemberName(name, branch.name)
		g.nodes[memberID] = runtimeNode{
			id: memberID, component: branch.component, inputType: branch.inputType, outputType: branch.outputType,
			info:        NodeInfo{ID: memberID, Kind: runnable.NodeKindRunnable, Label: memberID, InputType: typeName(branch.inputType), OutputType: typeName(branch.outputType)},
			options:     append([]runnable.Option(nil), branch.options...),
			optionTypes: append([]reflect.Type(nil), branch.optionTypes...),
			pre:         branch.pre,
			post:        branch.post,
			invoke:      branch.invoke,
			compile:     branch.compile,
			nestedGraph: branch.nestedGraph,
			parallelMember: &parallelMember{
				group:  name,
				branch: branch.name,
			},
		}
		g.nodeOrder = append(g.nodeOrder, memberID)
	}
	g.nodes[spec.join] = runtimeNode{
		id: spec.join, inputType: stateT, outputType: stateT,
		info:         NodeInfo{ID: spec.join, Kind: runnable.NodeKindParallel, Label: spec.join, InputType: typeName(stateT), OutputType: typeName(stateT)},
		parallelJoin: spec,
	}
	g.nodeOrder = append(g.nodeOrder, spec.join)
	if g.parallelGroups == nil {
		g.parallelGroups = map[string]*runtimeParallelNode{}
	}
	g.parallelGroups[name] = spec
	return nil
}

// 注意：component 保存原始组件对象（与普通节点一致），编译下钻经由
// branch.compile，嵌套图与 relay 语义分别由 nestedGraph/relay 标记。

func bindOptionalRuntimePreHandler(handler any) (*runtimePreHandler, error) {
	if handler == nil {
		return nil, nil
	}
	if bound, ok := handler.(*runtimePreHandler); ok {
		return bound, nil
	}
	return bindRuntimePreHandler(handler)
}

func bindOptionalRuntimePostHandler(handler any) (*runtimePostHandler, error) {
	if handler == nil {
		return nil, nil
	}
	if bound, ok := handler.(*runtimePostHandler); ok {
		return bound, nil
	}
	return bindRuntimePostHandler(handler)
}

// newRuntimeStateMerger 由类型化签名生成直接调用闭包，与反射路径保持相同的
// panic 包装语义。
func newRuntimeStateMerger[S any](handler func(context.Context, *S, []*S) error) runtimeStateMerger {
	return runtimeStateMerger{stateType: typeOf[*S](), invoke: func(ctx context.Context, base any, branches []any) (err error) {
		defer func() {
			if recovered := recover(); recovered != nil {
				err = fmt.Errorf("State merger panicked: %v", recovered)
			}
		}()
		typed := make([]*S, len(branches))
		for index, branch := range branches {
			typed[index] = typedArgument[*S](branch)
		}
		return handler(ctx, typedArgument[*S](base), typed)
	}}
}
