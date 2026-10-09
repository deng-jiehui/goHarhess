package graph

import (
	"context"
	"errors"
	"fmt"
	"reflect"
)

// DispatchTask 是动态分派清单的一个成员：目标节点与它的独立输入。
// 由 Send 构造；输入的实际类型在登记时与目标节点声明的输入类型比较，
// 并在持久化边界前冻结为 JSON。
type DispatchTask struct {
	Node  string
	input any
}

// Input 返回成员的任务输入（构造时传入的值）。
func (t DispatchTask) Input() any { return t.input }

// Send 生成一个动态分派任务：nodeID 是已注册的目标节点，input 是本次
// 成员执行冻结的独立输入。输入类型从传入值推导，登记时校验。
func Send[T any](nodeID string, input T) DispatchTask {
	return DispatchTask{Node: nodeID, input: input}
}

// dispatchSpec 登记动态分派：来源节点、允许目标集合、汇合节点与清单
// 生成函数。清单在每次到达来源节点时生成，属于一次新的组实例。
type dispatchSpec struct {
	targets   []string
	join      string
	stateType reflect.Type
	invoke    func(context.Context, any) ([]DispatchTask, error)
}

func cloneDispatches(in map[string]*dispatchSpec) map[string]*dispatchSpec {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]*dispatchSpec, len(in))
	for k, v := range in {
		copy := *v
		copy.targets = append([]string(nil), v.targets...)
		out[k] = &copy
	}
	return out
}

// AddDispatch 登记动态分派：运行时 handler 依据当前 State 生成任务清单，
// 框架为每个成员创建独立任务并按清单顺序汇合到 join。from 节点完成后
// 触发 handler；from 不能再声明普通出边、条件出边或静态扇出。
// 目标到汇合点的普通路径由使用者显式连线；目标可以是同一个节点。
//
// Go 方法不能声明类型参数，因此类型化入口是包级函数；S 与图的 State
// 类型一致，handler 收到本轮入口 State。
func AddDispatch[I, O, S any](g *Graph[I, O], from string, targets []string, join string, handler func(context.Context, *S) ([]DispatchTask, error)) error {
	if g == nil {
		return errors.New("graph cannot be nil")
	}
	if handler == nil {
		return errors.New("dispatch manifest handler must be a function")
	}
	if typeOf[*S]().Elem().Kind() != reflect.Struct {
		return errors.New("dispatch handler must have signature func(context.Context, *State) ([]DispatchTask, error)")
	}
	spec := &dispatchSpec{
		join:      join,
		stateType: typeOf[*S](),
	}
	spec.targets = append([]string(nil), targets...)
	spec.invoke = func(ctx context.Context, runtimeState any) (result []DispatchTask, err error) {
		defer func() {
			if p := recover(); p != nil {
				err = fmt.Errorf("dispatch handler panicked: %v", p)
			}
		}()
		return handler(ctx, typedArgument[*S](runtimeState))
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if _, ok := g.dispatches[from]; ok {
		return fmt.Errorf("dispatch %q already declared", from)
	}
	if _, ok := g.routes[from]; ok {
		return fmt.Errorf("dispatch %q cannot also declare a route", from)
	}
	if g.dispatches == nil {
		g.dispatches = map[string]*dispatchSpec{}
	}
	g.dispatches[from] = spec
	return nil
}

// validateDispatches 校验动态分派与静态拓扑的关系：来源不参与其他路由，
// 目标集合与汇合点存在，成员可达路径上的后续节点具备入口重建能力。
func (g *Graph[I, O]) validateDispatches() error {
	if len(g.dispatches) == 0 {
		return nil
	}
	if g.stepMerger == nil {
		return errors.New("dynamic dispatch requires a step merger")
	}
	if g.stepMerger.stateType != g.stateType {
		return errors.New("step merger State does not match graph State")
	}
	outgoing := map[string][]runtimeEdge{}
	for _, e := range g.edges {
		outgoing[e.from] = append(outgoing[e.from], e)
	}
	// 成员内部的路由候选目标与普通边同样参与可达性：Branch 声明的
	// 所有去向都可能被选中，任何一条到不了汇合点都应拒绝编译。
	routeTargets := map[string][]string{}
	for id, route := range g.routes {
		for target := range route.targets {
			routeTargets[id] = append(routeTargets[id], target)
		}
	}
	for from, spec := range g.dispatches {
		if from == Start || from == End {
			return fmt.Errorf("dispatch source %q is a reserved node", from)
		}
		if _, ok := g.nodes[from]; !ok {
			return fmt.Errorf("dispatch source %q is not registered", from)
		}
		if _, ok := g.forks[from]; ok {
			return fmt.Errorf("dispatch %q cannot also declare a fork", from)
		}
		if len(outgoing[from]) > 0 {
			return fmt.Errorf("dispatch %q cannot mix dispatch and ordinary edges", from)
		}
		if spec.stateType != g.stateType {
			return fmt.Errorf("dispatch %q handler State does not match graph State", from)
		}
		if spec.join == "" || spec.join == from || spec.join == Start {
			return fmt.Errorf("dispatch %q requires a distinct join node", from)
		}
		if _, ok := g.nodes[spec.join]; !ok {
			return fmt.Errorf("dispatch %q join %q is not registered", from, spec.join)
		}
		seen := map[string]bool{}
		for _, target := range spec.targets {
			if target == "" || seen[target] {
				return fmt.Errorf("dispatch %q has an empty or duplicate target %q", from, target)
			}
			seen[target] = true
			if target == spec.join || target == Start || target == End {
				return fmt.Errorf("dispatch %q target %q cannot be the join or a reserved node", from, target)
			}
			if _, ok := g.nodes[target]; !ok {
				return fmt.Errorf("dispatch %q target %q is not registered", from, target)
			}
			if _, ok := g.dispatches[target]; ok {
				return fmt.Errorf("dispatch %q target %q cannot itself be a dispatch source", from, target)
			}
			if _, ok := g.forks[target]; ok {
				return fmt.Errorf("dispatch %q target %q cannot be a fork source", from, target)
			}
			// 成员链的后续节点必须能从成员 State 重建输入：除成员入口
			// （首次调用使用冻结的分派输入）外，路径上的节点都要求 Pre。
			reached := map[string]bool{}
			revisit := false
			var walk func(string) error
			walk = func(id string) error {
				if id == spec.join {
					return nil
				}
				if id == target {
					revisit = true
				}
				if reached[id] {
					return nil
				}
				reached[id] = true
				if id == End {
					return fmt.Errorf("dispatch %q member path from %q does not reach join %q", from, target, spec.join)
				}
				if _, ok := g.nodes[id]; !ok {
					return fmt.Errorf("dispatch %q: node %q on member path is not registered", from, id)
				}
				if _, isFork := g.forks[id]; isFork {
					return fmt.Errorf("dispatch %q: fork source %q inside a member path is unsupported", from, id)
				}
				next := make([]string, 0, len(outgoing[id])+len(routeTargets[id]))
				for _, e := range outgoing[id] {
					next = append(next, e.to)
				}
				next = append(next, routeTargets[id]...)
				if len(next) == 0 {
					return fmt.Errorf("dispatch %q member path from %q does not reach join %q", from, target, spec.join)
				}
				for _, to := range next {
					if err := walk(to); err != nil {
						return err
					}
				}
				return nil
			}
			reached[target] = true
			targetNext := make([]string, 0, len(outgoing[target])+len(routeTargets[target]))
			for _, e := range outgoing[target] {
				targetNext = append(targetNext, e.to)
			}
			targetNext = append(targetNext, routeTargets[target]...)
			if len(targetNext) == 0 {
				return fmt.Errorf("dispatch %q member path from %q does not reach join %q", from, target, spec.join)
			}
			for _, to := range targetNext {
				if err := walk(to); err != nil {
					return err
				}
			}
			for id := range reached {
				if id == target && !revisit {
					// 成员入口的首次调用直接使用分派输入；入口再次被
					// 经过时按普通成员节点要求入口重建能力。
					continue
				}
				if g.nodes[id].pre == nil {
					return fmt.Errorf("dispatch %q member path node %q requires a PreHandler", from, id)
				}
			}
		}
	}
	return nil
}

// groupJoin 返回组的汇合节点与成员总数，供 runNativeStep 泛化处理
// 静态扇出与动态分派两种组。
func (g *Graph[I, O]) groupJoin(group *StepGroup) (join string, members int, err error) {
	if group.Dispatch {
		spec, ok := g.dispatches[group.Fork]
		if !ok {
			return "", 0, fmt.Errorf("%w: dispatch %q missing", ErrResumeIncompatible, group.Fork)
		}
		return spec.join, len(group.Members), nil
	}
	spec, ok := g.forks[group.Fork]
	if !ok {
		return "", 0, fmt.Errorf("%w: fork missing", ErrResumeIncompatible)
	}
	return spec.Join, len(spec.Branches), nil
}
