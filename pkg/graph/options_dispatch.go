package graph

import (
	"fmt"
	"reflect"
	"slices"

	"github.com/goharness/goharness/pkg/runnable"
)

// optionDispatch 是一次运行在入口静态分发的结果。
// 每个节点的最终选项遵循固定顺序：建图绑定 → 本次调用的同类组件广播 →
// 指定该节点的定向选项；同级内部按声明顺序。
type optionDispatch struct {
	// options 保存可直接传给节点调用的组件配置选项。
	options map[string][]runnable.Option
	// config 是当前 Graph 自身消费的条目。
	config []runnable.OptionSpec
}

func newOptionDispatch() *optionDispatch {
	return &optionDispatch{
		options: map[string][]runnable.Option{},
	}
}

// dispatchOptions 把调用选项与建图绑定选项静态分发到图内节点。
// 校验在入口完成：不存在的目标、无法继续的路径、声明类型不匹配都会报错；
// 合法目标位于未执行的条件分支不算未消费。
func (g *Graph[I, O]) dispatchOptions(options []runnable.Option) (*optionDispatch, error) {
	dispatch := newOptionDispatch()
	// 建图时绑定的节点默认选项先进入各自节点的列表。
	for id, node := range g.nodes {
		if len(node.options) > 0 {
			bound, err := runnable.FlattenOptions(node.options)
			if err != nil {
				return nil, fmt.Errorf("node %q bound options: %w", id, err)
			}
			dispatch.addBound(id, bound)
		}
	}
	specs, err := runnable.FlattenOptions(options)
	if err != nil {
		return nil, err
	}
	// 无目标的组件选项先按类型广播，再处理定向选项，保证覆盖优先级。
	var targeted []runnable.OptionSpec
	for _, spec := range specs {
		if len(spec.Path) > 0 {
			targeted = append(targeted, spec)
			continue
		}
		if err := g.dispatchUntargeted(dispatch, spec); err != nil {
			return nil, err
		}
	}
	for _, spec := range targeted {
		if err := g.dispatchTargeted(dispatch, spec); err != nil {
			return nil, err
		}
	}
	return dispatch, nil
}

// deliver 把已到达最终目标的选项包装为可传递的 Option；已消费的路径剥离，
// 组件解析时不再看到残留路径。
func deliver(spec runnable.OptionSpec) []runnable.Option {
	spec.Path = nil
	return runnable.OptionsFromSpecs([]runnable.OptionSpec{spec})
}

// passThrough 保留剩余路径把选项交给复合组件继续分发；仅在到达最终目标
// 后才可用 deliver 剥离路径。
func passThrough(spec runnable.OptionSpec) []runnable.Option {
	return runnable.OptionsFromSpecs([]runnable.OptionSpec{spec})
}

func (d *optionDispatch) addBound(node string, specs []runnable.OptionSpec) {
	for _, spec := range specs {
		d.options[node] = append(d.options[node], passThrough(spec)...)
	}
}

// dispatchUntargeted 处理无目标选项：Graph 自有配置在当前入口消费，
// 组件选项广播给当前作用域内所有声明接受该类型的节点。
func (g *Graph[I, O]) dispatchUntargeted(dispatch *optionDispatch, spec runnable.OptionSpec) error {
	switch {
	case spec.ConfigType == graphRunConfigType:
		dispatch.config = append(dispatch.config, spec)
		return nil
	}
	receivers := 0
	for id, node := range g.nodes {
		if !nodeAcceptsOption(node, spec.ConfigType) {
			continue
		}
		receivers++
		dispatch.options[id] = append(dispatch.options[id], deliver(spec)...)
	}
	if receivers == 0 {
		return fmt.Errorf("option %s for %s has no receiver in graph %q", spec.Describe(), spec.ConfigType, g.Name())
	}
	return nil
}

// dispatchTargeted 处理带路径的选项：每进入一层复合组件消费一个路径段，
// 定向配置到达所属组件解析。
func (g *Graph[I, O]) dispatchTargeted(dispatch *optionDispatch, spec runnable.OptionSpec) error {
	target := spec.Path[0]
	node, ok := g.nodes[target]
	if !ok {
		return fmt.Errorf("option %s targets unknown node %q in graph %q", spec.Describe(), target, g.Name())
	}
	if len(spec.Path) == 1 {
		// 声明了类型的节点在入口校验已知的不匹配；未声明的延迟到组件入口检查。
		// 转发型节点（ToolsNode/AgentNode）把选项原样交给内部组件自行解析，
		// 其声明类型只描述自身消费的配置，不限制透传内容。
		if !isOptionRelay(node.component) && len(node.optionTypes) > 0 && !slices.Contains(node.optionTypes, spec.ConfigType) {
			return fmt.Errorf("option %s for %s is not accepted by node %q", spec.Describe(), spec.ConfigType, target)
		}
		dispatch.options[target] = append(dispatch.options[target], deliver(spec)...)
		return nil
	}
	residual := spec
	residual.Path = spec.Path[1:]
	switch {
	case node.nestedGraph || isOptionRelay(node.component):
		// 嵌套图与转发型节点（如挂载子 Agent 的节点）在下一层入口继续消费剩余路径。
		dispatch.options[target] = append(dispatch.options[target], runnable.OptionsFromSpecs([]runnable.OptionSpec{residual})...)
		return nil
	default:
		return fmt.Errorf("option %s cannot continue path %v: node %q is not a composite", spec.Describe(), spec.Path, target)
	}
}

// optionRelay 是可选的复合组件标记：节点自身不是嵌套图，但内部还有
// 可继续消费路径段的组件（如挂载子 Agent 的节点）。
type optionRelay interface {
	OptionRelay() bool
}

func isOptionRelay(component any) bool {
	relay, ok := component.(optionRelay)
	return ok && relay != nil && relay.OptionRelay()
}

// nodeAcceptsOption 判断无目标组件选项在当前作用域是否被节点接收。
func nodeAcceptsOption(node runtimeNode, configType reflect.Type) bool {
	return slices.Contains(node.optionTypes, configType)
}
