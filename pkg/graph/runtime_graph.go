package graph

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"strings"
	"sync"

	"github.com/goharness/goharness/pkg/runnable"
	"github.com/goharness/goharness/pkg/state"
)

var (
	contextType     = reflect.TypeFor[context.Context]()
	errorType       = reflect.TypeFor[error]()
	optionSliceType = reflect.TypeFor[[]runnable.Option]()
	boolType        = reflect.TypeFor[bool]()
)

// Graph is the public graph boundary. Node and State types are captured when
// components are registered and validated by Compile.
type Graph[I, O any] struct {
	mu           sync.RWMutex
	name         string
	description  string
	inputType    reflect.Type
	outputType   reflect.Type
	stateType    reflect.Type
	stateFactory func(context.Context, I) (any, error)
	codec        StateCodec
	codecID      string
	codecVersion int
	codecType    reflect.Type
	nodes        map[string]runtimeNode
	nodeOrder    []string
	edges        []runtimeEdge
	maxSteps     int
	forks        map[string]forkSpec
	dispatches   map[string]*dispatchSpec
	routes       map[string]*runtimeRoute
	// boundCheckpointStore 是 Checkpointer 编译选项绑定的存储；运行入口
	// 在解析用户选项前作为默认值注入，WithCheckpoint 显式指定时仍覆盖。
	boundCheckpointStore state.Store
	stepMerger           *runtimeStepMerger
	maxConcurrency       int
	built                bool
	revision             string
	resumableErr         error
}

type runtimeNode struct {
	id          string
	component   any
	inputType   reflect.Type
	outputType  reflect.Type
	info        NodeInfo
	options     []runnable.Option
	optionTypes []reflect.Type
	pre         *runtimePreHandler
	post        *runtimePostHandler
	invoke      func(context.Context, any, []runnable.Option) (any, error)
	stream      func(context.Context, any, []runnable.Option) (erasedStream, error)
	collect     func(context.Context, erasedStream, []runnable.Option) (any, error)
	transform   func(context.Context, erasedStream, []runnable.Option) (erasedStream, error)
	compile     func(map[uintptr]bool) error
	nestedGraph bool
}

type runtimeEdge struct {
	from      string
	to        string
	label     string
	condition *runtimeCondition
}

type runtimePreHandler struct {
	stateType reflect.Type
	inputType reflect.Type
	invoke    func(context.Context, any) (any, error)
}

type runtimePostHandler struct {
	stateType  reflect.Type
	outputType reflect.Type
	invoke     func(context.Context, any, any) error
}

type runtimeCondition struct {
	inputType reflect.Type
	invoke    func(context.Context, any) (bool, error)
}

// runtimeRoute 是单次求值的路由决策：一次调用直接返回目标节点名。
// 目标集合在声明期固定，运行期不再按出边逐条求值谓词。
type runtimeRoute struct {
	inputType reflect.Type
	targets   map[string]bool
	invoke    func(context.Context, any) (string, error)
}

// NodeOption configures Graph-owned node adapters. Runnable options must be
// wrapped with WithNodeOptions so State handlers never leak to pure nodes.
type NodeOption interface {
	applyNode(*runtimeNodeConfig) error
}

type runtimeNodeOption func(*runtimeNodeConfig) error

func (f runtimeNodeOption) applyNode(config *runtimeNodeConfig) error {
	if f == nil {
		return errors.New("graph node option is nil")
	}
	return f(config)
}

type runtimeNodeConfig struct {
	pre     *runtimePreHandler
	post    *runtimePostHandler
	options []runnable.Option
}

// WithPreHandler maps runtime State to one pure node input. The signature
// func(context.Context, *S) (I, error) is checked by the Go compiler; how *S
// relates to the graph State and I to the node input is validated by Compile.
func WithPreHandler[S, I any](handler func(context.Context, *S) (I, error)) NodeOption {
	return runtimeNodeOption(func(config *runtimeNodeConfig) error {
		if handler == nil {
			return errors.New("pre handler must be a non-nil function")
		}
		if config.pre != nil {
			return errors.New("graph node has multiple pre handlers")
		}
		config.pre = newRuntimePreHandler(handler)
		return nil
	})
}

// WithPostHandler applies one pure node output to runtime State. The signature
// func(context.Context, *S, O) error is checked by the Go compiler.
func WithPostHandler[S, O any](handler func(context.Context, *S, O) error) NodeOption {
	return runtimeNodeOption(func(config *runtimeNodeConfig) error {
		if handler == nil {
			return errors.New("post handler must be a non-nil function")
		}
		if config.post != nil {
			return errors.New("graph node has multiple post handlers")
		}
		config.post = newRuntimePostHandler(handler)
		return nil
	})
}

// WithNodeOptions forwards invocation options only to the pure node.
func WithNodeOptions(options ...runnable.Option) NodeOption {
	return runtimeNodeOption(func(config *runtimeNodeConfig) error {
		for index, option := range options {
			if option == nil {
				return fmt.Errorf("graph node runnable option %d is nil", index)
			}
		}
		config.options = append(config.options, options...)
		return nil
	})
}

func NewGraph[I, O any](name string) *Graph[I, O] {
	return &Graph[I, O]{
		name:       name,
		inputType:  typeOf[I](),
		outputType: typeOf[O](),
		nodes:      map[string]runtimeNode{},
		maxSteps:   DefaultMaxSteps,
	}
}

// SetStateFactory is the compile-time checked form of (*Graph).SetStateFactory.
// Go methods cannot declare type parameters, so the typed variant is a package
// function; I and O are inferred from the Graph, S from the factory.
func SetStateFactory[I, O, S any](g *Graph[I, O], factory func(context.Context, I) (*S, error)) error {
	if g == nil {
		return validationError("graph.nil", "graph", "graph cannot be nil")
	}
	if factory == nil {
		return validationError("graph.state_factory.invalid", "state_factory", "state factory must be a non-nil function")
	}
	stateType := typeOf[*S]()
	if stateType.Elem().Kind() != reflect.Struct {
		return validationError("graph.state_factory.pointer", "state_factory", "state factory must return a pointer to struct, got %s", typeName(stateType))
	}
	boundFactory := func(ctx context.Context, input I) (result any, err error) {
		defer func() {
			if recovered := recover(); recovered != nil {
				err = fmt.Errorf("state factory panicked: %v", recovered)
			}
		}()
		state, err := factory(ctx, input)
		if err != nil {
			return nil, err
		}
		if state == nil {
			return nil, errors.New("state factory returned nil State")
		}
		return state, nil
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	g.stateType = stateType
	g.stateFactory = boundFactory
	return nil
}

// SetStateFactory configures one run-exclusive pointer State factory.
func (g *Graph[I, O]) SetStateFactory(factory any) error {
	if g == nil {
		return validationError("graph.nil", "graph", "graph cannot be nil")
	}
	value := reflect.ValueOf(factory)
	if !value.IsValid() || isNilReflectValue(value) || value.Kind() != reflect.Func {
		return validationError("graph.state_factory.invalid", "state_factory", "state factory must be a non-nil function")
	}
	typeOfFactory := value.Type()
	if typeOfFactory.NumIn() != 2 || typeOfFactory.In(0) != contextType || typeOfFactory.In(1) != typeOf[I]() || typeOfFactory.NumOut() != 2 || typeOfFactory.Out(1) != errorType {
		return validationError("graph.state_factory.signature", "state_factory", "state factory must have signature func(context.Context, %s) (*State, error)", typeName(typeOf[I]()))
	}
	stateType := typeOfFactory.Out(0)
	if stateType.Kind() != reflect.Pointer || stateType.Elem().Kind() != reflect.Struct {
		return validationError("graph.state_factory.pointer", "state_factory", "state factory must return a pointer to struct, got %s", typeName(stateType))
	}
	boundFactory := func(ctx context.Context, input I) (result any, err error) {
		defer func() {
			if recovered := recover(); recovered != nil {
				err = fmt.Errorf("state factory panicked: %v", recovered)
			}
		}()
		outputs := value.Call([]reflect.Value{reflect.ValueOf(ctx), reflectArgument(typeOf[I](), any(input))})
		if !outputs[1].IsNil() {
			return nil, outputs[1].Interface().(error)
		}
		if outputs[0].IsNil() {
			return nil, errors.New("state factory returned nil State")
		}
		return outputs[0].Interface(), nil
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	g.stateType = stateType
	g.stateFactory = boundFactory
	return nil
}

func (g *Graph[I, O]) AddNode(name string, node any, options ...NodeOption) error {
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
	invoker, inputType, outputType, info, component, err := bindRuntimeNode(name, node)
	if err != nil {
		return err
	}
	config := runtimeNodeConfig{}
	for index, option := range options {
		if option == nil {
			return validationError("node.options.nil", fmt.Sprintf("nodes[%q].options[%d]", name, index), "option cannot be nil")
		}
		if err := option.applyNode(&config); err != nil {
			return validationError("node.options.invalid", fmt.Sprintf("nodes[%q].options[%d]", name, index), "%v", err)
		}
	}
	if config.pre == nil {
		if driven, ok := node.(stateDriven); ok {
			config.pre = driven.stateNodePre()
		}
	}
	entry := runtimeNode{
		id: name, component: node, inputType: inputType, outputType: outputType, info: info,
		options: append([]runnable.Option(nil), config.options...), optionTypes: declaredOptionTypes(node),
		pre: config.pre, post: config.post, invoke: invoker,
	}
	if component != nil {
		entry.compile = component.compileGraph
		entry.nestedGraph = true
		entry.info.Kind = runnable.NodeKindGraph
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if _, exists := g.nodes[name]; exists {
		return validationError("node.id.duplicate", fmt.Sprintf("nodes[%q].id", name), "node ID is already registered")
	}
	if component != nil && reflect.ValueOf(node).Pointer() == reflect.ValueOf(g).Pointer() {
		return validationError("node.graph.self", fmt.Sprintf("nodes[%q]", name), "graph cannot contain itself")
	}
	g.nodes[name] = entry
	g.nodeOrder = append(g.nodeOrder, name)
	return nil
}

func (g *Graph[I, O]) AddEdge(from, to string) error {
	return g.addRuntimeEdge(from, to, "", nil)
}

func (g *Graph[I, O]) AddBranch(from, to, label string, predicate any) error {
	if g == nil {
		return validationError("graph.nil", "graph", "graph cannot be nil")
	}
	condition, err := bindRuntimeCondition(predicate)
	if err != nil {
		return err
	}
	return g.addRuntimeEdge(from, to, label, condition)
}

// AddBranch is the compile-time checked form of (*Graph).AddBranch. V is
// inferred from the predicate; it may declare the source output type or the
// State type, and Compile rejects a predicate matching neither or both.
func AddBranch[I, O, V any](g *Graph[I, O], from, to, label string, predicate func(context.Context, V) (bool, error)) error {
	if g == nil {
		return validationError("graph.nil", "graph", "graph cannot be nil")
	}
	if predicate == nil {
		return errors.New("graph branch predicate must be a non-nil function")
	}
	return g.addRuntimeEdge(from, to, label, newRuntimeCondition(predicate))
}

func (g *Graph[I, O]) addRuntimeEdge(from, to, label string, condition *runtimeCondition) error {
	if g == nil {
		return validationError("graph.nil", "graph", "graph cannot be nil")
	}
	from = strings.TrimSpace(from)
	to = strings.TrimSpace(to)
	if from == "" {
		return validationError("edge.from.empty", "edges.from", "source node cannot be empty")
	}
	if to == "" {
		return validationError("edge.to.empty", "edges.to", "target node cannot be empty")
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if _, ok := g.routes[from]; ok {
		return validationError("route.edge.mix", fmt.Sprintf("routes[%q]", from), "routed node cannot declare ordinary edges")
	}
	g.edges = append(g.edges, runtimeEdge{from: from, to: to, label: strings.TrimSpace(label), condition: condition})
	return nil
}

// addRuntimeRoute 登记单次求值的路由：from 的出边集合由路由声明接管，
// 不能再声明普通出边、条件出边、静态扇出、动态分派或并行组。
func (g *Graph[I, O]) addRuntimeRoute(from string, route *runtimeRoute) error {
	if g == nil {
		return validationError("graph.nil", "graph", "graph cannot be nil")
	}
	from = strings.TrimSpace(from)
	if from == "" || from == End {
		return validationError("route.from.invalid", "routes.from", "route source must be a node or START")
	}
	if route == nil || len(route.targets) == 0 {
		return validationError("route.targets.empty", fmt.Sprintf("routes[%q]", from), "route requires declared targets")
	}
	for target := range route.targets {
		if target == "" || target == Start {
			return validationError("route.targets.invalid", fmt.Sprintf("routes[%q]", from), "route target %q is invalid", target)
		}
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if _, ok := g.routes[from]; ok {
		return validationError("route.duplicate", fmt.Sprintf("routes[%q]", from), "route already declared")
	}
	for _, e := range g.edges {
		if e.from == from {
			return validationError("route.edge.mix", fmt.Sprintf("routes[%q]", from), "routed node cannot declare ordinary edges")
		}
	}
	if _, ok := g.forks[from]; ok {
		return validationError("route.edge.mix", fmt.Sprintf("routes[%q]", from), "routed node cannot also declare a fork")
	}
	if _, ok := g.dispatches[from]; ok {
		return validationError("route.edge.mix", fmt.Sprintf("routes[%q]", from), "routed node cannot also declare a dispatch")
	}
	if g.routes == nil {
		g.routes = map[string]*runtimeRoute{}
	}
	g.routes[from] = route
	return nil
}

// newRuntimeRoute 把一次决策包装为路由：invoke 每次选择只调用一次，
// 返回值必须是已声明目标之一，否则运行时按路由错误处理。
func newRuntimeRoute[V any](router func(context.Context, V) (string, error)) *runtimeRoute {
	return &runtimeRoute{inputType: typeOf[V](), targets: map[string]bool{}, invoke: func(ctx context.Context, input any) (target string, err error) {
		defer func() {
			if recovered := recover(); recovered != nil {
				err = fmt.Errorf("graph route panicked: %v", recovered)
			}
		}()
		return router(ctx, typedArgument[V](input))
	}}
}

func cloneRuntimeRoutes(in map[string]*runtimeRoute) map[string]*runtimeRoute {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]*runtimeRoute, len(in))
	for k, v := range in {
		copy := *v
		copy.targets = maps.Clone(v.targets)
		out[k] = &copy
	}
	return out
}

func (g *Graph[I, O]) Name() string {
	if g == nil {
		return ""
	}
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.name
}

func (g *Graph[I, O]) Description() string {
	if g == nil {
		return ""
	}
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.description
}

func (g *Graph[I, O]) SetDescription(value string) {
	if g == nil {
		return
	}
	g.mu.Lock()
	g.description = value
	g.mu.Unlock()
}

func (g *Graph[I, O]) SetMaxSteps(value int) error {
	if g == nil {
		return validationError("graph.nil", "graph", "graph cannot be nil")
	}
	if value <= 0 {
		return validationError("graph.max_steps.invalid", "max_steps", "must be greater than zero, got %d", value)
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	g.maxSteps = value
	return nil
}

func (g *Graph[I, O]) SetNodeOptions(id string, options ...runnable.Option) error {
	for index, option := range options {
		if option == nil {
			return validationError("node.options.nil", fmt.Sprintf("nodes[%q].options[%d]", id, index), "option cannot be nil")
		}
	}
	if g == nil {
		return validationError("graph.nil", "graph", "graph cannot be nil")
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	node, ok := g.nodes[id]
	if !ok {
		return validationError("node.missing", fmt.Sprintf("nodes[%q]", id), "node is not registered")
	}
	node.options = append([]runnable.Option(nil), options...)
	g.nodes[id] = node
	return nil
}

func (g *Graph[I, O]) Validate() error {
	if g == nil {
		return validationError("graph.nil", "graph", "graph cannot be nil")
	}
	g.mu.RLock()
	defer g.mu.RUnlock()
	if errs := g.validateRuntimeGraph(); len(errs) > 0 {
		return errs
	}
	return nil
}

// CompileOption 是编译期绑定能力的选项：能力在 Compile 时落到执行快照，
// 运行入口经 runnable.Option 逐项启用。当前支持 Checkpointer。
type CompileOption interface {
	applyCompile(*compileConfig) error
}

type compileOptionFunc func(*compileConfig) error

func (f compileOptionFunc) applyCompile(c *compileConfig) error {
	if f == nil {
		return errors.New("compile option is nil")
	}
	return f(c)
}

type compileConfig struct {
	checkpointStore state.Store
}

// Checkpointer 在编译期绑定检查点存储；运行时以 Thread("id") 启用一次
// 持久化执行。已有 WithCheckpoint(store, key) 运行选项不受影响。
func Checkpointer(store state.Store) CompileOption {
	return compileOptionFunc(func(c *compileConfig) error {
		if isNilCheckpointStore(store) {
			return errors.New("checkpointer store cannot be nil")
		}
		c.checkpointStore = store
		return nil
	})
}

// Compile validates and freezes one immutable execution snapshot. The Builder
// remains mutable and can be compiled again.
func (g *Graph[I, O]) Compile(options ...CompileOption) (*CompiledGraph[I, O], error) {
	if g == nil {
		return nil, validationError("graph.nil", "graph", "graph cannot be nil")
	}
	runtime := g.cloneBuilder()
	var config compileConfig
	for index, option := range options {
		if option == nil {
			return nil, validationError("graph.compile.option", fmt.Sprintf("compile_options[%d]", index), "option cannot be nil")
		}
		if err := option.applyCompile(&config); err != nil {
			return nil, validationError("graph.compile.option", fmt.Sprintf("compile_options[%d]", index), "%v", err)
		}
	}
	runtime.boundCheckpointStore = config.checkpointStore
	if err := runtime.compileGraph(map[uintptr]bool{}); err != nil {
		return nil, err
	}
	return compileRuntimePlans(runtime)
}

// hasOutgoing 报告节点是否已声明出边形式（普通/条件边、路由、扇出或
// 动态分派）；门面用它决定是否为并行成员补默认汇合边。
func (g *Graph[I, O]) hasOutgoing(id string) bool {
	g.mu.RLock()
	defer g.mu.RUnlock()
	if _, ok := g.routes[id]; ok {
		return true
	}
	if _, ok := g.forks[id]; ok {
		return true
	}
	if _, ok := g.dispatches[id]; ok {
		return true
	}
	for _, e := range g.edges {
		if e.from == id {
			return true
		}
	}
	return false
}

func (g *Graph[I, O]) cloneBuilder() *Graph[I, O] {
	g.mu.RLock()
	defer g.mu.RUnlock()
	nodes := make(map[string]runtimeNode, len(g.nodes))
	for id, node := range g.nodes {
		node.options = append([]runnable.Option(nil), node.options...)
		nodes[id] = node
	}
	return &Graph[I, O]{
		forks: cloneForks(g.forks), dispatches: cloneDispatches(g.dispatches), routes: cloneRuntimeRoutes(g.routes), stepMerger: g.stepMerger, maxConcurrency: g.maxConcurrency,

		name: g.name, description: g.description, inputType: g.inputType, outputType: g.outputType, stateType: g.stateType,
		stateFactory: g.stateFactory, codec: g.codec, codecID: g.codecID, codecVersion: g.codecVersion, codecType: g.codecType,
		nodes: nodes, nodeOrder: append([]string(nil), g.nodeOrder...), edges: append([]runtimeEdge(nil), g.edges...), maxSteps: g.maxSteps,
	}
}

func (g *Graph[I, O]) compileGraph(stack map[uintptr]bool) error {
	if g == nil {
		return validationError("graph.nil", "graph", "graph cannot be nil")
	}
	identity := reflect.ValueOf(g).Pointer()
	if stack[identity] {
		return validationError("graph.cycle", "graph", "nested graph cycle detected at %q", g.Name())
	}
	g.mu.RLock()
	if g.built {
		g.mu.RUnlock()
		return nil
	}
	compilers := make([]func(map[uintptr]bool) error, 0, len(g.nodes))
	for _, node := range g.nodes {
		if node.compile != nil {
			compilers = append(compilers, node.compile)
		}
	}
	g.mu.RUnlock()
	stack[identity] = true
	for _, compile := range compilers {
		if err := compile(stack); err != nil {
			delete(stack, identity)
			return err
		}
	}
	delete(stack, identity)
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.built {
		return nil
	}
	if errs := g.validateRuntimeGraph(); len(errs) > 0 {
		return errs
	}
	if err := g.compileRecoveryMetadata(); err != nil {
		return err
	}
	g.built = true
	return nil
}

func (g *Graph[I, O]) Snapshot() Snapshot {
	if g == nil {
		return Snapshot{}
	}
	g.mu.RLock()
	defer g.mu.RUnlock()
	nodes := make([]NodeInfo, 0, len(g.nodeOrder))
	for _, id := range g.nodeOrder {
		nodes = append(nodes, g.nodes[id].info)
	}
	edges := make([]EdgeInfo, len(g.edges))
	for index, edge := range g.edges {
		edges[index] = EdgeInfo{From: edge.from, To: edge.to, Label: edge.label, Conditional: edge.condition != nil}
	}
	// 路由按预声明目标投影为条件出边，快照与运行期看到同一份拓扑。
	for from, route := range g.routes {
		for _, target := range slices.Sorted(maps.Keys(route.targets)) {
			edges = append(edges, EdgeInfo{From: from, To: target, Conditional: true})
		}
	}
	return Snapshot{
		Name: g.name, Description: g.description, Built: g.built, MaxSteps: g.maxSteps,
		InputType: typeName(g.inputType), OutputType: typeName(g.outputType), StateType: typeName(g.stateType),
		Nodes: nodes, Edges: edges,
	}
}

func (g *Graph[I, O]) validateRuntimeTypes() error {
	if err := g.validateRuntimeCodec(); err != nil {
		return err
	}
	if g.stateFactory == nil {
		for name, node := range g.nodes {
			if node.pre != nil || node.post != nil {
				return validationError("graph.state_factory.missing", fmt.Sprintf("nodes[%q]", name), "State handlers require a State factory")
			}
		}
	}
	for name, node := range g.nodes {
		if node.pre != nil && (node.pre.stateType != g.stateType || node.pre.inputType != node.inputType) {
			return validationError("node.pre.type_mismatch", fmt.Sprintf("nodes[%q]", name), "pre handler types %s/%s do not match State/node %s/%s", typeName(node.pre.stateType), typeName(node.pre.inputType), typeName(g.stateType), typeName(node.inputType))
		}
		if node.post != nil && (node.post.stateType != g.stateType || node.post.outputType != node.outputType) {
			return validationError("node.post.type_mismatch", fmt.Sprintf("nodes[%q]", name), "post handler types %s/%s do not match State/node %s/%s", typeName(node.post.stateType), typeName(node.post.outputType), typeName(g.stateType), typeName(node.outputType))
		}
	}
	for index, edge := range g.edges {
		sourceType, ok := g.runtimeSourceType(edge.from)
		if !ok {
			continue
		}
		if edge.condition != nil {
			matchesOutput := sourceType.AssignableTo(edge.condition.inputType)
			matchesState := g.stateType != nil && g.stateType.AssignableTo(edge.condition.inputType)
			if matchesOutput && matchesState {
				return validationError("edge.branch.ambiguous", edgePath(index), "branch input %s matches source output and State", typeName(edge.condition.inputType))
			}
			if !matchesOutput && !matchesState {
				return validationError("edge.branch.type_mismatch", edgePath(index), "branch input %s matches neither source output %s nor State %s", typeName(edge.condition.inputType), typeName(sourceType), typeName(g.stateType))
			}
		}
		if edge.to == End {
			if !sourceType.AssignableTo(g.outputType) {
				return validationError("edge.end.type_mismatch", edgePath(index), "%s output %s is not assignable to graph output %s", edge.from, typeName(sourceType), typeName(g.outputType))
			}
			continue
		}
		target, exists := g.nodes[edge.to]
		if exists && target.pre == nil && !sourceType.AssignableTo(target.inputType) {
			return validationError("edge.type_mismatch", edgePath(index), "%s output %s is not assignable to %s input %s", edge.from, typeName(sourceType), edge.to, typeName(target.inputType))
		}
	}
	for from, route := range g.routes {
		sourceType, ok := g.runtimeSourceType(from)
		if !ok {
			continue
		}
		// 路由输入与条件边同一套决议：能匹配节点输出或 State 之一即可；
		// 两者同时匹配时运行期按 State 处理，不视为歧义。
		matchesOutput := sourceType.AssignableTo(route.inputType)
		matchesState := g.stateType != nil && g.stateType.AssignableTo(route.inputType)
		if !matchesOutput && !matchesState {
			return validationError("route.input.type_mismatch", fmt.Sprintf("routes[%q]", from), "route input %s matches neither source output %s nor State %s", typeName(route.inputType), typeName(sourceType), typeName(g.stateType))
		}
		// 目标类型与普通边同一套规则：直接消费时要求源输出可赋给目标
		// 输入；带 Pre/并行结构的节点自行重建输入，跳过比较；End 按图
		// 输出类型校验。
		for target := range route.targets {
			if target == End {
				if !sourceType.AssignableTo(g.outputType) {
					return validationError("route.end.type_mismatch", fmt.Sprintf("routes[%q]", from), "%s output %s is not assignable to graph output %s", from, typeName(sourceType), typeName(g.outputType))
				}
				continue
			}
			targetNode, exists := g.nodes[target]
			if exists && targetNode.pre == nil && !sourceType.AssignableTo(targetNode.inputType) {
				return validationError("route.target.type_mismatch", fmt.Sprintf("routes[%q]", from), "%s output %s is not assignable to %s input %s", from, typeName(sourceType), target, typeName(targetNode.inputType))
			}
		}
	}
	return nil
}

// validateRoutes 校验路由拓扑：来源是已注册节点或 START，预声明目标
// 全部存在；路由不再与其他出边形式混用（登记时已互斥）。
func (g *Graph[I, O]) validateRoutes() error {
	for from, route := range g.routes {
		if from != Start {
			if _, ok := g.nodes[from]; !ok {
				return fmt.Errorf("route %q source is not registered", from)
			}
		}
		for target := range route.targets {
			if target != End {
				if _, ok := g.nodes[target]; !ok {
					return fmt.Errorf("route %q target %q is not registered", from, target)
				}
			}
		}
	}
	return nil
}

func (g *Graph[I, O]) validateRuntimeGraph() ValidationErrors {
	var errs ValidationErrors
	if err := g.validateForks(); err != nil {
		errs = append(errs, validationError("graph.fork.invalid", "forks", "%v", err))
	}
	if err := g.validateDispatches(); err != nil {
		errs = append(errs, validationError("graph.dispatch.invalid", "dispatches", "%v", err))
	}
	if err := g.validateRoutes(); err != nil {
		errs = append(errs, validationError("graph.route.invalid", "routes", "%v", err))
	}
	if err := g.validateRuntimeTypes(); err != nil {
		switch value := err.(type) {
		case ValidationError:
			errs = append(errs, value)
		case ValidationErrors:
			errs = append(errs, value...)
		default:
			errs = append(errs, validationError("graph.runtime.invalid", "graph", "%v", err))
		}
	}
	if strings.TrimSpace(g.name) == "" {
		errs = append(errs, validationError("graph.name.empty", "name", "graph name cannot be empty"))
	}
	if g.maxSteps <= 0 {
		errs = append(errs, validationError("graph.max_steps.invalid", "max_steps", "must be greater than zero, got %d", g.maxSteps))
	}
	if len(g.nodes) == 0 {
		errs = append(errs, validationError("graph.nodes.empty", "nodes", "graph requires at least one node"))
	}
	edges := g.edges
	outgoing := map[string][]int{}
	reverse := map[string][]string{}
	seenEdges := map[string]int{}
	for index, candidate := range edges {
		path := edgePath(index)
		from, to := candidate.from, candidate.to
		if from == End {
			errs = append(errs, validationError("edge.from.end", path+".from", "END cannot have outgoing edges"))
		}
		if to == Start {
			errs = append(errs, validationError("edge.to.start", path+".to", "START cannot have incoming edges"))
		}
		if from != Start {
			if _, ok := g.nodes[from]; !ok {
				errs = append(errs, validationError("edge.from.missing", path+".from", "node %q is not registered", from))
			}
		}
		if to != End {
			if _, ok := g.nodes[to]; !ok {
				errs = append(errs, validationError("edge.to.missing", path+".to", "node %q is not registered", to))
			}
		}
		key := from + "\x00" + to + "\x00" + candidate.label
		if first, exists := seenEdges[key]; exists {
			errs = append(errs, validationError("edge.duplicate", path, "duplicates edges[%d]", first))
		} else {
			seenEdges[key] = index
		}
		outgoing[from] = append(outgoing[from], index)
		reverse[to] = append(reverse[to], from)
	}
	if len(outgoing[Start]) == 0 {
		// START 上的路由同样构成入口：候选目标即首节点集合。
		if _, routed := g.routes[Start]; !routed {
			errs = append(errs, validationError("graph.start.missing", "edges", "START requires an outgoing edge"))
		}
	}
	for from, indexes := range outgoing {
		unconditional, conditional := 0, 0
		for _, index := range indexes {
			if edges[index].condition == nil {
				unconditional++
			} else {
				conditional++
			}
		}
		if unconditional > 1 && len(g.forks[from].Branches) == 0 && g.dispatches[from] == nil {
			errs = append(errs, validationError("edge.fallback.multiple", fmt.Sprintf("outgoing[%q]", from), "only one unconditional edge is allowed, got %d", unconditional))
		}
		if conditional > 0 && unconditional != 1 {
			errs = append(errs, validationError("edge.fallback.required", fmt.Sprintf("outgoing[%q]", from), "conditional routing requires exactly one unconditional fallback"))
		}
	}
	for _, id := range g.nodeOrder {
		if len(outgoing[id]) == 0 {
			// 动态分派来源的路由由清单接管，没有静态出边。
			if _, ok := g.dispatches[id]; ok {
				continue
			}
			// 路由节点的出边集合由预声明目标接管，同样没有静态出边。
			if _, ok := g.routes[id]; ok {
				continue
			}
			errs = append(errs, validationError("node.outgoing.missing", fmt.Sprintf("nodes[%q]", id), "node requires an outgoing edge"))
		}
	}
	// 动态分派在静态拓扑中没有普通出边：可达性遍历使用「来源→各目标」
	// 的虚拟边补足，成员路径上的后续节点仍走真实边；路由目标同理投影。
	walkOutgoing, walkEdges := outgoing, edges
	if len(g.dispatches) > 0 || len(g.routes) > 0 {
		walkEdges = append([]runtimeEdge(nil), edges...)
		walkOutgoing = map[string][]int{}
		for from, indexes := range outgoing {
			walkOutgoing[from] = append([]int(nil), indexes...)
		}
		for from, spec := range g.dispatches {
			for _, target := range spec.targets {
				walkOutgoing[from] = append(walkOutgoing[from], len(walkEdges))
				walkEdges = append(walkEdges, runtimeEdge{from: from, to: target})
				reverse[target] = append(reverse[target], from)
			}
		}
		// 路由节点的出边以预声明目标投影，供可达性遍历看到完整拓扑。
		for from, route := range g.routes {
			targets := slices.Sorted(maps.Keys(route.targets))
			for _, target := range targets {
				walkOutgoing[from] = append(walkOutgoing[from], len(walkEdges))
				walkEdges = append(walkEdges, runtimeEdge{from: from, to: target})
				reverse[target] = append(reverse[target], from)
			}
		}
	}
	reachable := walkRuntimeForward(Start, walkOutgoing, walkEdges)
	for _, id := range g.nodeOrder {
		if !reachable[id] {
			errs = append(errs, validationError("node.unreachable", fmt.Sprintf("nodes[%q]", id), "node is not reachable from START"))
		}
	}
	canReachEnd := walkReverse(End, reverse)
	for _, id := range g.nodeOrder {
		if !canReachEnd[id] {
			errs = append(errs, validationError("node.end.unreachable", fmt.Sprintf("nodes[%q]", id), "node cannot reach END"))
		}
	}
	return errs
}

func walkRuntimeForward(start string, outgoing map[string][]int, edges []runtimeEdge) map[string]bool {
	visited := map[string]bool{}
	queue := []string{start}
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		if visited[current] {
			continue
		}
		visited[current] = true
		for _, index := range outgoing[current] {
			queue = append(queue, edges[index].to)
		}
	}
	return visited
}

func (g *Graph[I, O]) runtimeSourceType(id string) (reflect.Type, bool) {
	if id == Start {
		return g.inputType, true
	}
	node, ok := g.nodes[id]
	return node.outputType, ok
}

// optionTypesProvider 是组件可选的类型声明能力：只声明接受的配置类型，
// 不执行任何设置函数。未声明的组件不参与按类型广播，只能接收定向选项。
type optionTypesProvider interface {
	OptionTypes() []reflect.Type
}

// declaredOptionTypes 读取组件声明的可接受配置类型；返回值做防御性复制。
func declaredOptionTypes(node any) []reflect.Type {
	provider, ok := node.(optionTypesProvider)
	if !ok || provider == nil {
		return nil
	}
	types := provider.OptionTypes()
	return append([]reflect.Type(nil), types...)
}

func bindRuntimeNode(name string, node any) (func(context.Context, any, []runnable.Option) (any, error), reflect.Type, reflect.Type, NodeInfo, graphComponent, error) {
	value := reflect.ValueOf(node)
	if !value.IsValid() || isNilReflectValue(value) {
		return nil, nil, nil, NodeInfo{}, nil, validationError("node.nil", fmt.Sprintf("nodes[%q]", name), "node cannot be nil")
	}
	method := value.MethodByName("Invoke")
	if !method.IsValid() {
		return nil, nil, nil, NodeInfo{}, nil, validationError("node.invoke.missing", fmt.Sprintf("nodes[%q]", name), "node must expose Invoke")
	}
	typeOfMethod := method.Type()
	if !typeOfMethod.IsVariadic() || typeOfMethod.NumIn() != 3 || typeOfMethod.In(0) != contextType || typeOfMethod.In(2) != optionSliceType || typeOfMethod.NumOut() != 2 || typeOfMethod.Out(1) != errorType {
		return nil, nil, nil, NodeInfo{}, nil, validationError("node.invoke.signature", fmt.Sprintf("nodes[%q]", name), "Invoke must have signature func(context.Context, Input, ...runnable.Option) (Output, error)")
	}
	inputType, outputType := typeOfMethod.In(1), typeOfMethod.Out(0)
	info := NodeInfo{ID: name, Kind: runnable.NodeKindRunnable, Label: name, InputType: typeName(inputType), OutputType: typeName(outputType)}
	if named, ok := node.(runnable.Named); ok {
		info.Label = named.Name()
		info.Description = named.Description()
	}
	if described, ok := node.(runnable.NodeDescriptor); ok && described.NodeKind() != "" {
		info.Kind = described.NodeKind()
	}
	invoker := func(ctx context.Context, input any, options []runnable.Option) (output any, err error) {
		defer func() {
			if recovered := recover(); recovered != nil {
				err = fmt.Errorf("node %q panicked: %v", name, recovered)
			}
		}()
		inputValue := reflect.Zero(inputType)
		if input != nil {
			actual := reflect.ValueOf(input)
			if !actual.Type().AssignableTo(inputType) {
				return nil, fmt.Errorf("node %q received runtime type %s, want %s", name, typeName(actual.Type()), typeName(inputType))
			}
			inputValue = actual
		}
		outputs := method.CallSlice([]reflect.Value{reflect.ValueOf(ctx), inputValue, reflect.ValueOf(options)})
		if !outputs[1].IsNil() {
			return nil, outputs[1].Interface().(error)
		}
		return outputs[0].Interface(), nil
	}
	component, _ := node.(graphComponent)
	return invoker, inputType, outputType, info, component, nil
}

// typedArgument 与反射路径的 reflectArgument 语义一致：nil 接口值转为 T 的
// 零值；不匹配的类型仍由断言 panic，经外层 recover 包装为 handler 错误。
func typedArgument[T any](value any) T {
	if value == nil {
		var zero T
		return zero
	}
	return value.(T)
}

// newRuntimePreHandler 由类型化签名生成直接调用闭包；与反射路径保持相同的
// panic 包装语义，调用边界只做类型断言。
func newRuntimePreHandler[S, I any](handler func(context.Context, *S) (I, error)) *runtimePreHandler {
	return &runtimePreHandler{stateType: typeOf[*S](), inputType: typeOf[I](), invoke: func(ctx context.Context, state any) (output any, err error) {
		defer func() {
			if recovered := recover(); recovered != nil {
				err = fmt.Errorf("pre handler panicked: %v", recovered)
			}
		}()
		return handler(ctx, typedArgument[*S](state))
	}}
}

func newRuntimePostHandler[S, O any](handler func(context.Context, *S, O) error) *runtimePostHandler {
	return &runtimePostHandler{stateType: typeOf[*S](), outputType: typeOf[O](), invoke: func(ctx context.Context, state, output any) (err error) {
		defer func() {
			if recovered := recover(); recovered != nil {
				err = fmt.Errorf("post handler panicked: %v", recovered)
			}
		}()
		return handler(ctx, typedArgument[*S](state), typedArgument[O](output))
	}}
}

func newRuntimeCondition[V any](predicate func(context.Context, V) (bool, error)) *runtimeCondition {
	return &runtimeCondition{inputType: typeOf[V](), invoke: func(ctx context.Context, input any) (matched bool, err error) {
		defer func() {
			if recovered := recover(); recovered != nil {
				err = fmt.Errorf("graph branch predicate panicked: %v", recovered)
			}
		}()
		return predicate(ctx, typedArgument[V](input))
	}}
}

func bindRuntimeCondition(predicate any) (*runtimeCondition, error) {
	value := reflect.ValueOf(predicate)
	if !value.IsValid() || isNilReflectValue(value) || value.Kind() != reflect.Func {
		return nil, errors.New("graph branch predicate must be a non-nil function")
	}
	typeOfPredicate := value.Type()
	if typeOfPredicate.NumIn() != 2 || typeOfPredicate.In(0) != contextType || typeOfPredicate.NumOut() != 2 || typeOfPredicate.Out(0) != boolType || typeOfPredicate.Out(1) != errorType {
		return nil, errors.New("graph branch predicate must have signature func(context.Context, Value) (bool, error)")
	}
	inputType := typeOfPredicate.In(1)
	return &runtimeCondition{inputType: inputType, invoke: func(ctx context.Context, input any) (matched bool, err error) {
		defer func() {
			if recovered := recover(); recovered != nil {
				err = fmt.Errorf("graph branch predicate panicked: %v", recovered)
			}
		}()
		outputs := value.Call([]reflect.Value{reflect.ValueOf(ctx), reflectArgument(inputType, input)})
		if !outputs[1].IsNil() {
			return false, outputs[1].Interface().(error)
		}
		return outputs[0].Bool(), nil
	}}, nil
}

func isNilReflectValue(value reflect.Value) bool {
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

func reflectArgument(expected reflect.Type, value any) reflect.Value {
	if value == nil {
		return reflect.Zero(expected)
	}
	return reflect.ValueOf(value)
}

var _ runnable.Runnable[any, any] = (*CompiledGraph[any, any])(nil)
