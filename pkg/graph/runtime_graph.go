package graph

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"

	"goharness/pkg/runnable"
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
	built        bool
	revision     string
	resumableErr error
}

type runtimeNode struct {
	id          string
	component   any
	inputType   reflect.Type
	outputType  reflect.Type
	info        NodeInfo
	options     []runnable.Option
	pre         *runtimePreHandler
	post        *runtimePostHandler
	invoke      func(context.Context, any, []runnable.Option) (any, error)
	stream      func(context.Context, any, []runnable.Option) (erasedStream, error)
	collect     func(context.Context, erasedStream, []runnable.Option) (any, error)
	transform   func(context.Context, erasedStream, []runnable.Option) (erasedStream, error)
	compile     func(map[uintptr]bool) error
	nestedGraph bool
	parallel    *runtimeParallelNode
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

// WithPreHandler maps runtime State to one pure node input.
func WithPreHandler(handler any) NodeOption {
	return runtimeNodeOption(func(config *runtimeNodeConfig) error {
		value, err := bindRuntimePreHandler(handler)
		if err != nil {
			return err
		}
		if config.pre != nil {
			return errors.New("graph node has multiple pre handlers")
		}
		config.pre = value
		return nil
	})
}

// WithPostHandler applies one pure node output to runtime State.
func WithPostHandler(handler any) NodeOption {
	return runtimeNodeOption(func(config *runtimeNodeConfig) error {
		value, err := bindRuntimePostHandler(handler)
		if err != nil {
			return err
		}
		if config.post != nil {
			return errors.New("graph node has multiple post handlers")
		}
		config.post = value
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
	entry := runtimeNode{
		id: name, component: node, inputType: inputType, outputType: outputType, info: info,
		options: nodeScopedOptions(config.options), pre: config.pre, post: config.post, invoke: invoker,
	}
	if component != nil {
		entry.compile = component.compileGraph
		entry.nestedGraph = true
		entry.info.Kind = "graph"
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
	g.edges = append(g.edges, runtimeEdge{from: from, to: to, label: strings.TrimSpace(label), condition: condition})
	return nil
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
	scoped := nodeScopedOptions(options)
	g.mu.Lock()
	defer g.mu.Unlock()
	node, ok := g.nodes[id]
	if !ok {
		return validationError("node.missing", fmt.Sprintf("nodes[%q]", id), "node is not registered")
	}
	node.options = scoped
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

// Compile validates and freezes one immutable execution snapshot. The Builder
// remains mutable and can be compiled again.
func (g *Graph[I, O]) Compile() (*CompiledGraph[I, O], error) {
	if g == nil {
		return nil, validationError("graph.nil", "graph", "graph cannot be nil")
	}
	runtime := g.cloneBuilder()
	if err := runtime.compileGraph(map[uintptr]bool{}); err != nil {
		return nil, err
	}
	return compileRuntimePlans(runtime)
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
		if node.parallel != nil {
			if g.stateType == nil || node.parallel.merger.stateType != g.stateType {
				return validationError("parallel.state.type_mismatch", fmt.Sprintf("nodes[%q]", name), "parallel merger State %s does not match graph State %s", typeName(node.parallel.merger.stateType), typeName(g.stateType))
			}
			for index, branch := range node.parallel.branches {
				if branch.pre == nil || branch.pre.stateType != g.stateType || branch.pre.inputType != branch.inputType {
					return validationError("parallel.branch.pre.type_mismatch", fmt.Sprintf("nodes[%q].branches[%d]", name, index), "branch PreHandler must map graph State %s to node input %s", typeName(g.stateType), typeName(branch.inputType))
				}
				if branch.post == nil {
					return validationError("parallel.branch.post.missing", fmt.Sprintf("nodes[%q].branches[%d]", name, index), "parallel branch requires a PostHandler")
				}
				if branch.post.stateType != g.stateType || branch.post.outputType != branch.outputType {
					return validationError("parallel.branch.post.type_mismatch", fmt.Sprintf("nodes[%q].branches[%d]", name, index), "branch PostHandler types do not match graph State and node output")
				}
			}
		}
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
		if exists && target.pre == nil && target.parallel == nil && !sourceType.AssignableTo(target.inputType) {
			return validationError("edge.type_mismatch", edgePath(index), "%s output %s is not assignable to %s input %s", edge.from, typeName(sourceType), edge.to, typeName(target.inputType))
		}
	}
	return nil
}

func (g *Graph[I, O]) validateRuntimeGraph() ValidationErrors {
	var errs ValidationErrors
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
	outgoing := map[string][]int{}
	reverse := map[string][]string{}
	seenEdges := map[string]int{}
	for index, candidate := range g.edges {
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
		errs = append(errs, validationError("graph.start.missing", "edges", "START requires an outgoing edge"))
	}
	for from, indexes := range outgoing {
		unconditional, conditional := 0, 0
		for _, index := range indexes {
			if g.edges[index].condition == nil {
				unconditional++
			} else {
				conditional++
			}
		}
		if unconditional > 1 {
			errs = append(errs, validationError("edge.fallback.multiple", fmt.Sprintf("outgoing[%q]", from), "only one unconditional edge is allowed, got %d", unconditional))
		}
		if conditional > 0 && unconditional != 1 {
			errs = append(errs, validationError("edge.fallback.required", fmt.Sprintf("outgoing[%q]", from), "conditional routing requires exactly one unconditional fallback"))
		}
	}
	for _, id := range g.nodeOrder {
		if len(outgoing[id]) == 0 {
			errs = append(errs, validationError("node.outgoing.missing", fmt.Sprintf("nodes[%q]", id), "node requires an outgoing edge"))
		}
	}
	reachable := walkRuntimeForward(Start, outgoing, g.edges)
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
	info := NodeInfo{ID: name, Kind: "runnable", Label: name, InputType: typeName(inputType), OutputType: typeName(outputType)}
	if named, ok := node.(runnable.Named); ok {
		info.Label = named.Name()
		info.Description = named.Description()
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

func bindRuntimePreHandler(handler any) (*runtimePreHandler, error) {
	value := reflect.ValueOf(handler)
	if !value.IsValid() || isNilReflectValue(value) || value.Kind() != reflect.Func {
		return nil, errors.New("pre handler must be a non-nil function")
	}
	typeOfHandler := value.Type()
	if typeOfHandler.NumIn() != 2 || typeOfHandler.In(0) != contextType || typeOfHandler.NumOut() != 2 || typeOfHandler.Out(1) != errorType {
		return nil, errors.New("pre handler must have signature func(context.Context, *State) (Input, error)")
	}
	stateType, inputType := typeOfHandler.In(1), typeOfHandler.Out(0)
	return &runtimePreHandler{stateType: stateType, inputType: inputType, invoke: func(ctx context.Context, state any) (output any, err error) {
		defer func() {
			if recovered := recover(); recovered != nil {
				err = fmt.Errorf("pre handler panicked: %v", recovered)
			}
		}()
		outputs := value.Call([]reflect.Value{reflect.ValueOf(ctx), reflectArgument(stateType, state)})
		if !outputs[1].IsNil() {
			return nil, outputs[1].Interface().(error)
		}
		return outputs[0].Interface(), nil
	}}, nil
}

func bindRuntimePostHandler(handler any) (*runtimePostHandler, error) {
	value := reflect.ValueOf(handler)
	if !value.IsValid() || isNilReflectValue(value) || value.Kind() != reflect.Func {
		return nil, errors.New("post handler must be a non-nil function")
	}
	typeOfHandler := value.Type()
	if typeOfHandler.NumIn() != 3 || typeOfHandler.In(0) != contextType || typeOfHandler.NumOut() != 1 || typeOfHandler.Out(0) != errorType {
		return nil, errors.New("post handler must have signature func(context.Context, *State, Output) error")
	}
	stateType, outputType := typeOfHandler.In(1), typeOfHandler.In(2)
	return &runtimePostHandler{stateType: stateType, outputType: outputType, invoke: func(ctx context.Context, state, output any) (err error) {
		defer func() {
			if recovered := recover(); recovered != nil {
				err = fmt.Errorf("post handler panicked: %v", recovered)
			}
		}()
		outputs := value.Call([]reflect.Value{reflect.ValueOf(ctx), reflectArgument(stateType, state), reflectArgument(outputType, output)})
		if !outputs[0].IsNil() {
			return outputs[0].Interface().(error)
		}
		return nil
	}}, nil
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
