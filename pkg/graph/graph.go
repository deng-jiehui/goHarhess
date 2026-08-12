package graph

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"sync"

	"goharness/pkg/runnable"
)

type edge struct {
	info      EdgeInfo
	condition *anyCondition
}

// StateGraph exposes a fully typed API while storing heterogeneous nodes as
// private erased wrappers.
type StateGraph[I, O any, S Cloner[S]] struct {
	mu          sync.RWMutex
	name        string
	description string
	genState    StateGenerator[I, S]
	inputType   reflect.Type
	outputType  reflect.Type
	stateType   reflect.Type
	codec       StateCodec[S]
	nodes       map[string]anyNode
	nodeOrder   []string
	edges       []edge
	maxSteps    int
	built       bool
}

func NewStateGraph[I, O any, S Cloner[S]](name string, genState StateGenerator[I, S]) *StateGraph[I, O, S] {
	return &StateGraph[I, O, S]{
		name:       name,
		genState:   genState,
		inputType:  typeOf[I](),
		outputType: typeOf[O](),
		stateType:  typeOf[S](),
		nodes:      map[string]anyNode{},
		maxSteps:   DefaultMaxSteps,
	}
}

func (g *StateGraph[I, O, S]) graphComponent() {}

func (g *StateGraph[I, O, S]) Name() string {
	if g == nil {
		return ""
	}
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.name
}

func (g *StateGraph[I, O, S]) Description() string {
	if g == nil {
		return ""
	}
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.description
}

func (g *StateGraph[I, O, S]) SetDescription(description string) *StateGraph[I, O, S] {
	g.mu.Lock()
	g.description = description
	g.mu.Unlock()
	return g
}

func (g *StateGraph[I, O, S]) SetMaxSteps(maxSteps int) error {
	if maxSteps <= 0 {
		return validationError("graph.max_steps.invalid", "max_steps", "must be greater than zero, got %d", maxSteps)
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.built {
		return validationError("graph.built", "max_steps", "built graph cannot be modified")
	}
	g.maxSteps = maxSteps
	return nil
}

func (g *StateGraph[I, O, S]) SetNodeOptions(id string, options ...runnable.Option) error {
	for index, option := range options {
		if option == nil {
			return validationError("node.options.nil", fmt.Sprintf("nodes[%q].options[%d]", id, index), "option cannot be nil")
		}
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.built {
		return validationError("graph.built", "nodes", "built graph cannot be modified")
	}
	node, ok := g.nodes[id]
	if !ok {
		return validationError("node.missing", fmt.Sprintf("nodes[%q]", id), "node is not registered")
	}
	node.options = nodeScopedOptions(options)
	g.nodes[id] = node
	return nil
}

func nodeScopedOptions(options []runnable.Option) []runnable.Option {
	scoped := make([]runnable.Option, len(options))
	for index, option := range options {
		if runnable.ScopeOf(option) == runnable.OptionScopeNode {
			scoped[index] = option
		} else {
			scoped[index] = runnable.NodeOption(option)
		}
	}
	return scoped
}

func (g *StateGraph[I, O, S]) AddEdge(from, to string) error {
	return g.addEdge(from, to, "", nil)
}

// AddConditionalEdge binds a typed predicate to the value emitted by from.
func AddConditionalEdge[I, O any, S Cloner[S], T any](
	g *StateGraph[I, O, S],
	from, to, label string,
	condition func(context.Context, T) (bool, error),
) error {
	if g == nil {
		return validationError("graph.nil", "graph", "graph cannot be nil")
	}
	if condition == nil {
		return validationError("edge.condition.nil", "edges", "conditional edge requires a condition")
	}
	erased := &anyCondition{
		inputType: typeOf[T](),
		invoke: func(ctx context.Context, input erasedValue) (bool, error) {
			if input.typ == nil || !input.typ.AssignableTo(typeOf[T]()) {
				return false, fmt.Errorf("condition received erased type %s, want %s", typeName(input.typ), typeName(typeOf[T]()))
			}
			var typed T
			if input.value != nil {
				value, ok := input.value.(T)
				if !ok {
					return false, fmt.Errorf("condition received runtime type %T, want %s", input.value, typeName(typeOf[T]()))
				}
				typed = value
			}
			return condition(ctx, typed)
		},
	}
	return g.addEdge(from, to, label, erased)
}

func (g *StateGraph[I, O, S]) addEdge(from, to, label string, condition *anyCondition) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.built {
		return validationError("graph.built", "edges", "built graph cannot be modified")
	}
	from = strings.TrimSpace(from)
	to = strings.TrimSpace(to)
	if from == "" {
		return validationError("edge.from.empty", edgePath(len(g.edges))+".from", "source node cannot be empty")
	}
	if to == "" {
		return validationError("edge.to.empty", edgePath(len(g.edges))+".to", "target node cannot be empty")
	}
	g.edges = append(g.edges, edge{
		info:      EdgeInfo{From: from, To: to, Label: strings.TrimSpace(label), Conditional: condition != nil},
		condition: condition,
	})
	return nil
}

func (g *StateGraph[I, O, S]) Snapshot() Snapshot {
	g.mu.RLock()
	defer g.mu.RUnlock()
	nodes := make([]NodeInfo, 0, len(g.nodeOrder))
	for _, id := range g.nodeOrder {
		nodes = append(nodes, g.nodes[id].info)
	}
	edges := make([]EdgeInfo, len(g.edges))
	for index := range g.edges {
		edges[index] = g.edges[index].info
	}
	return Snapshot{
		Name: g.name, Description: g.description, Built: g.built, MaxSteps: g.maxSteps,
		InputType: typeName(g.inputType), OutputType: typeName(g.outputType), StateType: typeName(g.stateType),
		Nodes: nodes, Edges: edges,
	}
}

func (g *StateGraph[I, O, S]) Validate() error {
	if g == nil {
		return validationError("graph.nil", "graph", "graph cannot be nil")
	}
	g.mu.RLock()
	defer g.mu.RUnlock()
	if errs := g.validate(); len(errs) > 0 {
		return errs
	}
	return nil
}

func (g *StateGraph[I, O, S]) Compile() error {
	return g.compileGraph(map[uintptr]bool{})
}

func (g *StateGraph[I, O, S]) compileGraph(stack map[uintptr]bool) error {
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
	if errs := g.validate(); len(errs) > 0 {
		return errs
	}
	g.built = true
	return nil
}

func (g *StateGraph[I, O, S]) Build() error { return g.Compile() }

func (g *StateGraph[I, O, S]) IsBuilt() bool {
	if g == nil {
		return false
	}
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.built
}

func (g *StateGraph[I, O, S]) validate() ValidationErrors {
	var errs ValidationErrors
	if strings.TrimSpace(g.name) == "" {
		errs = append(errs, validationError("graph.name.empty", "name", "graph name cannot be empty"))
	}
	if g.genState == nil {
		errs = append(errs, validationError("graph.state_generator.nil", "gen_state", "state generator cannot be nil"))
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
		from, to := candidate.info.From, candidate.info.To
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
		key := from + "\x00" + to + "\x00" + candidate.info.Label
		if first, exists := seenEdges[key]; exists {
			errs = append(errs, validationError("edge.duplicate", path, "duplicates edges[%d]", first))
		} else {
			seenEdges[key] = index
		}
		outgoing[from] = append(outgoing[from], index)
		reverse[to] = append(reverse[to], from)

		sourceType, sourceOK := g.sourceType(from)
		targetType, targetOK := g.targetType(to)
		targetUsesStatePre := false
		if to != End {
			targetUsesStatePre = g.nodes[to].pre != nil
		}
		if sourceOK && targetOK && !targetUsesStatePre && !sourceType.AssignableTo(targetType) {
			errs = append(errs, validationError(
				"edge.type_mismatch", path,
				"%s output %s is not assignable to %s input %s",
				from, typeName(sourceType), to, typeName(targetType),
			))
		}
		if candidate.condition != nil && sourceOK && !sourceType.AssignableTo(candidate.condition.inputType) {
			errs = append(errs, validationError(
				"edge.condition.type_mismatch", path+".condition",
				"%s output %s is not assignable to condition input %s",
				from, typeName(sourceType), typeName(candidate.condition.inputType),
			))
		}
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
	reachable := walkForward(Start, outgoing, g.edges)
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

func (g *StateGraph[I, O, S]) sourceType(id string) (reflect.Type, bool) {
	if id == Start {
		return g.stateType, true
	}
	node, ok := g.nodes[id]
	if ok && node.post != nil {
		return g.stateType, true
	}
	return node.outputType, ok
}

func (g *StateGraph[I, O, S]) targetType(id string) (reflect.Type, bool) {
	if id == End {
		return g.outputType, true
	}
	node, ok := g.nodes[id]
	return node.inputType, ok
}

func walkForward(start string, outgoing map[string][]int, edges []edge) map[string]bool {
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
			queue = append(queue, edges[index].info.To)
		}
	}
	return visited
}

func walkReverse(start string, reverse map[string][]string) map[string]bool {
	visited := map[string]bool{}
	queue := []string{start}
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		if visited[current] {
			continue
		}
		visited[current] = true
		queue = append(queue, reverse[current]...)
	}
	return visited
}

func edgePath(index int) string { return fmt.Sprintf("edges[%d]", index) }
