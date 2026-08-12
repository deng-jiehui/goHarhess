package graph

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"goharness/pkg/runnable"
	"goharness/pkg/schema"
	statepkg "goharness/pkg/state"
)

func (g *StateGraph[I, O, S]) Invoke(ctx context.Context, input I, opts ...runnable.Option) (O, error) {
	var zero O
	if ctx == nil {
		return zero, errors.New("graph context cannot be nil")
	}
	plan, planned := runnable.OptionPlanFromContext(ctx)
	if !planned {
		var err error
		plan, err = runnable.NewOptionPlan(opts...)
		if err != nil {
			return zero, fmt.Errorf("resolve graph options: %w", err)
		}
		ctx = runnable.WithOptionPlan(ctx, plan)
	} else {
		local := make([]runnable.Option, 0, len(opts))
		for index, option := range opts {
			if option == nil {
				return zero, fmt.Errorf("graph option %d is nil", index)
			}
			if !runnable.IsForwardedOption(option) {
				local = append(local, option)
			}
		}
		if len(local) > 0 {
			var err error
			plan, err = plan.Cascade(local...)
			if err != nil {
				return zero, fmt.Errorf("cascade graph options: %w", err)
			}
			ctx = runnable.WithOptionPlan(ctx, plan)
		}
	}
	if err := g.Compile(); err != nil {
		return zero, err
	}
	ctx, err := enterGraphExecution(ctx, g.Name())
	if err != nil {
		return zero, fmt.Errorf("enter graph callback scope: %w", err)
	}
	initial, err := g.generateState(ctx, input)
	if err != nil {
		return zero, err
	}
	return g.execute(ctx, initial, plan)
}

func (g *StateGraph[I, O, S]) generateState(ctx context.Context, input I) (value erasedValue, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("generate graph state: %v", recovered)
		}
	}()
	if checkpoint, ok := statepkg.GraphStateCheckpointFromContext(ctx); ok && !hasStateScope(ctx) && !statepkg.GraphStateCheckpointIgnored(ctx) {
		return g.decodeState(ctx, checkpoint)
	}
	state := g.genState(ctx, input)
	state = state.Clone()
	return erasedValue{typ: g.stateType, value: any(state)}, nil
}

func hasStateScope(ctx context.Context) bool {
	_, ok := stateScopeFromContext(ctx)
	return ok
}

func (g *StateGraph[I, O, S]) cloneNodeInput(value erasedValue) (cloned erasedValue, err error) {
	if value.typ != g.stateType {
		return value, nil
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("clone graph state %s: %v", typeName(g.stateType), recovered)
		}
	}()
	var state S
	if value.value != nil {
		state = value.value.(S)
	}
	state = state.Clone()
	return erasedValue{typ: g.stateType, value: any(state)}, nil
}

func (g *StateGraph[I, O, S]) execute(ctx context.Context, currentValue erasedValue, plan runnable.OptionPlan) (O, error) {
	var zero O
	scope, err := g.newStateScope(currentValue)
	if err != nil {
		return zero, err
	}
	ctx = withStateScope(ctx, scope)
	if !isNilStateCodec(g.stateCodec()) {
		checkpoint, encodeErr := g.encodeState(ctx, scope)
		if encodeErr != nil {
			return zero, graphFailure(ctx, encodeErr)
		}
		ctx = bindGraphStateCheckpoint(ctx, checkpoint)
	}
	nodes, edges, maxSteps := g.executionPlan()
	if err := publishControl(ctx, Event{Type: EventGraphStart}); err != nil {
		return zero, err
	}
	current := Start
	visits := map[string]int{}
	startStep := 0
	resumeNode := ""
	cursor := statepkg.CursorStackFromContext(ctx)
	if len(cursor) >= DefaultMaxDepth {
		return zero, graphFailure(ctx, &ExecutionError{Code: "graph.max_depth", NodeID: g.Name(), Step: len(cursor), Err: fmt.Errorf("maximum graph depth of %d exceeded", DefaultMaxDepth)})
	}
	if len(cursor) > 0 {
		top := cursor[len(cursor)-1]
		if top.Graph == g.Name() {
			resumeNode = top.Node
			startStep = top.Step
			visits[top.Node] = top.Iteration
			cursor = cursor[:len(cursor)-1].Clone()
		}
	}
	if resumeNode != "" {
		node, ok := nodes[resumeNode]
		if !ok {
			return zero, graphFailure(ctx, &ExecutionError{Code: "graph.resume.node_missing", NodeID: resumeNode, Step: startStep, Err: errors.New("resume node is not present in execution plan")})
		}
		if node.pre == nil && !g.stateType.AssignableTo(node.inputType) {
			return zero, graphFailure(ctx, &ExecutionError{
				Code: "graph.resume.type_mismatch", NodeID: resumeNode, Step: startStep,
				Err: fmt.Errorf("generated state %s is not assignable to resume input %s", typeName(g.stateType), typeName(node.inputType)),
			})
		}
	}
	for step := startStep; ; step++ {
		if err := ctx.Err(); err != nil {
			return zero, graphFailure(ctx, err)
		}
		next := resumeNode
		resumeNode = ""
		var routeErr error
		if next == "" {
			next, routeErr = selectNext(ctx, current, currentValue, edges)
		}
		if routeErr != nil {
			return zero, graphFailure(ctx, &ExecutionError{Code: "route.select", NodeID: current, Step: step, Err: routeErr})
		}
		if next == End {
			if currentValue.typ == g.stateType {
				var cloneErr error
				currentValue, cloneErr = g.cloneNodeInput(currentValue)
				if cloneErr != nil {
					return zero, graphFailure(ctx, &ExecutionError{Code: "graph.output.clone", NodeID: current, Step: step, Err: cloneErr})
				}
			}
			if currentValue.value == nil {
				if err := publishControl(ctx, Event{Type: EventGraphEnd, Step: step}); err != nil {
					return zero, err
				}
				return zero, nil
			}
			output, ok := currentValue.value.(O)
			if !ok {
				return zero, graphFailure(ctx, &ExecutionError{Code: "graph.output.type", NodeID: current, Step: step, Err: fmt.Errorf("runtime type %T, want %s", currentValue.value, typeName(g.outputType))})
			}
			if err := publishControl(ctx, Event{Type: EventGraphEnd, Step: step}); err != nil {
				return zero, err
			}
			return output, nil
		}
		if step >= maxSteps {
			return zero, graphFailure(ctx, &ExecutionError{Code: "graph.max_steps", NodeID: next, Step: step, Err: fmt.Errorf("maximum of %d node executions exceeded", maxSteps)})
		}
		node, ok := nodes[next]
		if !ok {
			return zero, graphFailure(ctx, &ExecutionError{Code: "node.missing", NodeID: next, Step: step, Err: errors.New("node is not present in execution plan")})
		}
		visits[next]++
		frame := statepkg.CursorFrame{Graph: g.Name(), Node: next, Step: step, Iteration: visits[next] - 1}
		nodeCursor := append(cursor.Clone(), frame)
		nodeCtx := statepkg.WithCursorStack(ctx, nodeCursor)
		nodeCtx = beginNodeDataScope(nodeCtx)
		nodeStart := Event{Type: EventNodeStart, NodeID: next, Step: step}
		nodeCtx = bindEventContext(nodeCtx, nodeStart)
		if err := publishControl(nodeCtx, nodeStart); err != nil {
			return zero, graphFailure(nodeCtx, err)
		}
		nodePlan, optionErr := plan.Cascade(node.options...)
		if optionErr != nil {
			return zero, graphFailure(nodeCtx, &ExecutionError{Code: "node.options", NodeID: next, Step: step, Err: optionErr})
		}
		nodeCtx = runnable.WithOptionPlan(nodeCtx, nodePlan)
		nodeInput, cloneErr := g.cloneNodeInput(currentValue)
		if cloneErr != nil {
			return zero, graphFailure(nodeCtx, &ExecutionError{Code: "node.state.clone", NodeID: next, Step: step, Err: cloneErr})
		}
		if node.nestedGraph {
			nodeCtx = WithDataSinkIsolated(nodeCtx, nil)
			nodeCtx = statepkg.IgnoreGraphStateCheckpoint(nodeCtx)
		}
		output, invokeErr := node.invoke(nodeCtx, nodeInput, nodePlan.ForwardOptionsFor(runnable.OptionScopeNode)...)
		if invokeErr != nil {
			return zero, graphFailure(nodeCtx, &ExecutionError{Code: "node.invoke", NodeID: next, Step: step, Err: invokeErr})
		}
		currentValue = output
		if !isNilStateCodec(g.stateCodec()) {
			checkpoint, encodeErr := g.encodeState(nodeCtx, scope)
			if encodeErr != nil {
				return zero, graphFailure(nodeCtx, &ExecutionError{Code: "node.state_checkpoint", NodeID: next, Step: step, Err: encodeErr})
			}
			ctx = bindGraphStateCheckpoint(ctx, checkpoint)
			nodeCtx = bindGraphStateCheckpoint(nodeCtx, checkpoint)
		}
		finishNodeDataScope(nodeCtx, false)
		if err := publishControl(nodeCtx, Event{Type: EventNodeOutput, NodeID: next, Step: step}); err != nil {
			return zero, graphFailure(nodeCtx, err)
		}
		if err := publishControl(nodeCtx, Event{Type: EventNodeEnd, NodeID: next, Step: step}); err != nil {
			return zero, graphFailure(ctx, err)
		}
		current = next
	}
}

func (g *StateGraph[I, O, S]) newStateScope(initial erasedValue) (*stateScope, error) {
	if initial.typ != g.stateType {
		return nil, fmt.Errorf("graph initial state type %s, want %s", typeName(initial.typ), typeName(g.stateType))
	}
	scope := &stateScope{typ: g.stateType}
	var current S
	if initial.value != nil {
		value, ok := initial.value.(S)
		if !ok {
			return nil, fmt.Errorf("graph initial state runtime type %T, want %s", initial.value, typeName(g.stateType))
		}
		current = value
	}
	scope.snapshot = func() (erasedValue, error) {
		scope.mu.Lock()
		defer scope.mu.Unlock()
		cloned, err := cloneStateValue(current)
		if err != nil {
			return erasedValue{}, err
		}
		return erasedValue{typ: g.stateType, value: any(cloned)}, nil
	}
	scope.replace = func(value erasedValue) error {
		if value.typ == nil || !value.typ.AssignableTo(g.stateType) {
			return fmt.Errorf("state replacement type %s, want %s", typeName(value.typ), typeName(g.stateType))
		}
		var next S
		if value.value != nil {
			converted, ok := value.value.(S)
			if !ok {
				return fmt.Errorf("state replacement runtime type %T, want %s", value.value, typeName(g.stateType))
			}
			next = converted
		}
		cloned, err := cloneStateValue(next)
		if err != nil {
			return err
		}
		scope.mu.Lock()
		current = cloned
		scope.mu.Unlock()
		return nil
	}
	return scope, nil
}

func cloneStateValue[S Cloner[S]](value S) (cloned S, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("state Clone panicked: %v", recovered)
		}
	}()
	return value.Clone(), nil
}

func (g *StateGraph[I, O, S]) executionPlan() (map[string]anyNode, []edge, int) {
	g.mu.RLock()
	defer g.mu.RUnlock()
	nodes := make(map[string]anyNode, len(g.nodes))
	for id, node := range g.nodes {
		nodes[id] = node
	}
	edges := append([]edge(nil), g.edges...)
	return nodes, edges, g.maxSteps
}

func selectNext(ctx context.Context, from string, value erasedValue, edges []edge) (string, error) {
	fallback := ""
	for _, candidate := range edges {
		if candidate.info.From != from {
			continue
		}
		if candidate.condition == nil {
			fallback = candidate.info.To
			continue
		}
		matched, err := candidate.condition.invoke(ctx, value)
		if err != nil {
			return "", fmt.Errorf("edge %q -> %q (%s): %w", candidate.info.From, candidate.info.To, candidate.info.Label, err)
		}
		if matched {
			return candidate.info.To, nil
		}
	}
	if fallback != "" {
		return fallback, nil
	}
	return "", fmt.Errorf("no outgoing edge matched from %q", from)
}

func publishControl(ctx context.Context, event Event) error {
	publish(ctx, event)
	return nil
}

func graphFailure(ctx context.Context, err error) error {
	publish(ctx, Event{Type: EventGraphError, Err: err})
	return err
}

// StreamMessages exposes the existing message data plane for a graph whose
// final output is []schema.Message. Other StateGraph instantiations remain
// ordinary typed Runnables.
func StreamMessages[I any, S Cloner[S]](
	ctx context.Context,
	g *StateGraph[I, []schema.Message, S],
	input I,
	opts ...runnable.Option,
) (runnable.Stream[schema.MessageEvent], error) {
	if ctx == nil {
		return nil, errors.New("graph context cannot be nil")
	}
	if g == nil {
		return nil, errors.New("graph is nil")
	}
	if err := g.Compile(); err != nil {
		return nil, err
	}
	out := make(chan runnable.Item[schema.MessageEvent], 16)
	go func() {
		defer close(out)
		tracker := &nodeDataTracker{}
		sink := DataSinkFunc(func(dataCtx context.Context, event schema.MessageEvent) bool {
			select {
			case <-dataCtx.Done():
				return false
			case out <- runnable.Item[schema.MessageEvent]{Value: event.Clone()}:
				recordNodeData(dataCtx, event)
				return true
			}
		})
		runCtx := withNodeDataTracker(WithDataSink(ctx, sink), tracker)
		output, err := g.Invoke(runCtx, input, opts...)
		if err != nil {
			select {
			case <-ctx.Done():
			case out <- runnable.Item[schema.MessageEvent]{Err: err}:
			}
			return
		}
		finalSnapshot, hasFinalSnapshot := tracker.finalSnapshot()
		if len(output) > 0 && (!hasFinalSnapshot || !messagesEqual(finalSnapshot, output)) {
			EmitData(runCtx, schema.MessageEvent{Kind: schema.MessageEventSnapshot, Messages: output})
		}
		EmitData(runCtx, schema.MessageEvent{Kind: schema.MessageEventDone})
	}()
	return out, nil
}

func messagesEqual(left, right []schema.Message) bool {
	leftJSON, leftErr := json.Marshal(left)
	rightJSON, rightErr := json.Marshal(right)
	return leftErr == nil && rightErr == nil && bytes.Equal(leftJSON, rightJSON)
}
