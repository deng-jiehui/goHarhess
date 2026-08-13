package graph

import (
	"context"
	"errors"
	"fmt"

	"goharness/pkg/runnable"
	statepkg "goharness/pkg/state"
)

func (g *Graph[I, O]) invokeRuntime(ctx context.Context, input I, options ...runnable.Option) (O, error) {
	var zero O
	if ctx == nil {
		return zero, errors.New("graph context cannot be nil")
	}
	plan, planned := runnable.OptionPlanFromContext(ctx)
	if !planned {
		var err error
		plan, err = runnable.NewOptionPlan(options...)
		if err != nil {
			return zero, fmt.Errorf("resolve graph options: %w", err)
		}
		ctx = runnable.WithOptionPlan(ctx, plan)
	} else {
		local := make([]runnable.Option, 0, len(options))
		for index, option := range options {
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
	var err error
	ctx, err = enterGraphExecution(ctx, g.Name())
	if err != nil {
		return zero, fmt.Errorf("enter graph callback scope: %w", err)
	}

	var runtimeState any
	if g.stateFactory != nil {
		if checkpoint, ok := statepkg.GraphStateCheckpointFromContext(ctx); ok && !statepkg.GraphStateCheckpointIgnored(ctx) {
			runtimeState, err = g.restoreRuntimeState(ctx, checkpoint)
		} else {
			runtimeState, err = g.stateFactory(ctx, input)
		}
		if err != nil {
			return zero, graphFailure(ctx, err)
		}
		ctx = withRuntimeState(ctx, runtimeState)
		ctx = statepkg.WithoutGraphStateCheckpoint(ctx)
		ctx = withRuntimeStateCheckpointProvider(ctx, func(snapshotCtx context.Context) (statepkg.GraphStateCheckpoint, error) {
			return g.snapshotRuntimeState(snapshotCtx, runtimeState)
		})
	}
	return g.executeRuntime(ctx, erasedValue{typ: g.inputType, value: any(input)}, runtimeState, plan)
}

func (g *Graph[I, O]) executeRuntime(ctx context.Context, current erasedValue, runtimeState any, plan runnable.OptionPlan) (O, error) {
	var zero O
	nodes, edges, maxSteps := g.runtimeExecutionPlan()
	if err := publishControl(ctx, Event{Type: EventGraphStart}); err != nil {
		return zero, err
	}
	currentNode := Start
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
		if node.pre == nil || runtimeState == nil {
			return zero, graphFailure(ctx, &ExecutionError{
				Code: "graph.resume.type_mismatch", NodeID: resumeNode, Step: startStep,
				Err: errors.New("resume requires a runtime State and node PreHandler"),
			})
		}
	}
	for step := startStep; ; step++ {
		if err := ctx.Err(); err != nil {
			return zero, graphFailure(ctx, err)
		}
		next := resumeNode
		resumeNode = ""
		if next == "" {
			var routeErr error
			next, routeErr = g.selectRuntimeNext(ctx, currentNode, current, runtimeState, edges)
			if routeErr != nil {
				return zero, graphFailure(ctx, &ExecutionError{Code: "route.select", NodeID: currentNode, Step: step, Err: routeErr})
			}
		}
		if next == End {
			if current.value == nil {
				if err := publishControl(ctx, Event{Type: EventGraphEnd, Step: step}); err != nil {
					return zero, err
				}
				return zero, nil
			}
			output, ok := current.value.(O)
			if !ok {
				return zero, graphFailure(ctx, &ExecutionError{Code: "graph.output.type", NodeID: currentNode, Step: step, Err: fmt.Errorf("runtime type %T, want %s", current.value, typeName(g.outputType))})
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
		nodeCtx := statepkg.WithCursorStack(ctx, append(cursor.Clone(), frame))
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
		if node.parallel != nil {
			if runtimeState == nil {
				return zero, graphFailure(nodeCtx, &ExecutionError{Code: "node.parallel", NodeID: next, Step: step, Err: errors.New("runtime State is unavailable")})
			}
			if parallelErr := g.invokeRuntimeParallel(nodeCtx, node.parallel, runtimeState, nodePlan); parallelErr != nil {
				return zero, graphFailure(nodeCtx, &ExecutionError{Code: "node.invoke", NodeID: next, Step: step, Err: parallelErr})
			}
			current = erasedValue{typ: g.stateType, value: runtimeState}
			finishNodeDataScope(nodeCtx, false)
			if err := publishControl(nodeCtx, Event{Type: EventNodeOutput, NodeID: next, Step: step}); err != nil {
				return zero, graphFailure(nodeCtx, err)
			}
			if err := publishControl(nodeCtx, Event{Type: EventNodeEnd, NodeID: next, Step: step}); err != nil {
				return zero, graphFailure(ctx, err)
			}
			currentNode = next
			continue
		}
		nodeInput := current.value
		if node.pre != nil {
			if runtimeState == nil {
				return zero, graphFailure(nodeCtx, &ExecutionError{Code: "node.pre", NodeID: next, Step: step, Err: errors.New("runtime State is unavailable")})
			}
			mapped, preErr := node.pre.invoke(nodeCtx, runtimeState)
			if preErr != nil {
				return zero, graphFailure(nodeCtx, &ExecutionError{Code: "node.pre", NodeID: next, Step: step, Err: fmt.Errorf("node %q pre handler: %w", next, preErr)})
			}
			nodeInput = mapped
		}
		invokeCtx := nodeCtx
		if node.nestedGraph {
			invokeCtx = WithDataSinkIsolated(invokeCtx, nil)
			invokeCtx = withoutRuntimeStateCheckpointProvider(invokeCtx)
			invokeCtx = statepkg.IgnoreGraphStateCheckpoint(invokeCtx)
		}
		output, invokeErr := node.invoke(invokeCtx, nodeInput, nodePlan.ForwardOptionsFor(runnable.OptionScopeNode))
		if invokeErr != nil {
			return zero, graphFailure(nodeCtx, &ExecutionError{Code: "node.invoke", NodeID: next, Step: step, Err: invokeErr})
		}
		if node.post != nil {
			if runtimeState == nil {
				return zero, graphFailure(nodeCtx, &ExecutionError{Code: "node.post", NodeID: next, Step: step, Err: errors.New("runtime State is unavailable")})
			}
			if postErr := node.post.invoke(nodeCtx, runtimeState, output); postErr != nil {
				return zero, graphFailure(nodeCtx, &ExecutionError{Code: "node.post", NodeID: next, Step: step, Err: fmt.Errorf("node %q post handler: %w", next, postErr)})
			}
		}
		current = erasedValue{typ: node.outputType, value: output}
		finishNodeDataScope(nodeCtx, false)
		if err := publishControl(nodeCtx, Event{Type: EventNodeOutput, NodeID: next, Step: step}); err != nil {
			return zero, graphFailure(nodeCtx, err)
		}
		if err := publishControl(nodeCtx, Event{Type: EventNodeEnd, NodeID: next, Step: step}); err != nil {
			return zero, graphFailure(ctx, err)
		}
		currentNode = next
	}
}

func (g *Graph[I, O]) runtimeExecutionPlan() (map[string]runtimeNode, []runtimeEdge, int) {
	g.mu.RLock()
	defer g.mu.RUnlock()
	nodes := make(map[string]runtimeNode, len(g.nodes))
	for id, node := range g.nodes {
		nodes[id] = node
	}
	return nodes, append([]runtimeEdge(nil), g.edges...), g.maxSteps
}

func (g *Graph[I, O]) selectRuntimeNext(ctx context.Context, from string, value erasedValue, runtimeState any, edges []runtimeEdge) (string, error) {
	fallback := ""
	for _, candidate := range edges {
		if candidate.from != from {
			continue
		}
		if candidate.condition == nil {
			fallback = candidate.to
			continue
		}
		input := value.value
		if g.stateType != nil && g.stateType.AssignableTo(candidate.condition.inputType) && !value.typ.AssignableTo(candidate.condition.inputType) {
			input = runtimeState
		}
		matched, err := candidate.condition.invoke(ctx, input)
		if err != nil {
			return "", fmt.Errorf("edge %q -> %q (%s): %w", candidate.from, candidate.to, candidate.label, err)
		}
		if matched {
			return candidate.to, nil
		}
	}
	if fallback != "" {
		return fallback, nil
	}
	return "", fmt.Errorf("no outgoing edge matched from %q", from)
}
