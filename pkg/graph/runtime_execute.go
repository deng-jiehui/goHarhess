package graph

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"goharness/pkg/control"
	"goharness/pkg/runnable"
	statepkg "goharness/pkg/state"
)

func (g *Graph[I, O]) invokeRuntime(ctx context.Context, input I, options ...runnable.Option) (O, error) {
	return g.invokeRuntimeWithContinuation(ctx, input, nil, options...)
}

func (g *Graph[I, O]) resumeRuntime(ctx context.Context, options ...runnable.Option) (O, error) {
	var zero O
	if ctx == nil {
		return zero, errors.New("graph context cannot be nil")
	}
	plan, resolvedCtx, err := resolveRuntimePlan(ctx, options)
	if err != nil {
		return zero, err
	}
	if err := g.Compile(); err != nil {
		return zero, err
	}
	config, configured, err := checkpointFromPlan(plan)
	if err != nil {
		return zero, fmt.Errorf("resolve graph checkpoint: %w", err)
	}
	if !configured {
		return zero, ErrCheckpointRequired
	}
	if _, resumableErr := g.recoveryMetadata(); resumableErr != nil {
		return zero, resumableErr
	}
	data, found, err := config.Store.Load(resolvedCtx, config.Key)
	if err != nil {
		return zero, fmt.Errorf("load graph checkpoint %q: %w", config.Key, err)
	}
	if !found {
		return zero, fmt.Errorf("%w: %q", ErrCheckpointNotFound, config.Key)
	}
	continuation, err := DecodeContinuation(data)
	if err != nil {
		return zero, err
	}
	revision, _ := g.recoveryMetadata()
	if continuation.GraphName != g.Name() || continuation.GraphRevision != revision {
		return zero, fmt.Errorf("%w: checkpoint %q/%q, graph %q/%q", ErrGraphRevisionMismatch, continuation.GraphName, continuation.GraphRevision, g.Name(), revision)
	}
	var input I
	return g.invokeRuntimeWithPlan(resolvedCtx, input, plan, config, &continuation)
}

func (g *Graph[I, O]) invokeRuntimeWithContinuation(ctx context.Context, input I, continuation *Continuation, options ...runnable.Option) (O, error) {
	var zero O
	if ctx == nil {
		return zero, errors.New("graph context cannot be nil")
	}
	plan, resolvedCtx, err := resolveRuntimePlan(ctx, options)
	if err != nil {
		return zero, err
	}
	if err := g.Compile(); err != nil {
		return zero, err
	}
	config, configured, err := checkpointFromPlan(plan)
	if err != nil {
		return zero, fmt.Errorf("resolve graph checkpoint: %w", err)
	}
	if configured {
		if _, resumableErr := g.recoveryMetadata(); resumableErr != nil {
			return zero, resumableErr
		}
	}
	return g.invokeRuntimeWithPlan(resolvedCtx, input, plan, config, continuation)
}

func resolveRuntimePlan(ctx context.Context, options []runnable.Option) (runnable.OptionPlan, context.Context, error) {
	plan, planned := runnable.OptionPlanFromContext(ctx)
	if !planned {
		var err error
		plan, err = runnable.NewOptionPlan(options...)
		if err != nil {
			return runnable.OptionPlan{}, ctx, fmt.Errorf("resolve graph options: %w", err)
		}
		ctx = runnable.WithOptionPlan(ctx, plan)
	} else {
		local := make([]runnable.Option, 0, len(options))
		for index, option := range options {
			if option == nil {
				return runnable.OptionPlan{}, ctx, fmt.Errorf("graph option %d is nil", index)
			}
			if !runnable.IsForwardedOption(option) {
				local = append(local, option)
			}
		}
		if len(local) > 0 {
			var err error
			plan, err = plan.Cascade(local...)
			if err != nil {
				return runnable.OptionPlan{}, ctx, fmt.Errorf("cascade graph options: %w", err)
			}
			ctx = runnable.WithOptionPlan(ctx, plan)
		}
	}
	return plan, ctx, nil
}

func (g *Graph[I, O]) invokeRuntimeWithPlan(ctx context.Context, input I, plan runnable.OptionPlan, checkpoint checkpointConfig, continuation *Continuation) (O, error) {
	var zero O
	var err error
	ctx, err = enterGraphExecution(ctx, g.Name())
	if err != nil {
		return zero, fmt.Errorf("enter graph callback scope: %w", err)
	}

	var runtimeState any
	if g.stateFactory != nil {
		if continuation != nil {
			runtimeState, err = g.restoreRuntimeState(ctx, continuation.State)
		} else {
			factoryCtx := ctx
			if _, interrupted := control.AsInterrupt(context.Cause(ctx)); interrupted {
				factoryCtx = context.WithoutCancel(ctx)
			}
			runtimeState, err = g.stateFactory(factoryCtx, input)
		}
		if err != nil {
			return zero, graphFailure(ctx, err)
		}
		ctx = withRuntimeState(ctx, runtimeState)
		ctx = withRuntimeStateCheckpointProvider(ctx, func(snapshotCtx context.Context) (statepkg.GraphStateCheckpoint, error) {
			return g.snapshotRuntimeState(snapshotCtx, runtimeState)
		})
	}
	return g.executeRuntime(ctx, erasedValue{typ: g.inputType, value: any(input)}, runtimeState, plan, checkpoint, continuation)
}

func (g *Graph[I, O]) executeRuntime(ctx context.Context, current erasedValue, runtimeState any, plan runnable.OptionPlan, checkpoint checkpointConfig, continuation *Continuation) (O, error) {
	var zero O
	nodes, edges, maxSteps := g.runtimeExecutionPlan()
	if err := publishControl(ctx, Event{Type: EventGraphStart}); err != nil {
		return zero, err
	}
	currentNode := Start
	visits := map[string]int{}
	startStep := 0
	pendingNode := ""
	cursor := statepkg.CursorStackFromContext(ctx)
	if len(cursor) >= DefaultMaxDepth {
		return zero, graphFailure(ctx, &ExecutionError{Code: "graph.max_depth", NodeID: g.Name(), Step: len(cursor), Err: fmt.Errorf("maximum graph depth of %d exceeded", DefaultMaxDepth)})
	}
	if continuation != nil {
		pendingNode = continuation.ResumeNode
		startStep = continuation.Step
		visits = cloneVisits(continuation.Visits)
		if visits == nil {
			visits = map[string]int{}
		}
		cursor = continuation.CursorStack.Clone()
	} else if len(cursor) > 0 {
		top := cursor[len(cursor)-1]
		if top.Graph == g.Name() {
			pendingNode = top.Node
			startStep = top.Step
			visits[top.Node] = top.Iteration
			cursor = cursor[:len(cursor)-1].Clone()
		}
	}
	if pendingNode != "" {
		node, ok := nodes[pendingNode]
		if !ok {
			return zero, graphFailure(ctx, &ExecutionError{Code: "graph.resume.node_missing", NodeID: pendingNode, Step: startStep, Err: errors.New("resume node is not present in execution plan")})
		}
		if (node.pre == nil && node.parallel == nil) || runtimeState == nil {
			return zero, graphFailure(ctx, &ExecutionError{
				Code: "graph.resume.type_mismatch", NodeID: pendingNode, Step: startStep,
				Err: errors.New("resume requires a runtime State and node PreHandler"),
			})
		}
	}
	for step := startStep; ; step++ {
		next := pendingNode
		pendingNode = ""
		if next == "" {
			var routeErr error
			next, routeErr = g.selectRuntimeNext(context.WithoutCancel(ctx), currentNode, current, runtimeState, edges)
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
		if request, interrupted := control.AsInterrupt(context.Cause(ctx)); interrupted {
			return zero, g.persistInterruption(ctx, checkpoint, request, InterruptPointBeforeNode, next, step, visits, cursor, runtimeState)
		}
		if err := ctx.Err(); err != nil {
			return zero, graphFailure(ctx, err)
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
				if request, interrupted := interruptFromExecution(nodeCtx, parallelErr); interrupted {
					return zero, g.persistInterruption(nodeCtx, checkpoint, request, InterruptPointDuringNode, next, step, visits, cursor, runtimeState)
				}
				return zero, graphFailure(nodeCtx, &ExecutionError{Code: "node.invoke", NodeID: next, Step: step, Err: parallelErr})
			}
			if request, interrupted := control.AsInterrupt(context.Cause(ctx)); interrupted {
				return zero, g.persistInterruption(nodeCtx, checkpoint, request, InterruptPointDuringNode, next, step, visits, cursor, runtimeState)
			}
			if err := ctx.Err(); err != nil {
				return zero, graphFailure(nodeCtx, err)
			}
			current = erasedValue{typ: g.stateType, value: runtimeState}
			finishNodeDataScope(nodeCtx, false)
			if err := publishControl(nodeCtx, Event{Type: EventNodeOutput, NodeID: next, Step: step}); err != nil {
				return zero, graphFailure(nodeCtx, err)
			}
			if err := publishControl(nodeCtx, Event{Type: EventNodeEnd, NodeID: next, Step: step}); err != nil {
				return zero, graphFailure(ctx, err)
			}
			commitCtx := context.WithoutCancel(nodeCtx)
			routed, routeErr := g.selectRuntimeNext(commitCtx, next, current, runtimeState, edges)
			if routeErr != nil {
				return zero, graphFailure(nodeCtx, &ExecutionError{Code: "route.select", NodeID: next, Step: step, Err: routeErr})
			}
			if routed != End {
				if request, interrupted := control.AsInterrupt(context.Cause(ctx)); interrupted {
					return zero, g.persistInterruption(nodeCtx, checkpoint, request, InterruptPointAfterCommit, routed, step+1, visits, cursor, runtimeState)
				}
				if err := ctx.Err(); err != nil {
					return zero, graphFailure(nodeCtx, err)
				}
			}
			currentNode = next
			pendingNode = routed
			continue
		}
		nodeInput := current.value
		if node.pre != nil {
			if runtimeState == nil {
				return zero, graphFailure(nodeCtx, &ExecutionError{Code: "node.pre", NodeID: next, Step: step, Err: errors.New("runtime State is unavailable")})
			}
			mapped, preErr := node.pre.invoke(nodeCtx, runtimeState)
			if preErr != nil {
				if request, interrupted := interruptFromExecution(nodeCtx, preErr); interrupted {
					return zero, g.persistInterruption(nodeCtx, checkpoint, request, InterruptPointBeforeNode, next, step, visits, cursor, runtimeState)
				}
				return zero, graphFailure(nodeCtx, &ExecutionError{Code: "node.pre", NodeID: next, Step: step, Err: fmt.Errorf("node %q pre handler: %w", next, preErr)})
			}
			nodeInput = mapped
		}
		invokeCtx := nodeCtx
		if node.nestedGraph {
			invokeCtx = WithDataSinkIsolated(invokeCtx, nil)
			invokeCtx = withoutRuntimeStateCheckpointProvider(invokeCtx)
		}
		output, invokeErr := node.invoke(invokeCtx, nodeInput, nodePlan.ForwardOptionsFor(runnable.OptionScopeNode))
		if invokeErr != nil {
			if request, interrupted := interruptFromExecution(nodeCtx, invokeErr); interrupted {
				return zero, g.persistInterruption(nodeCtx, checkpoint, request, InterruptPointDuringNode, next, step, visits, cursor, runtimeState)
			}
			return zero, graphFailure(nodeCtx, &ExecutionError{Code: "node.invoke", NodeID: next, Step: step, Err: invokeErr})
		}
		if request, interrupted := control.AsInterrupt(context.Cause(ctx)); interrupted {
			return zero, g.persistInterruption(nodeCtx, checkpoint, request, InterruptPointDuringNode, next, step, visits, cursor, runtimeState)
		}
		if err := ctx.Err(); err != nil {
			return zero, graphFailure(nodeCtx, err)
		}
		commitCtx := context.WithoutCancel(nodeCtx)
		if node.post != nil {
			if runtimeState == nil {
				return zero, graphFailure(nodeCtx, &ExecutionError{Code: "node.post", NodeID: next, Step: step, Err: errors.New("runtime State is unavailable")})
			}
			if postErr := node.post.invoke(commitCtx, runtimeState, output); postErr != nil {
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
		routed, routeErr := g.selectRuntimeNext(commitCtx, next, current, runtimeState, edges)
		if routeErr != nil {
			return zero, graphFailure(nodeCtx, &ExecutionError{Code: "route.select", NodeID: next, Step: step, Err: routeErr})
		}
		if routed != End {
			if request, interrupted := control.AsInterrupt(context.Cause(ctx)); interrupted {
				return zero, g.persistInterruption(nodeCtx, checkpoint, request, InterruptPointAfterCommit, routed, step+1, visits, cursor, runtimeState)
			}
			if err := ctx.Err(); err != nil {
				return zero, graphFailure(nodeCtx, err)
			}
		}
		currentNode = next
		pendingNode = routed
	}
}

func interruptFromExecution(ctx context.Context, err error) (control.InterruptRequest, bool) {
	if request, ok := control.AsInterrupt(context.Cause(ctx)); ok {
		return request, true
	}
	return control.AsInterrupt(err)
}

func (g *Graph[I, O]) persistInterruption(
	ctx context.Context,
	checkpoint checkpointConfig,
	request control.InterruptRequest,
	point InterruptPoint,
	resumeNode string,
	step int,
	visits map[string]int,
	cursor statepkg.CursorStack,
	runtimeState any,
) error {
	if isNilCheckpointStore(checkpoint.Store) || strings.TrimSpace(checkpoint.Key) == "" {
		return ErrCheckpointRequired
	}
	if _, resumableErr := g.recoveryMetadata(); resumableErr != nil {
		return resumableErr
	}
	saveCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), checkpoint.Timeout)
	defer cancel()
	stateCheckpoint, err := g.snapshotRuntimeState(saveCtx, runtimeState)
	if err != nil {
		return fmt.Errorf("snapshot Graph State: %w", err)
	}
	revision, _ := g.recoveryMetadata()
	continuation := Continuation{
		SchemaVersion: CurrentContinuationSchemaVersion,
		GraphName:     g.Name(),
		GraphRevision: revision,
		ResumeNode:    resumeNode,
		Step:          step,
		Visits:        cloneVisits(visits),
		State:         stateCheckpoint,
		CursorStack:   cursor.Clone(),
		Point:         point,
		Reason:        request.Reason,
		Metadata:      clonePortableMetadata(request.Metadata),
		CreatedAt:     time.Now().UTC(),
	}
	data, err := continuation.JSON()
	if err != nil {
		return err
	}
	if err := checkpoint.Store.Save(saveCtx, checkpoint.Key, data); err != nil {
		return fmt.Errorf("save graph checkpoint %q: %w", checkpoint.Key, err)
	}
	return &InterruptedError{Point: point, Reason: request.Reason, Continuation: continuation.Clone()}
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
