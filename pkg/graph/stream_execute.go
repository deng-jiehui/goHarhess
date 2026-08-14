package graph

import (
	"context"
	"errors"
	"fmt"

	"goharness/pkg/control"
	"goharness/pkg/runnable"
)

type streamCarrier struct {
	value  erasedValue
	stream *erasedStream
}

type pendingStreamNode struct {
	ctx  context.Context
	id   string
	step int
}

func (g *Graph[I, O]) streamRuntime(ctx context.Context, input I, options ...runnable.Option) (*runnable.StreamReader[O], error) {
	if ctx == nil {
		return nil, errors.New("graph context cannot be nil")
	}
	plan, resolvedCtx, err := resolveRuntimePlan(ctx, options)
	if err != nil {
		return nil, err
	}
	if !g.built {
		return nil, errors.New("graph runtime is not compiled")
	}
	checkpoint, configured, err := checkpointFromPlan(plan)
	if err != nil {
		return nil, fmt.Errorf("resolve graph checkpoint: %w", err)
	}
	// Keep the output transport alive long enough to deliver a typed
	// interruption/checkpoint error after the caller cancels the run. The
	// execution context still observes the caller cancellation below.
	reader, writer := runnable.Pipe[O](context.WithoutCancel(resolvedCtx))
	go func() {
		executionCtx, cancel := context.WithCancelCause(resolvedCtx)
		defer cancel(nil)
		done := make(chan struct{})
		go func() {
			select {
			case <-reader.Context().Done():
				cancel(reader.Context().Err())
			case <-done:
			}
		}()
		defer close(done)
		var output O
		var runErr error
		if configured || g.hasParallelNode() {
			output, runErr = g.invokeRuntimeWithPlan(executionCtx, input, plan, checkpoint, nil)
		} else {
			output, runErr = g.executeStreamPlan(executionCtx, input, plan, checkpoint)
		}
		if runErr != nil {
			_ = writer.CloseWithError(runErr)
			return
		}
		if err := writer.Send(output); err != nil {
			return
		}
		_ = writer.Close()
	}()
	return reader, nil
}

func (g *Graph[I, O]) hasParallelNode() bool {
	for _, node := range g.nodes {
		if node.parallel != nil {
			return true
		}
	}
	return false
}

func (g *Graph[I, O]) executeStreamPlan(ctx context.Context, input I, plan runnable.OptionPlan, checkpoint checkpointConfig) (O, error) {
	var zero O
	ctx, err := enterGraphExecution(ctx, g.Name())
	if err != nil {
		return zero, fmt.Errorf("enter graph callback scope: %w", err)
	}
	revision, _ := g.recoveryMetadata()
	ctx = bindCallbacks(ctx, RunInfo{
		Name: g.Name(), Revision: revision, InputType: typeName(g.inputType), OutputType: typeName(g.outputType), ResumeMode: ResumeModeInvoke,
	}, g.Snapshot().Nodes)
	var runtimeState any
	if g.stateFactory != nil {
		runtimeState, err = g.stateFactory(ctx, input)
		if err != nil {
			return zero, graphFailure(ctx, err)
		}
		ctx = withRuntimeState(ctx, runtimeState)
		ctx = withRuntimeStateCheckpointProvider(ctx, func(snapshotCtx context.Context) (GraphStateCheckpoint, error) {
			return g.snapshotRuntimeState(snapshotCtx, runtimeState)
		})
	}
	if err := publishControl(ctx, Event{Type: EventGraphStart}); err != nil {
		return zero, err
	}

	nodes, edges, maxSteps := g.runtimeExecutionPlan()
	current := streamCarrier{value: erasedValue{typ: g.inputType, value: any(input)}}
	currentNode := Start
	pending := make([]pendingStreamNode, 0)
	visits := map[string]int{}
	cursor := CursorStackFromContext(ctx)
	for step := 0; ; step++ {
		if step > maxSteps {
			return zero, graphFailure(ctx, &ExecutionError{Code: "graph.max_steps", NodeID: currentNode, Step: step, Err: fmt.Errorf("maximum of %d node executions exceeded", maxSteps)})
		}
		if current.stream != nil && hasConditionalEdge(currentNode, edges) {
			if err := materializeStream(ctx, &current, &pending); err != nil {
				return zero, graphFailure(ctx, err)
			}
		}
		next, err := g.selectRuntimeNext(context.WithoutCancel(ctx), currentNode, current.value, runtimeState, edges)
		if err != nil {
			return zero, graphFailure(ctx, &ExecutionError{Code: "route.select", NodeID: currentNode, Step: step, Err: err})
		}
		if next == End {
			if err := materializeStream(ctx, &current, &pending); err != nil {
				return zero, graphFailure(ctx, err)
			}
			if current.value.value == nil {
				if err := publishControl(ctx, Event{Type: EventGraphEnd, Step: step}); err != nil {
					return zero, err
				}
				return zero, nil
			}
			output, ok := current.value.value.(O)
			if !ok {
				return zero, graphFailure(ctx, &ExecutionError{Code: "graph.output.type", NodeID: currentNode, Step: step, Err: fmt.Errorf("runtime type %T, want %s", current.value.value, typeName(g.outputType))})
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
			return zero, graphFailure(ctx, &InterruptedError{Point: InterruptPointBeforeNode, Reason: request.Reason})
		}
		if err := ctx.Err(); err != nil {
			return zero, graphFailure(ctx, err)
		}
		if current.stream != nil && (node.pre != nil || (node.transform == nil && node.collect == nil)) {
			if err := materializeStream(ctx, &current, &pending); err != nil {
				return zero, graphFailure(ctx, &ExecutionError{Code: "node.collect", NodeID: next, Step: step, Err: err})
			}
		}
		nodeCtx := ctx
		nodeCtx = bindEventContext(nodeCtx, Event{Type: EventNodeStart, NodeID: next, Step: step})
		if err := publishControl(nodeCtx, Event{Type: EventNodeStart, NodeID: next, Step: step}); err != nil {
			return zero, graphFailure(nodeCtx, err)
		}
		nodePlan, err := plan.Cascade(node.options...)
		if err != nil {
			return zero, graphFailure(nodeCtx, &ExecutionError{Code: "node.options", NodeID: next, Step: step, Err: err})
		}
		nodeCtx = runnable.WithOptionPlan(nodeCtx, nodePlan)
		forwarded := nodePlan.ForwardOptionsFor(runnable.OptionScopeNode)
		visits[next]++
		nodeInput := current.value.value
		if node.pre != nil {
			if runtimeState == nil {
				return zero, graphFailure(nodeCtx, &ExecutionError{Code: "node.pre", NodeID: next, Step: step, Err: errors.New("runtime State is unavailable")})
			}
			nodeInput, err = node.pre.invoke(nodeCtx, runtimeState)
			if err != nil {
				if request, interrupted := interruptFromExecution(nodeCtx, err); interrupted {
					return zero, g.persistInterruption(nodeCtx, checkpoint, request, InterruptPointBeforeNode, next, step, visits, cursor, runtimeState)
				}
				return zero, graphFailure(nodeCtx, &ExecutionError{Code: "node.pre", NodeID: next, Step: step, Err: err})
			}
		}

		var output streamCarrier
		switch {
		case current.stream != nil && node.transform != nil:
			stream, transformErr := node.transform(nodeCtx, *current.stream, forwarded)
			if transformErr != nil {
				if request, interrupted := interruptFromExecution(nodeCtx, transformErr); interrupted {
					return zero, g.persistInterruption(nodeCtx, checkpoint, request, InterruptPointDuringNode, next, step, visits, cursor, runtimeState)
				}
				return zero, graphFailure(nodeCtx, &ExecutionError{Code: "node.transform", NodeID: next, Step: step, Err: transformErr})
			}
			output = streamCarrier{stream: &stream}
			pending = append(pending, pendingStreamNode{ctx: nodeCtx, id: next, step: step})
		case current.stream != nil && node.collect != nil:
			value, collectErr := node.collect(nodeCtx, *current.stream, forwarded)
			if collectErr != nil {
				if request, interrupted := interruptFromExecution(nodeCtx, collectErr); interrupted {
					return zero, g.persistInterruption(nodeCtx, checkpoint, request, InterruptPointDuringNode, next, step, visits, cursor, runtimeState)
				}
				return zero, graphFailure(nodeCtx, &ExecutionError{Code: "node.collect", NodeID: next, Step: step, Err: collectErr})
			}
			finishPendingStreamNodes(&pending)
			output = streamCarrier{value: erasedValue{typ: node.outputType, value: value}}
			if node.post == nil {
				finishStreamNode(nodeCtx, next, step)
			}
		case node.stream != nil:
			stream, streamErr := node.stream(nodeCtx, nodeInput, forwarded)
			if streamErr != nil {
				if request, interrupted := interruptFromExecution(nodeCtx, streamErr); interrupted {
					return zero, g.persistInterruption(nodeCtx, checkpoint, request, InterruptPointDuringNode, next, step, visits, cursor, runtimeState)
				}
				return zero, graphFailure(nodeCtx, &ExecutionError{Code: "node.stream", NodeID: next, Step: step, Err: streamErr})
			}
			output = streamCarrier{stream: &stream}
			pending = append(pending, pendingStreamNode{ctx: nodeCtx, id: next, step: step})
		default:
			value, invokeErr := node.invoke(nodeCtx, nodeInput, forwarded)
			if invokeErr != nil {
				if request, interrupted := interruptFromExecution(nodeCtx, invokeErr); interrupted {
					return zero, g.persistInterruption(nodeCtx, checkpoint, request, InterruptPointDuringNode, next, step, visits, cursor, runtimeState)
				}
				return zero, graphFailure(nodeCtx, &ExecutionError{Code: "node.invoke", NodeID: next, Step: step, Err: invokeErr})
			}
			output = streamCarrier{value: erasedValue{typ: node.outputType, value: value}}
			if node.post == nil {
				finishStreamNode(nodeCtx, next, step)
			}
		}
		if node.post != nil {
			if err := collectStream(ctx, &output); err != nil {
				if request, interrupted := interruptFromExecution(nodeCtx, err); interrupted {
					return zero, g.persistInterruption(nodeCtx, checkpoint, request, InterruptPointDuringNode, next, step, visits, cursor, runtimeState)
				}
				return zero, graphFailure(nodeCtx, &ExecutionError{Code: "node.collect", NodeID: next, Step: step, Err: err})
			}
			finishPendingBeforeCurrent(&pending, next, step)
			if runtimeState == nil {
				return zero, graphFailure(nodeCtx, &ExecutionError{Code: "node.post", NodeID: next, Step: step, Err: errors.New("runtime State is unavailable")})
			}
			if err := node.post.invoke(context.WithoutCancel(nodeCtx), runtimeState, output.value.value); err != nil {
				if request, interrupted := interruptFromExecution(nodeCtx, err); interrupted {
					return zero, g.persistInterruption(nodeCtx, checkpoint, request, InterruptPointAfterCommit, next, step+1, visits, cursor, runtimeState)
				}
				return zero, graphFailure(nodeCtx, &ExecutionError{Code: "node.post", NodeID: next, Step: step, Err: err})
			}
			finishStreamNode(nodeCtx, next, step)
			removePendingCurrent(&pending, next, step)
		}
		current = output
		currentNode = next
	}
}

func materializeStream(ctx context.Context, carrier *streamCarrier, pending *[]pendingStreamNode) error {
	if err := collectStream(ctx, carrier); err != nil {
		return err
	}
	finishPendingStreamNodes(pending)
	return nil
}

func collectStream(ctx context.Context, carrier *streamCarrier) error {
	if carrier == nil || carrier.stream == nil {
		return nil
	}
	stream := carrier.stream
	value, err := stream.collect(ctx)
	if err != nil {
		return err
	}
	carrier.value = erasedValue{typ: stream.elemType, value: value}
	carrier.stream = nil
	return nil
}

func finishPendingStreamNodes(pending *[]pendingStreamNode) {
	for _, node := range *pending {
		finishStreamNode(node.ctx, node.id, node.step)
	}
	*pending = (*pending)[:0]
}

func finishPendingBeforeCurrent(pending *[]pendingStreamNode, id string, step int) {
	for len(*pending) > 0 {
		node := (*pending)[0]
		if node.id == id && node.step == step {
			return
		}
		finishStreamNode(node.ctx, node.id, node.step)
		*pending = (*pending)[1:]
	}
}

func removePendingCurrent(pending *[]pendingStreamNode, id string, step int) {
	if len(*pending) == 0 {
		return
	}
	node := (*pending)[0]
	if node.id == id && node.step == step {
		*pending = (*pending)[1:]
	}
}

func finishStreamNode(ctx context.Context, id string, step int) {
	_ = publishControl(ctx, Event{Type: EventNodeOutput, NodeID: id, Step: step})
	_ = publishControl(ctx, Event{Type: EventNodeEnd, NodeID: id, Step: step})
}

func hasConditionalEdge(from string, edges []runtimeEdge) bool {
	for _, edge := range edges {
		if edge.from == from && edge.condition != nil {
			return true
		}
	}
	return false
}
