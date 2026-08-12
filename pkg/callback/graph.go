package callback

import (
	"context"
	"sync"

	"goharness/pkg/graph"
)

// BindGraph creates exactly one child Graph span beneath the current Agent
// span and installs a control-plane EventSink. Each graph node gets its own
// child operation so APM consumers can render node-level waterfall timing.
func BindGraph(ctx context.Context, name string) context.Context {
	dispatcher, ok := DispatcherFromContext(ctx)
	if !ok {
		return ctx
	}
	graphCtx, operation := Begin(ctx, ComponentGraph, name, nil)
	callbackSink := &graphCallbackSink{dispatcher: dispatcher, operation: operation, ctx: graphCtx, nodes: make(map[graphNodeKey]graphNodeState)}
	var userSink graph.EventSink
	if existing, ok := graph.EventSinkFromContext(ctx); ok {
		if scoped, ok := existing.(interface{ userEventSink() graph.EventSink }); ok {
			userSink = scoped.userEventSink()
		} else {
			userSink = existing
		}
	}
	executionCtx := bindOperation(ctx, operation)
	executionCtx = WithDispatcher(executionCtx, dispatcher)
	return graph.WithEventSinkIsolated(executionCtx, &scopedGraphSink{callback: callbackSink, user: userSink})
}

type scopedGraphSink struct {
	callback *graphCallbackSink
	user     graph.EventSink
}

// Fork derives a callback manager and lifecycle sink for one nested Graph.
// The parent sink is deliberately not part of the child sink, so a child
// GraphEnd can only terminate the child lifecycle. Events still reach parent
// callbacks through Dispatcher.parent as an independently traced child span.
func (s *scopedGraphSink) Fork(ctx context.Context, name string) (context.Context, graph.EventSink, error) {
	if s == nil || s.callback == nil || s.callback.dispatcher == nil {
		return ctx, nil, nil
	}
	dispatcher, err := NewChild(s.callback.dispatcher, nil)
	if err != nil {
		return ctx, nil, err
	}
	if ctx == nil {
		ctx = s.callback.ctx
	}
	graphCtx, operation := Begin(ctx, ComponentGraph, name, nil)
	childCtx := bindOperation(ctx, operation)
	childCtx = WithDispatcher(childCtx, dispatcher)
	return childCtx, &scopedGraphSink{
		callback: &graphCallbackSink{dispatcher: dispatcher, operation: operation, ctx: graphCtx, nodes: make(map[graphNodeKey]graphNodeState)},
	}, nil
}

func (s *scopedGraphSink) Publish(ctx context.Context, event graph.Event) {
	if s == nil {
		return
	}
	if s.callback != nil {
		s.callback.Publish(ctx, event)
	}
	if s.user != nil {
		s.user.Publish(ctx, event)
	}
}

func (s *scopedGraphSink) BindEventContext(ctx context.Context, event graph.Event) context.Context {
	if s == nil || s.callback == nil {
		return ctx
	}
	return s.callback.BindEventContext(ctx, event)
}

func (s *scopedGraphSink) userEventSink() graph.EventSink {
	if s == nil {
		return nil
	}
	return s.user
}

type graphCallbackSink struct {
	dispatcher *SyncDispatcher
	operation  Operation
	ctx        context.Context
	mu         sync.Mutex
	started    bool
	terminal   bool
	nodes      map[graphNodeKey]graphNodeState
	queue      []graphDispatchBatch
	draining   bool
}

type graphNodeKey struct {
	id   string
	step int
}

type graphNodeState struct {
	operation Operation
	started   bool
}

type graphDispatchItem struct {
	ctx   context.Context
	event Event
}

type graphDispatchBatch struct {
	items []graphDispatchItem
	done  chan struct{}
}

type graphDispatchContextKey struct{}

func (s *graphCallbackSink) BindEventContext(ctx context.Context, event graph.Event) context.Context {
	if s == nil || event.Type != graph.EventNodeStart {
		return ctx
	}
	if ctx == nil {
		ctx = s.ctx
	}
	key := graphNodeKey{id: event.NodeID, step: event.Step}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.terminal {
		return ctx
	}
	if state, exists := s.nodes[key]; exists {
		return bindOperation(ctx, state.operation)
	}
	nodeCtx, operation := Begin(ctx, ComponentGraph, event.NodeID, graphNodeMetadata(event, GraphNodePhaseStart))
	s.nodes[key] = graphNodeState{operation: operation}
	return nodeCtx
}

func (s *graphCallbackSink) Publish(ctx context.Context, event graph.Event) {
	if s == nil || s.dispatcher == nil {
		return
	}
	if ctx == nil {
		ctx = s.ctx
	}
	metrics := Metrics{}
	var callbackEvent Event
	var nodeOperation Operation
	dispatchCtx := ctx
	var nodeErrorEvents []Event
	var nodeErrorContexts []context.Context
	var graphErrorEvent Event
	var dispatch bool
	var drain bool
	var done chan struct{}
	s.mu.Lock()
	if s.terminal {
		s.mu.Unlock()
		return
	}
	switch event.Type {
	case graph.EventGraphStart:
		if !s.started {
			s.started = true
			callbackEvent = StartEvent(EventGraphStart, s.operation, metrics)
			dispatchCtx = bindOperation(ctx, s.operation)
			dispatch = true
		}
	case graph.EventNodeStart:
		key := graphNodeKey{id: event.NodeID, step: event.Step}
		state, exists := s.nodes[key]
		if exists && state.started {
			s.mu.Unlock()
			return
		}
		if exists {
			nodeOperation = state.operation
		} else {
			_, nodeOperation = Begin(ctx, ComponentGraph, event.NodeID, graphNodeMetadata(event, GraphNodePhaseStart))
		}
		s.nodes[key] = graphNodeState{operation: nodeOperation, started: true}
		callbackEvent = StartEvent(EventGraphNodeStart, nodeOperation, metrics)
		dispatchCtx = bindOperation(ctx, nodeOperation)
		dispatch = true
	case graph.EventNodeOutput, graph.EventNodeEnd:
		key := graphNodeKey{id: event.NodeID, step: event.Step}
		state, exists := s.nodes[key]
		if !exists {
			s.mu.Unlock()
			return
		}
		nodeOperation = state.operation
		nodeOperation.Metadata = CloneMetadata(nodeOperation.Metadata)
		phase := GraphNodePhaseOutput
		if event.Type == graph.EventNodeEnd {
			phase = GraphNodePhaseEnd
			delete(s.nodes, key)
		}
		nodeOperation.Metadata[MetadataGraphNodePhase] = phase
		if event.Type == graph.EventNodeEnd {
			callbackEvent = EndEvent(EventGraphNodeEnd, nodeOperation, metrics)
		} else {
			callbackEvent = ProgressEvent(EventGraphNodeOutput, nodeOperation, metrics)
		}
		dispatchCtx = bindOperation(ctx, nodeOperation)
		dispatch = true
	case graph.EventGraphEnd:
		s.terminal = true
		callbackEvent = EndEvent(EventGraphEnd, s.operation, metrics)
		dispatchCtx = bindOperation(ctx, s.operation)
		dispatch = true
	case graph.EventGraphError:
		s.terminal = true
		for key, state := range s.nodes {
			operation := state.operation
			operation.Metadata = CloneMetadata(operation.Metadata)
			operation.Metadata[MetadataGraphNodePhase] = GraphNodePhaseEnd
			nodeErrorEvents = append(nodeErrorEvents, ErrorEvent(EventGraphNodeEnd, operation, event.Err, metrics))
			nodeErrorContexts = append(nodeErrorContexts, bindOperation(ctx, operation))
			delete(s.nodes, key)
		}
		graphErrorEvent = ErrorEvent(EventGraphError, s.operation, event.Err, metrics)
		callbackEvent = graphErrorEvent
		dispatchCtx = bindOperation(ctx, s.operation)
		dispatch = true
	}
	if dispatch {
		items := make([]graphDispatchItem, 0, len(nodeErrorEvents)+1)
		for index, nodeEvent := range nodeErrorEvents {
			items = append(items, graphDispatchItem{ctx: nodeErrorContexts[index], event: nodeEvent})
		}
		items = append(items, graphDispatchItem{ctx: dispatchCtx, event: callbackEvent})
		done = make(chan struct{})
		s.queue = append(s.queue, graphDispatchBatch{items: items, done: done})
		if !s.draining {
			s.draining = true
			drain = true
		}
	}
	s.mu.Unlock()
	if !dispatch {
		return
	}
	if drain {
		s.drainQueue()
		return
	}
	if owner, _ := ctx.Value(graphDispatchContextKey{}).(*graphCallbackSink); owner != s {
		<-done
	}
}

func (s *graphCallbackSink) drainQueue() {
	for {
		s.mu.Lock()
		if len(s.queue) == 0 {
			s.draining = false
			s.mu.Unlock()
			return
		}
		batch := s.queue[0]
		s.queue = s.queue[1:]
		s.mu.Unlock()
		for _, item := range batch.items {
			dispatchCtx := context.WithValue(item.ctx, graphDispatchContextKey{}, s)
			_ = s.dispatcher.Dispatch(dispatchCtx, item.event)
		}
		close(batch.done)
	}
}

func graphNodeMetadata(event graph.Event, phase GraphNodePhase) map[string]any {
	return map[string]any{
		MetadataGraphNodeID:    event.NodeID,
		MetadataGraphStep:      event.Step,
		MetadataGraphNodePhase: phase,
	}
}
