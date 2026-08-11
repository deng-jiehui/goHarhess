package callback

import (
	"context"
	"sync"

	"goharness/pkg/graph"
)

// BindGraph creates exactly one child Graph span beneath the current Agent
// span and installs a control-plane EventSink. Node events remain progress
// events of that Graph span; they do not create competing lifecycle owners.
func BindGraph(ctx context.Context, name string) context.Context {
	dispatcher, ok := DispatcherFromContext(ctx)
	if !ok {
		return ctx
	}
	graphCtx, operation := Begin(ctx, ComponentGraph, name, nil)
	callbackSink := &graphCallbackSink{dispatcher: dispatcher, operation: operation, ctx: graphCtx}
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
	parentCtx := ctx
	if s.callback.ctx != nil {
		parentCtx = s.callback.ctx
	}
	graphCtx, operation := Begin(parentCtx, ComponentGraph, name, nil)
	childCtx := bindOperation(ctx, operation)
	childCtx = WithDispatcher(childCtx, dispatcher)
	return childCtx, &scopedGraphSink{
		callback: &graphCallbackSink{dispatcher: dispatcher, operation: operation, ctx: graphCtx},
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
}

func (s *graphCallbackSink) Publish(ctx context.Context, event graph.Event) {
	if s == nil || s.dispatcher == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.terminal {
		return
	}
	metrics := Metrics{Custom: map[string]any{"step": event.Step, "node_id": event.NodeID}}
	if s.ctx != nil {
		ctx = s.ctx
	}
	switch event.Type {
	case graph.EventGraphStart:
		if !s.started {
			s.started = true
			s.dispatcher.Dispatch(ctx, StartEvent(EventGraphStart, s.operation, metrics))
		}
	case graph.EventNodeStart, graph.EventNodeOutput, graph.EventNodeEnd:
		callbackEvent := ProgressEvent(EventGraphNode, s.operation, metrics)
		callbackEvent.Metadata["graph.event_type"] = string(event.Type)
		s.dispatcher.Dispatch(ctx, callbackEvent)
	case graph.EventGraphEnd:
		s.terminal = true
		s.dispatcher.Dispatch(ctx, EndEvent(EventGraphEnd, s.operation, metrics))
	case graph.EventGraphError:
		s.terminal = true
		s.dispatcher.Dispatch(ctx, ErrorEvent(EventGraphError, s.operation, event.Err, metrics))
	}
}
