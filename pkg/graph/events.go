package graph

import (
	"context"
	"sync"
)

// EventSink receives Graph control-plane events. It is intentionally separate
// from Runnable/Streamer data output.
type EventSink interface {
	Publish(context.Context, Event)
}

// eventContextBinder lets framework-owned sinks install node-local runtime
// state before execution without expanding the public EventSink contract.
type eventContextBinder interface {
	BindEventContext(context.Context, Event) context.Context
}

// EventSinkForker creates an isolated sink for a nested graph execution.
// Implementations may use the supplied graph name to create a child span or
// callback scope. A nil sink means the nested execution is intentionally
// silent to its parent observer.
type EventSinkForker interface {
	Fork(context.Context, string) (context.Context, EventSink, error)
}

type EventSinkFunc func(context.Context, Event)

func (f EventSinkFunc) Publish(ctx context.Context, event Event) {
	if f != nil {
		f(ctx, event)
	}
}

type eventSinkContextKey struct{}
type graphExecutionContextKey struct{}

type graphExecutionScope struct {
	active bool
	name   string
}

func WithEventSink(ctx context.Context, sink EventSink) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, eventSinkContextKey{}, sink)
}

// WithEventSinkIsolated installs a control sink for one nested execution
// without forwarding child Graph lifecycle events to the parent span.
func WithEventSinkIsolated(ctx context.Context, sink EventSink) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	ctx = context.WithValue(ctx, eventSinkContextKey{}, sink)
	// A replacement sink owns a new lifecycle root. This also prevents an
	// Agent that installs its own callback manager from being mistaken for a
	// direct subgraph of the caller.
	return context.WithValue(ctx, graphExecutionContextKey{}, graphExecutionScope{})
}

func EventSinkFromContext(ctx context.Context) (EventSink, bool) {
	if ctx == nil {
		return nil, false
	}
	sink, ok := ctx.Value(eventSinkContextKey{}).(EventSink)
	return sink, ok && sink != nil
}

func enterGraphExecution(ctx context.Context, name string) (context.Context, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	parent, _ := ctx.Value(graphExecutionContextKey{}).(graphExecutionScope)
	if parent.active {
		var child EventSink
		if sink, ok := EventSinkFromContext(ctx); ok {
			if forker, ok := sink.(EventSinkForker); ok {
				var err error
				ctx, child, err = forker.Fork(ctx, name)
				if err != nil {
					return nil, err
				}
			}
		}
		// Raw sinks do not implicitly receive nested lifecycle events. A sink
		// that wants child observations must opt in through EventSinkForker.
		ctx = WithEventSinkIsolated(ctx, child)
	}
	return context.WithValue(ctx, graphExecutionContextKey{}, graphExecutionScope{active: true, name: name}), nil
}

func publish(ctx context.Context, event Event) {
	sink, ok := EventSinkFromContext(ctx)
	if !ok {
		return
	}
	// User observation cannot corrupt graph execution.
	defer func() { _ = recover() }()
	sink.Publish(ctx, event)
}

func bindEventContext(ctx context.Context, event Event) (bound context.Context) {
	if ctx == nil {
		ctx = context.Background()
	}
	bound = ctx
	sink, ok := EventSinkFromContext(ctx)
	if !ok {
		return bound
	}
	binder, ok := sink.(eventContextBinder)
	if !ok {
		return bound
	}
	defer func() {
		if recover() != nil || bound == nil {
			bound = ctx
		}
	}()
	bound = binder.BindEventContext(ctx, event)
	return bound
}

// MultiEventSink fans control events out without giving any observer ownership
// of the graph lifecycle.
type MultiEventSink struct {
	mu    sync.RWMutex
	sinks []EventSink
}

func NewEventSink(sinks ...EventSink) *MultiEventSink {
	filtered := make([]EventSink, 0, len(sinks))
	for _, sink := range sinks {
		if sink != nil {
			filtered = append(filtered, sink)
		}
	}
	return &MultiEventSink{sinks: filtered}
}

func (s *MultiEventSink) Publish(ctx context.Context, event Event) {
	if s == nil {
		return
	}
	s.mu.RLock()
	sinks := append([]EventSink(nil), s.sinks...)
	s.mu.RUnlock()
	for _, sink := range sinks {
		func() {
			defer func() { _ = recover() }()
			sink.Publish(ctx, event)
		}()
	}
}
