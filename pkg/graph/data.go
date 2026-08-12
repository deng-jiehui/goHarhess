package graph

import (
	"context"
	"sync"

	"goharness/pkg/schema"
)

// DataSink is the typed graph data plane. Model token/message events travel
// here; graph lifecycle events use EventSink instead.
type DataSink interface {
	Emit(context.Context, schema.MessageEvent) bool
}

type DataSinkFunc func(context.Context, schema.MessageEvent) bool

func (f DataSinkFunc) Emit(ctx context.Context, event schema.MessageEvent) bool {
	return f != nil && f(ctx, event)
}

type dataSinkContextKey struct{}
type nodeDataTrackerContextKey struct{}
type nodeDataScopeContextKey struct{}

type nodeDataScope struct {
	emitted     bool
	snapshot    []schema.Message
	hasSnapshot bool
}

type nodeDataTracker struct {
	mu      sync.Mutex
	current *nodeDataScope
	last    *nodeDataScope
}

func WithDataSink(ctx context.Context, sink DataSink) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if parent, ok := DataSinkFromContext(ctx); ok && sink != nil {
		sink = forwardingDataSink{current: sink, parent: parent}
	}
	return context.WithValue(ctx, dataSinkContextKey{}, sink)
}

// WithDataSinkIsolated replaces the current data sink for one nested
// invocation. It is used when the nested component returns its own stream and
// the enclosing node is responsible for forwarding those events exactly once.
func WithDataSinkIsolated(ctx context.Context, sink DataSink) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	ctx = context.WithValue(ctx, dataSinkContextKey{}, sink)
	ctx = context.WithValue(ctx, nodeDataTrackerContextKey{}, (*nodeDataTracker)(nil))
	return context.WithValue(ctx, nodeDataScopeContextKey{}, (*nodeDataScope)(nil))
}

func DataSinkFromContext(ctx context.Context) (DataSink, bool) {
	if ctx == nil {
		return nil, false
	}
	sink, ok := ctx.Value(dataSinkContextKey{}).(DataSink)
	return sink, ok && sink != nil
}

type forwardingDataSink struct {
	current DataSink
	parent  DataSink
}

func (s forwardingDataSink) Emit(ctx context.Context, event schema.MessageEvent) bool {
	if s.current == nil || !s.current.Emit(ctx, event) {
		return false
	}
	return s.parent == nil || s.parent.Emit(ctx, event)
}

func EmitData(ctx context.Context, event schema.MessageEvent) bool {
	if ctx == nil {
		return false
	}
	sink, ok := DataSinkFromContext(ctx)
	if !ok {
		return true
	}
	return sink.Emit(ctx, event)
}

func withNodeDataTracker(ctx context.Context, tracker *nodeDataTracker) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, nodeDataTrackerContextKey{}, tracker)
}

func beginNodeDataScope(ctx context.Context) context.Context {
	tracker, ok := nodeDataTrackerFromContext(ctx)
	if !ok {
		return ctx
	}
	scope := &nodeDataScope{}
	tracker.mu.Lock()
	tracker.current = scope
	tracker.mu.Unlock()
	return context.WithValue(ctx, nodeDataScopeContextKey{}, scope)
}

func finishNodeDataScope(ctx context.Context, hasOutput bool) {
	tracker, ok := nodeDataTrackerFromContext(ctx)
	if !ok {
		return
	}
	scope, ok := ctx.Value(nodeDataScopeContextKey{}).(*nodeDataScope)
	if !ok || scope == nil {
		return
	}
	tracker.mu.Lock()
	defer tracker.mu.Unlock()
	if tracker.current == scope && (hasOutput || scope.emitted) {
		tracker.last = scope
	}
}

func recordNodeData(ctx context.Context, event schema.MessageEvent) {
	tracker, ok := nodeDataTrackerFromContext(ctx)
	if !ok {
		return
	}
	scope, ok := ctx.Value(nodeDataScopeContextKey{}).(*nodeDataScope)
	if !ok || scope == nil {
		return
	}
	tracker.mu.Lock()
	defer tracker.mu.Unlock()
	if tracker.current != scope {
		return
	}
	switch event.Kind {
	case schema.MessageEventSnapshot:
		scope.emitted = true
		scope.snapshot = schema.CloneMessages(event.Messages)
		scope.hasSnapshot = true
	case schema.MessageEventDelta:
		scope.emitted = true
		scope.snapshot = nil
		scope.hasSnapshot = false
	}
}

func (t *nodeDataTracker) finalSnapshot() ([]schema.Message, bool) {
	if t == nil {
		return nil, false
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.last == nil {
		return nil, false
	}
	return schema.CloneMessages(t.last.snapshot), t.last.hasSnapshot
}

func nodeDataTrackerFromContext(ctx context.Context) (*nodeDataTracker, bool) {
	if ctx == nil {
		return nil, false
	}
	tracker, ok := ctx.Value(nodeDataTrackerContextKey{}).(*nodeDataTracker)
	return tracker, ok && tracker != nil
}
