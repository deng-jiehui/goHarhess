package state

import "context"

type checkpointContextKey struct{}
type cursorStackContextKey struct{}
type graphStateCheckpointContextKey struct{}
type graphStateCheckpointIgnoreKey struct{}

// WithCheckpoint makes an immutable checkpoint snapshot available to nested
// retrievers, callbacks, middleware and tools.
func WithCheckpoint(ctx context.Context, checkpoint Checkpoint) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, checkpointContextKey{}, checkpoint.Clone())
}

// FromContext returns an independent checkpoint snapshot for the current run.
func FromContext(ctx context.Context) (Checkpoint, bool) {
	if ctx == nil {
		return Checkpoint{}, false
	}
	checkpoint, ok := ctx.Value(checkpointContextKey{}).(Checkpoint)
	if !ok || checkpoint.ConversationID == "" {
		return Checkpoint{}, false
	}
	return checkpoint.Clone(), true
}

// ValuesFromContext returns durable conversation values without exposing the
// mutable map stored in the context.
func ValuesFromContext(ctx context.Context) map[string]any {
	checkpoint, ok := FromContext(ctx)
	if !ok {
		return map[string]any{}
	}
	return cloneMap(checkpoint.Values)
}

func WithCursorStack(ctx context.Context, stack CursorStack) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, cursorStackContextKey{}, stack.Clone())
}

func CursorStackFromContext(ctx context.Context) CursorStack {
	if ctx == nil {
		return nil
	}
	stack, _ := ctx.Value(cursorStackContextKey{}).(CursorStack)
	return stack.Clone()
}

// WithGraphStateCheckpoint makes an immutable typed graph State snapshot
// available to a graph invocation and to its callbacks.
func WithGraphStateCheckpoint(ctx context.Context, checkpoint GraphStateCheckpoint) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, graphStateCheckpointContextKey{}, checkpoint.Clone())
}

// GraphStateCheckpointFromContext returns an independent typed graph State
// snapshot for the current run.
func GraphStateCheckpointFromContext(ctx context.Context) (GraphStateCheckpoint, bool) {
	if ctx == nil {
		return GraphStateCheckpoint{}, false
	}
	checkpoint, ok := ctx.Value(graphStateCheckpointContextKey{}).(GraphStateCheckpoint)
	if !ok {
		return GraphStateCheckpoint{}, false
	}
	return checkpoint.Clone(), true
}

// WithoutGraphStateCheckpoint masks the current graph State snapshot for an
// internal graph that does not own that State codec. The parent Context remains
// available to the caller, while the derived Context cannot consume it.
func WithoutGraphStateCheckpoint(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, graphStateCheckpointContextKey{}, nil)
}

// IgnoreGraphStateCheckpoint marks an internal graph invocation as an
// observer of the parent run rather than the owner of its State checkpoint.
// The checkpoint remains readable by Agent callbacks and interruption logic.
func IgnoreGraphStateCheckpoint(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, graphStateCheckpointIgnoreKey{}, true)
}

func GraphStateCheckpointIgnored(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	ignored, _ := ctx.Value(graphStateCheckpointIgnoreKey{}).(bool)
	return ignored
}
