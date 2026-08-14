package state

import "context"

type checkpointContextKey struct{}

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
