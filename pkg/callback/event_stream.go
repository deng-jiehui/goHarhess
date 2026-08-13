package callback

import (
	"context"
	"reflect"
	"strings"
	"time"
)

// StreamCallback is an opaque, strongly typed real-time observation
// registration created by StreamCallbackOf.
type StreamCallback struct {
	name   string
	typ    reflect.Type
	invoke func(context.Context, any) error
}

type streamCallbacksContextKey struct{}

// WithStreamCallbacksIsolated starts a child observation scope without
// inheriting parent stream sinks. It is used for nested Graph/Agent runs that
// explicitly own forwarding of their live data.
func WithStreamCallbacksIsolated(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, streamCallbacksContextKey{}, []StreamCallback(nil))
}

// StreamCallbackOf creates a typed stream observer registration.
func StreamCallbackOf[T any](name string, fn func(context.Context, T) error) StreamCallback {
	return StreamCallback{
		name: strings.TrimSpace(name),
		typ:  reflect.TypeFor[T](),
		invoke: func(ctx context.Context, value any) error {
			if fn == nil {
				return nil
			}
			return fn(ctx, value.(T))
		},
	}
}

// WithStreamCallbacks appends real-time observers to the runtime Context.
func WithStreamCallbacks(ctx context.Context, callbacks ...StreamCallback) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	existing, _ := ctx.Value(streamCallbacksContextKey{}).([]StreamCallback)
	combined := append([]StreamCallback(nil), existing...)
	for _, streamCallback := range callbacks {
		if streamCallback.name == "" || streamCallback.invoke == nil {
			continue
		}
		combined = append(combined, streamCallback)
	}
	return context.WithValue(ctx, streamCallbacksContextKey{}, combined)
}

// EmitStream synchronously publishes one typed real-time value. Observer
// failures are isolated and reported to the Manager ErrorSink when present.
func EmitStream[T any](ctx context.Context, value T) {
	if ctx == nil {
		return
	}
	callbacks, _ := ctx.Value(streamCallbacksContextKey{}).([]StreamCallback)
	want := reflect.TypeFor[T]()
	manager, _ := ManagerFromContext(ctx)
	for _, streamCallback := range callbacks {
		if streamCallback.typ != want {
			continue
		}
		invocation, ok := InvocationFromContext(ctx)
		if !ok {
			invocation = Invocation{
				Component: "stream",
				Operation: streamCallback.name,
				Phase:     PhaseOutput,
				StartedAt: time.Now(),
				Timestamp: time.Now(),
			}
		} else {
			invocation = invocation.WithPhase(PhaseOutput)
		}
		if manager == nil {
			_, _ = invokeIsolated(streamCallback.name, withInvocation(ctx, invocation), func(current context.Context) error {
				return streamCallback.invoke(current, value)
			})
			continue
		}
		_ = manager.invoke(withInvocation(ctx, invocation), streamCallback.name, invocation, func(current context.Context) error {
			return streamCallback.invoke(current, value)
		})
	}
}
