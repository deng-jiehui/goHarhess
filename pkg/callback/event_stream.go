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
type streamSignalsContextKey struct{}
type streamSignal struct {
	typ reflect.Type
	fn  func()
}

// WithStreamSignal observes publication without requesting a payload snapshot.
// It is intended for framework progress guards such as model retries.
func WithStreamSignal[T any](ctx context.Context, fn func()) context.Context {
	existing, _ := ctx.Value(streamSignalsContextKey{}).([]streamSignal)
	values := append(append([]streamSignal(nil), existing...), streamSignal{reflect.TypeFor[T](), fn})
	return context.WithValue(ctx, streamSignalsContextKey{}, values)
}
func NotifyStream[T any](ctx context.Context) {
	if ctx == nil {
		return
	}
	values, _ := ctx.Value(streamSignalsContextKey{}).([]streamSignal)
	for _, v := range values {
		if v.typ == reflect.TypeFor[T]() && v.fn != nil {
			func() { defer func() { _ = recover() }(); v.fn() }()
		}
	}
}

// HasStreamCallbacks allows legacy snapshot producers to avoid full-message
// cloning when only the public incremental run stream is subscribed.
func HasStreamCallbacks[T any](ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	callbacks, _ := ctx.Value(streamCallbacksContextKey{}).([]StreamCallback)
	for _, c := range callbacks {
		if c.typ == reflect.TypeFor[T]() {
			return true
		}
	}
	return false
}

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
// Deprecated: publish UI messages and activities through pkg/stream. This
// function remains the compatibility API for existing typed observers.
func EmitStream[T any](ctx context.Context, value T) {
	if ctx == nil {
		return
	}
	NotifyStream[T](ctx)
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
