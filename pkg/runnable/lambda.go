package runnable

import (
	"context"
	"errors"
	"fmt"
)

// AnyRunnable is the type-erased invocation boundary used by registries and
// dynamic graph builders. Typed execution should continue to use Runnable.
type AnyRunnable interface {
	InvokeAny(context.Context, any, ...Option) (any, error)
}

// InvokeLambda adapts a typed Go function to Runnable and AnyRunnable.
type InvokeLambda[I, O any] struct {
	invoke func(context.Context, I, ...Option) (O, error)
}

// InvokableLambda wraps an ordinary function that does not consume options.
func InvokableLambda[I, O any](fn func(context.Context, I) (O, error)) *InvokeLambda[I, O] {
	if fn == nil {
		return &InvokeLambda[I, O]{}
	}
	return &InvokeLambda[I, O]{invoke: func(ctx context.Context, input I, _ ...Option) (O, error) {
		return fn(ctx, input)
	}}
}

// InvokableLambdaWithOptions wraps a function that consumes invocation options.
func InvokableLambdaWithOptions[I, O any](fn func(context.Context, I, ...Option) (O, error)) *InvokeLambda[I, O] {
	return &InvokeLambda[I, O]{invoke: fn}
}

func (l *InvokeLambda[I, O]) Invoke(ctx context.Context, input I, options ...Option) (O, error) {
	var zero O
	if l == nil || l.invoke == nil {
		return zero, errors.New("invokable lambda is nil")
	}
	return l.invoke(ctx, input, options...)
}

func (l *InvokeLambda[I, O]) InvokeAny(ctx context.Context, input any, options ...Option) (any, error) {
	typed, ok := input.(I)
	if !ok {
		var expected I
		return nil, fmt.Errorf("invokable lambda input type %T does not match %T", input, expected)
	}
	return l.Invoke(ctx, typed, options...)
}

// StreamLambda adapts a typed stream function. Invoke provides the standard
// full-value barrier by collecting the last streamed value.
type StreamLambda[I, O any] struct {
	stream  func(context.Context, I, ...Option) (Stream[O], error)
	reducer Reducer[O]
}

// StreamableLambda wraps an ordinary stream-producing function.
func StreamableLambda[I, O any](fn func(context.Context, I) (Stream[O], error)) *StreamLambda[I, O] {
	if fn == nil {
		return &StreamLambda[I, O]{}
	}
	return &StreamLambda[I, O]{stream: func(ctx context.Context, input I, _ ...Option) (Stream[O], error) {
		return fn(ctx, input)
	}}
}

// StreamableLambdaWithOptions wraps a stream function that consumes options.
func StreamableLambdaWithOptions[I, O any](fn func(context.Context, I, ...Option) (Stream[O], error)) *StreamLambda[I, O] {
	return &StreamLambda[I, O]{stream: fn}
}

// StreamableLambdaWithReducer adapts an incremental stream into a full-value
// Runnable by applying reducer to every item in order.
func StreamableLambdaWithReducer[I, O any](fn func(context.Context, I) (Stream[O], error), reducer Reducer[O]) *StreamLambda[I, O] {
	if fn == nil {
		return &StreamLambda[I, O]{reducer: reducer}
	}
	return &StreamLambda[I, O]{
		stream: func(ctx context.Context, input I, _ ...Option) (Stream[O], error) {
			return fn(ctx, input)
		},
		reducer: reducer,
	}
}

func (l *StreamLambda[I, O]) Stream(ctx context.Context, input I, options ...Option) (Stream[O], error) {
	if l == nil || l.stream == nil {
		return nil, errors.New("streamable lambda is nil")
	}
	return l.stream(ctx, input, options...)
}

func (l *StreamLambda[I, O]) Invoke(ctx context.Context, input I, options ...Option) (O, error) {
	var zero O
	stream, err := l.Stream(ctx, input, options...)
	if err != nil {
		return zero, err
	}
	return CollectWith(ctx, stream, l.reducer)
}

func (l *StreamLambda[I, O]) InvokeAny(ctx context.Context, input any, options ...Option) (any, error) {
	typed, ok := input.(I)
	if !ok {
		var expected I
		return nil, fmt.Errorf("streamable lambda input type %T does not match %T", input, expected)
	}
	return l.Invoke(ctx, typed, options...)
}

type erasedRunnable[I, O any] struct {
	value Runnable[I, O]
}

// Erase exposes any typed Runnable through AnyRunnable.
func Erase[I, O any](value Runnable[I, O]) AnyRunnable {
	return erasedRunnable[I, O]{value: value}
}

func (e erasedRunnable[I, O]) InvokeAny(ctx context.Context, input any, options ...Option) (any, error) {
	if e.value == nil {
		return nil, errors.New("type-erased runnable is nil")
	}
	typed, ok := input.(I)
	if !ok {
		var expected I
		return nil, fmt.Errorf("runnable input type %T does not match %T", input, expected)
	}
	return e.value.Invoke(ctx, typed, options...)
}

var (
	_ Runnable[any, any] = (*InvokeLambda[any, any])(nil)
	_ AnyRunnable        = (*InvokeLambda[any, any])(nil)
	_ Runnable[any, any] = (*StreamLambda[any, any])(nil)
	_ Streamer[any, any] = (*StreamLambda[any, any])(nil)
	_ AnyRunnable        = (*StreamLambda[any, any])(nil)
)
