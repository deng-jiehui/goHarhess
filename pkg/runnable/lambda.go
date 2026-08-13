package runnable

import "context"

type invokeLambda[I, O any] struct {
	fn func(context.Context, I, ...Option) (O, error)
}

func (l invokeLambda[I, O]) Invoke(ctx context.Context, input I, options ...Option) (O, error) {
	return l.fn(ctx, input, options...)
}

type streamLambda[I, O any] struct {
	fn func(context.Context, I, ...Option) (*StreamReader[O], error)
}

func (l streamLambda[I, O]) Stream(ctx context.Context, input I, options ...Option) (*StreamReader[O], error) {
	return l.fn(ctx, input, options...)
}

type transformLambda[I, O any] struct {
	fn func(context.Context, *StreamReader[I], ...Option) (*StreamReader[O], error)
}

func (l transformLambda[I, O]) Transform(ctx context.Context, input *StreamReader[I], options ...Option) (*StreamReader[O], error) {
	return l.fn(ctx, input, options...)
}

// InvokableLambda wraps an ordinary discrete function as a complete Adapter.
func InvokableLambda[I, O any](fn func(context.Context, I) (O, error)) *Adapter[I, O] {
	if fn == nil {
		return &Adapter[I, O]{}
	}
	value, _ := Adapt[I, O](invokeLambda[I, O]{fn: func(ctx context.Context, input I, _ ...Option) (O, error) {
		return fn(ctx, input)
	}})
	return value
}

// InvokableLambdaWithOptions wraps a discrete function that consumes options.
func InvokableLambdaWithOptions[I, O any](fn func(context.Context, I, ...Option) (O, error)) *Adapter[I, O] {
	if fn == nil {
		return &Adapter[I, O]{}
	}
	value, _ := Adapt[I, O](invokeLambda[I, O]{fn: fn})
	return value
}

// StreamableLambda wraps a stream-producing function as a complete Adapter.
func StreamableLambda[I, O any](fn func(context.Context, I) (*StreamReader[O], error)) *Adapter[I, O] {
	if fn == nil {
		return &Adapter[I, O]{}
	}
	value, _ := Adapt[I, O](streamLambda[I, O]{fn: func(ctx context.Context, input I, _ ...Option) (*StreamReader[O], error) {
		return fn(ctx, input)
	}})
	return value
}

// StreamableLambdaWithOptions wraps a stream function that consumes options.
func StreamableLambdaWithOptions[I, O any](fn func(context.Context, I, ...Option) (*StreamReader[O], error)) *Adapter[I, O] {
	if fn == nil {
		return &Adapter[I, O]{}
	}
	value, _ := Adapt[I, O](streamLambda[I, O]{fn: fn})
	return value
}

// TransformableLambda wraps a stream-to-stream function as a complete Adapter.
func TransformableLambda[I, O any](fn func(context.Context, *StreamReader[I]) (*StreamReader[O], error)) *Adapter[I, O] {
	if fn == nil {
		return &Adapter[I, O]{}
	}
	value, _ := Adapt[I, O](transformLambda[I, O]{fn: func(ctx context.Context, input *StreamReader[I], _ ...Option) (*StreamReader[O], error) {
		return fn(ctx, input)
	}})
	return value
}

// TransformableLambdaWithOptions wraps a transform function that consumes options.
func TransformableLambdaWithOptions[I, O any](fn func(context.Context, *StreamReader[I], ...Option) (*StreamReader[O], error)) *Adapter[I, O] {
	if fn == nil {
		return &Adapter[I, O]{}
	}
	value, _ := Adapt[I, O](transformLambda[I, O]{fn: fn})
	return value
}
