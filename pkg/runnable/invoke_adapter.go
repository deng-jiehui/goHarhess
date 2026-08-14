package runnable

import "context"

// InvokeFunc is the exact discrete execution signature used by promotion
// helpers for components whose native form is Invoke.
type InvokeFunc[I, O any] func(context.Context, I, ...Option) (O, error)

// StreamFromInvoke boxes one discrete result into a StreamReader.
func StreamFromInvoke[I, O any](ctx context.Context, input I, invoke InvokeFunc[I, O], options ...Option) (*StreamReader[O], error) {
	output, err := invoke(ctx, input, options...)
	return boxResult(ctx, output, err), nil
}

// CollectFromInvoke concatenates the input stream and invokes the component.
func CollectFromInvoke[I, O any](ctx context.Context, input *StreamReader[I], invoke InvokeFunc[I, O], options ...Option) (O, error) {
	var zero O
	value, err := Concat(ctx, input)
	if err != nil {
		return zero, err
	}
	return invoke(ctx, value, options...)
}

// TransformFromInvoke concatenates the input stream, invokes the component,
// and boxes its result.
func TransformFromInvoke[I, O any](ctx context.Context, input *StreamReader[I], invoke InvokeFunc[I, O], options ...Option) (*StreamReader[O], error) {
	value, err := Concat(ctx, input)
	if err != nil {
		return nil, err
	}
	return StreamFromInvoke(ctx, value, invoke, options...)
}
