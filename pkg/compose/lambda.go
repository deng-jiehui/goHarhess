package compose

import (
	"context"

	"goharness/pkg/runnable"
)

func InvokableLambda[I, O any](fn func(context.Context, I) (O, error)) *runnable.Adapter[I, O] {
	return runnable.InvokableLambda(fn)
}

func InvokableLambdaWithOptions[I, O any](fn func(context.Context, I, ...runnable.Option) (O, error)) *runnable.Adapter[I, O] {
	return runnable.InvokableLambdaWithOptions(fn)
}

func StreamableLambda[I, O any](fn func(context.Context, I) (*runnable.StreamReader[O], error)) *runnable.Adapter[I, O] {
	return runnable.StreamableLambda(fn)
}

func StreamableLambdaWithOptions[I, O any](fn func(context.Context, I, ...runnable.Option) (*runnable.StreamReader[O], error)) *runnable.Adapter[I, O] {
	return runnable.StreamableLambdaWithOptions(fn)
}

func TransformableLambda[I, O any](fn func(context.Context, *runnable.StreamReader[I]) (*runnable.StreamReader[O], error)) *runnable.Adapter[I, O] {
	return runnable.TransformableLambda(fn)
}

func TransformableLambdaWithOptions[I, O any](fn func(context.Context, *runnable.StreamReader[I], ...runnable.Option) (*runnable.StreamReader[O], error)) *runnable.Adapter[I, O] {
	return runnable.TransformableLambdaWithOptions(fn)
}
