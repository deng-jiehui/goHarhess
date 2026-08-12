package compose

import (
	"context"

	"goharness/pkg/runnable"
)

func InvokableLambda[I, O any](fn func(context.Context, I) (O, error)) *runnable.InvokeLambda[I, O] {
	return runnable.InvokableLambda(fn)
}

func InvokableLambdaWithOptions[I, O any](fn func(context.Context, I, ...runnable.Option) (O, error)) *runnable.InvokeLambda[I, O] {
	return runnable.InvokableLambdaWithOptions(fn)
}

func StreamableLambda[I, O any](fn func(context.Context, I) (runnable.Stream[O], error)) *runnable.StreamLambda[I, O] {
	return runnable.StreamableLambda(fn)
}

func StreamableLambdaWithOptions[I, O any](fn func(context.Context, I, ...runnable.Option) (runnable.Stream[O], error)) *runnable.StreamLambda[I, O] {
	return runnable.StreamableLambdaWithOptions(fn)
}

func StreamableLambdaWithReducer[I, O any](fn func(context.Context, I) (runnable.Stream[O], error), reducer runnable.Reducer[O]) *runnable.StreamLambda[I, O] {
	return runnable.StreamableLambdaWithReducer(fn, reducer)
}
