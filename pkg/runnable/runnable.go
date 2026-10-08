// Package runnable defines the single provider-neutral execution contract used
// to compose framework components. Components may additionally expose streaming
// or batch capabilities without forcing every component to implement them.
package runnable

import (
	"context"
	"errors"
)

// Runnable is the complete provider-neutral execution contract. Components
// normally implement one native form and use Adapt to bind the other forms.
type Runnable[I, O any] interface {
	Invoke(context.Context, I, ...Option) (O, error)
	Stream(context.Context, I, ...Option) (*StreamReader[O], error)
	Collect(context.Context, *StreamReader[I], ...Option) (O, error)
	Transform(context.Context, *StreamReader[I], ...Option) (*StreamReader[O], error)
}

// Outcome carries a child component result without forcing the Graph engine to
// decide whether that component's error is fatal or domain-visible state.
type Outcome[T any] struct {
	Value T
	Err   error
}

// Batchable is an optional bulk capability for a Runnable.
type Batchable[I, O any] interface {
	Batch(context.Context, []I, ...Option) ([]O, error)
}

// Named provides diagnostic metadata without coupling composition to Agent or
// Provider-specific interfaces.
type Named interface {
	Name() string
	Description() string
}

// NodeKind identifies a component's role in a compiled Graph snapshot without
// coupling the Graph package to domain packages.
type NodeKind string

const (
	NodeKindRunnable  NodeKind = "runnable"
	NodeKindGraph     NodeKind = "graph"
	NodeKindParallel  NodeKind = "parallel"
	NodeKindChatModel NodeKind = "chat_model"
	NodeKindTools     NodeKind = "tools"
	NodeKindRetriever NodeKind = "retriever"
	NodeKindMCP       NodeKind = "mcp"
	NodeKindAgent     NodeKind = "agent"
)

// NodeDescriptor provides Graph diagnostic metadata for a Runnable.
type NodeDescriptor interface {
	NodeKind() NodeKind
}

// NamedRunnable combines execution and diagnostic metadata for registry and
// application entry points without defining an Agent-specific interface.
type NamedRunnable[I, O any] interface {
	Runnable[I, O]
	Named
}

// Func is a function-backed Runnable.
type Func[I, O any] func(context.Context, I, ...Option) (O, error)

// Invoke calls the function-backed Runnable.
func (f Func[I, O]) Invoke(ctx context.Context, input I, options ...Option) (O, error) {
	var zero O
	if f == nil {
		return zero, errors.New("runnable function is nil")
	}
	return f(ctx, input, options...)
}

func (f Func[I, O]) Stream(ctx context.Context, input I, options ...Option) (*StreamReader[O], error) {
	return StreamFromInvoke(ctx, input, f.Invoke, options...)
}

func (f Func[I, O]) Collect(ctx context.Context, input *StreamReader[I], options ...Option) (O, error) {
	return CollectFromInvoke(ctx, input, f.Invoke, options...)
}

func (f Func[I, O]) Transform(ctx context.Context, input *StreamReader[I], options ...Option) (*StreamReader[O], error) {
	return TransformFromInvoke(ctx, input, f.Invoke, options...)
}

// Batch invokes a Runnable once per input when it does not provide a native
// Batchable implementation.
func Batch[I, O any](ctx context.Context, component Runnable[I, O], inputs []I, options ...Option) ([]O, error) {
	if component == nil {
		return nil, errors.New("runnable is nil")
	}
	if ctx == nil {
		return nil, errors.New("runnable context is nil")
	}
	if batchable, ok := component.(Batchable[I, O]); ok {
		return batchable.Batch(ctx, inputs, options...)
	}
	outputs := make([]O, len(inputs))
	for index, input := range inputs {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		output, err := component.Invoke(ctx, input, options...)
		if err != nil {
			return nil, err
		}
		outputs[index] = output
	}
	return outputs, nil
}
