package graph

import (
	"context"
	"errors"

	"goharness/pkg/runnable"
)

// CompiledGraph is an immutable, concurrently reusable Graph execution
// snapshot.
type CompiledGraph[I, O any] struct {
	runtime    *Graph[I, O]
	invokePlan func(context.Context, I, ...runnable.Option) (O, error)
	streamPlan func(context.Context, I, ...runnable.Option) (*runnable.StreamReader[O], error)
}

func (g *CompiledGraph[I, O]) Name() string {
	if g == nil || g.runtime == nil {
		return ""
	}
	return g.runtime.Name()
}
func (g *CompiledGraph[I, O]) Description() string {
	if g == nil || g.runtime == nil {
		return ""
	}
	return g.runtime.Description()
}
func (g *CompiledGraph[I, O]) Snapshot() Snapshot {
	if g == nil || g.runtime == nil {
		return Snapshot{}
	}
	return g.runtime.Snapshot()
}
func (g *CompiledGraph[I, O]) Invoke(ctx context.Context, input I, options ...runnable.Option) (O, error) {
	var zero O
	if g == nil || g.runtime == nil {
		return zero, errors.New("compiled graph is nil")
	}
	return g.invokePlan(ctx, input, options...)
}

func (g *CompiledGraph[I, O]) Stream(ctx context.Context, input I, options ...runnable.Option) (*runnable.StreamReader[O], error) {
	if g == nil || g.streamPlan == nil {
		return nil, errors.New("compiled graph is nil")
	}
	return g.streamPlan(ctx, input, options...)
}

func (g *CompiledGraph[I, O]) Collect(ctx context.Context, input *runnable.StreamReader[I], options ...runnable.Option) (O, error) {
	var zero O
	if g == nil || g.invokePlan == nil {
		return zero, errors.New("compiled graph is nil")
	}
	value, err := input.Collect(ctx)
	if err != nil {
		return zero, err
	}
	return g.invokePlan(ctx, value, options...)
}

func (g *CompiledGraph[I, O]) Transform(ctx context.Context, input *runnable.StreamReader[I], options ...runnable.Option) (*runnable.StreamReader[O], error) {
	if g == nil || g.streamPlan == nil {
		return nil, errors.New("compiled graph is nil")
	}
	value, err := input.Collect(ctx)
	if err != nil {
		return nil, err
	}
	return g.streamPlan(ctx, value, options...)
}
func (g *CompiledGraph[I, O]) Resume(ctx context.Context, options ...runnable.Option) (O, error) {
	var zero O
	if g == nil || g.runtime == nil {
		return zero, errors.New("compiled graph is nil")
	}
	return g.runtime.resumeRuntime(ctx, options...)
}

func (g *CompiledGraph[I, O]) ResumeStream(ctx context.Context, options ...runnable.Option) (*runnable.StreamReader[O], error) {
	if g == nil || g.runtime == nil {
		return nil, errors.New("compiled graph is nil")
	}
	if ctx == nil {
		return nil, errors.New("graph context cannot be nil")
	}
	reader, writer := runnable.Pipe[O](ctx)
	go func() {
		output, err := g.runtime.resumeRuntime(reader.Context(), options...)
		if err != nil {
			_ = writer.CloseWithError(err)
			return
		}
		if err := writer.Send(output); err != nil {
			return
		}
		_ = writer.Close()
	}()
	return reader, nil
}

func (*CompiledGraph[I, O]) graphComponent() {}
func (g *CompiledGraph[I, O]) compileGraph(map[uintptr]bool) error {
	if g == nil || g.runtime == nil {
		return errors.New("compiled graph is nil")
	}
	return nil
}
