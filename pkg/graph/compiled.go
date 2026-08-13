package graph

import (
	"context"
	"errors"

	"goharness/pkg/runnable"
)

// CompiledGraph is an immutable, concurrently reusable Graph execution
// snapshot.
type CompiledGraph[I, O any] struct {
	runtime *Graph[I, O]
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
	return g.runtime.invokeRuntime(ctx, input, options...)
}
func (g *CompiledGraph[I, O]) Resume(ctx context.Context, options ...runnable.Option) (O, error) {
	var zero O
	if g == nil || g.runtime == nil {
		return zero, errors.New("compiled graph is nil")
	}
	return g.runtime.resumeRuntime(ctx, options...)
}

func (*CompiledGraph[I, O]) graphComponent() {}
func (g *CompiledGraph[I, O]) compileGraph(map[uintptr]bool) error {
	if g == nil || g.runtime == nil {
		return errors.New("compiled graph is nil")
	}
	return nil
}
