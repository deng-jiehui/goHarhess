package graph

import (
	"context"
	"errors"

	"github.com/goharness/goharness/pkg/runnable"
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

// Recoverable 报告图是否具备开启持久化时恢复所需的契约；不具备时返回原因。
// 调用方在持久化接入前据此拒绝无法恢复的图，而不是等运行挂起后才发现
// 无法重建。
func (g *CompiledGraph[I, O]) Recoverable() error {
	if g == nil || g.runtime == nil {
		return errors.New("compiled graph is nil")
	}
	_, err := g.runtime.recoveryMetadata()
	return err
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
	return g.runtime.resumeStreamRuntime(ctx, options...)
}

func (*CompiledGraph[I, O]) graphComponent() {}
func (g *CompiledGraph[I, O]) compileGraph(map[uintptr]bool) error {
	if g == nil || g.runtime == nil {
		return errors.New("compiled graph is nil")
	}
	return nil
}
