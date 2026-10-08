package graph

import (
	"context"
	"github.com/goharness/goharness/pkg/runnable"
	"github.com/goharness/goharness/pkg/stream"
)

// StreamEvents executes the streaming plan, preserving node data streams while
// multiplexing public observations and exactly one final result.
func (g *CompiledGraph[I, O]) StreamEvents(ctx context.Context, input I, config stream.Config, options ...runnable.Option) (*stream.Reader[O], error) {
	return stream.Run(ctx, func(run context.Context) (O, error) {
		var zero O
		r, err := g.Stream(run, input, options...)
		if err != nil {
			return zero, err
		}
		defer r.Close()
		return r.Collect(r.Context())
	}, config)
}

// ResumeStreamEvents 恢复执行，并通过同一公共事件协议发布结果。
func (g *CompiledGraph[I, O]) ResumeStreamEvents(ctx context.Context, config stream.Config, options ...runnable.Option) (*stream.Reader[O], error) {
	return stream.Run(ctx, func(run context.Context) (O, error) {
		var zero O
		r, err := g.ResumeStream(run, options...)
		if err != nil {
			return zero, err
		}
		defer r.Close()
		return r.Collect(r.Context())
	}, config)
}
