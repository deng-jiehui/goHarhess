package agent

import (
	"context"
	"errors"

	"github.com/goharness/goharness/internal/streamcompat"
	"github.com/goharness/goharness/pkg/callback"
	"github.com/goharness/goharness/pkg/llm"
	"github.com/goharness/goharness/pkg/runnable"
	"github.com/goharness/goharness/pkg/schema"
	"github.com/goharness/goharness/pkg/stream"
)

// runLifecycle 转发 Graph 已完成持久化的输出。
type runLifecycle struct {
	ctx context.Context
}

type modelStreamOwnerKey struct{}

func (b *base) beginRun(ctx context.Context, _ []schema.Message, _ RunOptions) (context.Context, *runLifecycle, error) {
	if ctx == nil {
		return nil, nil, errors.New("agent run context cannot be nil")
	}
	ctx, _ = callback.EnterScope(ctx)
	return ctx, &runLifecycle{ctx: ctx}, nil
}

func (*runLifecycle) fail(error) {}

func (l *runLifecycle) wrap(upstream *runnable.StreamReader[[]schema.Message]) (*runnable.StreamReader[[]schema.Message], error) {
	if upstream == nil {
		return nil, errors.New("agent returned a nil stream reader")
	}
	reader, writer := runnable.Pipe[[]schema.Message](context.WithoutCancel(l.ctx))
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer upstream.Close()
		output, err := upstream.Collect(upstream.Context())
		if err != nil {
			_ = writer.CloseWithError(err)
			return
		}
		output = schema.CloneMessages(output)
		stream.EnsureMessageIDs(output)

		streamcompat.Snapshot(l.ctx, output)
		if err := writer.Send(output); err != nil {
			return
		}
		_ = writer.Close()
	}()
	go func() {
		select {
		case <-reader.Context().Done():
			_ = upstream.Close()
		case <-done:
		}
	}()
	return reader, nil
}

func (b *base) bindModelStream(ctx context.Context) context.Context {
	// Public run streams receive canonical model deltas directly. Keep legacy
	// snapshot forwarding only when an application explicitly subscribed to it.
	if !callback.HasStreamCallbacks[schema.MessageEvent](ctx) {
		return ctx
	}
	ctx = context.WithValue(ctx, modelStreamOwnerKey{}, b)
	return callback.WithStreamCallbacks(ctx, callback.StreamCallbackOf[llm.Response]("agent-llm", func(dataCtx context.Context, response llm.Response) error {
		if owner, _ := dataCtx.Value(modelStreamOwnerKey{}).(*base); owner != b {
			return nil
		}
		streamcompat.Snapshot(dataCtx, b.withAgentName(response.Messages))
		return nil
	}))
}
