package agent

import (
	"context"
	"errors"

	"goharness/pkg/callback"
	"goharness/pkg/llm"
	"goharness/pkg/runnable"
	"goharness/pkg/schema"
)

// runLifecycle owns conversation commit. Agent execution lifecycle is the
// lifecycle of its compiled Graph.
type runLifecycle struct {
	ctx     context.Context
	session *conversationSession
}

type modelStreamOwnerKey struct{}

func (b *base) beginRun(ctx context.Context, _ []schema.Message, opts RunOptions) (context.Context, *runLifecycle, error) {
	if ctx == nil {
		return nil, nil, errors.New("agent run context cannot be nil")
	}
	ctx, _ = callback.EnterScope(ctx)
	ctx = callback.WithMetadata(ctx, opts.Metadata)
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
		if err := l.session.commit(l.ctx, output); err != nil {
			_ = writer.CloseWithError(err)
			return
		}
		callback.EmitStream(l.ctx, schema.MessageEvent{Kind: schema.MessageEventSnapshot, Messages: output})
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
	ctx = context.WithValue(ctx, modelStreamOwnerKey{}, b)
	return callback.WithStreamCallbacks(ctx, callback.StreamCallbackOf[llm.Response]("agent-llm", func(dataCtx context.Context, response llm.Response) error {
		if owner, _ := dataCtx.Value(modelStreamOwnerKey{}).(*base); owner != b {
			return nil
		}
		callback.EmitStream(dataCtx, schema.MessageEvent{
			Kind:     schema.MessageEventSnapshot,
			Messages: b.withAgentName(response.Messages),
		})
		return nil
	}))
}
