package agent

import (
	"context"
	"errors"

	"goharness/pkg/control"
	"goharness/pkg/graph"
	"goharness/pkg/runnable"
	"goharness/pkg/schema"
)

// Agent is the complete public Agent contract: one final result, one typed
// message data stream, and stable diagnostic naming.
type Agent interface {
	runnable.NamedRunnable[[]schema.Message, []schema.Message]
	runnable.Streamer[[]schema.Message, schema.MessageEvent]
}

func emitMessageItem(ctx context.Context, out chan<- runnable.Item[schema.MessageEvent], item runnable.Item[schema.MessageEvent]) bool {
	item.Value = item.Value.Clone()
	select {
	case <-ctx.Done():
		return false
	case out <- item:
		return true
	}
}

func appendMessages(left, right []schema.Message) []schema.Message {
	combined := make([]schema.Message, 0, len(left)+len(right))
	combined = append(combined, schema.CloneMessages(left)...)
	combined = append(combined, schema.CloneMessages(right)...)
	return combined
}

func reduceMessageEvents(ctx context.Context, stream runnable.Stream[schema.MessageEvent]) ([]schema.Message, error) {
	if ctx == nil {
		return nil, errors.New("agent context is nil")
	}
	if stream == nil {
		return nil, errors.New("agent event stream is nil")
	}
	var final []schema.Message
	for {
		var item runnable.Item[schema.MessageEvent]
		var ok bool
		select {
		case <-ctx.Done():
			if _, interrupted := control.AsInterrupt(context.Cause(ctx)); !interrupted {
				return nil, ctx.Err()
			}
			item, ok = <-stream
		case item, ok = <-stream:
		}
		if !ok {
			return schema.CloneMessages(final), nil
		}
		if item.Err != nil {
			return nil, item.Err
		}
		if item.Value.Kind == schema.MessageEventSnapshot {
			final = schema.CloneMessages(item.Value.Messages)
		}
		if item.Value.Kind == schema.MessageEventDone {
			return schema.CloneMessages(final), nil
		}
	}
}

func prefetchMessageEvents(ctx context.Context, stream runnable.Stream[schema.MessageEvent]) (runnable.Stream[schema.MessageEvent], error) {
	if stream == nil {
		return nil, errors.New("agent recipe returned a nil stream")
	}
	var first runnable.Item[schema.MessageEvent]
	var ok bool
	select {
	case <-ctx.Done():
		if _, interrupted := control.AsInterrupt(context.Cause(ctx)); !interrupted {
			return nil, ctx.Err()
		}
		first, ok = <-stream
	case first, ok = <-stream:
	}
	if !ok {
		out := make(chan runnable.Item[schema.MessageEvent])
		close(out)
		return out, nil
	}
	if first.Err != nil {
		return nil, first.Err
	}
	out := make(chan runnable.Item[schema.MessageEvent], 1)
	out <- first
	go func() {
		defer close(out)
		for item := range stream {
			if item.Err != nil && interruptCauseRequested(ctx) {
				out <- item
				return
			}
			if !emitMessageItem(ctx, out, item) {
				if _, interrupted := control.AsInterrupt(context.Cause(ctx)); interrupted {
					continue
				}
				return
			}
		}
	}()
	return out, nil
}

func isPersistedInterruption(err error) bool {
	return errors.Is(err, graph.ErrInterrupted)
}

func interruptCauseRequested(ctx context.Context) bool {
	_, interrupted := control.AsInterrupt(context.Cause(ctx))
	return interrupted
}
