package graph

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"

	"goharness/pkg/runnable"
	"goharness/pkg/schema"
)

func publishControl(ctx context.Context, event Event) error {
	publish(ctx, event)
	return nil
}

func graphFailure(ctx context.Context, err error) error {
	publish(ctx, Event{Type: EventGraphError, Err: err})
	return err
}

type messageGraph[I any] interface {
	runnable.Runnable[I, []schema.Message]
	Compile() error
}

type resumableMessageGraph interface {
	Resume(context.Context, ...runnable.Option) ([]schema.Message, error)
	Compile() error
}

// StreamMessages exposes the message data plane for a Graph whose final output
// is []schema.Message. Lifecycle events stay on EventSink.
func StreamMessages[I any](
	ctx context.Context,
	g messageGraph[I],
	input I,
	opts ...runnable.Option,
) (runnable.Stream[schema.MessageEvent], error) {
	if ctx == nil {
		return nil, errors.New("graph context cannot be nil")
	}
	if g == nil {
		return nil, errors.New("graph is nil")
	}
	if err := g.Compile(); err != nil {
		return nil, err
	}
	out := make(chan runnable.Item[schema.MessageEvent], 16)
	go func() {
		defer close(out)
		tracker := &nodeDataTracker{}
		sink := DataSinkFunc(func(dataCtx context.Context, event schema.MessageEvent) bool {
			select {
			case <-dataCtx.Done():
				return false
			case out <- runnable.Item[schema.MessageEvent]{Value: event.Clone()}:
				recordNodeData(dataCtx, event)
				return true
			}
		})
		runCtx := withNodeDataTracker(WithDataSink(ctx, sink), tracker)
		output, err := g.Invoke(runCtx, input, opts...)
		if err != nil {
			select {
			case <-ctx.Done():
			case out <- runnable.Item[schema.MessageEvent]{Err: err}:
			}
			return
		}
		finalSnapshot, hasFinalSnapshot := tracker.finalSnapshot()
		if !hasFinalSnapshot || !messagesEqual(finalSnapshot, output) {
			EmitData(runCtx, schema.MessageEvent{Kind: schema.MessageEventSnapshot, Messages: output})
		}
		EmitData(runCtx, schema.MessageEvent{Kind: schema.MessageEventDone})
	}()
	return out, nil
}

// ResumeMessages exposes the message data plane for a resumed Graph.
func ResumeMessages(
	ctx context.Context,
	g resumableMessageGraph,
	opts ...runnable.Option,
) (runnable.Stream[schema.MessageEvent], error) {
	if ctx == nil {
		return nil, errors.New("graph context cannot be nil")
	}
	if g == nil {
		return nil, errors.New("graph is nil")
	}
	if err := g.Compile(); err != nil {
		return nil, err
	}
	return streamMessageExecution(ctx, func(runCtx context.Context) ([]schema.Message, error) {
		return g.Resume(runCtx, opts...)
	}), nil
}

func streamMessageExecution(ctx context.Context, invoke func(context.Context) ([]schema.Message, error)) runnable.Stream[schema.MessageEvent] {
	out := make(chan runnable.Item[schema.MessageEvent], 16)
	go func() {
		defer close(out)
		tracker := &nodeDataTracker{}
		sink := DataSinkFunc(func(dataCtx context.Context, event schema.MessageEvent) bool {
			select {
			case <-dataCtx.Done():
				return false
			case out <- runnable.Item[schema.MessageEvent]{Value: event.Clone()}:
				recordNodeData(dataCtx, event)
				return true
			}
		})
		runCtx := withNodeDataTracker(WithDataSink(ctx, sink), tracker)
		output, err := invoke(runCtx)
		if err != nil {
			select {
			case <-ctx.Done():
			case out <- runnable.Item[schema.MessageEvent]{Err: err}:
			}
			return
		}
		finalSnapshot, hasFinalSnapshot := tracker.finalSnapshot()
		if !hasFinalSnapshot || !messagesEqual(finalSnapshot, output) {
			EmitData(runCtx, schema.MessageEvent{Kind: schema.MessageEventSnapshot, Messages: output})
		}
		EmitData(runCtx, schema.MessageEvent{Kind: schema.MessageEventDone})
	}()
	return out
}

func messagesEqual(left, right []schema.Message) bool {
	leftJSON, leftErr := json.Marshal(left)
	rightJSON, rightErr := json.Marshal(right)
	return leftErr == nil && rightErr == nil && bytes.Equal(leftJSON, rightJSON)
}
