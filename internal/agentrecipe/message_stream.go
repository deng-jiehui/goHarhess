package agentrecipe

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"

	"goharness/pkg/callback"
	"goharness/pkg/control"
	"goharness/pkg/runnable"
	"goharness/pkg/schema"
)

// streamMessageExecution bridges the legacy Agent message stream to the
// Context-scoped EventStream. Graph itself only exposes its final output.
func streamMessageExecution(ctx context.Context, invoke func(context.Context) ([]schema.Message, error)) runnable.Stream[schema.MessageEvent] {
	out := make(chan runnable.Item[schema.MessageEvent], 16)
	if ctx == nil {
		close(out)
		return out
	}
	var final []schema.Message
	var hasSnapshot bool
	streamCallback := callback.StreamCallbackOf[schema.MessageEvent]("agent-message", func(dataCtx context.Context, event schema.MessageEvent) error {
		if event.Kind == schema.MessageEventSnapshot {
			final = schema.CloneMessages(event.Messages)
			hasSnapshot = true
		}
		select {
		case <-dataCtx.Done():
			return dataCtx.Err()
		case out <- runnable.Item[schema.MessageEvent]{Value: event.Clone()}:
			return nil
		}
	})
	runCtx := callback.WithStreamCallbacks(ctx, streamCallback)
	go func() {
		defer close(out)
		output, err := invoke(runCtx)
		if err != nil {
			if isInterruptContext(ctx) {
				out <- runnable.Item[schema.MessageEvent]{Err: err}
				return
			}
			if errors.Is(err, context.Canceled) {
				return
			}
			out <- runnable.Item[schema.MessageEvent]{Err: err}
			return
		}
		if !hasSnapshot || !messagesEqual(final, output) {
			callback.EmitStream(runCtx, schema.MessageEvent{Kind: schema.MessageEventSnapshot, Messages: schema.CloneMessages(output)})
		}
		callback.EmitStream(runCtx, schema.MessageEvent{Kind: schema.MessageEventDone})
	}()
	return out
}

func isInterruptContext(ctx context.Context) bool {
	_, ok := control.AsInterrupt(context.Cause(ctx))
	return ok
}

func messagesEqual(left, right []schema.Message) bool {
	leftJSON, leftErr := json.Marshal(left)
	rightJSON, rightErr := json.Marshal(right)
	return leftErr == nil && rightErr == nil && bytes.Equal(leftJSON, rightJSON)
}
