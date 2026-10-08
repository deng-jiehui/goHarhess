package llm

import (
	"context"
	"errors"
	"fmt"

	"github.com/goharness/goharness/pkg/callback"
	"github.com/goharness/goharness/pkg/runnable"
	"github.com/goharness/goharness/pkg/schema"
)

type wireEventKind string

const (
	wireEventDelta wireEventKind = "delta"
	wireEventUsage wireEventKind = "usage"
	wireEventDone  wireEventKind = "done"
)

// wireEvent is private: provider decoders normalize their protocol here, then
// the adapter emits one complete public Response through StreamReader.
type wireEvent struct {
	kind         wireEventKind
	messages     []schema.Message
	usage        *Usage
	finishReason string
	responseID   string
	model        string
	err          error
}

func responseReaderFromWire(ctx context.Context, events <-chan wireEvent, cancelUpstream context.CancelFunc) (*runnable.StreamReader[Response], error) {
	if ctx == nil {
		return nil, errors.New("model stream context cannot be nil")
	}
	if events == nil {
		return nil, errors.New("model wire event stream cannot be nil")
	}
	reader, writer := runnable.Pipe[Response](context.WithoutCancel(ctx))
	go func() {
		defer func() {
			if cancelUpstream != nil {
				cancelUpstream()
			}
			_ = writer.Close()
		}()
		accumulator := newWireAccumulator(ctx)
		response := Response{}
		var responseID, model string
		for {
			select {
			case <-reader.Context().Done():
				return
			case <-ctx.Done():
				_ = writer.CloseWithError(ctx.Err())
				return
			case event, open := <-events:
				if !open {
					_ = writer.CloseWithError(errors.New("model wire stream closed without completion"))
					return
				}
				if event.err != nil {
					_ = writer.CloseWithError(event.err)
					return
				}
				if event.usage != nil {
					response.Usage = cloneUsage(event.usage)
				}
				if event.finishReason != "" {
					response.FinishReason = event.finishReason
				}
				if event.responseID != "" {
					responseID = event.responseID
				}
				if event.model != "" {
					model = event.model
				}
				switch event.kind {
				case wireEventDelta:
					callback.NotifyStream[Response](ctx)
					err := accumulator.apply(event.messages)
					if err != nil {
						_ = writer.CloseWithError(err)
						return
					}
					if callback.HasStreamCallbacks[Response](ctx) {
						response.Messages = accumulator.messages()
						callback.EmitStream(ctx, response.Clone())
					}
				case wireEventUsage:
					if callback.HasStreamCallbacks[Response](ctx) {
						response.Messages = accumulator.messages()
						callback.EmitStream(ctx, response.Clone())
					}
				case wireEventDone:
					var err error
					response.Messages, err = accumulator.finish(&schema.ResponseMeta{ID: responseID, Model: model, FinishReason: response.FinishReason, Usage: response.Usage})
					if err != nil {
						_ = writer.CloseWithError(err)
						return
					}
					for i := range response.Messages {
						if err := response.Messages[i].Validate(); err != nil {
							_ = writer.CloseWithError(fmt.Errorf("final response message %d: %w", i, err))
							return
						}
					}
					if err := writer.Send(response.Clone()); err != nil {
						return
					}
					return
				default:
					_ = writer.CloseWithError(errors.New("unknown model wire event kind"))
					return
				}
			}
		}
	}()
	return reader, nil
}

func sendWireEvent(ctx context.Context, out chan<- wireEvent, event wireEvent) bool {
	select {
	case <-ctx.Done():
		return false
	case out <- event:
		return true
	}
}
