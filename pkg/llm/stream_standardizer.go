package llm

import (
	"context"
	"errors"

	"goharness/pkg/runnable"
	"goharness/pkg/schema"
)

type wireEventKind string

const (
	wireEventDelta wireEventKind = "delta"
	wireEventUsage wireEventKind = "usage"
	wireEventDone  wireEventKind = "done"
)

// wireEvent is private: provider decoders normalize their protocol here, and
// only public llm.Event values cross the Streamer boundary.
type wireEvent struct {
	kind         wireEventKind
	messages     []schema.Message
	usage        *Usage
	finishReason string
	err          error
}

func snapshotWireEvents(ctx context.Context, events <-chan wireEvent) <-chan wireEvent {
	out := make(chan wireEvent)
	go func() {
		defer close(out)
		if ctx == nil {
			sendWireEvent(context.Background(), out, wireEvent{err: errors.New("snapshot stream context cannot be nil")})
			return
		}
		if events == nil {
			sendWireEvent(ctx, out, wireEvent{err: errors.New("snapshot stream events cannot be nil")})
			return
		}
		accumulator := NewDeltaAccumulator(true)
		for event := range events {
			if event.err != nil {
				sendWireEvent(ctx, out, event)
				return
			}
			if event.kind == wireEventDelta && len(event.messages) > 0 {
				messages, err := accumulator.Apply(Event{Kind: EventDelta, Messages: event.messages})
				if err != nil {
					sendWireEvent(ctx, out, wireEvent{err: err})
					return
				}
				event.messages = messages
			}
			if !sendWireEvent(ctx, out, event) || event.kind == wireEventDone {
				return
			}
		}
	}()
	return out
}

func standardizeWireEvents(ctx context.Context, events <-chan wireEvent) runnable.Stream[Event] {
	out := make(chan runnable.Item[Event])
	go func() {
		defer close(out)
		if ctx == nil {
			out <- runnable.Item[Event]{Err: errors.New("model stream context cannot be nil")}
			return
		}
		if events == nil {
			out <- runnable.Item[Event]{Err: errors.New("model wire event stream cannot be nil")}
			return
		}
		for event := range events {
			if event.err != nil {
				sendModelEvent(ctx, out, runnable.Item[Event]{Err: event.err})
				return
			}
			value := Event{Messages: event.messages, Usage: event.usage, FinishReason: event.finishReason}
			switch event.kind {
			case wireEventDelta:
				value.Kind = EventDelta
			case wireEventUsage:
				value.Kind = EventUsage
			case wireEventDone:
				value.Kind = EventDone
			default:
				sendModelEvent(ctx, out, runnable.Item[Event]{Err: errors.New("unknown model wire event kind")})
				return
			}
			if !sendModelEvent(ctx, out, runnable.Item[Event]{Value: value}) || value.Kind == EventDone {
				return
			}
		}
	}()
	return out
}

func sendWireEvent(ctx context.Context, out chan<- wireEvent, event wireEvent) bool {
	select {
	case <-ctx.Done():
		return false
	case out <- event:
		return true
	}
}
