package callback_test

import (
	"context"
	"testing"
	"time"

	"goharness/pkg/callback"
)

type observerFunc func(context.Context, callback.Event)

func (f observerFunc) Observe(ctx context.Context, event callback.Event) {
	f(ctx, event)
}

func TestAsyncObserverManagerIsExplicitAndDoesNotBlock(t *testing.T) {
	received := make(chan callback.Event, 1)
	manager := callback.NewAsyncObserverManager(observerFunc(func(_ context.Context, event callback.Event) {
		received <- event
	}))
	started := time.Now()
	manager.Publish(context.Background(), callback.Event{Type: callback.EventAgentChunk})
	if elapsed := time.Since(started); elapsed > 50*time.Millisecond {
		t.Fatalf("Publish took %s", elapsed)
	}
	select {
	case event := <-received:
		if event.Type != callback.EventAgentChunk {
			t.Fatalf("event type = %s", event.Type)
		}
	case <-time.After(time.Second):
		t.Fatal("async observer did not receive event")
	}
}
