package llm_test

import (
	"context"
	"strings"
	"testing"

	"goharness/pkg/llm"
	"goharness/pkg/runnable"
	"goharness/pkg/schema"
)

func TestReduceEventsRejectsStreamWithoutDone(t *testing.T) {
	stream := make(chan runnable.Item[llm.Event], 1)
	stream <- runnable.Item[llm.Event]{Value: llm.Event{
		Kind:     llm.EventDelta,
		Messages: []schema.Message{schema.AssistantMessage("partial")},
	}}
	close(stream)

	_, err := llm.ReduceEvents(context.Background(), stream, true)
	if err == nil || !strings.Contains(err.Error(), "without done event") {
		t.Fatalf("reduce error=%v", err)
	}
}
