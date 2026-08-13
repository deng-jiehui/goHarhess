package agent

import (
	"context"
	"strings"
	"testing"

	"goharness/pkg/callback"
	"goharness/pkg/runnable"
	"goharness/pkg/schema"
)

func TestRunLifecycleRejectsStreamWithoutDone(t *testing.T) {
	dispatcher, err := callback.New(nil, callback.Config{})
	if err != nil {
		t.Fatal(err)
	}
	stream := make(chan runnable.Item[schema.MessageEvent], 1)
	stream <- runnable.Item[schema.MessageEvent]{Value: schema.MessageEvent{
		Kind:     schema.MessageEventSnapshot,
		Messages: []schema.Message{schema.AssistantMessage("partial")},
	}}
	close(stream)

	lifecycle := &runLifecycle{ctx: context.Background(), dispatcher: dispatcher}
	wrapped := lifecycle.wrap(stream, 1)
	var sawError bool
	var sawDone bool
	for item := range wrapped {
		if item.Err != nil {
			sawError = strings.Contains(item.Err.Error(), "done event")
		}
		if item.Value.Kind == schema.MessageEventDone {
			sawDone = true
		}
	}
	if !sawError {
		t.Fatal("incomplete stream did not produce an error")
	}
	if sawDone {
		t.Fatal("incomplete stream produced a successful Done event")
	}
}

func TestReduceMessageEventsAcceptsEmptyFinalSnapshot(t *testing.T) {
	stream := make(chan runnable.Item[schema.MessageEvent], 3)
	stream <- runnable.Item[schema.MessageEvent]{Value: schema.MessageEvent{
		Kind:     schema.MessageEventSnapshot,
		Messages: []schema.Message{schema.AssistantMessage("stale")},
	}}
	stream <- runnable.Item[schema.MessageEvent]{Value: schema.MessageEvent{Kind: schema.MessageEventSnapshot}}
	stream <- runnable.Item[schema.MessageEvent]{Value: schema.MessageEvent{Kind: schema.MessageEventDone}}
	close(stream)

	output, err := reduceMessageEvents(context.Background(), stream)
	if err != nil {
		t.Fatal(err)
	}
	if len(output) != 0 {
		t.Fatalf("output = %#v, want empty final snapshot", output)
	}
}
