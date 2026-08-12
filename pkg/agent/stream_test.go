package agent_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"goharness/pkg/agent"
	"goharness/pkg/llm"
	"goharness/pkg/runnable"
	"goharness/pkg/schema"
)

func TestAgentStreamBuffersOutputAsynchronously(t *testing.T) {
	model := &burstModel{produced: make(chan struct{})}
	value, err := agent.NewBasicAgent(
		agent.BaseConfig{Name: "async", Model: model},
		agent.WithStreamBufferSize(4),
	)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	stream, err := value.Stream(
		ctx,
		[]schema.Message{schema.UserMessage("burst")},
		agent.WithDeltaStream(true),
	)
	if err != nil {
		t.Fatal(err)
	}

	select {
	case <-model.produced:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("model producer blocked before the Agent output was consumed")
	}

	deltas := 0
	done := false
	for item := range stream {
		if item.Err != nil {
			t.Fatal(item.Err)
		}
		switch item.Value.Kind {
		case schema.MessageEventDelta:
			deltas++
		case schema.MessageEventDone:
			done = true
		}
	}
	if deltas != 8 || !done {
		t.Fatalf("stream events: deltas=%d done=%t", deltas, done)
	}
}

type burstModel struct {
	produced chan struct{}
}

func (m *burstModel) Invoke(ctx context.Context, request llm.Request, options ...runnable.Option) (llm.Response, error) {
	stream, err := m.Stream(ctx, request, options...)
	if err != nil {
		return llm.Response{}, err
	}
	return llm.ReduceEvents(ctx, stream, request.Options.DeltaStream)
}

func (m *burstModel) Stream(ctx context.Context, request llm.Request, _ ...runnable.Option) (runnable.Stream[llm.Event], error) {
	out := make(chan runnable.Item[llm.Event])
	go func() {
		defer close(out)
		defer close(m.produced)
		for index := range 8 {
			select {
			case <-ctx.Done():
				return
			case out <- runnable.Item[llm.Event]{Value: llm.Event{Kind: llm.EventDelta, Messages: []schema.Message{schema.AssistantMessage(fmt.Sprint(index))}}}:
			}
		}
		out <- runnable.Item[llm.Event]{Value: llm.Event{Kind: llm.EventDone}}
	}()
	return out, nil
}

func (m *burstModel) Capabilities() llm.Capabilities {
	return llm.Capabilities{Streaming: true}
}

func (m *burstModel) ModelInfo() llm.ModelInfo {
	return llm.ModelInfo{Name: "burst", Provider: "test", ModelType: "scripted"}
}
