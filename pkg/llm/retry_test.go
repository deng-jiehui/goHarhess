package llm_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"goharness/pkg/llm"
	"goharness/pkg/runnable"
	"goharness/pkg/schema"
)

type retryModel struct {
	invokeCalls int32
	streamCalls int32
	streamError bool
}

func (m *retryModel) Invoke(context.Context, llm.Request, ...runnable.Option) (llm.Response, error) {
	call := atomic.AddInt32(&m.invokeCalls, 1)
	if call < 3 {
		return llm.Response{}, errors.New("temporary")
	}
	return llm.Response{Messages: []schema.Message{schema.AssistantMessage("ok")}}, nil
}

func (m *retryModel) Stream(ctx context.Context, _ llm.Request, _ ...runnable.Option) (runnable.Stream[llm.Event], error) {
	call := atomic.AddInt32(&m.streamCalls, 1)
	out := make(chan runnable.Item[llm.Event], 2)
	if m.streamError && call == 1 {
		out <- runnable.Item[llm.Event]{Err: errors.New("temporary stream")}
		close(out)
		return out, nil
	}
	out <- runnable.Item[llm.Event]{Value: llm.Event{Kind: llm.EventDelta, Messages: []schema.Message{schema.AssistantMessage("ok")}}}
	out <- runnable.Item[llm.Event]{Value: llm.Event{Kind: llm.EventDone}}
	close(out)
	return out, nil
}

func (*retryModel) Capabilities() llm.Capabilities { return llm.Capabilities{Streaming: true} }
func (*retryModel) ModelInfo() llm.ModelInfo       { return llm.ModelInfo{Name: "retry-test"} }

func TestRetryingModelRetriesInvoke(t *testing.T) {
	base := &retryModel{}
	model, err := llm.NewRetryingModel(base, llm.RetryConfig{MaxAttempts: 3, Retryable: func(error) bool { return true }})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := model.Invoke(context.Background(), llm.Request{}); err != nil {
		t.Fatal(err)
	}
	if got := atomic.LoadInt32(&base.invokeCalls); got != 3 {
		t.Fatalf("invoke calls = %d, want 3", got)
	}
}

func TestRetryingModelRetriesOnlyBeforeStreamData(t *testing.T) {
	base := &retryModel{streamError: true}
	model, err := llm.NewRetryingModel(base, llm.RetryConfig{MaxAttempts: 2, Retryable: func(error) bool { return true }})
	if err != nil {
		t.Fatal(err)
	}
	stream, err := model.Stream(context.Background(), llm.Request{})
	if err != nil {
		t.Fatal(err)
	}
	response, err := llm.ReduceEvents(context.Background(), stream, true)
	if err != nil || len(response.Messages) != 1 || response.Messages[0].Text() != "ok" {
		t.Fatalf("stream response = %#v, %v", response, err)
	}
	if got := atomic.LoadInt32(&base.streamCalls); got != 2 {
		t.Fatalf("stream calls = %d, want 2", got)
	}
}
