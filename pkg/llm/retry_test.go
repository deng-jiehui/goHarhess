package llm_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"goharness/pkg/callback"
	"goharness/pkg/llm"
	"goharness/pkg/runnable"
	"goharness/pkg/schema"
)

type retryModel struct {
	invokeCalls  int32
	streamCalls  int32
	failFirst    bool
	emptyFirst   bool
	partialFirst bool
}

func (m *retryModel) Invoke(context.Context, llm.Request, ...runnable.Option) (llm.Response, error) {
	call := atomic.AddInt32(&m.invokeCalls, 1)
	if call < 3 {
		return llm.Response{}, errors.New("temporary")
	}
	return llm.Response{Messages: []schema.Message{schema.NewMessage(schema.RoleAssistant, schema.NewTextBlock("ok"))}}, nil
}

func (m *retryModel) Stream(ctx context.Context, _ llm.Request, _ ...runnable.Option) (*runnable.StreamReader[llm.Response], error) {
	call := atomic.AddInt32(&m.streamCalls, 1)
	reader, writer := runnable.Pipe[llm.Response](ctx)
	if call == 1 && m.failFirst {
		if m.partialFirst {
			callback.EmitStream(ctx, llm.Response{Messages: []schema.Message{schema.NewMessage(schema.RoleAssistant, schema.NewTextBlock("partial"))}})
		}
		_ = writer.CloseWithError(errors.New("temporary stream"))
		return reader, nil
	}
	if call == 1 && m.emptyFirst {
		_ = writer.Close()
		return reader, nil
	}
	go func() {
		response := llm.Response{Messages: []schema.Message{schema.NewMessage(schema.RoleAssistant, schema.NewTextBlock("ok"))}}
		callback.EmitStream(ctx, response.Clone())
		_ = writer.Send(response)
		_ = writer.Close()
	}()
	return reader, nil
}
func (m *retryModel) Collect(ctx context.Context, input *runnable.StreamReader[llm.Request], options ...runnable.Option) (llm.Response, error) {
	return runnable.CollectFromInvoke(ctx, input, m.Invoke, options...)
}
func (m *retryModel) Transform(ctx context.Context, input *runnable.StreamReader[llm.Request], options ...runnable.Option) (*runnable.StreamReader[llm.Response], error) {
	value, err := input.Collect(ctx)
	if err != nil {
		return nil, err
	}
	return m.Stream(ctx, value, options...)
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
		t.Fatalf("invoke calls=%d, want 3", got)
	}
}

func TestRetryingModelRetriesBeforeStreamData(t *testing.T) {
	base := &retryModel{failFirst: true}
	model, _ := llm.NewRetryingModel(base, llm.RetryConfig{MaxAttempts: 2, Retryable: func(error) bool { return true }})
	reader, err := model.Stream(context.Background(), llm.Request{})
	if err != nil {
		t.Fatal(err)
	}
	response, err := reader.Collect(context.Background())
	if err != nil || response.Messages[0].JoinedText("") != "ok" {
		t.Fatalf("response=%#v err=%v", response, err)
	}
	if got := atomic.LoadInt32(&base.streamCalls); got != 2 {
		t.Fatalf("stream calls=%d, want 2", got)
	}
}

func TestRetryingModelRetriesEmptyReaderBeforeData(t *testing.T) {
	base := &retryModel{emptyFirst: true}
	model, _ := llm.NewRetryingModel(base, llm.RetryConfig{MaxAttempts: 2, Retryable: func(error) bool { return true }})
	reader, err := model.Stream(context.Background(), llm.Request{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reader.Collect(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := atomic.LoadInt32(&base.streamCalls); got != 2 {
		t.Fatalf("stream calls=%d, want 2", got)
	}
}

func TestRetryingModelDoesNotRetryAfterStreamData(t *testing.T) {
	base := &retryModel{failFirst: true, partialFirst: true}
	model, _ := llm.NewRetryingModel(base, llm.RetryConfig{MaxAttempts: 2, Retryable: func(error) bool { return true }})
	reader, err := model.Stream(context.Background(), llm.Request{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reader.Recv(); err == nil {
		t.Fatal("partial stream error was accepted")
	}
	if got := atomic.LoadInt32(&base.streamCalls); got != 1 {
		t.Fatalf("stream calls=%d, want 1", got)
	}
}
