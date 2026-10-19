package llm

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/goharness/goharness/pkg/observe"
	"github.com/goharness/goharness/pkg/runnable"
	"github.com/goharness/goharness/pkg/schema"
	"github.com/goharness/goharness/pkg/stream"
)

// 快照投递失败也必须结束模型观测，不能留下未收尾的调用。
func TestSlowConsumerClosesModelCallback(t *testing.T) {
	var starts, ends, failures atomic.Int64
	terminal := make(chan observe.Event, 1)
	ctx := observe.With(context.Background(), observe.ObserverFunc(func(_ context.Context, event observe.Event) {
		if event.Kind != observe.KindLLM {
			return
		}
		switch event.Phase {
		case observe.PhaseStart:
			starts.Add(1)
		case observe.PhaseEnd:
			ends.Add(1)
		case observe.PhaseError:
			if errors.Is(event.Err, stream.ErrSlowConsumer) {
				failures.Add(1)
			}
			terminal <- event
		}
	}))
	node, err := NewChatModelNode(NewScriptedModel([]schema.Message{
		schema.AssistantMessage("first"), schema.AssistantMessage("second"),
	}))
	if err != nil {
		t.Fatal(err)
	}
	finished := make(chan error, 1)
	reader, err := stream.Run(ctx, func(run context.Context) (Response, error) {
		response, callErr := node.Invoke(run, Request{})
		finished <- callErr
		return response, callErr
	}, stream.Config{Buffer: 0, SendTimeout: 20 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	select {
	case callErr := <-finished:
		if !errors.Is(callErr, stream.ErrSlowConsumer) && !errors.Is(callErr, context.Canceled) {
			t.Fatalf("调用错误 = %v，期望慢消费者错误", callErr)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("模型调用没有退出")
	}
	// 运行取消可能先唤醒调用方，终止事件要等模型生产协程退出后验证。
	select {
	case event := <-terminal:
		if !errors.Is(event.Err, stream.ErrSlowConsumer) {
			t.Fatalf("观测错误 = %v，期望慢消费者错误", event.Err)
		}
	case <-time.After(time.Second):
		t.Fatal("模型调用没有终止观测")
	}
	if _, err := reader.Recv(); !errors.Is(err, stream.ErrSlowConsumer) {
		t.Fatalf("公共流错误 = %v，期望慢消费者错误", err)
	}
	if starts.Load() != 1 || ends.Load() != 0 || failures.Load() != 1 {
		t.Fatalf("开始/结束/失败 = %d/%d/%d", starts.Load(), ends.Load(), failures.Load())
	}
}

// 调用方主动关闭 Reader 属于取消，不能误报成模型响应截断。
func TestClosingObservedReaderReportsCancellation(t *testing.T) {
	terminal := make(chan observe.Event, 2)
	ctx := observe.With(context.Background(), observe.ObserverFunc(func(_ context.Context, event observe.Event) {
		if event.Kind == observe.KindLLM && (event.Phase == observe.PhaseEnd || event.Phase == observe.PhaseError) {
			terminal <- event
		}
	}))
	upstream, _ := runnable.Pipe[Response](context.Background())
	reader, err := ObserveReader(ctx, "cancel", Request{}, upstream)
	if err != nil {
		t.Fatal(err)
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case event := <-terminal:
		if event.Phase != observe.PhaseError || !errors.Is(event.Err, context.Canceled) {
			t.Fatalf("终止事件 = %+v，期望取消错误", event)
		}
	case <-time.After(time.Second):
		t.Fatal("关闭 Reader 后没有终止事件")
	}
}
