package llm_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/goharness/goharness/pkg/llm"
	"github.com/goharness/goharness/pkg/runnable"
	"github.com/goharness/goharness/pkg/schema"
)

type rateLimitModel struct{ active int32 }

func (m *rateLimitModel) Invoke(ctx context.Context, _ llm.Request, _ ...runnable.Option) (llm.Response, error) {
	current := atomic.AddInt32(&m.active, 1)
	defer atomic.AddInt32(&m.active, -1)
	if current > 1 {
		return llm.Response{}, context.DeadlineExceeded
	}
	select {
	case <-ctx.Done():
		return llm.Response{}, ctx.Err()
	case <-time.After(5 * time.Millisecond):
	}
	return llm.Response{Messages: []schema.Message{schema.AssistantMessage("ok")}}, nil
}
func (m *rateLimitModel) Stream(ctx context.Context, request llm.Request, options ...runnable.Option) (*runnable.StreamReader[llm.Response], error) {
	return runnable.StreamFromInvoke(ctx, request, m.Invoke, options...)
}
func (m *rateLimitModel) Collect(ctx context.Context, input *runnable.StreamReader[llm.Request], options ...runnable.Option) (llm.Response, error) {
	return runnable.CollectFromInvoke(ctx, input, m.Invoke, options...)
}
func (m *rateLimitModel) Transform(ctx context.Context, input *runnable.StreamReader[llm.Request], options ...runnable.Option) (*runnable.StreamReader[llm.Response], error) {
	return runnable.TransformFromInvoke(ctx, input, m.Invoke, options...)
}
func (*rateLimitModel) Capabilities() llm.Capabilities { return llm.Capabilities{Streaming: true} }
func (*rateLimitModel) ModelInfo() llm.ModelInfo       { return llm.ModelInfo{Name: "rate-limit-test"} }

func TestRateLimitedModelBoundsConcurrentInvocations(t *testing.T) {
	model, err := llm.NewRateLimitedModel(&rateLimitModel{}, llm.RateLimitConfig{MaxConcurrent: 1})
	if err != nil {
		t.Fatal(err)
	}
	results := make(chan error, 2)
	go func() { _, err := model.Invoke(context.Background(), llm.Request{}); results <- err }()
	go func() { _, err := model.Invoke(context.Background(), llm.Request{}); results <- err }()
	for range 2 {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
}

func TestRateLimitedModelReportsEmptyUpstream(t *testing.T) {
	base := &emptyReaderModel{}
	model, err := llm.NewRateLimitedModel(base, llm.RateLimitConfig{MaxConcurrent: 1})
	if err != nil {
		t.Fatal(err)
	}
	reader, err := model.Stream(context.Background(), llm.Request{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reader.Recv(); !errors.Is(err, llm.ErrStreamTruncated) {
		t.Fatalf("Recv() error=%v, want ErrStreamTruncated", err)
	}
}

func TestRateLimitedModelCloseReleasesStreamSlot(t *testing.T) {
	base := &blockingReaderModel{}
	model, err := llm.NewRateLimitedModel(base, llm.RateLimitConfig{MaxConcurrent: 1})
	if err != nil {
		t.Fatal(err)
	}
	first, err := model.Stream(context.Background(), llm.Request{})
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	second, err := model.Stream(ctx, llm.Request{})
	if err != nil {
		t.Fatalf("second Stream did not acquire released slot: %v", err)
	}
	_ = second.Close()
}

type emptyReaderModel struct{}

func (*emptyReaderModel) Invoke(context.Context, llm.Request, ...runnable.Option) (llm.Response, error) {
	return llm.Response{}, nil
}
func (*emptyReaderModel) Stream(ctx context.Context, _ llm.Request, _ ...runnable.Option) (*runnable.StreamReader[llm.Response], error) {
	reader, writer := runnable.Pipe[llm.Response](ctx)
	_ = writer.Close()
	return reader, nil
}
func (m *emptyReaderModel) Collect(ctx context.Context, input *runnable.StreamReader[llm.Request], options ...runnable.Option) (llm.Response, error) {
	return runnable.CollectFromInvoke(ctx, input, m.Invoke, options...)
}
func (m *emptyReaderModel) Transform(ctx context.Context, input *runnable.StreamReader[llm.Request], options ...runnable.Option) (*runnable.StreamReader[llm.Response], error) {
	return runnable.TransformFromInvoke(ctx, input, m.Invoke, options...)
}
func (*emptyReaderModel) Capabilities() llm.Capabilities { return llm.Capabilities{Streaming: true} }
func (*emptyReaderModel) ModelInfo() llm.ModelInfo       { return llm.ModelInfo{Name: "empty"} }

type blockingReaderModel struct{}

func (*blockingReaderModel) Invoke(context.Context, llm.Request, ...runnable.Option) (llm.Response, error) {
	return llm.Response{}, errors.New("blocking model only supports Stream in this test")
}
func (*blockingReaderModel) Stream(ctx context.Context, _ llm.Request, _ ...runnable.Option) (*runnable.StreamReader[llm.Response], error) {
	reader, _ := runnable.Pipe[llm.Response](ctx)
	return reader, nil
}
func (m *blockingReaderModel) Collect(ctx context.Context, input *runnable.StreamReader[llm.Request], options ...runnable.Option) (llm.Response, error) {
	return runnable.CollectFromInvoke(ctx, input, m.Invoke, options...)
}
func (m *blockingReaderModel) Transform(ctx context.Context, input *runnable.StreamReader[llm.Request], options ...runnable.Option) (*runnable.StreamReader[llm.Response], error) {
	return runnable.TransformFromInvoke(ctx, input, m.Invoke, options...)
}
func (*blockingReaderModel) Capabilities() llm.Capabilities { return llm.Capabilities{Streaming: true} }
func (*blockingReaderModel) ModelInfo() llm.ModelInfo       { return llm.ModelInfo{Name: "blocking"} }
