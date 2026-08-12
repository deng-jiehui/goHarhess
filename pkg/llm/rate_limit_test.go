package llm_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"goharness/pkg/llm"
	"goharness/pkg/runnable"
	"goharness/pkg/schema"
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

func (m *rateLimitModel) Stream(ctx context.Context, _ llm.Request, _ ...runnable.Option) (runnable.Stream[llm.Event], error) {
	out := make(chan runnable.Item[llm.Event], 1)
	out <- runnable.Item[llm.Event]{Value: llm.Event{Kind: llm.EventDone}}
	close(out)
	return out, nil
}

func (*rateLimitModel) Capabilities() llm.Capabilities { return llm.Capabilities{Streaming: true} }
func (*rateLimitModel) ModelInfo() llm.ModelInfo       { return llm.ModelInfo{Name: "rate-limit-test"} }

func TestRateLimitedModelBoundsConcurrentInvocations(t *testing.T) {
	model, err := llm.NewRateLimitedModel(&rateLimitModel{}, llm.RateLimitConfig{MaxConcurrent: 1})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	results := make(chan error, 2)
	go func() { _, err := model.Invoke(ctx, llm.Request{}); results <- err }()
	go func() { _, err := model.Invoke(ctx, llm.Request{}); results <- err }()
	for range 2 {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
}
