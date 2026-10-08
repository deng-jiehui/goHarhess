package llm

import (
	"context"
	"errors"
	"io"
	"reflect"
	"sync"
	"time"

	"github.com/goharness/goharness/pkg/runnable"
)

type RateLimitConfig struct {
	MaxConcurrent int
	MinInterval   time.Duration
}

type RateLimitedModel struct {
	model         ChatModel
	maxConcurrent int
	minInterval   time.Duration
	semaphore     chan struct{}
	mu            sync.Mutex
	nextAllowed   time.Time
}

func NewRateLimitedModel(model ChatModel, config RateLimitConfig) (*RateLimitedModel, error) {
	if model == nil {
		return nil, errors.New("rate-limited model requires a model")
	}
	if config.MaxConcurrent < 0 {
		return nil, errors.New("rate-limited model max concurrency cannot be negative")
	}
	if config.MinInterval < 0 {
		return nil, errors.New("rate-limited model minimum interval cannot be negative")
	}
	var semaphore chan struct{}
	if config.MaxConcurrent > 0 {
		semaphore = make(chan struct{}, config.MaxConcurrent)
	}
	return &RateLimitedModel{model: model, maxConcurrent: config.MaxConcurrent, minInterval: config.MinInterval, semaphore: semaphore}, nil
}

func (m *RateLimitedModel) Capabilities() Capabilities { return m.model.Capabilities() }

// UnwrapModel 返回被包装的下层模型，供策略链检查与重组使用。
func (m *RateLimitedModel) UnwrapModel() ChatModel { return m.model }

// Policy 返回生效中的限流配置。
func (m *RateLimitedModel) Policy() RateLimitConfig {
	return RateLimitConfig{MaxConcurrent: m.maxConcurrent, MinInterval: m.minInterval}
}

// OptionTypes 透传下游模型声明的扩展配置类型，让 Provider 专属选项能穿过
// 限流包装层参与按类型分发。
func (m *RateLimitedModel) OptionTypes() []reflect.Type {
	if m == nil {
		return nil
	}
	if provider, ok := m.model.(optionTypesProvider); ok && provider != nil {
		return provider.OptionTypes()
	}
	return nil
}
func (m *RateLimitedModel) ModelInfo() ModelInfo {
	info := DescribeModel(m.model)
	if info.Attributes == nil {
		info.Attributes = map[string]any{}
	}
	info.Attributes["rate_limit_max_concurrent"] = m.maxConcurrent
	info.Attributes["rate_limit_min_interval_ms"] = m.minInterval.Milliseconds()
	return info
}

func (m *RateLimitedModel) Invoke(ctx context.Context, request Request, options ...runnable.Option) (Response, error) {
	release, err := m.acquire(ctx)
	if err != nil {
		return Response{}, err
	}
	defer release()
	return m.model.Invoke(ctx, request, options...)
}

func (m *RateLimitedModel) Stream(ctx context.Context, request Request, options ...runnable.Option) (*runnable.StreamReader[Response], error) {
	release, err := m.acquire(ctx)
	if err != nil {
		return nil, err
	}
	upstream, err := m.model.Stream(ctx, request, options...)
	if err != nil {
		release()
		return nil, err
	}
	if upstream == nil {
		release()
		return nil, errors.New("rate-limited model returned a nil stream")
	}
	reader, writer := runnable.Pipe[Response](context.WithoutCancel(ctx))
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer release()
		defer upstream.Close()
		defer writer.Close()
		sawResponse := false
		for {
			value, recvErr := upstream.Recv()
			if errors.Is(recvErr, io.EOF) {
				if !sawResponse {
					_ = writer.CloseWithError(ErrStreamTruncated)
				}
				return
			}
			if recvErr != nil {
				_ = writer.CloseWithError(recvErr)
				return
			}
			if err := writer.Send(value.Clone()); err != nil {
				return
			}
			sawResponse = true
		}
	}()
	closeUpstreamWhenReaderStops(reader, upstream, done)
	return reader, nil
}

func (m *RateLimitedModel) Collect(ctx context.Context, input *runnable.StreamReader[Request], options ...runnable.Option) (Response, error) {
	value, err := input.Collect(ctx)
	if err != nil {
		return Response{}, err
	}
	return m.Invoke(ctx, value, options...)
}
func (m *RateLimitedModel) Transform(ctx context.Context, input *runnable.StreamReader[Request], options ...runnable.Option) (*runnable.StreamReader[Response], error) {
	value, err := input.Collect(ctx)
	if err != nil {
		return nil, err
	}
	return m.Stream(ctx, value, options...)
}

func (m *RateLimitedModel) acquire(ctx context.Context) (func(), error) {
	if ctx == nil {
		return func() {}, errors.New("rate-limited model context cannot be nil")
	}
	if err := ctx.Err(); err != nil {
		return func() {}, err
	}
	if m.semaphore != nil {
		select {
		case <-ctx.Done():
			return func() {}, ctx.Err()
		case m.semaphore <- struct{}{}:
		}
	}
	var once sync.Once
	release := func() {
		once.Do(func() {
			if m.semaphore != nil {
				<-m.semaphore
			}
		})
	}
	if m.minInterval <= 0 {
		return release, nil
	}
	m.mu.Lock()
	waitUntil := m.nextAllowed
	now := time.Now()
	if waitUntil.Before(now) {
		waitUntil = now
	}
	m.nextAllowed = waitUntil.Add(m.minInterval)
	m.mu.Unlock()
	if delay := time.Until(waitUntil); delay > 0 {
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			release()
			return func() {}, ctx.Err()
		case <-timer.C:
		}
	}
	return release, nil
}

var _ ChatModel = (*RateLimitedModel)(nil)
