package llm

import (
	"context"
	"errors"
	"sync"
	"time"

	"goharness/pkg/runnable"
)

// RateLimitConfig controls the optional model request limiter. A zero
// MaxConcurrent means unlimited concurrency; MinInterval spaces request
// starts globally for this decorator.
type RateLimitConfig struct {
	MaxConcurrent int
	MinInterval   time.Duration
}

// RateLimitedModel decorates a Model with bounded request admission. A stream
// holds its admission slot until the upstream stream terminates.
type RateLimitedModel struct {
	model         Model
	maxConcurrent int
	minInterval   time.Duration
	semaphore     chan struct{}

	mu          sync.Mutex
	nextAllowed time.Time
}

func NewRateLimitedModel(model Model, config RateLimitConfig) (*RateLimitedModel, error) {
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
	return &RateLimitedModel{
		model:         model,
		maxConcurrent: config.MaxConcurrent,
		minInterval:   config.MinInterval,
		semaphore:     semaphore,
	}, nil
}

func (m *RateLimitedModel) Capabilities() Capabilities { return m.model.Capabilities() }

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

func (m *RateLimitedModel) Stream(ctx context.Context, request Request, options ...runnable.Option) (runnable.Stream[Event], error) {
	release, err := m.acquire(ctx)
	if err != nil {
		return nil, err
	}
	stream, err := m.model.Stream(ctx, request, options...)
	if err != nil {
		release()
		return nil, err
	}
	if stream == nil {
		release()
		return nil, errors.New("rate-limited model returned a nil stream")
	}
	out := make(chan runnable.Item[Event], 16)
	go func() {
		defer close(out)
		defer release()
		for {
			select {
			case <-ctx.Done():
				return
			case item, ok := <-stream:
				if !ok {
					return
				}
				if item.Err != nil {
					out <- item
					return
				}
				select {
				case <-ctx.Done():
					return
				case out <- runnable.Item[Event]{Value: item.Value.Clone()}:
				}
			}
		}
	}()
	return out, nil
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
	released := false
	release := func() {
		if released {
			return
		}
		released = true
		if m.semaphore != nil {
			<-m.semaphore
		}
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

var _ Model = (*RateLimitedModel)(nil)
