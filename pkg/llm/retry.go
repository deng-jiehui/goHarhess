package llm

import (
	"context"
	"errors"
	"net"
	"time"

	"goharness/pkg/runnable"
)

// RetryConfig controls the optional model retry decorator. MaxAttempts is the
// total number of provider attempts, so 1 disables retries.
type RetryConfig struct {
	MaxAttempts  int
	InitialDelay time.Duration
	MaxDelay     time.Duration
	Retryable    func(error) bool
}

// RetryingModel decorates a Model with bounded retries. Stream retries are
// allowed only before the first successful event is delivered.
type RetryingModel struct {
	model     Model
	config    RetryConfig
	retryable func(error) bool
}

func NewRetryingModel(model Model, config RetryConfig) (*RetryingModel, error) {
	if model == nil {
		return nil, errors.New("retrying model requires a model")
	}
	if config.MaxAttempts <= 0 {
		config.MaxAttempts = 1
	}
	if config.InitialDelay < 0 {
		config.InitialDelay = 0
	}
	if config.MaxDelay <= 0 {
		config.MaxDelay = 30 * time.Second
	}
	retryable := config.Retryable
	if retryable == nil {
		retryable = defaultRetryable
	}
	return &RetryingModel{model: model, config: config, retryable: retryable}, nil
}

func (m *RetryingModel) Capabilities() Capabilities {
	return m.model.Capabilities()
}

func (m *RetryingModel) ModelInfo() ModelInfo {
	info := DescribeModel(m.model)
	if info.Attributes == nil {
		info.Attributes = map[string]any{}
	}
	info.Attributes["retry_max_attempts"] = m.config.MaxAttempts
	return info
}

func (m *RetryingModel) Invoke(ctx context.Context, request Request, options ...runnable.Option) (Response, error) {
	if ctx == nil {
		return Response{}, errors.New("retrying model context cannot be nil")
	}
	var last error
	for attempt := 0; attempt < m.config.MaxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return Response{}, err
		}
		response, err := m.model.Invoke(ctx, request, options...)
		if err == nil {
			return response, nil
		}
		last = err
		if attempt+1 >= m.config.MaxAttempts || !m.retryable(err) {
			break
		}
		if err := waitRetry(ctx, retryDelay(m.config, attempt)); err != nil {
			return Response{}, err
		}
	}
	return Response{}, last
}

func (m *RetryingModel) Stream(ctx context.Context, request Request, options ...runnable.Option) (runnable.Stream[Event], error) {
	if ctx == nil {
		return nil, errors.New("retrying model context cannot be nil")
	}
	stream, attempt, err := m.openStream(ctx, request, options...)
	if err != nil {
		return nil, err
	}
	out := make(chan runnable.Item[Event], 16)
	go func() {
		defer close(out)
		started := false
		for {
			item, ok := <-stream
			if !ok {
				return
			}
			if item.Err != nil && !started && attempt+1 < m.config.MaxAttempts && m.retryable(item.Err) {
				var next runnable.Stream[Event]
				var openErr error
				for nextAttempt := attempt + 1; nextAttempt < m.config.MaxAttempts; nextAttempt++ {
					if err := waitRetry(ctx, retryDelay(m.config, nextAttempt-1)); err != nil {
						out <- runnable.Item[Event]{Err: err}
						return
					}
					next, openErr = m.model.Stream(ctx, request, options...)
					if openErr == nil && next != nil {
						stream = next
						attempt = nextAttempt
						break
					}
					if openErr == nil {
						openErr = errors.New("model returned a nil stream")
					}
					attempt = nextAttempt
					if !m.retryable(openErr) || nextAttempt+1 >= m.config.MaxAttempts {
						break
					}
				}
				if next == nil {
					out <- runnable.Item[Event]{Err: openErr}
					return
				}
				continue
			}
			if item.Err != nil {
				out <- item
				return
			}
			started = true
			select {
			case <-ctx.Done():
				return
			case out <- runnable.Item[Event]{Value: item.Value.Clone()}:
			}
		}
	}()
	return out, nil
}

func (m *RetryingModel) openStream(ctx context.Context, request Request, options ...runnable.Option) (runnable.Stream[Event], int, error) {
	for attempt := 0; attempt < m.config.MaxAttempts; attempt++ {
		stream, err := m.model.Stream(ctx, request, options...)
		if err == nil && stream != nil {
			return stream, attempt, nil
		}
		if err == nil {
			err = errors.New("model returned a nil stream")
		}
		if attempt+1 >= m.config.MaxAttempts || !m.retryable(err) {
			return nil, attempt, err
		}
		if waitErr := waitRetry(ctx, retryDelay(m.config, attempt)); waitErr != nil {
			return nil, attempt, waitErr
		}
	}
	return nil, m.config.MaxAttempts - 1, errors.New("model retry attempts exhausted")
}

func retryDelay(config RetryConfig, attempt int) time.Duration {
	delay := config.InitialDelay
	for i := 0; i < attempt && delay < config.MaxDelay; i++ {
		delay *= 2
	}
	if delay > config.MaxDelay {
		return config.MaxDelay
	}
	return delay
}

func waitRetry(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func defaultRetryable(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	var status interface{ StatusCode() int }
	if errors.As(err, &status) {
		code := status.StatusCode()
		return code == 408 || code == 425 || code == 429 || code >= 500
	}
	var networkErr net.Error
	return errors.As(err, &networkErr)
}

var _ Model = (*RetryingModel)(nil)
