package llm

import (
	"context"
	"errors"
	"io"
	"net"
	"sync/atomic"
	"time"

	"goharness/pkg/callback"
	"goharness/pkg/runnable"
)

type RetryConfig struct {
	MaxAttempts  int
	InitialDelay time.Duration
	MaxDelay     time.Duration
	Retryable    func(error) bool
}

type RetryingModel struct {
	model     ChatModel
	config    RetryConfig
	retryable func(error) bool
}

func NewRetryingModel(model ChatModel, config RetryConfig) (*RetryingModel, error) {
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

func (m *RetryingModel) Capabilities() Capabilities { return m.model.Capabilities() }
func (m *RetryingModel) ModelInfo() ModelInfo {
	info := DescribeModel(m.model)
	if info.Attributes == nil {
		info.Attributes = map[string]any{}
	}
	info.Attributes["retry_max_attempts"] = m.config.MaxAttempts
	return info
}

func (m *RetryingModel) Invoke(ctx context.Context, request Request, options ...runnable.Option) (Response, error) {
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

func (m *RetryingModel) Stream(ctx context.Context, request Request, options ...runnable.Option) (*runnable.StreamReader[Response], error) {
	if ctx == nil {
		return nil, errors.New("retrying model context cannot be nil")
	}
	reader, writer := runnable.Pipe[Response](context.WithoutCancel(ctx))
	executionCtx, cancel := context.WithCancel(ctx)
	go func() {
		select {
		case <-reader.Context().Done():
			cancel()
		case <-executionCtx.Done():
		}
	}()
	go func() {
		defer cancel()
		defer writer.Close()
		for attempt := 0; attempt < m.config.MaxAttempts; attempt++ {
			var started atomic.Bool
			attemptCtx := callback.WithStreamCallbacks(executionCtx, callback.StreamCallbackOf[Response]("retry-start", func(context.Context, Response) error {
				started.Store(true)
				return nil
			}))
			upstream, err := m.model.Stream(attemptCtx, request, options...)
			if err == nil && upstream != nil {
				value, recvErr := upstream.Recv()
				if recvErr == nil {
					started.Store(true)
					if sendErr := writer.Send(value.Clone()); sendErr != nil {
						_ = upstream.Close()
						return
					}
					if _, extraErr := upstream.Recv(); extraErr != nil && !errors.Is(extraErr, io.EOF) {
						_ = writer.CloseWithError(extraErr)
					}
					_ = upstream.Close()
					return
				}
				_ = upstream.Close()
				if errors.Is(recvErr, io.EOF) {
					err = ErrStreamTruncated
				} else {
					err = recvErr
				}
			} else if err == nil {
				err = errors.New("model returned a nil stream")
			}
			if started.Load() || attempt+1 >= m.config.MaxAttempts || !m.retryable(err) {
				_ = writer.CloseWithError(err)
				return
			}
			if err := waitRetry(executionCtx, retryDelay(m.config, attempt)); err != nil {
				_ = writer.CloseWithError(err)
				return
			}
		}
	}()
	return reader, nil
}

func (m *RetryingModel) Collect(ctx context.Context, input *runnable.StreamReader[Request], options ...runnable.Option) (Response, error) {
	value, err := input.Collect(ctx)
	if err != nil {
		return Response{}, err
	}
	return m.Invoke(ctx, value, options...)
}
func (m *RetryingModel) Transform(ctx context.Context, input *runnable.StreamReader[Request], options ...runnable.Option) (*runnable.StreamReader[Response], error) {
	value, err := input.Collect(ctx)
	if err != nil {
		return nil, err
	}
	return m.Stream(ctx, value, options...)
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

var _ ChatModel = (*RetryingModel)(nil)
