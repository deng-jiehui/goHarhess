package llm

import (
	"context"
	"errors"
	"io"
	"net"
	"reflect"
	"sync/atomic"
	"time"

	"github.com/goharness/goharness/pkg/runnable"
	"github.com/goharness/goharness/pkg/stream"
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

// UnwrapModel 返回被包装的下层模型，供策略链检查与重组使用。
func (m *RetryingModel) UnwrapModel() ChatModel { return m.model }

// Policy 返回生效中的重试配置，Retryable 回填为实际使用的判定函数。
func (m *RetryingModel) Policy() RetryConfig {
	config := m.config
	config.Retryable = m.retryable
	return config
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
	forward, err := m.forwardOptions(options)
	if err != nil {
		return Response{}, err
	}
	var last error
	for attempt := 0; attempt < m.config.MaxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return Response{}, err
		}
		response, err := m.model.Invoke(ctx, request, forward...)
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
	forward, err := m.forwardOptions(options)
	if err != nil {
		return nil, err
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
			attemptCtx := withStreamProduced(executionCtx, func() {
				started.Store(true)
			})
			attemptCtx, published := stream.TrackMessages(attemptCtx)
			upstream, err := m.model.Stream(attemptCtx, request, forward...)
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
			if started.Load() || published.Load() || attempt+1 >= m.config.MaxAttempts || !m.retryable(err) {
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

// forwardOptions 在进入重试循环时冻结整份有效配置：每种组件配置类型只
// 解析一次并包装为快照选项，重试与下游收到的都是冻结结果，原始设置函数
// 不会因重放或转发重复执行。
func (m *RetryingModel) forwardOptions(options []runnable.Option) ([]runnable.Option, error) {
	return runnable.FreezeOptions(options)
}

// OptionTypes 透传下游模型声明的扩展配置类型。
func (m *RetryingModel) OptionTypes() []reflect.Type {
	if m == nil {
		return nil
	}
	if provider, ok := m.model.(optionTypesProvider); ok && provider != nil {
		return provider.OptionTypes()
	}
	return nil
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
