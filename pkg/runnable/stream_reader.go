package runnable

import (
	"context"
	"errors"
	"io"
	"sync"
)

var errNilPipeContext = errors.New("runnable pipe context is nil")

// ConcatFunc combines the next streamed value with the accumulated value.
type ConcatFunc[T any] func(accumulated, next T) (T, error)

type pipeConfig[T any] struct {
	concat ConcatFunc[T]
}

// PipeOption configures one StreamReader and StreamWriter pair.
type PipeOption[T any] func(*pipeConfig[T])

// WithConcat binds a domain-specific reduction strategy to a stream.
func WithConcat[T any](concat ConcatFunc[T]) PipeOption[T] {
	return func(config *pipeConfig[T]) {
		if concat != nil {
			config.concat = concat
		}
	}
}

type pipeState[T any] struct {
	ctx    context.Context
	cancel context.CancelFunc
	values chan T
	done   chan struct{}
	concat ConcatFunc[T]

	closeOnce sync.Once
	mu        sync.Mutex
	terminal  error
	delivered bool
}

// StreamReader is a single-consumer, cancellation-aware stream.
type StreamReader[T any] struct {
	state *pipeState[T]
}

// StreamWriter is the producer side of a StreamReader.
type StreamWriter[T any] struct {
	state *pipeState[T]
}

// Pipe creates an ordered, unbuffered stream with backpressure.
func Pipe[T any](ctx context.Context, options ...PipeOption[T]) (*StreamReader[T], *StreamWriter[T]) {
	config := pipeConfig[T]{concat: defaultConcat[T]()}
	for _, option := range options {
		if option != nil {
			option(&config)
		}
	}
	if ctx == nil {
		state := &pipeState[T]{values: make(chan T), done: make(chan struct{}), concat: config.concat}
		state.finish(errNilPipeContext)
		return &StreamReader[T]{state: state}, &StreamWriter[T]{state: state}
	}
	pipeCtx, cancel := context.WithCancel(ctx)
	state := &pipeState[T]{ctx: pipeCtx, cancel: cancel, values: make(chan T), done: make(chan struct{}), concat: config.concat}
	return &StreamReader[T]{state: state}, &StreamWriter[T]{state: state}
}

// Recv receives one value. A normal terminal state is reported as io.EOF.
func (r *StreamReader[T]) Recv() (T, error) {
	if r == nil || r.state == nil {
		var zero T
		return zero, errors.New("runnable stream reader is nil")
	}
	return r.recv(r.state.ctx)
}

// Collect consumes the stream using the Concat strategy bound when the Reader
// was created.
func (r *StreamReader[T]) Collect(ctx context.Context) (T, error) {
	return Concat(ctx, r)
}

// Context is canceled when either side closes or the parent Context ends.
func (r *StreamReader[T]) Context() context.Context {
	if r == nil || r.state == nil || r.state.ctx == nil {
		return context.Background()
	}
	return r.state.ctx
}

func (r *StreamReader[T]) recv(ctx context.Context) (T, error) {
	var zero T
	if r == nil || r.state == nil {
		return zero, errors.New("runnable stream reader is nil")
	}
	state := r.state
	if err, closed := state.terminalForReader(); closed {
		return zero, err
	}
	if ctx == nil {
		return zero, errNilPipeContext
	}
	select {
	case value := <-state.values:
		return value, nil
	case <-state.done:
		err, _ := state.terminalForReader()
		return zero, err
	case <-ctx.Done():
		state.finish(ctx.Err())
		err, _ := state.terminalForReader()
		return zero, err
	}
}

// Close releases the Reader and cancels a blocked or future producer.
func (r *StreamReader[T]) Close() error {
	if r == nil || r.state == nil {
		return nil
	}
	r.state.finish(context.Canceled)
	if r.state.cancel != nil {
		r.state.cancel()
	}
	return nil
}

// Send publishes one value and blocks until the consumer receives it.
func (w *StreamWriter[T]) Send(value T) error {
	if w == nil || w.state == nil {
		return errors.New("runnable stream writer is nil")
	}
	state := w.state
	if err, closed := state.terminalForWriter(); closed {
		return err
	}
	if state.ctx == nil {
		return errNilPipeContext
	}
	select {
	case state.values <- value:
		return nil
	case <-state.done:
		err, _ := state.terminalForWriter()
		return err
	case <-state.ctx.Done():
		state.finish(state.ctx.Err())
		return state.ctx.Err()
	}
}

// Close completes the stream normally.
func (w *StreamWriter[T]) Close() error {
	if w == nil || w.state == nil {
		return nil
	}
	w.state.finish(nil)
	return nil
}

// CloseWithError completes the stream with a terminal error.
func (w *StreamWriter[T]) CloseWithError(err error) error {
	if w == nil || w.state == nil {
		return nil
	}
	w.state.finish(err)
	return nil
}

func (s *pipeState[T]) finish(err error) {
	s.closeOnce.Do(func() {
		s.mu.Lock()
		s.terminal = err
		s.mu.Unlock()
		close(s.done)
	})
}

func (s *pipeState[T]) terminalForReader() (error, bool) {
	select {
	case <-s.done:
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.terminal != nil && !s.delivered {
			s.delivered = true
			return s.terminal, true
		}
		return io.EOF, true
	default:
		return nil, false
	}
}

func (s *pipeState[T]) terminalForWriter() (error, bool) {
	select {
	case <-s.done:
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.terminal != nil {
			return s.terminal, true
		}
		return io.ErrClosedPipe, true
	default:
		return nil, false
	}
}
