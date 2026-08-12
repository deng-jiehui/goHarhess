package callback

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"
)

const maxTimeout = 5 * time.Minute

var errHandlerTimeout = errors.New("callback Handler timeout")

// Config controls synchronous callback execution.
type Config struct {
	Timeout      time.Duration
	FilterChunks bool
}

type handlerEntry struct {
	name  string
	value Handler
}

// SyncDispatcher invokes handlers inline in the caller's goroutine. It has no
// queue, worker or flush lifecycle, so accepted events cannot be dropped.
type SyncDispatcher struct {
	handlers   []handlerEntry
	errorSinks []ErrorSink
	parent     *SyncDispatcher
	config     Config
	sequence   *atomic.Uint64
}

// New validates handlers and creates a synchronous dispatcher.
func New(handlers []Handler, config Config, errorSinks ...ErrorSink) (*SyncDispatcher, error) {
	if config.Timeout < 0 || config.Timeout > maxTimeout {
		return nil, fmt.Errorf("callback timeout must be zero or at most %s: %s", maxTimeout, config.Timeout)
	}
	dispatcher := &SyncDispatcher{config: config, sequence: &atomic.Uint64{}}
	for index, value := range handlers {
		if value == nil {
			return nil, fmt.Errorf("callback at index %d cannot be nil", index)
		}
		name, err := readName(value, index)
		if err != nil {
			return nil, err
		}
		dispatcher.handlers = append(dispatcher.handlers, handlerEntry{name: name, value: value})
	}
	for index, sink := range errorSinks {
		if sink == nil {
			return nil, fmt.Errorf("callback error sink at index %d cannot be nil", index)
		}
		dispatcher.errorSinks = append(dispatcher.errorSinks, sink)
	}
	return dispatcher, nil
}

// NewChild creates a local synchronous callback scope. Accepted events are
// forwarded to parent before local handlers execute.
func NewChild(parent *SyncDispatcher, handlers []Handler, errorSinks ...ErrorSink) (*SyncDispatcher, error) {
	if parent == nil {
		return New(handlers, Config{}, errorSinks...)
	}
	inherited := append([]ErrorSink(nil), parent.errorSinks...)
	inherited = append(inherited, errorSinks...)
	child, err := New(handlers, parent.config, inherited...)
	if err != nil {
		return nil, err
	}
	child.parent = parent
	child.sequence = parent.sequence
	return child, nil
}

// Dispatch synchronously invokes every handler in registration order. Handler
// failures are aggregated and returned; callers inside framework business
// paths deliberately ignore this error so observation cannot alter results.
func (d *SyncDispatcher) Dispatch(ctx context.Context, event Event) error {
	if d == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	var failures []error
	if d.parent != nil {
		if err := d.parent.Dispatch(ctx, event); err != nil {
			failures = append(failures, err)
		}
	}
	if d.config.FilterChunks && isChunkEvent(event.Type) {
		return errors.Join(failures...)
	}
	if len(d.handlers) == 0 {
		return errors.Join(failures...)
	}
	event.Sequence = d.sequence.Add(1)
	for _, entry := range d.handlers {
		handlerEvent := event.Clone()
		handlerCtx := ctx
		var cancel context.CancelFunc
		if d.config.Timeout > 0 {
			handlerCtx, cancel = context.WithTimeoutCause(ctx, d.config.Timeout, errHandlerTimeout)
		}
		err, panicked := invoke(entry, handlerCtx, handlerEvent)
		timedOut := d.config.Timeout > 0 && errors.Is(context.Cause(handlerCtx), errHandlerTimeout)
		if cancel != nil {
			cancel()
		}
		if timedOut {
			timeoutErr := &TimeoutError{Callback: entry.name, Timeout: d.config.Timeout}
			if err == nil {
				err = timeoutErr
			} else if !panicked {
				err = errors.Join(err, timeoutErr)
			}
		}
		if err != nil {
			callbackErr := CallbackError{Callback: entry.name, Event: handlerEvent.Clone(), Err: err}
			failures = append(failures, callbackErr)
			d.report(ctx, callbackErr)
		}
	}
	return errors.Join(failures...)
}

func (d *SyncDispatcher) report(ctx context.Context, callbackErr CallbackError) {
	for _, sink := range d.errorSinks {
		func() {
			defer func() { _ = recover() }()
			sink.OnError(ctx, callbackErr)
		}()
	}
}

func isChunkEvent(eventType EventType) bool {
	return eventType == EventAgentChunk || eventType == EventLLMChunk
}

func invoke(entry handlerEntry, ctx context.Context, event Event) (err error, panicked bool) {
	defer func() {
		if recovered := recover(); recovered != nil {
			panicked = true
			err = &PanicError{Callback: entry.name, Value: recovered}
		}
	}()
	return entry.value.Handle(ctx, event), false
}

func readName(value Handler, index int) (name string, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("read callback name at index %d: %v", index, recovered)
		}
	}()
	if named, ok := value.(NamedHandler); ok {
		if name = strings.TrimSpace(named.Name()); name != "" {
			return name, nil
		}
	}
	return fmt.Sprintf("callback[%d]", index), nil
}
