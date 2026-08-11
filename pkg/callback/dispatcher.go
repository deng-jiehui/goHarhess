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
	handlers []handlerEntry
	parent   *SyncDispatcher
	config   Config
	sequence atomic.Uint64
}

// New validates handlers and creates a synchronous dispatcher.
func New(handlers []Handler, config Config) (*SyncDispatcher, error) {
	if config.Timeout < 0 || config.Timeout > maxTimeout {
		return nil, fmt.Errorf("callback timeout must be zero or at most %s: %s", maxTimeout, config.Timeout)
	}
	dispatcher := &SyncDispatcher{config: config}
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
	return dispatcher, nil
}

// NewChild creates a local synchronous callback scope. Accepted events are
// forwarded to parent before local handlers execute.
func NewChild(parent *SyncDispatcher, handlers []Handler) (*SyncDispatcher, error) {
	if parent == nil {
		return New(handlers, Config{})
	}
	child, err := New(handlers, parent.config)
	if err != nil {
		return nil, err
	}
	child.parent = parent
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
			handlerCtx, cancel = context.WithTimeout(ctx, d.config.Timeout)
		}
		err, panicked := invoke(entry, handlerCtx, handlerEvent)
		timedOut := d.config.Timeout > 0 && errors.Is(handlerCtx.Err(), context.DeadlineExceeded)
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
			failures = append(failures, CallbackError{Callback: entry.name, Event: handlerEvent.Clone(), Err: err})
		}
	}
	return errors.Join(failures...)
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
