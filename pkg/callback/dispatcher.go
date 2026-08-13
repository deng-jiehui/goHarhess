package callback

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"
)

const maxTimeout = 5 * time.Minute

var errHandlerTimeout = errors.New("callback Handler timeout")

type Config struct {
	Timeout time.Duration
}

type handlerEntry struct {
	name  string
	value Handler
}

// Manager owns immutable Handler and ErrorSink registrations.
type Manager struct {
	handlers   []handlerEntry
	errorSinks []ErrorSink
	config     Config
}

func NewManager(config Config, handlers []Handler, errorSinks ...ErrorSink) (*Manager, error) {
	if config.Timeout < 0 || config.Timeout > maxTimeout {
		return nil, fmt.Errorf("callback timeout must be zero or at most %s: %s", maxTimeout, config.Timeout)
	}
	manager := &Manager{config: config}
	for index, value := range handlers {
		if isNil(value) {
			return nil, fmt.Errorf("callback at index %d cannot be nil", index)
		}
		name, err := handlerName(value, index)
		if err != nil {
			return nil, err
		}
		manager.handlers = append(manager.handlers, handlerEntry{name: name, value: value})
	}
	for index, sink := range errorSinks {
		if isNil(sink) {
			return nil, fmt.Errorf("callback error sink at index %d cannot be nil", index)
		}
		manager.errorSinks = append(manager.errorSinks, sink)
	}
	return manager, nil
}

// Dispatch invokes registered Handlers that implement T in registration order.
func Dispatch[T Handler](ctx context.Context, invocation Invocation, invoke func(context.Context, T) error) error {
	if ctx == nil {
		ctx = context.Background()
	}
	manager, ok := ManagerFromContext(ctx)
	if !ok || invoke == nil {
		return nil
	}
	var failures []error
	for _, entry := range manager.handlers {
		typed, matches := entry.value.(T)
		if !matches {
			continue
		}
		callInvocation := invocation.Clone()
		callCtx := withInvocation(ctx, callInvocation)
		err := manager.invoke(callCtx, entry.name, callInvocation, func(current context.Context) error {
			return invoke(current, typed)
		})
		if err != nil {
			failures = append(failures, err)
		}
	}
	return errors.Join(failures...)
}

func (m *Manager) invoke(ctx context.Context, name string, invocation Invocation, invoke func(context.Context) error) error {
	callCtx := ctx
	var cancel context.CancelFunc
	if m != nil && m.config.Timeout > 0 {
		callCtx, cancel = context.WithTimeoutCause(ctx, m.config.Timeout, errHandlerTimeout)
	}
	err, panicked := invokeIsolated(name, callCtx, invoke)
	timedOut := m != nil && m.config.Timeout > 0 && errors.Is(context.Cause(callCtx), errHandlerTimeout)
	if cancel != nil {
		cancel()
	}
	if timedOut {
		timeoutErr := &TimeoutError{Handler: name, Timeout: m.config.Timeout}
		if err == nil {
			err = timeoutErr
		} else if !panicked {
			err = errors.Join(err, timeoutErr)
		}
	}
	if err == nil {
		return nil
	}
	failure := CallbackError{Handler: name, Invocation: invocation.Clone(), Err: err}
	m.report(ctx, failure)
	return failure
}

func (m *Manager) report(ctx context.Context, failure CallbackError) {
	if m == nil {
		return
	}
	for _, sink := range m.errorSinks {
		func() {
			defer func() { _ = recover() }()
			sink.OnError(ctx, failure)
		}()
	}
}

func invokeIsolated(name string, ctx context.Context, invoke func(context.Context) error) (err error, panicked bool) {
	defer func() {
		if recovered := recover(); recovered != nil {
			panicked = true
			err = &PanicError{Handler: name, Value: recovered}
		}
	}()
	return invoke(ctx), false
}

func handlerName(value Handler, index int) (name string, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("read callback name at index %d: %v", index, recovered)
		}
	}()
	name = strings.TrimSpace(value.Name())
	if name == "" {
		return "", fmt.Errorf("callback at index %d has an empty name", index)
	}
	return name, nil
}

func isNil(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}
