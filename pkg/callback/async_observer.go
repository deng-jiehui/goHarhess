package callback

import "context"

// Observer is an explicitly asynchronous event consumer. Agent construction
// never creates an AsyncObserverManager; applications own its lifecycle.
type Observer interface {
	Observe(context.Context, Event)
}

// ObserverFunc adapts a function to Observer.
type ObserverFunc func(context.Context, Event)

func (f ObserverFunc) Observe(ctx context.Context, event Event) {
	if f != nil {
		f(ctx, event)
	}
}

// AsyncObserverManager starts one application-owned goroutine per observer
// and event. It deliberately provides no queue, retry, flush or loss policy.
type AsyncObserverManager struct {
	observers []Observer
}

// NewAsyncObserverManager creates an explicit asynchronous observer helper.
func NewAsyncObserverManager(observers ...Observer) *AsyncObserverManager {
	return &AsyncObserverManager{observers: append([]Observer(nil), observers...)}
}

// Publish clones the event for each observer and returns immediately. The
// caller owns buffering, shutdown, retry and cancellation decisions.
func (m *AsyncObserverManager) Publish(ctx context.Context, event Event) {
	if m == nil {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	for _, observer := range m.observers {
		if observer == nil {
			continue
		}
		snapshot := event.Clone()
		go func(observer Observer, snapshot Event) {
			defer func() { _ = recover() }()
			observer.Observe(ctx, snapshot)
		}(observer, snapshot)
	}
}
