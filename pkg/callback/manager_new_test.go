package callback_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/goharness/goharness/pkg/callback"
)

type auditCallback interface {
	callback.Handler
	OnAudit(context.Context, string) error
}

type otherCallback interface {
	callback.Handler
	OnOther(context.Context) error
}

type auditFuncs struct {
	name    string
	onAudit func(context.Context, string) error
	onOther func(context.Context) error
}

func (f auditFuncs) Name() string { return f.name }
func (f auditFuncs) OnAudit(ctx context.Context, value string) error {
	return f.onAudit(ctx, value)
}
func (f auditFuncs) OnOther(ctx context.Context) error { return f.onOther(ctx) }

func TestManagerDispatchSelectsTypedHandlersInOrder(t *testing.T) {
	var calls []string
	first := auditFuncs{name: "first", onAudit: func(context.Context, string) error {
		calls = append(calls, "first")
		return nil
	}}
	unrelated := otherOnly{name: "other", call: func(context.Context) error {
		t.Fatal("unrelated handler was called")
		return nil
	}}
	second := auditFuncs{name: "second", onAudit: func(context.Context, string) error {
		calls = append(calls, "second")
		return nil
	}}
	manager, err := callback.NewManager(callback.Config{}, []callback.Handler{first, unrelated, second})
	if err != nil {
		t.Fatal(err)
	}
	ctx := callback.WithManager(context.Background(), manager)
	err = callback.Dispatch[auditCallback](ctx, callback.Invocation{Component: "test", Operation: "audit", Phase: callback.PhaseStart}, func(ctx context.Context, handler auditCallback) error {
		return handler.OnAudit(ctx, "value")
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(calls) != 2 || calls[0] != "first" || calls[1] != "second" {
		t.Fatalf("calls = %#v", calls)
	}
}

type otherOnly struct {
	name string
	call func(context.Context) error
}

func (o otherOnly) Name() string                      { return o.name }
func (o otherOnly) OnOther(ctx context.Context) error { return o.call(ctx) }

func TestManagerIsolatesFailuresAndReportsErrorSink(t *testing.T) {
	sentinel := errors.New("audit failed")
	var calls []string
	var reported []callback.CallbackError
	handlers := []callback.Handler{
		auditFuncs{name: "error", onAudit: func(context.Context, string) error {
			calls = append(calls, "error")
			return sentinel
		}},
		auditFuncs{name: "panic", onAudit: func(context.Context, string) error {
			calls = append(calls, "panic")
			panic("broken")
		}},
		auditFuncs{name: "last", onAudit: func(context.Context, string) error {
			calls = append(calls, "last")
			return nil
		}},
	}
	manager, err := callback.NewManager(callback.Config{}, handlers, callback.ErrorSinkFunc(func(_ context.Context, failure callback.CallbackError) {
		reported = append(reported, failure)
	}))
	if err != nil {
		t.Fatal(err)
	}
	ctx := callback.WithManager(context.Background(), manager)
	dispatchErr := callback.Dispatch[auditCallback](ctx, callback.Invocation{Component: "test", Operation: "audit", Phase: callback.PhaseError}, func(ctx context.Context, handler auditCallback) error {
		return handler.OnAudit(ctx, "value")
	})
	if !errors.Is(dispatchErr, sentinel) || len(calls) != 3 || len(reported) != 2 {
		t.Fatalf("Dispatch() = %v; calls=%#v reported=%#v", dispatchErr, calls, reported)
	}
	if reported[0].Handler != "error" || reported[0].Invocation.Operation != "audit" {
		t.Fatalf("first reported failure = %#v", reported[0])
	}
	var panicErr *callback.PanicError
	if !errors.As(reported[1].Err, &panicErr) {
		t.Fatalf("panic failure = %v", reported[1].Err)
	}
}

func TestManagerDistinguishesOwnTimeoutFromParentCancellation(t *testing.T) {
	handler := auditFuncs{name: "slow", onAudit: func(ctx context.Context, _ string) error {
		<-ctx.Done()
		return ctx.Err()
	}}
	manager, err := callback.NewManager(callback.Config{Timeout: 10 * time.Millisecond}, []callback.Handler{handler})
	if err != nil {
		t.Fatal(err)
	}
	err = callback.Dispatch[auditCallback](callback.WithManager(context.Background(), manager), callback.Invocation{}, func(ctx context.Context, handler auditCallback) error {
		return handler.OnAudit(ctx, "value")
	})
	var timeoutErr *callback.TimeoutError
	if !errors.As(err, &timeoutErr) {
		t.Fatalf("manager timeout error = %v", err)
	}

	parent, cancel := context.WithCancel(context.Background())
	cancel()
	err = callback.Dispatch[auditCallback](callback.WithManager(parent, manager), callback.Invocation{}, func(ctx context.Context, handler auditCallback) error {
		return handler.OnAudit(ctx, "value")
	})
	if !errors.Is(err, context.Canceled) || errors.As(err, &timeoutErr) {
		t.Fatalf("parent cancellation error = %v", err)
	}
}

func TestManagerDispatchIsSafeForConcurrentUse(t *testing.T) {
	var mu sync.Mutex
	calls := 0
	handler := auditFuncs{name: "count", onAudit: func(context.Context, string) error {
		mu.Lock()
		calls++
		mu.Unlock()
		return nil
	}}
	manager, err := callback.NewManager(callback.Config{}, []callback.Handler{handler})
	if err != nil {
		t.Fatal(err)
	}
	ctx := callback.WithManager(context.Background(), manager)
	var wait sync.WaitGroup
	for range 32 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			_ = callback.Dispatch[auditCallback](ctx, callback.Invocation{}, func(ctx context.Context, handler auditCallback) error {
				return handler.OnAudit(ctx, "value")
			})
		}()
	}
	wait.Wait()
	if calls != 32 {
		t.Fatalf("calls = %d", calls)
	}
}
