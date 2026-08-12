package toolruntime

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"goharness/pkg/runnable"
	"goharness/pkg/tool"
)

type traceKey struct{}

func TestRuntimeOrdersMiddlewareAndToolAndPreservesContext(t *testing.T) {
	var order []string
	middleware := tool.MiddlewareFuncs{
		MiddlewareName: "masking",
		HandleFunc: func(ctx context.Context, spec tool.Spec, input tool.CallInput, next tool.Next) (tool.Result, error) {
			order = append(order, "middleware")
			if ctx.Value(traceKey{}) != "trace-1" {
				t.Fatal("caller context value did not reach middleware")
			}
			input.Args["masked"] = true
			return next(ctx, input)
		},
	}
	runtime := mustRuntime(t, []tool.Middleware{middleware})
	value := tool.NewFunctionTool(tool.Spec{Name: "read", IsReadOnly: true}, func(ctx context.Context, input tool.CallInput) (tool.Result, error) {
		order = append(order, "tool")
		if input.Args["masked"] != true || ctx.Value(traceKey{}) != "trace-1" {
			t.Fatalf("tool input = %#v, trace = %v", input.Args, ctx.Value(traceKey{}))
		}
		if _, err := tool.RequireSubject(ctx); err != nil {
			t.Fatalf("caller Subject was stripped: %v", err)
		}
		return tool.TextResult("ok"), nil
	})
	binding := registerTool(t, runtime, value)

	runCtx := context.WithValue(context.Background(), traceKey{}, "trace-1")
	runCtx = tool.WithSubject(runCtx, tool.Subject{ID: "user-1"})
	result, err := runtime.NewScope(1).Execute(
		runCtx,
		binding,
		tool.CallInput{Args: map[string]any{}},
	)
	if err != nil {
		t.Fatal(err)
	}
	content, err := result.ToContent()
	if err != nil || content.String() != "ok" {
		t.Fatalf("result = %#v, err = %v", result, err)
	}
	want := []string{"middleware", "tool"}
	if !slices.Equal(order, want) {
		t.Fatalf("order = %#v, want %#v", order, want)
	}
}

func TestRuntimeLabelsMiddlewareDenialAndSkipsTool(t *testing.T) {
	reason := errors.New("viewer cannot mutate")
	middleware := tool.MiddlewareFuncs{
		MiddlewareName: "rbac",
		HandleFunc: func(ctx context.Context, spec tool.Spec, input tool.CallInput, next tool.Next) (tool.Result, error) {
			return tool.Result{}, tool.Deny(reason)
		},
	}
	runtime := mustRuntime(t, []tool.Middleware{middleware})
	var calls atomic.Int32
	value := tool.NewFunctionTool(tool.Spec{Name: "delete"}, func(context.Context, tool.CallInput) (tool.Result, error) {
		calls.Add(1)
		return tool.TextResult("deleted"), nil
	})
	binding := registerTool(t, runtime, value)

	_, err := runtime.NewScope(1).Execute(context.Background(), binding, tool.CallInput{})
	if !errors.Is(err, tool.ErrPermissionDenied) || !errors.Is(err, reason) {
		t.Fatalf("Execute error = %v", err)
	}
	if !strings.Contains(err.Error(), `middleware "rbac"`) {
		t.Fatalf("Execute error = %q", err)
	}
	if calls.Load() != 0 {
		t.Fatalf("tool calls = %d, want 0", calls.Load())
	}
}

func TestRuntimeLabelsMiddlewareErrorReturnedAfterNext(t *testing.T) {
	reason := errors.New("billing commit failed")
	middleware := tool.MiddlewareFuncs{
		MiddlewareName: "billing",
		HandleFunc: func(ctx context.Context, spec tool.Spec, input tool.CallInput, next tool.Next) (tool.Result, error) {
			if _, err := next(ctx, input); err != nil {
				return tool.Result{}, err
			}
			return tool.Result{}, reason
		},
	}
	runtime := mustRuntime(t, []tool.Middleware{middleware})
	value := tool.NewFunctionTool(tool.Spec{Name: "charge"}, func(context.Context, tool.CallInput) (tool.Result, error) {
		return tool.TextResult("charged"), nil
	})
	binding := registerTool(t, runtime, value)

	_, err := runtime.NewScope(1).Execute(context.Background(), binding, tool.CallInput{})
	var extensionErr *tool.ExtensionError
	if !errors.As(err, &extensionErr) || extensionErr.Name != "billing" || !errors.Is(err, reason) {
		t.Fatalf("Execute error = %#v", err)
	}
}

func TestRuntimeRecoversMiddlewareAndToolPanics(t *testing.T) {
	tests := []struct {
		name        string
		middlewares []tool.Middleware
		handler     tool.Handler
		component   string
	}{
		{
			name: "middleware",
			middlewares: []tool.Middleware{tool.MiddlewareFuncs{
				MiddlewareName: "broken",
				HandleFunc: func(context.Context, tool.Spec, tool.CallInput, tool.Next) (tool.Result, error) {
					panic("middleware panic")
				},
			}},
			handler:   func(context.Context, tool.CallInput) (tool.Result, error) { return tool.TextResult("unused"), nil },
			component: `middleware "broken"`,
		},
		{
			name:      "tool",
			handler:   func(context.Context, tool.CallInput) (tool.Result, error) { panic("tool panic") },
			component: `tool "run"`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			runtime := mustRuntime(t, test.middlewares)
			value := tool.NewFunctionTool(tool.Spec{Name: "run"}, test.handler)
			binding := registerTool(t, runtime, value)
			_, err := runtime.NewScope(1).Execute(context.Background(), binding, tool.CallInput{})
			if !errors.Is(err, tool.ErrExecutionPanic) || !strings.Contains(err.Error(), test.component) {
				t.Fatalf("Execute error = %v", err)
			}
		})
	}
}

func TestRuntimeCancellationPreemptsUncooperativeTool(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	runtime := mustRuntime(t, nil)
	value := tool.NewFunctionTool(tool.Spec{Name: "slow"}, func(context.Context, tool.CallInput) (tool.Result, error) {
		close(started)
		<-release
		return tool.TextResult("late"), nil
	})
	binding := registerTool(t, runtime, value)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := runtime.NewScope(1).Execute(ctx, binding, tool.CallInput{})
		done <- err
	}()
	waitSignal(t, started, "tool start")
	cancel()
	if err := waitError(t, done); !errors.Is(err, context.Canceled) {
		t.Fatalf("Execute error = %v, want context canceled", err)
	}
	close(release)
}

func TestConfiguredTimeoutControlsToolExecution(t *testing.T) {
	config := defaultConfig()
	config.Timeout = 25 * time.Millisecond
	runtime, err := New(nil, config)
	if err != nil {
		t.Fatal(err)
	}
	value := tool.NewFunctionTool(tool.Spec{Name: "configured-timeout"}, func(ctx context.Context, input tool.CallInput) (tool.Result, error) {
		<-ctx.Done()
		return tool.Result{}, ctx.Err()
	})
	binding := registerTool(t, runtime, value)
	startedAt := time.Now()

	_, err = runtime.NewScope(1, time.Second).Execute(context.Background(), binding, tool.CallInput{})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Execute error = %v, want deadline exceeded", err)
	}
	if elapsed := time.Since(startedAt); elapsed > 300*time.Millisecond {
		t.Fatalf("configured timeout took too long: %s", elapsed)
	}
}

func TestEffectiveTimeoutEnforcesSystemCap(t *testing.T) {
	tests := []struct {
		name       string
		configured time.Duration
		want       time.Duration
	}{
		{name: "configured", configured: 5 * time.Second, want: 5 * time.Second},
		{name: "system cap", configured: 10 * time.Minute, want: maxExecutionDuration},
		{name: "invalid fallback", want: maxExecutionDuration},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := effectiveTimeout(test.configured); got != test.want {
				t.Fatalf("effectiveTimeout = %s, want %s", got, test.want)
			}
		})
	}
}

func TestRuntimeRetriesOnlyReadOnlyTools(t *testing.T) {
	config := defaultConfig()
	config.MaxRetries = 2
	config.RetryBackoff = 0
	runtime, err := New(nil, config)
	if err != nil {
		t.Fatal(err)
	}
	transient := errors.New("transient failure")
	var readCalls atomic.Int32
	readTool := tool.NewFunctionTool(tool.Spec{Name: "read", IsReadOnly: true}, func(context.Context, tool.CallInput) (tool.Result, error) {
		if readCalls.Add(1) < 3 {
			return tool.Result{}, transient
		}
		return tool.TextResult("ok"), nil
	})
	readBinding := registerTool(t, runtime, readTool)
	if _, err := runtime.NewScope(1).Execute(context.Background(), readBinding, tool.CallInput{}); err != nil {
		t.Fatal(err)
	}
	if readCalls.Load() != 3 {
		t.Fatalf("read tool calls = %d, want 3", readCalls.Load())
	}

	var writeCalls atomic.Int32
	writeTool := tool.NewFunctionTool(tool.Spec{Name: "write"}, func(context.Context, tool.CallInput) (tool.Result, error) {
		writeCalls.Add(1)
		return tool.Result{}, transient
	})
	writeBinding := registerTool(t, runtime, writeTool)
	_, err = runtime.NewScope(1).Execute(context.Background(), writeBinding, tool.CallInput{})
	if !errors.Is(err, transient) || writeCalls.Load() != 1 {
		t.Fatalf("write calls = %d, error = %v", writeCalls.Load(), err)
	}
}

func TestRuntimeRejectsDepthBeyondSystemLimit(t *testing.T) {
	runtime := mustRuntime(t, nil)
	value := tool.NewFunctionTool(tool.Spec{Name: "recursive"}, nil)
	binding := registerTool(t, runtime, value)
	ctx := context.WithValue(context.Background(), executionStateKey{}, executionState{depth: maxExecutionDepth})

	_, err := runtime.NewScope(1).Execute(ctx, binding, tool.CallInput{})
	if !errors.Is(err, tool.ErrMaxDepthExceeded) {
		t.Fatalf("Execute error = %v", err)
	}
	var depthErr *tool.MaxDepthError
	if !errors.As(err, &depthErr) || depthErr.Depth != maxExecutionDepth+1 {
		t.Fatalf("depth error = %#v", depthErr)
	}
}

func TestRuntimeRejectsDeadlockRiskAndMiddlewareCannotSwallowIt(t *testing.T) {
	middleware := tool.MiddlewareFuncs{
		MiddlewareName: "unsafe-fallback",
		HandleFunc: func(ctx context.Context, spec tool.Spec, input tool.CallInput, next tool.Next) (tool.Result, error) {
			_, _ = next(context.Background(), input)
			return tool.TextResult("pretend-success"), nil
		},
	}
	runtime := mustRuntime(t, []tool.Middleware{middleware})
	value := tool.NewFunctionTool(tool.Spec{
		Name: "nested",
		Concurrency: tool.ConcurrencyPolicy{
			Group:       "network",
			MaxParallel: 1,
		},
	}, nil)
	binding := registerTool(t, runtime, value)
	ctx := context.WithValue(context.Background(), executionStateKey{}, executionState{heldGroups: []string{"network"}})

	_, err := runtime.NewScope(1).Execute(ctx, binding, tool.CallInput{})
	if !errors.Is(err, tool.ErrDeadlockRisk) {
		t.Fatalf("Execute error = %v", err)
	}
}

func TestRuntimeSemaphoreLimitsEveryMiddlewareNextCall(t *testing.T) {
	started := make(chan struct{}, 2)
	release := make(chan struct{})
	middleware := tool.MiddlewareFuncs{
		MiddlewareName: "parallel-retry",
		HandleFunc: func(ctx context.Context, spec tool.Spec, input tool.CallInput, next tool.Next) (tool.Result, error) {
			var wait sync.WaitGroup
			wait.Add(2)
			for range 2 {
				go func() {
					defer wait.Done()
					_, _ = next(ctx, input)
				}()
			}
			wait.Wait()
			return tool.TextResult("done"), nil
		},
	}
	runtime := mustRuntime(t, []tool.Middleware{middleware})
	value := tool.NewFunctionTool(tool.Spec{
		Name: "network",
		Concurrency: tool.ConcurrencyPolicy{
			Group:       "network",
			MaxParallel: 2,
		},
	}, func(context.Context, tool.CallInput) (tool.Result, error) {
		started <- struct{}{}
		<-release
		return tool.TextResult("ok"), nil
	})
	binding := registerTool(t, runtime, value)
	done := make(chan error, 1)
	go func() {
		_, err := runtime.NewScope(1).Execute(context.Background(), binding, tool.CallInput{})
		done <- err
	}()
	waitSignal(t, started, "first next call")
	assertNoSignal(t, started)
	close(release)
	if err := waitError(t, done); err != nil {
		t.Fatal(err)
	}
	waitSignal(t, started, "second next call")
}

func TestRuntimeWaitsForNextStartedBeforeMiddlewareReturns(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	middleware := tool.MiddlewareFuncs{
		MiddlewareName: "async-next",
		HandleFunc: func(ctx context.Context, spec tool.Spec, input tool.CallInput, next tool.Next) (tool.Result, error) {
			go func() {
				_, _ = next(ctx, input)
			}()
			<-started
			return tool.TextResult("middleware-returned"), nil
		},
	}
	runtime := mustRuntime(t, []tool.Middleware{middleware})
	value := tool.NewFunctionTool(tool.Spec{Name: "async"}, func(context.Context, tool.CallInput) (tool.Result, error) {
		close(started)
		<-release
		return tool.TextResult("tool-returned"), nil
	})
	binding := registerTool(t, runtime, value)
	done := make(chan error, 1)
	go func() {
		_, err := runtime.NewScope(1).Execute(context.Background(), binding, tool.CallInput{})
		done <- err
	}()
	waitSignal(t, started, "async next tool start")
	select {
	case err := <-done:
		t.Fatalf("runtime returned before async next completed: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	if err := waitError(t, done); err != nil {
		t.Fatal(err)
	}
}

func TestRuntimeRejectsConflictingConcurrencyDeclarations(t *testing.T) {
	runtime := mustRuntime(t, nil)
	if _, err := runtime.Register(tool.NewFunctionTool(tool.Spec{
		Name: "first", Concurrency: tool.ConcurrencyPolicy{Group: "browser", MaxParallel: 1},
	}, nil)); err != nil {
		t.Fatal(err)
	}
	_, err := runtime.Register(tool.NewFunctionTool(tool.Spec{
		Name: "second", Concurrency: tool.ConcurrencyPolicy{Group: "browser", MaxParallel: 2},
	}, nil))
	if err == nil || !strings.Contains(err.Error(), "already has max parallel 1") {
		t.Fatalf("Register error = %v", err)
	}
}

func TestRuntimeRejectsConcurrencyAboveSystemLimit(t *testing.T) {
	runtime := mustRuntime(t, nil)
	_, err := runtime.Register(tool.NewFunctionTool(tool.Spec{
		Name: "unbounded",
		Concurrency: tool.ConcurrencyPolicy{
			Group:       "network",
			MaxParallel: maxGroupParallelism + 1,
		},
	}, nil))
	if err == nil || !strings.Contains(err.Error(), "exceeds system limit") {
		t.Fatalf("Register error = %v", err)
	}
}

func TestRuntimeRecoversSpecPanicDuringRegistration(t *testing.T) {
	value := &customTool{
		specFunc: func() tool.Spec {
			panic("spec panic")
		},
	}
	runtime := mustRuntime(t, nil)

	_, err := runtime.Register(value)
	if !errors.Is(err, tool.ErrExecutionPanic) || !strings.Contains(err.Error(), "tool Spec") {
		t.Fatalf("Register error = %v", err)
	}
}

func TestRuntimeRecoversExtensionNamePanicDuringConfiguration(t *testing.T) {
	_, err := New([]tool.Middleware{panickingNameMiddleware{}})
	if !errors.Is(err, tool.ErrExecutionPanic) || !strings.Contains(err.Error(), "middleware Name") {
		t.Fatalf("New error = %v", err)
	}
}

func TestRuntimeUsesFrozenSpecSnapshot(t *testing.T) {
	var specCalls atomic.Int32
	value := &customTool{
		specFunc: func() tool.Spec {
			if specCalls.Add(1) > 1 {
				panic("Spec called after registration")
			}
			return tool.Spec{Name: "stable", IsReadOnly: true}
		},
		callFunc: func(context.Context, tool.CallInput) (tool.Result, error) {
			return tool.TextResult("ok"), nil
		},
	}
	runtime := mustRuntime(t, nil)
	binding := registerTool(t, runtime, value)

	_, err := runtime.NewScope(1).Execute(context.Background(), binding, tool.CallInput{})
	if err != nil {
		t.Fatal(err)
	}
	if specCalls.Load() != 1 || binding.Spec().Name != "stable" {
		t.Fatalf("Spec calls = %d, snapshot = %#v", specCalls.Load(), binding.Spec())
	}
}

func TestNewScopeHonorsConfiguredAndSystemConcurrencyLimits(t *testing.T) {
	runtime := mustRuntime(t, nil)
	if capacity := cap(runtime.NewScope(maxBatchParallelism + 100).semaphore); capacity != defaultConfig().MaxConcurrency {
		t.Fatalf("default scope capacity = %d, want %d", capacity, defaultConfig().MaxConcurrency)
	}

	config := defaultConfig()
	config.MaxConcurrency = maxBatchParallelism
	runtime, err := New(nil, config)
	if err != nil {
		t.Fatal(err)
	}
	if capacity := cap(runtime.NewScope(maxBatchParallelism + 100).semaphore); capacity != maxBatchParallelism {
		t.Fatalf("configured scope capacity = %d, want %d", capacity, maxBatchParallelism)
	}
}

func mustRuntime(t *testing.T, middlewares []tool.Middleware) *Runtime {
	t.Helper()
	runtime, err := New(middlewares)
	if err != nil {
		t.Fatal(err)
	}
	return runtime
}

func registerTool(t *testing.T, runtime *Runtime, value tool.Tool) *Binding {
	t.Helper()
	binding, err := runtime.Register(value)
	if err != nil {
		t.Fatal(err)
	}
	return binding
}

func waitSignal(t *testing.T, signal <-chan struct{}, name string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for %s", name)
	}
}

func assertNoSignal(t *testing.T, signal <-chan struct{}) {
	t.Helper()
	select {
	case <-signal:
		t.Fatal("operation exceeded semaphore limit")
	case <-time.After(50 * time.Millisecond):
	}
}

func waitError(t *testing.T, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for execution")
		return nil
	}
}

type customTool struct {
	specFunc func() tool.Spec
	callFunc tool.Handler
}

type panickingNameMiddleware struct{}

func (panickingNameMiddleware) Name() string {
	panic("name panic")
}

func (panickingNameMiddleware) Handle(
	ctx context.Context,
	spec tool.Spec,
	input tool.CallInput,
	next tool.Next,
) (tool.Result, error) {
	return next(ctx, input)
}

func (t *customTool) Spec() tool.Spec {
	return t.specFunc()
}

func (t *customTool) Invoke(ctx context.Context, input tool.CallInput, _ ...runnable.Option) (tool.Result, error) {
	if t.callFunc == nil {
		return tool.Result{}, errors.New("custom tool call is not configured")
	}
	return t.callFunc(ctx, input)
}
