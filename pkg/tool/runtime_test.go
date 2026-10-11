package tool

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/goharness/goharness/pkg/runnable"
)

type traceKey struct{}

func TestRuntimeOrdersMiddlewareAndToolAndPreservesContext(t *testing.T) {
	var order []string
	middleware := MiddlewareFuncs{
		MiddlewareName: "masking",
		HandleFunc: func(ctx context.Context, spec Spec, input CallInput, next Next) (Result, error) {
			order = append(order, "middleware")
			if ctx.Value(traceKey{}) != "trace-1" {
				t.Fatal("caller context value did not reach middleware")
			}
			input.Args["masked"] = true
			return next(ctx, input)
		},
	}
	runtime := mustRuntime(t, []Middleware{middleware})
	value := NewFunctionTool(Spec{Name: "read", IsReadOnly: true}, func(ctx context.Context, input CallInput) (Result, error) {
		order = append(order, "tool")
		if input.Args["masked"] != true || ctx.Value(traceKey{}) != "trace-1" {
			t.Fatalf("tool input = %#v, trace = %v", input.Args, ctx.Value(traceKey{}))
		}
		if _, err := RequireSubject(ctx); err != nil {
			t.Fatalf("caller Subject was stripped: %v", err)
		}
		return TextResult("ok"), nil
	})
	binding := registerTool(t, runtime, value)

	runCtx := context.WithValue(context.Background(), traceKey{}, "trace-1")
	runCtx = WithSubject(runCtx, Subject{ID: "user-1"})
	result, err := runtime.NewScope(1).Execute(
		runCtx,
		binding,
		CallInput{Args: map[string]any{}},
	)
	if err != nil {
		t.Fatal(err)
	}
	blocks, err := result.ToContentBlocks()
	if err != nil || len(blocks) != 1 || blocks[0].Text == nil || blocks[0].Text.Text != "ok" {
		t.Fatalf("result = %#v, err = %v", result, err)
	}
	want := []string{"middleware", "tool"}
	if !slices.Equal(order, want) {
		t.Fatalf("order = %#v, want %#v", order, want)
	}
}

func TestRuntimeLabelsMiddlewareDenialAndSkipsTool(t *testing.T) {
	reason := errors.New("viewer cannot mutate")
	middleware := MiddlewareFuncs{
		MiddlewareName: "rbac",
		HandleFunc: func(ctx context.Context, spec Spec, input CallInput, next Next) (Result, error) {
			return Result{}, Deny(reason)
		},
	}
	runtime := mustRuntime(t, []Middleware{middleware})
	var calls atomic.Int32
	value := NewFunctionTool(Spec{Name: "delete"}, func(context.Context, CallInput) (Result, error) {
		calls.Add(1)
		return TextResult("deleted"), nil
	})
	binding := registerTool(t, runtime, value)

	_, err := runtime.NewScope(1).Execute(context.Background(), binding, CallInput{})
	if !errors.Is(err, ErrPermissionDenied) || !errors.Is(err, reason) {
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
	middleware := MiddlewareFuncs{
		MiddlewareName: "billing",
		HandleFunc: func(ctx context.Context, spec Spec, input CallInput, next Next) (Result, error) {
			if _, err := next(ctx, input); err != nil {
				return Result{}, err
			}
			return Result{}, reason
		},
	}
	runtime := mustRuntime(t, []Middleware{middleware})
	value := NewFunctionTool(Spec{Name: "charge"}, func(context.Context, CallInput) (Result, error) {
		return TextResult("charged"), nil
	})
	binding := registerTool(t, runtime, value)

	_, err := runtime.NewScope(1).Execute(context.Background(), binding, CallInput{})
	var extensionErr *ExtensionError
	if !errors.As(err, &extensionErr) || extensionErr.Name != "billing" || !errors.Is(err, reason) {
		t.Fatalf("Execute error = %#v", err)
	}
}

func TestRuntimeRecoversMiddlewareAndToolPanics(t *testing.T) {
	tests := []struct {
		name        string
		middlewares []Middleware
		handler     Handler
		component   string
	}{
		{
			name: "middleware",
			middlewares: []Middleware{MiddlewareFuncs{
				MiddlewareName: "broken",
				HandleFunc: func(context.Context, Spec, CallInput, Next) (Result, error) {
					panic("middleware panic")
				},
			}},
			handler:   func(context.Context, CallInput) (Result, error) { return TextResult("unused"), nil },
			component: `middleware "broken"`,
		},
		{
			name:      "tool",
			handler:   func(context.Context, CallInput) (Result, error) { panic("tool panic") },
			component: `tool "run"`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			runtime := mustRuntime(t, test.middlewares)
			value := NewFunctionTool(Spec{Name: "run"}, test.handler)
			binding := registerTool(t, runtime, value)
			_, err := runtime.NewScope(1).Execute(context.Background(), binding, CallInput{})
			if !errors.Is(err, ErrExecutionPanic) || !strings.Contains(err.Error(), test.component) {
				t.Fatalf("Execute error = %v", err)
			}
		})
	}
}

func TestRuntimeCancellationPreemptsUncooperativeTool(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	runtime := mustRuntime(t, nil)
	value := NewFunctionTool(Spec{Name: "slow"}, func(context.Context, CallInput) (Result, error) {
		close(started)
		<-release
		return TextResult("late"), nil
	})
	binding := registerTool(t, runtime, value)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := runtime.NewScope(1).Execute(ctx, binding, CallInput{})
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
	config := defaultRuntimeConfig()
	config.Timeout = 25 * time.Millisecond
	runtime, err := NewRuntime(nil, config)
	if err != nil {
		t.Fatal(err)
	}
	value := NewFunctionTool(Spec{Name: "configured-timeout"}, func(ctx context.Context, input CallInput) (Result, error) {
		<-ctx.Done()
		return Result{}, ctx.Err()
	})
	binding := registerTool(t, runtime, value)
	startedAt := time.Now()

	_, err = runtime.NewScope(1, time.Second).Execute(context.Background(), binding, CallInput{})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Execute error = %v, want deadline exceeded", err)
	}
	if elapsed := time.Since(startedAt); elapsed > 300*time.Millisecond {
		t.Fatalf("configured timeout took too long: %s", elapsed)
	}
}

// 工具直接回传 ctx.Err() 时，框架超时不允许被 bound ctx 的 cancelCtx
// 包装误分类成 Canceled。循环执行覆盖 control.Done 与 completed 的
// select 竞争：修复前约半数迭代会拿到 context canceled。
func TestTimeoutClassificationSurvivesToolContextError(t *testing.T) {
	config := defaultRuntimeConfig()
	config.Timeout = 5 * time.Millisecond
	runtime, err := NewRuntime(nil, config)
	if err != nil {
		t.Fatal(err)
	}
	value := NewFunctionTool(Spec{Name: "echo-ctx-err"}, func(ctx context.Context, input CallInput) (Result, error) {
		<-ctx.Done()
		return Result{}, ctx.Err()
	})
	binding := registerTool(t, runtime, value)
	for i := 0; i < 30; i++ {
		if _, err := runtime.NewScope(1, 5*time.Millisecond).Execute(context.Background(), binding, CallInput{}); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("iteration %d: Execute error = %v, want deadline exceeded", i, err)
		}
	}
}

// 同一循环下主动取消必须稳定分类为 Canceled，不被超时路径改写。
func TestCancelClassificationStableAcrossIterations(t *testing.T) {
	runtime := mustRuntime(t, nil)
	value := NewFunctionTool(Spec{Name: "echo-ctx-err"}, func(ctx context.Context, input CallInput) (Result, error) {
		<-ctx.Done()
		return Result{}, ctx.Err()
	})
	binding := registerTool(t, runtime, value)
	for i := 0; i < 10; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() {
			_, err := runtime.NewScope(1).Execute(ctx, binding, CallInput{})
			done <- err
		}()
		time.Sleep(2 * time.Millisecond)
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatalf("iteration %d: Execute error = %v, want context canceled", i, err)
		}
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
	config := defaultRuntimeConfig()
	config.MaxRetries = 2
	config.RetryBackoff = 0
	runtime, err := NewRuntime(nil, config)
	if err != nil {
		t.Fatal(err)
	}
	transient := errors.New("transient failure")
	var readCalls atomic.Int32
	readTool := NewFunctionTool(Spec{Name: "read", IsReadOnly: true}, func(context.Context, CallInput) (Result, error) {
		if readCalls.Add(1) < 3 {
			return Result{}, transient
		}
		return TextResult("ok"), nil
	})
	readBinding := registerTool(t, runtime, readTool)
	if _, err := runtime.NewScope(1).Execute(context.Background(), readBinding, CallInput{}); err != nil {
		t.Fatal(err)
	}
	if readCalls.Load() != 3 {
		t.Fatalf("read tool calls = %d, want 3", readCalls.Load())
	}

	var writeCalls atomic.Int32
	writeTool := NewFunctionTool(Spec{Name: "write"}, func(context.Context, CallInput) (Result, error) {
		writeCalls.Add(1)
		return Result{}, transient
	})
	writeBinding := registerTool(t, runtime, writeTool)
	_, err = runtime.NewScope(1).Execute(context.Background(), writeBinding, CallInput{})
	if !errors.Is(err, transient) || writeCalls.Load() != 1 {
		t.Fatalf("write calls = %d, error = %v", writeCalls.Load(), err)
	}
}

func TestRuntimeRejectsDepthBeyondSystemLimit(t *testing.T) {
	runtime := mustRuntime(t, nil)
	value := NewFunctionTool(Spec{Name: "recursive"}, nil)
	binding := registerTool(t, runtime, value)
	ctx := context.WithValue(context.Background(), executionStateKey{}, executionState{depth: maxExecutionDepth})

	_, err := runtime.NewScope(1).Execute(ctx, binding, CallInput{})
	if !errors.Is(err, ErrMaxDepthExceeded) {
		t.Fatalf("Execute error = %v", err)
	}
	var depthErr *MaxDepthError
	if !errors.As(err, &depthErr) || depthErr.Depth != maxExecutionDepth+1 {
		t.Fatalf("depth error = %#v", depthErr)
	}
}

func TestRuntimeRejectsDeadlockRiskAndMiddlewareCannotSwallowIt(t *testing.T) {
	middleware := MiddlewareFuncs{
		MiddlewareName: "unsafe-fallback",
		HandleFunc: func(ctx context.Context, spec Spec, input CallInput, next Next) (Result, error) {
			_, _ = next(context.Background(), input)
			return TextResult("pretend-success"), nil
		},
	}
	runtime := mustRuntime(t, []Middleware{middleware})
	value := NewFunctionTool(Spec{
		Name: "nested",
		Concurrency: ConcurrencyPolicy{
			Group:       "network",
			MaxParallel: 1,
		},
	}, nil)
	binding := registerTool(t, runtime, value)
	ctx := context.WithValue(context.Background(), executionStateKey{}, executionState{held: map[*scheduler][]string{runtime.scheduler: {"network"}}})

	_, err := runtime.NewScope(1).Execute(ctx, binding, CallInput{})
	if !errors.Is(err, ErrDeadlockRisk) {
		t.Fatalf("Execute error = %v", err)
	}
}

func TestRuntimeSemaphoreLimitsEveryMiddlewareNextCall(t *testing.T) {
	started := make(chan struct{}, 2)
	release := make(chan struct{})
	middleware := MiddlewareFuncs{
		MiddlewareName: "parallel-retry",
		HandleFunc: func(ctx context.Context, spec Spec, input CallInput, next Next) (Result, error) {
			var wait sync.WaitGroup
			wait.Add(2)
			for range 2 {
				go func() {
					defer wait.Done()
					_, _ = next(ctx, input)
				}()
			}
			wait.Wait()
			return TextResult("done"), nil
		},
	}
	runtime := mustRuntime(t, []Middleware{middleware})
	value := NewFunctionTool(Spec{
		Name: "network",
		Concurrency: ConcurrencyPolicy{
			Group:       "network",
			MaxParallel: 2,
		},
	}, func(context.Context, CallInput) (Result, error) {
		started <- struct{}{}
		<-release
		return TextResult("ok"), nil
	})
	binding := registerTool(t, runtime, value)
	done := make(chan error, 1)
	go func() {
		_, err := runtime.NewScope(1).Execute(context.Background(), binding, CallInput{})
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
	middleware := MiddlewareFuncs{
		MiddlewareName: "async-next",
		HandleFunc: func(ctx context.Context, spec Spec, input CallInput, next Next) (Result, error) {
			go func() {
				_, _ = next(ctx, input)
			}()
			<-started
			return TextResult("middleware-returned"), nil
		},
	}
	runtime := mustRuntime(t, []Middleware{middleware})
	value := NewFunctionTool(Spec{Name: "async"}, func(context.Context, CallInput) (Result, error) {
		close(started)
		<-release
		return TextResult("tool-returned"), nil
	})
	binding := registerTool(t, runtime, value)
	done := make(chan error, 1)
	go func() {
		_, err := runtime.NewScope(1).Execute(context.Background(), binding, CallInput{})
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
	if _, err := runtime.Register(NewFunctionTool(Spec{
		Name: "first", Concurrency: ConcurrencyPolicy{Group: "browser", MaxParallel: 1},
	}, nil)); err != nil {
		t.Fatal(err)
	}
	_, err := runtime.Register(NewFunctionTool(Spec{
		Name: "second", Concurrency: ConcurrencyPolicy{Group: "browser", MaxParallel: 2},
	}, nil))
	if err == nil || !strings.Contains(err.Error(), "already has max parallel 1") {
		t.Fatalf("Register error = %v", err)
	}
}

func TestRuntimeRejectsConcurrencyAboveSystemLimit(t *testing.T) {
	runtime := mustRuntime(t, nil)
	_, err := runtime.Register(NewFunctionTool(Spec{
		Name: "unbounded",
		Concurrency: ConcurrencyPolicy{
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
		specFunc: func() Spec {
			panic("spec panic")
		},
	}
	runtime := mustRuntime(t, nil)

	_, err := runtime.Register(value)
	if !errors.Is(err, ErrExecutionPanic) || !strings.Contains(err.Error(), "tool Spec") {
		t.Fatalf("Register error = %v", err)
	}
}

func TestRuntimeRecoversExtensionNamePanicDuringConfiguration(t *testing.T) {
	_, err := NewRuntime([]Middleware{panickingNameMiddleware{}})
	if !errors.Is(err, ErrExecutionPanic) || !strings.Contains(err.Error(), "middleware Name") {
		t.Fatalf("New error = %v", err)
	}
}

func TestRuntimeUsesFrozenSpecSnapshot(t *testing.T) {
	var specCalls atomic.Int32
	value := &customTool{
		specFunc: func() Spec {
			if specCalls.Add(1) > 1 {
				panic("Spec called after registration")
			}
			return Spec{Name: "stable", IsReadOnly: true}
		},
		callFunc: func(context.Context, CallInput) (Result, error) {
			return TextResult("ok"), nil
		},
	}
	runtime := mustRuntime(t, nil)
	binding := registerTool(t, runtime, value)

	_, err := runtime.NewScope(1).Execute(context.Background(), binding, CallInput{})
	if err != nil {
		t.Fatal(err)
	}
	if specCalls.Load() != 1 || binding.Spec().Name != "stable" {
		t.Fatalf("Spec calls = %d, snapshot = %#v", specCalls.Load(), binding.Spec())
	}
}

func TestNewScopeHonorsConfiguredAndSystemConcurrencyLimits(t *testing.T) {
	runtime := mustRuntime(t, nil)
	if capacity := cap(runtime.NewScope(maxBatchParallelism + 100).semaphore); capacity != defaultRuntimeConfig().MaxConcurrency {
		t.Fatalf("default scope capacity = %d, want %d", capacity, defaultRuntimeConfig().MaxConcurrency)
	}

	config := defaultRuntimeConfig()
	config.MaxConcurrency = maxBatchParallelism
	runtime, err := NewRuntime(nil, config)
	if err != nil {
		t.Fatal(err)
	}
	if capacity := cap(runtime.NewScope(maxBatchParallelism + 100).semaphore); capacity != maxBatchParallelism {
		t.Fatalf("configured scope capacity = %d, want %d", capacity, maxBatchParallelism)
	}
}

func mustRuntime(t *testing.T, middlewares []Middleware) *Runtime {
	t.Helper()
	runtime, err := NewRuntime(middlewares)
	if err != nil {
		t.Fatal(err)
	}
	return runtime
}

func registerTool(t *testing.T, runtime *Runtime, value Tool) *Binding {
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
	specFunc func() Spec
	callFunc Handler
}

type panickingNameMiddleware struct{}

func (panickingNameMiddleware) Name() string {
	panic("name panic")
}

func (panickingNameMiddleware) Handle(
	ctx context.Context,
	spec Spec,
	input CallInput,
	next Next,
) (Result, error) {
	return next(ctx, input)
}

func (t *customTool) Spec() Spec {
	return t.specFunc()
}

func (t *customTool) Invoke(ctx context.Context, input CallInput, _ ...runnable.Option) (Result, error) {
	if t.callFunc == nil {
		return Result{}, errors.New("custom tool call is not configured")
	}
	return t.callFunc(ctx, input)
}
func (t *customTool) Stream(ctx context.Context, input CallInput, options ...runnable.Option) (*runnable.StreamReader[Result], error) {
	return runnable.StreamFromInvoke(ctx, input, t.Invoke, options...)
}
func (t *customTool) Collect(ctx context.Context, input *runnable.StreamReader[CallInput], options ...runnable.Option) (Result, error) {
	return runnable.CollectFromInvoke(ctx, input, t.Invoke, options...)
}
func (t *customTool) Transform(ctx context.Context, input *runnable.StreamReader[CallInput], options ...runnable.Option) (*runnable.StreamReader[Result], error) {
	return runnable.TransformFromInvoke(ctx, input, t.Invoke, options...)
}
