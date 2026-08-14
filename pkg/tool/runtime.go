package tool

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"goharness/pkg/runnable"
)

const (
	maxExecutionDepth    = 32
	maxBatchParallelism  = 64
	maxRuntimeWorkers    = 128
	maxExecutionDuration = 5 * time.Minute
	maxToolRetries       = 10
	maxRetryBackoff      = 30 * time.Second
)

// RuntimeConfig bounds tool execution independently from any Agent.
type RuntimeConfig struct {
	MaxDepth       int
	Timeout        time.Duration
	MaxConcurrency int
	MaxRetries     int
	RetryBackoff   time.Duration
}

func defaultRuntimeConfig() RuntimeConfig {
	return RuntimeConfig{
		MaxDepth:       16,
		Timeout:        30 * time.Second,
		MaxConcurrency: 8,
		RetryBackoff:   100 * time.Millisecond,
	}
}

func validateRuntimeConfig(config RuntimeConfig) error {
	if config.MaxDepth < 1 || config.MaxDepth > maxExecutionDepth {
		return fmt.Errorf("tool runtime max depth must be between 1 and %d: %d", maxExecutionDepth, config.MaxDepth)
	}
	if config.Timeout <= 0 || config.Timeout > maxExecutionDuration {
		return fmt.Errorf("tool runtime timeout must be greater than zero and at most %s: %s", maxExecutionDuration, config.Timeout)
	}
	if config.MaxConcurrency < 1 || config.MaxConcurrency > maxBatchParallelism {
		return fmt.Errorf("tool runtime max concurrency must be between 1 and %d: %d", maxBatchParallelism, config.MaxConcurrency)
	}
	if config.MaxRetries < 0 || config.MaxRetries > maxToolRetries {
		return fmt.Errorf("tool runtime max retries must be between 0 and %d: %d", maxToolRetries, config.MaxRetries)
	}
	if config.RetryBackoff < 0 || config.RetryBackoff > maxRetryBackoff {
		return fmt.Errorf("tool runtime retry backoff must be between zero and %s: %s", maxRetryBackoff, config.RetryBackoff)
	}
	return nil
}

type middlewareEntry struct {
	name  string
	value Middleware
}

// Binding is an immutable runtime association between a tool and its validated Spec.
type Binding struct {
	value Tool
	spec  Spec
}

// Spec returns the validated registration snapshot.
func (b *Binding) Spec() Spec {
	if b == nil {
		return Spec{}
	}
	return b.spec
}

// Runtime is exported only inside Go's internal boundary for use by Agent.
type Runtime struct {
	scheduler   *scheduler
	middlewares []middlewareEntry
	workers     chan struct{}
	config      RuntimeConfig
}

// NewRuntime validates extensions and creates the guarded tool executor.
func NewRuntime(middlewares []Middleware, configs ...RuntimeConfig) (*Runtime, error) {
	config := defaultRuntimeConfig()
	if len(configs) > 0 {
		config = configs[0]
	}
	if err := validateRuntimeConfig(config); err != nil {
		return nil, err
	}
	runtime := &Runtime{
		scheduler: newScheduler(),
		workers:   make(chan struct{}, maxRuntimeWorkers),
		config:    config,
	}
	for index, middleware := range middlewares {
		if middleware == nil {
			return nil, fmt.Errorf("middleware at index %d cannot be nil", index)
		}
		name, err := middlewareName(middleware, index)
		if err != nil {
			return nil, fmt.Errorf("read middleware name at index %d: %w", index, err)
		}
		runtime.middlewares = append(runtime.middlewares, middlewareEntry{
			name:  name,
			value: middleware,
		})
	}
	return runtime, nil
}

// Register snapshots a tool Spec and reserves its internal concurrency group.
func (r *Runtime) Register(value Tool) (*Binding, error) {
	if r == nil || r.scheduler == nil {
		return nil, errors.New("tool runtime is not configured")
	}
	if value == nil {
		return nil, errors.New("tool cannot be nil")
	}
	spec, err := readSpec(value)
	if err != nil {
		return nil, err
	}
	if err := spec.Validate(); err != nil {
		return nil, err
	}
	if err := r.scheduler.register(spec.Concurrency); err != nil {
		return nil, err
	}
	return &Binding{value: value, spec: spec}, nil
}

// NewScope creates the fixed Promise.all resource pool for one tool batch.
func (r *Runtime) NewScope(maxParallel int, defaultTimeout ...time.Duration) *Scope {
	limit := r.config.MaxConcurrency
	if maxParallel > 0 && maxParallel < limit {
		limit = maxParallel
	}
	if limit <= 0 || limit > maxBatchParallelism {
		limit = maxBatchParallelism
	}
	timeout := r.config.Timeout
	if len(defaultTimeout) > 0 && defaultTimeout[0] > 0 && defaultTimeout[0] < timeout {
		timeout = defaultTimeout[0]
	}
	return &Scope{
		runtime:        r,
		semaphore:      make(chan struct{}, limit),
		defaultTimeout: timeout,
	}
}

// Scope owns one batch's non-replaceable semaphore.
type Scope struct {
	runtime        *Runtime
	semaphore      chan struct{}
	defaultTimeout time.Duration
}

// Execute runs user extensions and the tool behind all system guards.
func (s *Scope) Execute(ctx context.Context, binding *Binding, input CallInput, options ...runnable.Option) (Result, error) {
	if s == nil || s.runtime == nil || s.runtime.scheduler == nil || s.runtime.workers == nil {
		return Result{}, errors.New("tool execution scope is not configured")
	}
	if ctx == nil {
		return Result{}, ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	plan, err := runnable.NewOptionPlan(options...)
	if err != nil {
		return Result{}, fmt.Errorf("resolve tool options: %w", err)
	}
	nodeOptions := plan.ForwardOptionsFor(runnable.OptionScopeNode)
	nodePlan, err := runnable.NewOptionPlan(nodeOptions...)
	if err != nil {
		return Result{}, fmt.Errorf("isolate tool options: %w", err)
	}
	ctx = runnable.WithOptionPlan(ctx, nodePlan)
	options = nodePlan.ForwardOptionsFor(runnable.OptionScopeNode)
	requested := nodeExecutionOptionsFromPlan(nodePlan)
	timeout := s.defaultTimeout
	contextLimits := executionLimitsFromContext(ctx)
	if contextLimits.Timeout > 0 && contextLimits.Timeout < timeout {
		timeout = contextLimits.Timeout
	}
	if requested.Timeout > 0 && requested.Timeout < timeout {
		timeout = requested.Timeout
	}
	if binding == nil || binding.value == nil {
		return Result{}, errors.New("tool binding is not configured")
	}
	httpOptions, configured := HTTPOptionsFromContext(ctx)
	if !configured {
		httpOptions = HTTPOptions{Timeout: effectiveTimeout(timeout)}
	}
	ctx = WithHTTPOptions(ctx, httpOptions)
	control, cancel := context.WithTimeout(ctx, effectiveTimeout(timeout))
	defer cancel()
	input.HTTPClient = HTTPClient(control)
	if err := acquire(control, s.runtime.workers); err != nil {
		return Result{}, err
	}

	completed := make(chan executionResult, 1)
	go func() {
		defer release(s.runtime.workers)
		result, err := s.runtime.invoke(control, s.semaphore, binding, input, options)
		completed <- executionResult{result: result, err: err}
	}()
	select {
	case <-control.Done():
		return Result{}, control.Err()
	case completed := <-completed:
		return completed.result, completed.err
	}
}

func effectiveTimeout(configured time.Duration) time.Duration {
	timeout := configured
	if timeout <= 0 || timeout > maxExecutionDuration {
		timeout = maxExecutionDuration
	}
	return timeout
}

type executionResult struct {
	result Result
	err    error
}

func (r *Runtime) invoke(
	control context.Context,
	semaphore chan struct{},
	binding *Binding,
	input CallInput,
	options []runnable.Option,
) (result Result, err error) {
	spec := binding.spec
	state := stateFromContext(control)
	state.depth++
	if state.depth > r.config.MaxDepth {
		return Result{}, &MaxDepthError{Depth: state.depth, MaxDepth: r.config.MaxDepth}
	}

	ctx, cleanup := bindContext(control, control, state)
	defer cleanup()
	ctx, invocation := BeginCallback(ctx, spec, input)
	defer func() { EndCallback(ctx, invocation, spec, input, result, err) }()
	defer recoverPanic("user extension zone", &result, &err)
	return r.invokeMiddleware(ctx, control, state, semaphore, binding.value, spec, input, options, 0)
}

func (r *Runtime) invokeMiddleware(
	ctx context.Context,
	control context.Context,
	state executionState,
	semaphore chan struct{},
	value Tool,
	spec Spec,
	input CallInput,
	options []runnable.Option,
	index int,
) (Result, error) {
	if err := control.Err(); err != nil {
		return Result{}, err
	}
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	if index >= len(r.middlewares) {
		return r.invokeTool(ctx, control, state, semaphore, value, spec, input, options)
	}
	entry := r.middlewares[index]
	var nextCalled atomic.Bool
	var nextWait sync.WaitGroup
	var lifecycle struct {
		sync.Mutex
		accepting bool
	}
	lifecycle.accepting = true
	var protected struct {
		sync.Mutex
		err error
	}
	var downstream struct {
		sync.Mutex
		errors []error
	}
	next := func(nextCtx context.Context, nextInput CallInput) (Result, error) {
		nextCalled.Store(true)
		lifecycle.Lock()
		if !lifecycle.accepting {
			lifecycle.Unlock()
			return Result{}, ErrNextUnavailable
		}
		nextWait.Add(1)
		lifecycle.Unlock()
		defer nextWait.Done()

		var result Result
		var err error
		if nextCtx == nil {
			err = ErrNilContext
		} else {
			boundCtx, cleanup := bindContext(control, nextCtx, state)
			defer cleanup()
			result, err = r.invokeMiddleware(boundCtx, control, state, semaphore, value, spec, nextInput, options, index+1)
		}
		if isProtectedError(err) {
			protected.Lock()
			if protected.err == nil {
				protected.err = err
			}
			protected.Unlock()
		}
		if err != nil {
			downstream.Lock()
			downstream.errors = append(downstream.errors, err)
			downstream.Unlock()
		}
		return result, err
	}
	result, err := callMiddleware(entry, ctx, spec, input, next)
	lifecycle.Lock()
	lifecycle.accepting = false
	lifecycle.Unlock()
	nextWait.Wait()
	protected.Lock()
	protectedErr := protected.err
	protected.Unlock()
	if protectedErr != nil {
		return Result{}, protectedErr
	}
	if err == nil {
		return result, nil
	}
	if errors.Is(err, ErrPermissionDenied) {
		return result, classifyExtensionError("middleware", entry.name, "handle", spec.Name, err)
	}
	downstream.Lock()
	downstreamErrors := append([]error(nil), downstream.errors...)
	downstream.Unlock()
	if !nextCalled.Load() || !wrapsAny(err, downstreamErrors) {
		return result, &ExtensionError{Kind: "middleware", Name: entry.name, Phase: "handle", Err: err}
	}
	return result, err
}

func (r *Runtime) invokeTool(
	ctx context.Context,
	control context.Context,
	state executionState,
	semaphore chan struct{},
	value Tool,
	spec Spec,
	input CallInput,
	options []runnable.Option,
) (result Result, err error) {
	if err := control.Err(); err != nil {
		return Result{}, err
	}
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	attempts := 1
	if spec.IsReadOnly {
		attempts += r.config.MaxRetries
	}
	for attempt := 1; attempt <= attempts; attempt++ {
		result, err = r.invokeToolAttempt(ctx, control, state, semaphore, value, spec, input, options)
		if err == nil || isProtectedError(err) {
			return result, err
		}
		if attempt == attempts {
			if attempts > 1 {
				return result, fmt.Errorf("tool %q failed after %d attempts: %w", spec.Name, attempts, err)
			}
			return result, err
		}
		if err := waitRetry(control, r.config.RetryBackoff); err != nil {
			return Result{}, err
		}
	}
	return result, err
}

func (r *Runtime) invokeToolAttempt(
	ctx context.Context,
	control context.Context,
	state executionState,
	semaphore chan struct{},
	value Tool,
	spec Spec,
	input CallInput,
	options []runnable.Option,
) (result Result, err error) {
	if err := acquire(control, semaphore); err != nil {
		return Result{}, err
	}
	defer release(semaphore)
	boundCtx, cleanup := bindContext(control, ctx, state)
	defer cleanup()
	defer recoverPanic(fmt.Sprintf("tool %q", spec.Name), &result, &err)
	return r.scheduler.execute(boundCtx, spec.Concurrency, func(callCtx context.Context) (Result, error) {
		input.HTTPClient = HTTPClient(callCtx)
		return value.Invoke(callCtx, input, options...)
	})
}

func waitRetry(ctx context.Context, backoff time.Duration) error {
	if backoff <= 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(backoff)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func callMiddleware(
	entry middlewareEntry,
	ctx context.Context,
	spec Spec,
	input CallInput,
	next Next,
) (result Result, err error) {
	defer recoverPanic(fmt.Sprintf("middleware %q", entry.name), &result, &err)
	return entry.value.Handle(ctx, spec, input, next)
}

func classifyExtensionError(kind, name, phase, toolName string, err error) error {
	if errors.Is(err, ErrPermissionDenied) {
		var denied *PermissionDeniedError
		if errors.As(err, &denied) && denied.Err != nil {
			err = denied.Err
		}
		return &PermissionDeniedError{
			ToolName: toolName,
			Layer:    fmt.Sprintf("%s %q", kind, name),
			Err:      err,
		}
	}
	return &ExtensionError{Kind: kind, Name: name, Phase: phase, Err: err}
}

func recoverPanic(component string, result *Result, err *error) {
	if recovered := recover(); recovered != nil {
		*result = Result{}
		*err = panicError(component, recovered)
	}
}

func panicError(component string, recovered any) error {
	panicErr := &ExecutionPanicError{Component: component, Value: recovered}
	if recoveredErr, ok := recovered.(error); ok {
		panicErr.Err = recoveredErr
	}
	return panicErr
}

func readSpec(value Tool) (spec Spec, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			spec = Spec{}
			err = panicError("tool Spec", recovered)
		}
	}()
	return value.Spec(), nil
}

func isProtectedError(err error) bool {
	return err != nil && (errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, ErrNilContext) ||
		errors.Is(err, ErrExecutionPanic) ||
		errors.Is(err, ErrMaxDepthExceeded) ||
		errors.Is(err, ErrDeadlockRisk) ||
		errors.Is(err, ErrPermissionDenied) ||
		errors.Is(err, ErrNextUnavailable))
}

func middlewareName(value Middleware, index int) (name string, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			name = ""
			err = panicError("middleware Name", recovered)
		}
	}()
	if named, ok := value.(NamedMiddleware); ok {
		if name := strings.TrimSpace(named.Name()); name != "" {
			return name, nil
		}
	}
	return fmt.Sprintf("middleware[%d]", index), nil
}

func wrapsAny(err error, candidates []error) bool {
	for _, candidate := range candidates {
		if errors.Is(err, candidate) {
			return true
		}
	}
	return false
}
