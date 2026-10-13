package tool

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// MaxWorkers=2 时四个并发 Execute 最多两个同时占用 worker，其余排队；
// 排队等待遵守调用方 ctx 取消。
func TestRuntimeMaxWorkersCapsConcurrency(t *testing.T) {
	release := make(chan struct{})
	var running int32
	var peak int32
	blocking := NewFunctionTool(Spec{
		Name:        "block",
		Concurrency: ConcurrencyPolicy{Group: "block", MaxParallel: 8},
	}, func(ctx context.Context, _ CallInput) (Result, error) {
		current := atomic.AddInt32(&running, 1)
		for {
			seen := atomic.LoadInt32(&peak)
			if current <= seen || atomic.CompareAndSwapInt32(&peak, seen, current) {
				break
			}
		}
		defer atomic.AddInt32(&running, -1)
		select {
		case <-release:
			return TextResult("ok"), nil
		case <-ctx.Done():
			return Result{}, ctx.Err()
		}
	})
	config := defaultRuntimeConfig()
	config.MaxWorkers = 2
	config.MaxAbandoned = 2
	runtime, err := NewRuntime(nil, config)
	if err != nil {
		t.Fatal(err)
	}
	binding := registerTool(t, runtime, blocking)
	scope := runtime.NewScope(4, 5*time.Second)

	var wg sync.WaitGroup
	results := make(chan error, 3)
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := scope.Execute(context.Background(), binding, CallInput{})
			results <- err
		}()
	}
	// 等到两个 worker 被占用后，第四个调用携带可取消 ctx 排队。
	deadline := time.Now().Add(2 * time.Second)
	for atomic.LoadInt32(&running) < 2 {
		if time.Now().After(deadline) {
			t.Fatalf("running = %d, want 2", running)
		}
		time.Sleep(5 * time.Millisecond)
	}
	waitCtx, cancel := context.WithCancel(context.Background())
	waitDone := make(chan error, 1)
	go func() {
		_, err := scope.Execute(waitCtx, binding, CallInput{})
		waitDone <- err
	}()
	// 等排队调用进入等待后取消：它应立刻返回 ctx.Canceled，不占 worker。
	time.Sleep(20 * time.Millisecond)
	cancel()
	select {
	case err := <-waitDone:
		if err != context.Canceled {
			t.Fatalf("waiting Execute error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("waiting Execute did not respect cancellation")
	}
	if atomic.LoadInt32(&running) != 2 {
		t.Fatalf("running = %d, want 2 while queued call waited", running)
	}
	close(release)
	wg.Wait()
	for i := 0; i < 3; i++ {
		if err := <-results; err != nil {
			t.Fatalf("Execute error = %v", err)
		}
	}
	if got := atomic.LoadInt32(&peak); got != 2 {
		t.Fatalf("peak concurrency = %d, want 2", got)
	}
}

func TestRuntimeMaxWorkersValidation(t *testing.T) {
	base := defaultRuntimeConfig()
	for _, value := range []int{-1, MaxRuntimeWorkers + 1} {
		config := base
		config.MaxWorkers = value
		if _, err := NewRuntime(nil, config); err == nil {
			t.Fatalf("MaxWorkers=%d accepted", value)
		}
	}
	config := base
	config.MaxWorkers = 2
	config.MaxAbandoned = 3 // 上限随 MaxWorkers 收紧
	if _, err := NewRuntime(nil, config); err == nil {
		t.Fatal("MaxAbandoned > MaxWorkers accepted")
	}
	config.MaxAbandoned = 2
	if _, err := NewRuntime(nil, config); err != nil {
		t.Fatalf("MaxAbandoned = MaxWorkers rejected: %v", err)
	}
}
