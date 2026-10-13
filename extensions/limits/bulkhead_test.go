package limits_test

import (
	"context"
	"errors"
	"runtime"
	"testing"
	"time"

	"github.com/goharness/goharness/extensions/limits"
	"github.com/goharness/goharness/extensions/tenant"
	"github.com/goharness/goharness/pkg/agent"
	"github.com/goharness/goharness/pkg/graph"
	"github.com/goharness/goharness/pkg/llm"
	"github.com/goharness/goharness/pkg/runnable"
	"github.com/goharness/goharness/pkg/schema"
	"github.com/goharness/goharness/pkg/state"
	"github.com/goharness/goharness/pkg/tool"
)

// blockingAgent 返回一个模型在 release 关闭前一直等待的 Agent。
func blockingAgent(t *testing.T, store state.Store, release <-chan struct{}) runnable.Runnable[[]schema.Message, []schema.Message] {
	t.Helper()
	a, err := agent.New(llm.NewHandlerModel(func(ctx context.Context, _ llm.Request, _ llm.Options) ([]schema.Message, error) {
		select {
		case <-release:
			return []schema.Message{schema.AssistantMessage("ok")}, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}), agent.WithCheckpointStore(store))
	if err != nil {
		t.Fatal(err)
	}
	return a.(runnable.Runnable[[]schema.Message, []schema.Message])
}

func convOptions(key string) []runnable.Option {
	return []runnable.Option{agent.WithConversationID(key)}
}

// 并发上限：第二个调用在第一个未结束时立即被拒绝。
func TestBulkheadRejectsWhenFull(t *testing.T) {
	release := make(chan struct{})
	a := blockingAgent(t, state.NewMemoryStore(), release)
	bh, err := limits.NewBulkhead[[]schema.Message, []schema.Message](a, limits.BulkheadConfig{MaxConcurrent: 1})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := bh.Invoke(context.Background(), []schema.Message{schema.UserMessage("x")}, convOptions("s1")...)
		done <- err
	}()
	time.Sleep(60 * time.Millisecond)
	_, err = bh.Invoke(context.Background(), []schema.Message{schema.UserMessage("y")}, convOptions("s2")...)
	if !errors.Is(err, limits.ErrOverloaded) {
		t.Fatalf("err = %v, want ErrOverloaded", err)
	}
	if bh.Rejected() != 1 {
		t.Fatalf("rejected = %d", bh.Rejected())
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

// 有界排队：第二名排队者成功等到名额，第三名因队列满被拒。
func TestBulkheadBoundedQueue(t *testing.T) {
	release := make(chan struct{})
	a := blockingAgent(t, state.NewMemoryStore(), release)
	bh, err := limits.NewBulkhead[[]schema.Message, []schema.Message](a, limits.BulkheadConfig{MaxConcurrent: 1, MaxQueue: 1, QueueTimeout: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	first := make(chan error, 1)
	second := make(chan error, 1)
	go func() {
		_, err := bh.Invoke(context.Background(), []schema.Message{schema.UserMessage("x")}, convOptions("s1")...)
		first <- err
	}()
	time.Sleep(50 * time.Millisecond)
	go func() {
		_, err := bh.Invoke(context.Background(), []schema.Message{schema.UserMessage("y")}, convOptions("s2")...)
		second <- err
	}()
	time.Sleep(50 * time.Millisecond)
	_, err = bh.Invoke(context.Background(), []schema.Message{schema.UserMessage("z")}, convOptions("s3")...)
	if !errors.Is(err, limits.ErrOverloaded) {
		t.Fatalf("third err = %v", err)
	}
	close(release)
	for _, ch := range []chan error{first, second} {
		if err := <-ch; err != nil {
			t.Fatal(err)
		}
	}
}

// 排队超时：QueueTimeout 内拿不到名额返回 ErrOverloaded。
func TestBulkheadQueueTimeout(t *testing.T) {
	release := make(chan struct{})
	a := blockingAgent(t, state.NewMemoryStore(), release)
	bh, err := limits.NewBulkhead[[]schema.Message, []schema.Message](a, limits.BulkheadConfig{MaxConcurrent: 1, MaxQueue: 1, QueueTimeout: 60 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		_, _ = bh.Invoke(context.Background(), []schema.Message{schema.UserMessage("x")}, convOptions("s1")...)
	}()
	time.Sleep(50 * time.Millisecond)
	start := time.Now()
	_, err = bh.Invoke(context.Background(), []schema.Message{schema.UserMessage("y")}, convOptions("s2")...)
	close(release)
	if !errors.Is(err, limits.ErrOverloaded) {
		t.Fatalf("err = %v", err)
	}
	if elapsed := time.Since(start); elapsed < 50*time.Millisecond || elapsed > time.Second {
		t.Fatalf("queue wait = %s", elapsed)
	}
}

// 单租户上限：同一租户第二个调用被拒，另一租户不受影响；租户表在用完后清理。
func TestBulkheadPerTenant(t *testing.T) {
	release := make(chan struct{})
	a := blockingAgent(t, state.NewMemoryStore(), release)
	bh, err := limits.NewBulkhead[[]schema.Message, []schema.Message](a, limits.BulkheadConfig{MaxConcurrent: 4, PerTenant: 1})
	if err != nil {
		t.Fatal(err)
	}
	ctxA, _ := tenant.With(context.Background(), tool.Subject{TenantID: "tenant-a", ID: "u1"})
	ctxB, _ := tenant.With(context.Background(), tool.Subject{TenantID: "tenant-b", ID: "u2"})
	done := make(chan error, 1)
	go func() {
		_, err := bh.Invoke(ctxA, []schema.Message{schema.UserMessage("x")}, convOptions("s1")...)
		done <- err
	}()
	time.Sleep(60 * time.Millisecond)
	_, err = bh.Invoke(ctxA, []schema.Message{schema.UserMessage("y")}, convOptions("s2")...)
	if !errors.Is(err, limits.ErrOverloaded) {
		t.Fatalf("same tenant err = %v", err)
	}
	other := make(chan error, 1)
	go func() {
		_, err := bh.Invoke(ctxB, []schema.Message{schema.UserMessage("z")}, convOptions("s3")...)
		other <- err
	}()
	time.Sleep(60 * time.Millisecond)
	close(release)
	for _, ch := range []chan error{done, other} {
		if err := <-ch; err != nil {
			t.Fatal(err)
		}
	}
}

// 排空：Drain 取消在途运行（检查点运行以中断收尾），新调用返回
// ErrDraining；挂起进度可由另一个实例恢复。
func TestBulkheadDrainInterruptsAndResumes(t *testing.T) {
	store := state.NewMemoryStore()
	release := make(chan struct{})
	a := blockingAgent(t, store, release)
	bh, err := limits.NewBulkhead[[]schema.Message, []schema.Message](a, limits.BulkheadConfig{MaxConcurrent: 2})
	if err != nil {
		t.Fatal(err)
	}
	runErr := make(chan error, 1)
	go func() {
		_, err := bh.Invoke(context.Background(), []schema.Message{schema.UserMessage("x")}, convOptions("conv-1")...)
		runErr <- err
	}()
	time.Sleep(80 * time.Millisecond)
	if err := bh.Drain(context.Background(), "shutdown"); err != nil {
		t.Fatal(err)
	}
	if err := <-runErr; !errors.Is(err, graph.ErrInterrupted) {
		t.Fatalf("run err = %v, want ErrInterrupted", err)
	}
	_, err = bh.Invoke(context.Background(), []schema.Message{schema.UserMessage("y")}, convOptions("s2")...)
	if !errors.Is(err, limits.ErrDraining) {
		t.Fatalf("post-drain err = %v", err)
	}
	close(release)
	// 同一检查点存储上的另一个实例可以恢复挂起的运行。
	if _, err := agent.Resume(context.Background(), a.(agent.Agent), agent.WithCheckpoint(store, "conv-1")); err != nil {
		t.Fatalf("resume err = %v", err)
	}
}

// 流式名额在读端关闭后才释放。
func TestBulkheadStreamHoldsSlotUntilClose(t *testing.T) {
	store := state.NewMemoryStore()
	a0, err := agent.New(llm.NewScriptedModel([]schema.Message{schema.AssistantMessage("ok")}), agent.WithCheckpointStore(store))
	if err != nil {
		t.Fatal(err)
	}
	a := a0.(runnable.Runnable[[]schema.Message, []schema.Message])
	bh, err := limits.NewBulkhead[[]schema.Message, []schema.Message](a, limits.BulkheadConfig{MaxConcurrent: 1})
	if err != nil {
		t.Fatal(err)
	}
	reader, err := bh.Stream(context.Background(), []schema.Message{schema.UserMessage("x")}, convOptions("s1")...)
	if err != nil {
		t.Fatal(err)
	}
	// 流仍在读取，第二个调用被拒（即便上游已推完数据，读者未关闭名额不释放）。
	time.Sleep(60 * time.Millisecond)
	_, err = bh.Invoke(context.Background(), []schema.Message{schema.UserMessage("y")}, convOptions("s2")...)
	if !errors.Is(err, limits.ErrOverloaded) {
		t.Fatalf("err = %v", err)
	}
	_ = reader.Close()
	time.Sleep(60 * time.Millisecond)
	if _, err := bh.Invoke(context.Background(), []schema.Message{schema.UserMessage("y")}, convOptions("s3")...); err != nil {
		t.Fatalf("after close err = %v", err)
	}
}

// 协程回落：全部调用结束后 goroutine 数回落。
func TestBulkheadGoroutinesSettle(t *testing.T) {
	store := state.NewMemoryStore()
	a0, err := agent.New(llm.NewScriptedModel([]schema.Message{schema.AssistantMessage("ok")}), agent.WithCheckpointStore(store))
	if err != nil {
		t.Fatal(err)
	}
	a := a0.(runnable.Runnable[[]schema.Message, []schema.Message])
	bh, err := limits.NewBulkhead[[]schema.Message, []schema.Message](a, limits.BulkheadConfig{MaxConcurrent: 2, MaxQueue: 2})
	if err != nil {
		t.Fatal(err)
	}
	before := runtime.NumGoroutine()
	for i := 0; i < 10; i++ {
		if _, err := bh.Invoke(context.Background(), []schema.Message{schema.UserMessage("x")}, convOptions("s")...); err != nil {
			t.Fatal(err)
		}
	}
	deadline := time.Now().Add(2 * time.Second)
	for runtime.NumGoroutine() > before && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if runtime.NumGoroutine() > before {
		t.Fatalf("goroutines before=%d now=%d", before, runtime.NumGoroutine())
	}
}
