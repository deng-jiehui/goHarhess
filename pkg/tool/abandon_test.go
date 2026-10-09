package tool

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/goharness/goharness/pkg/observe"
	"github.com/goharness/goharness/pkg/schema"
)

// stuckTool 忽略 ctx：Execute 超时返回后仍阻塞在 release 上，模拟不响应
// 取消的工具占住 worker 槽位的场景。并发组放宽到 8，让多个调用能同时
// 进入工具本体而不是排在组外（排在组外的调用会响应取消正常退出）。
func stuckTool(name string, release <-chan struct{}, started chan<- struct{}) Tool {
	return NewFunctionTool(Spec{
		Name:        name,
		Concurrency: ConcurrencyPolicy{Group: name, MaxParallel: 8},
	}, func(context.Context, CallInput) (Result, error) {
		if started != nil {
			close(started)
		}
		<-release
		return TextResult("late"), nil
	})
}

func waitAbandoned(t *testing.T, runtime *Runtime, name string, want int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for runtime.abandonedCount(name) != want {
		if time.Now().After(deadline) {
			t.Fatalf("abandoned count for %q = %d, want %d", name, runtime.abandonedCount(name), want)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// 超时返回的工具计数为未归还，放弃事件经 OnToolError 告警；工具最终退出后
// 计数回落。
func TestRuntimeAbandonCountsAndAlerts(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	runtime := mustRuntime(t, nil)
	binding := registerTool(t, runtime, stuckTool("slow", release, started))

	alerts := make(chan error, 4)
	observer := observe.ObserverFunc(func(_ context.Context, event observe.Event) {
		if event.Kind == observe.KindTool && event.Phase == observe.PhaseError {
			alerts <- event.Err
		}
	})
	ctx := observe.With(context.Background(), observer)

	_, err := runtime.NewScope(1, 20*time.Millisecond).Execute(ctx, binding, CallInput{})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Execute error = %v, want deadline exceeded", err)
	}
	if got := runtime.abandonedCount("slow"); got != 1 {
		t.Fatalf("abandoned count = %d, want 1", got)
	}
	select {
	case alert := <-alerts:
		var abandoned *AbandonedError
		if !errors.As(alert, &abandoned) {
			t.Fatalf("alert error = %v, want AbandonedError", alert)
		}
		if !errors.Is(alert, ErrToolAbandoned) || abandoned.ToolName != "slow" || abandoned.Abandoned != 1 {
			t.Fatalf("alert = %#v", abandoned)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no abandon alert dispatched")
	}

	close(release)
	waitAbandoned(t, runtime, "slow", 0)
}

// 复核点：放弃判定收尾的是工具原始 span（同一 SpanID，真实耗时到判定
// 时刻）；worker 随后退出只补退出事件，不再发第二个终态改写超时结论。
func TestRuntimeAbandonClosesOriginalSpan(t *testing.T) {
	release := make(chan struct{})
	runtime := mustRuntime(t, nil)
	binding := registerTool(t, runtime, stuckTool("slow", release, nil))

	type toolEvent struct {
		phase  observe.Phase
		spanID string
		err    error
		attrs  map[string]any
	}
	events := make(chan toolEvent, 8)
	observer := observe.ObserverFunc(func(_ context.Context, event observe.Event) {
		if event.Kind == observe.KindTool {
			events <- toolEvent{phase: event.Phase, spanID: event.Trace.SpanID, err: event.Err, attrs: event.Attrs}
		}
	})
	ctx := observe.With(context.Background(), observer)

	_, err := runtime.NewScope(1, 20*time.Millisecond).Execute(ctx, binding, CallInput{})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Execute error = %v, want deadline exceeded", err)
	}

	start := <-events
	abandoned := <-events
	if start.phase != observe.PhaseStart {
		t.Fatalf("first event phase = %s, want start", start.phase)
	}
	var abandonErr *AbandonedError
	if abandoned.phase != observe.PhaseError || !errors.As(abandoned.err, &abandonErr) {
		t.Fatalf("second event = %+v, want abandon error", abandoned)
	}
	if abandoned.spanID != start.spanID {
		t.Fatalf("abandon event span %s != tool start span %s: original record must be closed", abandoned.spanID, start.spanID)
	}
	if abandoned.attrs["tool.abandoned"] != true {
		t.Fatalf("abandon attrs = %v", abandoned.attrs)
	}

	// 放行 worker 退出：补一条退出事件（同 span），随后不应再有终态。
	close(release)
	exit := <-events
	if exit.phase != observe.PhaseEvent || exit.spanID != start.spanID || exit.attrs["tool.abandoned_exit"] != true {
		t.Fatalf("exit event = %+v", exit)
	}
	select {
	case extra := <-events:
		t.Fatalf("unexpected extra terminal/duplicate event after exit: %+v", extra)
	case <-time.After(200 * time.Millisecond):
	}
}

// 同一工具的未归还执行达到阈值后新调用直接拒绝；归还后自动恢复。
func TestRuntimeBreakerRejectsAfterAbandonThreshold(t *testing.T) {
	config := defaultRuntimeConfig()
	config.MaxAbandoned = 2
	runtime, err := NewRuntime(nil, config)
	if err != nil {
		t.Fatal(err)
	}
	release := make(chan struct{})
	binding := registerTool(t, runtime, stuckTool("stuck", release, nil))

	scope := runtime.NewScope(4, 15*time.Millisecond)
	for i := 0; i < 2; i++ {
		if _, err := scope.Execute(context.Background(), binding, CallInput{}); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("call %d error = %v, want deadline exceeded", i, err)
		}
	}
	if got := runtime.abandonedCount("stuck"); got != 2 {
		t.Fatalf("abandoned count = %d, want 2", got)
	}

	start := time.Now()
	_, err = scope.Execute(context.Background(), binding, CallInput{})
	var unavailable *UnavailableError
	if !errors.As(err, &unavailable) || !errors.Is(err, ErrToolUnavailable) {
		t.Fatalf("Execute error = %v, want UnavailableError", err)
	}
	if elapsed := time.Since(start); elapsed > 15*time.Millisecond {
		t.Fatalf("rejected call took %s, expected immediate rejection", elapsed)
	}
	if unavailable.Abandoned != 2 || unavailable.Threshold != 2 {
		t.Fatalf("unavailable = %#v", unavailable)
	}

	// 其他工具不受该工具熔断影响。
	okBinding := registerTool(t, runtime, NewFunctionTool(Spec{Name: "ok"}, func(context.Context, CallInput) (Result, error) {
		return TextResult("ok"), nil
	}))
	if _, err := scope.Execute(context.Background(), okBinding, CallInput{}); err != nil {
		t.Fatalf("unrelated tool error = %v", err)
	}

	// 后台 worker 归还后计数回落，熔断自动解除。
	close(release)
	waitAbandoned(t, runtime, "stuck", 0)
	result, err := scope.Execute(context.Background(), binding, CallInput{})
	if err != nil {
		t.Fatalf("Execute after settle error = %v", err)
	}
	blocks, _ := result.ToContentBlocks()
	if len(blocks) != 1 || blocks[0].Text == nil || blocks[0].Text.Text != "late" {
		t.Fatalf("result = %#v", result)
	}
}

// MaxAbandoned 为 0 时关闭熔断：未归还执行继续计数并告警，但新调用不被拒绝。
func TestRuntimeBreakerDisabledWhenMaxAbandonedZero(t *testing.T) {
	config := defaultRuntimeConfig()
	config.MaxAbandoned = 0
	runtime, err := NewRuntime(nil, config)
	if err != nil {
		t.Fatal(err)
	}
	release := make(chan struct{})
	defer close(release)
	binding := registerTool(t, runtime, stuckTool("stuck", release, nil))

	scope := runtime.NewScope(4, 15*time.Millisecond)
	for i := 0; i < 2; i++ {
		if _, err := scope.Execute(context.Background(), binding, CallInput{}); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("call %d error = %v, want deadline exceeded", i, err)
		}
	}
	if _, err := scope.Execute(context.Background(), binding, CallInput{}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Execute error = %v, want deadline exceeded (breaker must stay off)", err)
	}
}

// 响应取消的工具退出后，未归还计数必须清零：登记与归还的判定经同一临界区
// 协调，worker 在登记前先退出的竞态不会留下残留计数，也不会误触发熔断。
func TestRuntimeAbandonCountClearsWhenToolRespectsCancel(t *testing.T) {
	config := defaultRuntimeConfig()
	config.MaxAbandoned = 4
	runtime, err := NewRuntime(nil, config)
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{}, 256)
	binding := registerTool(t, runtime, NewFunctionTool(Spec{
		Name:        "cooperative",
		Concurrency: ConcurrencyPolicy{Group: "cooperative", MaxParallel: 8},
	}, func(ctx context.Context, _ CallInput) (Result, error) {
		started <- struct{}{}
		<-ctx.Done() // 正常响应取消
		return Result{}, ctx.Err()
	}))

	scope := runtime.NewScope(8, time.Minute)
	for i := 0; i < 200; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() {
			_, err := scope.Execute(ctx, binding, CallInput{})
			done <- err
		}()
		<-started
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatalf("iteration %d: Execute error = %v, want context canceled", i, err)
		}
	}
	waitAbandoned(t, runtime, "cooperative", 0)
}

// 放弃告警读取的是执行前制备的快照：工具仍在并发修改可变参数时，告警派发
// 与参数克隆不构成数据竞争，观察者看到的是执行前的输入。
func TestRuntimeAbandonAlertSnapshotsInput(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	runtime := mustRuntime(t, nil)
	binding := registerTool(t, runtime, NewFunctionTool(Spec{
		Name:        "mutating",
		Concurrency: ConcurrencyPolicy{Group: "mutating", MaxParallel: 8},
	}, func(_ context.Context, input CallInput) (Result, error) {
		for {
			select {
			case <-release:
				return TextResult("done"), nil
			default:
			}
			values, _ := input.Args["values"].([]any)
			input.Args["values"] = append(values, len(values)) // 持续修改可变载荷
		}
	}))

	observed := make(chan int, 1)
	collector := observe.ObserverFunc(func(_ context.Context, event observe.Event) {
		var abandoned *AbandonedError
		if event.Kind != observe.KindTool || event.Phase != observe.PhaseError || !errors.As(event.Err, &abandoned) {
			return
		}
		input, _ := event.Payload.(CallInput)
		values, _ := input.Args["values"].([]any)
		observed <- len(values)
	})
	ctx := observe.With(context.Background(),
		observe.CapturePayload(func(_ observe.Event, raw any) any { return raw }),
		collector,
	)

	_, err := runtime.NewScope(1, 20*time.Millisecond).Execute(ctx, binding, CallInput{
		Args: map[string]any{"values": []any{1, 2}},
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Execute error = %v, want deadline exceeded", err)
	}
	select {
	case got := <-observed:
		if got != 2 {
			t.Fatalf("alert snapshot values len = %d, want pre-execution 2", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no abandon alert dispatched")
	}
}

func TestRuntimeConfigRejectsInvalidMaxAbandoned(t *testing.T) {
	for _, value := range []int{-1, maxRuntimeWorkers + 1} {
		config := defaultRuntimeConfig()
		config.MaxAbandoned = value
		if _, err := NewRuntime(nil, config); err == nil {
			t.Fatalf("MaxAbandoned = %d accepted, want error", value)
		}
	}
}

// RetryBackoffSet 区分「未配置」与「显式零值」：未标记的零值沿用默认间隔，
// 标记后按给定值（含零）生效。
func TestToolsNodeRetryBackoffSetFlag(t *testing.T) {
	measureGap := func(t *testing.T, config ToolNodeConfig) time.Duration {
		t.Helper()
		var starts []time.Time
		flaky := NewFunctionTool(Spec{Name: "flaky", IsReadOnly: true}, func(_ context.Context, _ CallInput) (Result, error) {
			starts = append(starts, time.Now())
			if len(starts) == 1 {
				return Result{}, errors.New("transient")
			}
			return TextResult("ok"), nil
		})
		config.Tools = []Tool{flaky}
		config.MaxRetries = 1
		node, err := NewToolsNode(config)
		if err != nil {
			t.Fatal(err)
		}
		call := schema.NewMessage(schema.RoleAssistant, schema.NewFunctionToolCallBlock("c1", "flaky", `{}`))
		if _, err := node.Invoke(context.Background(), []schema.Message{call}); err != nil {
			t.Fatal(err)
		}
		if len(starts) != 2 {
			t.Fatalf("tool attempts = %d, want 2", len(starts))
		}
		return starts[1].Sub(starts[0])
	}
	if gap := measureGap(t, ToolNodeConfig{RetryBackoffSet: true}); gap > 30*time.Millisecond {
		t.Fatalf("explicit zero backoff gap = %s, want ~0", gap)
	}
	if gap := measureGap(t, ToolNodeConfig{RetryBackoff: 60 * time.Millisecond}); gap < 45*time.Millisecond {
		t.Fatalf("positive backoff gap = %s, want ~60ms", gap)
	}
	if gap := measureGap(t, ToolNodeConfig{}); gap < 80*time.Millisecond {
		t.Fatalf("unset backoff gap = %s, want default ~100ms", gap)
	}
}

// 熔断拒绝经 ToolsNode 转成模型可读的 isError 结果，不终止节点。
func TestToolsNodeReportsUnavailableAsToolResult(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	node, err := NewToolsNode(ToolNodeConfig{
		Tools:        []Tool{stuckTool("stuck", release, nil)},
		Timeout:      15 * time.Millisecond,
		MaxAbandoned: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	call := func(name string) schema.Message {
		return schema.NewMessage(schema.RoleAssistant, schema.NewFunctionToolCallBlock("c1", name, `{}`))
	}
	resultText := func(t *testing.T, msg schema.Message) string {
		t.Helper()
		results := msg.FunctionToolResults()
		if len(results) != 1 || len(results[0].ContentBlocks) != 1 || results[0].ContentBlocks[0].Text == nil {
			t.Fatalf("unexpected tool result message: %#v", msg)
		}
		return results[0].ContentBlocks[0].Text.Text
	}
	out, err := node.Invoke(context.Background(), []schema.Message{call("stuck")})
	if err != nil || len(out) != 1 {
		t.Fatalf("first invoke out=%d err=%v", len(out), err)
	}
	if text := resultText(t, out[0]); text != `tool "stuck" timed out` {
		t.Fatalf("first result = %q", text)
	}
	out, err = node.Invoke(context.Background(), []schema.Message{call("stuck")})
	if err != nil || len(out) != 1 {
		t.Fatalf("second invoke out=%d err=%v", len(out), err)
	}
	if text := resultText(t, out[0]); text != `tool "stuck" is temporarily unavailable` {
		t.Fatalf("second result = %q", text)
	}
}
