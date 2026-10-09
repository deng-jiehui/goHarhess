package observe_test

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/goharness/goharness/pkg/observe"
)

// collector 记录到达的事件序列。
type collector struct {
	events []observe.Event
}

func (c *collector) OnEvent(_ context.Context, event observe.Event) {
	c.events = append(c.events, event)
}

func span(ctx context.Context, kind observe.Kind, name string) (context.Context, observe.Invocation) {
	return observe.Begin(ctx, string(kind), name, nil)
}

func TestWithDeliversEventsInRegistrationOrder(t *testing.T) {
	var order []string
	first := observe.ObserverFunc(func(context.Context, observe.Event) { order = append(order, "first") })
	second := observe.ObserverFunc(func(context.Context, observe.Event) { order = append(order, "second") })
	ctx := observe.With(observe.With(context.Background(), first), second, nil)

	if !observe.Active(ctx) {
		t.Fatal("Active() = false, want true")
	}
	observe.Emit(ctx, observe.Event{Kind: observe.KindNode})
	if len(order) != 2 || order[0] != "first" || order[1] != "second" {
		t.Fatalf("order = %v", order)
	}
	if observe.Active(context.Background()) {
		t.Fatal("Active() = true on empty context")
	}
}

func TestEmitFillsTimeAndTraceFromContext(t *testing.T) {
	collected := &collector{}
	ctx := observe.With(context.Background(), collected)
	ctx, invocation := span(ctx, observe.KindNode, "search")

	observe.Emit(ctx, observe.Event{Kind: observe.KindLLM, Phase: observe.PhaseStart})
	if len(collected.events) != 1 {
		t.Fatalf("events = %d", len(collected.events))
	}
	event := collected.events[0]
	if event.Time.IsZero() {
		t.Fatal("Time not filled")
	}
	if event.Trace != invocation.Trace {
		t.Fatalf("Trace = %+v, want %+v", event.Trace, invocation.Trace)
	}
}

func TestObserverPanicIsIsolated(t *testing.T) {
	var logs bytes.Buffer
	defaultLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	defer slog.SetDefault(defaultLogger)

	panicky := observe.ObserverFunc(func(context.Context, observe.Event) { panic("boom") })
	collected := &collector{}
	ctx := observe.With(context.Background(), panicky, collected)

	observe.Emit(ctx, observe.Event{Kind: observe.KindTool, Phase: observe.PhaseStart})
	if len(collected.events) != 1 {
		t.Fatal("panic in one observer stopped dispatch to the next")
	}
	if !strings.Contains(logs.String(), "observer panicked") {
		t.Fatalf("panic not logged: %q", logs.String())
	}
}

func TestCapturePayloadRedactsOnlyWhenRequested(t *testing.T) {
	collected := &collector{}
	ctx := observe.With(context.Background(), collected)
	observe.EmitPayload(ctx, observe.Event{Kind: observe.KindLLM}, "raw-secret")
	if collected.events[0].Payload != nil {
		t.Fatalf("Payload = %v, want nil without CapturePayload", collected.events[0].Payload)
	}

	withPayload := observe.With(ctx, observe.CapturePayload(func(_ observe.Event, raw any) any {
		return "redacted:" + raw.(string)
	}))
	observe.EmitPayload(withPayload, observe.Event{Kind: observe.KindLLM}, "raw-secret")
	got := collected.events[1].Payload
	if got != "redacted:raw-secret" {
		t.Fatalf("Payload = %v", got)
	}
}

func TestMultiPreservesOrderAndSkipsNil(t *testing.T) {
	var order []string
	multi := observe.Multi(
		observe.ObserverFunc(func(context.Context, observe.Event) { order = append(order, "a") }),
		nil,
		observe.ObserverFunc(func(context.Context, observe.Event) { order = append(order, "b") }),
	)
	multi.OnEvent(context.Background(), observe.Event{})
	if len(order) != 2 || order[0] != "a" || order[1] != "b" {
		t.Fatalf("order = %v", order)
	}
}

func TestEventOfMergesAndOverridesAttrs(t *testing.T) {
	invocation := observe.Invocation{
		Operation: "op",
		Phase:     observe.PhaseEnd,
		Duration:  time.Second,
		Metadata:  map[string]any{"a": 1, "b": 2},
	}
	event := observe.EventOf(invocation, observe.KindTool, map[string]any{"b": 3, "c": 4})
	if event.Name != "op" || event.Phase != observe.PhaseEnd || event.Duration != time.Second {
		t.Fatalf("event = %+v", event)
	}
	if event.Attrs["a"] != 1 || event.Attrs["b"] != 3 || event.Attrs["c"] != 4 {
		t.Fatalf("attrs = %v", event.Attrs)
	}
	invocation.Metadata["a"] = 99
	if event.Attrs["a"] != 1 {
		t.Fatal("event attrs share the invocation metadata map")
	}
}

func TestAsyncDropWhenFullCountsDropped(t *testing.T) {
	entered := make(chan struct{})
	gate := make(chan struct{})
	inner := observe.ObserverFunc(func(context.Context, observe.Event) {
		select {
		case <-entered:
		default:
			close(entered)
		}
		<-gate
	})
	async := observe.Async(inner, 1, true)
	defer async.Close()
	ctx := observe.With(context.Background(), async)

	observe.Emit(ctx, observe.Event{Kind: observe.KindNode})
	<-entered                                                // 等 drain 取走第一件并阻塞在 inner 里，队列腾空。
	observe.Emit(ctx, observe.Event{Kind: observe.KindNode}) // 入队
	observe.Emit(ctx, observe.Event{Kind: observe.KindNode}) // 队列满 → 丢弃
	close(gate)

	if dropped := async.Dropped(); dropped != 1 {
		t.Fatalf("Dropped() = %d, want 1", dropped)
	}
}

func TestAsyncBlockingSendUnblocksOnCancelAndStop(t *testing.T) {
	gate := make(chan struct{})
	inner := observe.ObserverFunc(func(context.Context, observe.Event) { <-gate })
	async := observe.Async(inner, 0, false)
	ctx := observe.With(context.Background(), async)

	observe.Emit(ctx, observe.Event{Kind: observe.KindNode})
	<-time.After(50 * time.Millisecond) // drain 已取走事件并阻塞，队列容量为 0。

	canceled, cancel := context.WithCancel(ctx)
	cancel()
	done := make(chan struct{})
	go func() {
		observe.Emit(canceled, observe.Event{Kind: observe.KindNode})
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("blocking Emit did not respect ctx cancellation")
	}

	done = make(chan struct{})
	go func() {
		observe.Emit(ctx, observe.Event{Kind: observe.KindNode})
		close(done)
	}()
	closed := make(chan struct{})
	go func() { async.Close(); close(closed) }()
	// Close 关闭 stop 后，阻塞的发送应立即返回；drain 仍阻塞在 inner 内。
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("blocking Emit did not return after Close")
	}
	close(gate) // inner 放行后 Close 排空结束。
	select {
	case <-closed:
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not finish after drain released")
	}
}

func TestAsyncCloseDrainsQueue(t *testing.T) {
	var handled atomic.Int64
	gate := make(chan struct{})
	release := make(chan struct{})
	inner := observe.ObserverFunc(func(context.Context, observe.Event) {
		<-gate
		handled.Add(1)
		<-release
	})
	async := observe.Async(inner, 8, true)
	ctx := observe.With(context.Background(), async)

	observe.Emit(ctx, observe.Event{Kind: observe.KindNode})
	observe.Emit(ctx, observe.Event{Kind: observe.KindNode})
	close(gate) // inner 开始消费
	close(release)
	async.Close() // 必须等到两件都处理完
	if handled.Load() != 2 {
		t.Fatalf("handled = %d, want 2", handled.Load())
	}
	// Close 后的事件直接丢弃，不再入队。
	observe.Emit(ctx, observe.Event{Kind: observe.KindNode})
	if handled.Load() != 2 {
		t.Fatal("event delivered after Close")
	}
}

func TestSlogPrintsIndentedTree(t *testing.T) {
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	observer := observe.Slog(logger)

	ctx := observe.With(context.Background(), observer)
	runCtx, run := observe.Begin(ctx, "graph", "main", nil)
	nodeCtx, node := observe.Begin(runCtx, "graph.node", "search", nil)

	observe.Emit(nodeCtx, observe.EventOf(node.WithPhase(observe.PhaseStart), observe.KindNode, nil))
	end := node.WithPhase(observe.PhaseEnd)
	end.Duration = 42 * time.Millisecond
	observe.Emit(nodeCtx, observe.EventOf(end, observe.KindNode, nil))
	observe.Emit(runCtx, observe.EventOf(run.WithPhase(observe.PhaseEnd), observe.KindRun, nil))

	out := logs.String()
	if !strings.Contains(out, "node start") || !strings.Contains(out, "node end") || !strings.Contains(out, "run end") {
		t.Fatalf("log output missing spans: %q", out)
	}
	if strings.Index(out, "node end") < strings.Index(out, "  node end") {
		t.Fatalf("node end not indented under run: %q", out)
	}
}

func TestErrorEventCarriesErr(t *testing.T) {
	collected := &collector{}
	ctx := observe.With(context.Background(), collected)
	boom := errors.New("boom")
	event := observe.Event{Kind: observe.KindLLM, Phase: observe.PhaseError, Err: boom}
	observe.Emit(ctx, event)
	if !errors.Is(collected.events[0].Err, boom) {
		t.Fatalf("Err = %v", collected.events[0].Err)
	}
}

// TestEmitPayloadRedactCannotMutateBusinessData 复现审查发现：脱敏函数拿到的
// 必须与业务输入无关的独立副本，改写不应污染调用方数据。
func TestEmitPayloadRedactCannotMutateBusinessData(t *testing.T) {
	collected := &collector{}
	ctx := observe.With(context.Background(), collected,
		observe.CapturePayload(func(_ observe.Event, raw any) any {
			payload := raw.(map[string]any)
			payload["secret"] = "***"
			return payload
		}))
	raw := map[string]any{"secret": "s3cr3t"}
	observe.EmitPayload(ctx, observe.Event{Kind: observe.KindLLM}, raw)

	if raw["secret"] != "s3cr3t" {
		t.Fatalf("redact mutated business payload: %v", raw)
	}
	got, ok := collected.events[0].Payload.(map[string]any)
	if !ok || got["secret"] != "***" {
		t.Fatalf("payload not redacted: %#v", collected.events[0].Payload)
	}
}

// TestEmitPayloadRedactPanicIsIsolated 脱敏 panic 不能冲出业务调用：事件
// 继续分发（Payload 留空），异常记入日志。
func TestEmitPayloadRedactPanicIsIsolated(t *testing.T) {
	var logs bytes.Buffer
	defaultLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	defer slog.SetDefault(defaultLogger)

	collected := &collector{}
	ctx := observe.With(context.Background(), collected,
		observe.CapturePayload(func(observe.Event, any) any { panic("redact boom") }))
	observe.EmitPayload(ctx, observe.Event{Kind: observe.KindLLM, Phase: observe.PhaseStart}, "raw")

	if len(collected.events) != 1 {
		t.Fatalf("events = %d", len(collected.events))
	}
	if collected.events[0].Payload != nil {
		t.Fatalf("Payload = %v, want nil after redact panic", collected.events[0].Payload)
	}
	if !strings.Contains(logs.String(), "redact panicked") {
		t.Fatalf("redact panic not logged: %q", logs.String())
	}
}

// TestMultiDispatchContinuesAfterPanic Multi 中第一个观察者 panic 后，
// 后续观察者必须继续收到事件。
func TestMultiDispatchContinuesAfterPanic(t *testing.T) {
	var logs bytes.Buffer
	defaultLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	defer slog.SetDefault(defaultLogger)

	panicky := observe.ObserverFunc(func(context.Context, observe.Event) { panic("boom") })
	collected := &collector{}
	ctx := observe.With(context.Background(), observe.Multi(panicky, collected))

	observe.Emit(ctx, observe.Event{Kind: observe.KindTool, Phase: observe.PhaseStart})
	if len(collected.events) != 1 {
		t.Fatal("panic in first Multi observer stopped dispatch to the next")
	}
	if !strings.Contains(logs.String(), "observer panicked") {
		t.Fatalf("panic not logged: %q", logs.String())
	}
}

// TestAsyncObserverPanicIsolated Async 消费端 panic 与其他观察者共用同一
// 隔离与日志语义；Close 正常排空返回。
func TestAsyncObserverPanicIsolated(t *testing.T) {
	var logs bytes.Buffer
	defaultLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	defer slog.SetDefault(defaultLogger)

	panicky := observe.ObserverFunc(func(context.Context, observe.Event) { panic("async boom") })
	async := observe.Async(panicky, 4, false)
	ctx := observe.With(context.Background(), async)

	observe.Emit(ctx, observe.Event{Kind: observe.KindTool, Phase: observe.PhaseStart})
	async.Close()

	if !strings.Contains(logs.String(), "observer panicked") {
		t.Fatalf("async panic not logged: %q", logs.String())
	}
}

// sealedPayload 带未导出字段与 Clone() 约定：反射无法复制未导出字段，
// 必须走领域 Clone——否则脱敏改写会穿透指针污染业务数据。
type sealedPayload struct{ token *string }

func (p sealedPayload) Clone() sealedPayload {
	s := *p.token
	return sealedPayload{token: &s}
}

// opaquePayload 不可隔离：未导出字段且无 Clone()。
type opaquePayload struct{ token *string }

// shallowClonePayload 的领域 Clone() 是宽松实现：返回成功但共享内部
// 引用——外层 Clone 成功不能证明内部已隔离。
type shallowClonePayload struct{ token *string }

func (p shallowClonePayload) Clone() shallowClonePayload { return p }

// anyCloneMeta 按 schema/state 元数据约定实现 Clone() any。
type anyCloneMeta struct{ values map[string]string }

func (m *anyCloneMeta) Clone() any {
	out := make(map[string]string, len(m.values))
	for k, v := range m.values {
		out[k] = v
	}
	return &anyCloneMeta{values: out}
}

// looseCarrierPayload 的领域 Clone() 宽松共享 Meta 指针；反射兜底可以
// 逐字段复制，Meta 内部由 Clone() any 完成隔离。
type looseCarrierPayload struct{ Meta *anyCloneMeta }

func (p looseCarrierPayload) Clone() looseCarrierPayload { return p }

func TestEmitPayloadHonorsDomainClone(t *testing.T) {
	collected := &collector{}
	token := "s3cr3t"
	ctx := observe.With(context.Background(), collected,
		observe.CapturePayload(func(_ observe.Event, raw any) any {
			payload := raw.(sealedPayload)
			*payload.token = "***"
			return payload
		}))
	raw := sealedPayload{token: &token}
	observe.EmitPayload(ctx, observe.Event{Kind: observe.KindLLM}, raw)

	if token != "s3cr3t" {
		t.Fatalf("redact escaped isolation: %q", token)
	}
	got := collected.events[0].Payload.(sealedPayload)
	if *got.token != "***" {
		t.Fatalf("payload = %q", *got.token)
	}
	if got.token == raw.token {
		t.Fatal("Clone() not honored: payload shares pointer with business data")
	}
}

func TestEmitPayloadSkipsUnisolatableValue(t *testing.T) {
	var logs bytes.Buffer
	defaultLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	defer slog.SetDefault(defaultLogger)

	collected := &collector{}
	redacted := atomic.Int32{}
	ctx := observe.With(context.Background(), collected,
		observe.CapturePayload(func(_ observe.Event, raw any) any {
			redacted.Add(1)
			return raw
		}))
	token := "s3cr3t"
	observe.EmitPayload(ctx, observe.Event{Kind: observe.KindLLM}, opaquePayload{token: &token})

	// 不可隔离的载荷不交给脱敏函数：Payload 留空，业务数据不受影响。
	if redacted.Load() != 0 {
		t.Fatal("redact received unisolatable payload")
	}
	if collected.events[0].Payload != nil {
		t.Fatalf("Payload = %v, want nil", collected.events[0].Payload)
	}
	if token != "s3cr3t" {
		t.Fatalf("business data touched: %q", token)
	}
	if !strings.Contains(logs.String(), "not isolatable") {
		t.Fatalf("skip not logged: %q", logs.String())
	}
}

// TestEmitPayloadRejectsNonIsolatingClone 复核点：领域 Clone() 返回成功
// 但内部仍共享引用时，脱敏副本必须与业务数据零共享——共享则跳过载荷。
func TestEmitPayloadRejectsNonIsolatingClone(t *testing.T) {
	collected := &collector{}
	token := "s3cr3t"
	ctx := observe.With(context.Background(), collected,
		observe.CapturePayload(func(_ observe.Event, raw any) any {
			payload := raw.(shallowClonePayload)
			*payload.token = "***"
			return payload
		}))
	observe.EmitPayload(ctx, observe.Event{Kind: observe.KindLLM}, shallowClonePayload{token: &token})

	if token != "s3cr3t" {
		t.Fatalf("redact escaped isolation: %q", token)
	}
	if collected.events[0].Payload != nil {
		t.Fatalf("Payload = %v, want nil for non-isolating clone", collected.events[0].Payload)
	}
}

// TestEmitPayloadRepairsSharedFieldViaAnyClone 复核点：外层 Clone 宽松
// 共享的内部字段，反射兜底仍能经 Clone() any 完成隔离并保留载荷。
func TestEmitPayloadRepairsSharedFieldViaAnyClone(t *testing.T) {
	collected := &collector{}
	ctx := observe.With(context.Background(), collected,
		observe.CapturePayload(func(_ observe.Event, raw any) any {
			payload := raw.(looseCarrierPayload)
			payload.Meta.values["k"] = "***"
			return payload
		}))
	meta := &anyCloneMeta{values: map[string]string{"k": "s3cr3t"}}
	observe.EmitPayload(ctx, observe.Event{Kind: observe.Kind("retrieval")}, looseCarrierPayload{Meta: meta})

	if meta.values["k"] != "s3cr3t" {
		t.Fatalf("redact escaped isolation: %q", meta.values["k"])
	}
	got, ok := collected.events[0].Payload.(looseCarrierPayload)
	if !ok {
		t.Fatalf("Payload = %#v", collected.events[0].Payload)
	}
	if got.Meta == meta {
		t.Fatal("payload shares meta pointer with business data")
	}
	if got.Meta.values["k"] != "***" {
		t.Fatalf("payload = %q", got.Meta.values["k"])
	}
}
