package otel

import (
	"context"
	"testing"
	"time"

	"github.com/goharness/goharness/pkg/observe"
	"go.opentelemetry.io/otel/trace"
)

// TestStandaloneOperationSpansAreReleased 复核点：直接调用组件（无 run
// 事件）产生的 span 登记必须在自身结束时清理，否则长期运行持续泄漏。
func TestStandaloneOperationSpansAreReleased(t *testing.T) {
	b := &bridge{tracer: trace.NewNoopTracerProvider().Tracer("test"), spans: map[string]map[string]spanEntry{}}
	ctx := observe.With(context.Background(), b)
	for i := 0; i < 128; i++ {
		// 独立调用：Begin 生成全新 traceID 且无父级。
		opCtx, invocation := observe.Begin(ctx, "llm", "chat", nil)
		observe.Emit(opCtx, observe.EventOf(invocation, observe.KindLLM, nil))
		observe.Emit(opCtx, observe.EventOf(invocation.WithPhase(observe.PhaseEnd), observe.KindLLM, nil))
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.spans) != 0 {
		t.Fatalf("span records leaked: %d traces retained", len(b.spans))
	}
}

// TestStandaloneOperationLateChildAfterEndUsesRemoteParent 独立根操作结束
// 清理后，迟到的子事件退回框架 ID 合成的远端父级（与孤儿事件语义一致），
// 记录照常建立且不崩溃。
func TestStandaloneOperationLateChildAfterEndUsesRemoteParent(t *testing.T) {
	b := &bridge{tracer: trace.NewNoopTracerProvider().Tracer("test"), spans: map[string]map[string]spanEntry{}}
	ctx := observe.With(context.Background(), b)

	opCtx, invocation := observe.Begin(ctx, "llm", "chat", nil)
	observe.Emit(opCtx, observe.EventOf(invocation, observe.KindLLM, nil))
	observe.Emit(opCtx, observe.EventOf(invocation.WithPhase(observe.PhaseEnd), observe.KindLLM, nil))
	b.mu.Lock()
	if len(b.spans) != 0 {
		b.mu.Unlock()
		t.Fatal("root operation record not cleaned up")
	}
	b.mu.Unlock()

	child := observe.Event{
		Kind:  observe.KindLLM,
		Phase: observe.PhaseStart,
		Name:  "late",
		Trace: observe.Trace{TraceID: invocation.Trace.TraceID, SpanID: "eeeeeeeeeeeeeeee", ParentSpanID: invocation.Trace.SpanID},
		Time:  time.Now(),
	}
	b.OnEvent(ctx, child)
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.spans[child.Trace.TraceID][child.Trace.SpanID]; !ok {
		t.Fatal("late child span not recorded")
	}
}
