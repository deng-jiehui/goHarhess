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

// TestActiveDescendantSurvivesRunEndAndSelfCleans 复核点：run 收尾时仍在
// 执行的后代记录（后台工具）必须保留；其结束事件到达后收尾并自清。
func TestActiveDescendantSurvivesRunEndAndSelfCleans(t *testing.T) {
	b := &bridge{tracer: trace.NewNoopTracerProvider().Tracer("test"), spans: map[string]map[string]spanEntry{}}
	ctx := observe.With(context.Background(), b)

	runTrace := observe.Trace{TraceID: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", SpanID: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}
	nodeTrace := observe.Trace{TraceID: runTrace.TraceID, SpanID: "cccccccccccccccccccccccccccccccc", ParentSpanID: runTrace.SpanID}
	toolTrace := observe.Trace{TraceID: runTrace.TraceID, SpanID: "dddddddddddddddddddddddddddddddd", ParentSpanID: nodeTrace.SpanID}
	emit := func(kind observe.Kind, phase observe.Phase, name string, tr observe.Trace) {
		observe.Emit(ctx, observe.Event{Kind: kind, Phase: phase, Name: name, Trace: tr, Time: time.Now()})
	}
	emit(observe.KindRun, observe.PhaseStart, "g", runTrace)
	emit(observe.KindNode, observe.PhaseStart, "n", nodeTrace)
	emit(observe.KindTool, observe.PhaseStart, "slow", toolTrace)
	emit(observe.KindNode, observe.PhaseEnd, "n", nodeTrace)
	// 图超时返回：run 收尾只回收已结束记录，活动工具记录保留。
	emit(observe.KindRun, observe.PhaseEnd, "g", runTrace)
	b.mu.Lock()
	if _, ok := b.spans[runTrace.TraceID][toolTrace.SpanID]; !ok {
		b.mu.Unlock()
		t.Fatal("active tool record removed at run end")
	}
	if _, ok := b.spans[runTrace.TraceID][nodeTrace.SpanID]; ok {
		b.mu.Unlock()
		t.Fatal("ended node record kept at run end")
	}
	b.mu.Unlock()
	// 后台工具退出：收尾原记录并自清，整个 trace 记录归零。
	emit(observe.KindTool, observe.PhaseEnd, "slow", toolTrace)
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.spans) != 0 {
		t.Fatalf("records leaked after background tool end: %d traces", len(b.spans))
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
