package otel_test

import (
	"context"
	"errors"
	"testing"
	"time"

	otel "github.com/goharness/goharness/extensions/otel"
	"github.com/goharness/goharness/pkg/observe"
	"github.com/goharness/goharness/pkg/schema"

	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	oteltrace "go.opentelemetry.io/otel/trace"
)

// newRecorder 用 SDK 的内存 SpanRecorder 验证桥接输出。
func newRecorder(t *testing.T) (*tracetest.SpanRecorder, oteltrace.Tracer) {
	t.Helper()
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	return recorder, provider.Tracer("test")
}

func event(phase observe.Phase, trace observe.Trace) observe.Event {
	return observe.Event{
		Kind:     observe.KindNode,
		Phase:    phase,
		Name:     "node",
		Trace:    trace,
		Time:     time.Now(),
		Duration: 5 * time.Millisecond,
		Attrs:    map[string]any{"graph.step": 1},
	}
}

func TestObserverBuildsSpanTreeAndClosesIt(t *testing.T) {
	recorder, tracer := newRecorder(t)
	ctx := observe.With(context.Background(), otel.Observer(tracer))

	runTrace := observe.Trace{TraceID: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", RunID: "r1", SpanID: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}
	nodeTrace := observe.Trace{TraceID: runTrace.TraceID, RunID: "r1", SpanID: "cccccccccccccccccccccccccccccccc", ParentSpanID: runTrace.SpanID}

	observe.Emit(ctx, observe.Event{Kind: observe.KindRun, Phase: observe.PhaseStart, Name: "g", Trace: runTrace, Time: time.Now()})
	observe.Emit(ctx, event(observe.PhaseStart, nodeTrace))
	observe.Emit(ctx, event(observe.PhaseOutput, nodeTrace))
	observe.Emit(ctx, event(observe.PhaseEnd, nodeTrace))
	observe.Emit(ctx, observe.Event{Kind: observe.KindRun, Phase: observe.PhaseEnd, Name: "g", Trace: runTrace, Time: time.Now(), Duration: 10 * time.Millisecond})

	spans := recorder.Ended()
	if len(spans) != 2 {
		t.Fatalf("ended spans=%d", len(spans))
	}
	var runSpan, nodeSpan sdktrace.ReadOnlySpan
	for _, s := range spans {
		switch s.Name() {
		case "run:g":
			runSpan = s
		case "node:node":
			nodeSpan = s
		}
	}
	if runSpan == nil || nodeSpan == nil {
		t.Fatalf("spans=%v", recorder.Ended())
	}
	if got := nodeSpan.Parent().SpanID().String(); got != runSpan.SpanContext().SpanID().String() {
		t.Fatalf("node parent=%s want %s", got, runSpan.SpanContext().SpanID())
	}
	if nodeSpan.SpanContext().TraceID() != runSpan.SpanContext().TraceID() {
		t.Fatal("node span lost trace linkage")
	}
	if len(nodeSpan.Events()) != 1 || nodeSpan.Events()[0].Name != "node.output:node" {
		t.Fatalf("node span events=%v", nodeSpan.Events())
	}
	if runSpan.Status().Code != codes.Ok {
		t.Fatalf("run status=%v", runSpan.Status())
	}
}

func TestObserverRecordsErrorAndUsage(t *testing.T) {
	recorder, tracer := newRecorder(t)
	ctx := observe.With(context.Background(), otel.Observer(tracer))
	reason := errors.New("provider failed")

	spanTrace := observe.Trace{TraceID: "dddddddddddddddddddddddddddddddd", SpanID: "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"}
	observe.Emit(ctx, observe.Event{Kind: observe.KindLLM, Phase: observe.PhaseStart, Name: "chat", Trace: spanTrace, Time: time.Now()})
	observe.Emit(ctx, observe.Event{
		Kind: observe.KindLLM, Phase: observe.PhaseError, Name: "chat", Trace: spanTrace,
		Time: time.Now(), Duration: time.Millisecond, Err: reason,
		Usage: &schema.TokenUsage{InputTokens: 3, OutputTokens: 2, TotalTokens: 5},
	})

	spans := recorder.Ended()
	if len(spans) != 1 {
		t.Fatalf("ended spans=%d", len(spans))
	}
	span := spans[0]
	if span.Status().Code != codes.Error || len(span.Events()) == 0 {
		t.Fatalf("span status=%v events=%v", span.Status(), span.Events())
	}
	var sawInput bool
	for _, attr := range span.Attributes() {
		if string(attr.Key) == "llm.usage.input_tokens" && attr.Value.AsInt64() == 3 {
			sawInput = true
		}
	}
	if !sawInput {
		t.Fatalf("missing usage attrs: %v", span.Attributes())
	}
}

func TestObserverOrphanEndStillRecords(t *testing.T) {
	recorder, tracer := newRecorder(t)
	ctx := observe.With(context.Background(), otel.Observer(tracer))
	observe.Emit(ctx, observe.Event{Kind: observe.KindNode, Phase: observe.PhaseEnd, Name: "late", Trace: observe.Trace{TraceID: "11111111111111111111111111111111", SpanID: "22222222222222222222222222222222"}, Time: time.Now(), Duration: time.Millisecond})
	if len(recorder.Ended()) != 1 {
		t.Fatalf("ended spans=%d", len(recorder.Ended()))
	}
}

// TestLateChildAfterNodeEndKeepsRealParent 复核点：节点 span 结束后才发射的
// checkpoint save 仍须挂到该节点真实 span 下，而不是落到框架 ID 合成的
// 远端父上下文所生成的另一条追踪。
func TestLateChildAfterNodeEndKeepsRealParent(t *testing.T) {
	recorder, tracer := newRecorder(t)
	ctx := observe.With(context.Background(), otel.Observer(tracer))

	runTrace := observe.Trace{TraceID: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", RunID: "r1", SpanID: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}
	nodeTrace := observe.Trace{TraceID: runTrace.TraceID, RunID: "r1", SpanID: "cccccccccccccccccccccccccccccccc", ParentSpanID: runTrace.SpanID}
	saveTrace := observe.Trace{TraceID: runTrace.TraceID, RunID: "r1", SpanID: "dddddddddddddddddddddddddddddddd", ParentSpanID: nodeTrace.SpanID}

	observe.Emit(ctx, observe.Event{Kind: observe.KindRun, Phase: observe.PhaseStart, Name: "g", Trace: runTrace, Time: time.Now()})
	observe.Emit(ctx, event(observe.PhaseStart, nodeTrace))
	observe.Emit(ctx, event(observe.PhaseEnd, nodeTrace))
	// 节点 span 已结束后 checkpoint save 才开始（同步与异步模式都会走这条路径）。
	observe.Emit(ctx, observe.Event{Kind: observe.KindCheckpoint, Phase: observe.PhaseStart, Name: "save", Trace: saveTrace, Time: time.Now()})
	observe.Emit(ctx, observe.Event{Kind: observe.KindCheckpoint, Phase: observe.PhaseEnd, Name: "save", Trace: saveTrace, Time: time.Now(), Duration: time.Millisecond})
	observe.Emit(ctx, observe.Event{Kind: observe.KindRun, Phase: observe.PhaseEnd, Name: "g", Trace: runTrace, Time: time.Now(), Duration: 10 * time.Millisecond})

	var runSpan, nodeSpan, saveSpan sdktrace.ReadOnlySpan
	for _, s := range recorder.Ended() {
		switch s.Name() {
		case "run:g":
			runSpan = s
		case "node:node":
			nodeSpan = s
		case "checkpoint:save":
			saveSpan = s
		}
	}
	if runSpan == nil || nodeSpan == nil || saveSpan == nil {
		t.Fatalf("spans=%v", recorder.Ended())
	}
	if got := saveSpan.Parent().SpanID(); got != nodeSpan.SpanContext().SpanID() {
		t.Fatalf("save parent=%s want node %s", got, nodeSpan.SpanContext().SpanID())
	}
	if saveSpan.SpanContext().TraceID() != runSpan.SpanContext().TraceID() {
		t.Fatal("save span escaped to another trace")
	}
}

// TestNestedRunEndKeepsParentSpans 嵌套 run 收尾只清理自己的子树：父节点
// 结束后迟到的 checkpoint save 仍能挂到父节点真实 span 下。
func TestNestedRunEndKeepsParentSpans(t *testing.T) {
	recorder, tracer := newRecorder(t)
	ctx := observe.With(context.Background(), otel.Observer(tracer))

	runTrace := observe.Trace{TraceID: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", RunID: "r1", SpanID: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}
	nodeTrace := observe.Trace{TraceID: runTrace.TraceID, RunID: "r1", SpanID: "cccccccccccccccccccccccccccccccc", ParentSpanID: runTrace.SpanID}
	childRun := observe.Trace{TraceID: runTrace.TraceID, RunID: "r2", SpanID: "dddddddddddddddddddddddddddddddd", ParentSpanID: nodeTrace.SpanID}
	saveTrace := observe.Trace{TraceID: runTrace.TraceID, RunID: "r1", SpanID: "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee", ParentSpanID: nodeTrace.SpanID}

	observe.Emit(ctx, observe.Event{Kind: observe.KindRun, Phase: observe.PhaseStart, Name: "g", Trace: runTrace, Time: time.Now()})
	observe.Emit(ctx, event(observe.PhaseStart, nodeTrace))
	observe.Emit(ctx, observe.Event{Kind: observe.KindRun, Phase: observe.PhaseStart, Name: "child", Trace: childRun, Time: time.Now()})
	observe.Emit(ctx, observe.Event{Kind: observe.KindRun, Phase: observe.PhaseEnd, Name: "child", Trace: childRun, Time: time.Now(), Duration: time.Millisecond})
	observe.Emit(ctx, event(observe.PhaseEnd, nodeTrace))
	// 子 run 收尾后父节点迟到的 checkpoint save：父级记录必须仍在。
	observe.Emit(ctx, observe.Event{Kind: observe.KindCheckpoint, Phase: observe.PhaseStart, Name: "save", Trace: saveTrace, Time: time.Now()})
	observe.Emit(ctx, observe.Event{Kind: observe.KindCheckpoint, Phase: observe.PhaseEnd, Name: "save", Trace: saveTrace, Time: time.Now(), Duration: time.Millisecond})
	observe.Emit(ctx, observe.Event{Kind: observe.KindRun, Phase: observe.PhaseEnd, Name: "g", Trace: runTrace, Time: time.Now(), Duration: 10 * time.Millisecond})

	var nodeSpan, saveSpan sdktrace.ReadOnlySpan
	for _, s := range recorder.Ended() {
		switch s.Name() {
		case "node:node":
			nodeSpan = s
		case "checkpoint:save":
			saveSpan = s
		}
	}
	if nodeSpan == nil || saveSpan == nil {
		t.Fatalf("spans=%v", recorder.Ended())
	}
	if got := saveSpan.Parent().SpanID(); got != nodeSpan.SpanContext().SpanID() {
		t.Fatalf("save parent=%s want node %s", got, nodeSpan.SpanContext().SpanID())
	}
}

// TestBackgroundToolEndAfterRunClosesRealSpan 复核点：图超时先返回、后台
// 工具随后才退出时，run 收尾只回收已结束记录；工具的结束事件必须收尾
// 其原始 span（真实耗时、正确父级），而不是补一条瞬时记录。
func TestBackgroundToolEndAfterRunClosesRealSpan(t *testing.T) {
	recorder, tracer := newRecorder(t)
	ctx := observe.With(context.Background(), otel.Observer(tracer))

	base := time.Now()
	runTrace := observe.Trace{TraceID: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", RunID: "r1", SpanID: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}
	nodeTrace := observe.Trace{TraceID: runTrace.TraceID, RunID: "r1", SpanID: "cccccccccccccccccccccccccccccccc", ParentSpanID: runTrace.SpanID}
	toolTrace := observe.Trace{TraceID: runTrace.TraceID, RunID: "r1", SpanID: "dddddddddddddddddddddddddddddddd", ParentSpanID: nodeTrace.SpanID}
	toolEnd := observe.Event{Kind: observe.KindTool, Phase: observe.PhaseEnd, Name: "slow", Trace: toolTrace}

	observe.Emit(ctx, observe.Event{Kind: observe.KindRun, Phase: observe.PhaseStart, Name: "g", Trace: runTrace, Time: base})
	observe.Emit(ctx, event(observe.PhaseStart, nodeTrace))
	observe.Emit(ctx, observe.Event{Kind: observe.KindTool, Phase: observe.PhaseStart, Name: "slow", Trace: toolTrace, Time: base})
	observe.Emit(ctx, event(observe.PhaseEnd, nodeTrace))
	// 图超时先返回：节点与 run 收尾，但工具仍在后台执行。
	observe.Emit(ctx, observe.Event{Kind: observe.KindRun, Phase: observe.PhaseEnd, Name: "g", Trace: runTrace, Time: base.Add(10 * time.Millisecond), Duration: 10 * time.Millisecond})
	// 工具随后才退出：结束事件必须找到并收尾原始记录。
	toolEnd.Time = base.Add(25 * time.Millisecond)
	toolEnd.Duration = 25 * time.Millisecond
	observe.Emit(ctx, toolEnd)

	var nodeSpan, toolSpan sdktrace.ReadOnlySpan
	for _, s := range recorder.Ended() {
		switch s.Name() {
		case "node:node":
			nodeSpan = s
		case "tool:slow":
			toolSpan = s
		}
	}
	if nodeSpan == nil || toolSpan == nil {
		t.Fatalf("spans=%v", recorder.Ended())
	}
	if got := toolSpan.Parent().SpanID(); got != nodeSpan.SpanContext().SpanID() {
		t.Fatalf("tool parent=%s want node %s", got, nodeSpan.SpanContext().SpanID())
	}
	// 原始 span 收尾：起止时间对应真实执行区间，不是补发的瞬时记录。
	if got := toolSpan.EndTime().Sub(toolSpan.StartTime()); got != 25*time.Millisecond {
		t.Fatalf("tool span duration=%v", got)
	}
}
