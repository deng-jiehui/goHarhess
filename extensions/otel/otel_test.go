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
