// Package otel 提供一行接入 OpenTelemetry 的 Observer：把 observe 事件
// 流映射为 OTel span——start 开 span，end/error 收尾并回填耗时与 token
// 用量，瞬时阶段（output/event）作为 span event 附加。
//
//	ctx := observe.With(ctx, otel.Observer(tracer))
//
// 本扩展只做桥接；exporter、sampler、resource 由调用方在
// TracerProvider 上配置。
package otel

import (
	"context"
	"fmt"
	"sync"

	"github.com/goharness/goharness/pkg/observe"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// Observer 返回把 observe.Event 映射为 OTel span 的观测者。
// tracer 为 nil 时退化为 noop，安全用于可选接入。
func Observer(tracer trace.Tracer) observe.Observer {
	if tracer == nil {
		tracer = trace.NewNoopTracerProvider().Tracer("goharness")
	}
	return &bridge{tracer: tracer, spans: map[string]trace.Span{}}
}

// With 是 observe.With(ctx, Observer(tracer)) 的便捷形式。
func With(ctx context.Context, tracer trace.Tracer) context.Context {
	return observe.With(ctx, Observer(tracer))
}

type bridge struct {
	tracer trace.Tracer
	mu     sync.Mutex
	spans  map[string]trace.Span
}

func (b *bridge) OnEvent(ctx context.Context, event observe.Event) {
	switch event.Phase {
	case observe.PhaseStart:
		b.start(ctx, event)
	case observe.PhaseEnd:
		b.finish(event, nil)
	case observe.PhaseError:
		b.finish(event, event.Err)
	default:
		b.annotate(event)
	}
}

func (b *bridge) start(ctx context.Context, event observe.Event) {
	parent := b.parentContext(ctx, event)
	_, span := b.tracer.Start(parent, spanName(event),
		trace.WithTimestamp(event.Time),
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(attrsOf(event)...),
	)
	b.mu.Lock()
	b.spans[event.Trace.SpanID] = span
	b.mu.Unlock()
}

func (b *bridge) finish(event observe.Event, err error) {
	b.mu.Lock()
	span, ok := b.spans[event.Trace.SpanID]
	delete(b.spans, event.Trace.SpanID)
	b.mu.Unlock()
	if !ok {
		// 未见过 start（如调用方只注册了下游阶段）：补一条瞬时 span，
		// 保证错误与 token 用量不丢。
		_, span = b.tracer.Start(context.Background(), spanName(event),
			trace.WithTimestamp(event.Time.Add(-event.Duration)),
			trace.WithSpanKind(trace.SpanKindInternal),
		)
	}
	if attrs := attrsOf(event); len(attrs) > 0 {
		span.SetAttributes(attrs...)
	}
	if usage := event.Usage; usage != nil {
		span.SetAttributes(
			attribute.Int("llm.usage.input_tokens", usage.InputTokens),
			attribute.Int("llm.usage.output_tokens", usage.OutputTokens),
			attribute.Int("llm.usage.total_tokens", usage.TotalTokens),
		)
		if usage.ReasoningTokens != 0 {
			span.SetAttributes(attribute.Int("llm.usage.reasoning_tokens", usage.ReasoningTokens))
		}
	}
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	} else {
		span.SetStatus(codes.Ok, "")
	}
	span.End(trace.WithTimestamp(event.Time))
}

// annotate 把瞬时阶段记为宿主 span 上的 event；宿主 span 未找到时丢弃，
// 与 observe 的尽力而为语义一致。
func (b *bridge) annotate(event observe.Event) {
	b.mu.Lock()
	span, ok := b.spans[event.Trace.SpanID]
	b.mu.Unlock()
	if !ok {
		return
	}
	span.AddEvent(string(event.Kind)+"."+string(event.Phase)+":"+event.Name,
		trace.WithTimestamp(event.Time),
		trace.WithAttributes(attrsOf(event)...),
	)
}

// parentContext 优先复用已登记的父 span；父 span 缺失（如只观测子树）
// 时用 TraceID/ParentSpanID 合成远端父上下文，保证追踪不断链。
func (b *bridge) parentContext(ctx context.Context, event observe.Event) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	b.mu.Lock()
	if parent, ok := b.spans[event.Trace.ParentSpanID]; ok {
		b.mu.Unlock()
		return trace.ContextWithSpanContext(ctx, parent.SpanContext())
	}
	b.mu.Unlock()
	traceID, err := trace.TraceIDFromHex(truncateHex(event.Trace.TraceID, 32))
	if err != nil || !traceID.IsValid() {
		return ctx
	}
	spanID, err := trace.SpanIDFromHex(truncateHex(event.Trace.ParentSpanID, 16))
	if err != nil || !spanID.IsValid() {
		return ctx
	}
	remote := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    traceID,
		SpanID:     spanID,
		TraceFlags: trace.FlagsSampled,
		Remote:     true,
	})
	return trace.ContextWithRemoteSpanContext(ctx, remote)
}

// spanName 按「类别:操作名」命名，调用树在 tracing 后端可直接分组成层。
func spanName(event observe.Event) string {
	if event.Kind == "" {
		return event.Name
	}
	return string(event.Kind) + ":" + event.Name
}

// attrsOf 把事件属性摊平为 OTel attribute；复合值转字符串，避免反射开销。
func attrsOf(event observe.Event) []attribute.KeyValue {
	if event.Attrs == nil {
		return nil
	}
	out := make([]attribute.KeyValue, 0, len(event.Attrs)+1)
	for key, value := range event.Attrs {
		switch v := value.(type) {
		case string:
			out = append(out, attribute.String(key, v))
		case bool:
			out = append(out, attribute.Bool(key, v))
		case int:
			out = append(out, attribute.Int(key, v))
		case int64:
			out = append(out, attribute.Int64(key, v))
		case float64:
			out = append(out, attribute.Float64(key, v))
		default:
			out = append(out, attribute.String(key, fmt.Sprint(v)))
		}
	}
	return out
}

// truncateHex 取 id 末尾 size 个十六进制字符：observe 的 SpanID 比
// OTel 的 8 字节 SpanID 长，截取保持低位一致且合法。
func truncateHex(id string, size int) string {
	if len(id) > size {
		return id[len(id)-size:]
	}
	return id
}
