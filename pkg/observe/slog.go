package observe

import (
	"context"
	"log/slog"
	"strings"
	"sync"
)

// Slog 返回把事件打印为缩进调用树的 Observer：start 按 span 深度缩进，
// end/error 追加耗时，llm end 附 token 用量。l 为 nil 时用 slog.Default()。
func Slog(l *slog.Logger) Observer {
	if l == nil {
		l = slog.Default()
	}
	return &slogObserver{logger: l, depths: map[string]int{}}
}

type slogObserver struct {
	logger *slog.Logger
	mu     sync.Mutex
	depths map[string]int // spanID → 树深度；end/error 消费后删除
}

func (o *slogObserver) OnEvent(ctx context.Context, event Event) {
	depth := o.depth(event)
	attrs := make([]slog.Attr, 0, len(event.Attrs)+10)
	attrs = append(attrs,
		slog.String("name", event.Name),
		slog.String("kind", string(event.Kind)),
		slog.String("phase", string(event.Phase)),
		slog.String("trace_id", event.Trace.TraceID),
		slog.String("span_id", event.Trace.SpanID),
		slog.String("parent_span_id", event.Trace.ParentSpanID),
		slog.String("run_id", event.Trace.RunID))
	if event.Duration > 0 {
		attrs = append(attrs, slog.Duration("duration", event.Duration))
	}
	if event.Usage != nil {
		attrs = append(attrs,
			slog.Int("input_tokens", event.Usage.InputTokens),
			slog.Int("output_tokens", event.Usage.OutputTokens),
			slog.Int("total_tokens", event.Usage.TotalTokens))
	}
	if event.Err != nil {
		attrs = append(attrs, slog.String("error", event.Err.Error()))
	}
	for key, value := range event.Attrs {
		// 身份字段取自事件本身，防止业务元数据覆盖日志关联键。
		switch key {
		case "kind", "phase", "trace_id", "span_id", "parent_span_id", "run_id":
			continue
		}
		attrs = append(attrs, slog.Any(key, value))
	}
	if event.Payload != nil {
		attrs = append(attrs, slog.Any("payload", event.Payload))
	}
	level := slog.LevelInfo
	if event.Phase == PhaseError {
		level = slog.LevelError
	}
	msg := strings.Repeat("  ", depth) + string(event.Kind) + " " + string(event.Phase)
	o.logger.LogAttrs(ctx, level, msg, attrs...)
}

func (o *slogObserver) depth(event Event) int {
	o.mu.Lock()
	defer o.mu.Unlock()
	switch event.Phase {
	case PhaseStart:
		depth := o.depths[event.Trace.ParentSpanID]
		o.depths[event.Trace.SpanID] = depth + 1
		return depth
	case PhaseEnd, PhaseError:
		depth, ok := o.depths[event.Trace.SpanID]
		if !ok {
			return 0
		}
		delete(o.depths, event.Trace.SpanID)
		return depth - 1
	default:
		return o.depths[event.Trace.SpanID]
	}
}
