package app

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/goharness/autoresearch/internal/domain"
	"github.com/goharness/goharness/pkg/observe"
	"github.com/goharness/goharness/pkg/schema"
)

// runtimeEventPayload 是开发诊断用的脱敏事件摘要，不包含模型请求和回复正文。
type runtimeEventPayload struct {
	Kind       string             `json:"kind"`
	Phase      string             `json:"phase"`
	Name       string             `json:"name,omitempty"`
	Time       time.Time          `json:"time"`
	DurationMS int64              `json:"duration_ms,omitempty"`
	TraceID    string             `json:"trace_id,omitempty"`
	SpanID     string             `json:"span_id,omitempty"`
	ParentSpan string             `json:"parent_span_id,omitempty"`
	Attrs      map[string]any     `json:"attrs,omitempty"`
	Usage      *schema.TokenUsage `json:"usage,omitempty"`
	Error      string             `json:"error,omitempty"`
}

func marshalRuntimeEvent(event observe.Event) ([]byte, error) {
	payload := runtimeEventPayload{
		Kind:       string(event.Kind),
		Phase:      string(event.Phase),
		Name:       event.Name,
		Time:       event.Time,
		DurationMS: event.Duration.Milliseconds(),
		TraceID:    event.Trace.TraceID,
		SpanID:     event.Trace.SpanID,
		ParentSpan: event.Trace.ParentSpanID,
		Attrs:      cloneRuntimeAttrs(event.Attrs),
		Usage:      cloneRuntimeUsage(event.Usage),
	}
	if event.Err != nil {
		payload.Error = event.Err.Error()
	}
	return json.Marshal(payload)
}

func cloneRuntimeAttrs(attrs map[string]any) map[string]any {
	if len(attrs) == 0 {
		return nil
	}
	cloned := make(map[string]any, len(attrs))
	for key, value := range attrs {
		cloned[key] = value
	}
	return cloned
}

func cloneRuntimeUsage(usage *schema.TokenUsage) *schema.TokenUsage {
	if usage == nil {
		return nil
	}
	cloned := *usage
	cloned.Extra = cloneRuntimeAttrs(usage.Extra)
	return &cloned
}

// runtimeEventSink 把观测事件写入已有 events 表；写入失败只影响诊断记录，
// 不应改变研究图的业务结果。
type runtimeEventSink struct {
	runtime   domain.RuntimeRepository
	scope     domain.Scope
	sessionID string
	turnID    string
	attempt   int
	sequence  atomic.Uint64
}

func (s *runtimeEventSink) OnEvent(_ context.Context, event observe.Event) {
	if s == nil || s.runtime == nil {
		return
	}
	payload, err := marshalRuntimeEvent(event)
	if err != nil {
		slog.Default().Error("运行事件编码失败", "error", err, "kind", event.Kind, "phase", event.Phase)
		return
	}
	_, err = s.runtime.AppendEvent(context.Background(), s.scope, domain.SessionEvent{
		SessionID:   s.sessionID,
		TurnID:      s.turnID,
		Attempt:     s.attempt,
		Kind:        "runtime." + string(event.Kind),
		PayloadJSON: payload,
		StreamRunID: event.Trace.RunID,
		Sequence:    s.sequence.Add(1),
	})
	if err != nil {
		slog.Default().Error("运行事件保存失败", "error", err, "kind", event.Kind, "phase", event.Phase)
	}
}

func newRuntimeEventObserver(runtime domain.RuntimeRepository, scope domain.Scope, sessionID, turnID string, attempt int) *observe.AsyncObserver {
	return observe.Async(&runtimeEventSink{
		runtime: runtime, scope: scope, sessionID: sessionID, turnID: turnID, attempt: attempt,
	}, 256, false)
}
