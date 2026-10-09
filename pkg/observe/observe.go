package observe

import (
	"context"
	"log/slog"
	"time"
)

type observersContextKey struct{}
type payloadContextKey struct{}

// RedactFunc 把发射点的原始载荷转成可观测形态；返回 nil 时该事件不带载荷。
type RedactFunc func(Event, any) any

// payloadRequest 由 CapturePayload 产生：它在 With 中注册脱敏钩子，
// 自身不是事件订阅者。
type payloadRequest struct {
	redact RedactFunc
}

func (payloadRequest) OnEvent(context.Context, Event) {}

// CapturePayload 显式开启载荷捕获：把它交给 With 后，发射点在事件中
// 填充经 redact 脱敏的 Payload。默认事件只有元数据，不含 prompt 与回复。
// 必须作为 With 的直接参数传入——包进 Multi 后 With 无法识别脱敏钩子。
func CapturePayload(redact RedactFunc) Observer {
	return payloadRequest{redact: redact}
}

// With 把 Observer 注册到 Context，一次运行的全部组件事件都会送达。
// 同步分发：Invoke 返回前事件已处理完毕。慢速观察用 Async 包装。
func With(ctx context.Context, observers ...Observer) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	existing, _ := ctx.Value(observersContextKey{}).([]Observer)
	combined := append([]Observer(nil), existing...)
	for _, o := range observers {
		if o == nil {
			continue
		}
		if req, ok := o.(payloadRequest); ok {
			ctx = context.WithValue(ctx, payloadContextKey{}, req)
			continue
		}
		combined = append(combined, o)
	}
	return context.WithValue(ctx, observersContextKey{}, combined)
}

// Active 报告 ctx 上是否注册了 Observer；发射点据此决定是否组装事件。
func Active(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	observers, _ := ctx.Value(observersContextKey{}).([]Observer)
	return len(observers) > 0
}

// Emit 把事件同步交给注册的全部 Observer。单个 Observer 的 panic 被捕获
// 并记录，不影响业务结果与其余 Observer。
func Emit(ctx context.Context, event Event) {
	emit(ctx, event, nil)
}

// EmitPayload 与 Emit 相同，但 raw 是发射点的原始载荷：仅当 ctx 经
// CapturePayload 注册脱敏钩子时，载荷脱敏后进入 Event.Payload。
func EmitPayload(ctx context.Context, event Event, raw any) {
	emit(ctx, event, func() any {
		req, _ := ctx.Value(payloadContextKey{}).(payloadRequest)
		if req.redact == nil {
			return nil
		}
		return redactPayload(ctx, event, req.redact, raw)
	})
}

// redactPayload 先把载荷克隆成独立副本再交给用户脱敏函数：脱敏可以就地
// 改写副本（如把密钥替换为 "***"），业务数据与调用方持有的原始输入不受
// 影响。脱敏函数本身也可能 panic：异常在此被隔离并记录，事件继续以无
// Payload 的形态分发——不能回退到未脱敏的原始载荷。
func redactPayload(ctx context.Context, event Event, redact RedactFunc, raw any) (payload any) {
	defer func() {
		if recovered := recover(); recovered != nil {
			payload = nil
			slog.Default().LogAttrs(ctx, slog.LevelWarn, "observe: redact panicked",
				slog.String("kind", string(event.Kind)),
				slog.String("phase", string(event.Phase)),
				slog.String("name", event.Name),
				slog.String("trace_id", event.Trace.TraceID),
				slog.String("span_id", event.Trace.SpanID),
				slog.Any("panic", recovered))
		}
	}()
	return redact(event, CloneValue(raw))
}

func emit(ctx context.Context, event Event, payload func() any) {
	if ctx == nil {
		return
	}
	observers, _ := ctx.Value(observersContextKey{}).([]Observer)
	if len(observers) == 0 {
		return
	}
	if event.Time.IsZero() {
		event.Time = time.Now()
	}
	if event.Trace == (Trace{}) {
		event.Trace, _ = TraceFromContext(ctx)
	}
	if payload != nil {
		event.Payload = payload()
	}
	for _, observer := range observers {
		dispatchOne(ctx, observer, event)
	}
}

func dispatchOne(ctx context.Context, observer Observer, event Event) {
	defer func() {
		if recovered := recover(); recovered != nil {
			slog.Default().LogAttrs(ctx, slog.LevelWarn, "observe: observer panicked",
				slog.String("kind", string(event.Kind)),
				slog.String("phase", string(event.Phase)),
				slog.String("name", event.Name),
				slog.String("trace_id", event.Trace.TraceID),
				slog.String("span_id", event.Trace.SpanID),
				slog.Any("panic", recovered))
		}
	}()
	observer.OnEvent(ctx, event)
}
