package observe

import (
	"context"
	"time"

	"github.com/goharness/goharness/pkg/schema"
)

// Kind 是事件的组件类别。核心定义 run / node / llm / tool / interrupt /
// checkpoint；扩展可自定义（如 retrieval、mcp）。
type Kind string

const (
	KindRun        Kind = "run"
	KindNode       Kind = "node"
	KindLLM        Kind = "llm"
	KindTool       Kind = "tool"
	KindInterrupt  Kind = "interrupt"
	KindCheckpoint Kind = "checkpoint"
)

// 观测元数据与事件属性共用的规范键名：tenant、limits、otel 等组件统一引用，
// 避免各处自造同义词。conversation_id 沿用 agent 已使用的键，不另造
// session_id。
const (
	AttrTenantID       = "tenant_id"
	AttrUserID         = "user_id"
	AttrConversationID = "conversation_id"
)

// Event 是一次 span 级生命周期事件，不含 token 级流式增量。
type Event struct {
	Kind     Kind
	Phase    Phase // start / end / error；瞬时事件用 event
	Name     string
	Trace    Trace // TraceID、SpanID、ParentSpanID、RunID
	Time     time.Time
	Duration time.Duration      // end、error 时有效
	Usage    *schema.TokenUsage // llm end 时有效
	Err      error
	Attrs    map[string]any // step、visit、finish_reason、tool_call_id 等少量结构化属性
	// Payload 是脱敏后的调用载荷，默认 nil；仅当 ctx 经 CapturePayload
	// 注册脱敏钩子后由发射点填充。Observer 只读，不得修改。
	Payload any
}

// Observer 接收运行事件。实现必须并发安全、尽快返回；耗时上报交给
// 接入组件自己的后台队列，或用 Async 包装。
type Observer interface {
	OnEvent(context.Context, Event)
}

// ObserverFunc adapts a function to Observer.
type ObserverFunc func(context.Context, Event)

func (f ObserverFunc) OnEvent(ctx context.Context, event Event) {
	if f != nil {
		f(ctx, event)
	}
}

// EventOf 把 Invocation 快照映射为事件骨架：Phase、Trace、Time、Duration
// 取自快照，Attrs 为快照元数据的克隆，attrs 中的键覆盖同名元数据。
func EventOf(inv Invocation, kind Kind, attrs map[string]any) Event {
	merged := CloneMetadata(inv.Metadata)
	for key, value := range attrs {
		merged[key] = value
	}
	return Event{
		Kind:     kind,
		Phase:    inv.Phase,
		Name:     inv.Operation,
		Trace:    inv.Trace,
		Time:     inv.Timestamp,
		Duration: inv.Duration,
		Attrs:    merged,
	}
}

// Multi 把多个 Observer 合成一个，按注册顺序同步调用。每个 Observer
// 经 dispatchOne 单独分发：单个 panic 被隔离并记录，后续 Observer 照常
// 收到事件，与直接注册到 With 的行为一致。
func Multi(observers ...Observer) Observer {
	filtered := make([]Observer, 0, len(observers))
	for _, o := range observers {
		if o != nil {
			filtered = append(filtered, o)
		}
	}
	return ObserverFunc(func(ctx context.Context, event Event) {
		for _, o := range filtered {
			dispatchOne(ctx, o, event)
		}
	})
}
