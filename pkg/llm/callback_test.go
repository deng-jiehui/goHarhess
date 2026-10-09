package llm_test

import (
	"context"
	"testing"

	"github.com/goharness/goharness/pkg/llm"
	"github.com/goharness/goharness/pkg/observe"
	"github.com/goharness/goharness/pkg/schema"
)

func TestBeginCallbackEmitsLLMStartEventWithRedactedPayload(t *testing.T) {
	var events []observe.Event
	ctx := observe.With(context.Background(),
		observe.CapturePayload(func(_ observe.Event, raw any) any { return raw }),
		observe.ObserverFunc(func(_ context.Context, event observe.Event) { events = append(events, event) }),
	)
	request := llm.Request{Messages: []schema.Message{{Role: schema.RoleUser, ContentBlocks: []schema.ContentBlock{schema.NewTextBlock("hello")}}}}
	modelCtx, invocation := llm.BeginCallback(ctx, "model", nil, request)
	if invocation.Trace.SpanID == "" {
		t.Fatal("invocation missing span")
	}
	llm.EndCallback(modelCtx, invocation, llm.Response{Messages: []schema.Message{schema.AssistantMessage("ok")}})
	if len(events) != 2 || events[0].Kind != observe.KindLLM || events[0].Phase != observe.PhaseStart {
		t.Fatalf("events = %#v", events)
	}
	if events[1].Phase != observe.PhaseEnd {
		t.Fatalf("end event = %#v", events[1])
	}
	payload, ok := events[0].Payload.(llm.Request)
	if !ok || len(payload.Messages) != 1 {
		t.Fatalf("payload = %#v", events[0].Payload)
	}
}

// TestCapturePayloadMutationCannotReachBusinessRequest 复现审查 P1：
// 脱敏对载荷的改写不能污染模型实际收到的请求。
func TestCapturePayloadMutationCannotReachBusinessRequest(t *testing.T) {
	var seen llm.Request
	node, err := llm.NewChatModelNode(llm.NewHandlerModel(
		func(_ context.Context, request llm.Request, _ llm.Options) ([]schema.Message, error) {
			seen = request
			return []schema.Message{schema.AssistantMessage("ok")}, nil
		}))
	if err != nil {
		t.Fatal(err)
	}
	var events []observe.Event
	ctx := observe.With(context.Background(),
		observe.CapturePayload(func(_ observe.Event, raw any) any {
			request := raw.(llm.Request)
			// 深克隆后改写：若隔离失效，模型将收到 "***"。
			request.Messages[0].ContentBlocks[0].Text.Text = "***"
			return request
		}),
		observe.ObserverFunc(func(_ context.Context, event observe.Event) { events = append(events, event) }),
	)
	request := llm.Request{Messages: []schema.Message{{
		Role:          schema.RoleUser,
		ContentBlocks: []schema.ContentBlock{schema.NewTextBlock("secret")},
	}}}
	if _, err := node.Invoke(ctx, request); err != nil {
		t.Fatal(err)
	}
	if got := seen.Messages[0].JoinedText(""); got != "secret" {
		t.Fatalf("model received mutated request: %q", got)
	}
	payload, ok := events[0].Payload.(llm.Request)
	if !ok || payload.Messages[0].JoinedText("") != "***" {
		t.Fatalf("payload = %#v", events[0].Payload)
	}
	if request.Messages[0].JoinedText("") != "secret" {
		t.Fatal("redact mutated caller request")
	}
}
