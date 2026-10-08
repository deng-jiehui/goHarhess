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
