package tenant_test

import (
	"context"
	"sync"
	"testing"

	otelobs "github.com/goharness/goharness/extensions/otel"
	"github.com/goharness/goharness/extensions/tenant"
	"github.com/goharness/goharness/pkg/agent"
	"github.com/goharness/goharness/pkg/llm"
	"github.com/goharness/goharness/pkg/observe"
	"github.com/goharness/goharness/pkg/schema"
	"github.com/goharness/goharness/pkg/state"
	"github.com/goharness/goharness/pkg/tool"

	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// 契约 6.3：tenant.With 作用域下运行带工具调用与检查点的 ReAct，
// run/node/llm/tool/checkpoint 每个事件的 Attrs 都带 tenant_id 与
// user_id，OTel 的每个 span 也带同样属性。
func TestTenantAttrsPropagateToEventsAndSpans(t *testing.T) {
	model := llm.NewScriptedModel(
		[]schema.Message{schema.NewMessage(schema.RoleAssistant, schema.NewFunctionToolCallBlock("call-1", "echo", `{"x":1}`))},
		[]schema.Message{schema.AssistantMessage("done")},
	)
	a, err := agent.New(model,
		agent.WithTools(tool.NewFunctionTool(tool.Spec{Name: "echo"}, func(context.Context, tool.CallInput) (tool.Result, error) {
			return tool.TextResult("ok"), nil
		})),
		agent.WithCheckpointStore(state.NewMemoryStore()),
	)
	if err != nil {
		t.Fatal(err)
	}
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	defer provider.Shutdown(context.Background())

	var mu sync.Mutex
	var events []observe.Event
	collector := observe.ObserverFunc(func(_ context.Context, e observe.Event) {
		mu.Lock()
		events = append(events, e)
		mu.Unlock()
	})
	ctx := observe.With(context.Background(), observe.Multi(collector, otelobs.Observer(provider.Tracer("test"))))
	ctx, err = tenant.With(ctx, tool.Subject{TenantID: "tenant-a", ID: "user-1"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.Invoke(ctx, []schema.Message{schema.UserMessage("hi")}, agent.WithConversationID("s1")); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(events) == 0 {
		t.Fatal("no events captured")
	}
	for _, e := range events {
		if e.Attrs[observe.AttrTenantID] != "tenant-a" || e.Attrs[observe.AttrUserID] != "user-1" {
			t.Fatalf("event kind=%s phase=%s attrs=%v missing tenant attrs", e.Kind, e.Phase, e.Attrs)
		}
	}
	spans := recorder.Ended()
	if len(spans) == 0 {
		t.Fatal("no spans recorded")
	}
	for _, span := range spans {
		got := map[attribute.Key]string{}
		for _, kv := range span.Attributes() {
			got[kv.Key] = kv.Value.AsString()
		}
		if got["tenant_id"] != "tenant-a" || got["user_id"] != "user-1" {
			t.Fatalf("span %q attributes missing tenant attrs: %v", span.Name(), span.Attributes())
		}
	}
}
