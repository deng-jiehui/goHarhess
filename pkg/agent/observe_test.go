package agent_test

import (
	"context"
	"sync"
	"testing"

	"github.com/goharness/goharness/pkg/agent"
	"github.com/goharness/goharness/pkg/llm"
	"github.com/goharness/goharness/pkg/observe"
	"github.com/goharness/goharness/pkg/schema"
	"github.com/goharness/goharness/pkg/tool"
)

type observeLog struct {
	mu     sync.Mutex
	events []observe.Event
}

func (l *observeLog) OnEvent(_ context.Context, event observe.Event) {
	l.mu.Lock()
	l.events = append(l.events, event)
	l.mu.Unlock()
}

// TestObserveReactParallelToolTree 黄金序列：一次 ReAct 运行中模型先并行发起
// 两个工具调用再作答；全部 llm/tool span 必须落在同一条 trace 上、并挂到
// run 调用树内。
func TestObserveReactParallelToolTree(t *testing.T) {
	log := &observeLog{}
	calls := schema.NewMessage(schema.RoleAssistant,
		schema.NewFunctionToolCallBlock("call-1", "search", `{}`),
		schema.NewFunctionToolCallBlock("call-2", "fetch", `{}`),
	)
	model := llm.NewScriptedModel(
		[]schema.Message{calls},
		[]schema.Message{schema.AssistantMessage("done")},
	)
	value, err := agent.NewReactAgent(agent.ReactAgentConfig{BaseConfig: agent.BaseConfig{
		Name:  "react",
		Model: model,
		Tools: []tool.Tool{
			tool.NewFunctionTool(tool.Spec{Name: "search", IsReadOnly: true}, func(context.Context, tool.CallInput) (tool.Result, error) { return tool.TextResult("found"), nil }),
			tool.NewFunctionTool(tool.Spec{Name: "fetch", IsReadOnly: true}, func(context.Context, tool.CallInput) (tool.Result, error) { return tool.TextResult("page"), nil }),
		},
	}})
	if err != nil {
		t.Fatal(err)
	}
	ctx := observe.With(context.Background(), log)
	if _, err := value.Invoke(ctx, []schema.Message{schema.UserMessage("hi")}); err != nil {
		t.Fatal(err)
	}
	log.mu.Lock()
	defer log.mu.Unlock()
	var llmStart, llmEnd, toolStart, toolEnd, runEnd int
	toolNames := map[string]bool{}
	for _, event := range log.events {
		switch {
		case event.Kind == observe.KindLLM && event.Phase == observe.PhaseStart:
			llmStart++
		case event.Kind == observe.KindLLM && event.Phase == observe.PhaseEnd:
			llmEnd++
		case event.Kind == observe.KindTool && event.Phase == observe.PhaseStart:
			toolStart++
			toolNames[event.Name] = true
		case event.Kind == observe.KindTool && event.Phase == observe.PhaseEnd:
			toolEnd++
		case event.Kind == observe.KindRun && event.Phase == observe.PhaseEnd:
			runEnd++
		}
	}
	if llmStart != 2 || llmEnd != 2 {
		t.Fatalf("llm events start=%d end=%d, want 2/2", llmStart, llmEnd)
	}
	if toolStart != 2 || toolEnd != 2 || !toolNames["search"] || !toolNames["fetch"] {
		t.Fatalf("tool events = %v start=%d end=%d", toolNames, toolStart, toolEnd)
	}
	if runEnd != 1 {
		t.Fatalf("run end = %d", runEnd)
	}
	// 全链路同 trace；llm/tool span 都有父 span（挂在节点之下）。
	var traceID string
	for _, event := range log.events {
		if traceID == "" {
			traceID = event.Trace.TraceID
		}
		if event.Trace.TraceID != traceID {
			t.Fatalf("mixed traces: %q vs %q", event.Trace.TraceID, traceID)
		}
		if (event.Kind == observe.KindLLM || event.Kind == observe.KindTool) && event.Trace.ParentSpanID == "" {
			t.Fatalf("%s %s has no parent span", event.Kind, event.Name)
		}
	}
}
