package agent_test

import (
	"context"
	"testing"

	"github.com/goharness/goharness/pkg/agent"
	"github.com/goharness/goharness/pkg/llm"
	"github.com/goharness/goharness/pkg/observe"
	"github.com/goharness/goharness/pkg/schema"
	"github.com/goharness/goharness/pkg/tool"
)

// TestRunEventsCarryAgentAttr（W2-1）：命名 Agent 的运行事件及全部子事件
// 的 Attrs 都携带 agent 键，供指标标签与 span 属性使用；嵌套的无名运行
// 显式覆盖为继承的空值，不沿用外层父级名。
func TestRunEventsCarryAgentAttr(t *testing.T) {
	log := &observeLog{}
	named, err := agent.New(llm.NewScriptedModel([]schema.Message{schema.AssistantMessage("ok")}), agent.WithName("planner"))
	if err != nil {
		t.Fatal(err)
	}
	ctx := observe.With(context.Background(), log)
	if _, err := named.Invoke(ctx, []schema.Message{schema.UserMessage("hi")}); err != nil {
		t.Fatal(err)
	}
	log.mu.Lock()
	if len(log.events) == 0 {
		t.Fatal("no events")
	}
	for _, event := range log.events {
		if event.Attrs["agent"] != "planner" {
			t.Fatalf("event %s/%s attrs=%v, want agent=planner", event.Kind, event.Phase, event.Attrs)
		}
	}
	log.mu.Unlock()

	// 命名父 Agent 的工具调用里嵌套无名运行：子运行事件 agent 为空，
	// 不继承父级名。
	log2 := &observeLog{}
	inner, err := agent.New(llm.NewScriptedModel([]schema.Message{schema.AssistantMessage("inner-ok")}))
	if err != nil {
		t.Fatal(err)
	}
	parentModel := llm.NewScriptedModel(
		[]schema.Message{schema.NewMessage(schema.RoleAssistant, schema.NewFunctionToolCallBlock("c1", "inner", `{}`))},
		[]schema.Message{schema.AssistantMessage("done")},
	)
	parent, err := agent.New(parentModel, agent.WithName("outer"), agent.WithTools(
		tool.NewFunctionTool(tool.Spec{Name: "inner"}, func(ctx context.Context, _ tool.CallInput) (tool.Result, error) {
			out, err := inner.Invoke(ctx, []schema.Message{schema.UserMessage("go")})
			if err != nil {
				return tool.Result{}, err
			}
			return tool.TextResult(out[len(out)-1].JoinedText("")), nil
		})))
	if err != nil {
		t.Fatal(err)
	}
	ctx2 := observe.With(context.Background(), log2)
	if _, err := parent.Invoke(ctx2, []schema.Message{schema.UserMessage("go")}); err != nil {
		t.Fatal(err)
	}
	log2.mu.Lock()
	defer log2.mu.Unlock()
	seen := map[string]bool{}
	for _, event := range log2.events {
		name, _ := event.Attrs["agent"].(string)
		seen[name] = true
		if event.Kind == observe.KindRun && name != "outer" && name != "" {
			t.Fatalf("run event agent=%q attrs=%v", name, event.Attrs)
		}
	}
	if !seen["outer"] || !seen[""] {
		t.Fatalf("agent values seen = %v, want outer and empty", seen)
	}
}
