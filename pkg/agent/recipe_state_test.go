package agent_test

import (
	"context"
	"reflect"
	"sync/atomic"
	"testing"

	"github.com/goharness/goharness/pkg/agent"
	"github.com/goharness/goharness/pkg/llm"
	"github.com/goharness/goharness/pkg/observe"
	"github.com/goharness/goharness/pkg/schema"
	"github.com/goharness/goharness/pkg/tool"
)

func TestAgentRecipesKeepControlStateOutOfGraphValues(t *testing.T) {
	if _, exists := reflect.TypeOf(observe.Event{}).FieldByName("State"); exists {
		t.Fatal("observe event exposes erased state")
	}
	var seen atomic.Int32
	ctx := observe.With(context.Background(), observe.ObserverFunc(func(_ context.Context, _ observe.Event) {
		seen.Add(1)
	}))

	search := tool.NewFunctionTool(tool.Spec{Name: "search", IsReadOnly: true}, func(context.Context, tool.CallInput) (tool.Result, error) {
		return tool.TextResult("found"), nil
	})
	toolAgent, err := agent.New(llm.NewScriptedModel(
		[]schema.Message{functionCallMessage("search", "call-1")},
		[]schema.Message{schema.AssistantMessage("done")},
	), agent.WithTools(search))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := toolAgent.Invoke(ctx, []schema.Message{schema.UserMessage("find")}); err != nil {
		t.Fatal(err)
	}

	member, err := agent.New(llm.NewScriptedModel([]schema.Message{schema.AssistantMessage("one")}), agent.WithName("member"))
	if err != nil {
		t.Fatal(err)
	}
	// 编排图中的子 Agent 节点同样不得把 State 泄漏进观测事件。
	orchestration := newSupervisorApp(t, "group", llm.NewScriptedModel(
		[]schema.Message{routingCall("member")},
		[]schema.Message{schema.AssistantMessage("done")},
	), false, 2, member)
	if _, err := orchestration.Invoke(ctx, supervisorState{Messages: []schema.Message{schema.UserMessage("discuss")}}); err != nil {
		t.Fatal(err)
	}

	if seen.Load() == 0 {
		t.Fatal("graph control events were not published")
	}
}
