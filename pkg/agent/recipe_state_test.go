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
	toolAgent, err := agent.New(agent.BaseConfig{
		Model: llm.NewScriptedModel(
			[]schema.Message{functionCallMessage("search", "call-1")},
			[]schema.Message{schema.AssistantMessage("done")},
		),
		Tools: []tool.Tool{search},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := toolAgent.Invoke(ctx, []schema.Message{schema.UserMessage("find")}); err != nil {
		t.Fatal(err)
	}

	first, err := agent.NewBasicAgent(agent.BaseConfig{
		Name:  "first",
		Model: llm.NewScriptedModel([]schema.Message{schema.AssistantMessage("one")}),
	})
	if err != nil {
		t.Fatal(err)
	}
	second, err := agent.NewBasicAgent(agent.BaseConfig{
		Name:  "second",
		Model: llm.NewScriptedModel([]schema.Message{schema.AssistantMessage("two")}),
	})
	if err != nil {
		t.Fatal(err)
	}
	group, err := agent.NewGroupChat(agent.GroupChatConfig{Agents: []agent.Agent{first, second}, MaxRounds: 2})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := group.Invoke(ctx, []schema.Message{schema.UserMessage("discuss")}); err != nil {
		t.Fatal(err)
	}

	if seen.Load() == 0 {
		t.Fatal("graph control events were not published")
	}
}
