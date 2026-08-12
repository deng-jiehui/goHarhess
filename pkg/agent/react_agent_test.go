package agent_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"goharness/pkg/agent"
	"goharness/pkg/llm"
	"goharness/pkg/schema"
	"goharness/pkg/tool"
)

func TestReactAgentTextActionLoop(t *testing.T) {
	echo := tool.NewFunctionTool(tool.Spec{
		Name:        "echo",
		Description: "Returns the supplied text.",
	}, func(ctx context.Context, input tool.CallInput) (tool.Result, error) {
		return tool.TextResult(fmt.Sprint(input.Args["text"])), nil
	})
	model := llm.NewScriptedModel(
		[]schema.Message{schema.AssistantMessage("Thought: I should call the tool.\nAction: echo\nAction Input: {\"text\":\"hello\"}")},
		[]schema.Message{schema.AssistantMessage("Thought: The tool returned the answer.\nFinal Answer: done")},
	)

	react, err := agent.NewReactAgent(agent.ReactAgentConfig{
		BaseConfig: agent.BaseConfig{
			Name:  "react",
			Model: model,
			Tools: []tool.Tool{echo},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	last, err := react.Invoke(context.Background(), []schema.Message{schema.UserMessage("say hello")})
	if err != nil {
		t.Fatal(err)
	}
	if len(last) != 3 {
		t.Fatalf("len(last) = %d, want 3: %#v", len(last), last)
	}
	if last[1].Role != schema.RoleAssistant || !strings.Contains(last[1].Text(), "Observation: hello") {
		t.Fatalf("observation message = %#v", last[1])
	}
	if !strings.Contains(last[2].Text(), "Final Answer: done") {
		t.Fatalf("final message = %#v", last[2])
	}
}

func TestReactAgentAcceptsStructuredFunctionCall(t *testing.T) {
	echo := tool.NewFunctionTool(tool.Spec{Name: "echo"}, func(ctx context.Context, input tool.CallInput) (tool.Result, error) {
		return tool.TextResult(fmt.Sprint(input.Args["text"])), nil
	})
	model := llm.NewScriptedModel(
		[]schema.Message{{
			Role:    schema.RoleAssistant,
			Content: schema.TextContent(""),
			FunctionCall: &schema.FunctionCall{
				Name:      "echo",
				Arguments: `{"text":"structured"}`,
			},
			Extra: map[string]any{"function_id": "call-1"},
		}},
		[]schema.Message{schema.AssistantMessage("done")},
	)

	react, err := agent.NewReactAgent(agent.ReactAgentConfig{
		BaseConfig: agent.BaseConfig{
			Name:  "react",
			Model: model,
			Tools: []tool.Tool{echo},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	last, err := react.Invoke(context.Background(), []schema.Message{schema.UserMessage("call echo")})
	if err != nil {
		t.Fatal(err)
	}
	if len(last) != 3 {
		t.Fatalf("len(last) = %d, want 3: %#v", len(last), last)
	}
	if last[1].Role != schema.RoleFunction || last[1].Name != "echo" {
		t.Fatalf("function observation = %#v", last[1])
	}
	if last[1].FunctionID("") != "call-1" {
		t.Fatalf("function id = %q, want call-1", last[1].FunctionID(""))
	}
}

func TestParseReactAction(t *testing.T) {
	action, ok := agent.ParseReactAction("Thought: search\nAction: search\nAction Input: ```json\n{\"q\":\"go\"}\n```")
	if !ok {
		t.Fatal("ParseReactAction returned false")
	}
	if action.Thought != "search" || action.Name != "search" || action.Input != `{"q":"go"}` {
		t.Fatalf("action = %#v", action)
	}
}
