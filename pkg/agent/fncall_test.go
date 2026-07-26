package agent_test

import (
	"context"
	"fmt"
	"testing"

	"goharness/pkg/agent"
	"goharness/pkg/llm"
	"goharness/pkg/schema"
	"goharness/pkg/tool"
)

func TestFnCallAgentCallsToolAndContinues(t *testing.T) {
	ctx := context.Background()
	echo := tool.NewFunctionTool(tool.Spec{Name: "echo"}, func(ctx context.Context, input tool.CallInput) (tool.Result, error) {
		return tool.TextResult(fmt.Sprint(input.Args["text"])), nil
	})
	model := llm.NewScriptedModel(
		[]schema.Message{{
			Role:    schema.RoleAssistant,
			Content: schema.TextContent(""),
			FunctionCall: &schema.FunctionCall{
				Name:      "echo",
				Arguments: `{"text":"hello"}`,
			},
		}},
		[]schema.Message{schema.AssistantMessage("done")},
	)

	a, err := agent.NewFnCallAgent(agent.BaseConfig{
		Name:  "tester",
		Model: model,
		Tools: []tool.Tool{echo},
	})
	if err != nil {
		t.Fatal(err)
	}

	last, err := agent.RunSync(ctx, a, []schema.Message{schema.UserMessage("call echo")})
	if err != nil {
		t.Fatal(err)
	}
	if len(last) != 3 {
		t.Fatalf("len(last) = %d, want 3: %#v", len(last), last)
	}
	if last[1].Role != schema.RoleFunction || last[1].Name != "echo" {
		t.Fatalf("tool message = %#v", last[1])
	}
	if last[1].Text() != "hello" {
		t.Fatalf("tool content = %q, want hello", last[1].Text())
	}
	if last[2].Text() != "done" {
		t.Fatalf("final content = %q, want done", last[2].Text())
	}
}
