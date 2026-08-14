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

func TestReactAgentCallsToolAndContinues(t *testing.T) {
	ctx := context.Background()
	echo := tool.NewFunctionTool(tool.Spec{Name: "echo"}, func(ctx context.Context, input tool.CallInput) (tool.Result, error) {
		return tool.TextResult(fmt.Sprint(input.Args["text"])), nil
	})
	model := llm.NewScriptedModel(
		[]schema.Message{schema.NewMessage(schema.RoleAssistant, schema.NewFunctionToolCallBlock("call-1", "echo", `{"text":"hello"}`))},
		[]schema.Message{schema.AssistantMessage("done")},
	)

	a, err := agent.New(agent.BaseConfig{
		Name:  "tester",
		Model: model,
		Tools: []tool.Tool{echo},
	})
	if err != nil {
		t.Fatal(err)
	}

	last, err := a.Invoke(ctx, []schema.Message{schema.UserMessage("call echo")})
	if err != nil {
		t.Fatal(err)
	}
	if len(last) != 3 {
		t.Fatalf("len(last) = %d, want 3: %#v", len(last), last)
	}
	results := last[1].FunctionToolResults()
	if last[1].Role != schema.RoleTool || len(results) != 1 || results[0].Name != "echo" || results[0].CallID != "call-1" {
		t.Fatalf("tool message = %#v", last[1])
	}
	if toolResultText(last[1]) != "hello" {
		t.Fatalf("tool content = %q, want hello", toolResultText(last[1]))
	}
	if last[2].JoinedText("") != "done" {
		t.Fatalf("final content = %q, want done", last[2].JoinedText(""))
	}
}

func TestReactAgentAcceptsBlankArgumentsForNoArgumentTool(t *testing.T) {
	var calls int
	ping := tool.NewFunctionTool(tool.Spec{Name: "ping"}, func(_ context.Context, input tool.CallInput) (tool.Result, error) {
		calls++
		if len(input.Args) != 0 {
			t.Fatalf("args=%#v", input.Args)
		}
		return tool.TextResult("pong"), nil
	})
	call := schema.NewMessage(schema.RoleAssistant, schema.NewFunctionToolCallBlock("ping-1", "ping", `{}`))
	model := llm.NewScriptedModel([]schema.Message{call}, []schema.Message{schema.AssistantMessage("done")})
	value, err := agent.New(agent.BaseConfig{Model: model, Tools: []tool.Tool{ping}})
	if err != nil {
		t.Fatal(err)
	}
	output, err := value.Invoke(context.Background(), []schema.Message{schema.UserMessage("ping")})
	if err != nil {
		t.Fatal(err)
	}
	if calls != 1 || len(output) != 3 || toolResultText(output[1]) != "pong" {
		t.Fatalf("calls=%d output=%#v", calls, output)
	}
}

func toolResultText(message schema.Message) string {
	results := message.FunctionToolResults()
	if len(results) != 1 {
		return ""
	}
	parts := make([]string, 0)
	for _, block := range results[0].ContentBlocks {
		if block.Type == schema.ContentBlockTypeText && block.Text != nil {
			parts = append(parts, block.Text.Text)
		}
	}
	return strings.Join(parts, "")
}
