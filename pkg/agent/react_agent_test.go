package agent_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/goharness/goharness/pkg/agent"
	"github.com/goharness/goharness/pkg/llm"
	"github.com/goharness/goharness/pkg/schema"
	"github.com/goharness/goharness/pkg/tool"
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
		TextReAct: true,
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
	if last[1].Role != schema.RoleAssistant || !strings.Contains(last[1].JoinedText(""), "Observation: hello") {
		t.Fatalf("observation message = %#v", last[1])
	}
	if !strings.Contains(last[2].JoinedText(""), "Final Answer: done") {
		t.Fatalf("final message = %#v", last[2])
	}
}

func TestReactAgentAcceptsStructuredFunctionCall(t *testing.T) {
	echo := tool.NewFunctionTool(tool.Spec{Name: "echo"}, func(ctx context.Context, input tool.CallInput) (tool.Result, error) {
		return tool.TextResult(fmt.Sprint(input.Args["text"])), nil
	})
	model := llm.NewScriptedModel(
		[]schema.Message{schema.NewMessage(schema.RoleAssistant, schema.NewFunctionToolCallBlock("call-1", "echo", `{"text":"structured"}`))},
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
	results := last[1].FunctionToolResults()
	if last[1].Role != schema.RoleTool || len(results) != 1 || results[0].Name != "echo" {
		t.Fatalf("function observation = %#v", last[1])
	}
	if results[0].CallID != "call-1" {
		t.Fatalf("function id = %q, want call-1", results[0].CallID)
	}
}

// 默认原生模式不解析正文 Action：文本输出视为最终答案，工具不执行。
func TestReactAgentNativeModeIgnoresTextAction(t *testing.T) {
	called := false
	echo := tool.NewFunctionTool(tool.Spec{Name: "echo"}, func(ctx context.Context, input tool.CallInput) (tool.Result, error) {
		called = true
		return tool.TextResult("echo"), nil
	})
	var functions int
	model := llm.NewHandlerModel(func(_ context.Context, request llm.Request, opts llm.Options) ([]schema.Message, error) {
		functions = len(request.Functions)
		return []schema.Message{schema.AssistantMessage("Thought: try text\nAction: echo\nAction Input: {\"text\":\"x\"}")}, nil
	})
	react, err := agent.NewReactAgent(agent.ReactAgentConfig{
		BaseConfig: agent.BaseConfig{Name: "react", Model: model, Tools: []tool.Tool{echo}},
	})
	if err != nil {
		t.Fatal(err)
	}
	last, err := react.Invoke(context.Background(), []schema.Message{schema.UserMessage("call echo")})
	if err != nil {
		t.Fatal(err)
	}
	if called {
		t.Fatal("tool must not run for a text action in native mode")
	}
	if functions != 1 {
		t.Fatalf("functions=%d, want 1", functions)
	}
	if len(last) != 1 || !strings.Contains(last[0].JoinedText(""), "Action: echo") {
		t.Fatalf("final message = %#v", last)
	}
}

// 文本兼容模式请求不携带 Functions，工具 schema 写进系统提示。
func TestReactAgentTextModeOmitsFunctions(t *testing.T) {
	echo := tool.NewFunctionTool(tool.Spec{Name: "echo", Description: "Returns the supplied text."}, func(ctx context.Context, input tool.CallInput) (tool.Result, error) {
		return tool.TextResult("ok"), nil
	})
	var functions int
	var system string
	model := llm.NewHandlerModel(func(_ context.Context, request llm.Request, opts llm.Options) ([]schema.Message, error) {
		functions = len(request.Functions)
		if len(request.Messages) > 0 && request.Messages[0].Role == schema.RoleSystem {
			system = request.Messages[0].JoinedText("")
		}
		return []schema.Message{schema.AssistantMessage("Final Answer: done")}, nil
	})
	react, err := agent.NewReactAgent(agent.ReactAgentConfig{
		BaseConfig: agent.BaseConfig{Name: "react", Model: model, Tools: []tool.Tool{echo}},
		TextReAct:  true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := react.Invoke(context.Background(), []schema.Message{schema.UserMessage("hi")}); err != nil {
		t.Fatal(err)
	}
	if functions != 0 {
		t.Fatalf("functions=%d, want 0 in text mode", functions)
	}
	if !strings.Contains(system, "Action:") || !strings.Contains(system, "echo") {
		t.Fatalf("system prompt missing text protocol or tool list: %q", system)
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
