package react_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	"goharness/pkg/agent"
	"goharness/pkg/agent/react"
	"goharness/pkg/graph"
	"goharness/pkg/llm"
	"goharness/pkg/schema"
	"goharness/pkg/tool"
)

func TestNewAgentAutomaticallyUsesReactToolsLoop(t *testing.T) {
	echo := tool.NewFunctionTool(tool.Spec{Name: "echo"}, func(_ context.Context, input tool.CallInput) (tool.Result, error) {
		return tool.TextResult(fmt.Sprint(input.Args["text"])), nil
	})
	var calls atomic.Int32
	var toolsNodeStarts atomic.Int32
	model := llm.NewHandlerModel(func(_ context.Context, messages []schema.Message, options llm.ChatOptions) ([]schema.Message, error) {
		if len(options.Functions) != 1 || options.Functions[0].Name != "echo" {
			return nil, fmt.Errorf("functions = %#v", options.Functions)
		}
		if calls.Add(1) == 1 {
			message := schema.AssistantMessage("")
			message.FunctionCall = &schema.FunctionCall{Name: "echo", Arguments: `{"text":"react"}`}
			message.Extra = map[string]any{"function_id": "react-1"}
			return []schema.Message{message}, nil
		}
		if len(messages) < 3 || messages[len(messages)-1].Text() != "react" {
			return nil, fmt.Errorf("messages = %#v", messages)
		}
		return []schema.Message{schema.AssistantMessage("done")}, nil
	})
	value, err := react.NewAgent(context.Background(), react.Config{BaseConfig: agent.BaseConfig{
		Name:  "react",
		Model: model,
		Tools: []tool.BaseTool{echo},
	}})
	if err != nil {
		t.Fatal(err)
	}
	ctx := graph.WithEventSink(context.Background(), graph.EventSinkFunc(func(_ context.Context, event graph.Event) {
		if event.Type == graph.EventNodeStart && event.NodeID == "tools" {
			toolsNodeStarts.Add(1)
		}
	}))
	output, err := value.Invoke(ctx, []schema.Message{schema.UserMessage("use echo")})
	if err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 2 || toolsNodeStarts.Load() != 1 || len(output) != 3 || output[2].Text() != "done" {
		t.Fatalf("calls=%d tools-node-starts=%d output=%#v", calls.Load(), toolsNodeStarts.Load(), output)
	}
}

func TestNewAgentRejectsNilConstructionContext(t *testing.T) {
	if _, err := react.NewAgent(nil, react.Config{}); err == nil {
		t.Fatal("nil construction context was accepted")
	}
}
