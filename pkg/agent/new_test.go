package agent_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"goharness/pkg/agent"
	"goharness/pkg/graph"
	"goharness/pkg/llm"
	"goharness/pkg/schema"
	"goharness/pkg/tool"
)

func TestNewAgentAutoComposesToolsNodeFromBaseTool(t *testing.T) {
	echo := tool.NewFunctionTool(tool.Spec{
		Name: "echo",
		Parameters: map[string]any{
			"type":       "object",
			"properties": map[string]any{"text": map[string]any{"type": "string"}},
		},
	}, func(_ context.Context, input tool.CallInput) (tool.Result, error) {
		return tool.TextResult(fmt.Sprint(input.Args["text"])), nil
	})
	var modelCalls atomic.Int32
	model := llm.NewHandlerModel(func(_ context.Context, messages []schema.Message, options llm.ChatOptions) ([]schema.Message, error) {
		call := modelCalls.Add(1)
		if len(options.Functions) != 1 || options.Functions[0].Name != "echo" {
			return nil, fmt.Errorf("model definitions = %#v", options.Functions)
		}
		if call == 1 {
			message := schema.AssistantMessage("")
			message.FunctionCall = &schema.FunctionCall{Name: "echo", Arguments: `{"text":"hello"}`}
			message.Extra = map[string]any{"function_id": "echo-1"}
			return []schema.Message{message}, nil
		}
		if len(messages) < 3 {
			return nil, fmt.Errorf("second turn messages = %#v", messages)
		}
		observation := messages[len(messages)-1]
		if observation.Role != schema.RoleFunction || observation.FunctionID("") != "echo-1" || observation.Text() != "hello" {
			return nil, fmt.Errorf("tool observation = %#v", observation)
		}
		return []schema.Message{schema.AssistantMessage("done")}, nil
	})
	value, err := agent.New(agent.BaseConfig{Name: "auto", Model: model}, agent.WithTool(echo))
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := value.(*agent.ReactAgent); !ok {
		t.Fatalf("agent.New returned %T, want *agent.ReactAgent", value)
	}
	var mu sync.Mutex
	started := map[string]int{}
	ctx := graph.WithEventSink(context.Background(), graph.EventSinkFunc(func(_ context.Context, event graph.Event) {
		if event.Type == graph.EventNodeStart {
			mu.Lock()
			started[event.NodeID]++
			mu.Unlock()
		}
	}))
	output, err := value.Invoke(ctx, []schema.Message{schema.UserMessage("echo hello")})
	if err != nil {
		t.Fatal(err)
	}
	if modelCalls.Load() != 2 || len(output) != 3 || output[len(output)-1].Text() != "done" {
		t.Fatalf("calls=%d output=%#v", modelCalls.Load(), output)
	}
	mu.Lock()
	defer mu.Unlock()
	if started["llm"] != 2 || started["tools"] != 1 {
		t.Fatalf("automatic graph nodes = %#v", started)
	}
}

func TestNewAgentWithoutToolsUsesBasicRecipe(t *testing.T) {
	value, err := agent.New(agent.BaseConfig{Model: llm.NewScriptedModel([]schema.Message{schema.AssistantMessage("ok")})})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := value.(*agent.BasicAgent); !ok {
		t.Fatalf("agent.New returned %T, want *agent.BasicAgent", value)
	}
}

func TestBasicAgentRejectsIgnoredTools(t *testing.T) {
	value := tool.NewFunctionTool(tool.Spec{Name: "unused"}, func(context.Context, tool.CallInput) (tool.Result, error) {
		return tool.TextResult("unused"), nil
	})
	if _, err := agent.NewBasicAgent(agent.BaseConfig{Model: llm.NewScriptedModel(), Tools: []tool.BaseTool{value}}); err == nil {
		t.Fatal("NewBasicAgent accepted tools it cannot execute")
	}
}
