package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"goharness/pkg/agent"
	"goharness/pkg/llm"
	"goharness/pkg/schema"
	"goharness/pkg/tool"
)

func main() {
	ctx := context.Background()

	echoTool := tool.NewFunctionTool(tool.Spec{
		Name:        "echo",
		Description: "Return the text field from arguments.",
		Parameters: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"text": map[string]any{"type": "string"},
			},
			"required": []string{"text"},
		},
	}, func(ctx context.Context, input tool.CallInput) (tool.Result, error) {
		return tool.TextResult(fmt.Sprint(input.Args["text"])), nil
	})

	model := llm.NewScriptedModel(
		[]schema.Message{{
			Role:    schema.RoleAssistant,
			Content: schema.TextContent(""),
			FunctionCall: &schema.FunctionCall{
				Name:      "echo",
				Arguments: `{"text":"hello from tool"}`,
			},
			Extra: map[string]any{"function_id": "1"},
		}},
		[]schema.Message{schema.AssistantMessage("tool call finished")},
	)

	a, err := agent.New(agent.BaseConfig{
		Name:          "assistant",
		SystemMessage: "You are a helpful assistant.",
		Model:         model,
		Tools:         []tool.Tool{echoTool},
	})
	if err != nil {
		log.Fatal(err)
	}

	last, err := a.Invoke(ctx, []schema.Message{schema.UserMessage("please use echo")})
	if err != nil {
		log.Fatal(err)
	}

	for _, msg := range last {
		data, _ := json.MarshalIndent(msg, "", "  ")
		fmt.Println(string(data))
	}
}
