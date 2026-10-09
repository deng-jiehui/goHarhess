package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"

	"github.com/goharness/goharness/pkg/agent"
	"github.com/goharness/goharness/pkg/graph"
	"github.com/goharness/goharness/pkg/llm"
	"github.com/goharness/goharness/pkg/schema"
	"github.com/goharness/goharness/pkg/state"
	"github.com/goharness/goharness/pkg/tool"
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
		[]schema.Message{schema.NewMessage(schema.RoleAssistant, schema.NewFunctionToolCallBlock("1", "echo", `{"text":"hello from tool"}`))},
		[]schema.Message{schema.AssistantMessage("tool call finished")},
	)
	approval := tool.BeforeBatchFunc(func(ctx context.Context, batch tool.Batch) error {
		answer, err := graph.Interrupt(ctx, map[string]any{"action": "approve_tools", "calls": batch.Calls})
		if err != nil {
			return err
		}
		if approved, ok := answer.(bool); !ok || !approved {
			return errors.New("工具执行未获批准")
		}
		return nil
	})

	a, err := agent.New(model,
		agent.WithName("assistant"),
		agent.WithSystemMessage("You are a helpful assistant."),
		agent.WithTool(echoTool),
		agent.WithBeforeToolBatch(approval))
	if err != nil {
		log.Fatal(err)
	}

	store := state.NewMemoryStore()
	checkpoint := agent.WithCheckpoint(store, "example-run")
	last, err := a.Invoke(ctx, []schema.Message{schema.UserMessage("please use echo")}, checkpoint)
	var interrupted *graph.InterruptedError
	if errors.As(err, &interrupted) {
		pending := interrupted.PendingInterrupts()
		// 示例模拟外部输入；实际应用由界面或外部系统提交。
		last, err = agent.Resume(ctx, a, checkpoint, agent.WithResume(pending[0].ID, true))
	}
	if err != nil {
		log.Fatal(err)
	}

	for _, msg := range last {
		data, _ := json.MarshalIndent(msg, "", "  ")
		fmt.Println(string(data))
	}
}
