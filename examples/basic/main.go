package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"

	"goharness/pkg/agent"
	"goharness/pkg/control"
	"goharness/pkg/graph"
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
	approved := false
	approval := tool.BeforeBatchFunc(func(_ context.Context, batch tool.Batch) error {
		if !approved {
			return control.RequestInterrupt("approve tool batch", map[string]any{
				"tool_call_id": batch.Calls[0].ID,
			})
		}
		return nil
	})

	a, err := agent.New(agent.BaseConfig{
		Name:          "assistant",
		SystemMessage: "You are a helpful assistant.",
		Model:         model,
		Tools:         []tool.Tool{echoTool},
	}, agent.WithBeforeToolBatch(approval))
	if err != nil {
		log.Fatal(err)
	}

	store := &memoryExecutionStore{}
	checkpoint := agent.WithCheckpoint(store, "example-run")
	last, err := a.Invoke(ctx, []schema.Message{schema.UserMessage("please use echo")}, checkpoint)
	if errors.Is(err, graph.ErrInterrupted) {
		approved = true
		last, err = agent.Resume(ctx, a, checkpoint)
	}
	if err != nil {
		log.Fatal(err)
	}

	for _, msg := range last {
		data, _ := json.MarshalIndent(msg, "", "  ")
		fmt.Println(string(data))
	}
}

type memoryExecutionStore struct {
	mu   sync.Mutex
	data map[string][]byte
}

func (s *memoryExecutionStore) Save(_ context.Context, key string, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.data == nil {
		s.data = map[string][]byte{}
	}
	s.data[key] = append([]byte(nil), data...)
	return nil
}

func (s *memoryExecutionStore) Load(_ context.Context, key string) ([]byte, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, ok := s.data[key]
	return append([]byte(nil), data...), ok, nil
}
