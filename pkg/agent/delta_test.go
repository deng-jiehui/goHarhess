package agent_test

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"goharness/pkg/agent"
	"goharness/pkg/llm"
	"goharness/pkg/runnable"
	"goharness/pkg/schema"
	"goharness/pkg/tool"
)

func TestReactAgentAssemblesDeltaFunctionCall(t *testing.T) {
	model := newDeltaScriptedModel(
		[]llm.Event{
			{Messages: []schema.Message{{
				Role: schema.RoleAssistant,
				FunctionCall: &schema.FunctionCall{
					Name:      "ec",
					Arguments: `{"text":"hel`,
				},
			}}},
			{Messages: []schema.Message{{
				Role: schema.RoleAssistant,
				FunctionCall: &schema.FunctionCall{
					Name:      "ho",
					Arguments: `lo"}`,
				},
			}}},
		},
		[]llm.Event{
			{Messages: []schema.Message{schema.AssistantMessage("d")}},
			{Messages: []schema.Message{schema.AssistantMessage("one")}},
		},
	)
	echo := tool.NewFunctionTool(tool.Spec{Name: "echo"}, func(ctx context.Context, input tool.CallInput) (tool.Result, error) {
		return tool.TextResult(fmt.Sprint(input.Args["text"])), nil
	})
	value, err := agent.New(agent.BaseConfig{Name: "fn", Model: model, Tools: []tool.Tool{echo}})
	if err != nil {
		t.Fatal(err)
	}

	last, err := value.Invoke(context.Background(), []schema.Message{schema.UserMessage("call echo")}, agent.WithDeltaStream(true))
	if err != nil {
		t.Fatal(err)
	}
	if len(last) != 3 {
		t.Fatalf("messages = %#v", last)
	}
	if last[0].FunctionCall == nil || last[0].FunctionCall.Name != "echo" || last[0].FunctionCall.Arguments != `{"text":"hello"}` {
		t.Fatalf("function call = %#v", last[0].FunctionCall)
	}
	if last[1].Role != schema.RoleFunction || last[1].Text() != "hello" {
		t.Fatalf("observation = %#v", last[1])
	}
	if last[2].Text() != "done" {
		t.Fatalf("final response = %#v", last[2])
	}
	model.assertDeltaStream(t)
}

func TestReactAgentAssemblesDeltaAction(t *testing.T) {
	model := newDeltaScriptedModel(
		[]llm.Event{
			{Messages: []schema.Message{schema.AssistantMessage("Thought: call echo\nAct")}},
			{Messages: []schema.Message{schema.AssistantMessage("ion: echo\nAction Input: {\"text\":\"hi\"}")}},
		},
		[]llm.Event{
			{Messages: []schema.Message{schema.AssistantMessage("Final ")}},
			{Messages: []schema.Message{schema.AssistantMessage("Answer: done")}},
		},
	)
	echo := tool.NewFunctionTool(tool.Spec{Name: "echo"}, func(ctx context.Context, input tool.CallInput) (tool.Result, error) {
		return tool.TextResult(fmt.Sprint(input.Args["text"])), nil
	})
	value, err := agent.NewReactAgent(agent.ReactAgentConfig{BaseConfig: agent.BaseConfig{
		Name: "react", Model: model, Tools: []tool.Tool{echo},
	}})
	if err != nil {
		t.Fatal(err)
	}

	last, err := value.Invoke(context.Background(), []schema.Message{schema.UserMessage("call echo")}, agent.WithDeltaStream(true))
	if err != nil {
		t.Fatal(err)
	}
	if len(last) != 3 {
		t.Fatalf("messages = %#v", last)
	}
	if last[1].Text() != "Observation: hi\nThought: " {
		t.Fatalf("observation = %#v", last[1])
	}
	if last[2].Text() != "Final Answer: done" {
		t.Fatalf("final response = %#v", last[2])
	}
	model.assertDeltaStream(t)
}

type deltaScriptedModel struct {
	mu        sync.Mutex
	responses [][]llm.Event
	options   []llm.ChatOptions
}

func newDeltaScriptedModel(responses ...[]llm.Event) *deltaScriptedModel {
	return &deltaScriptedModel{responses: responses}
}

func (m *deltaScriptedModel) Invoke(ctx context.Context, request llm.Request, options ...runnable.Option) (llm.Response, error) {
	stream, err := m.Stream(ctx, request, options...)
	if err != nil {
		return llm.Response{}, err
	}
	return llm.ReduceEvents(ctx, stream, request.Options.DeltaStream)
}

func (m *deltaScriptedModel) Stream(ctx context.Context, request llm.Request, _ ...runnable.Option) (runnable.Stream[llm.Event], error) {
	m.mu.Lock()
	if len(m.responses) == 0 {
		m.mu.Unlock()
		return nil, fmt.Errorf("no scripted response")
	}
	events := m.responses[0]
	m.responses = m.responses[1:]
	m.options = append(m.options, request.Options)
	m.mu.Unlock()

	out := make(chan runnable.Item[llm.Event])
	go func() {
		defer close(out)
		for _, event := range events {
			event.Kind = llm.EventDelta
			select {
			case <-ctx.Done():
				out <- runnable.Item[llm.Event]{Err: ctx.Err()}
				return
			case out <- runnable.Item[llm.Event]{Value: event}:
			}
		}
		out <- runnable.Item[llm.Event]{Value: llm.Event{Kind: llm.EventDone}}
	}()
	return out, nil
}

func (m *deltaScriptedModel) Capabilities() llm.Capabilities {
	return llm.Capabilities{Streaming: true, ToolCalls: true, Reasoning: true}
}

func (m *deltaScriptedModel) ModelInfo() llm.ModelInfo {
	return llm.ModelInfo{Name: "delta-scripted", Provider: "test", ModelType: "scripted"}
}

func (m *deltaScriptedModel) assertDeltaStream(t *testing.T) {
	t.Helper()
	m.mu.Lock()
	defer m.mu.Unlock()
	for i, opts := range m.options {
		if !opts.DeltaStream {
			t.Fatalf("call %d did not enable DeltaStream", i)
		}
	}
}
