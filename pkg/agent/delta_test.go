package agent_test

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/goharness/goharness/pkg/agent"
	"github.com/goharness/goharness/pkg/llm"
	"github.com/goharness/goharness/pkg/runnable"
	"github.com/goharness/goharness/pkg/schema"
	"github.com/goharness/goharness/pkg/tool"
)

func TestReactAgentAssemblesDeltaFunctionCall(t *testing.T) {
	model := newDeltaScriptedModel(
		[]llm.Response{
			{Messages: []schema.Message{schema.NewMessage(schema.RoleAssistant, schema.ContentBlock{Type: schema.ContentBlockTypeFunctionToolCall, FunctionToolCall: &schema.FunctionToolCall{ID: "call-", Name: "ec", Arguments: `{"text":"hel`}})}},
			{Messages: []schema.Message{{
				Role:          schema.RoleAssistant,
				ContentBlocks: []schema.ContentBlock{{Type: schema.ContentBlockTypeFunctionToolCall, FunctionToolCall: &schema.FunctionToolCall{ID: "1", Name: "ho", Arguments: `lo"}`}}},
			}}},
		},
		[]llm.Response{
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
	calls := last[0].FunctionToolCalls()
	if len(calls) != 1 || calls[0].ID != "call-1" || calls[0].Name != "echo" || calls[0].Arguments != `{"text":"hello"}` {
		t.Fatalf("function calls = %#v", calls)
	}
	if last[1].Role != schema.RoleTool || len(last[1].FunctionToolResults()) != 1 {
		t.Fatalf("observation = %#v", last[1])
	}
	if last[2].JoinedText("") != "done" {
		t.Fatalf("final response = %#v", last[2])
	}
	model.assertDeltaStream(t)
}

func TestReactAgentAssemblesDeltaAction(t *testing.T) {
	model := newDeltaScriptedModel(
		[]llm.Response{
			{Messages: []schema.Message{schema.AssistantMessage("Thought: call echo\nAct")}},
			{Messages: []schema.Message{schema.AssistantMessage("ion: echo\nAction Input: {\"text\":\"hi\"}")}},
		},
		[]llm.Response{
			{Messages: []schema.Message{schema.AssistantMessage("Final ")}},
			{Messages: []schema.Message{schema.AssistantMessage("Answer: done")}},
		},
	)
	echo := tool.NewFunctionTool(tool.Spec{Name: "echo"}, func(ctx context.Context, input tool.CallInput) (tool.Result, error) {
		return tool.TextResult(fmt.Sprint(input.Args["text"])), nil
	})
	value, err := agent.NewReactAgent(agent.ReactAgentConfig{BaseConfig: agent.BaseConfig{
		Name: "react", Model: model, Tools: []tool.Tool{echo},
	}, TextReAct: true})
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
	if last[1].JoinedText("") != "Observation: hi\nThought: " {
		t.Fatalf("observation = %#v", last[1])
	}
	if last[2].JoinedText("") != "Final Answer: done" {
		t.Fatalf("final response = %#v", last[2])
	}
	model.assertDeltaStream(t)
}

type deltaScriptedModel struct {
	mu        sync.Mutex
	responses [][]llm.Response
	options   []llm.Options
}

func newDeltaScriptedModel(responses ...[]llm.Response) *deltaScriptedModel {
	return &deltaScriptedModel{responses: responses}
}

func (m *deltaScriptedModel) Invoke(ctx context.Context, request llm.Request, options ...runnable.Option) (llm.Response, error) {
	stream, err := m.Stream(ctx, request, options...)
	if err != nil {
		return llm.Response{}, err
	}
	return stream.Collect(ctx)
}

func (m *deltaScriptedModel) Stream(ctx context.Context, request llm.Request, options ...runnable.Option) (*runnable.StreamReader[llm.Response], error) {
	resolved, _, err := runnable.ResolveComponentOptions(llm.Options{}, options...)
	if err != nil {
		return nil, err
	}
	m.mu.Lock()
	if len(m.responses) == 0 {
		m.mu.Unlock()
		return nil, fmt.Errorf("no scripted response")
	}
	events := m.responses[0]
	m.responses = m.responses[1:]
	m.options = append(m.options, resolved)
	m.mu.Unlock()

	reader, writer := runnable.Pipe[llm.Response](ctx)
	go func() {
		accumulator := llm.NewDeltaAccumulator(resolved.DeltaStream)
		for _, event := range events {
			messages, err := accumulator.Apply(event)
			if err != nil {
				_ = writer.CloseWithError(err)
				return
			}
			if err := writer.Send(llm.Response{Messages: messages, Usage: event.Usage, FinishReason: event.FinishReason}); err != nil {
				return
			}
		}
		_ = writer.Close()
	}()
	return reader, nil
}

func (m *deltaScriptedModel) Capabilities() llm.Capabilities {
	return llm.Capabilities{Streaming: true, ToolCalls: true, Reasoning: true}
}

func (m *deltaScriptedModel) ModelInfo() llm.ModelInfo {
	return llm.ModelInfo{Name: "delta-scripted", Provider: "test", ModelType: "scripted"}
}
func (m *deltaScriptedModel) Collect(ctx context.Context, in *runnable.StreamReader[llm.Request], opts ...runnable.Option) (llm.Response, error) {
	return runnable.CollectFromInvoke(ctx, in, m.Invoke, opts...)
}
func (m *deltaScriptedModel) Transform(ctx context.Context, in *runnable.StreamReader[llm.Request], opts ...runnable.Option) (*runnable.StreamReader[llm.Response], error) {
	return runnable.TransformFromInvoke(ctx, in, m.Invoke, opts...)
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
