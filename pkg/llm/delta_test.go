package llm_test

import (
	"testing"

	"goharness/pkg/llm"
	"goharness/pkg/schema"
)

func TestDeltaAccumulatorMergesMessageFields(t *testing.T) {
	accumulator := llm.NewDeltaAccumulator(true)
	first := llm.Event{Messages: []schema.Message{{
		Role:             schema.RoleAssistant,
		Content:          schema.TextContent("Hel"),
		ReasoningContent: schema.TextContent("think "),
		FunctionCall: &schema.FunctionCall{
			Name:      "ec",
			Arguments: `{"text":"hel`,
		},
	}}}
	second := llm.Event{Messages: []schema.Message{{
		Role:             schema.RoleAssistant,
		Content:          schema.TextContent("lo"),
		ReasoningContent: schema.TextContent("again"),
		FunctionCall: &schema.FunctionCall{
			Name:      "ho",
			Arguments: `lo"}`,
		},
		Extra: map[string]any{"function_id": "call-1"},
	}}}

	if _, err := accumulator.Apply(first); err != nil {
		t.Fatal(err)
	}
	messages, err := accumulator.Apply(second)
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) != 1 {
		t.Fatalf("messages = %#v", messages)
	}
	message := messages[0]
	if message.Text() != "Hello" {
		t.Fatalf("content = %q, want Hello", message.Text())
	}
	if message.ReasoningContent.String() != "think again" {
		t.Fatalf("reasoning = %q, want think again", message.ReasoningContent.String())
	}
	if message.FunctionCall == nil || message.FunctionCall.Name != "echo" || message.FunctionCall.Arguments != `{"text":"hello"}` {
		t.Fatalf("function call = %#v", message.FunctionCall)
	}
	if message.FunctionID("") != "call-1" {
		t.Fatalf("function ID = %q, want call-1", message.FunctionID(""))
	}
}

func TestDeltaAccumulatorAcceptsCumulativeChunks(t *testing.T) {
	accumulator := llm.NewDeltaAccumulator(true)
	if _, err := accumulator.Apply(llm.Event{Messages: []schema.Message{schema.AssistantMessage("Hel")}}); err != nil {
		t.Fatal(err)
	}
	messages, err := accumulator.Apply(llm.Event{Messages: []schema.Message{schema.AssistantMessage("Hello")}})
	if err != nil {
		t.Fatal(err)
	}
	if messages[0].Text() != "Hello" {
		t.Fatalf("content = %q, want Hello", messages[0].Text())
	}
}

func TestDeltaAccumulatorPassesThroughSnapshotsWhenDisabled(t *testing.T) {
	accumulator := llm.NewDeltaAccumulator(false)
	if _, err := accumulator.Apply(llm.Event{Messages: []schema.Message{schema.AssistantMessage("Hel")}}); err != nil {
		t.Fatal(err)
	}
	messages, err := accumulator.Apply(llm.Event{Messages: []schema.Message{schema.AssistantMessage("lo")}})
	if err != nil {
		t.Fatal(err)
	}
	if messages[0].Text() != "lo" {
		t.Fatalf("content = %q, want lo", messages[0].Text())
	}
}
