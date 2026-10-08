package llm_test

import (
	"testing"

	"github.com/goharness/goharness/pkg/llm"
	"github.com/goharness/goharness/pkg/schema"
)

func TestDeltaAccumulatorMergesFunctionBlocksByPosition(t *testing.T) {
	accumulator := llm.NewDeltaAccumulator(true)
	first := llm.Response{Messages: []schema.Message{schema.NewMessage(schema.RoleAssistant,
		schema.NewTextBlock("Hel"),
		schema.NewFunctionToolCallBlock("call-1", "wea", `{"city":`),
		schema.NewFunctionToolCallBlock("call-2", "a", `{"city":`),
	)}}
	second := llm.Response{Messages: []schema.Message{schema.NewMessage(schema.RoleAssistant,
		schema.NewTextBlock("lo"),
		schema.NewFunctionToolCallBlock("", "ther", `"Shanghai"}`),
		schema.NewFunctionToolCallBlock("", "ir", `"Shanghai"}`),
	)}}

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
	if messages[0].JoinedText("") != "Hello" {
		t.Fatalf("text=%q", messages[0].JoinedText(""))
	}
	calls := messages[0].FunctionToolCalls()
	if len(calls) != 2 || calls[0].Name != "weather" || calls[1].Name != "air" {
		t.Fatalf("calls=%#v", calls)
	}
}

func TestDeltaAccumulatorAcceptsCumulativeChunks(t *testing.T) {
	accumulator := llm.NewDeltaAccumulator(true)
	if _, err := accumulator.Apply(llm.Response{Messages: []schema.Message{schema.NewMessage(schema.RoleAssistant, schema.NewTextBlock("Hel"))}}); err != nil {
		t.Fatal(err)
	}
	messages, err := accumulator.Apply(llm.Response{Messages: []schema.Message{schema.NewMessage(schema.RoleAssistant, schema.NewTextBlock("Hello"))}})
	if err != nil {
		t.Fatal(err)
	}
	if messages[0].JoinedText("") != "Hello" {
		t.Fatalf("content = %q, want Hello", messages[0].JoinedText(""))
	}
}

func TestDeltaAccumulatorPassesThroughSnapshotsWhenDisabled(t *testing.T) {
	accumulator := llm.NewDeltaAccumulator(false)
	if _, err := accumulator.Apply(llm.Response{Messages: []schema.Message{schema.NewMessage(schema.RoleAssistant, schema.NewTextBlock("Hel"))}}); err != nil {
		t.Fatal(err)
	}
	messages, err := accumulator.Apply(llm.Response{Messages: []schema.Message{schema.NewMessage(schema.RoleAssistant, schema.NewTextBlock("lo"))}})
	if err != nil {
		t.Fatal(err)
	}
	if messages[0].JoinedText("") != "lo" {
		t.Fatalf("content = %q, want lo", messages[0].JoinedText(""))
	}
}

func TestDeltaAccumulatorRejectsBlockTypeChange(t *testing.T) {
	accumulator := llm.NewDeltaAccumulator(true)
	_, _ = accumulator.Apply(llm.Response{Messages: []schema.Message{schema.NewMessage(schema.RoleAssistant, schema.NewTextBlock("partial"))}})
	_, err := accumulator.Apply(llm.Response{Messages: []schema.Message{schema.NewMessage(schema.RoleAssistant, schema.NewReasoningBlock("changed", ""))}})
	if err == nil {
		t.Fatal("block type change was accepted")
	}
}
