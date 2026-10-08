package schema

import "testing"

func TestMessageAccumulatorPreservesRepeatedDeltas(t *testing.T) {
	a := NewMessageAccumulator()
	for _, d := range []MessageDelta{
		{MessageID: "m", Op: MessageStart, Role: RoleAssistant},
		{MessageID: "m", Op: BlockStart, BlockIndex: 0, Block: ptrBlock(NewTextBlock(""))},
		{MessageID: "m", Op: BlockAppend, BlockIndex: 0, Text: "哈"},
		{MessageID: "m", Op: BlockAppend, BlockIndex: 0, Text: "哈"},
		{MessageID: "m", Op: MessageEnd},
	} {
		if err := a.Apply(d); err != nil {
			t.Fatal(err)
		}
	}
	m, ok := a.Message("m")
	if !ok || m.JoinedText("") != "哈哈" {
		t.Fatalf("%+v", m)
	}
	if err := a.Apply(MessageDelta{MessageID: "m", Op: BlockAppend, BlockIndex: 0, Text: "!"}); err == nil {
		t.Fatal("accepted update after completion")
	}
}

func TestMessageAccumulatorValidatesToolAtEnd(t *testing.T) {
	a := NewMessageAccumulator()
	for _, d := range []MessageDelta{
		{MessageID: "m", Op: MessageStart, Role: RoleAssistant},
		{MessageID: "m", Op: BlockStart, BlockIndex: 0, Block: ptrBlock(NewFunctionToolCallBlock("call", "search", ""))},
		{MessageID: "m", Op: BlockAppend, BlockIndex: 0, Arguments: `{"id":`},
	} {
		if err := a.Apply(d); err != nil {
			t.Fatal(err)
		}
	}
	if err := a.Apply(MessageDelta{MessageID: "m", Op: MessageEnd}); err == nil {
		t.Fatal("accepted partial JSON")
	}
	if err := a.Apply(MessageDelta{MessageID: "m", Op: BlockAppend, BlockIndex: 0, Arguments: `1}`}); err != nil {
		t.Fatal(err)
	}
	if err := a.Apply(MessageDelta{MessageID: "m", Op: MessageEnd}); err != nil {
		t.Fatal(err)
	}
}
func ptrBlock(b ContentBlock) *ContentBlock { return &b }

func TestDeltaRejectsAmbiguousPayload(t *testing.T) {
	a := NewMessageAccumulator()
	if err := a.Apply(MessageDelta{MessageID: "m", Op: MessageStart, Role: RoleAssistant, Text: "ignored"}); err == nil {
		t.Fatal("accepted ignored payload")
	}
}
