package contextengine_test

import (
	"context"
	"testing"

	"github.com/goharness/goharness/pkg/contextengine"
	"github.com/goharness/goharness/pkg/schema"
)

func TestConversationAndContextBuilders(t *testing.T) {
	conversation, err := contextengine.NewConversationBuilder().
		System("policy").
		UserBlocks(schema.NewTextBlock("inspect"), schema.NewFileBlock("guide.pdf", "application/pdf")).
		AssistantCall("call-1", "search", `{"q":"go"}`).
		Reasoning("need retrieval", "sig").
		ToolResult("call-1", "search", false, schema.NewTextBlock("found")).
		Assistant("answer").
		Metadata("quality", "high").
		Build()
	if err != nil {
		t.Fatal(err)
	}
	if len(conversation) != 5 || len(conversation[2].ContentBlocks) != 2 || conversation[3].Role != schema.RoleTool {
		t.Fatalf("conversation = %#v", conversation)
	}
	engine, err := contextengine.NewContextBuilder(contextengine.Config{})
	if err != nil {
		t.Fatal(err)
	}
	result, err := contextengine.NewRequestBuilder(engine).
		Pinned(conversation[0]).
		History(conversation[1:4]...).
		Input(conversation[4]).
		TokenBudget(1000, 100).
		Build(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Messages) != 5 || result.Messages[4].JoinedText("") != "answer" {
		t.Fatalf("assembled messages = %#v", result.Messages)
	}
}

func TestConversationBuilderReportsInvalidReasoningPlacement(t *testing.T) {
	_, err := contextengine.NewConversationBuilder().User("hello").Reasoning("invalid", "sig").Build()
	if err == nil {
		t.Fatal("Build accepted reasoning on a user message")
	}
}

func TestConversationBuilderKeepsAssistantBlockOrder(t *testing.T) {
	messages, err := contextengine.NewConversationBuilder().
		User("weather").
		AssistantBlocks(schema.NewReasoningBlock("plan", "sig"), schema.NewTextBlock("checking"), schema.NewFunctionToolCallBlock("call-1", "weather", `{}`)).
		ToolResult("call-1", "weather", false, schema.NewTextBlock("sunny")).Build()
	if err != nil {
		t.Fatal(err)
	}
	blocks := messages[1].ContentBlocks
	if len(blocks) != 3 || blocks[0].Type != schema.ContentBlockTypeReasoning || blocks[1].Type != schema.ContentBlockTypeText || blocks[2].Type != schema.ContentBlockTypeFunctionToolCall {
		t.Fatalf("blocks=%#v", blocks)
	}
}

func TestConversationBuilderPreservesNilAndEmptyBlockSlices(t *testing.T) {
	messages, err := contextengine.NewConversationBuilder().
		Message(schema.Message{Role: schema.RoleUser, ContentBlocks: nil}).
		Message(schema.Message{Role: schema.RoleAssistant, ContentBlocks: []schema.ContentBlock{}}).
		Build()
	if err != nil {
		t.Fatal(err)
	}
	if messages[0].ContentBlocks != nil {
		t.Fatalf("nil blocks cloned as %#v", messages[0].ContentBlocks)
	}
	if messages[1].ContentBlocks == nil || len(messages[1].ContentBlocks) != 0 {
		t.Fatalf("empty blocks cloned as %#v", messages[1].ContentBlocks)
	}
}
