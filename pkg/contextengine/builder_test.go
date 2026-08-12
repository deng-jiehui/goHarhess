package contextengine_test

import (
	"context"
	"testing"

	"goharness/pkg/contextengine"
	"goharness/pkg/schema"
)

func TestConversationAndContextBuilders(t *testing.T) {
	conversation, err := contextengine.NewConversationBuilder().
		System("policy").
		UserContent(schema.MultiContent(schema.TextItem("inspect"), schema.ContentItem{File: "guide.pdf"})).
		AssistantCall("search", `{"q":"go"}`, "call-1").
		Reasoning("need retrieval").
		Function("search", schema.TextContent("found"), "call-1").
		Assistant("answer").
		Metadata("quality", "high").
		Build()
	if err != nil {
		t.Fatal(err)
	}
	if len(conversation) != 5 || conversation[2].FunctionCall == nil || conversation[3].FunctionID("") != "call-1" {
		t.Fatalf("conversation = %#v", conversation)
	}
	result, err := contextengine.NewContextBuilder(nil).
		Pinned(conversation[0]).
		History(conversation[1:4]...).
		Input(conversation[4]).
		TokenBudget(1000, 100).
		Build(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Messages) != 5 || result.Messages[4].Text() != "answer" {
		t.Fatalf("assembled messages = %#v", result.Messages)
	}
}

func TestConversationBuilderReportsInvalidReasoningPlacement(t *testing.T) {
	_, err := contextengine.NewConversationBuilder().User("hello").Reasoning("invalid").Build()
	if err == nil {
		t.Fatal("Build accepted reasoning on a user message")
	}
}
