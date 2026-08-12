package contextengine_test

import (
	"context"
	"strings"
	"testing"
	"unicode/utf8"

	"goharness/pkg/contextengine"
	"goharness/pkg/schema"
)

type runeCounter struct{}

func (runeCounter) Count(text string) int { return utf8.RuneCountInString(text) }

func TestAssemblerRepairsRolesAndAlignsStablePromptCachePrefix(t *testing.T) {
	assembler, err := contextengine.New(contextengine.Config{
		MaxContextTokens:     1000,
		ReservedOutputTokens: 100,
		MergeAdjacentRoles:   true,
		PromptCache: contextengine.PromptCacheConfig{
			Enabled:         true,
			MinPrefixTokens: 0,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	call := schema.Message{Role: schema.RoleAssistant, FunctionCall: &schema.FunctionCall{Name: "search", Arguments: "{}"}}
	result, err := assembler.Invoke(context.Background(), contextengine.Request{
		Pinned: []schema.Message{schema.SystemMessage("stable instructions")},
		History: []schema.Message{
			schema.AssistantMessage("orphan assistant"),
			schema.SystemMessage("runtime policy"),
			schema.UserMessage("question"),
			call,
			schema.FunctionMessage("wrong", schema.TextContent("drop me")),
			schema.FunctionMessage("search", schema.TextContent("result")),
		},
		Input: []schema.Message{schema.UserMessage("follow up"), schema.UserMessage("more detail")},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Messages) != 6 {
		t.Fatalf("messages = %#v", result.Messages)
	}
	if result.Messages[0].Role != schema.RoleSystem || result.Messages[1].Role != schema.RoleSystem {
		t.Fatalf("system prefix = %#v", result.Messages[:2])
	}
	marker, ok := result.Messages[1].Extra[contextengine.PromptCacheMetadataKey].(map[string]any)
	if !ok || marker["breakpoint"] != true {
		t.Fatalf("prompt cache marker = %#v", result.Messages[1].Extra)
	}
	if result.Messages[len(result.Messages)-1].Text() != "follow up\n\nmore detail" {
		t.Fatalf("merged user = %q", result.Messages[len(result.Messages)-1].Text())
	}
	if result.Stats.RoleRepairs != 3 || !result.Stats.PromptCacheAligned {
		t.Fatalf("stats = %#v", result.Stats)
	}
}

func TestAssemblerDropsOldTurnsAndTruncatesOversizedLatestInput(t *testing.T) {
	assembler, err := contextengine.New(contextengine.Config{
		MaxContextTokens:     70,
		ReservedOutputTokens: 10,
		Counter:              runeCounter{},
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := assembler.Invoke(context.Background(), contextengine.Request{
		Pinned: []schema.Message{schema.SystemMessage("policy")},
		History: []schema.Message{
			schema.UserMessage("old question one"), schema.AssistantMessage("old answer one"),
			schema.UserMessage("old question two"), schema.AssistantMessage("old answer two"),
		},
		Input: []schema.Message{schema.UserMessage(strings.Repeat("x", 100) + "LATEST")},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Stats.DroppedTurns == 0 || result.Stats.TruncatedMessages == 0 {
		t.Fatalf("stats = %#v", result.Stats)
	}
	if result.Stats.OutputTokens > result.Stats.BudgetTokens {
		t.Fatalf("output tokens %d exceed budget %d", result.Stats.OutputTokens, result.Stats.BudgetTokens)
	}
	last := result.Messages[len(result.Messages)-1]
	if !strings.HasSuffix(last.Text(), "LATEST") {
		t.Fatalf("latest user tail was not preserved: %q", last.Text())
	}
}

func TestAssemblerStrictPolicyRejectsOrphanFunctionResult(t *testing.T) {
	assembler, err := contextengine.New(contextengine.Config{
		MaxContextTokens:     100,
		ReservedOutputTokens: 10,
		RolePolicy:           contextengine.RolePolicyStrict,
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = assembler.Invoke(context.Background(), contextengine.Request{
		Input: []schema.Message{schema.UserMessage("run"), schema.FunctionMessage("search", schema.TextContent("orphan"))},
	})
	if err == nil || !strings.Contains(err.Error(), "no preceding assistant call") {
		t.Fatalf("Assemble error = %v", err)
	}
}

func TestAssemblerPreservesParallelFunctionCallsAndResults(t *testing.T) {
	assembler := contextengine.NewDefault()
	result, err := assembler.Invoke(context.Background(), contextengine.Request{Input: []schema.Message{
		schema.UserMessage("run both"),
		{Role: schema.RoleAssistant, FunctionCall: &schema.FunctionCall{Name: "first", Arguments: "{}"}},
		{Role: schema.RoleAssistant, FunctionCall: &schema.FunctionCall{Name: "second", Arguments: "{}"}},
		schema.FunctionMessage("first", schema.TextContent("one")),
		schema.FunctionMessage("second", schema.TextContent("two")),
	}})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Messages) != 5 || result.Stats.RoleRepairs != 0 {
		t.Fatalf("parallel tool context = %#v, stats=%#v", result.Messages, result.Stats)
	}
}

func TestAssemblerPreservesParallelCallsWithTheSameToolName(t *testing.T) {
	assembler := contextengine.NewDefault()
	result, err := assembler.Invoke(context.Background(), contextengine.Request{Input: []schema.Message{
		schema.UserMessage("run both searches"),
		{Role: schema.RoleAssistant, FunctionCall: &schema.FunctionCall{Name: "search", Arguments: `{"q":"one"}`}},
		{Role: schema.RoleAssistant, FunctionCall: &schema.FunctionCall{Name: "search", Arguments: `{"q":"two"}`}},
		schema.FunctionMessage("search", schema.TextContent("one")),
		schema.FunctionMessage("search", schema.TextContent("two")),
	}})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Messages) != 5 || result.Stats.RoleRepairs != 0 {
		t.Fatalf("same-tool parallel context = %#v, stats=%#v", result.Messages, result.Stats)
	}
}
