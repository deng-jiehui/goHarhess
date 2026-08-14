package contextengine_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"unicode/utf8"

	"goharness/pkg/contextengine"
	"goharness/pkg/schema"
)

type runeCounter struct{}

func (runeCounter) Count(text string) int { return utf8.RuneCountInString(text) }

type nonAdditiveCounter struct{}

func (nonAdditiveCounter) Count(text string) int {
	if text == "lefttail" {
		return 20
	}
	return utf8.RuneCountInString(text)
}

func TestAssemblerRepairsRolesAndAlignsStablePromptCachePrefix(t *testing.T) {
	assembler, err := contextengine.NewContextBuilder(contextengine.Config{
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
	call := schema.NewMessage(schema.RoleAssistant, schema.NewFunctionToolCallBlock("call-1", "search", "{}"))
	result, err := assembler.Invoke(context.Background(), contextengine.Request{
		Pinned: []schema.Message{schema.SystemMessage("stable instructions")},
		History: []schema.Message{
			schema.AssistantMessage("orphan assistant"),
			schema.SystemMessage("runtime policy"),
			schema.UserMessage("question"),
			call,
			schema.NewMessage(schema.RoleTool, schema.NewFunctionToolResultBlock("wrong-id", "wrong", false, schema.NewTextBlock("drop me"))),
			schema.NewMessage(schema.RoleTool, schema.NewFunctionToolResultBlock("call-1", "search", false, schema.NewTextBlock("result"))),
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
	if result.Messages[len(result.Messages)-1].JoinedText("") != "follow upmore detail" {
		t.Fatalf("merged user = %q", result.Messages[len(result.Messages)-1].JoinedText(""))
	}
	if result.Stats.RoleRepairs != 3 || !result.Stats.PromptCacheAligned {
		t.Fatalf("stats = %#v", result.Stats)
	}
}

func TestAssemblerDropsOldTurnsAndTruncatesOversizedLatestInput(t *testing.T) {
	assembler, err := contextengine.NewContextBuilder(contextengine.Config{
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
	if !strings.HasSuffix(last.JoinedText(""), "LATEST") {
		t.Fatalf("latest user tail was not preserved: %q", last.JoinedText(""))
	}
}

func TestAssemblerStrictPolicyRejectsOrphanFunctionResult(t *testing.T) {
	assembler, err := contextengine.NewContextBuilder(contextengine.Config{
		MaxContextTokens:     100,
		ReservedOutputTokens: 10,
		RolePolicy:           contextengine.RolePolicyStrict,
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = assembler.Invoke(context.Background(), contextengine.Request{
		Input: []schema.Message{schema.UserMessage("run"), schema.NewMessage(schema.RoleTool, schema.NewFunctionToolResultBlock("missing", "search", false, schema.NewTextBlock("orphan")))},
	})
	if err == nil || !strings.Contains(err.Error(), "no preceding assistant call") {
		t.Fatalf("Assemble error = %v", err)
	}
}

func TestAssemblerStrictPolicyRejectsMixedValidAndOrphanToolResults(t *testing.T) {
	assembler, err := contextengine.NewContextBuilder(contextengine.Config{
		MaxContextTokens:     100,
		ReservedOutputTokens: 10,
		RolePolicy:           contextengine.RolePolicyStrict,
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = assembler.Invoke(context.Background(), contextengine.Request{Input: []schema.Message{
		schema.UserMessage("run"),
		schema.NewMessage(schema.RoleAssistant, schema.NewFunctionToolCallBlock("call-1", "search", `{}`)),
		schema.NewMessage(schema.RoleTool,
			schema.NewFunctionToolResultBlock("call-1", "search", false, schema.NewTextBlock("valid")),
			schema.NewFunctionToolResultBlock("missing", "search", false, schema.NewTextBlock("orphan")),
		),
	}})
	if err == nil || !strings.Contains(err.Error(), "no preceding assistant call") {
		t.Fatalf("Invoke error = %v", err)
	}
}

func TestAssemblerRepairDropsMixedToolResultMessageAtomically(t *testing.T) {
	assembler, err := contextengine.NewContextBuilder(contextengine.Config{MaxContextTokens: 100, ReservedOutputTokens: 10})
	if err != nil {
		t.Fatal(err)
	}
	result, err := assembler.Invoke(context.Background(), contextengine.Request{Input: []schema.Message{
		schema.UserMessage("run"),
		schema.NewMessage(schema.RoleAssistant, schema.NewFunctionToolCallBlock("call-1", "search", `{}`)),
		schema.NewMessage(schema.RoleTool,
			schema.NewFunctionToolResultBlock("call-1", "search", false, schema.NewTextBlock("discarded valid")),
			schema.NewFunctionToolResultBlock("missing", "search", false, schema.NewTextBlock("orphan")),
		),
		schema.NewMessage(schema.RoleTool, schema.NewFunctionToolResultBlock("call-1", "search", false, schema.NewTextBlock("kept"))),
	}})
	if err != nil {
		t.Fatal(err)
	}
	if result.Stats.RoleRepairs != 1 || len(result.Messages) != 3 {
		t.Fatalf("messages=%#v stats=%#v", result.Messages, result.Stats)
	}
	blocks := result.Messages[2].ContentBlocks
	if len(blocks) != 1 || blocks[0].FunctionToolResult == nil || blocks[0].FunctionToolResult.ContentBlocks[0].Text.Text != "kept" {
		t.Fatalf("tool blocks=%#v", blocks)
	}
}

func TestAssemblerRepairDropsDuplicateToolResultsAtomically(t *testing.T) {
	assembler, err := contextengine.NewContextBuilder(contextengine.Config{MaxContextTokens: 100, ReservedOutputTokens: 10})
	if err != nil {
		t.Fatal(err)
	}
	result, err := assembler.Invoke(context.Background(), contextengine.Request{Input: []schema.Message{
		schema.UserMessage("run"),
		schema.NewMessage(schema.RoleAssistant, schema.NewFunctionToolCallBlock("call-1", "search", `{}`)),
		schema.NewMessage(schema.RoleTool,
			schema.NewFunctionToolResultBlock("call-1", "search", false, schema.NewTextBlock("first")),
			schema.NewFunctionToolResultBlock("call-1", "search", false, schema.NewTextBlock("duplicate")),
		),
		schema.NewMessage(schema.RoleTool, schema.NewFunctionToolResultBlock("call-1", "search", false, schema.NewTextBlock("kept"))),
	}})
	if err != nil {
		t.Fatal(err)
	}
	if result.Stats.RoleRepairs != 1 || len(result.Messages) != 3 {
		t.Fatalf("messages=%#v stats=%#v", result.Messages, result.Stats)
	}
	blocks := result.Messages[2].ContentBlocks
	if len(blocks) != 1 || blocks[0].FunctionToolResult == nil || blocks[0].FunctionToolResult.ContentBlocks[0].Text.Text != "kept" {
		t.Fatalf("tool blocks=%#v", blocks)
	}
}

func TestAssemblerStrictPolicyRejectsDuplicateToolResults(t *testing.T) {
	assembler, err := contextengine.NewContextBuilder(contextengine.Config{
		MaxContextTokens:     100,
		ReservedOutputTokens: 10,
		RolePolicy:           contextengine.RolePolicyStrict,
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = assembler.Invoke(context.Background(), contextengine.Request{Input: []schema.Message{
		schema.UserMessage("run"),
		schema.NewMessage(schema.RoleAssistant, schema.NewFunctionToolCallBlock("call-1", "search", `{}`)),
		schema.NewMessage(schema.RoleTool,
			schema.NewFunctionToolResultBlock("call-1", "search", false, schema.NewTextBlock("first")),
			schema.NewFunctionToolResultBlock("call-1", "search", false, schema.NewTextBlock("duplicate")),
		),
	}})
	if err == nil || !strings.Contains(err.Error(), "no preceding assistant call") {
		t.Fatalf("Invoke error = %v", err)
	}
}

func TestAssemblerPreservesParallelFunctionCallsAndResults(t *testing.T) {
	assembler, err := contextengine.NewContextBuilder(contextengine.Config{})
	if err != nil {
		t.Fatal(err)
	}
	result, err := assembler.Invoke(context.Background(), contextengine.Request{Input: []schema.Message{
		schema.UserMessage("run both"),
		schema.NewMessage(schema.RoleAssistant, schema.NewFunctionToolCallBlock("first-id", "first", "{}")),
		schema.NewMessage(schema.RoleAssistant, schema.NewFunctionToolCallBlock("second-id", "second", "{}")),
		schema.NewMessage(schema.RoleTool, schema.NewFunctionToolResultBlock("first-id", "first", false, schema.NewTextBlock("one"))),
		schema.NewMessage(schema.RoleTool, schema.NewFunctionToolResultBlock("second-id", "second", false, schema.NewTextBlock("two"))),
	}})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Messages) != 5 || result.Stats.RoleRepairs != 0 {
		t.Fatalf("parallel tool context = %#v, stats=%#v", result.Messages, result.Stats)
	}
}

func TestAssemblerPreservesParallelCallsWithTheSameToolName(t *testing.T) {
	assembler, err := contextengine.NewContextBuilder(contextengine.Config{})
	if err != nil {
		t.Fatal(err)
	}
	result, err := assembler.Invoke(context.Background(), contextengine.Request{Input: []schema.Message{
		schema.UserMessage("run both searches"),
		schema.NewMessage(schema.RoleAssistant, schema.NewFunctionToolCallBlock("search-1", "search", `{"q":"one"}`)),
		schema.NewMessage(schema.RoleAssistant, schema.NewFunctionToolCallBlock("search-2", "search", `{"q":"two"}`)),
		schema.NewMessage(schema.RoleTool, schema.NewFunctionToolResultBlock("search-1", "search", false, schema.NewTextBlock("one"))),
		schema.NewMessage(schema.RoleTool, schema.NewFunctionToolResultBlock("search-2", "search", false, schema.NewTextBlock("two"))),
	}})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Messages) != 5 || result.Stats.RoleRepairs != 0 {
		t.Fatalf("same-tool parallel context = %#v, stats=%#v", result.Messages, result.Stats)
	}
}

func TestContextBuilderTruncatesTextWithoutFlatteningBlocks(t *testing.T) {
	assembler, err := contextengine.NewContextBuilder(contextengine.Config{MaxContextTokens: 40, ReservedOutputTokens: 5})
	if err != nil {
		t.Fatal(err)
	}
	message := schema.NewMessage(schema.RoleUser, schema.NewTextBlock(strings.Repeat("prefix ", 50)), schema.NewImageBlock("file:///diagram.png", "image/png"), schema.NewTextBlock("LATEST"))
	result, err := assembler.Invoke(context.Background(), contextengine.Request{Input: []schema.Message{message}})
	if err != nil {
		t.Fatal(err)
	}
	blocks := result.Messages[0].ContentBlocks
	if len(blocks) != 3 || blocks[0].Type != schema.ContentBlockTypeText || blocks[1].Type != schema.ContentBlockTypeImage || blocks[2].Type != schema.ContentBlockTypeText || blocks[2].Text.Text != "LATEST" {
		t.Fatalf("blocks=%#v", blocks)
	}
}

func TestContextBuilderBudgetsTopLevelTextBlocksSeparately(t *testing.T) {
	assembler, err := contextengine.NewContextBuilder(contextengine.Config{
		MaxContextTokens:     16,
		ReservedOutputTokens: 1,
		Counter:              nonAdditiveCounter{},
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := assembler.Invoke(context.Background(), contextengine.Request{Input: []schema.Message{
		schema.NewMessage(schema.RoleUser, schema.NewTextBlock("left"), schema.NewTextBlock("tail")),
	}})
	if err != nil {
		t.Fatal(err)
	}
	blocks := result.Messages[0].ContentBlocks
	if len(blocks) != 2 || blocks[0].Text == nil || blocks[0].Text.Text != "eft" || blocks[1].Text == nil || blocks[1].Text.Text != "tail" {
		t.Fatalf("blocks=%#v", blocks)
	}
}

func TestContextBuilderCountsEveryBlockPayloadRecursively(t *testing.T) {
	assembler, err := contextengine.NewContextBuilder(contextengine.Config{MaxContextTokens: 100, ReservedOutputTokens: 10, Counter: runeCounter{}})
	if err != nil {
		t.Fatal(err)
	}
	message := schema.NewMessage(schema.RoleUser,
		schema.NewTextBlock("text"), schema.NewReasoningBlock("reason", "sig"), schema.NewImageBlock("u", "m"),
		schema.NewFunctionToolCallBlock("i", "n", `{}`),
		schema.NewFunctionToolResultBlock("c", "r", false, schema.NewTextBlock("z")),
		schema.NewOpaqueBlock("k", json.RawMessage(`{}`)),
	)
	result, err := assembler.Invoke(context.Background(), contextengine.Request{Input: []schema.Message{message}})
	if err != nil {
		t.Fatal(err)
	}
	if result.Stats.InputTokens != 33 {
		t.Fatalf("input tokens=%d", result.Stats.InputTokens)
	}
}
