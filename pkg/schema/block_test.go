package schema_test

import (
	"encoding/json"
	"strings"
	"testing"

	"goharness/pkg/schema"
)

func TestMessageContentBlocksRoundTripInOrder(t *testing.T) {
	raw := json.RawMessage(`{"provider_type":"server_tool_use","value":7}`)
	message := schema.NewMessage(schema.RoleAssistant,
		schema.NewReasoningBlock("plan", "sig-1"),
		schema.NewTextBlock("checking"),
		schema.NewFunctionToolCallBlock("call-1", "weather", `{"city":"Shanghai"}`),
		schema.NewOpaqueBlock("provider.server_tool_use", raw),
	)
	message.ResponseMeta = &schema.ResponseMeta{
		ID: "resp-1", Model: "model-1", FinishReason: "tool_calls",
		Usage: &schema.TokenUsage{InputTokens: 10, OutputTokens: 4, CacheReadTokens: 8},
	}

	data, err := json.Marshal(message)
	if err != nil {
		t.Fatal(err)
	}
	var got schema.Message
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatal(err)
	}
	if err := got.Validate(); err != nil {
		t.Fatal(err)
	}
	if len(got.ContentBlocks) != 4 {
		t.Fatalf("blocks=%d", len(got.ContentBlocks))
	}
	if got.ContentBlocks[0].Type != schema.ContentBlockTypeReasoning ||
		got.ContentBlocks[1].Type != schema.ContentBlockTypeText ||
		got.ContentBlocks[2].Type != schema.ContentBlockTypeFunctionToolCall ||
		got.ContentBlocks[3].Type != schema.ContentBlockTypeOpaque {
		t.Fatalf("order=%#v", got.ContentBlocks)
	}
	if string(got.ContentBlocks[3].Opaque.Raw) != string(raw) {
		t.Fatalf("raw=%s", got.ContentBlocks[3].Opaque.Raw)
	}
	if got.ResponseMeta.Usage.CacheReadTokens != 8 {
		t.Fatalf("meta=%#v", got.ResponseMeta)
	}
}

func TestContentBlockValidateRejectsMalformedBlocks(t *testing.T) {
	tests := []struct {
		name  string
		block schema.ContentBlock
		want  string
	}{
		{
			name:  "mismatched payload",
			block: schema.ContentBlock{Type: schema.ContentBlockTypeText, Reasoning: &schema.Reasoning{Text: "plan"}},
			want:  "requires text payload",
		},
		{
			name: "two payloads",
			block: schema.ContentBlock{Type: schema.ContentBlockTypeText,
				Text: &schema.Text{Text: "answer"}, Reasoning: &schema.Reasoning{Text: "plan"}},
			want: "exactly one payload",
		},
		{
			name:  "invalid opaque json",
			block: schema.ContentBlock{Type: schema.ContentBlockTypeOpaque, Opaque: &schema.OpaqueContent{Kind: "provider.data", Raw: json.RawMessage(`{`)}},
			want:  "valid JSON",
		},
		{
			name:  "missing call id",
			block: schema.NewFunctionToolCallBlock("", "weather", `{}`),
			want:  "requires an id",
		},
		{
			name:  "missing call name",
			block: schema.NewFunctionToolCallBlock("call-1", "", `{}`),
			want:  "requires a name",
		},
		{
			name:  "arguments are not an object",
			block: schema.NewFunctionToolCallBlock("call-1", "weather", `[]`),
			want:  "JSON object",
		},
		{
			name:  "arguments are null",
			block: schema.NewFunctionToolCallBlock("call-1", "weather", `null`),
			want:  "JSON object",
		},
		{
			name:  "missing result call id",
			block: schema.NewFunctionToolResultBlock("", "weather", false),
			want:  "requires a call id",
		},
		{
			name:  "missing result name",
			block: schema.NewFunctionToolResultBlock("call-1", "", false),
			want:  "requires a name",
		},
		{
			name:  "media has no source",
			block: schema.ContentBlock{Type: schema.ContentBlockTypeFile, File: &schema.MediaContent{}},
			want:  "exactly one source",
		},
		{
			name: "media has two sources",
			block: schema.ContentBlock{Type: schema.ContentBlockTypeImage,
				Image: &schema.MediaContent{URI: "image.png", Data: json.RawMessage(`"bytes"`)}},
			want: "exactly one source",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.block.Validate()
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Validate() error = %v, want containing %q", err, tt.want)
			}
		})
	}
}

func TestMessageValidateReportsNestedResultPath(t *testing.T) {
	message := schema.NewMessage(schema.RoleTool,
		schema.NewFunctionToolResultBlock("call-1", "weather", false,
			schema.NewFunctionToolResultBlock("call-2", "lookup", false,
				schema.ContentBlock{Type: schema.ContentBlockTypeText},
			),
		),
	)
	err := message.Validate()
	if err == nil || !strings.Contains(err.Error(), "content_blocks[0]: content_blocks[0]: content_blocks[0]") {
		t.Fatalf("Validate() error = %v, want complete nested block path", err)
	}
}

func TestMessageValidateRejectsNegativeUsageCounts(t *testing.T) {
	fields := []struct {
		name  string
		usage schema.TokenUsage
	}{
		{"input", schema.TokenUsage{InputTokens: -1}},
		{"output", schema.TokenUsage{OutputTokens: -1}},
		{"total", schema.TokenUsage{TotalTokens: -1}},
		{"reasoning", schema.TokenUsage{ReasoningTokens: -1}},
		{"cache read", schema.TokenUsage{CacheReadTokens: -1}},
		{"cache write", schema.TokenUsage{CacheWriteTokens: -1}},
	}
	for _, field := range fields {
		t.Run(field.name, func(t *testing.T) {
			message := schema.UserMessage("hello")
			message.ResponseMeta = &schema.ResponseMeta{Usage: &field.usage}
			if err := message.Validate(); err == nil || !strings.Contains(err.Error(), "must not be negative") {
				t.Fatalf("Validate() error = %v", err)
			}
		})
	}
}

func TestCloneMessagesIsolatesNestedBlocksAndMetadata(t *testing.T) {
	source := schema.NewMessage(schema.RoleTool,
		schema.NewFunctionToolResultBlock("call-1", "weather", false,
			schema.NewTextBlock("sunny"),
		),
	)
	source.ResponseMeta = &schema.ResponseMeta{Extra: map[string]any{"trace": map[string]any{"id": "a"}}}
	cloned, err := schema.CloneMessagesStrict([]schema.Message{source})
	if err != nil {
		t.Fatal(err)
	}
	cloned[0].ContentBlocks[0].FunctionToolResult.ContentBlocks[0].Text.Text = "rain"
	cloned[0].ResponseMeta.Extra["trace"].(map[string]any)["id"] = "b"
	if source.ContentBlocks[0].FunctionToolResult.ContentBlocks[0].Text.Text != "sunny" {
		t.Fatal("nested block shares state")
	}
	if source.ResponseMeta.Extra["trace"].(map[string]any)["id"] != "a" {
		t.Fatal("metadata shares state")
	}
}

func TestCloneMessagesStrictPreservesEarlierBlockPayloadError(t *testing.T) {
	message := schema.NewMessage(schema.RoleUser, schema.ContentBlock{
		Type:  schema.ContentBlockTypeText,
		Text:  &schema.Text{Text: "answer", Extra: map[string]any{"mutable": &mutableMessageValue{Value: "shared"}}},
		Image: &schema.MediaContent{URI: "image.png"},
	})

	_, err := schema.CloneMessagesStrict([]schema.Message{message})
	if err == nil || !strings.Contains(err.Error(), `content_blocks[0].text.extra["mutable"]`) {
		t.Fatalf("CloneMessagesStrict error = %v", err)
	}
}

func TestMessageBlockAccessorsAreTopLevelAndDefensive(t *testing.T) {
	message := schema.NewMessage(schema.RoleAssistant,
		schema.NewTextBlock("first"),
		schema.NewReasoningBlock("plan", "sig"),
		schema.NewFunctionToolCallBlock("call-1", "weather", `{}`),
		schema.NewFunctionToolResultBlock("call-1", "weather", false,
			schema.NewTextBlock("nested"),
			schema.NewOpaqueBlock("provider.result", json.RawMessage(`{"ok":true}`)),
		),
		schema.NewTextBlock("second"),
	)
	message.ContentBlocks[0].Text.Extra = map[string]any{"nested": map[string]any{"value": "source"}}
	message.ContentBlocks[1].Reasoning.Extra = map[string]any{"values": []string{"source"}}
	message.ContentBlocks[2].FunctionToolCall.Extra = map[string]any{"trace": map[string]any{"id": "source"}}
	message.ContentBlocks[3].FunctionToolResult.Extra = map[string]any{"values": []string{"source"}}

	texts := message.TextBlocks()
	reasoning := message.ReasoningBlocks()
	calls := message.FunctionToolCalls()
	results := message.FunctionToolResults()
	if len(texts) != 2 || len(reasoning) != 1 || len(calls) != 1 || len(results) != 1 {
		t.Fatalf("accessors returned texts=%d reasoning=%d calls=%d results=%d", len(texts), len(reasoning), len(calls), len(results))
	}
	if got := message.JoinedText("|"); got != "first|second" {
		t.Fatalf("JoinedText() = %q, want first|second", got)
	}

	texts[0].Extra["nested"].(map[string]any)["value"] = "changed"
	reasoning[0].Extra["values"].([]string)[0] = "changed"
	calls[0].Extra["trace"].(map[string]any)["id"] = "changed"
	results[0].Extra["values"].([]string)[0] = "changed"
	results[0].ContentBlocks[0].Text.Text = "changed"
	results[0].ContentBlocks[1].Opaque.Raw[2] = 'x'

	if message.ContentBlocks[0].Text.Extra["nested"].(map[string]any)["value"] != "source" ||
		message.ContentBlocks[1].Reasoning.Extra["values"].([]string)[0] != "source" ||
		message.ContentBlocks[2].FunctionToolCall.Extra["trace"].(map[string]any)["id"] != "source" ||
		message.ContentBlocks[3].FunctionToolResult.Extra["values"].([]string)[0] != "source" ||
		message.ContentBlocks[3].FunctionToolResult.ContentBlocks[0].Text.Text != "nested" ||
		string(message.ContentBlocks[3].FunctionToolResult.ContentBlocks[1].Opaque.Raw) != `{"ok":true}` {
		t.Fatal("accessor result shares mutable state with source")
	}
}

func TestMediaBlockConstructorsValidate(t *testing.T) {
	blocks := []schema.ContentBlock{
		schema.NewImageBlock("image.png", "image/png"),
		schema.NewFileBlock("notes.pdf", "application/pdf"),
		schema.NewAudioBlock("audio.mp3", "audio/mpeg"),
		schema.NewVideoBlock("video.mp4", "video/mp4"),
	}
	for i, block := range blocks {
		if err := block.Validate(); err != nil {
			t.Fatalf("block %d: %v", i, err)
		}
	}
}
