package llm

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/goharness/goharness/pkg/schema"
)

func TestOpenAIResponseKeepsParallelCallsInOneMessage(t *testing.T) {
	data := []byte(`{
	  "id":"resp-1","model":"model-1",
	  "choices":[{"index":0,"finish_reason":"tool_calls","message":{
	    "role":"assistant","content":"checking","reasoning_content":"plan",
	    "tool_calls":[
	      {"id":"call-1","type":"function","function":{"name":"weather","arguments":"{\"city\":\"Shanghai\"}"}},
	      {"id":"call-2","type":"function","function":{"name":"air","arguments":"{\"city\":\"Shanghai\"}"}}
	    ]
	  }}],
	  "usage":{"prompt_tokens":20,"completion_tokens":8,"total_tokens":28,
	    "prompt_tokens_details":{"cached_tokens":16,"cache_write_tokens":4},
	    "completion_tokens_details":{"reasoning_tokens":3}}
	}`)
	messages, usage, finish, err := decodeOpenAIResponse(data, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) != 1 {
		t.Fatalf("messages=%d", len(messages))
	}
	calls := messages[0].FunctionToolCalls()
	if len(calls) != 2 || calls[0].ID != "call-1" || calls[1].ID != "call-2" {
		t.Fatalf("calls=%#v", calls)
	}
	if usage.CacheReadTokens != 16 || usage.CacheWriteTokens != 4 || finish != "tool_calls" {
		t.Fatalf("usage=%#v finish=%q", usage, finish)
	}
	if messages[0].ResponseMeta == nil || messages[0].ResponseMeta.ID != "resp-1" || messages[0].ResponseMeta.Model != "model-1" {
		t.Fatalf("meta=%#v", messages[0].ResponseMeta)
	}
}

func TestOpenAICompatibleEncodesBlockMessages(t *testing.T) {
	messages := []schema.Message{schema.NewMessage(schema.RoleAssistant,
		schema.NewTextBlock("checking"),
		schema.NewReasoningBlock("plan", ""),
		schema.NewFunctionToolCallBlock("call-1", "weather", `{"city":"Shanghai"}`),
		schema.NewFunctionToolCallBlock("call-2", "air", `{"city":"Shanghai"}`),
	)}
	encoded, err := encodeOpenAIMessages(messages, true, false)
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(encoded[0])
	if err != nil {
		t.Fatal(err)
	}
	var got struct {
		Content   string `json:"content"`
		Reasoning string `json:"reasoning_content"`
		ToolCalls []struct {
			ID string `json:"id"`
		} `json:"tool_calls"`
	}
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatal(err)
	}
	if got.Content != "checking" || got.Reasoning != "plan" || len(got.ToolCalls) != 2 || got.ToolCalls[0].ID != "call-1" || got.ToolCalls[1].ID != "call-2" {
		t.Fatalf("encoded=%s", data)
	}
}

func TestOpenAICompatibleRejectsOpaqueBlock(t *testing.T) {
	messages := []schema.Message{
		schema.UserMessage("ok"),
		schema.NewMessage(schema.RoleUser, schema.NewOpaqueBlock("vendor", json.RawMessage(`{"x":1}`))),
	}
	_, err := encodeOpenAIMessages(messages, false, false)
	if err == nil || !strings.Contains(err.Error(), "messages[1].content_blocks[0]") || !strings.Contains(err.Error(), "opaque") {
		t.Fatalf("error=%v", err)
	}
}

func TestOpenAICompatibleRejectsInterleavedToolCallOrder(t *testing.T) {
	messages := []schema.Message{schema.NewMessage(schema.RoleAssistant,
		schema.NewTextBlock("before"), schema.NewFunctionToolCallBlock("c", "f", `{}`), schema.NewTextBlock("after"),
	)}
	_, err := encodeOpenAIMessages(messages, true, false)
	if err == nil || !strings.Contains(err.Error(), "messages[0].content_blocks[2]") {
		t.Fatalf("err=%v", err)
	}
}

func TestOpenAICompatibleRejectsDisabledReasoning(t *testing.T) {
	_, err := encodeOpenAIMessages([]schema.Message{schema.NewMessage(schema.RoleAssistant, schema.NewReasoningBlock("plan", ""))}, false, false)
	if err == nil || !strings.Contains(err.Error(), "reasoning") {
		t.Fatalf("err=%v", err)
	}
}

func TestOpenAIResponsePreservesMixedContentParts(t *testing.T) {
	rawUnknown := []byte(`{ "value" : 3, "type" : "vendor_x" }`)
	data := append([]byte(`{"choices":[{"index":0,"message":{"role":"assistant","content":[{"type":"text","text":"a"},{"type":"image_url","image_url":{"url":"https://x"}},`), rawUnknown...)
	data = append(data, []byte(`]}}]}`)...)
	messages, _, _, err := decodeOpenAIResponse(data, false)
	if err != nil {
		t.Fatal(err)
	}
	blocks := messages[0].ContentBlocks
	if len(blocks) != 3 || blocks[0].Type != schema.ContentBlockTypeText || blocks[1].Type != schema.ContentBlockTypeImage || blocks[2].Type != schema.ContentBlockTypeOpaque {
		t.Fatalf("blocks=%#v", blocks)
	}
	if !bytes.Equal(blocks[2].Opaque.Raw, rawUnknown) {
		t.Fatalf("opaque raw=%q want=%q", blocks[2].Opaque.Raw, rawUnknown)
	}
}

func TestOpenAIStreamPreservesMixedContentParts(t *testing.T) {
	rawUnknown := []byte(`{"type":"vendor_x", "value" : [1, 2]}`)
	data := append([]byte(`{"choices":[{"index":0,"delta":{"role":"assistant","content":[{"type":"text","text":"a"},{"type":"image_url","image_url":{"url":"https://x"}},`), rawUnknown...)
	data = append(data, []byte(`]}}]}`)...)
	messages, _, _, err := decodeOpenAIResponse(data, true)
	if err != nil {
		t.Fatal(err)
	}
	blocks := messages[0].ContentBlocks
	if len(blocks) != 3 || blocks[0].Type != schema.ContentBlockTypeText || blocks[1].Type != schema.ContentBlockTypeImage || blocks[2].Type != schema.ContentBlockTypeOpaque {
		t.Fatalf("blocks=%#v", blocks)
	}
	if !bytes.Equal(blocks[2].Opaque.Raw, rawUnknown) {
		t.Fatalf("opaque raw=%q want=%q", blocks[2].Opaque.Raw, rawUnknown)
	}
}

func TestOpenAIStreamAppendsMixedContentArraysAcrossEvents(t *testing.T) {
	rawOne := []byte(`{ "type":"vendor_one", "n":1 }`)
	rawTwo := []byte(`{"n":2, "type" : "vendor_two"}`)
	makeEvent := func(text string, image bool, raw []byte) []byte {
		parts := `[{"type":"text","text":"` + text + `"}`
		if image {
			parts += `,{"type":"image_url","image_url":{"url":"https://image"}}`
		}
		parts += `,` + string(raw) + `]`
		return []byte(`{"choices":[{"index":0,"delta":{"role":"assistant","content":` + parts + `}}]}`)
	}
	accumulator := newWireAccumulator(context.Background())
	for _, event := range [][]byte{makeEvent("first", true, rawOne), makeEvent("second", false, rawTwo)} {
		messages, _, _, err := decodeOpenAIResponse(event, true)
		if err != nil {
			t.Fatal(err)
		}
		if err := accumulator.apply(messages); err != nil {
			t.Fatal(err)
		}
	}
	message := accumulator.messages()[0]
	blocks := message.ContentBlocks
	want := []schema.ContentBlockType{schema.ContentBlockTypeText, schema.ContentBlockTypeImage, schema.ContentBlockTypeOpaque, schema.ContentBlockTypeText, schema.ContentBlockTypeOpaque}
	if len(blocks) != len(want) {
		t.Fatalf("blocks=%#v", blocks)
	}
	for i := range want {
		if blocks[i].Type != want[i] {
			t.Fatalf("blocks[%d]=%q want=%q", i, blocks[i].Type, want[i])
		}
	}
	if blocks[0].Text.Text != "first" || blocks[3].Text.Text != "second" {
		t.Fatalf("blocks=%#v", blocks)
	}
	if !bytes.Equal(blocks[2].Opaque.Raw, rawOne) || !bytes.Equal(blocks[4].Opaque.Raw, rawTwo) {
		t.Fatalf("opaque=%q %q", blocks[2].Opaque.Raw, blocks[4].Opaque.Raw)
	}
	if len(message.Extra) != 0 {
		t.Fatalf("private delta metadata leaked: %#v", message.Extra)
	}
}

func TestOpenAIStreamPassThroughRemovesPrivateAppendMarker(t *testing.T) {
	rawUnknown := []byte(`{ "type":"vendor_x", "value":7 }`)
	data := append([]byte(`{"choices":[{"index":0,"delta":{"role":"assistant","content":[{"type":"text","text":"a"},`), rawUnknown...)
	data = append(data, []byte(`]}}]}`)...)
	messages, _, _, err := decodeOpenAIResponse(data, true)
	if err != nil {
		t.Fatal(err)
	}
	accumulator := newWireAccumulator(context.Background())
	if err := accumulator.apply(messages); err != nil {
		t.Fatal(err)
	}
	output := accumulator.messages()
	if len(output) != 1 || len(output[0].ContentBlocks) != 2 {
		t.Fatalf("output=%#v", output)
	}
	if output[0].ContentBlocks[0].Text.Text != "a" || !bytes.Equal(output[0].ContentBlocks[1].Opaque.Raw, rawUnknown) {
		t.Fatalf("blocks=%#v", output[0].ContentBlocks)
	}
	if len(output[0].Extra) != 0 {
		t.Fatalf("private delta metadata leaked: %#v", output[0].Extra)
	}
}

func TestOpenAIToolResultRejectsMultipleNestedBlocks(t *testing.T) {
	m := schema.NewMessage(schema.RoleTool, schema.NewFunctionToolResultBlock("c", "f", false, schema.NewTextBlock("a"), schema.NewTextBlock("b")))
	_, err := encodeOpenAIMessages([]schema.Message{m}, true, false)
	if err == nil || !strings.Contains(err.Error(), "function tool result") {
		t.Fatalf("err=%v", err)
	}
}
