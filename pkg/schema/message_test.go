package schema_test

import (
	"encoding/json"
	"strings"
	"testing"

	"goharness/pkg/schema"
)

func TestMessageTextContentJSON(t *testing.T) {
	msg := schema.UserMessage("hello")
	data, err := json.Marshal(msg)
	if err != nil {
		t.Fatal(err)
	}

	var got schema.Message
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatal(err)
	}
	if got.Role != schema.RoleUser {
		t.Fatalf("role = %q, want %q", got.Role, schema.RoleUser)
	}
	if got.Text() != "hello" {
		t.Fatalf("content = %q, want hello", got.Text())
	}
}

type mutableMessageValue struct {
	Value string
}

func TestCloneMessagesStrictRecursivelyIsolatesMutableContent(t *testing.T) {
	message := schema.NewMessage(schema.RoleUser, schema.MultiContent(
		schema.ContentItem{Audio: map[string]any{"samples": []int{1, 2}}},
		schema.ContentItem{Video: []map[string]any{{"frames": []string{"first"}}}},
	))
	message.Extra = map[string]any{
		"nested": map[string]any{"items": []string{"original"}},
	}
	cloned, err := schema.CloneMessagesStrict([]schema.Message{message})
	if err != nil {
		t.Fatal(err)
	}
	cloned[0].Extra["nested"].(map[string]any)["items"].([]string)[0] = "changed"
	items := cloned[0].Content.Items()
	items[0].Audio.(map[string]any)["samples"].([]int)[0] = 9
	items[1].Video.([]map[string]any)[0]["frames"].([]string)[0] = "changed"

	if message.Extra["nested"].(map[string]any)["items"].([]string)[0] != "original" {
		t.Fatal("cloned message Extra shares nested state")
	}
	originalItems := message.Content.Items()
	if originalItems[0].Audio.(map[string]any)["samples"].([]int)[0] != 1 {
		t.Fatal("cloned audio content shares nested state")
	}
	if originalItems[1].Video.([]map[string]any)[0]["frames"].([]string)[0] != "first" {
		t.Fatal("cloned video content shares nested state")
	}
}

func TestCloneMessagesStrictRejectsUnknownPointer(t *testing.T) {
	message := schema.UserMessage("hello")
	message.Extra = map[string]any{"mutable": &mutableMessageValue{Value: "shared"}}
	_, err := schema.CloneMessagesStrict([]schema.Message{message})
	if err == nil || !strings.Contains(err.Error(), "implement Clone() any") {
		t.Fatalf("CloneMessagesStrict error = %v", err)
	}
}

func TestMessageMultimodalContentJSON(t *testing.T) {
	msg := schema.NewMessage(schema.RoleUser, schema.MultiContent(
		schema.TextItem("read this"),
		schema.ContentItem{File: "notes.pdf"},
	))
	data, err := json.Marshal(msg)
	if err != nil {
		t.Fatal(err)
	}

	var got schema.Message
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatal(err)
	}
	if !got.Content.IsMultimodal() {
		t.Fatal("content should be multimodal")
	}
	files := schema.ExtractFiles([]schema.Message{got}, false)
	if len(files) != 1 || files[0] != "notes.pdf" {
		t.Fatalf("files = %#v, want notes.pdf", files)
	}
}
