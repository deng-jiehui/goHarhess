package schema_test

import (
	"encoding/json"
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
