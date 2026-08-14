package schema_test

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"goharness/pkg/schema"
)

func TestMessageHasOnlyBlockContentModel(t *testing.T) {
	root, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(root, "go.mod")); err == nil {
			break
		}
		parent := filepath.Dir(root)
		if parent == root {
			t.Fatal("repository root not found")
		}
		root = parent
	}
	patterns := []*regexp.Regexp{
		regexp.MustCompile(`schema\.Content\b|\bschema\.FunctionCall\b|\bContentItem\b|\bContentType\b|\b(TextContent|MultiContent)\s*\(`),
		regexp.MustCompile(`\bRoleFunction\b|\.FunctionID\s*\(|func \(m Message\) Text\(\)`),
		regexp.MustCompile(`\bReasoningContent\s+Content\b|\bFunctionCall\s*\*\s*FunctionCall\b|\bContent\s+Content\s+`),
	}
	var findings []string
	for _, dir := range []string{"pkg", "internal", "extensions"} {
		err := filepath.WalkDir(filepath.Join(root, dir), func(path string, entry os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() || filepath.Ext(path) != ".go" || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			data, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			for lineNo, line := range strings.Split(string(data), "\n") {
				for _, pattern := range patterns {
					if pattern.MatchString(line) {
						findings = append(findings, fmt.Sprintf("%s:%d: %s", filepath.ToSlash(path), lineNo+1, strings.TrimSpace(line)))
						break
					}
				}
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	if len(findings) > 0 {
		t.Fatalf("legacy production schema API remains:\n%s", strings.Join(findings, "\n"))
	}
}

func TestLegacyScannerDoesNotAllowProviderTokenToMaskLegacyToken(t *testing.T) {
	patterns := []*regexp.Regexp{
		regexp.MustCompile(`schema\.Content\b|\bschema\.FunctionCall\b|\bContentItem\b|\bContentType\b|\b(TextContent|MultiContent)\s*\(`),
		regexp.MustCompile(`\bRoleFunction\b|\.FunctionID\s*\(|func \(m Message\) Text\(\)`),
		regexp.MustCompile(`\bReasoningContent\s+Content\b|\bFunctionCall\s*\*\s*FunctionCall\b|\bContent\s+Content\s+`),
	}
	for _, line := range []string{`FunctionCall *FunctionCall // provider FunctionCall`, `ReasoningContent Content // provider reasoning_content`} {
		found := false
		for _, pattern := range patterns {
			found = found || pattern.MatchString(line)
		}
		if !found {
			t.Fatalf("legacy token was masked: %q", line)
		}
	}
}

func TestMessageTextBlocksJSON(t *testing.T) {
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
	if got.JoinedText("") != "hello" {
		t.Fatalf("content = %q, want hello", got.JoinedText(""))
	}
}

type mutableMessageValue struct {
	Value string
}

func TestCloneMessagesStrictRecursivelyIsolatesMutableContent(t *testing.T) {
	message := schema.NewMessage(schema.RoleUser, schema.NewAudioBlock("audio.wav", "audio/wav"), schema.NewVideoBlock("video.mp4", "video/mp4"))
	message.Extra = map[string]any{
		"nested": map[string]any{"items": []string{"original"}},
	}
	cloned, err := schema.CloneMessagesStrict([]schema.Message{message})
	if err != nil {
		t.Fatal(err)
	}
	cloned[0].Extra["nested"].(map[string]any)["items"].([]string)[0] = "changed"

	if message.Extra["nested"].(map[string]any)["items"].([]string)[0] != "original" {
		t.Fatal("cloned message Extra shares nested state")
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

func TestMessageMultimodalBlocksJSON(t *testing.T) {
	msg := schema.NewMessage(schema.RoleUser, schema.NewTextBlock("read this"), schema.NewFileBlock("notes.pdf", "application/pdf"))
	data, err := json.Marshal(msg)
	if err != nil {
		t.Fatal(err)
	}

	var got schema.Message
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatal(err)
	}
	if len(got.ContentBlocks) != 2 || got.ContentBlocks[1].File == nil || got.ContentBlocks[1].File.URI != "notes.pdf" {
		t.Fatalf("content blocks = %#v, want notes.pdf", got.ContentBlocks)
	}
}

func TestBlockAwareMessageHelpers(t *testing.T) {
	messages := []schema.Message{
		schema.UserMessage("earlier"),
		schema.NewMessage(schema.RoleAssistant,
			schema.NewTextBlock("answer"),
			schema.NewFileBlock("assistant.pdf", "application/pdf"),
		),
		schema.NewMessage(schema.RoleUser,
			schema.NewTextBlock("你好"),
			schema.NewImageBlock("image.png", "image/png"),
			schema.NewFileBlock("notes.pdf", "application/pdf"),
			schema.NewTextBlock("world"),
		),
	}

	if !schema.HasChinese(messages) {
		t.Fatal("HasChinese() = false, want true for top-level text block")
	}
	if got := schema.LastUserText(messages); got != "你好world" {
		t.Fatalf("LastUserText() = %q, want 你好world", got)
	}
	if got := schema.ExtractFiles(messages, false); strings.Join(got, ",") != "assistant.pdf,notes.pdf" {
		t.Fatalf("ExtractFiles(false) = %#v", got)
	}
	if got := schema.ExtractFiles(messages, true); strings.Join(got, ",") != "assistant.pdf,image.png,notes.pdf" {
		t.Fatalf("ExtractFiles(true) = %#v", got)
	}
	if got := messages[2].JoinedText(""); strings.Contains(got, "image.png") || strings.Contains(got, "notes.pdf") {
		t.Fatalf("JoinedText() includes media URI: %q", got)
	}
}
