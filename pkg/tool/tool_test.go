package tool_test

import (
	"strings"
	"testing"

	"github.com/goharness/goharness/pkg/schema"
	"github.com/goharness/goharness/pkg/tool"
)

func TestParseArgsStrict(t *testing.T) {
	args, err := tool.ParseArgsStrict(`{"query":"go","limit":2}`)
	if err != nil {
		t.Fatal(err)
	}
	if args["query"] != "go" || args["limit"] != float64(2) {
		t.Fatalf("args = %#v", args)
	}
}

func TestParseArgsStrictRejectsInvalidOrNonObjectJSON(t *testing.T) {
	for _, raw := range []string{`{"query":`, `[]`, `null`} {
		t.Run(raw, func(t *testing.T) {
			_, err := tool.ParseArgsStrict(raw)
			if err == nil {
				t.Fatalf("ParseArgsStrict(%q) returned nil error", raw)
			}
			if !strings.Contains(err.Error(), "tool arguments") {
				t.Fatalf("ParseArgsStrict(%q) error = %v", raw, err)
			}
		})
	}
}

func TestParseArgsRemainsTolerantForPartialStreams(t *testing.T) {
	args := tool.ParseArgs(`{"query":`)
	if len(args) != 0 {
		t.Fatalf("args = %#v, want empty map", args)
	}
}

func TestResultToContentBlocksPreservesExplicitBlocks(t *testing.T) {
	result := tool.Result{ContentBlocks: []schema.ContentBlock{
		schema.NewTextBlock("first"),
		schema.NewImageBlock("https://example.test/a.png", "image/png"),
	}}
	blocks, err := result.ToContentBlocks()
	if err != nil {
		t.Fatal(err)
	}
	if len(blocks) != 2 || blocks[0].Text == nil || blocks[0].Text.Text != "first" {
		t.Fatalf("blocks=%#v", blocks)
	}
	blocks[0].Text.Text = "mutated"
	if result.ContentBlocks[0].Text.Text != "first" {
		t.Fatalf("result blocks aliased returned blocks: %#v", result.ContentBlocks)
	}
}

func TestResultToContentBlocksRejectsUnsupportedMutableValues(t *testing.T) {
	result := tool.BlockResult(schema.ContentBlock{Type: schema.ContentBlockTypeText, Text: &schema.Text{Text: "x", Extra: map[string]any{"bad": make(chan int)}}})
	if _, err := result.ToContentBlocks(); err == nil {
		t.Fatal("ToContentBlocks() error = nil, want unsupported mutable value error")
	}
}

func TestResultDataProducesIndentedJSONTextBlock(t *testing.T) {
	blocks, err := tool.DataResult(map[string]any{"ok": true}).ToContentBlocks()
	if err != nil {
		t.Fatal(err)
	}
	if len(blocks) != 1 || blocks[0].Text == nil || blocks[0].Text.Text != "{\n  \"ok\": true\n}" {
		t.Fatalf("blocks=%#v", blocks)
	}
}

func TestResultToContentBlocksAddsSerializationContext(t *testing.T) {
	_, err := tool.DataResult(make(chan int)).ToContentBlocks()
	if err == nil || !strings.Contains(err.Error(), "marshal tool result") {
		t.Fatalf("ToContentBlocks error = %v", err)
	}
}
