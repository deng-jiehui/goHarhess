package retrieval_test

import (
	"context"
	"strings"
	"testing"

	"github.com/goharness/goharness/extensions/retrieval"
)

func TestTextChunkerRespectsTokenLimitsOverlapAndOffsets(t *testing.T) {
	chunker, err := retrieval.NewTextChunker(retrieval.ChunkConfig{
		MaxTokens: 6, OverlapTokens: 2, MinTokens: 2,
	}, wordCounter{})
	if err != nil {
		t.Fatal(err)
	}
	document := retrieval.Document{
		ID:       "guide",
		Text:     "# Intro\n\none two three four. five six seven eight.\n\n# Next\n\nnine ten eleven twelve.",
		Metadata: retrieval.Metadata{"source": map[string]any{"path": "guide.md"}},
	}
	chunks, err := chunker.Invoke(context.Background(), document)
	if err != nil {
		t.Fatal(err)
	}
	if len(chunks) < 3 {
		t.Fatalf("chunks = %#v", chunks)
	}
	runes := []rune(document.Text)
	overlapFound := false
	for index, chunk := range chunks {
		if chunk.Index != index || chunk.Tokens > 6 || chunk.Tokens == 0 {
			t.Fatalf("chunk %d = %#v", index, chunk)
		}
		if got := string(runes[chunk.Start:chunk.End]); got != chunk.Text {
			t.Fatalf("chunk %d offsets produce %q, want %q", index, got, chunk.Text)
		}
		if index > 0 && chunk.Start < chunks[index-1].End {
			overlapFound = true
		}
	}
	if !overlapFound {
		t.Fatal("chunker did not produce any configured overlap")
	}
	again, err := chunker.Invoke(context.Background(), document)
	if err != nil {
		t.Fatal(err)
	}
	for index := range chunks {
		if chunks[index].ID != again[index].ID {
			t.Fatalf("chunk ID changed: %q != %q", chunks[index].ID, again[index].ID)
		}
	}
	chunks[0].Metadata["source"].(map[string]any)["path"] = "changed"
	againSource := again[0].Metadata["source"].(map[string]any)
	if againSource["path"] != "guide.md" {
		t.Fatalf("chunk metadata was not defensively cloned: %#v", again[0].Metadata)
	}
}

func TestApproxTokenCounterHandlesCJKAndWords(t *testing.T) {
	if got := (retrieval.ApproxTokenCounter{}).Count("你好 world!"); got != 4 {
		t.Fatalf("tokens = %d, want 4", got)
	}
}

type wordCounter struct{}

func (wordCounter) Count(text string) int {
	return len(strings.Fields(text))
}
