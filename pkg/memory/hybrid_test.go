package memory_test

import (
	"context"
	"strings"
	"testing"

	"goharness/pkg/memory"
	"goharness/pkg/retrieval"
	"goharness/pkg/runnable"
)

func TestHybridRetrieverFiltersFilesAndHonorsReferenceBudget(t *testing.T) {
	searcher := &recordingHybridSearch{hits: []retrieval.SearchHit{
		{ID: "a", Source: "keyword+vector", Score: 0.9, Text: "one two three four five six seven eight"},
		{ID: "b", Source: "vector", Score: 0.8, Text: "nine ten eleven twelve"},
	}}
	retriever, err := memory.NewHybridRetriever(memory.HybridRetrieverConfig{
		Searcher: searcher, Index: "docs", Namespace: "tenant", TopK: 2, TokenCounter: fieldCounter{},
	})
	if err != nil {
		t.Fatal(err)
	}
	contextText, err := retriever.Invoke(context.Background(), memory.RetrieveRequest{
		Query: "question", Files: []string{"guide.pdf"}, Config: memory.RAGConfig{MaxRefToken: 10},
	})
	if err != nil {
		t.Fatal(err)
	}
	if (fieldCounter{}).Count(contextText) > 10 || !strings.Contains(contextText, "Reference 1") {
		t.Fatalf("context = %q", contextText)
	}
	query := searcher.query
	if query.Index != "docs" || query.Namespace != "tenant" || query.TopK != 2 {
		t.Fatalf("query = %#v", query)
	}
	fileFilter := query.Filter["file"].(map[string]any)["$in"].([]any)
	if len(fileFilter) != 1 || fileFilter[0] != "guide.pdf" {
		t.Fatalf("file filter = %#v", fileFilter)
	}
}

type recordingHybridSearch struct {
	hits  []retrieval.SearchHit
	query retrieval.HybridQuery
}

func (s *recordingHybridSearch) Invoke(ctx context.Context, query retrieval.HybridQuery, _ ...runnable.Option) ([]retrieval.SearchHit, error) {
	s.query = query
	return append([]retrieval.SearchHit(nil), s.hits...), nil
}

type fieldCounter struct{}

func (fieldCounter) Count(text string) int {
	return len(strings.Fields(text))
}
