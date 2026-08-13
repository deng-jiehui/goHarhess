package retrieval_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"goharness/pkg/callback"
	"goharness/pkg/retrieval"
	"goharness/pkg/runnable"
)

func TestHybridSearcherDispatchesLifecycleCallbacksWithoutChangingResults(t *testing.T) {
	events := make(chan string, 2)
	observer := retrieval.CallbackFuncs{CallbackName: "retrieval-audit", Start: func(context.Context, retrieval.CallbackInput) error { events <- "start"; return nil }, End: func(context.Context, retrieval.CallbackOutput) error { events <- "end"; return nil }}
	manager, err := callback.NewManager(callback.Config{}, []callback.Handler{observer})
	if err != nil {
		t.Fatal(err)
	}
	searcher := mustHybridSearcher(t, retrieval.HybridSearchConfig{
		Embedder: fixedEmbedder{vector: []float32{1}}, VectorStore: &fixedVectorStore{hits: []retrieval.SearchHit{{ID: "a", Score: 1, Text: "A"}}}, KeywordSearch: fixedSearchEngine{hits: []retrieval.SearchHit{{ID: "a", Score: 1, Text: "A"}}}, FinalTopK: 1,
	})
	hits, err := searcher.Invoke(callback.WithManager(context.Background(), manager), retrieval.HybridQuery{Text: "query"})
	if err != nil || len(hits) != 1 || hits[0].ID != "a" {
		t.Fatalf("hits=%#v err=%v", hits, err)
	}
	close(events)
	var got []string
	for event := range events {
		got = append(got, event)
	}
	if fmt.Sprint(got) != "[start end]" {
		t.Fatalf("events=%v", got)
	}
}

func TestDocumentIndexerBatchesEmbeddingAndUpsert(t *testing.T) {
	chunker := fixedChunker{chunks: []retrieval.Chunk{
		{ID: "c1", DocumentID: "d", Index: 0, Text: "one", Tokens: 1},
		{ID: "c2", DocumentID: "d", Index: 1, Text: "two", Tokens: 1},
		{ID: "c3", DocumentID: "d", Index: 2, Text: "three", Tokens: 1},
		{ID: "c4", DocumentID: "d", Index: 3, Text: "four", Tokens: 1},
		{ID: "c5", DocumentID: "d", Index: 4, Text: "five", Tokens: 1},
	}}
	embedder := &batchEmbedder{}
	store := &capturingVectorStore{}
	indexer, err := retrieval.NewDocumentIndexer(retrieval.IndexerConfig{
		Chunker: chunker, Embedder: embedder, Store: store,
		EmbeddingModel: "embed", Namespace: "docs",
		EmbeddingBatchSize: 2, UpsertBatchSize: 3, MaxConcurrency: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	stats, err := indexer.Invoke(context.Background(), []retrieval.Document{{ID: "d", Text: "ignored"}})
	if err != nil {
		t.Fatal(err)
	}
	if stats.Documents != 1 || stats.Chunks != 5 || stats.Tokens != 5 || stats.EmbeddingBatches != 3 || stats.UpsertBatches != 2 {
		t.Fatalf("stats = %#v", stats)
	}
	if got := embedder.BatchSizes(); fmt.Sprint(got) != "[1 2 2]" && fmt.Sprint(got) != "[2 1 2]" && fmt.Sprint(got) != "[2 2 1]" {
		t.Fatalf("embedding batches = %#v", got)
	}
	batches := store.Batches()
	if len(batches) != 2 || len(batches[0]) != 3 || len(batches[1]) != 2 {
		t.Fatalf("upsert batches = %#v", batches)
	}
	for _, batch := range batches {
		for _, record := range batch {
			if record.Namespace != "docs" || len(record.Vector) != 1 || record.Metadata["_rag_document_id"] != "d" {
				t.Fatalf("record = %#v", record)
			}
		}
	}
}

func TestHybridSearcherFusesAndDeduplicatesResults(t *testing.T) {
	vector := &fixedVectorStore{hits: []retrieval.SearchHit{
		{ID: "a", Score: 0.9, Text: "A", Vector: []float32{1, 0}},
		{ID: "b", Score: 0.8, Text: "B", Vector: []float32{0.8, 0.2}},
	}}
	keyword := fixedSearchEngine{hits: []retrieval.SearchHit{
		{ID: "b", Score: 12, Text: "B keyword"},
		{ID: "c", Score: 10, Text: "C"},
	}}
	searcher := mustHybridSearcher(t, retrieval.HybridSearchConfig{
		Embedder: fixedEmbedder{vector: []float32{1, 0}}, VectorStore: vector, KeywordSearch: keyword,
		FinalTopK: 3, Fusion: retrieval.FusionRRF,
	})
	hits, err := searcher.Invoke(context.Background(), retrieval.HybridQuery{Text: "query"})
	if err != nil {
		t.Fatal(err)
	}
	if len(hits) != 3 || hits[0].ID != "b" {
		t.Fatalf("hits = %#v", hits)
	}
	if hits[0].Source != "keyword+vector" {
		t.Fatalf("source = %q", hits[0].Source)
	}
	if hits[0].Raw["vector_score"] != 0.8 || hits[0].Raw["keyword_score"] != float64(12) {
		t.Fatalf("raw scores = %#v", hits[0].Raw)
	}
}

func TestHybridSearcherAppliesMMRAndRerank(t *testing.T) {
	vector := &fixedVectorStore{hits: []retrieval.SearchHit{
		{ID: "a", Score: 0.9, Text: "A", Vector: []float32{1, 0}},
		{ID: "b", Score: 0.8, Text: "B", Vector: []float32{0.99, 0.01}},
		{ID: "c", Score: 0.7, Text: "C", Vector: []float32{0, 1}},
	}}
	keyword := fixedSearchEngine{hits: []retrieval.SearchHit{
		{ID: "a", Score: 3, Text: "A"}, {ID: "b", Score: 2, Text: "B"}, {ID: "c", Score: 1, Text: "C"},
	}}
	reranker := &fixedReranker{results: []retrieval.RerankResult{{Index: 1, Score: 0.99}, {Index: 0, Score: 0.8}}}
	searcher := mustHybridSearcher(t, retrieval.HybridSearchConfig{
		Embedder: fixedEmbedder{vector: []float32{1, 0}}, VectorStore: vector, KeywordSearch: keyword,
		Reranker: reranker, FinalTopK: 2, RerankTopK: 3, MMRLambda: 0.5,
	})
	hits, err := searcher.Invoke(context.Background(), retrieval.HybridQuery{Text: "query"})
	if err != nil {
		t.Fatal(err)
	}
	if len(hits) != 2 || hits[0].ID != "c" || hits[0].Score != 0.99 {
		t.Fatalf("reranked hits = %#v", hits)
	}
	if !vector.LastQuery().Include.Vectors {
		t.Fatal("MMR did not request vectors")
	}
}

func TestHybridSearcherCanDegradeToOneRetriever(t *testing.T) {
	vector := &fixedVectorStore{hits: []retrieval.SearchHit{{ID: "a", Score: 1, Text: "A"}}}
	keywordErr := errors.New("search unavailable")
	config := retrieval.HybridSearchConfig{
		Embedder: fixedEmbedder{vector: []float32{1}}, VectorStore: vector,
		KeywordSearch: fixedSearchEngine{err: keywordErr}, FinalTopK: 1,
	}
	searcher := mustHybridSearcher(t, config)
	hits, err := searcher.Invoke(context.Background(), retrieval.HybridQuery{Text: "query"})
	if err != nil || len(hits) != 1 || hits[0].ID != "a" {
		t.Fatalf("hits = %#v, err = %v", hits, err)
	}
	config.RequireBoth = true
	searcher = mustHybridSearcher(t, config)
	if _, err := searcher.Invoke(context.Background(), retrieval.HybridQuery{Text: "query"}); !errors.Is(err, keywordErr) {
		t.Fatalf("strict error = %v", err)
	}
}

type fixedChunker struct {
	chunks []retrieval.Chunk
}

func (c fixedChunker) Invoke(context.Context, retrieval.Document, ...runnable.Option) ([]retrieval.Chunk, error) {
	return append([]retrieval.Chunk(nil), c.chunks...), nil
}

type batchEmbedder struct {
	mu         sync.Mutex
	batchSizes []int
}

func (e *batchEmbedder) Invoke(ctx context.Context, request retrieval.EmbeddingRequest, _ ...runnable.Option) (retrieval.EmbeddingResponse, error) {
	e.mu.Lock()
	e.batchSizes = append(e.batchSizes, len(request.Inputs))
	e.mu.Unlock()
	vectors := make([][]float32, len(request.Inputs))
	for index, input := range request.Inputs {
		vectors[index] = []float32{float32(len(input))}
	}
	return retrieval.EmbeddingResponse{Vectors: vectors}, nil
}

func (e *batchEmbedder) BatchSizes() []int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]int(nil), e.batchSizes...)
}

type capturingVectorStore struct {
	mu      sync.Mutex
	batches [][]retrieval.VectorRecord
}

func (s *capturingVectorStore) Invoke(ctx context.Context, request retrieval.VectorRequest, _ ...runnable.Option) (retrieval.VectorResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if request.Operation == retrieval.VectorUpsertOperation {
		s.batches = append(s.batches, append([]retrieval.VectorRecord(nil), request.Records...))
	}
	return retrieval.VectorResponse{}, nil
}

func (s *capturingVectorStore) Batches() [][]retrieval.VectorRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([][]retrieval.VectorRecord(nil), s.batches...)
}

type fixedEmbedder struct {
	vector []float32
	err    error
}

func (e fixedEmbedder) Invoke(context.Context, retrieval.EmbeddingRequest, ...runnable.Option) (retrieval.EmbeddingResponse, error) {
	if e.err != nil {
		return retrieval.EmbeddingResponse{}, e.err
	}
	return retrieval.EmbeddingResponse{Vectors: [][]float32{append([]float32(nil), e.vector...)}}, nil
}

type fixedVectorStore struct {
	mu        sync.Mutex
	hits      []retrieval.SearchHit
	err       error
	lastQuery retrieval.VectorQuery
}

func (s *fixedVectorStore) Invoke(ctx context.Context, request retrieval.VectorRequest, _ ...runnable.Option) (retrieval.VectorResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if request.Query != nil {
		s.lastQuery = *request.Query
	}
	return retrieval.VectorResponse{Hits: append([]retrieval.SearchHit(nil), s.hits...)}, s.err
}

func (s *fixedVectorStore) LastQuery() retrieval.VectorQuery {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastQuery
}

type fixedSearchEngine struct {
	hits []retrieval.SearchHit
	err  error
}

func (s fixedSearchEngine) Invoke(context.Context, retrieval.TextSearchRequest, ...runnable.Option) ([]retrieval.SearchHit, error) {
	return append([]retrieval.SearchHit(nil), s.hits...), s.err
}

type fixedReranker struct {
	results []retrieval.RerankResult
	err     error
}

func (r *fixedReranker) Invoke(context.Context, retrieval.RerankRequest, ...runnable.Option) ([]retrieval.RerankResult, error) {
	return append([]retrieval.RerankResult(nil), r.results...), r.err
}

func mustHybridSearcher(t *testing.T, config retrieval.HybridSearchConfig) *retrieval.HybridSearcher {
	t.Helper()
	searcher, err := retrieval.NewHybridSearcher(config)
	if err != nil {
		t.Fatal(err)
	}
	return searcher
}
