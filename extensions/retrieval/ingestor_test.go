package retrieval_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/goharness/goharness/extensions/retrieval"
	"github.com/goharness/goharness/extensions/retrieval/loader"
	"github.com/goharness/goharness/pkg/runnable"
	"github.com/goharness/goharness/pkg/state"
)

// fakeVectorStore 记录 upsert/delete/list 调用，用于断言副作用次数。
type fakeVectorStore struct {
	mu          sync.Mutex
	upsertCalls atomic.Int32
	listCalls   atomic.Int32
	deleteCalls atomic.Int32
	records     map[string]retrieval.VectorRecord // ns/id → record
	listErr     error
}

func newFakeVectorStore() *fakeVectorStore {
	return &fakeVectorStore{records: map[string]retrieval.VectorRecord{}}
}

func (f *fakeVectorStore) Invoke(_ context.Context, req retrieval.VectorRequest, _ ...runnable.Option) (retrieval.VectorResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	switch req.Operation {
	case retrieval.VectorUpsertOperation:
		for _, rec := range req.Records {
			f.records[rec.Namespace+"/"+rec.ID] = rec
			f.upsertCalls.Add(1)
		}
	case retrieval.VectorListOperation:
		f.listCalls.Add(1)
		var hits []retrieval.SearchHit
		want, _ := req.Filter["_rag_document_id"].(string)
		for key, rec := range f.records {
			if !strings.HasPrefix(key, req.Namespace+"/") {
				continue
			}
			if want != "" && rec.Metadata["_rag_document_id"] != want {
				continue
			}
			hits = append(hits, retrieval.SearchHit{ID: rec.ID, Metadata: rec.Metadata})
		}
		return retrieval.VectorResponse{Hits: hits}, f.listErr
	case retrieval.VectorDeleteOperation:
		f.deleteCalls.Add(1)
		for _, id := range req.IDs {
			delete(f.records, req.Namespace+"/"+id)
		}
	}
	return retrieval.VectorResponse{}, nil
}

func (f *fakeVectorStore) Stream(ctx context.Context, in retrieval.VectorRequest, o ...runnable.Option) (*runnable.StreamReader[retrieval.VectorResponse], error) {
	return runnable.StreamFromInvoke(ctx, in, f.Invoke, o...)
}
func (f *fakeVectorStore) Collect(ctx context.Context, in *runnable.StreamReader[retrieval.VectorRequest], o ...runnable.Option) (retrieval.VectorResponse, error) {
	return runnable.CollectFromInvoke(ctx, in, f.Invoke, o...)
}
func (f *fakeVectorStore) Transform(ctx context.Context, in *runnable.StreamReader[retrieval.VectorRequest], o ...runnable.Option) (*runnable.StreamReader[retrieval.VectorResponse], error) {
	return runnable.TransformFromInvoke(ctx, in, f.Invoke, o...)
}

// countEmbedder 统计嵌入调用次数；failAt ≥0 时第 N 次调用失败。
type countEmbedder struct {
	calls  atomic.Int32
	failAt atomic.Int32
	dim    int
}

func (e *countEmbedder) Invoke(_ context.Context, req retrieval.EmbeddingRequest, _ ...runnable.Option) (retrieval.EmbeddingResponse, error) {
	call := e.calls.Add(1)
	if at := e.failAt.Load(); at >= 0 && int(call) == int(at) {
		return retrieval.EmbeddingResponse{}, errors.New("embedder injected failure")
	}
	vectors := make([][]float32, len(req.Inputs))
	for i := range vectors {
		vectors[i] = []float32{float32(call), float32(i)}
	}
	return retrieval.EmbeddingResponse{Vectors: vectors}, nil
}

func (e *countEmbedder) Stream(ctx context.Context, in retrieval.EmbeddingRequest, o ...runnable.Option) (*runnable.StreamReader[retrieval.EmbeddingResponse], error) {
	return runnable.StreamFromInvoke(ctx, in, e.Invoke, o...)
}
func (e *countEmbedder) Collect(ctx context.Context, in *runnable.StreamReader[retrieval.EmbeddingRequest], o ...runnable.Option) (retrieval.EmbeddingResponse, error) {
	return runnable.CollectFromInvoke(ctx, in, e.Invoke, o...)
}
func (e *countEmbedder) Transform(ctx context.Context, in *runnable.StreamReader[retrieval.EmbeddingRequest], o ...runnable.Option) (*runnable.StreamReader[retrieval.EmbeddingResponse], error) {
	return runnable.TransformFromInvoke(ctx, in, e.Invoke, o...)
}

func newTestIngestor(t *testing.T, store *fakeVectorStore, emb *countEmbedder, checkpoints state.Store) *retrieval.Ingestor {
	t.Helper()
	chunker, err := retrieval.NewTextChunker(retrieval.ChunkConfig{MaxTokens: 4, DisableOverlap: true}, nil)
	if err != nil {
		t.Fatal(err)
	}
	ing, err := retrieval.NewIngestor(retrieval.IngestorConfig{
		Loader:             loader.Text(),
		Chunker:            runnable.Func[retrieval.Document, []retrieval.Chunk](chunker.Invoke),
		Embedder:           emb,
		Store:              store,
		Namespace:          "ns-test",
		EmbeddingBatchSize: 2,
		Checkpoints:        checkpoints,
	})
	if err != nil {
		t.Fatal(err)
	}
	return ing
}

func ingestReq(id, text string) retrieval.IngestRequest {
	return retrieval.IngestRequest{
		ID:     id,
		Source: retrieval.Source{Body: strings.NewReader(text)},
	}
}

// 幂等：重复入库同一版本不产生副作用。
func TestIngestorIdempotent(t *testing.T) {
	store := newFakeVectorStore()
	emb := &countEmbedder{failAt: atomic.Int32{}}
	emb.failAt.Store(-1)
	ing := newTestIngestor(t, store, emb, state.NewMemoryStore())

	stats, err := ing.Invoke(context.Background(), ingestReq("doc1", "a b c d e f g h"))
	if err != nil {
		t.Fatal(err)
	}
	if stats.Chunks != 2 || stats.Upserted != 2 || stats.Unchanged {
		t.Fatalf("stats = %+v", stats)
	}
	embedCalls := emb.calls.Load()
	upsertCalls := store.upsertCalls.Load()

	stats2, err := ing.Invoke(context.Background(), ingestReq("doc1", "a b c d e f g h"))
	if err != nil {
		t.Fatal(err)
	}
	if !stats2.Unchanged {
		t.Fatalf("repeat ingest should be unchanged: %+v", stats2)
	}
	if emb.calls.Load() != embedCalls {
		t.Fatal("re-ingest re-embedded chunks")
	}
	if store.upsertCalls.Load() != upsertCalls {
		t.Fatal("re-ingest re-upserted chunks")
	}
}

// 两步更新：新块先写，旧版本块后删，无空窗。
func TestIngestorUpdateRemovesStale(t *testing.T) {
	store := newFakeVectorStore()
	emb := &countEmbedder{}
	emb.failAt.Store(-1)
	ing := newTestIngestor(t, store, emb, state.NewMemoryStore())

	if _, err := ing.Invoke(context.Background(), ingestReq("doc1", "a b c d e f g h")); err != nil {
		t.Fatal(err)
	}
	oldIDs := map[string]bool{}
	for key := range store.records {
		oldIDs[key] = true
	}
	stats, err := ing.Invoke(context.Background(), ingestReq("doc1", "x y z"))
	if err != nil {
		t.Fatal(err)
	}
	if stats.Unchanged {
		t.Fatal("new content should not be unchanged")
	}
	if stats.Removed != 2 {
		t.Fatalf("removed = %d, want 2 old chunks", stats.Removed)
	}
	for key := range oldIDs {
		if _, ok := store.records[key]; ok {
			t.Fatalf("stale chunk %q still present", key)
		}
	}
	if len(store.records) != 1 {
		t.Fatalf("records = %d, want 1 new chunk", len(store.records))
	}
	for _, rec := range store.records {
		if rec.Text != "x y z" {
			t.Fatalf("record text = %q", rec.Text)
		}
	}
}

// 中途失败恢复：第 2 批嵌入失败，重跑时只重做未完成批次。
func TestIngestorResumeSkipsDoneBatches(t *testing.T) {
	store := newFakeVectorStore()
	emb := &countEmbedder{}
	emb.failAt.Store(2) // 第 2 次嵌入调用失败
	ing := newTestIngestor(t, store, emb, state.NewMemoryStore())

	// 6 块 → 3 个批次（每批 2 块）。
	text := strings.Repeat("w ", 24)
	_, err := ing.Invoke(context.Background(), ingestReq("doc1", text))
	if err == nil {
		t.Fatal("expected failure at batch 2")
	}
	calls := emb.calls.Load()
	emb.failAt.Store(-1)

	stats, err := ing.Invoke(context.Background(), ingestReq("doc1", text))
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	if stats.Upserted != 6 {
		t.Fatalf("upserted = %d, want 6", stats.Upserted)
	}
	// 恢复的跑只应重做批次 2、3（第 1 批成员终态复用）。
	resumed := emb.calls.Load() - calls
	if resumed < 1 || resumed > 2 {
		t.Fatalf("resumed embed calls = %d, want 1-2 (batch 2-3)", resumed)
	}
	if len(store.records) != 6 {
		t.Fatalf("records = %d, want 6", len(store.records))
	}
}

// 空文档：无分块，直接结束。
func TestIngestorEmptyDocument(t *testing.T) {
	store := newFakeVectorStore()
	emb := &countEmbedder{}
	emb.failAt.Store(-1)
	ing := newTestIngestor(t, store, emb, state.NewMemoryStore())

	stats, err := ing.Invoke(context.Background(), ingestReq("doc1", "   "))
	if err != nil {
		t.Fatal(err)
	}
	if stats.Chunks != 0 || emb.calls.Load() != 0 {
		t.Fatalf("stats = %+v, embed calls = %d", stats, emb.calls.Load())
	}
}

// 无检查点配置也可运行（不可恢复）。
func TestIngestorWithoutCheckpoints(t *testing.T) {
	store := newFakeVectorStore()
	emb := &countEmbedder{}
	emb.failAt.Store(-1)
	ing := newTestIngestor(t, store, emb, nil)

	stats, err := ing.Invoke(context.Background(), ingestReq("doc1", "a b"))
	if err != nil {
		t.Fatal(err)
	}
	if stats.Upserted != 1 {
		t.Fatalf("stats = %+v", stats)
	}
}

// 缺 ID / Body 被拒。
func TestIngestorValidation(t *testing.T) {
	ing := newTestIngestor(t, newFakeVectorStore(), &countEmbedder{}, nil)
	if _, err := ing.Invoke(context.Background(), retrieval.IngestRequest{Source: retrieval.Source{Body: strings.NewReader("x")}}); err == nil {
		t.Fatal("missing ID should fail")
	}
	if _, err := ing.Invoke(context.Background(), retrieval.IngestRequest{ID: "d"}); err == nil {
		t.Fatal("missing body should fail")
	}
}
