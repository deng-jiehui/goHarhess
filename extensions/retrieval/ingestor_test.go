package retrieval_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

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

func newTestIngestor(t *testing.T, store *fakeVectorStore, emb runnable.Runnable[retrieval.EmbeddingRequest, retrieval.EmbeddingResponse], checkpoints state.Store, mutate ...func(*retrieval.IngestorConfig)) *retrieval.Ingestor {
	t.Helper()
	chunker, err := retrieval.NewTextChunker(retrieval.ChunkConfig{MaxTokens: 4, DisableOverlap: true}, nil)
	if err != nil {
		t.Fatal(err)
	}
	cfg := retrieval.IngestorConfig{
		Loader:             loader.Text(),
		Chunker:            runnable.Func[retrieval.Document, []retrieval.Chunk](chunker.Invoke),
		Embedder:           emb,
		Store:              store,
		Namespace:          "ns-test",
		EmbeddingBatchSize: 2,
		Checkpoints:        checkpoints,
	}
	for _, fn := range mutate {
		fn(&cfg)
	}
	ing, err := retrieval.NewIngestor(cfg)
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

// 中途失败恢复（确定性）：MaxConcurrency=1 使批次按序执行，第 1 批
// 完成并落记录后第 2 次嵌入调用失败，第 3 批不会启动；恢复只重做
// 批次 2、3。
func TestIngestorResumeSkipsDoneBatches(t *testing.T) {
	store := newFakeVectorStore()
	emb := &countEmbedder{}
	emb.failAt.Store(2) // 第 2 次嵌入调用失败
	ing := newTestIngestor(t, store, emb, state.NewMemoryStore(), func(cfg *retrieval.IngestorConfig) {
		cfg.MaxConcurrency = 1
	})

	// 6 块 → 3 个批次（每批 2 块）。
	text := strings.Repeat("w ", 24)
	_, err := ing.Invoke(context.Background(), ingestReq("doc1", text))
	if err == nil {
		t.Fatal("expected failure at batch 2")
	}
	if calls := emb.calls.Load(); calls != 2 {
		t.Fatalf("first-run embed calls = %d, want exactly 2 (batch 3 must not start)", calls)
	}
	emb.failAt.Store(-1)

	stats, err := ing.Invoke(context.Background(), ingestReq("doc1", text))
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	if stats.Upserted != 6 {
		t.Fatalf("upserted = %d, want 6", stats.Upserted)
	}
	// 恢复的跑恰好重做批次 2、3（第 1 批成员终态复用）。
	if resumed := emb.calls.Load(); resumed != 4 {
		t.Fatalf("resumed embed calls = %d total, want 4 (2 first-run + 2 resumed)", resumed)
	}
	if len(store.records) != 6 {
		t.Fatalf("records = %d, want 6", len(store.records))
	}
}

// 并发失败恢复：默认并行度下第 2 次嵌入调用失败，失败瞬间仍在执行的
// 兄弟批次会被取消并在恢复时重做（至少一次语义）；恢复结果必须完整
// 且无重复，恢复期间的嵌入调用数只断言上界——这是并发语义的真实契约。
func TestIngestorResumeAfterConcurrentFailureIsComplete(t *testing.T) {
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
	resumed := emb.calls.Load() - calls
	if resumed < 1 || resumed > 3 {
		t.Fatalf("resumed embed calls = %d, want 1-3 (canceled siblings rerun)", resumed)
	}
	if len(store.records) != 6 {
		t.Fatalf("records = %d, want 6 (complete, no duplicates)", len(store.records))
	}
}

// barrierEmbedder 记录在途嵌入调用数峰值；barrierN>0 时调用等待在途
// 数达到 barrierN 才返回——达到后屏障自行撤除（批次尾部可能没有可
// 配对的兄弟调用，持续屏障会死锁）。超时即证明并行度被错误限低。
type barrierEmbedder struct {
	inflight atomic.Int32
	peak     atomic.Int32
	barrierN atomic.Int32
}

func (e *barrierEmbedder) Invoke(ctx context.Context, req retrieval.EmbeddingRequest, _ ...runnable.Option) (retrieval.EmbeddingResponse, error) {
	n := e.inflight.Add(1)
	defer e.inflight.Add(-1)
	for {
		if p := e.peak.Load(); n <= p || e.peak.CompareAndSwap(p, n) {
			break
		}
	}
	if want := e.barrierN.Load(); want > 0 {
		if n >= want {
			e.barrierN.Store(0) // 已证明可达限额：撤除屏障放行尾部
		} else {
			deadline := time.Now().Add(10 * time.Second)
			// 屏障撤除（barrierN→0）后条件为假，等待方自然放行。
			for e.inflight.Load() < e.barrierN.Load() {
				if time.Now().After(deadline) {
					return retrieval.EmbeddingResponse{}, errors.New("embed barrier timeout: calls serialized")
				}
				select {
				case <-ctx.Done():
					return retrieval.EmbeddingResponse{}, ctx.Err()
				case <-time.After(time.Millisecond):
				}
			}
		}
	}
	vectors := make([][]float32, len(req.Inputs))
	for i := range vectors {
		vectors[i] = []float32{1, float32(i)}
	}
	return retrieval.EmbeddingResponse{Vectors: vectors}, nil
}

func (e *barrierEmbedder) Stream(ctx context.Context, in retrieval.EmbeddingRequest, o ...runnable.Option) (*runnable.StreamReader[retrieval.EmbeddingResponse], error) {
	return runnable.StreamFromInvoke(ctx, in, e.Invoke, o...)
}
func (e *barrierEmbedder) Collect(ctx context.Context, in *runnable.StreamReader[retrieval.EmbeddingRequest], o ...runnable.Option) (retrieval.EmbeddingResponse, error) {
	return runnable.CollectFromInvoke(ctx, in, e.Invoke, o...)
}
func (e *barrierEmbedder) Transform(ctx context.Context, in *runnable.StreamReader[retrieval.EmbeddingRequest], o ...runnable.Option) (*runnable.StreamReader[retrieval.EmbeddingResponse], error) {
	return runnable.TransformFromInvoke(ctx, in, e.Invoke, o...)
}

// MaxConcurrency 限制同时执行的嵌入批次：限额 2 且屏障为 2 时峰值
// 必须恰好达到 2（否则屏障超时）；默认配置峰值不超过默认值 4；
// 负数返回配置错误。
func TestIngestorMaxConcurrency(t *testing.T) {
	// 限额 2：屏障要求两个调用同时在途，若被限成 1 则超时失败。
	emb := &barrierEmbedder{}
	emb.barrierN.Store(2)
	ing := newTestIngestor(t, newFakeVectorStore(), emb, nil, func(cfg *retrieval.IngestorConfig) {
		cfg.MaxConcurrency = 2
	})
	many := strings.Repeat("w ", 48) // 12 块 → 6 个批次
	stats, err := ing.Invoke(context.Background(), ingestReq("doc-many", many))
	if err != nil {
		t.Fatalf("ingest with limit 2: %v", err)
	}
	if stats.EmbeddingBatches != 6 {
		t.Fatalf("batches = %d, want 6", stats.EmbeddingBatches)
	}
	if peak := emb.peak.Load(); peak > 2 {
		t.Fatalf("inflight peak = %d, want <= 2", peak)
	}

	// 默认限额 4：10 个批次无屏障，峰值不超过 4。
	emb2 := &barrierEmbedder{}
	ing2 := newTestIngestor(t, newFakeVectorStore(), emb2, nil)
	long := strings.Repeat("w ", 80) // 20 块 → 10 个批次
	stats2, err := ing2.Invoke(context.Background(), ingestReq("doc-long", long))
	if err != nil {
		t.Fatalf("ingest with default limit: %v", err)
	}
	if stats2.EmbeddingBatches != 10 {
		t.Fatalf("batches = %d, want 10", stats2.EmbeddingBatches)
	}
	if peak := emb2.peak.Load(); peak > 4 {
		t.Fatalf("inflight peak = %d, want <= 4", peak)
	}

	// 负数返回配置错误。
	chunker, err := retrieval.NewTextChunker(retrieval.ChunkConfig{MaxTokens: 4, DisableOverlap: true}, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = retrieval.NewIngestor(retrieval.IngestorConfig{
		Loader:             loader.Text(),
		Chunker:            runnable.Func[retrieval.Document, []retrieval.Chunk](chunker.Invoke),
		Embedder:           emb2,
		Store:              newFakeVectorStore(),
		EmbeddingBatchSize: 2,
		MaxConcurrency:     -1,
	})
	if err == nil {
		t.Fatal("negative MaxConcurrency should be a config error")
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
