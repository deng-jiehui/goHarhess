package redisstore_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/goharness/goharness/extensions/retrieval"
	"github.com/goharness/goharness/extensions/retrieval/redisstore"
)

// 集成测试只在 GOHARNESS_TEST_REDIS_STACK_ADDR 指向真实 Redis Stack /
// Redis 8（带 Search 模块）时运行。
func stackClient(t *testing.T) *redis.Client {
	t.Helper()
	addr := os.Getenv("GOHARNESS_TEST_REDIS_STACK_ADDR")
	if addr == "" {
		t.Skip("GOHARNESS_TEST_REDIS_STACK_ADDR not set")
	}
	// go-redis v9 默认 RESP3，FT.SEARCH 的 RESP3 应答在库里尚属不稳定
	// 特性；适配器要求 RESP2 客户端（Options.Protocol: 2）。
	return redis.NewClient(&redis.Options{Addr: addr, Protocol: 2})
}

func newStore(t *testing.T, c redis.UniversalClient, name string) *redisstore.Store {
	t.Helper()
	s, err := redisstore.New(redisstore.Config{
		Client: c, Index: name, Prefix: "it:" + name + ":", Dim: 4,
		Distance: redisstore.Cosine,
		Metadata: []redisstore.MetadataField{
			{Name: "kind", Type: redisstore.FieldTag},
			{Name: "year", Type: redisstore.FieldNumeric},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func vec(v float32) []float32 { return []float32{v, 0, 0, 0} }

func TestRedisStackVectorRoundTrip(t *testing.T) {
	c := stackClient(t)
	ctx := context.Background()
	idx := "it_vec_" + time.Now().Format("150405.000000")
	s := newStore(t, c, idx)
	defer c.Do(ctx, "FT.DROPINDEX", idx).Err()
	if err := s.EnsureIndex(ctx); err != nil {
		t.Fatal(err)
	}
	// 幂等：第二次不报错。
	if err := s.EnsureIndex(ctx); err != nil {
		t.Fatal(err)
	}
	vs := s.VectorStore()
	_, err := vs.Invoke(ctx, retrieval.VectorRequest{
		Operation: retrieval.VectorUpsertOperation,
		Records: []retrieval.VectorRecord{
			{ID: "a1", Namespace: "ta", Text: "alpha doc", Vector: vec(1), Metadata: retrieval.Metadata{"kind": "doc", "year": 2024, "free": "x"}},
			{ID: "a2", Namespace: "ta", Text: "beta doc", Vector: vec(0), Metadata: retrieval.Metadata{"kind": "doc"}},
			{ID: "b1", Namespace: "tb", Text: "alpha too", Vector: vec(1)},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(300 * time.Millisecond) // 等索引异步建立

	// 向量检索：租户 A 只能看到 A 的文档。
	resp, err := vs.Invoke(ctx, retrieval.VectorRequest{
		Operation: retrieval.VectorQueryOperation,
		Query:     &retrieval.VectorQuery{Namespace: "ta", Vector: vec(1), TopK: 5},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Hits) == 0 || resp.Hits[0].ID != "a1" {
		t.Fatalf("hits = %+v", resp.Hits)
	}
	for _, h := range resp.Hits {
		if h.Metadata["_ns"] != "ta" {
			t.Fatalf("cross-tenant hit: %+v", h)
		}
	}
	// 元数据过滤。
	resp, err = vs.Invoke(ctx, retrieval.VectorRequest{
		Operation: retrieval.VectorQueryOperation,
		Query: &retrieval.VectorQuery{Namespace: "ta", Vector: vec(1), TopK: 5,
			Filter: retrieval.Metadata{"year": 2024}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Hits) != 1 || resp.Hits[0].ID != "a1" {
		t.Fatalf("filtered hits = %+v", resp.Hits)
	}
	// 未声明字段过滤报错。
	_, err = vs.Invoke(ctx, retrieval.VectorRequest{
		Operation: retrieval.VectorQueryOperation,
		Query:     &retrieval.VectorQuery{Namespace: "ta", Vector: vec(1), TopK: 1, Filter: retrieval.Metadata{"nope": "x"}},
	})
	if err == nil {
		t.Fatal("undeclared filter must fail")
	}
	// 维度不一致报错。
	_, err = vs.Invoke(ctx, retrieval.VectorRequest{
		Operation: retrieval.VectorUpsertOperation,
		Records:   []retrieval.VectorRecord{{ID: "bad", Namespace: "ta", Text: "x", Vector: []float32{1, 2}}},
	})
	if err == nil {
		t.Fatal("dim mismatch must fail")
	}
}

func TestRedisStackKeywordAndDelete(t *testing.T) {
	c := stackClient(t)
	ctx := context.Background()
	idx := "it_kw_" + time.Now().Format("150405.000000")
	s := newStore(t, c, idx)
	defer c.Do(ctx, "FT.DROPINDEX", idx).Err()
	if err := s.EnsureIndex(ctx); err != nil {
		t.Fatal(err)
	}
	vs := s.VectorStore()
	_, err := vs.Invoke(ctx, retrieval.VectorRequest{
		Operation: retrieval.VectorUpsertOperation,
		Records: []retrieval.VectorRecord{
			{ID: "k1", Namespace: "ta", Text: "the quick brown fox", Vector: vec(1)},
			{ID: "k2", Namespace: "tb", Text: "quick fox", Vector: vec(1)},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(300 * time.Millisecond)
	ks := s.KeywordSearch()
	hits, err := ks.Invoke(ctx, retrieval.TextSearchRequest{
		Query:   "quick",
		TopK:    5,
		Filters: retrieval.Metadata{"_ns": "ta"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(hits) != 1 || hits[0].ID != "k1" {
		t.Fatalf("hits = %+v", hits)
	}
	// 注入式查询文本不报错也不泄露。
	if _, err := ks.Invoke(ctx, retrieval.TextSearchRequest{
		Query:   `x" (foo) | -bar @ns:{tb}`,
		Filters: retrieval.Metadata{"_ns": "ta"},
	}); err != nil {
		t.Fatal(err)
	}
	// 按 ID 删除。
	if _, err := vs.Invoke(ctx, retrieval.VectorRequest{
		Operation: retrieval.VectorDeleteOperation, Namespace: "ta", IDs: []string{"k1"},
	}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(200 * time.Millisecond)
	hits, _ = ks.Invoke(ctx, retrieval.TextSearchRequest{Query: "quick", Filters: retrieval.Metadata{"_ns": "ta"}})
	if len(hits) != 0 {
		t.Fatal("deleted doc still found")
	}
	// 整命名空间删除。
	if _, err := vs.Invoke(ctx, retrieval.VectorRequest{
		Operation: retrieval.VectorDeleteOperation, Namespace: "tb",
	}); err != nil {
		t.Fatal(err)
	}
}

// list 操作：命名空间+元数据过滤的纯分页列举，用于入库清理旧版本。
func TestRedisStackList(t *testing.T) {
	c := stackClient(t)
	ctx := context.Background()
	idx := "it_ls_" + time.Now().Format("150405.000000")
	s, err := redisstore.New(redisstore.Config{
		Client: c, Index: idx, Prefix: "it:" + idx + ":", Dim: 4,
		Distance: redisstore.Cosine,
		Metadata: []redisstore.MetadataField{
			{Name: "_rag_document_id", Type: redisstore.FieldTag},
			{Name: "_rag_version", Type: redisstore.FieldTag},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Do(ctx, "FT.DROPINDEX", idx).Err()
	if err := s.EnsureIndex(ctx); err != nil {
		t.Fatal(err)
	}
	vs := s.VectorStore()
	_, err = vs.Invoke(ctx, retrieval.VectorRequest{
		Operation: retrieval.VectorUpsertOperation,
		Records: []retrieval.VectorRecord{
			{ID: "d1", Namespace: "ta", Text: "a", Vector: vec(1), Metadata: retrieval.Metadata{"_rag_document_id": "doc", "_rag_version": "v1"}},
			{ID: "d2", Namespace: "ta", Text: "b", Vector: vec(1), Metadata: retrieval.Metadata{"_rag_document_id": "doc", "_rag_version": "v1"}},
			{ID: "d3", Namespace: "ta", Text: "c", Vector: vec(1), Metadata: retrieval.Metadata{"_rag_document_id": "doc", "_rag_version": "v2"}},
			{ID: "x1", Namespace: "tb", Text: "d", Vector: vec(1), Metadata: retrieval.Metadata{"_rag_document_id": "doc", "_rag_version": "v1"}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(300 * time.Millisecond)

	resp, err := vs.Invoke(ctx, retrieval.VectorRequest{
		Operation: retrieval.VectorListOperation,
		Namespace: "ta",
		Filter:    retrieval.Metadata{"_rag_document_id": "doc"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Hits) != 3 {
		t.Fatalf("list hits = %d, want 3 (tb excluded)", len(resp.Hits))
	}
	// 按版本过滤出旧块并删除——入库清理的两步后半。
	var stale []string
	for _, h := range resp.Hits {
		if h.Metadata["_rag_version"] != "v2" {
			stale = append(stale, h.ID)
		}
	}
	if len(stale) != 2 {
		t.Fatalf("stale = %v", stale)
	}
	if _, err := vs.Invoke(ctx, retrieval.VectorRequest{
		Operation: retrieval.VectorDeleteOperation, Namespace: "ta", IDs: stale,
	}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(200 * time.Millisecond)
	resp, err = vs.Invoke(ctx, retrieval.VectorRequest{
		Operation: retrieval.VectorListOperation,
		Namespace: "ta",
		Filter:    retrieval.Metadata{"_rag_document_id": "doc"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Hits) != 1 || resp.Hits[0].ID != "d3" {
		t.Fatalf("after cleanup hits = %+v", resp.Hits)
	}
	// 空命名空间拒绝。
	_, err = vs.Invoke(ctx, retrieval.VectorRequest{
		Operation: retrieval.VectorListOperation, Filter: retrieval.Metadata{"_rag_document_id": "doc"},
	})
	if err == nil {
		t.Fatal("empty namespace must fail")
	}
}

func TestRedisStackEnsureIndexMismatch(t *testing.T) {
	c := stackClient(t)
	ctx := context.Background()
	idx := "it_dim_" + time.Now().Format("150405.000000")
	s := newStore(t, c, idx)
	defer c.Do(ctx, "FT.DROPINDEX", idx).Err()
	if err := s.EnsureIndex(ctx); err != nil {
		t.Fatal(err)
	}
	other, err := redisstore.New(redisstore.Config{Client: c, Index: idx, Prefix: "it:" + idx + ":", Dim: 8, Distance: redisstore.Cosine})
	if err != nil {
		t.Fatal(err)
	}
	if err := other.EnsureIndex(ctx); err == nil {
		t.Fatal("dim mismatch must fail")
	}
}
