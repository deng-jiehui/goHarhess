package eval

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"math"
	"sort"
	"strings"
	"sync"

	"github.com/goharness/goharness/extensions/retrieval"
	"github.com/goharness/goharness/pkg/runnable"
)

// HashEmbedder 是确定性假嵌入器：每个词经 SHA-256 投影到 dim 个桶
// 上的 ±1（SimHash 风格），共享词越多的文本余弦越相似。
// 只用于离线评测回归，不代表真实语义检索质量。
type HashEmbedder struct {
	Dim int
}

func (h HashEmbedder) Invoke(_ context.Context, req retrieval.EmbeddingRequest, _ ...runnable.Option) (retrieval.EmbeddingResponse, error) {
	dim := h.Dim
	if dim <= 0 {
		dim = 64
	}
	vectors := make([][]float32, len(req.Inputs))
	for i, text := range req.Inputs {
		acc := make([]float64, dim)
		for _, term := range strings.Fields(strings.ToLower(text)) {
			sum := sha256.Sum256([]byte(term))
			bucket := int(binary.BigEndian.Uint32(sum[:4]) % uint32(dim))
			sign := 1.0
			if sum[4]&1 == 0 {
				sign = -1.0
			}
			acc[bucket] += sign
		}
		// L2 归一化，余弦即点积。
		var norm float64
		for _, v := range acc {
			norm += v * v
		}
		vec := make([]float32, dim)
		if norm > 0 {
			for i := range acc {
				vec[i] = float32(acc[i] / math.Sqrt(norm))
			}
		}
		vectors[i] = vec
	}
	return retrieval.EmbeddingResponse{Vectors: vectors}, nil
}

func (h HashEmbedder) Stream(ctx context.Context, in retrieval.EmbeddingRequest, o ...runnable.Option) (*runnable.StreamReader[retrieval.EmbeddingResponse], error) {
	return runnable.StreamFromInvoke(ctx, in, h.Invoke, o...)
}
func (h HashEmbedder) Collect(ctx context.Context, in *runnable.StreamReader[retrieval.EmbeddingRequest], o ...runnable.Option) (retrieval.EmbeddingResponse, error) {
	return runnable.CollectFromInvoke(ctx, in, h.Invoke, o...)
}
func (h HashEmbedder) Transform(ctx context.Context, in *runnable.StreamReader[retrieval.EmbeddingRequest], o ...runnable.Option) (*runnable.StreamReader[retrieval.EmbeddingResponse], error) {
	return runnable.TransformFromInvoke(ctx, in, h.Invoke, o...)
}

// MemoryVectorStore 是评测专用内存向量存储：余弦相似度排序，
// 支持命名空间与元数据精确过滤；不做持久化。
type MemoryVectorStore struct {
	mu      sync.Mutex
	records []retrieval.VectorRecord
}

func (m *MemoryVectorStore) Invoke(_ context.Context, req retrieval.VectorRequest, _ ...runnable.Option) (retrieval.VectorResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	switch req.Operation {
	case retrieval.VectorUpsertOperation:
		for _, rec := range req.Records {
			replaced := false
			for i := range m.records {
				if m.records[i].ID == rec.ID && m.records[i].Namespace == rec.Namespace {
					m.records[i] = rec
					replaced = true
					break
				}
			}
			if !replaced {
				m.records = append(m.records, rec)
			}
		}
	case retrieval.VectorQueryOperation:
		return retrieval.VectorResponse{Hits: m.query(req.Query)}, nil
	case retrieval.VectorDeleteOperation:
		var keep []retrieval.VectorRecord
		drop := map[string]bool{}
		for _, id := range req.IDs {
			drop[id] = true
		}
		for _, rec := range m.records {
			if rec.Namespace == req.Namespace && (len(req.IDs) == 0 || drop[rec.ID]) {
				continue
			}
			keep = append(keep, rec)
		}
		m.records = keep
	case retrieval.VectorListOperation:
		var hits []retrieval.SearchHit
		for _, rec := range m.records {
			if rec.Namespace != req.Namespace || !matchFilter(rec, req.Filter) {
				continue
			}
			hits = append(hits, retrieval.SearchHit{ID: rec.ID, Text: rec.Text, Metadata: rec.Metadata})
		}
		return retrieval.VectorResponse{Hits: hits}, nil
	}
	return retrieval.VectorResponse{}, nil
}

func (m *MemoryVectorStore) Stream(ctx context.Context, in retrieval.VectorRequest, o ...runnable.Option) (*runnable.StreamReader[retrieval.VectorResponse], error) {
	return runnable.StreamFromInvoke(ctx, in, m.Invoke, o...)
}
func (m *MemoryVectorStore) Collect(ctx context.Context, in *runnable.StreamReader[retrieval.VectorRequest], o ...runnable.Option) (retrieval.VectorResponse, error) {
	return runnable.CollectFromInvoke(ctx, in, m.Invoke, o...)
}
func (m *MemoryVectorStore) Transform(ctx context.Context, in *runnable.StreamReader[retrieval.VectorRequest], o ...runnable.Option) (*runnable.StreamReader[retrieval.VectorResponse], error) {
	return runnable.TransformFromInvoke(ctx, in, m.Invoke, o...)
}

func matchFilter(rec retrieval.VectorRecord, filter retrieval.Metadata) bool {
	for key, want := range filter {
		got, ok := rec.Metadata[key]
		if !ok || got != want {
			return false
		}
	}
	return true
}

func (m *MemoryVectorStore) query(q *retrieval.VectorQuery) []retrieval.SearchHit {
	type scored struct {
		rec   retrieval.VectorRecord
		score float64
	}
	var candidates []scored
	for _, rec := range m.records {
		if rec.Namespace != q.Namespace || !matchFilter(rec, q.Filter) {
			continue
		}
		var dot, na, nb float64
		for i := range rec.Vector {
			dot += float64(rec.Vector[i]) * float64(q.Vector[i])
			na += float64(rec.Vector[i]) * float64(rec.Vector[i])
			nb += float64(q.Vector[i]) * float64(q.Vector[i])
		}
		score := 0.0
		if na > 0 && nb > 0 {
			score = dot / (math.Sqrt(na) * math.Sqrt(nb))
		}
		if q.MinScore > 0 && score < float64(q.MinScore) {
			continue
		}
		candidates = append(candidates, scored{rec, score})
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].score != candidates[j].score {
			return candidates[i].score > candidates[j].score
		}
		return candidates[i].rec.ID < candidates[j].rec.ID
	})
	k := q.TopK
	if k <= 0 {
		k = 10
	}
	hits := make([]retrieval.SearchHit, 0, k)
	for i, c := range candidates {
		if i >= k {
			break
		}
		hits = append(hits, retrieval.SearchHit{
			ID: c.rec.ID, Score: c.score, Text: c.rec.Text,
			Vector: c.rec.Vector, Metadata: c.rec.Metadata, Source: "memory",
		})
	}
	return hits
}

// MemoryKeywordSearch 是评测专用关键词检索：词项重叠数打分（无 IDF），
// 确定性与哈希嵌入器一致。
type MemoryKeywordSearch struct {
	Store *MemoryVectorStore
}

func (k MemoryKeywordSearch) Invoke(_ context.Context, req retrieval.TextSearchRequest, _ ...runnable.Option) ([]retrieval.SearchHit, error) {
	k.Store.mu.Lock()
	defer k.Store.mu.Unlock()
	terms := map[string]bool{}
	for _, term := range strings.Fields(strings.ToLower(req.Query)) {
		terms[term] = true
	}
	ns := ""
	if req.Filters != nil {
		ns, _ = req.Filters["_ns"].(string)
	}
	type scored struct {
		rec   retrieval.VectorRecord
		score float64
	}
	var candidates []scored
	for _, rec := range k.Store.records {
		if rec.Namespace != ns {
			continue
		}
		match := 0
		for _, term := range strings.Fields(strings.ToLower(rec.Text)) {
			if terms[term] {
				match++
			}
		}
		if match > 0 {
			candidates = append(candidates, scored{rec, float64(match)})
		}
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].score != candidates[j].score {
			return candidates[i].score > candidates[j].score
		}
		return candidates[i].rec.ID < candidates[j].rec.ID
	})
	limit := req.TopK
	if limit <= 0 {
		limit = 10
	}
	hits := make([]retrieval.SearchHit, 0, limit)
	for i, c := range candidates {
		if i >= limit {
			break
		}
		hits = append(hits, retrieval.SearchHit{ID: c.rec.ID, Score: c.score, Text: c.rec.Text, Source: "memory-keyword"})
	}
	return hits, nil
}

func (k MemoryKeywordSearch) Stream(ctx context.Context, in retrieval.TextSearchRequest, o ...runnable.Option) (*runnable.StreamReader[[]retrieval.SearchHit], error) {
	return runnable.StreamFromInvoke(ctx, in, k.Invoke, o...)
}
func (k MemoryKeywordSearch) Collect(ctx context.Context, in *runnable.StreamReader[retrieval.TextSearchRequest], o ...runnable.Option) ([]retrieval.SearchHit, error) {
	return runnable.CollectFromInvoke(ctx, in, k.Invoke, o...)
}
func (k MemoryKeywordSearch) Transform(ctx context.Context, in *runnable.StreamReader[retrieval.TextSearchRequest], o ...runnable.Option) (*runnable.StreamReader[[]retrieval.SearchHit], error) {
	return runnable.TransformFromInvoke(ctx, in, k.Invoke, o...)
}
