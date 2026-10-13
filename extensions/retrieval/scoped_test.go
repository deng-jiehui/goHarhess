package retrieval_test

import (
	"context"
	"errors"
	"testing"

	retrievalcache "github.com/goharness/goharness/extensions/cache/retrieval"
	"github.com/goharness/goharness/extensions/retrieval"
	"github.com/goharness/goharness/extensions/tenant"
	"github.com/goharness/goharness/pkg/runnable"
	"github.com/goharness/goharness/pkg/tool"
)

// spyStore 记录收到的请求并回送固定命中（命中里带命名空间以便断言）。
type spyStore struct {
	last retrieval.VectorRequest
}

func (s *spyStore) Invoke(_ context.Context, req retrieval.VectorRequest, _ ...runnable.Option) (retrieval.VectorResponse, error) {
	s.last = req
	if req.Operation == retrieval.VectorQueryOperation {
		return retrieval.VectorResponse{Hits: []retrieval.SearchHit{
			{ID: "h1", Score: 0.1, Text: req.Query.Namespace + "/doc"},
		}}, nil
	}
	return retrieval.VectorResponse{}, nil
}

func (s *spyStore) Stream(ctx context.Context, in retrieval.VectorRequest, o ...runnable.Option) (*runnable.StreamReader[retrieval.VectorResponse], error) {
	return runnable.StreamFromInvoke(ctx, in, s.Invoke, o...)
}

func (s *spyStore) Collect(ctx context.Context, in *runnable.StreamReader[retrieval.VectorRequest], o ...runnable.Option) (retrieval.VectorResponse, error) {
	return runnable.CollectFromInvoke(ctx, in, s.Invoke, o...)
}

func (s *spyStore) Transform(ctx context.Context, in *runnable.StreamReader[retrieval.VectorRequest], o ...runnable.Option) (*runnable.StreamReader[retrieval.VectorResponse], error) {
	return runnable.TransformFromInvoke(ctx, in, s.Invoke, o...)
}

func scopedCtx(t *testing.T, tenantID string) context.Context {
	t.Helper()
	ctx, err := tenant.With(context.Background(), tool.Subject{TenantID: tenantID, ID: "u1"})
	if err != nil {
		t.Fatal(err)
	}
	return ctx
}

func TestScopedForcesTenantNamespace(t *testing.T) {
	spy := &spyStore{}
	s := retrieval.Scoped(spy)
	ctx := scopedCtx(t, "ta")
	_, err := s.Invoke(ctx, retrieval.VectorRequest{
		Operation: retrieval.VectorQueryOperation,
		Query:     &retrieval.VectorQuery{Vector: []float32{1}, TopK: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	if spy.last.Query.Namespace != "ta" {
		t.Fatalf("namespace = %q", spy.last.Query.Namespace)
	}
}

func TestScopedRejectsForeignNamespace(t *testing.T) {
	s := retrieval.Scoped(&spyStore{})
	ctx := scopedCtx(t, "ta")
	_, err := s.Invoke(ctx, retrieval.VectorRequest{
		Operation: retrieval.VectorQueryOperation,
		Query:     &retrieval.VectorQuery{Namespace: "tb", Vector: []float32{1}},
	})
	if !errors.Is(err, tenant.ErrTenantMismatch) {
		t.Fatalf("err = %v", err)
	}
	// upsert 里的他租户记录同样被拒。
	_, err = s.Invoke(ctx, retrieval.VectorRequest{
		Operation: retrieval.VectorUpsertOperation,
		Records:   []retrieval.VectorRecord{{ID: "x", Namespace: "tb", Vector: []float32{1}}},
	})
	if !errors.Is(err, tenant.ErrTenantMismatch) {
		t.Fatalf("upsert err = %v", err)
	}
	// 无作用域拒绝。
	_, err = s.Invoke(context.Background(), retrieval.VectorRequest{
		Operation: retrieval.VectorQueryOperation,
		Query:     &retrieval.VectorQuery{Vector: []float32{1}},
	})
	if !errors.Is(err, tenant.ErrNoScope) {
		t.Fatalf("no-scope err = %v", err)
	}
}

// 缓存路径与存储路径同租户：A 的 Scoped 调用产生的缓存键与 B 的不同，
// B 不可能命中 A 的缓存。
func TestScopedUnifiesCacheTenant(t *testing.T) {
	spy := &spyStore{}
	mem, err := retrievalcache.NewMemoryVectorCache(retrievalcache.MemoryVectorCacheConfig{Capacity: 100})
	if err != nil {
		t.Fatal(err)
	}
	cached, err := retrievalcache.NewCachedVectorStore(spy, retrievalcache.VectorCacheConfig{
		Store: mem, StoreID: "documents",
	})
	if err != nil {
		t.Fatal(err)
	}
	s := retrieval.Scoped(cached)
	// A 连续两次相同查询：第二次应命中缓存（spy 计数不变）。
	ctxA := scopedCtx(t, "ta")
	q := func() retrieval.VectorRequest {
		return retrieval.VectorRequest{
			Operation: retrieval.VectorQueryOperation,
			Query:     &retrieval.VectorQuery{Vector: []float32{1}, TopK: 1},
		}
	}
	if _, err := s.Invoke(ctxA, q()); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Invoke(ctxA, q()); err != nil {
		t.Fatal(err)
	}
	hitsB, err := s.Invoke(scopedCtx(t, "tb"), q())
	if err != nil {
		t.Fatal(err)
	}
	// B 的同形查询必须真的到达了底层存储（而不是命中 A 的缓存），
	// 且底层看到的命名空间是 tb。
	if spy.last.Query.Namespace != "tb" {
		t.Fatalf("namespace leaked or cache hit wrong tenant: %q", spy.last.Query.Namespace)
	}
	if len(hitsB.Hits) != 1 || hitsB.Hits[0].Text != "tb/doc" {
		t.Fatalf("hits = %+v", hitsB.Hits)
	}
}
