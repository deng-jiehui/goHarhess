package retrieval_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	retrievalcache "goharness/extensions/cache/retrieval"
	"goharness/pkg/retrieval"
	"goharness/pkg/runnable"
)

func TestCachedVectorStoreCachesExactQueryAndClonesHits(t *testing.T) {
	upstream := &recordingVectorStore{hits: []retrieval.SearchHit{{
		ID:     "doc-1",
		Score:  0.9,
		Text:   "original",
		Vector: []float32{1, 2},
		Metadata: retrieval.Metadata{
			"nested": map[string]any{"value": "original"},
		},
	}}}
	cached := newCachedVectorStore(t, upstream, time.Minute)
	query := retrieval.VectorQuery{
		Vector: []float32{0.1, 0.2}, TopK: 3,
		Filter:  retrieval.Metadata{"b": 2, "a": 1},
		Include: retrieval.IncludeOptions{Text: true, Metadata: true},
	}
	first := mustVectorQuery(t, cached, context.Background(), query)
	first[0].Text = "changed"
	first[0].Vector[0] = 99
	first[0].Metadata["nested"].(map[string]any)["value"] = "changed"

	// JSON key generation is canonical for map key order.
	query.Filter = retrieval.Metadata{"a": 1, "b": 2}
	second := mustVectorQuery(t, cached, context.Background(), query)
	if upstream.Calls() != 1 {
		t.Fatalf("upstream calls = %d, want 1", upstream.Calls())
	}
	if second[0].Text != "original" || second[0].Vector[0] != 1 {
		t.Fatalf("cached hit was mutated: %#v", second[0])
	}
	nested := second[0].Metadata["nested"].(map[string]any)
	if nested["value"] != "original" {
		t.Fatalf("nested metadata = %#v", nested)
	}
	stats := cached.Stats()
	if stats.Hits != 1 || stats.Misses != 1 || stats.Writes != 1 {
		t.Fatalf("stats = %#v", stats)
	}
}

func TestCachedVectorStoreIsolatesTenantsAndQueryShape(t *testing.T) {
	upstream := &recordingVectorStore{hits: []retrieval.SearchHit{{ID: "doc"}}}
	cached := newCachedVectorStore(t, upstream, time.Minute)
	base := retrieval.VectorQuery{Vector: []float32{1}, TopK: 1, Namespace: "docs"}

	for _, tenant := range []string{"tenant-a", "tenant-b", "tenant-a"} {
		ctx := retrievalcache.WithVectorCacheTenant(context.Background(), tenant)
		mustVectorQuery(t, cached, ctx, base)
	}
	changed := base
	changed.TopK = 2
	mustVectorQuery(t, cached, retrievalcache.WithVectorCacheTenant(context.Background(), "tenant-a"), changed)
	changed = base
	changed.Vector = []float32{2}
	mustVectorQuery(t, cached, retrievalcache.WithVectorCacheTenant(context.Background(), "tenant-a"), changed)
	if upstream.Calls() != 4 {
		t.Fatalf("upstream calls = %d, want 4", upstream.Calls())
	}
}

func TestCachedVectorStoreInvalidatesOnlyMutatedNamespace(t *testing.T) {
	upstream := &recordingVectorStore{dynamic: true}
	cached := newCachedVectorStore(t, upstream, time.Minute)
	queryA := retrieval.VectorQuery{Vector: []float32{1}, TopK: 1, Namespace: "a"}
	queryB := retrieval.VectorQuery{Vector: []float32{1}, TopK: 1, Namespace: "b"}
	mustVectorQuery(t, cached, context.Background(), queryA)
	mustVectorQuery(t, cached, context.Background(), queryB)
	mustVectorQuery(t, cached, context.Background(), queryA)
	mustVectorQuery(t, cached, context.Background(), queryB)

	if _, err := cached.Invoke(context.Background(), retrieval.VectorRequest{
		Operation: retrieval.VectorUpsertOperation,
		Records:   []retrieval.VectorRecord{{ID: "new", Namespace: "a"}},
	}); err != nil {
		t.Fatal(err)
	}
	mustVectorQuery(t, cached, context.Background(), queryA)
	mustVectorQuery(t, cached, context.Background(), queryB)
	if upstream.Calls() != 3 {
		t.Fatalf("upstream calls = %d, want 3", upstream.Calls())
	}
	if cached.Stats().Invalidations != 1 {
		t.Fatalf("stats = %#v", cached.Stats())
	}
}

func TestCachedVectorStoreCombinesConcurrentMisses(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	upstream := &recordingVectorStore{
		hits:    []retrieval.SearchHit{{ID: "shared"}},
		started: started,
		release: release,
	}
	cached := newCachedVectorStore(t, upstream, time.Minute)
	query := retrieval.VectorQuery{Vector: []float32{1, 2, 3}, TopK: 5}

	const callers = 12
	var wait sync.WaitGroup
	wait.Add(callers)
	errors := make(chan error, callers)
	for range callers {
		go func() {
			defer wait.Done()
			response, err := cached.Invoke(context.Background(), vectorQueryRequest(query))
			hits := response.Hits
			if err == nil && (len(hits) != 1 || hits[0].ID != "shared") {
				err = fmt.Errorf("hits = %#v", hits)
			}
			errors <- err
		}()
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("upstream query did not start")
	}
	waitForVectorShared(t, cached, callers-1)
	close(release)
	wait.Wait()
	close(errors)
	for err := range errors {
		if err != nil {
			t.Fatal(err)
		}
	}
	if upstream.Calls() != 1 {
		t.Fatalf("upstream calls = %d, want 1", upstream.Calls())
	}
}

func TestCachedVectorStoreExpiresEntries(t *testing.T) {
	upstream := &recordingVectorStore{hits: []retrieval.SearchHit{{ID: "doc"}}}
	cached := newCachedVectorStore(t, upstream, 15*time.Millisecond)
	query := retrieval.VectorQuery{Vector: []float32{1}, TopK: 1}
	mustVectorQuery(t, cached, context.Background(), query)
	time.Sleep(25 * time.Millisecond)
	mustVectorQuery(t, cached, context.Background(), query)
	if upstream.Calls() != 2 {
		t.Fatalf("upstream calls = %d, want 2", upstream.Calls())
	}
}

func TestCachedVectorStoreBypassDoesNotReadOrWriteCache(t *testing.T) {
	upstream := &recordingVectorStore{hits: []retrieval.SearchHit{{ID: "doc"}}}
	cached := newCachedVectorStore(t, upstream, time.Minute)
	query := retrieval.VectorQuery{Vector: []float32{1}, TopK: 1}
	ctx := retrievalcache.WithVectorCacheBypass(context.Background())
	mustVectorQuery(t, cached, ctx, query)
	mustVectorQuery(t, cached, ctx, query)
	if upstream.Calls() != 2 || cached.Stats().Bypasses != 2 {
		t.Fatalf("calls = %d, stats = %#v", upstream.Calls(), cached.Stats())
	}
}

func TestCachedVectorStoreForwardsRunnableOptions(t *testing.T) {
	upstream := &recordingVectorStore{hits: []retrieval.SearchHit{{ID: "option"}}}
	cached := newCachedVectorStore(t, upstream, time.Minute)
	option := runnable.OptionFunc(func(set *runnable.OptionSet) { set.Values["provider"] = "fast" })
	if _, err := cached.Invoke(context.Background(), vectorQueryRequest(retrieval.VectorQuery{Vector: []float32{1}, TopK: 1}), option); err != nil {
		t.Fatal(err)
	}
	if !upstream.OptionsSeen() {
		t.Fatal("vector cache dropped downstream runnable options")
	}
}

func TestMemoryVectorCacheUsesLRUEviction(t *testing.T) {
	store, err := retrievalcache.NewMemoryVectorCache(retrievalcache.MemoryVectorCacheConfig{Capacity: 2})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	set := func(key string) {
		t.Helper()
		if err := store.Set(ctx, key, retrievalcache.VectorCacheEntry{Hits: []retrieval.SearchHit{{ID: key}}}, time.Minute); err != nil {
			t.Fatal(err)
		}
	}
	set("a")
	set("b")
	if _, found, err := store.Get(ctx, "a"); err != nil || !found {
		t.Fatalf("read a: found = %t, err = %v", found, err)
	}
	set("c")
	if _, found, err := store.Get(ctx, "b"); err != nil || found {
		t.Fatalf("evicted b: found = %t, err = %v", found, err)
	}
}

func TestRedisVectorCacheRoundTripTTLAndVersion(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	store, err := retrievalcache.NewRedisVectorCache(retrievalcache.RedisVectorCacheConfig{Client: client, Prefix: "test-vector"})
	if err != nil {
		t.Fatal(err)
	}
	entry := retrievalcache.VectorCacheEntry{
		Hits:      []retrieval.SearchHit{{ID: "redis", Vector: []float32{1}}},
		CreatedAt: time.Now().UTC(),
	}
	if err := store.Set(context.Background(), "key", entry, time.Minute); err != nil {
		t.Fatal(err)
	}
	got, found, err := store.Get(context.Background(), "key")
	if err != nil || !found || got.Hits[0].ID != "redis" {
		t.Fatalf("entry = %#v, found = %t, err = %v", got, found, err)
	}
	if version, err := store.Version(context.Background(), "scope"); err != nil || version != 0 {
		t.Fatalf("initial version = %d, err = %v", version, err)
	}
	if version, err := store.BumpVersion(context.Background(), "scope"); err != nil || version != 1 {
		t.Fatalf("bumped version = %d, err = %v", version, err)
	}
	server.FastForward(time.Minute + time.Second)
	if _, found, err := store.Get(context.Background(), "key"); err != nil || found {
		t.Fatalf("expired found = %t, err = %v", found, err)
	}
}

type recordingVectorStore struct {
	mu          sync.Mutex
	calls       int
	hits        []retrieval.SearchHit
	optionsSeen bool
	dynamic     bool
	started     chan struct{}
	release     <-chan struct{}
}

func (s *recordingVectorStore) Invoke(ctx context.Context, request retrieval.VectorRequest, options ...runnable.Option) (retrieval.VectorResponse, error) {
	if len(options) > 0 {
		s.mu.Lock()
		s.optionsSeen = true
		s.mu.Unlock()
	}
	if request.Operation != retrieval.VectorQueryOperation {
		return retrieval.VectorResponse{}, nil
	}
	s.mu.Lock()
	s.calls++
	call := s.calls
	if s.started != nil && call == 1 {
		close(s.started)
	}
	hits := append([]retrieval.SearchHit(nil), s.hits...)
	dynamic := s.dynamic
	release := s.release
	s.mu.Unlock()
	if release != nil && call == 1 {
		select {
		case <-ctx.Done():
			return retrieval.VectorResponse{}, ctx.Err()
		case <-release:
		}
	}
	if dynamic {
		hits = []retrieval.SearchHit{{ID: fmt.Sprintf("call-%d", call)}}
	}
	return retrieval.VectorResponse{Hits: hits}, nil
}

func (s *recordingVectorStore) Stream(ctx context.Context, input retrieval.VectorRequest, options ...runnable.Option) (*runnable.StreamReader[retrieval.VectorResponse], error) {
	return runnable.StreamFromInvoke(ctx, input, s.Invoke, options...)
}

func (s *recordingVectorStore) Collect(ctx context.Context, input *runnable.StreamReader[retrieval.VectorRequest], options ...runnable.Option) (retrieval.VectorResponse, error) {
	return runnable.CollectFromInvoke(ctx, input, s.Invoke, options...)
}

func (s *recordingVectorStore) Transform(ctx context.Context, input *runnable.StreamReader[retrieval.VectorRequest], options ...runnable.Option) (*runnable.StreamReader[retrieval.VectorResponse], error) {
	return runnable.TransformFromInvoke(ctx, input, s.Invoke, options...)
}

func (s *recordingVectorStore) OptionsSeen() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.optionsSeen
}

func (s *recordingVectorStore) Calls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

func newCachedVectorStore(t *testing.T, upstream runnable.Runnable[retrieval.VectorRequest, retrieval.VectorResponse], ttl time.Duration) *retrievalcache.CachedVectorStore {
	t.Helper()
	store, err := retrievalcache.NewMemoryVectorCache(retrievalcache.MemoryVectorCacheConfig{Capacity: 100})
	if err != nil {
		t.Fatal(err)
	}
	cached, err := retrievalcache.NewCachedVectorStore(upstream, retrievalcache.VectorCacheConfig{
		Store: store, StoreID: "documents", TTL: ttl,
	})
	if err != nil {
		t.Fatal(err)
	}
	return cached
}

func mustVectorQuery(t *testing.T, store runnable.Runnable[retrieval.VectorRequest, retrieval.VectorResponse], ctx context.Context, query retrieval.VectorQuery) []retrieval.SearchHit {
	t.Helper()
	response, err := store.Invoke(ctx, vectorQueryRequest(query))
	if err != nil {
		t.Fatal(err)
	}
	return response.Hits
}

func vectorQueryRequest(query retrieval.VectorQuery) retrieval.VectorRequest {
	return retrieval.VectorRequest{Operation: retrieval.VectorQueryOperation, Query: &query}
}

func waitForVectorShared(t *testing.T, cached *retrievalcache.CachedVectorStore, want uint64) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for cached.Stats().Shared != want && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if cached.Stats().Shared != want {
		t.Fatalf("shared = %d, want %d", cached.Stats().Shared, want)
	}
}
