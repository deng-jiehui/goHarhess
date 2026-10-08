package retrieval

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	coreretrieval "github.com/goharness/goharness/pkg/retrieval"
	"github.com/goharness/goharness/pkg/runnable"
)

const (
	DefaultVectorCacheTTL       = time.Minute
	DefaultVectorCacheNamespace = "vector-search"
	vectorCacheKeyVersion       = "v1"
	defaultVectorNamespace      = "_default"
)

// VectorCacheEntry is the immutable value stored for one exact vector query.
type VectorCacheEntry struct {
	Hits      []coreretrieval.SearchHit `json:"hits"`
	CreatedAt time.Time                 `json:"created_at"`
}

func (e VectorCacheEntry) Clone() VectorCacheEntry {
	return VectorCacheEntry{Hits: cloneSearchHits(e.Hits), CreatedAt: e.CreatedAt}
}

// VectorCacheStore stores query entries and per-scope data versions. Bumping a
// version invalidates previous entries without an expensive prefix scan.
type VectorCacheStore interface {
	Get(context.Context, string) (VectorCacheEntry, bool, error)
	Set(context.Context, string, VectorCacheEntry, time.Duration) error
	Version(context.Context, string) (uint64, error)
	BumpVersion(context.Context, string) (uint64, error)
}

type VectorCacheConfig struct {
	Store             VectorCacheStore
	StoreID           string
	CacheNamespace    string
	TTL               time.Duration
	StrictStoreErrors bool
}

type VectorCacheStats struct {
	Hits          uint64
	Misses        uint64
	Writes        uint64
	Shared        uint64
	Bypasses      uint64
	StoreErrors   uint64
	Invalidations uint64
}

type vectorCacheCounters struct {
	hits          atomic.Uint64
	misses        atomic.Uint64
	writes        atomic.Uint64
	shared        atomic.Uint64
	bypasses      atomic.Uint64
	storeErrors   atomic.Uint64
	invalidations atomic.Uint64
}

// CacheInvalidationError means the upstream mutation succeeded but its cache
// version could not be advanced. Callers should not blindly retry the mutation.
type CacheInvalidationError struct {
	Operation string
	Err       error
}

func (e *CacheInvalidationError) Error() string {
	return fmt.Sprintf("vector %s succeeded but cache invalidation failed: %v", e.Operation, e.Err)
}

func (e *CacheInvalidationError) Unwrap() error {
	return e.Err
}

// CachedVectorStore adds exact-query caching to a vector Runnable.
type CachedVectorStore struct {
	next              runnable.Runnable[coreretrieval.VectorRequest, coreretrieval.VectorResponse]
	store             VectorCacheStore
	storeID           string
	cacheNamespace    string
	ttl               time.Duration
	strictStoreErrors bool

	mu      sync.Mutex
	flights map[string]*vectorCacheFlight
	stats   vectorCacheCounters
}

var _ runnable.Runnable[coreretrieval.VectorRequest, coreretrieval.VectorResponse] = (*CachedVectorStore)(nil)

func NewCachedVectorStore(next runnable.Runnable[coreretrieval.VectorRequest, coreretrieval.VectorResponse], cfg VectorCacheConfig) (*CachedVectorStore, error) {
	if next == nil {
		return nil, errors.New("cached vector store requires an underlying store")
	}
	if cfg.Store == nil {
		return nil, errors.New("cached vector store requires a cache store")
	}
	storeID := strings.TrimSpace(cfg.StoreID)
	if storeID == "" {
		return nil, errors.New("cached vector store requires a store ID")
	}
	cacheNamespace := strings.TrimSpace(cfg.CacheNamespace)
	if cacheNamespace == "" {
		cacheNamespace = DefaultVectorCacheNamespace
	}
	ttl := cfg.TTL
	if ttl == 0 {
		ttl = DefaultVectorCacheTTL
	}
	if ttl < 0 {
		return nil, fmt.Errorf("vector cache TTL cannot be negative: %s", ttl)
	}
	return &CachedVectorStore{
		next:              next,
		store:             cfg.Store,
		storeID:           storeID,
		cacheNamespace:    cacheNamespace,
		ttl:               ttl,
		strictStoreErrors: cfg.StrictStoreErrors,
		flights:           map[string]*vectorCacheFlight{},
	}, nil
}

func (c *CachedVectorStore) Invoke(ctx context.Context, request coreretrieval.VectorRequest, options ...runnable.Option) (coreretrieval.VectorResponse, error) {
	switch request.Operation {
	case coreretrieval.VectorQueryOperation:
		if request.Query == nil {
			return coreretrieval.VectorResponse{}, errors.New("vector query operation requires a query")
		}
		hits, err := c.query(ctx, *request.Query, options...)
		return coreretrieval.VectorResponse{Hits: hits}, err
	case coreretrieval.VectorUpsertOperation:
		return coreretrieval.VectorResponse{}, c.upsert(ctx, request.Records, options...)
	case coreretrieval.VectorDeleteOperation:
		return coreretrieval.VectorResponse{}, c.delete(ctx, request.Namespace, request.IDs, options...)
	default:
		return coreretrieval.VectorResponse{}, fmt.Errorf("unsupported vector operation %q", request.Operation)
	}
}

func (c *CachedVectorStore) Stream(ctx context.Context, input coreretrieval.VectorRequest, options ...runnable.Option) (*runnable.StreamReader[coreretrieval.VectorResponse], error) {
	return runnable.StreamFromInvoke(ctx, input, c.Invoke, options...)
}
func (c *CachedVectorStore) Collect(ctx context.Context, input *runnable.StreamReader[coreretrieval.VectorRequest], options ...runnable.Option) (coreretrieval.VectorResponse, error) {
	return runnable.CollectFromInvoke(ctx, input, c.Invoke, options...)
}
func (c *CachedVectorStore) Transform(ctx context.Context, input *runnable.StreamReader[coreretrieval.VectorRequest], options ...runnable.Option) (*runnable.StreamReader[coreretrieval.VectorResponse], error) {
	return runnable.TransformFromInvoke(ctx, input, c.Invoke, options...)
}

func (c *CachedVectorStore) Stats() VectorCacheStats {
	return VectorCacheStats{
		Hits:          c.stats.hits.Load(),
		Misses:        c.stats.misses.Load(),
		Writes:        c.stats.writes.Load(),
		Shared:        c.stats.shared.Load(),
		Bypasses:      c.stats.bypasses.Load(),
		StoreErrors:   c.stats.storeErrors.Load(),
		Invalidations: c.stats.invalidations.Load(),
	}
}

type vectorCacheContextKey int

const (
	vectorCacheTenantKey vectorCacheContextKey = iota
	vectorCacheBypassKey
)

func WithVectorCacheTenant(ctx context.Context, tenant string) context.Context {
	return context.WithValue(ctx, vectorCacheTenantKey, strings.TrimSpace(tenant))
}

func WithVectorCacheBypass(ctx context.Context) context.Context {
	return context.WithValue(ctx, vectorCacheBypassKey, true)
}

func (c *CachedVectorStore) query(ctx context.Context, query coreretrieval.VectorQuery, options ...runnable.Option) ([]coreretrieval.SearchHit, error) {
	if ctx == nil {
		return nil, errors.New("vector query context cannot be nil")
	}
	query = cloneVectorQuery(query)
	if bypass, _ := ctx.Value(vectorCacheBypassKey).(bool); bypass {
		c.stats.bypasses.Add(1)
		return c.queryUpstream(ctx, query, options...)
	}

	scope := c.scopeKey(query.Namespace)
	version, err := c.store.Version(ctx, scope)
	if err != nil {
		c.stats.storeErrors.Add(1)
		if c.strictStoreErrors {
			return nil, fmt.Errorf("read vector cache version: %w", err)
		}
		c.stats.bypasses.Add(1)
		return c.queryUpstream(ctx, query, options...)
	}
	key, err := c.cacheKey(ctx, query, version)
	if err != nil {
		if c.strictStoreErrors {
			return nil, fmt.Errorf("build vector cache key: %w", err)
		}
		c.stats.bypasses.Add(1)
		return c.queryUpstream(ctx, query, options...)
	}
	entry, found, err := c.store.Get(ctx, key)
	if err != nil {
		c.stats.storeErrors.Add(1)
		if c.strictStoreErrors {
			return nil, fmt.Errorf("read vector cache: %w", err)
		}
		c.stats.bypasses.Add(1)
		return c.queryUpstream(ctx, query, options...)
	}
	if found {
		c.stats.hits.Add(1)
		return cloneSearchHits(entry.Hits), nil
	}
	c.stats.misses.Add(1)

	c.mu.Lock()
	if flight := c.flights[key]; flight != nil {
		c.stats.shared.Add(1)
		c.mu.Unlock()
		return flight.wait(ctx)
	}
	flight := &vectorCacheFlight{done: make(chan struct{})}
	c.flights[key] = flight
	c.mu.Unlock()

	hits, queryErr := c.queryUpstream(ctx, query, options...)
	if queryErr == nil {
		entry := VectorCacheEntry{Hits: cloneSearchHits(hits), CreatedAt: time.Now().UTC()}
		if setErr := c.store.Set(context.WithoutCancel(ctx), key, entry, c.ttl); setErr != nil {
			c.stats.storeErrors.Add(1)
			if c.strictStoreErrors {
				queryErr = fmt.Errorf("write vector cache: %w", setErr)
				hits = nil
			}
		} else {
			c.stats.writes.Add(1)
		}
	}
	c.completeFlight(key, flight, hits, queryErr)
	return cloneSearchHits(hits), queryErr
}

func (c *CachedVectorStore) upsert(ctx context.Context, records []coreretrieval.VectorRecord, options ...runnable.Option) error {
	if ctx == nil {
		return errors.New("vector upsert context cannot be nil")
	}
	cloned := cloneVectorRecords(records)
	if _, err := c.next.Invoke(ctx, coreretrieval.VectorRequest{Operation: coreretrieval.VectorUpsertOperation, Records: cloned}, options...); err != nil {
		return err
	}
	namespaces := make(map[string]struct{})
	for _, record := range cloned {
		namespaces[normalizeVectorNamespace(record.Namespace)] = struct{}{}
	}
	return c.invalidate(ctx, "upsert", namespaces)
}

func (c *CachedVectorStore) delete(ctx context.Context, namespace string, ids []string, options ...runnable.Option) error {
	if ctx == nil {
		return errors.New("vector delete context cannot be nil")
	}
	if _, err := c.next.Invoke(ctx, coreretrieval.VectorRequest{Operation: coreretrieval.VectorDeleteOperation, Namespace: namespace, IDs: append([]string(nil), ids...)}, options...); err != nil {
		return err
	}
	return c.invalidate(ctx, "delete", map[string]struct{}{normalizeVectorNamespace(namespace): {}})
}

func (c *CachedVectorStore) queryUpstream(ctx context.Context, query coreretrieval.VectorQuery, options ...runnable.Option) ([]coreretrieval.SearchHit, error) {
	response, err := c.next.Invoke(ctx, coreretrieval.VectorRequest{Operation: coreretrieval.VectorQueryOperation, Query: pointerToVectorQuery(cloneVectorQuery(query))}, options...)
	return cloneSearchHits(response.Hits), err
}

func (c *CachedVectorStore) invalidate(ctx context.Context, operation string, namespaces map[string]struct{}) error {
	var invalidationErrors []error
	for namespace := range namespaces {
		if _, err := c.store.BumpVersion(ctx, c.scopeKey(namespace)); err != nil {
			c.stats.storeErrors.Add(1)
			invalidationErrors = append(invalidationErrors, err)
			continue
		}
		c.stats.invalidations.Add(1)
	}
	if len(invalidationErrors) > 0 {
		return &CacheInvalidationError{Operation: operation, Err: errors.Join(invalidationErrors...)}
	}
	return nil
}

func (c *CachedVectorStore) cacheKey(ctx context.Context, query coreretrieval.VectorQuery, version uint64) (string, error) {
	tenant, _ := ctx.Value(vectorCacheTenantKey).(string)
	if tenant == "" {
		tenant = "default"
	}
	payload := struct {
		Version uint64                    `json:"data_version"`
		Query   coreretrieval.VectorQuery `json:"query"`
	}{Version: version, Query: query}
	data, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	storeHash := sha256.Sum256([]byte(c.storeID))
	tenantHash := sha256.Sum256([]byte(tenant))
	queryHash := sha256.Sum256(data)
	return fmt.Sprintf(
		"%s:%s:%s:%s:%s",
		c.cacheNamespace,
		vectorCacheKeyVersion,
		hex.EncodeToString(storeHash[:8]),
		hex.EncodeToString(tenantHash[:8]),
		hex.EncodeToString(queryHash[:]),
	), nil
}

func (c *CachedVectorStore) scopeKey(namespace string) string {
	namespace = normalizeVectorNamespace(namespace)
	sum := sha256.Sum256([]byte(c.cacheNamespace + "\x00" + c.storeID + "\x00" + namespace))
	return hex.EncodeToString(sum[:])
}

func (c *CachedVectorStore) completeFlight(key string, flight *vectorCacheFlight, hits []coreretrieval.SearchHit, err error) {
	flight.hits = cloneSearchHits(hits)
	flight.err = err
	close(flight.done)
	c.mu.Lock()
	if c.flights[key] == flight {
		delete(c.flights, key)
	}
	c.mu.Unlock()
}

type vectorCacheFlight struct {
	done chan struct{}
	hits []coreretrieval.SearchHit
	err  error
}

func (f *vectorCacheFlight) wait(ctx context.Context) ([]coreretrieval.SearchHit, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-f.done:
		return cloneSearchHits(f.hits), f.err
	}
}

func normalizeVectorNamespace(namespace string) string {
	namespace = strings.TrimSpace(namespace)
	if namespace == "" {
		return defaultVectorNamespace
	}
	return namespace
}

func cloneVectorQuery(query coreretrieval.VectorQuery) coreretrieval.VectorQuery {
	query.Vector = append([]float32(nil), query.Vector...)
	query.Filter = cloneMetadata(query.Filter)
	query.Raw = cloneStringAnyMap(query.Raw)
	return query
}

func cloneVectorRecords(records []coreretrieval.VectorRecord) []coreretrieval.VectorRecord {
	cloned := make([]coreretrieval.VectorRecord, len(records))
	for index, record := range records {
		cloned[index] = record
		cloned[index].Vector = append([]float32(nil), record.Vector...)
		cloned[index].Metadata = cloneMetadata(record.Metadata)
	}
	return cloned
}

func cloneSearchHits(hits []coreretrieval.SearchHit) []coreretrieval.SearchHit {
	cloned := make([]coreretrieval.SearchHit, len(hits))
	for index, hit := range hits {
		cloned[index] = hit
		cloned[index].Vector = append([]float32(nil), hit.Vector...)
		cloned[index].Metadata = cloneMetadata(hit.Metadata)
		cloned[index].Raw = cloneStringAnyMap(hit.Raw)
	}
	return cloned
}

func cloneStringAnyMap(values map[string]any) map[string]any {
	if values == nil {
		return nil
	}
	cloned := make(map[string]any, len(values))
	for key, value := range values {
		cloned[key] = cloneMetadataValue(value)
	}
	return cloned
}

func cloneMetadata(metadata coreretrieval.Metadata) coreretrieval.Metadata {
	if metadata == nil {
		return nil
	}
	cloned := make(coreretrieval.Metadata, len(metadata))
	for key, value := range metadata {
		cloned[key] = cloneMetadataValue(value)
	}
	return cloned
}

func cloneMetadataValue(value any) any {
	switch current := value.(type) {
	case coreretrieval.Metadata:
		return cloneMetadata(current)
	case map[string]any:
		cloned := make(map[string]any, len(current))
		for key, nested := range current {
			cloned[key] = cloneMetadataValue(nested)
		}
		return cloned
	case []any:
		cloned := make([]any, len(current))
		for index := range current {
			cloned[index] = cloneMetadataValue(current[index])
		}
		return cloned
	case []string:
		return append([]string(nil), current...)
	case []float32:
		return append([]float32(nil), current...)
	case []float64:
		return append([]float64(nil), current...)
	case []int:
		return append([]int(nil), current...)
	default:
		return current
	}
}

func pointerToVectorQuery(query coreretrieval.VectorQuery) *coreretrieval.VectorQuery {
	return &query
}
