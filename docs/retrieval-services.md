# Retrieval and Inference HTTP Services

`pkg/httpclient.DefaultPool` is the shared transport boundary for HTTP-based
integrations. One `http.Transport` automatically maintains separate connection
pools per target host. Clients with different timeouts share that Transport.

```text
DefaultPool / one Transport
  client timeout=0s   -> LLM SSE streams
  client timeout=30s  -> vector and search REST APIs
  client timeout=10s  -> embedding and rerank APIs
```

## Service examples

Qdrant:

```go
service, err := httpclient.NewService(httpclient.ServiceConfig{
    BaseURL:     "https://qdrant.example.com",
    APIKey:      os.Getenv("QDRANT_API_KEY"),
    APIKeyHeader: "api-key",
    Timeout:      30 * time.Second,
})
```

Pinecone uses the same configuration with `APIKeyHeader: "Api-Key"`.
Elasticsearch API-key authentication can set `APIKeyHeader: "Authorization"`
and `APIKeyPrefix: "ApiKey "`. Chroma or internal Milvus REST gateways can omit
the API key. Hosted embedding and rerank APIs normally use the default Bearer
authorization.

Adapters implement provider-neutral `runnable.Runnable` contracts directly:

- Vector databases: `Runnable[retrieval.VectorRequest, retrieval.VectorResponse]`.
- Search engines: `Runnable[retrieval.TextSearchRequest, []retrieval.SearchHit]`.
- Embedding: `Runnable[retrieval.EmbeddingRequest, retrieval.EmbeddingResponse]`.
- Rerank: `Runnable[retrieval.RerankRequest, []retrieval.RerankResult]`.

`VectorRequest.Operation` selects query, upsert, or delete without requiring
multiple execution interfaces. HTTP adapters should use
`httpclient.NewService` or `httpclient.NewClient`.

## Pool isolation

Use a dedicated `httpclient.Pool` when an integration requires different mTLS
certificates, a different proxy, custom dial behavior, or strict traffic
isolation. Changing request timeout alone does not require a separate Pool.

Call `Pool.CloseIdleConnections` during controlled shutdown when a dedicated
Pool is no longer needed. Active requests are not interrupted.

## Vector query cache extension

Caching is intentionally outside the framework module. Import the optional
`goharness/extensions` module and wrap any concrete vector adapter with its
cache decorator:

```go
import retrievalcache "goharness/extensions/cache/retrieval"

cache, err := retrievalcache.NewMemoryVectorCache(retrievalcache.MemoryVectorCacheConfig{
    Capacity: 10_000,
})
vectorStore, err := retrievalcache.NewCachedVectorStore(qdrantStore, retrievalcache.VectorCacheConfig{
    Store:   cache,
    StoreID: "qdrant:documents",
    TTL:     time.Minute,
})
```

The extension module provides `RedisVectorCache` for multiple application
instances. Exact-query keys include tenant, Store ID, vector, TopK, filters,
namespace and include options. Concurrent misses for one key are combined.

Successful `VectorUpsertOperation` and `VectorDeleteOperation` invocations increment a per-namespace data
version. New queries stop seeing old cache entries immediately; old entries
expire through TTL instead of requiring a Redis key scan. Use
`retrievalcache.WithVectorCacheTenant` for tenant isolation and
`retrievalcache.WithVectorCacheBypass` for freshness-critical reads.
