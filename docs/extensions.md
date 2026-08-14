# Optional Extensions

The framework module contains provider-neutral contracts and does not select a
cache backend or a Redis client. Optional implementations live in the nested
`extensions` module so applications can choose their own dependencies.

## Cache decorators

The cache extension decorates provider values and preserves the canonical
Runnable contracts, so cached models and vector stores can be passed directly
to Agents and RAG pipelines:

```go
import (
    llmcache "goharness/extensions/cache/llm"
    retrievalcache "goharness/extensions/cache/retrieval"
)

cachedModel, err := llmcache.NewCachedModel(model, llmcache.CacheConfig{
    Store:   memoryCache,
    ModelID: "provider:model",
})

cachedStore, err := retrievalcache.NewCachedVectorStore(vectorStore,
    retrievalcache.VectorCacheConfig{
        Store:   vectorCache,
        StoreID: "documents",
    })
```

The extension includes memory and Redis cache stores. They are not imported by
the core packages and can be replaced with a user-defined implementation of
the extension's `CacheStore` or `VectorCacheStore` contract.

## Redis checkpoint store

Redis checkpoint persistence is also an optional adapter:

```go
import redisstore "goharness/extensions/state/redis"

store, err := redisstore.NewRedisStore(redisstore.RedisStoreConfig{
    Client: redisClient,
    Prefix: "customer-support:checkpoint",
})
```

The returned value implements the core `state.Store` interface. Applications
that do not need Redis can keep the core `state.MemoryStore` or provide their
own store without pulling in a Redis dependency.

## MCP tools

The `extensions/mcp` package uses `github.com/mark3labs/mcp-go` to connect to
MCP servers over stdio, SSE or streamable HTTP. It translates `tools/list`
schemas into core `tool.Tool` values and exposes a dynamic `ToolsNode` for
Graph/Runnable composition. See [MCP Tools](mcp-tools.md).
