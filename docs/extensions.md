# 可选扩展

框架模块包含与 Provider 无关的契约，不选择
缓存后端或 Redis 客户端。可选实现位于嵌套的
`extensions` 模块，应用可以按需选择依赖。

## 缓存装饰器

缓存扩展装饰 Provider 值并保留规范的
Runnable 契约，因此缓存模型和向量存储可以直接传给
Agent 和 RAG 管道：

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

扩展包含内存和 Redis 缓存存储。核心包不会导入这些依赖，
应用也可以替换为自定义的 `CacheStore` 或 `VectorCacheStore` 实现。

## Redis Checkpoint 存储

Redis Checkpoint 持久化同样是可选适配器：

```go
import redisstore "goharness/extensions/state/redis"

store, err := redisstore.NewRedisStore(redisstore.RedisStoreConfig{
    Client: redisClient,
    Prefix: "customer-support:checkpoint",
})
```

返回值实现核心 `state.Store` 接口。不需要 Redis 的应用可以继续使用核心 `state.MemoryStore`，或提供
自己的存储，而无需引入 Redis 依赖。

## MCP 工具

`extensions/mcp` 包使用 `github.com/mark3labs/mcp-go` 通过 stdio、SSE 或可流式 HTTP
连接 MCP 服务器。它将 `tools/list` schema 转换为核心 `tool.Tool` 值，并提供动态
`ToolsNode` 用于 Graph/Runnable 组合。详见 [MCP 工具](mcp-tools.md)。
