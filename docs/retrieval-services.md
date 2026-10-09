# 检索与推理 HTTP 服务

`pkg/httpclient.DefaultPool` 是基于 HTTP 的集成默认连接池。
一个 `http.Transport` 会按目标主机自动维护独立连接池，不同超时的客户端共享该 Transport。

```text
DefaultPool / one Transport
  client timeout=0s   -> LLM SSE streams
  client timeout=30s  -> vector and search REST APIs
  client timeout=10s  -> embedding and rerank APIs
```

## 服务示例

Qdrant 等 REST 集成用池化客户端与标准库构造请求：

```go
client := httpclient.NewClient(httpclient.ClientConfig{Timeout: 30 * time.Second})

request, err := http.NewRequestWithContext(ctx, http.MethodPost,
    "https://qdrant.example.com/collections/docs/points/search", body)
if err != nil {
    return err
}
request.Header.Set("Content-Type", "application/json")
request.Header.Set("api-key", os.Getenv("QDRANT_API_KEY"))
response, err := client.Do(request)
```

Pinecone 改用 `Api-Key` 头；Elasticsearch 用 `Authorization: ApiKey …`；
Chroma 或内部 Milvus REST 网关可以省略 API key。托管的 Embedding 和
Rerank API 通常使用默认 Bearer 授权。

适配器直接实现与 Provider 无关的 `runnable.Runnable` 契约：

- 向量数据库：`Runnable[retrieval.VectorRequest, retrieval.VectorResponse]`。
- 搜索引擎：`Runnable[retrieval.TextSearchRequest, []retrieval.SearchHit]`。
- Embedding：`Runnable[retrieval.EmbeddingRequest, retrieval.EmbeddingResponse]`。
- Rerank：`Runnable[retrieval.RerankRequest, []retrieval.RerankResult]`。

`VectorRequest.Operation` 选择 query、upsert 或 delete，无需多个执行接口。
HTTP 适配器应使用 `httpclient.NewClient` 获取池化客户端。

## 连接池隔离

当集成需要不同的 mTLS 证书、不同代理、自定义拨号行为或严格流量隔离时，
可以使用独立的 `httpclient.Pool`。仅改变请求超时不需要单独的 Pool。
受控关闭期间，专用 Pool 不再需要时调用 `Pool.CloseIdleConnections`；活动请求不会被中断。

## 向量查询缓存扩展

缓存有意放在框架模块之外。导入可选的
`github.com/goharness/goharness/extensions` 模块，并使用其中的缓存装饰器：

```go
import retrievalcache "github.com/goharness/github.com/goharness/goharness/extensions/cache/retrieval"

cache, err := retrievalcache.NewMemoryVectorCache(retrievalcache.MemoryVectorCacheConfig{
    Capacity: 10_000,
})
vectorStore, err := retrievalcache.NewCachedVectorStore(qdrantStore, retrievalcache.VectorCacheConfig{
    Store:   cache,
    StoreID: "qdrant:documents",
    TTL:     time.Minute,
})
```

扩展模块为多实例应用提供 `RedisVectorCache`。精确查询键包含 tenant、Store ID、vector、TopK、filters、namespace 和 include 选项；相同键的并发未命中会合并。

成功调用 `VectorUpsertOperation` 和 `VectorDeleteOperation` 会递增每个 namespace 的数据
版本。新查询会立即看不到旧缓存条目；旧条目通过 TTL 过期，无需扫描 Redis key。
使用 `retrievalcache.WithVectorCacheTenant` 实现 tenant 隔离，使用
`retrievalcache.WithVectorCacheBypass` 处理对新鲜度敏感的读取。
